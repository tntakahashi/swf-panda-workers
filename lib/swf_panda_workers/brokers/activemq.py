#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""
ActiveMQ broker clients for SWF Panda workers.

Provides:
  - Publisher  : connects to a broker, sends messages, auto-reconnects.
  - Subscriber : connects to a broker, receives messages, dispatches to a
                 handler callable, auto-resubscribes.

Broker configuration dict (passed as `broker` kwarg):
{
    "host":      "localhost",      # required
    "port":      61613,            # required (STOMP port)
    "destination": "/topic/...",   # required
    "username":  "admin",          # optional
    "password":  "admin",          # optional
    "use_ssl":   false,            # optional (default: false)
    "ssl_key_file":    "...",      # optional, path to client key
    "ssl_cert_file":   "...",      # optional, path to client certificate
    "ssl_ca_certs":    "...",      # optional, path to CA bundle
    "selector":  "instance='prod' AND run_id='123'",  # optional
}

Usage example
-------------
publisher = Publisher(
    name="WorkerPublisher",
    namespace="prod",
    broker={"host": "localhost", "port": 61613, "destination": "/topic/panda.workers"},
)
publisher.publish({"msg_type": "run_imminent", ...}, headers={...})
publisher.monitor()   # call periodically; reconnects if the connection dropped
publisher.stop()

subscriber = Subscriber(
    name="WorkerSubscriber",
    namespace="prod",
    broker={"host": "localhost", "port": 61613, "destination": "/topic/panda.workers"},
    handler=my_handler_fn,           # callable(header, msg, handler_kwargs)
    handler_kwargs={"key": "value"},
)
subscriber.monitor()  # call periodically; resubscribes if needed
subscriber.stop()
"""

import json
import logging
import os
import threading
import traceback

import stomp

logger = logging.getLogger(__name__)

if os.environ.get("STOMP_DEBUG") in ("1", "true", "True"):
    logging.getLogger("stomp").setLevel(logging.DEBUG)
else:
    logging.getLogger("stomp").setLevel(logging.CRITICAL)

# ---------------------------------------------------------------------------
# Internal STOMP listener
# ---------------------------------------------------------------------------


class _MessageListener(stomp.ConnectionListener):
    """STOMP listener that dispatches received frames to a handler callable."""

    def __init__(self, handler, handler_kwargs=None, name="Listener", log=None):
        self._handler = handler
        self._handler_kwargs = handler_kwargs or {}
        self._name = name
        self._log = log or logger

    def on_message(self, frame):
        """Called by stomp.py for every MESSAGE frame."""
        headers = frame.headers
        raw_body = frame.body

        try:
            msg = json.loads(raw_body)
        except (json.JSONDecodeError, TypeError) as exc:
            self._log.warning(
                f"[{self._name}] Could not parse message body as JSON: {exc} | raw={raw_body!r}"
            )
            return

        try:
            self._handler(headers, msg, self._handler_kwargs)
        except Exception as exc:
            self._log.error(
                f"[{self._name}] handler raised exception: {exc}\n{traceback.format_exc()}"
            )

    def on_error(self, frame):
        self._log.error(f"[{self._name}] STOMP error frame: {frame.body!r}")

    def on_disconnected(self):
        self._log.warning(f"[{self._name}] STOMP connection lost.")

    def on_connected(self, frame):
        self._log.info(f"[{self._name}] STOMP connection established.")


# ---------------------------------------------------------------------------
# Internal connection helper
# ---------------------------------------------------------------------------

def _build_connection(broker: dict, listener=None, log=None):
    """
    Build and return a connected stomp.Connection.

    :param broker: broker config dict (see module docstring).
    :param listener: optional stomp.ConnectionListener to attach before connect.
    :param log: logger instance.
    :returns: connected stomp.Connection.
    :raises: stomp.exception.ConnectFailedException on failure.
    """
    _log = log or logger
    host = broker["host"]
    port = int(broker.get("port", 61613))
    username = broker.get("username", "")
    password = broker.get("password", "")
    use_ssl = broker.get("use_ssl", False)

    conn = stomp.Connection12(
        host_and_ports=[(host, port)],
        keepalive=True,
        try_loopback_connect=False,
        auto_content_length=False,
        # Shorter heartbeats (ms) so client/broker detect dead peers faster
        heartbeats=(50000, 50000),
        # timeout=broker_timeout,
    )

    if use_ssl:
        conn.set_ssl(
            for_hosts=[(host, port)],
            key_file=broker.get("ssl_key_file"),
            cert_file=broker.get("ssl_cert_file"),
            ca_certs=broker.get("ssl_ca_certs"),
        )

    if listener is not None:
        conn.set_listener("", listener)

    _log.debug(f"Connecting to STOMP broker {host}:{port}")
    conn.connect(username, password, wait=True)
    return conn


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------

class Publisher:
    """
    STOMP publisher for a single destination.

    Thread-safe: `publish()` acquires a lock; `monitor()` reconnects as needed.

    Constructor args
    ----------------
    name : str
        Human-readable label used in log messages.
    namespace : str
        The `instance` value injected into published message headers when the
        caller does not supply an ``instance`` header explicitly.
    broker : dict
        Broker configuration (host, port, destination, credentials, SSL …).
        See module docstring.
    broadcast : bool
        Unused by this implementation (kept for API compatibility with the
        iDDS version).  When True, the destination is used as-is (topic).
    logger : logging.Logger, optional
        Parent logger; falls back to the module logger.
    """

    def __init__(
        self,
        name="Publisher",
        namespace=None,
        broker=None,
        broadcast=False,
        logger=None,
    ):
        self._name = name
        self._namespace = namespace
        self._broker = broker or {}
        self._broadcast = broadcast
        self._log = logger or logging.getLogger(__name__)

        self._conn = None
        self._lock = threading.Lock()
        self._stopped = False

        self._destination = self._broker.get("destination", "")

        self._connect()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, msg: dict, headers: dict = None):
        """
        Serialize *msg* to JSON and send it to the configured destination.

        :param msg:     Message body (dict).
        :param headers: STOMP headers dict.  ``instance`` is injected from
                        ``self._namespace`` if absent.
        """
        if self._stopped:
            self._log.warning(f"[{self._name}] publish() called after stop(); ignoring.")
            return

        send_headers = dict(headers or {})
        if self._namespace and "instance" not in send_headers:
            send_headers["instance"] = self._namespace

        body = json.dumps(msg, default=str)

        with self._lock:
            if not self._is_connected():
                self._log.warning(
                    f"[{self._name}] Not connected; attempting reconnect before publish."
                )
                self._connect()

            try:
                self._conn.send(
                    destination=self._destination,
                    body=body,
                    headers=send_headers,
                    content_type="application/json",
                )
                self._log.debug(
                    f"[{self._name}] Sent msg_type={msg.get('msg_type')} "
                    f"run_id={msg.get('run_id')} to {self._destination}"
                )
            except Exception as exc:
                self._log.error(
                    f"[{self._name}] Failed to send message: {exc}\n{traceback.format_exc()}"
                )
                self._conn = None  # mark as disconnected for next monitor() cycle

    def monitor(self):
        """
        Check connection health and reconnect if necessary.
        Call this periodically from the main worker loop.
        """
        if self._stopped:
            return
        with self._lock:
            if not self._is_connected():
                self._log.info(f"[{self._name}] monitor(): reconnecting …")
                self._connect()

    def stop(self):
        """Disconnect cleanly."""
        self._stopped = True
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.disconnect()
                except Exception:
                    pass
                self._conn = None
        self._log.info(f"[{self._name}] stopped.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_connected(self):
        return self._conn is not None and self._conn.is_connected()

    def _connect(self):
        """Attempt to create a new STOMP connection (no-op if broker config empty)."""
        if not self._broker.get("host"):
            return
        try:
            self._conn = _build_connection(self._broker, log=self._log)
            self._log.info(
                f"[{self._name}] Connected to {self._broker['host']}:{self._broker.get('port', 61613)} "
                f"destination={self._destination}"
            )
        except Exception as exc:
            self._log.error(
                f"[{self._name}] Connection failed: {exc}"
            )
            self._conn = None


# ---------------------------------------------------------------------------
# Subscriber
# ---------------------------------------------------------------------------

class Subscriber:
    """
    STOMP subscriber for a single destination.

    Spawns a background connection thread (STOMP heartbeating / I/O is
    handled by stomp.py internally).  Received messages are dispatched to
    *handler* on the stomp.py receiver thread.

    Constructor args
    ----------------
    name : str
        Human-readable label used in log messages.
    namespace : str
        The ``instance`` selector value injected into the STOMP subscription
        headers (combined with any broker-level selector).
    broker : dict
        Broker configuration (host, port, destination, credentials,
        selector, prefetch_size, SSL …).  See module docstring.
    handler : callable(header: dict, msg: dict, handler_kwargs: dict)
        Function called for each received message.
    handler_kwargs : dict, optional
        Extra keyword arguments forwarded to *handler* unchanged.
    logger : logging.Logger, optional
    """

    # Subscriber ID counter (unique per process)
    _id_counter = 0
    _id_lock = threading.Lock()

    def __init__(
        self,
        name="Subscriber",
        namespace=None,
        broker=None,
        handler=None,
        handler_kwargs=None,
        logger=None,
    ):
        self._name = name
        self._namespace = namespace
        self._broker = broker or {}
        self._handler = handler
        self._handler_kwargs = handler_kwargs or {}
        self._log = logger or logging.getLogger(__name__)

        self._conn = None
        self._lock = threading.Lock()
        self._stopped = False
        self._subscription_id = self._next_id()

        self._destination = self._broker.get("destination", "")

        self._connect_and_subscribe()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def monitor(self):
        """
        Re-subscribe if the connection has been lost.
        Call this periodically from the main worker loop.
        """
        if self._stopped:
            return
        with self._lock:
            if not self._is_connected():
                self._log.info(f"[{self._name}] monitor(): reconnecting and resubscribing …")
                self._connect_and_subscribe()

    def stop(self):
        """Unsubscribe and disconnect cleanly."""
        self._stopped = True
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.unsubscribe(id=self._subscription_id)
                except Exception:
                    pass
                try:
                    self._conn.disconnect()
                except Exception:
                    pass
                self._conn = None
        self._log.info(f"[{self._name}] stopped.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @classmethod
    def _next_id(cls):
        with cls._id_lock:
            cls._id_counter += 1
            return cls._id_counter

    def _is_connected(self):
        return self._conn is not None and self._conn.is_connected()

    def _build_selector(self):
        """
        Merge a namespace/instance filter with any broker-level selector.

        Returns a combined selector string, or None if nothing to filter.
        """
        parts = []

        # Namespace (instance) filter
        if self._namespace:
            parts.append(f"instance='{self._namespace}'")

        # Broker-level selector (may already include run_id filter, etc.)
        broker_selector = self._broker.get("selector", "")
        if broker_selector:
            parts.append(f"({broker_selector})")

        return " AND ".join(parts) if parts else None

    def _connect_and_subscribe(self):
        """Create connection, attach listener, and subscribe."""
        if not self._broker.get("host"):
            return

        try:
            listener = _MessageListener(
                handler=self._handler,
                handler_kwargs=self._handler_kwargs,
                name=self._name,
                log=self._log,
            )
            self._conn = _build_connection(self._broker, listener=listener, log=self._log)

            sub_headers = {}
            selector = self._build_selector()
            if selector:
                sub_headers["selector"] = selector

            self._conn.subscribe(
                destination=self._destination,
                id=self._subscription_id,
                ack="auto",
                headers=sub_headers,
            )
            self._log.info(
                f"[{self._name}] Subscribed to {self._destination} "
                f"(selector={selector or 'none'}, id={self._subscription_id})"
            )
        except Exception as exc:
            self._log.error(
                f"[{self._name}] Subscribe failed: {exc}\n{traceback.format_exc()}"
            )
            self._conn = None
