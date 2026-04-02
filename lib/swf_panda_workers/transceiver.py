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
Prompt Transceiver Agent

This agent receives and processes messages from the SWF Processing Agent
and manages workflow tasks, workers, and transformers.

Message Format Specification:
See main/prompt.md for complete message format specifications.

All messages should contain:
- 'msg_type': Type of message ('run_imminent', 'created_workflow_task', 'run_stop', etc.)
- 'run_id': Unique run identifier (e.g., 20250914185722)
- 'created_at': Timestamp when message was created
- 'content': Message-specific content

Headers should contain:
- 'persistent': 'true'
- 'ttl': Time to live in milliseconds (default: 12 * 3600 * 1000)
- 'vo': 'eic'
- 'instance': 'dev', 'prod', etc.
- 'msg_type': Message type
- 'run_id': Run identifier
"""

import json
import logging
import os
import random
import tempfile
import threading
import traceback

from swf_common_lib.base_agent import BaseAgent

from .brokers.activemq import Publisher, Subscriber
from .prompt.handlers.panda import PandaClient
from .prompt.handlers.workerhandler import worker_handler
from .utils.cache import PersistentTTLCache
from .utils.config import build_transceiver_kwargs, load_config

VALID_MODES = ("message", "rest")

logger = logging.getLogger(__name__)


class Transceiver(BaseAgent):
    """
    Transceiver agent for SWF Panda Workers.

    Extends BaseAgent for monitor heartbeat, registration, and state
    management.  Receives workflow management messages on
    /topic/panda.workers and /queue/panda.results.worker, then
    coordinates workflow creation, worker scaling, and transformer
    lifecycle via /topic/panda.workers and /topic/panda.transformer.

    Message flow:
      a. EIC          -> 'run_imminent'          -> /topic/panda.workers
      b. Transceiver  -> 'create_workflow_task'  -> /topic/panda.workers
      c. iDDS         -> 'created_workflow_task' -> /topic/panda.workers
      d. Transceiver  -> 'adjust_worker'         -> /topic/panda.workers
      e. Transformer  -> 'transformer_heartbeat' -> /topic/panda.workers
      f. EIC          -> 'run_end'               -> /topic/panda.workers
      g. Transceiver  -> 'stop_transformer'      -> /topic/panda.transformer  (broadcast)
      h. Transceiver  -> 'close_workflow_task'   -> /topic/panda.workers
    """

    def __init__(
        self,
        namespace=None,
        num_threads=8,
        timetolive=12 * 3600 * 1000,
        transformer_broadcast_broker=None,
        worker_subscriber_broker=None,
        slice_result_subscriber_broker=None,
        panda_workers_publisher_broker=None,
        panda_attributes=None,
        slice_config=None,
        mode="message",
        cache_path=None,
        config_path=None,
        debug=False,
    ):
        if mode not in VALID_MODES:
            raise ValueError(
                f"Invalid transceiver mode: {mode!r}. Must be one of {VALID_MODES}"
            )

        # BaseAgent requires a testbed.toml for namespace config.  When
        # namespace is supplied directly (from swf_panda_workers.yaml) and no
        # explicit config_path is given, write a minimal TOML so BaseAgent's
        # load_testbed_config() succeeds.
        _tmp = None
        if config_path is None and namespace is not None:
            _tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".toml", delete=False
            )
            _tmp.write(f'[testbed]\nnamespace = "{namespace}"\n')
            _tmp.close()
            config_path = _tmp.name

        try:
            super().__init__(
                agent_type="PanDA-worker",
                subscription_queue="/topic/epictopic",
                config_path=config_path,
                debug=debug,
            )
        finally:
            if _tmp is not None:
                os.unlink(_tmp.name)

        # yaml namespace takes priority over testbed.toml
        if namespace is not None:
            self.namespace = namespace

        self.num_threads = num_threads
        self.timetolive = timetolive
        self.transformer_broadcast_broker = transformer_broadcast_broker
        self.worker_subscriber_broker = worker_subscriber_broker
        self.slice_result_subscriber_broker = slice_result_subscriber_broker
        self.panda_workers_publisher_broker = panda_workers_publisher_broker
        self.panda_attributes = panda_attributes or {}
        self.slice_config = slice_config or {}
        self.mode = mode
        self.cache_path = cache_path

        self.graceful_stop = threading.Event()
        self._lock = threading.RLock()

        _ttl = 3600 * 24 * 3  # 3 days
        # Cache: run_id -> {run_id, request_id, transform_id, workload_id}, expiration 3 days
        self.run_to_idds_ids_cache = PersistentTTLCache(cache_path, "idds_ids", ttl=_ttl)
        # Cache: run_id -> current core_count, expiration 3 days
        self.run_to_core_count_cache = PersistentTTLCache(cache_path, "core_count", ttl=_ttl)

        # Populated by _run_worker; referenced by on_message (BaseAgent STOMP path)
        self._handler_kwargs = {}

    # ------------------------------------------------------------------
    # BaseAgent overrides
    # ------------------------------------------------------------------

    def get_next_agent_id(self):
        """Return an agent ID from the monitor API, or a random fallback."""
        try:
            return super().get_next_agent_id()
        except Exception:
            return str(random.randint(1000, 9999))

    def _api_request(self, method, endpoint, json_data=None):
        """Wrap BaseAgent's API request; silently skip when no monitor URL."""
        if not self.monitor_url:
            return None
        try:
            return super()._api_request(method, endpoint, json_data)
        except Exception as exc:
            self.logger.debug("Monitor API request failed (non-fatal): %s", exc)
            return None

    def on_message(self, frame):
        """
        Override BaseAgent's abstract on_message.

        Parses the raw STOMP frame and forwards to _dispatch.  This path is
        only exercised if BaseAgent's own self.conn is ever subscribed (which
        only happens if super().run() is called directly).  In normal
        operation the Subscriber objects in _run_worker call _dispatch
        directly.
        """
        try:
            msg = json.loads(frame.body)
        except (json.JSONDecodeError, TypeError) as exc:
            self.logger.warning("Could not parse STOMP frame as JSON: %s", exc)
            return
        self._dispatch(frame.headers, msg, self._handler_kwargs)

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def get_run_id(self, msg):
        """Get run_id from message."""
        return msg.get("run_id", None)

    def cache_idds_ids(self, msg, idds_ids):
        """Cache {request_id, transform_id, workload_id} for a given run_id."""
        run_id = self.get_run_id(msg)
        if run_id:
            self.run_to_idds_ids_cache[run_id] = idds_ids
            self.logger.debug("Cached idds_ids=%s for run_id=%s", idds_ids, run_id)

    def get_idds_ids_from_cache(self, msg):
        """Retrieve cached {request_id, transform_id, workload_id} for a given run_id."""
        run_id = self.get_run_id(msg)
        idds_ids = self.run_to_idds_ids_cache.get(run_id, None)
        if not idds_ids:
            self.logger.warning("No cached idds_ids found for run_id=%s", run_id)
        return idds_ids

    # ------------------------------------------------------------------
    # Message dispatch
    # ------------------------------------------------------------------

    def _dispatch(self, header, msg, handler_kwargs):
        """
        Route an incoming message to the appropriate worker_handler call.

        Called by Subscriber objects (via _MessageListener) and by on_message
        (via BaseAgent's STOMP connection).

        Supported message types:
        - 'run_imminent':          publish create_workflow_task to /topic/panda.workers
        - 'created_workflow_task': cache iDDS IDs; publish adjust_worker
        - 'run_end' / 'run_stop':  broadcast stop_transformer; publish adjust_worker (0)
        - 'transformer_heartbeat': log heartbeat
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")

        # Namespace filtering
        msg_namespace = msg.get("namespace")
        if self.namespace and msg_namespace and msg_namespace != self.namespace:
            self.logger.debug(
                "Ignoring message from namespace '%s' (ours: '%s')",
                msg_namespace,
                self.namespace,
            )
            return

        self.logger.debug("Received message: msg_type=%s, run_id=%s", msg_type, run_id)

        with self.processing():
            try:
                if msg_type == "run_imminent_worker":
                    content = msg.get("content", {})
                    core_count = content.get("num_cores_per_worker") or content.get("core_count")
                    site = content.get("site") or self.panda_attributes.get("site")
                    if core_count and run_id:
                        try:
                            self.run_to_core_count_cache[run_id] = {
                                "initial_core_count": core_count,
                                "current_core_count": core_count,
                                "initial_site": site,
                                "current_site": site,
                            }
                        except Exception:
                            self.run_to_core_count_cache[run_id] = core_count
                    worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
                elif msg_type == "created_workflow_task":
                    ret = worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
                    if ret:
                        self.cache_idds_ids(msg, ret)
                elif msg_type in ("adjusted_worker",):
                    ret = worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
                    if ret:
                        self.cache_idds_ids(msg, ret)
                elif msg_type in ("closed_workflow_task",):
                    ret = worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
                    if ret:
                        self.cache_idds_ids(msg, ret)
                elif msg_type in ("run_end", "end_run", "run_stop"):
                    idds_ids = self.get_idds_ids_from_cache(msg)
                    worker_handler(header, msg, idds_ids, handler_kwargs, logger=self.logger)
                elif msg_type == "slice_result":
                    idds_ids = self.get_idds_ids_from_cache(msg)
                    worker_handler(header, msg, idds_ids, handler_kwargs, logger=self.logger)
                elif msg_type == "transformer_heartbeat":
                    worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
                else:
                    self.logger.warning("Unknown msg_type: %s", msg_type)
            except Exception as error:
                self.logger.critical(
                    "on_message exception for msg_type=%s: %s\n%s",
                    msg_type,
                    error,
                    traceback.format_exc(),
                )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def stop(self):
        """Signal the agent to stop."""
        import signal as _signal
        self.logger.info("Stop requested")
        self.graceful_stop.set()
        # Interrupt BaseAgent's time.sleep(60) heartbeat loop via SIGTERM,
        # which BaseAgent.run() converts to KeyboardInterrupt.
        os.kill(os.getpid(), _signal.SIGTERM)

    def run(self):
        """
        Start the transceiver.

        Builds publishers and the slice-result subscriber upfront (so
        on_message() has handler_kwargs before BaseAgent starts dispatching),
        monitors them in a background thread, then delegates the main loop —
        primary STOMP subscription to self.subscription_queue, heartbeat,
        reconnect, and signal handling — to BaseAgent.run().
        """
        self.logger.info("Transceiver starting (namespace=%s)", self.namespace)

        transformer_broadcaster = None
        worker_subscriber = None
        panda_workers_publisher = None
        slice_result_subscriber = None

        if self.transformer_broadcast_broker:
            transformer_broadcaster = Publisher(
                name="TransformerBroadcaster",
                namespace=self.namespace,
                broker=self.transformer_broadcast_broker,
                broadcast=True,
                logger=self.logger,
            )
        if self.panda_workers_publisher_broker:
            panda_workers_publisher = Publisher(
                name="PandaWorkersPublisher",
                namespace=self.namespace,
                broker=self.panda_workers_publisher_broker,
                logger=self.logger,
            )

        panda_client = PandaClient() if self.mode == "rest" else None

        self._handler_kwargs = {
            "transformer_broadcaster": transformer_broadcaster,
            "panda_workers_publisher": panda_workers_publisher,
            "panda_attributes": self.panda_attributes,
            "timetolive": self.timetolive,
            "slice_config": self.slice_config,
            "core_count_cache": self.run_to_core_count_cache,
            "run_to_idds_ids_cache": self.run_to_idds_ids_cache,
            "mode": self.mode,
            "panda_client": panda_client,
        }

        if self.worker_subscriber_broker:
            worker_subscriber = Subscriber(
                name="WorkerSubscriber",
                namespace=self.namespace,
                broker=self.worker_subscriber_broker,
                handler=self._dispatch,
                handler_kwargs=self._handler_kwargs,
                logger=self.logger,
            )
        if self.slice_result_subscriber_broker:
            slice_result_subscriber = Subscriber(
                name="SliceResultSubscriber",
                namespace=self.namespace,
                broker=self.slice_result_subscriber_broker,
                handler=self._dispatch,
                handler_kwargs=self._handler_kwargs,
                logger=self.logger,
            )

        def _monitor():
            while not self.graceful_stop.is_set():
                try:
                    if transformer_broadcaster:
                        transformer_broadcaster.monitor()
                    if panda_workers_publisher:
                        panda_workers_publisher.monitor()
                    if worker_subscriber:
                        worker_subscriber.monitor()
                    if slice_result_subscriber:
                        slice_result_subscriber.monitor()
                except Exception as error:
                    self.logger.critical(
                        "Monitor exception: %s\n%s", str(error), traceback.format_exc()
                    )
                self.graceful_stop.wait(1)

        monitor_thread = threading.Thread(
            target=_monitor, daemon=True, name="TransceiverMonitor"
        )
        monitor_thread.start()

        try:
            super().run()  # blocks: subscribe, heartbeat loop, reconnect, signal handling
        finally:
            self.graceful_stop.set()
            if transformer_broadcaster:
                transformer_broadcaster.stop()
            if panda_workers_publisher:
                panda_workers_publisher.stop()
            if worker_subscriber:
                worker_subscriber.stop()
            if slice_result_subscriber:
                slice_result_subscriber.stop()
            monitor_thread.join(timeout=15)
            self.logger.info("Transceiver stopped")


def main():
    """Entry point: ``swf-panda-workers`` CLI command."""
    import argparse
    import signal

    parser = argparse.ArgumentParser(
        description="SWF Panda Workers — transceiver agent",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config", metavar="PATH",
        help="Path to swf_panda_workers.yaml (overrides default search path)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)-30s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    cfg = load_config(args.config)
    agent = Transceiver(**build_transceiver_kwargs(cfg))

    def _handle_signal(signum, _):
        logger.info("Received signal %d, shutting down …", signum)
        agent.stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    agent.run()


if __name__ == "__main__":
    main()
