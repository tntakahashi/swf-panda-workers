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

import logging
import signal
import threading
import traceback

from .brokers.activemq import Publisher, Subscriber
from .prompt.handlers.panda import PandaClient
from .prompt.handlers.workerhandler import worker_handler
from .utils.cache import PersistentTTLCache
from .utils.config import build_transceiver_kwargs, load_config

VALID_MODES = ("message", "rest")

logger = logging.getLogger(__name__)


class Transceiver:
    """
    Standalone transceiver agent for SWF Panda Workers.

    Receives workflow management messages on /topic/panda.workers and
    coordinates workflow creation, worker scaling, and transformer lifecycle
    via /topic/panda.workers and /topic/panda.transformer.

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
    ):
        if mode not in VALID_MODES:
            raise ValueError(
                f"Invalid transceiver mode: {mode!r}. Must be one of {VALID_MODES}"
            )
        self.logger = logging.getLogger(__name__)
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

    def get_run_id(self, msg):
        """Get run_id from message."""
        return msg.get("run_id", None)

    def cache_idds_ids(self, msg, idds_ids):
        """Cache {request_id, transform_id, workload_id} for a given run_id."""
        run_id = self.get_run_id(msg)
        if run_id:
            self.run_to_idds_ids_cache[run_id] = idds_ids
            self.logger.debug(f"Cached idds_ids={idds_ids} for run_id={run_id}")

    def get_idds_ids_from_cache(self, msg):
        """Retrieve cached {request_id, transform_id, workload_id} for a given run_id."""
        run_id = self.get_run_id(msg)
        idds_ids = self.run_to_idds_ids_cache.get(run_id, None)
        if not idds_ids:
            self.logger.warning(f"No cached idds_ids found for run_id={run_id}")
        return idds_ids

    def on_message(self, header, msg, handler_kwargs={}):
        """
        Dispatch incoming messages from /topic/panda.workers.

        Supported message types:
        - 'run_imminent':          publish create_workflow_task to /topic/panda.workers
        - 'created_workflow_task': cache iDDS IDs; publish adjust_worker
        - 'run_end' / 'run_stop':  broadcast stop_transformer; publish adjust_worker (0)
        - 'transformer_heartbeat': log heartbeat
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")

        self.logger.debug(f"Received message: msg_type={msg_type}, run_id={run_id}")

        try:
            if msg_type == "run_imminent":
                content = msg.get("content", {})
                core_count = content.get("num_cores_per_worker") or content.get("core_count")
                site = content.get("site") or self.panda_attributes.get("site")
                if core_count and run_id:
                    # store initial and current core_count along with site so scaling uses the initial value
                    try:
                        self.run_to_core_count_cache[run_id] = {
                            "initial_core_count": core_count,
                            "current_core_count": core_count,
                            "initial_site": site,
                            "current_site": site,
                        }
                    except Exception:
                        # fallback to storing a bare number if cache write fails
                        self.run_to_core_count_cache[run_id] = core_count
                worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
            elif msg_type == "created_workflow_task":
                ret = worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
                if ret:
                    self.cache_idds_ids(msg, ret)
            elif msg_type in ("adjusted_worker"):
                # iDDS may send an adjusted_worker message after applying new params
                ret = worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
                if ret:
                    self.cache_idds_ids(msg, ret)
            elif msg_type in ("closed_workflow_task"):
                # iDDS may notify workflow closure
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
                self.logger.warning(f"Unknown msg_type: {msg_type}")
        except Exception as error:
            self.logger.critical(
                f"on_message exception for msg_type={msg_type}: {error}\n{traceback.format_exc()}"
            )

    def _run_worker(self):
        """
        Connect publishers and subscriber, then monitor them until stopped.
        Intended to run in a dedicated thread; restarts from run() on unexpected exit.
        """
        self.logger.info("Worker thread starting")

        transformer_broadcaster = None
        worker_subscriber = None
        slice_result_subscriber = None
        panda_workers_publisher = None

        try:
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

            handler_kwargs = {
                "transformer_broadcaster": transformer_broadcaster,
                "panda_workers_publisher": panda_workers_publisher,
                "panda_attributes": self.panda_attributes,
                "timetolive": self.timetolive,
                "slice_config": self.slice_config,
                "core_count_cache": self.run_to_core_count_cache,
                # expose the persistent id mapping cache so handlers can record idds ids
                "run_to_idds_ids_cache": self.run_to_idds_ids_cache,
                "mode": self.mode,
                "panda_client": panda_client,
            }

            if self.worker_subscriber_broker:
                worker_subscriber = Subscriber(
                    name="WorkerSubscriber",
                    namespace=self.namespace,
                    broker=self.worker_subscriber_broker,
                    handler=self.on_message,
                    handler_kwargs=handler_kwargs,
                    logger=self.logger,
                )
            if self.slice_result_subscriber_broker:
                slice_result_subscriber = Subscriber(
                    name="SliceResultSubscriber",
                    namespace=self.namespace,
                    broker=self.slice_result_subscriber_broker,
                    handler=self.on_message,
                    handler_kwargs=handler_kwargs,
                    logger=self.logger,
                )

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
                    self.graceful_stop.wait(1)
                except Exception as error:
                    self.logger.critical(
                        "Worker monitor exception: %s\n%s"
                        % (str(error), traceback.format_exc())
                    )
        except Exception as error:
            self.logger.critical(
                "Worker setup exception: %s\n%s" % (str(error), traceback.format_exc())
            )
        finally:
            if transformer_broadcaster:
                transformer_broadcaster.stop()
            if panda_workers_publisher:
                panda_workers_publisher.stop()
            if worker_subscriber:
                worker_subscriber.stop()
            if slice_result_subscriber:
                slice_result_subscriber.stop()
            self.logger.info("Worker thread stopped")

    def stop(self):
        """Signal the agent to stop."""
        self.logger.info("Stop requested")
        self.graceful_stop.set()

    def run(self):
        """
        Start the agent. Launches _run_worker in a supervised thread and
        blocks until stopped (SIGTERM / SIGINT / stop()).
        """
        self.logger.info("Transceiver starting (namespace=%s)", self.namespace)

        def _supervised():
            while not self.graceful_stop.is_set():
                try:
                    self._run_worker()
                except Exception as error:
                    self.logger.critical(
                        "Worker thread crashed: %s\n%s"
                        % (str(error), traceback.format_exc())
                    )
                if not self.graceful_stop.is_set():
                    self.logger.info("Restarting worker thread in 5s …")
                    self.graceful_stop.wait(5)

        worker_thread = threading.Thread(target=_supervised, daemon=True, name="TransceiverWorker")
        worker_thread.start()

        try:
            while not self.graceful_stop.is_set():
                self.graceful_stop.wait(timeout=60)
        except KeyboardInterrupt:
            self.stop()

        worker_thread.join(timeout=15)
        self.logger.info("Transceiver stopped")


def main():
    """Entry point: ``swf-panda-workers`` CLI command."""
    import argparse

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
