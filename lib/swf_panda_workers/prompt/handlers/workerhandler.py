#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026


import datetime
import logging


_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------

def _build_create_workflow_task_message(msg, panda_attributes, timetolive):
    """
    Build a 'create_workflow_task' message dict and headers without publishing.

    Returns (workflow_msg, headers).
    """
    run_id = msg.get("run_id")
    content = msg.get("content", {})

    now = datetime.datetime.now(datetime.timezone.utc)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    site = content.get("site") or panda_attributes.get("site", "")
    scope = panda_attributes.get("scope", f"EIC_{year}")
    transform_tag = panda_attributes.get("transform_tag", "")

    workflow = {
        "scope": scope,
        "name": f"{scope}_{transform_tag}_fastprocessing_{site}_{year}{month}{day}",
        "requester": panda_attributes.get("username", ""),
        "username": panda_attributes.get("username", ""),
        "transform_tag": transform_tag,
        "cloud": panda_attributes.get("cloud", ""),
        "campaign": panda_attributes.get("campaign", ""),
        "campaign_scope": f"{panda_attributes.get('campaign', '')}_{year}",
        "campaign_group": f"{panda_attributes.get('campaign', '')}_{year}_{month}",
        "campaign_tag": panda_attributes.get("campaign_tag", ""),
        "content": {
            **content,
            "run_id": run_id,
            "created_at": now.isoformat(),
            "core_count": content.get("num_cores_per_worker") or content.get("core_count"),
            "memory_per_core": content.get("num_ram_per_core") or content.get("memory_per_core"),
            "site": site,
            "panda_attributes": panda_attributes,
            "workflow": workflow,
        },
    }

    workflow_msg = {
        "msg_type": "create_workflow_task",
        "run_id": run_id,
        "created_at": now.isoformat(),
        "content": {
            "run_id": run_id,
            "created_at": now.isoformat(),
            "workflow": workflow,
        },
    }

    headers = {
        "persistent": "true",
        "ttl": timetolive,
        "vo": "eic",
        "msg_type": "create_workflow_task",
        "run_id": str(run_id),
    }

    return workflow_msg, headers


def _build_adjust_worker_message(msg, idds_ids, timetolive):
    """
    Build an 'adjust_worker' message dict and headers without publishing.

    Returns (adjust_msg, headers).
    """
    run_id = msg.get("run_id")
    content = msg.get("content", {})
    idds_ids = idds_ids or {}

    adjust_msg = {
        "msg_type": "adjust_worker",
        "run_id": run_id,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "content": {
            "run_id": run_id,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "request_id": idds_ids.get("request_id"),
            "transform_id": idds_ids.get("transform_id"),
            "workload_id": idds_ids.get("workload_id"),
            "core_count": content.get("core_count") or content.get("num_cores_per_worker"),
            "memory_per_core": content.get("memory_per_core") or content.get("num_ram_per_core"),
            "site": content.get("site"),
        },
    }

    headers = {
        "persistent": "true",
        "ttl": timetolive,
        "vo": "eic",
        "msg_type": "adjust_worker",
        "run_id": str(run_id),
    }

    return adjust_msg, headers


def _build_close_workflow_task_message(idds_ids, run_id, timetolive):
    """
    Build a 'close_workflow_task' message dict and headers without publishing.

    Returns (close_msg, headers).
    """
    idds_ids = idds_ids or {}

    close_msg = {
        "msg_type": "close_workflow_task",
        "run_id": run_id,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "content": {
            "run_id": run_id,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "request_id": idds_ids.get("request_id"),
            "transform_id": idds_ids.get("transform_id"),
            "workload_id": idds_ids.get("workload_id"),
        },
    }

    headers = {
        "persistent": "true",
        "ttl": timetolive,
        "vo": "eic",
        "msg_type": "close_workflow_task",
        "run_id": str(run_id),
    }

    return close_msg, headers


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def handle_slice_result(msg, idds_ids, handler_kwargs, timetolive, logger):
    """
    Handle a 'slice_result' message: scale up workers if actual processing time
    exceeds the configured threshold by 20 % (scale 1.2×) or 50 % (scale 1.5×).

    Reads/updates core_count from handler_kwargs['core_count_cache'][run_id].
    Configured threshold comes from handler_kwargs['slice_config']['processing_time']
    (default 30 s).
    """
    logger = logger or _logger
    run_id = msg.get("run_id")
    content = msg.get("content", {})
    actual_time = content.get("processing_time")

    slice_cfg = handler_kwargs.get("slice_config", {})
    config_time = slice_cfg.get("processing_time", 30)

    core_count_cache = handler_kwargs.get("core_count_cache", {})
    current_core_count = core_count_cache.get(run_id)

    if actual_time is None or current_core_count is None:
        if logger:
            logger.warning(
                f"slice_result: cannot scale workers for run_id={run_id}; "
                f"actual_time={actual_time}, cached core_count={current_core_count}"
            )
        return

    if actual_time > config_time * 1.5:
        scale = 1.5
    elif actual_time > config_time * 1.2:
        scale = 1.2
    else:
        if logger:
            logger.info(
                f"slice_result: processing_time={actual_time}s within threshold "
                f"({config_time}s) for run_id={run_id}, no scaling needed"
            )
        return

    new_core_count = int(current_core_count * scale)
    core_count_cache[run_id] = new_core_count

    adjusted_msg = {
        "run_id": run_id,
        "content": {
            "core_count": new_core_count,
            "site": content.get("site"),
        },
    }

    mode = handler_kwargs.get("mode", "message")
    panda_workers_publisher = handler_kwargs.get("panda_workers_publisher")
    panda_client = handler_kwargs.get("panda_client")

    adjust_msg, headers = _build_adjust_worker_message(adjusted_msg, idds_ids, timetolive)
    if mode == "message":
        if panda_workers_publisher:
            panda_workers_publisher.publish(adjust_msg, headers=headers)
        else:
            logger.error(
                f"panda_workers_publisher not available; "
                f"cannot send adjust_worker for run_id={run_id}"
            )
    else:
        panda_client.idds_adjust_worker(adjust_msg["content"], logger=logger)

    logger.info(
        f"slice_result: scaled workers by {scale}x for run_id={run_id}, "
        f"core_count: {current_core_count} -> {new_core_count} "
        f"(actual_time={actual_time}s, threshold={config_time}s)"
    )


def worker_handler(_header, msg, idds_ids=None, handler_kwargs={}, logger=None):
    """
    Handle worker-related messages.

    Supported message types:
    - run_imminent: Publish create_workflow_task to /topic/panda.workers
    - created_workflow_task: Return iDDS IDs; publish adjust_worker to /topic/panda.workers
    - run_end/run_stop/end_run: Broadcast stop to transformers; publish close_workflow_task
    - slice_result: Scale up workers if processing_time exceeds threshold
    - transformer_heartbeat: Track transformer health

    :param header: Message header
    :param msg: Message body
    :param idds_ids: Dict with request_id, transform_id, workload_id (required for run_end)
    :param handler_kwargs: Handler configuration
    :return: Dict with run_id, request_id, transform_id, workload_id if created_workflow_task
    """
    logger = logger or _logger
    ret = {}
    msg_type = msg.get("msg_type")
    run_id = msg.get("run_id")

    timetolive = 12 * 3600 * 1000
    panda_attributes = {}

    transformer_broadcaster = handler_kwargs.get("transformer_broadcaster", None)
    panda_workers_publisher = handler_kwargs.get("panda_workers_publisher", None)
    timetolive = handler_kwargs.get("timetolive", timetolive)
    panda_attributes = handler_kwargs.get("panda_attributes", panda_attributes)
    mode = handler_kwargs.get("mode", "message")
    panda_client = handler_kwargs.get("panda_client", None)

    if mode not in ("message", "rest"):
        raise ValueError(
            f"Invalid transceiver mode: {mode!r}. Must be 'message' or 'rest'"
        )

    try:
        if msg_type == "run_imminent":
            workflow_msg, headers = _build_create_workflow_task_message(
                msg, panda_attributes, timetolive
            )
            if mode == "message":
                if panda_workers_publisher:
                    panda_workers_publisher.publish(workflow_msg, headers=headers)
                    logger.info(
                        f"Published create_workflow_task to /topic/panda.workers for run_id={run_id}"
                    )
                else:
                    logger.error(
                        f"panda_workers_publisher not available; "
                        f"cannot send create_workflow_task for run_id={run_id}"
                    )
            else:
                content = workflow_msg["content"]
                workflow = content["workflow"]
                panda_client.idds_create_workflow_task(workflow, logger=logger)
            logger.info(f"Handled run_imminent: run_id={run_id}")

        elif msg_type == "created_workflow_task":
            content = msg.get("content", {})
            request_id = content.get("request_id")
            transform_id = content.get("transform_id")
            workload_id = content.get("workload_id")
            ret = {
                "run_id": run_id,
                "request_id": request_id,
                "transform_id": transform_id,
                "workload_id": workload_id,
            }
            logger.info(
                f"Handled created_workflow_task: run_id={run_id}, "
                f"request_id={request_id}, transform_id={transform_id}, workload_id={workload_id}"
            )

        elif msg_type in ["run_end", "run_stop", "end_run"]:
            created_at_original = msg.get("created_at")
            if transformer_broadcaster:
                stop_msg = {
                    "msg_type": "stop_transformer",
                    "run_id": run_id,
                    "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "content": {"requested_at": created_at_original},
                }
                stop_header = {
                    "persistent": "true",
                    "ttl": timetolive,
                    "vo": "eic",
                    "msg_type": "stop_transformer",
                    "run_id": str(run_id),
                }
                transformer_broadcaster.publish(stop_msg, headers=stop_header)
                logger.info(f"Sent stop_transformer broadcast for run_id={run_id}")

            close_msg, headers = _build_close_workflow_task_message(idds_ids, run_id, timetolive)
            if mode == "message":
                if panda_workers_publisher:
                    panda_workers_publisher.publish(close_msg, headers=headers)
                    logger.info(
                        f"Published close_workflow_task to /topic/panda.workers for run_id={run_id}"
                    )
                else:
                    logger.error(
                        f"panda_workers_publisher not available; "
                        f"cannot send close_workflow_task for run_id={run_id}"
                    )
            else:
                panda_client.idds_close_workflow_task(close_msg["content"], logger=logger)
            logger.info(f"Handled {msg_type}: run_id={run_id}")

        elif msg_type == "slice_result":
            handle_slice_result(
                msg,
                idds_ids,
                handler_kwargs,
                timetolive,
                logger,
            )
            logger.info(f"Handled slice_result: run_id={run_id}")

        elif msg_type == "transformer_heartbeat":
            transformer_id = msg.get("content", {}).get("id")
            hostname = msg.get("content", {}).get("hostname")
            if logger:
                logger.info(
                    f"Transformer heartbeat: run_id={run_id}, "
                    f"transformer_id={transformer_id}, hostname={hostname}"
                )

        else:
            if logger:
                logger.warning(
                    f"Unknown message type in worker_handler: {msg_type}, run_id={run_id}"
                )

    except Exception as ex:
        if logger:
            logger.error(
                f"Error in worker_handler for msg_type={msg_type}, run_id={run_id}: {ex}",
                exc_info=True,
            )

    return ret
