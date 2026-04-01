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
Configuration loading and parsing for swf-panda-workers.
"""

import logging
import os
import sys

import yaml

_logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_NAME = "swf_panda_workers.yaml"


def load_config(config_path=None):
    """
    Load configuration from a YAML file.

    Search order (first existing file wins):
      1. ``config_path`` argument
      2. ``$SWF_PANDA_WORKERS_CONFIG`` environment variable
      3. ``~/.config/swf_panda_workers/swf_panda_workers.yaml``
      4. ``$PREFIX/etc/swf_panda_workers/swf_panda_workers.yaml``  (installed)
      5. ``<repo>/config/swf_panda_workers.yaml``                  (development)

    ``${VAR}`` placeholders in the file are expanded via :func:`os.path.expandvars`.
    """
    candidates = [
        config_path,
        os.environ.get("SWF_PANDA_WORKERS_CONFIG"),
        os.path.expanduser(f"~/.config/swf_panda_workers/{_DEFAULT_CONFIG_NAME}"),
        os.path.join(sys.prefix, "etc", "swf_panda_workers", _DEFAULT_CONFIG_NAME),
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "config", _DEFAULT_CONFIG_NAME),
    ]

    resolved = None
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            resolved = os.path.abspath(candidate)
            break

    if resolved is None:
        raise FileNotFoundError(
            f"Config file '{_DEFAULT_CONFIG_NAME}' not found. "
            f"Set SWF_PANDA_WORKERS_CONFIG or pass --config."
        )

    _logger.info(f"Loading config from {resolved}")
    with open(resolved) as fh:
        content = os.path.expandvars(fh.read())
    return yaml.safe_load(content)


def build_transceiver_kwargs(cfg):
    """
    Extract Transceiver constructor kwargs from a loaded config dict.

    :param cfg: dict returned by :func:`load_config`.
    :returns: dict suitable for ``**kwargs`` expansion into ``Transceiver()``.
    """
    t_cfg = cfg.get("transceiver", {})
    b_cfg = cfg.get("brokers", {})
    p_cfg = cfg.get("panda", {})
    s_cfg = cfg.get("slice", {})
    c_cfg = cfg.get("cache", {})

    return {
        "namespace": t_cfg.get("namespace", "dev"),
        "num_threads": t_cfg.get("num_threads", 8),
        "timetolive": t_cfg.get("timetolive", 12 * 3600 * 1000),
        "transformer_broadcast_broker": b_cfg.get("transformer_broadcast"),
        "worker_subscriber_broker": b_cfg.get("worker_subscriber"),
        "slice_result_subscriber_broker": b_cfg.get("slice_result_subscriber"),
        "panda_workers_publisher_broker": b_cfg.get("panda_workers_publisher"),
        "panda_attributes": p_cfg or {},
        "slice_config": s_cfg or {},
        "mode": t_cfg.get("mode", "message"),
        "cache_path": c_cfg.get("path"),
    }
