#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

import os
import traceback

import configparser as ConfigParser


class PandaClient(object):

    def __init__(self, *args, **kwargs):
        super(PandaClient, self).__init__()
        self.load_panda_urls()

    def load_panda_config(self):
        panda_config = ConfigParser.ConfigParser()
        if os.environ.get("IDDS_PANDA_CONFIG", None):
            configfile = os.environ["IDDS_PANDA_CONFIG"]
            if panda_config.read(configfile) == [configfile]:
                return panda_config

        configfiles = [
            "%s/etc/panda/panda.cfg" % os.environ.get("IDDS_HOME", ""),
            "/etc/panda/panda.cfg",
            "/opt/idds/etc/panda/panda.cfg",
            "%s/etc/panda/panda.cfg" % os.environ.get("VIRTUAL_ENV", ""),
        ]
        for configfile in configfiles:
            if panda_config.read(configfile) == [configfile]:
                return panda_config
        return panda_config

    def load_panda_urls(self):
        panda_config = self.load_panda_config()
        # self.logger.debug("panda config: %s" % panda_config)
        self.panda_url = None
        self.panda_url_ssl = None
        self.panda_monitor = None
        self.panda_auth = None
        self.panda_auth_vo = None
        self.panda_config_root = None
        self.pandacache_url = None
        self.panda_verify_host = None

        if panda_config.has_section("panda"):
            if "PANDA_MONITOR_URL" not in os.environ and panda_config.has_option(
                "panda", "panda_monitor_url"
            ):
                self.panda_monitor = panda_config.get("panda", "panda_monitor_url")
                os.environ["PANDA_MONITOR_URL"] = self.panda_monitor
                # self.logger.debug("Panda monitor url: %s" % str(self.panda_monitor))
            if "PANDA_URL" not in os.environ and panda_config.has_option(
                "panda", "panda_url"
            ):
                self.panda_url = panda_config.get("panda", "panda_url")
                os.environ["PANDA_URL"] = self.panda_url
                # self.logger.debug("Panda url: %s" % str(self.panda_url))
            if "PANDACACHE_URL" not in os.environ and panda_config.has_option(
                "panda", "pandacache_url"
            ):
                self.pandacache_url = panda_config.get("panda", "pandacache_url")
                os.environ["PANDACACHE_URL"] = self.pandacache_url
                # self.logger.debug("Pandacache url: %s" % str(self.pandacache_url))
            if "PANDA_VERIFY_HOST" not in os.environ and panda_config.has_option(
                "panda", "panda_verify_host"
            ):
                self.panda_verify_host = panda_config.get("panda", "panda_verify_host")
                os.environ["PANDA_VERIFY_HOST"] = self.panda_verify_host
                # self.logger.debug("Panda verify host: %s" % str(self.panda_verify_host))
            if "PANDA_URL_SSL" not in os.environ and panda_config.has_option(
                "panda", "panda_url_ssl"
            ):
                self.panda_url_ssl = panda_config.get("panda", "panda_url_ssl")
                os.environ["PANDA_URL_SSL"] = self.panda_url_ssl
                # self.logger.debug("Panda url ssl: %s" % str(self.panda_url_ssl))
            if "PANDA_AUTH" not in os.environ and panda_config.has_option(
                "panda", "panda_auth"
            ):
                self.panda_auth = panda_config.get("panda", "panda_auth")
                os.environ["PANDA_AUTH"] = self.panda_auth
            if "PANDA_AUTH_VO" not in os.environ and panda_config.has_option(
                "panda", "panda_auth_vo"
            ):
                self.panda_auth_vo = panda_config.get("panda", "panda_auth_vo")
                os.environ["PANDA_AUTH_VO"] = self.panda_auth_vo
            if "PANDA_CONFIG_ROOT" not in os.environ and panda_config.has_option(
                "panda", "panda_config_root"
            ):
                self.panda_config_root = panda_config.get("panda", "panda_config_root")
                os.environ["PANDA_CONFIG_ROOT"] = self.panda_config_root

    def get_idds_server(self):
        panda_config = self.load_panda_config()
        if "IDDS_SERVER" in os.environ:
            return os.environ["IDDS_SERVER"]
        if panda_config.has_section("panda") and panda_config.has_option("panda", "idds_server"):
            return panda_config.get("panda", "idds_server")
        return None

    def get_idds_client(self):
        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api

        idds_server = self.get_idds_server()
        client = idds_api.get_api(
            idds_utils.json_dumps,
            idds_host=idds_server,
            compress=True,
            verbose=False,
            manager=True,
        )
        return client

    def idds_create_workflow_task(self, workflow, logger=None, log_prefix=""):
        """
        Create an iDDS workflow and PanDA task directly via the iDDS client.

        Corresponds to the 'create_workflow_task' STOMP message.

        :param workflow: workflow_msg['content']['workflow'] — the full workflow dict
                        with run/task parameters embedded in workflow['content']
        :param logger: Optional logger
        :param log_prefix: Log message prefix
        """
        try:
            content = workflow.get("content", {})
            run_id = content.get("run_id")
            if logger:
                logger.info(log_prefix + f"idds_create_workflow_task: run_id={run_id}")
            client = self.get_idds_client()
            ret = client.create_workflow_task(workflow=workflow)
            if logger:
                logger.info(log_prefix + f"idds_create_workflow_task: result={ret}")
            return ret
        except Exception as ex:
            if logger:
                logger.error(log_prefix + str(ex))
                logger.error(traceback.format_exc())
            raise ex

    def idds_adjust_worker(self, content, logger=None, log_prefix=""):
        """
        Adjust the worker count for an iDDS transform directly via the iDDS client.

        Corresponds to the 'adjust_worker' STOMP message.

        :param content: adjust_msg['content'] with run_id, request_id, transform_id,
                        workload_id, core_count, memory_per_core, site
        :param logger: Optional logger
        :param log_prefix: Log message prefix
        """
        try:
            run_id = content.get("run_id")
            request_id = content.get("request_id")
            transform_id = content.get("transform_id")
            workload_id = content.get("workload_id")
            if logger:
                logger.info(
                    log_prefix
                    + f"idds_adjust_worker: run_id={run_id}, request_id={request_id}, "
                    + f"transform_id={transform_id}, workload_id={workload_id}"
                )
            client = self.get_idds_client()
            transform_properties = {
                "core_count": content.get("core_count"),
                "memory_per_core": content.get("memory_per_core"),
                "site": content.get("site"),
                "content": content,
            }
            ret = client.adjust_worker(
                request_id=request_id,
                transform_id=transform_id,
                workload_id=workload_id,
                parameters=transform_properties,
            )
            if logger:
                logger.info(log_prefix + f"idds_adjust_worker: result={ret}")
            return ret
        except Exception as ex:
            if logger:
                logger.error(log_prefix + str(ex))
                logger.error(traceback.format_exc())
            raise ex

    def idds_close_workflow_task(self, content, logger=None, log_prefix=""):
        """
        Close an iDDS workflow task directly via the iDDS client.

        Corresponds to the 'close_workflow_task' STOMP message.

        :param content: close_msg['content'] with request_id, transform_id, workload_id
        :param logger: Optional logger
        :param log_prefix: Log message prefix
        """
        try:
            request_id = content.get("request_id")
            run_id = content.get("run_id")
            if logger:
                logger.info(
                    log_prefix
                    + f"idds_close_workflow_task: run_id={run_id}, request_id={request_id}"
                )
            client = self.get_idds_client()
            ret = client.close_workflow_task(request_id=request_id, parameters=content)
            if logger:
                logger.info(log_prefix + f"idds_close_workflow_task: result={ret}")
            return ret
        except Exception as ex:
            if logger:
                logger.error(log_prefix + str(ex))
                logger.error(traceback.format_exc())
            raise ex
