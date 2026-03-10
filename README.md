# swf-panda-workers

Lightweight agents that manage the lifecycle of Panda workers and the transformer pipeline via message topics.

## Overview

This repository contains components that listen for run lifecycle messages and coordinate the creation, scaling, and shutdown of Panda workers. The agents interact with messaging topics to: start runs, adjust worker counts, submit iDDS/Panda tasks, and notify the transformer to stop when a run finishes.

## Architecture (high level)

- A controller listens to `/topic/panda.workers` for run lifecycle messages.
- On receiving a `run_imminent` or `adjust_worker` message, the controller triggers iDDS to create an iDDS workflow and a PanDA task. Two modes are supported, selectable via configuration:
  1. **Direct API mode** — the controller calls `idds.client.create_workflow_task()` directly (in-process iDDS client call).
  2. **Message mode** — the controller publishes a message to `/topic/idds.workflow`; the iDDS agent subscribes to that topic and creates the workflow and PanDA task asynchronously.
- PanDA jobs produced by the task are consumed by pilots/workers which run the transformer from `BNLNPPS/swf-transform`.
- In message mode, after iDDS creates the workflow and task it replies with a `created_workflow_task` message to `/topic/panda.workers`, containing `request_id`, `transform_id`, and `workload_id`. The transceiver stores the mapping `run_id → (request_id, transform_id, workload_id)`.
- The transceiver publishes `adjust_worker` messages to `/topic/idds.workflow` to request pilot scaling from Harvester, including the `run_id`, `request_id`, `transform_id`, `workload_id`, `core_count`, `memory_per_core`, and `site`.
- The transformer component (`BNLNPPS/swf-transform`) listens on `/topic/panda.transformer` for `end_run` messages; when received it transitions to stopping mode and exits once no more `slice` messages remain.

## Topics & message types

- `/topic/panda.workers`
	- `run_imminent` — request to create workers for a new run. Triggers iDDS workflow + PanDA task creation (direct API or message mode). Payloads include `target_worker_count` and may include `core_count`, `memory_per_core`, and `site`.
	- `created_workflow_task` — reply from iDDS (message mode) after successfully creating the workflow and PanDA task. Contains `request_id`, `transform_id`, and `workload_id`. The transceiver uses this to cache the mapping `run_id → (request_id, transform_id, workload_id)`.

- `/topic/idds.workflow`
	- `create_workflow_task` — published by the transceiver (message mode) to request iDDS to create an iDDS workflow and PanDA task. Includes `task_param_map` (full PanDA task parameters built by the worker handler), `core_count`, `memory_per_core`, and `site`. The iDDS agent subscribes to this topic and handles task submission asynchronously.
	- `adjust_worker` — published by the transceiver to request pilot scaling from Harvester. Includes `run_id`, `request_id`, `transform_id`, `workload_id`, `core_count`, `memory_per_core`, and `site`.

- `/topic/panda.transformer`
	- `slice` — payloads consumed by the transformer pipeline for processing.
	- `end_run` — signals that the run should stop; the transformer will finish processing existing slices then exit.

- `/queue/panda.results.worker`
	- `slice_result` — result messages published by the transformer after processing each slice. The worker agent subscribes to this queue to collect per-slice outcomes (timing, status, result payload) for monitoring and bookkeeping.

## License

This project is licensed under the terms in the `LICENSE` file.

## Contact

Repository: BNLNPPS/swf-panda-workers
For questions or to request features, please open an issue.
