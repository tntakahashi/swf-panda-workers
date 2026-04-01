# swf-panda-workers

Lightweight agents that manage the lifecycle of Panda workers and the transformer pipeline via message topics.

## Overview

This repository contains components that listen for run lifecycle messages and coordinate the creation, scaling, and shutdown of Panda workers. The agents interact with messaging topics to: start runs, adjust worker counts, submit iDDS/Panda tasks, and notify the transformer to stop when a run finishes.

## Architecture (high level)

- The transceiver listens to `/topic/panda.workers` for run lifecycle messages (`run_imminent`, `created_workflow_task`, `run_end`/`run_stop`/`end_run`, `transformer_heartbeat`) from the EIC control system and from iDDS.
- It also listens to `/queue/panda.results.worker` for `slice_result` messages from the transformer, and scales workers up if processing time exceeds the configured threshold.
- On receiving `run_imminent`, the transceiver triggers iDDS to create a workflow and PanDA task. Two modes are supported, selectable via configuration:
  1. **Message mode** — publishes a `create_workflow_task` message to `/topic/panda.workers`; the iDDS agent subscribes and creates the workflow and PanDA task asynchronously.
  2. **Direct API mode** — calls the iDDS REST API directly via `PandaClient`.
- After iDDS creates the workflow and task it replies with a `created_workflow_task` message to `/topic/panda.workers`. The transceiver caches the mapping `run_id → (request_id, transform_id, workload_id)`.
- The transceiver publishes `adjust_worker` and `close_workflow_task` messages to `/topic/panda.workers` for worker scaling and run teardown.
- On `run_end`/`run_stop`/`end_run`, the transceiver broadcasts a `stop_transformer` message to `/topic/panda.transformer` and closes the workflow task.
- PanDA jobs produced by the task are consumed by pilots/workers running the transformer from `BNLNPPS/swf-transform`.

## Topics & message types

- `/topic/panda.workers` (inbound)
  - `run_imminent` — request to create workers for a new run. Triggers iDDS workflow + PanDA task creation.
  - `created_workflow_task` — reply from iDDS after successfully creating the workflow and PanDA task. Contains `request_id`, `transform_id`, and `workload_id`.
  - `run_end` / `run_stop` / `end_run` — signals the run has finished; triggers `stop_transformer` broadcast and workflow close.
  - `transformer_heartbeat` — periodic heartbeat from the transformer; logged for monitoring.

- `/topic/panda.workers` (outbound, message mode)
  - `create_workflow_task` — sent by the transceiver to request iDDS to create a workflow and PanDA task.
  - `adjust_worker` — sent by the transceiver to request pilot scaling. Includes `run_id`, `request_id`, `transform_id`, `workload_id`, `core_count`, `memory_per_core`, and `site`.
  - `close_workflow_task` — sent by the transceiver to close the iDDS workflow when a run ends.

- `/topic/panda.transformer` (outbound)
  - `stop_transformer` — broadcast to signal transformers to stop after the run ends.

- `/queue/panda.results.worker` (inbound)
  - `slice_result` — result published by the transformer after processing each slice. The transceiver uses per-slice processing time to decide whether to scale workers up (1.2× or 1.5× depending on how much the threshold is exceeded).

## License

This project is licensed under the terms in the `LICENSE` file.

## Contact

Repository: BNLNPPS/swf-panda-workers
For questions or to request features, please open an issue.
