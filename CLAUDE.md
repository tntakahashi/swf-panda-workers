# CLAUDE.md — Project guide for Claude Code

## Project overview

**swf-panda-workers** is a transceiver agent for EIC workflow management. It bridges run lifecycle messages (from the EIC control system) with iDDS/PanDA worker creation and scaling.

Package entrypoint: `swf_panda_workers.transceiver:main` → CLI command `swf-panda-workers`.

## Repository layout

```
config/
  swf_panda_workers.yaml          # Runtime configuration (all tunables)
lib/swf_panda_workers/
  transceiver.py                  # Transceiver agent: main loop, broker wiring, on_message dispatch
  brokers/
    activemq.py                   # Publisher / Subscriber wrappers (STOMP over ActiveMQ)
  prompt/handlers/
    workerhandler.py              # Message handlers + publish helpers (create/adjust/close)
    panda.py                      # PandaClient: iDDS REST API wrappers
pyproject.toml                    # Build config; install with `pip install -e .`
```

## Key architecture decisions

### Transceiver mode (`transceiver.mode` in yaml)

Two mutually exclusive modes — anything else raises `ValueError` at startup:

| Mode | Outbound iDDS calls |
|------|-------------------|
| `message` | Publishes STOMP messages to `/topic/idds.workflow` |
| `rest` | Calls iDDS REST API directly via `PandaClient` |

Mode is validated once in `Transceiver.__init__` and again defensively in `worker_handler`. `panda_client` is instantiated only in `rest` mode and passed through `handler_kwargs`.

### Message flow

```
run_imminent       → create_workflow_task  (message: STOMP | rest: PandaClient.idds_create_workflow_task)
created_workflow_task → cache run_id → {request_id, transform_id, workload_id}
slice_result       → adjust_worker         (message: STOMP | rest: PandaClient.idds_adjust_worker)
run_end/run_stop/end_run → stop_transformer broadcast + close_workflow_task
                                           (message: STOMP | rest: PandaClient.idds_close_workflow_task)
```

### Caches (TTLCache, 3-day TTL, in `Transceiver`)

- `run_to_idds_ids_cache`: `run_id → {run_id, request_id, transform_id, workload_id}`
- `run_to_core_count_cache`: `run_id → current core_count` (seeded from `run_imminent`, updated by `slice_result` scaling)

### Worker scaling (`slice_result`)

Configured via `slice.processing_time` (default 30 s):
- actual > 1.5 × threshold → scale core_count × 1.5
- actual > 1.2 × threshold → scale core_count × 1.2
- otherwise → no action

## Topics & message types

| Topic / Queue | Direction | Message types |
|---|---|---|
| `/topic/panda.workers` | inbound | `run_imminent`, `created_workflow_task`, `run_end`, `run_stop`, `end_run`, `transformer_heartbeat` |
| `/topic/idds.workflow` | outbound | `create_workflow_task`, `adjust_worker`, `close_workflow_task` |
| `/topic/panda.transformer` | outbound | `stop_transformer` (broadcast) |
| `/queue/panda.results.worker` | inbound | `slice_result` |

## handler_kwargs keys

Passed from `Transceiver._run_worker` into every `worker_handler` call:

| Key | Type | Description |
|---|---|---|
| `transformer_broadcaster` | `Publisher\|None` | Broadcasts `stop_transformer` |
| `idds_workflow_publisher` | `Publisher\|None` | Publishes to `/topic/idds.workflow` (message mode) |
| `panda_attributes` | `dict` | PanDA task params forwarded from `panda:` config section |
| `timetolive` | `int` | STOMP message TTL in ms |
| `slice_config` | `dict` | `{processing_time: <seconds>}` |
| `core_count_cache` | `TTLCache` | Shared mutable cache; `handle_slice_result` updates it in-place |
| `mode` | `str` | `"message"` or `"rest"` |
| `panda_client` | `PandaClient\|None` | Set only when `mode == "rest"` |

## PandaClient (panda.py)

iDDS server URL resolved from (in order): `$IDDS_SERVER` env var → `panda.idds_server` in `panda.cfg`.

Client obtained via:
```python
import pandaclient.idds_api as idds_api
import idds.common.utils as idds_utils
client = idds_api.get_api(idds_utils.json_dumps, idds_host=..., compress=True, manager=True)
```

Methods mirror the three outbound message types:
- `idds_create_workflow_task(run_id, content)`
- `idds_adjust_worker(run_id, idds_ids, content)`
- `idds_close_workflow_task(run_id, idds_ids)`

## Logging convention

All functions in `workerhandler.py` accept `logger=None` and fall back to the module-level `_logger = logging.getLogger(__name__)` via `logger = logger or _logger` at the top of each function body.

## Configuration file search order

1. `--config` CLI argument
2. `$SWF_PANDA_WORKERS_CONFIG` env var
3. `~/.config/swf_panda_workers/swf_panda_workers.yaml`
4. `$PREFIX/etc/swf_panda_workers/swf_panda_workers.yaml`
5. `<repo>/config/swf_panda_workers.yaml`

`${VAR}` placeholders in the yaml are expanded via `os.path.expandvars`.

## Install & run

```bash
pip install -e .
swf-panda-workers --config config/swf_panda_workers.yaml --log-level DEBUG
```
