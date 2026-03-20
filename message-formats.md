# Message & API formats

## STOMP message formats (`/topic/idds.workflow`)

All outbound messages share the same STOMP headers:

```
persistent: "true"
ttl:        <timetolive ms>
vo:         "eic"
msg_type:   <see per-message below>
run_id:     <str(run_id)>
```

---

### `create_workflow_task`

Published on `run_imminent`. Requests iDDS to create a workflow and PanDA task.

```json
{
  "msg_type": "create_workflow_task",
  "run_id": "<run_id>",
  "created_at": "<ISO 8601 UTC>",
  "content": {
    "workflow": {
      "scope": "<panda_attributes.scope | 'EIC_<YYYY>'>",
      "name": "<scope>_<transform_tag>_fastprocessing_<site>_<YYYYMMDD>",
      "requester": "<panda_attributes.username>",
      "username": "<panda_attributes.username>",
      "transform_tag": "<panda_attributes.transform_tag>",
      "cloud": "<panda_attributes.cloud>",
      "campaign": "<panda_attributes.campaign>",
      "campaign_scope": "<campaign>_<YYYY>",
      "campaign_group": "<campaign>_<YYYY>_<MM>",
      "campaign_tag": "<panda_attributes.campaign_tag>",
      "content": {
        "run_id": "<run_id>",
        "created_at": "<ISO 8601 UTC>",
        "core_count": "<num_cores_per_worker or core_count from inbound msg>",
        "memory_per_core": "<num_ram_per_core or memory_per_core from inbound msg>",
        "site": "<content.site or panda_attributes.site>",
        "panda_attributes": {
          "vo": "wlcg",
          "queue": "<PanDA queue name, e.g. BNL_OSG_2>",
          "site": "<physical site, e.g. BNL_ATLAS_2>",
          "cloud": "<region, e.g. US>",
          "working_group": "<e.g. EIC>",
          "priority": 900,
          "core_count": 1,
          "ram_count": 4000,
          "num_workers": 1,
          "max_walltime": 3600,
          "max_attempt": 3,
          "idle_timeout": 120,
          "username": "iDDS",
          "task_type": "iDDS",
          "processing_type": "<e.g. urgent | null>"
        },
        "...": "<all other fields forwarded verbatim from the inbound run_imminent content>"
      }
    }
  }
}
```

---

### `adjust_worker`

Published on `slice_result` when processing time exceeds the scaling threshold.

```json
{
  "msg_type": "adjust_worker",
  "run_id": "<run_id>",
  "created_at": "<ISO 8601 UTC>",
  "content": {
    "run_id": "<run_id>",
    "created_at": "<ISO 8601 UTC>",
    "request_id": "<idds_ids.request_id>",
    "transform_id": "<idds_ids.transform_id>",
    "workload_id": "<idds_ids.workload_id>",
    "core_count": "<content.core_count or content.num_cores_per_worker>",
    "memory_per_core": "<content.memory_per_core or content.num_ram_per_core>",
    "site": "<content.site>"
  }
}
```

---

### `close_workflow_task`

Published on `run_end`, `run_stop`, or `end_run`.

```json
{
  "msg_type": "close_workflow_task",
  "run_id": "<run_id>",
  "created_at": "<ISO 8601 UTC>",
  "content": {
    "run_id": "<run_id>",
    "created_at": "<ISO 8601 UTC>",
    "request_id": "<idds_ids.request_id>",
    "transform_id": "<idds_ids.transform_id>",
    "workload_id": "<idds_ids.workload_id>"
  }
}
```

---

## PandaClient REST API methods (`panda.py`)

Used in `rest` mode instead of publishing STOMP messages. The `content` parameter in each method corresponds directly to the `content` field of the equivalent STOMP message.

---

### `PandaClient.idds_create_workflow_task(workflow)`

Corresponds to the `create_workflow_task` STOMP message.

| Parameter  | Type   | Value |
|------------|--------|-------|
| `workflow` | `dict` | `message["content"]["workflow"]` — the full workflow dict with run/task parameters |

Internally calls:
```python
client.create_workflow_task(workflow=workflow)
```

---

### `PandaClient.idds_adjust_worker(content)`

Corresponds to the `adjust_worker` STOMP message.

| Parameter | Type   | Value |
|-----------|--------|-------|
| `content` | `dict` | `message["content"]` — must include `request_id`, `transform_id`, `workload_id`, `core_count`, `memory_per_core`, `site` |

Internally calls:
```python
client.adjust_worker(
    request_id=content["request_id"],
    transform_id=content["transform_id"],
    workload_id=content["workload_id"],
    parameters={
        "core_count": content["core_count"],
        "memory_per_core": content["memory_per_core"],
        "site": content["site"],
        "content": content,
    },
)
```

---

### `PandaClient.idds_close_workflow_task(content)`

Corresponds to the `close_workflow_task` STOMP message.

| Parameter | Type   | Value |
|-----------|--------|-------|
| `content` | `dict` | `message["content"]` — must include `request_id`; `transform_id`, `workload_id`, `run_id` are forwarded as `parameters` |

Internally calls:
```python
client.close_workflow_task(
    request_id=content["request_id"],
    parameters=content,
)
```
