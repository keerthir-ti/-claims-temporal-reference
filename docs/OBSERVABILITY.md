# Observability with ELK

The claims team already runs ELK for logs. This repo plugs Temporal into that stack instead of adding
another tool. Java and Python workers write JSON lines to stdout with the **same Temporal field
names**, Filebeat ships them, and Metricbeat scrapes the SDK metrics into the same Elasticsearch.

```bash
docker compose --profile elk up -d       # Elasticsearch, Kibana, Filebeat, Metricbeat, kibana-setup
open http://localhost:5601               # data views + saved searches are created automatically
```

> Resources: the ELK profile needs about 3 GB more on top of the core stack. On a Docker VM under
> about 10 GB, Elasticsearch gets OOM-killed. It restarts on its own, but Kibana then sits in
> "degraded" for minutes. The setup script tolerates both.

## What lands where

| Signal | Source | Index | Key fields |
|---|---|---|---|
| Application + SDK logs | stdout of every container labelled `claims.logs=true` | `claims-logs-*` | `claim.id`, `claim.track_run`, `app.WorkflowId`, `app.RunId`, `app.WorkflowType`, `app.TaskQueue`, `app.ActivityType`, `app.Attempt`, `app.level`, `message` |
| Temporal SDK metrics | Prometheus endpoint `:9464` on every worker and the bridge | `claims-metrics-*` | `prometheus.metrics.*`, `prometheus.labels.task_queue`, `workflow_type`, `activity_type` |
| Business timeline | `claim_projection` table (Postgres) | — (SQL) | `claim_id`, `stage_version`, `track`, `step`, `status`, `detail`, `workflow_id`, `run_id` |
| Execution detail | Temporal UI / CLI | — | full Event History per run |

How the fields line up across languages:

- **Java:** the Temporal SDK puts `WorkflowId`, `RunId`, `WorkflowType`, `TaskQueue`, `ActivityType`,
  `Attempt` into the SLF4J MDC, and `logstash-logback-encoder` writes the MDC as JSON fields
  (`java/src/main/resources/logback.xml`).
- **Python:** `python/claims/temporal/client/log_format.py` adds the same keys from `workflow.info()` /
  `activity.info()` to each record, and `python-json-logger` writes JSON.
- **Filebeat** (`observability/filebeat.yml`) parses the JSON into `app.*` and derives `claim.id`
  from the Workflow ID (`claim:{claimId}:{track}:{stageVersion}`). So **one field finds the parent,
  every child, the grandchild and every activity attempt**, in both languages.

## Saved searches (created by `observability/kibana-setup.sh`)

| Saved search | Query | Answers |
|---|---|---|
| Claim trail | `claim.id : "CLM-1003-XXXX"` | Everything that happened to one claim across bridge, parent, TSA, IS, STP |
| Lifecycle transitions | `app.message : Transition*` | Track changes with reasons |
| Activity retries | `app.Attempt > 1` | Flaky downstreams. Retried payments (idempotent replays). |
| Bridge dead letters | `app.message : DEAD_LETTER*` | Events for claims with no running lifecycle: reconcile them |
| Stale-stage rejections | `app.message : *stale*` | Late completions blocked by stage-version fencing |

Verified on this stack: one claim (CLM-1003) produced 41 log documents from 4 services (Java
lifecycle and STP, Python TSA and IS), all found with a single `claim.id` term.

## "How do I know events are not firing?" (kickoff question)

| Symptom | Where it shows | Meaning / action |
|---|---|---|
| ClaimsPro event never reached Temporal | Bridge logs: no `Signalled …`/`Signal-With-Start …` line for the claim | CDC feed or bridge problem. Alert on the bridge request rate dropping to 0. |
| Event reached a closed claim | `DEAD_LETTER` saved search | Expected after closure. Unexpected during an open claim (workflow failed?). |
| Event arrived but was not acted on | Parent log `Duplicate event … ignored` or `buffering` | Dedupe or not relevant to the active track (by design) |
| Event was lost entirely | STP log `Backstop poll found the estimate; ESTIMATE_RECEIVED event was missed` | The durable-timer backstop recovered it. Count these: a rising rate means the CDC feed is dropping events. |
| Claim stuck | Temporal visibility: `ClaimTrack = "MANUAL" AND StageVersion = …`, or `ExecutionStatus = "Running" AND StartTime < …` | Waiting on a human (SLA timer will escalate) or a pending activity (see retries) |

## Metrics to alert on

All are emitted by both SDKs. Names are as they appear in Prometheus or Metricbeat.

| Metric | Alert when | Usually means |
|---|---|---|
| `temporal_workflow_task_schedule_to_start_latency` | p95 > 1 s for 5 min | Too few workflow pollers or workers on that task queue (CAT surge) |
| `temporal_activity_schedule_to_start_latency` | p95 > 5 s | Activity backlog. Scale that track's deployment. |
| `temporal_workflow_task_execution_failed` | any increase | **Non-determinism after a deploy** or a payload that can't be deserialized. Page someone. |
| `temporal_activity_execution_failed` | rate rises | Downstream (ClaimsPro) failing. Check by `activity_type`. |
| `temporal_worker_task_slots_available` | 0 for long periods | Worker saturated |
| `temporal_sticky_cache_size` / `…_cache_miss` | misses rise | Cache too small. Workflows are replaying from scratch. |

Temporal Cloud also exposes **namespace-level** metrics (APS, throttling, service latency) through
its Prometheus-compatible endpoint. Scrape those into the same index with a second Metricbeat
`prometheus` module (bearer auth). The sample Grafana/Datadog dashboards shown on the kickoff call map
directly onto these series.

## Business view vs developer view

The meeting notes asked for two layers that can be joined when needed:

```
Business: "why did we pay claim X $2,450?"
  claim_projection → stage 4, STP, ISSUE_PAYMENT, "estimate 245000", workflow_id, run_id
                                                                         │
Developer: ─────────────────────────────────────────────────────────────┘
  Kibana  claim.id : X and app.WorkflowId : "claim:X:stp:4"   → every log line + retry of that run
  Temporal UI / archived history for run_id                    → exact signals, timers, activity inputs/outputs
```

## AI tracks (TSA / IS)

The AI calls are activities (`tsa.assessRisk`, `is.analyzeCoverage`). Keep the LLM/agent tracing
in the AI observability tool already in use ("LAMP" in the kickoff, presumably Langfuse),
and put the **Temporal `WorkflowId` and `ActivityId` into its trace metadata**. Then an AI trace
links to the claim trail in Kibana and back. Because activity results are recorded in history, a
replay never calls the model again, and the triage decision stays reproducible.
