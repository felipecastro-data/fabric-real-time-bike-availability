# Design: scalability, cost, governance and security

This document covers how the streaming platform behaves, what it costs, who owns and can reach what, and what changes
as it grows. The reasoning behind the main choices is in the [architecture decision records](adr/).

Figures are from dev, test and the first prod deploy (2026-11-12 → 11-13) unless stated otherwise. Rows marked
**(7-day run)** are filled in from prod after the 2026-11-28 → 12-05 run.

## 1. Scalability

### Volumes

| Stream / table | Volume | Notes |
|---|---:|---|
| GBFS `station_status` | 2,079 stations, 763 KB per feed, `ttl` 60 s | 1,178 classic + 901 lightweight |
| Events pushed (every station per changed feed) | ≈ 2.7M / day (upper bound 3.0M) | [ADR 0003](adr/0003-push-full-snapshots-dedupe-downstream.md); hourly restart gaps excluded |
| Distinct states `(station_id, last_reported)` | 57 % of events in dev (3,569 / 6,237) | Prod ratio: **(7-day run)** |
| Peak ingest | 69 events/s in a minute (dev) | One poll = 2,079 events sent in batches ≤ 64 KB |
| `dim_station` | 2,079 rows + one per real change | Grows by rename, capacity change, relocation > 100 m or removal |
| `fact_availability_15min` | 2,079 rows per 15-minute window ≈ 200k / day | One row per station and window, from every valid poll |

### Compute per environment

| Environment | Starter pool max nodes | Session timeout | Runs |
|---|---:|---:|---|
| dev | 2 | 20 min | On demand: bounded poller runs, Silver/Gold `availableNow` |
| test | 2 | 20 min | The deploy smoke run only |
| prod | 4 | 30 min | Smoke run on each deploy; 24/7 schedules 2026-11-28 → 12-05 |

Prod schedules (`infra/prod_schedules.json`):

- poller hourly, about 55 minutes of polling per run;
- Silver/Gold stream hourly, about 50 minutes with a 1-minute trigger, stopping between batches so two runs never
  share a checkpoint;
- station snapshot daily at 06:00 Chicago;
- SCD2 + data-quality gate daily at 06:30 Chicago.

### Measured runtimes (prod smoke run as the service principal, 2026-11-13)

| Step | Time |
|---|---:|
| `nb_bronze_station_information` (snapshot) | 40 s |
| `nb_gbfs_poller`, 3 minutes of polling | 182 s |
| `nb_silver_availability`, `availableNow` | 162 s |
| `nb_gold_dim_station` (SCD2 + data-quality gate + re-key) | 142 s |
| Whole stage deploy (push, smoke run, model refresh, Activator) | about 14 min |

Latency per hop, from KQL `latency_by_hop()` in dev:

- poll → ingest: p50 1.1–1.4 s, p95 2–20 s;
- feed → poll: p50 about 48 s.

Prod p50 / p95: **(7-day run)**.

### What changes at 10× (about 20k stations or a 6-second feed)

| Pressure | Change |
|---|---|
| One poller can't send 20k events per poll in time | Shard the poller by `region_id` (one schedule per shard). Downstream is unchanged because the keys are per station. |
| Bronze flush (≤ 120 s) and Silver micro-batches grow | Prod pool to larger nodes. Partition Bronze by `polled_date`. `OPTIMIZE` daily. |
| KQL ingestion and materialized views | Eventhouse scales by itself. Shorten hot cache and keep 30-day retention. |
| Event volume costs more than it's worth | Switch to changes-only push with the poller's state in a lakehouse file ([ADR 0003](adr/0003-push-full-snapshots-dedupe-downstream.md)'s rejected option) |

## 2. Cost

### Capacity

Everything runs on a Fabric **trial capacity (FTL64)**, which ends about 2026-12-11. There is no Azure subscription,
so the platform uses no Azure Function, Key Vault or Event Hubs namespace of its own.

### What uses capacity units

| Workload | When | Note |
|---|---|---|
| Poller (Python notebook) | 24/7 during the run | Small, but always on: it sleeps between polls |
| Silver/Gold stream (Spark) | 24/7 during the run | The largest Spark consumer: one session per hour |
| Eventstream + Eventhouse ingestion, materialized views | 24/7 during the run | Scales with events: one more reason to measure the deduplication ratio |
| Daily snapshot + SCD2 | 2 short runs a day | Negligible |
| Deploy smoke run | Each deploy, 3 stages | About 14 minutes per stage |
| Dashboard queries + Activator | Every 5 min (Activator), plus viewers | Small |

Measured CU use from the Capacity Metrics app: **(7-day run)**.

### Cost controls

- Only prod runs continuously, and only for 7 days. The schedules have an end date, so they stop on their own.
- dev and test never run on a schedule: they run a bounded poller and `availableNow` streams.
- After the run: the monitor is switched off and the Eventstream stopped. The Eventhouse keeps its 30 days of data,
  with no new ingestion.

## 3. Governance

### Ownership and layers

| Layer | Store | Contents | Contract |
|---|---|---|---|
| Hot | `kqldb_bikes` | `station_status_raw` → `station_status` (update policy), dedup view, latest view, functions | Schema and policies in `kql/schema.kql`, applied by CI to every stage |
| Bronze | `lh_bronze` | `station_status_events` (as sent, plus Eventstream timestamps), `station_information` daily snapshots, wheels, run records | Event envelope with `schema_version`; snapshot replaced per Chicago date only |
| Silver | `lh_silver` | `station_availability` (one row per state), `availability_deadletter` with reason codes | Validation in `divvy_rti.availability`; Bronze = Silver + dead-letter checked by the data-quality gate |
| Gold | `lh_gold` | `fact_availability_15min`, `dim_station` (SCD2), `dq_results`, shortcuts `fact_trips` / `dim_date` | SCD2 invariants and dead-letter rate ≤ 1 % gate every daily run |
| Serving | workspace items | `rtd_bike_availability`, `sm_bike_availability` (Direct Lake), `act_bike_alerts` | Git definitions deployed by CI ([ADR 0006](adr/0006-ownership-terraform-vs-rest-deploy.md)) |

### Lineage

- Every event carries `poll_id`, `polled_at` and `feed_last_updated`. Bronze adds the Eventstream enqueue and
  processing times, so any Gold window can be traced to the polls that built it.
- Dead-letter rows keep a SHA-256 of the Bronze row and a reason code.
- Every notebook writes a JSON run record (inputs, counts, timings, traceback on failure) to
  `<lakehouse>/Files/runs/<notebook>/<date>/<run_id>.json`.
- Medallion `fact_trips` is read in place through a OneLake shortcut, never copied
  ([ADR 0007](adr/0007-station-key-bridge-short-name.md)).

### Retention

| Data | Current | Policy |
|---|---|---|
| KQL tables and views | 30 days, 7 days hot cache | Enough for the dashboard and alerts. History lives in Delta. |
| Bronze / Silver / Gold Delta | Kept; default Delta history | Add `OPTIMIZE` + `VACUUM` if the run is extended beyond a week |
| Streaming checkpoints | `Files/checkpoints/…_v2` | Kept with the tables; deleting one means a rebuild from Bronze |
| Run records, `dq_results` | Append-only | Kept: they are the audit trail |

GBFS is a public feed with no personal data. The only personal value in the platform is the alert recipient, which is
kept out of Git (next section).

## 4. Security

### Identities

| Identity | Used by | Access |
|---|---|---|
| Entra service principal `sp-fabric-medallion-cicd` | GitHub Actions through **OIDC federation** (no client secret) | Admin on the three workspaces (it creates them through Terraform). Deploys every item, **owns the prod schedules**, so scheduled jobs run as it. |
| Human administrator | Portal work, approvals | Admin on every workspace |
| Notebooks at run time | Eventstream custom endpoint, Fabric API | `notebookutils` tokens for the identity that runs the job. No keys or connection strings anywhere. |
| Dashboard viewers | KQL external table over `lh_gold.dim_station` | `;impersonate`: the viewer's own OneLake access applies |

The federated credentials are scoped to this repository's `main` branch, pull requests and the `platform` / `dev` /
`test` / `prod` environments, using GitHub's immutable owner and repository IDs.

There are two stored secrets:

- `TF_API_TOKEN` for the HCP Terraform state;
- `ALERT_EMAIL` for the Activator recipient. The repo is public, so the definition holds `{{Env:ALERT_EMAIL}}`, which
  is filled only at deploy time.

### Who can change what

| Change | Gate |
|---|---|
| Anything on `main` | Pull request, green `ci` (ruff, pytest, item validator, wheel, actionlint), squash merge |
| Platform (Terraform apply) | GitHub environment `platform`: required reviewer |
| Prod items and schedules | GitHub environment `prod`: required reviewer, after dev and test deployed and smoke-ran green |
| Prod data | Only the scheduled notebooks and the smoke run write it. Lakehouses and the Eventhouse have `prevent_destroy`. |

### Hardening for a real tenant

- One service principal per stage with **Contributor**, and a separate platform identity for workspace creation.
- Key Vault-backed secrets once an Azure subscription exists. The alert recipient becomes a distribution list or a
  Teams channel.
- Viewers get **Viewer** on Gold and the dashboard only, never on Bronze/Silver or the KQL raw table.
- Outbound access protection: the only external call is the public GBFS HTTPS feed.
