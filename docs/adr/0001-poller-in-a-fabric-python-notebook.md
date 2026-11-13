# ADR 0001 — The GBFS poller runs in a Fabric Python notebook

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Divvy publishes station status as a GBFS v2.3 feed (`station_status.json`, about 763 KB, 2,079 stations, `ttl = 60`).
Nothing pushes it; something has to poll it every minute and send one event per station to the Eventstream.

The poller must:

- poll every 60 s, skip a feed that hasn't changed (ETag / `last_updated`), and retry with backoff;
- turn one feed document into 2,079 typed events, and reject malformed stations with a reason code;
- authenticate to the Eventstream custom endpoint with Entra ID, with no connection string or key;
- be unit-tested in this repo and deployed by the same CI as everything else.

The tenant has **no Azure subscription**, only a Fabric trial capacity.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Azure Function** (timer trigger) | Always on, no hourly restart, cheap per execution | Needs an Azure subscription (none), and a second deploy and identity outside Fabric |
| B | **Eventstream's own HTTP polling source** | No code to run | The feed is one 763 KB document. Per-station events, validation and reason codes would have to be rebuilt in Eventstream operators or KQL, outside the tested Python package. There is no unchanged-feed skip. |
| C | **Fabric Python notebook** (non-Spark), scheduled hourly, loops about 55 min | Stays inside Fabric and CI; logic lives in the `divvy_rti` wheel and is tested on recorded fixtures | Restarts every hour (a gap of one to a few polls), and it is a notebook billed while it sleeps between polls |

## Decision

**Option C.**

- `divvy_rti.gbfs` is the client (stdlib only, conditional GET, retries). `divvy_rti.stream` holds the loop logic
  (batches ≤ 64 KB; larger batches timed out on AMQP). Both are tested on fixtures of 20 stations.
- `nb_gbfs_poller` installs the wheel from `lh_bronze/Files/wheels/` and finds the custom endpoint by name at run
  time, so the same notebook works in every stage.
- Authentication: `notebookutils.credentials.getToken("https://eventhubs.azure.net")`, wrapped as an `azure-core`
  credential. This was proven in Spike A, and it works when the job runs as the CI service principal.
- In prod, a schedule starts it every hour from `infra/prod_schedules.json`. The schedule is owned by the service
  principal, so scheduled runs run as it.

## Consequences

- There is a short gap at every hourly restart. Job start-up took about 3 minutes in the service-principal runs. The
  monitor allows 15 minutes without a poll before it raises an issue.
- Overlapping runs, or polls that see the same feed twice, are harmless: downstream deduplicates on
  `(station_id, last_reported)` ([ADR 0003](0003-push-full-snapshots-dedupe-downstream.md)).
- Terraform can't schedule notebooks (the `fabric_item_job_scheduler` resource rejects Notebook items), so the
  schedule is created by the deploy workflow through the REST API ([ADR 0006](0006-ownership-terraform-vs-rest-deploy.md)).
- **Revisit when:** an Azure subscription is available, or the use case needs polling without hourly gaps. The client
  package moves to a Function unchanged.
