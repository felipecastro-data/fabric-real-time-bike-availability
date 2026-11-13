# ADR 0006 — Terraform owns the containers; a REST deploy owns item definitions and schedules

- **Status:** Accepted (differs from medallion
  [ADR 0001](https://github.com/felipecastro-data/fabric-medallion-cicd-terraform/blob/main/docs/adr/0001-terraform-plus-fabric-deployment-pipelines.md),
  which promotes items with a Fabric deployment pipeline)
- **Date:** 2026-11-13

## Context

The Real-Time Intelligence items hold references to other items in the same workspace by ID:

- the Eventstream's destinations point at the KQL database and `lh_bronze`;
- the dashboard's data source points at the KQL database URI;
- the semantic model points at the `lh_gold` SQL endpoint;
- Activator points at the KQL database, the dashboard and an email recipient.

Fabric rejects cross-workspace and all-zeros IDs in these places. Git sync and the deployment pipeline copy a
definition as it is, so the dev IDs would reach test and prod.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Terraform for everything** (item definitions as resources) | One tool | The provider can't schedule notebooks (`fabric_item_job_scheduler` accepts only dataflow, datapipeline and lakehouse in 1.15.1). Definitions edited in the UI drift from the state. |
| B | **Git sync in dev + deployment pipeline to test/prod** (medallion approach) | Native Fabric promotion | Stage-specific IDs inside definitions don't survive it, and the pipeline doesn't cover them. |
| C | **Terraform for containers; one REST deploy script for definitions, run per stage with IDs filled in** | Same Git definitions in every stage; each stage proven by a smoke run | A custom script (`tools/fabric_ops.py`) to maintain |

## Decision

**Option C.**

- **Terraform** (`infra/terraform`, applied as the service principal behind the `platform` approval) creates, per
  stage: the workspace and roles, `lh_bronze` / `lh_silver` / `lh_gold` (`prevent_destroy`), `eh_bikes` +
  `kqldb_bikes`, an empty `es_station_status`, the shortcuts to medallion Gold, and the Spark settings.
- **`deploy.yml`** runs `tools/deploy_stage.sh` for dev → test → prod (prod behind a required reviewer), as the service
  principal. It uploads the wheel and pushes the Eventstream and notebooks. It applies `kql/schema.kql`, pushes the
  dashboard, smoke-runs the pipeline (snapshot, 3-minute poll, Silver/Gold, SCD2 + data-quality gate), refreshes the
  SQL endpoint, pushes and frames the model, and pushes Activator. `{{workspace}}`, `{{<Type>:<name>}}` and
  `{{Env:NAME}}` placeholders are filled from the target stage.
- **Prod schedules** live in `infra/prod_schedules.json` and are applied by `fabric_ops.py schedule` in the prod stage.
  The service principal owns them, so scheduled runs run as it.

## Consequences

- The first deploy as the service principal (2026-11-13) was green in all three stages, with no permission fixes
  needed: Power BI API, Kusto script and Activator all worked.
- A fresh stage is reproducible: `terraform apply`, then one `deploy` run.
- Every merge that touches deployable paths runs a smoke run in prod as well. Avoid such merges during the 7-day run.
- The deployment pipeline that Terraform created in Step 2 is no longer used to promote items. It is kept as a stage
  map and is a candidate for removal.
