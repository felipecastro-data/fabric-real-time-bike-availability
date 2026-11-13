# ADR 0002 — One Eventstream feeds two sinks: Eventhouse (hot) and Delta (warm)

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Two audiences read the same station events:

- **Operations** wants the network as it is right now: a live dashboard and an alert when a busy station runs empty.
  Seconds matter.
- **Analysts** want clean history: deduplicated availability, 15-minute facts keyed to the station version, and a
  join to the batch `fact_trips` from the medallion project. Correctness and replays matter more than seconds.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Eventhouse only** (KQL), with Delta exposed from it | One copy; KQL serves the dashboard and Activator | The warm path needs an append-only Delta source with Spark checkpoints, a dead-letter and exactly-once MERGEs. A Delta copy written on KQL's schedule doesn't give that, and Direct Lake + medallion joins live on the lakehouse side. |
| B | **Delta only** (lakehouse) | One copy; Spark does everything | The dashboard and alerts would trail by the Bronze flush (≤ 120 s) plus a Spark micro-batch. That means minutes, not seconds. |
| C | **Both, from one Eventstream** | Each path uses the engine built for it, from the same events | Two copies of the data, and two schemas to keep in step |

## Decision

**Option C.** `es_station_status` has one custom-endpoint source and two destinations:

- **Hot:** `kqldb_bikes.station_status_raw`. An update policy computes the fill rate and the empty / full / offline /
  stale flags. Materialized views keep the deduplicated state and the latest row per station. Retention is 30 days,
  with 7 days in hot cache. It serves the Real-Time Dashboard and Activator.
- **Warm:** `lh_bronze.dbo.station_status_events` (Delta, flushed within 120 s). Spark Structured Streaming writes Silver
  `station_availability` and the dead-letter, then Gold `fact_availability_15min`. The Direct Lake model reads Gold.

The stale rule (`last_reported` more than one hour old) is the same on both sides: in the KQL update policy and in
`divvy_rti.availability`.

## Consequences

- Poll to queryable in KQL: p50 about 1 s, p95 2–20 s in dev. The prod 7-day figures are reported in `PLAN.md`
  after the run.
- Storage is doubled, but the KQL copy expires after 30 days, while Delta keeps the history.
- The two paths are reconciled by data-quality checks: Bronze rows = Silver + dead-letter, and the KQL dedup view
  keys on the same `(station_id, last_reported)` as Silver.
- A schema change touches `kql/schema.kql` and the Spark schema together. A unit test keeps the KQL external table
  `dim_station` equal to the Spark `DIM_SCHEMA`.
