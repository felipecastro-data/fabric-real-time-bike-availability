# ADR 0004 — `dim_station` SCD2 dated by GBFS feed time, detected by hash-diff

- **Status:** Accepted (supersedes the load-time dating of medallion
  [ADR 0004](https://github.com/felipecastro-data/fabric-medallion-cicd-terraform/blob/main/docs/adr/0004-dim-station-scd2-by-snapshot-diff.md))
- **Date:** 2026-11-13

## Context

In the medallion project, a station version starts when the weekly pipeline *noticed* the change. That ADR's
"revisit when" says to date versions from the feed once a station feed is available. GBFS `station_information` is
that feed. It lists every station's name, `short_name`, capacity and location, and it carries `last_updated`.

The feed has no change log either, so changes must still be found by comparing snapshots. The question is what to
compare, and which time to use.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Load-time dating** (medallion approach) | Simple | A version's start depends on when the job ran. A replay or a late run gives different history. |
| B | **Compare column by column against the current version** | Explicit | Long predicates, easy to get wrong when columns are added. Every attribute change becomes a new version. |
| C | **Hash-diff of the tracked attributes, `valid_from` = feed `last_updated`** | One comparison per station. Dates come from the source, so reruns and replays give the same history. | Precision is the snapshot interval (daily). A change undone within a day is never seen. |

## Decision

**Option C**, in `divvy_rti.dim_station`, run daily by `nb_gold_dim_station`:

- A daily `station_information` snapshot (`lh_bronze.dbo.station_information`) is replaced per Chicago date
  (delete-day + append; Fabric's older delta-rs ignores an overwrite predicate).
- **Type 2** = SHA-256 over `name`, `short_name` and `capacity`, **or** a relocation of more than 100 m. The current
  version closes and a new one opens with `valid_from` = the snapshot's `last_updated`.
- **Type 1** (updated in place) = coordinate drift ≤ 100 m, `region_id`, `address`, `is_charging`.
- A station missing from a snapshot is closed with `end_reason = 'removed'`. History is never deleted.
- One Delta MERGE per snapshot. Pending snapshots are applied oldest first, a rerun is a no-op, and an older snapshot
  that lands late is never applied after a newer one.
- Keys: durable `station_key` = hash(`station_id`); `station_version_key` = hash(`station_id`, `valid_from`).
- After the load, a data-quality gate checks: unique version key, one current version per station, contiguous
  intervals, and open end ⇔ current. Results go to `lh_gold.dbo.dq_results`, and the run fails on an error.

## Consequences

- The Gold 15-minute fact stores the as-of `station_version_key`, assigned at write time. `rekey_facts` re-keys
  windows written before the day's snapshot was applied, with retries if a stream write happens at the same moment.
- Versions can be reproduced: rebuilding from Bronze snapshots gives the same `valid_from` values.
- A rename that is undone between two daily snapshots never shows up. That is acceptable for station metadata.
- Day-2 behaviour in Fabric (a real diff between two dev snapshots) is checked on 2026-11-14 and recorded in `PLAN.md`.
