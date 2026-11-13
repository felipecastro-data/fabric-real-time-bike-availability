# ADR 0007 — Stations join trips through GBFS `short_name`; 901 stations have none

- **Status:** Accepted
- **Date:** 2026-11-13

## Context

The model compares live availability with trip demand from the medallion `fact_trips`. The two sources name stations
differently:

- **GBFS** `station_id` is a 19-digit number. `short_name` looks like `CHI00307`.
- **Trips** `start_station_id` is `CHI00307`. That is the GBFS **`short_name`**, not the GBFS `station_id`.
- Of the 2,079 GBFS stations, 1,178 are `classic` and have a `short_name`. The other **901 `lightweight` stations have
  none**, so no key matches them to trips.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Join on station name** | Covers more stations | Names get renamed (which is why SCD2 exists) and are spelled differently across sources. Joins would silently go wrong. |
| B | **Nearest station by lat/lon** | Could reach lightweight stations | Ambiguous where stations sit close together. A coordinate in trips isn't a station identity. |
| C | **Shortcut to the medallion `dim_station`** and join through it | Reuses medallion keys | Two SCD2 dimensions for one station, plus a dependency on another repo's model. |
| D | **Compute the medallion key from `short_name`** (`trips_station_key = xxhash64(short_name) & 0x7FFFFFFFFFFFFFFF`, the medallion `station_key` formula) | Direct join to `fact_trips.start_station_key`; no extra shortcut | Only stations with a `short_name` can join |

## Decision

**Option D.**

- `dim_station.trips_station_key` is computed in `divvy_rti.dim_station`. It is null for lightweight stations.
  `station_type` is derived from it (`classic` if there is a `short_name`, else `lightweight`), because the feed has
  no such field.
- In the Direct Lake model, trips filter stations through `TREATAS` on `trips_station_key`, not through a relationship:
  several versions of one station share a trips key.

## Consequences

- Coverage, measured in dev on 2026-11-13:
  - **1,178 of 2,079** stations have a trips key;
  - **95.9 %** of September 2026 trips that start at a station start at a current GBFS station (575,754 / 600,121).
- The 901 lightweight stations are fully in the availability analysis (dashboard, alerts, 15-minute fact), but have no
  trip demand in the model.
- If Divvy gives lightweight stations a `short_name`, the next snapshot opens a new type 2 version (`short_name` is
  hashed) that has a trips key. No code change is needed.
