# ADR 0003 — Push every station on every poll; deduplicate downstream

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

Each feed refresh lists all 2,079 stations. Most of them haven't changed since the last poll: a station only
reports a new `last_reported` when its dock state changes, and some report times from months ago. Pushing everything
means up to about 3.0M events a day. In dev, 6,237 polled events held 3,569 distinct
`(station_id, last_reported)` states (57 %).

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Push only changed stations** (the poller remembers the last `last_reported` per station) | About half the events and ingestion cost | The poller needs state that survives its hourly restart, or it pushes everything after each restart anyway. An idle station sends nothing, so "no news" looks the same as "poller down". The 15-minute fact can't count minutes empty for a station it never hears from. |
| B | **Push the full snapshot each time the feed changes; deduplicate downstream** | Stateless poller, so restarts and overlaps are harmless. Every poll is an observation. Each consumer chooses its own grain. | More events, storage and ingestion |

## Decision

**Option B.**

- The poller skips only a feed that hasn't changed (`304 Not Modified` or the same `last_updated`). Otherwise it sends
  every station, tagged with `poll_id` and `polled_at`.
- **KQL:** the materialized view `station_status_dedup` keys on `(station_id, last_reported)`, and `station_latest`
  keeps the newest row per station.
- **Silver:** an insert-only MERGE on `(station_id, last_reported)`. The first poll that saw a state wins, so replays
  are safe.
- **Gold:** `fact_availability_15min` is built from **every valid poll**, not from deduplicated Silver.
  `dropDuplicatesWithinWatermark(station_id, poll_id)` removes only true duplicates. An idle station is still observed
  every minute, so `minutes_empty` / `minutes_full` are real counts of observations.

## Consequences

- Volume is driven by the poll rate, not by station activity. That is about 2,079 × 1,320 polls ≈ 2.7M events a day,
  after the hourly restart gaps.
- The raw-to-deduplicated ratio is a reported metric of the prod run (`PLAN.md`, Step 8 results).
- Liveness is easy to monitor: no new poll in KQL for 15 minutes means the poller or the Eventstream has stopped.
