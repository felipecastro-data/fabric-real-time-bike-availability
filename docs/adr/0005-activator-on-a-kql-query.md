# ADR 0005 — Alerts come from Activator on a KQL query, not a rule on the event stream

- **Status:** Accepted
- **Date:** 2026-11-13

## Context

The first rule, "a station has under 10 % bikes", would send hundreds of emails a day: many small stations sit near
empty for hours. The rule chosen instead is **"busy station dropping"**: a reporting, in-service station whose 15-minute
average fill rate falls below 10 % after being at or above 30 % an hour earlier.

That rule compares two windows an hour apart, per station. A rule that looks at one event at a time can't express it.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Activator as an Eventstream destination** (rule per event) | Lowest latency | Sees one poll at a time: a single 0-bike reading fires the alert. Comparing against an hour-old 15-minute average needs state that the rule doesn't hold. |
| B | **Activator on a KQL query** (from a Real-Time Dashboard tile, run every 5 min) | The windows and the hour-ago comparison are plain KQL, tested in the same schema script. The alert fires on a per-station transition. | Up to 5 minutes of query delay on top of the 15-minute window. It's bound to a dashboard tile and its UI limits. |

## Decision

**Option B.**

- `near_empty_candidates()` in `kql/schema.kql` returns **every** in-service station that reported in the last
  15 minutes, with `alert_fill_rate` = the 15-minute fill rate if the station was at or above 30 % an hour ago, else
  1.0. Every station has a value on every run, so "becomes less than 0.1" is a clean transition per station.
- The tile "Alert candidates" is a column chart of the 100 lowest `alert_fill_rate` values. Activator `act_bike_alerts`
  queries it every 5 minutes, groups by `station_id`, and sends an email when the value becomes less than 0.1.
- The Activator definition is in Git (`fabric/act_bike_alerts.Reflex/`). Workspace and item IDs are placeholders, and
  the recipient is `{{Env:ALERT_EMAIL}}`, filled from a GitHub secret because the repo is public.

## Consequences

- Constraints found in the UI:
  - alerts can't be set on **table** tiles;
  - a chart allows **at most 100 series** (more gives "Max series types exceeded");
  - the condition fields stay disabled while the tile returns no rows.
- Only the 100 lowest values are evaluated. A station that crosses 0.1 is among the lowest, unless more than 100 cross
  at once.
- A station flapping around 0.1 can alert more than once.
- Whether alert emails fire in prod, and how many, is a result of the 7-day run (`PLAN.md`).
- **Revisit when:** Activator can run a KQL query directly without a dashboard tile, or the rule becomes a single-event
  condition.
