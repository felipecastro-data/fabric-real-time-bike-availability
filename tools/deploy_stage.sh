#!/usr/bin/env bash
# Deploy every Git-managed item to one stage workspace, then prove it works with a bounded smoke run.
#   tools/deploy_stage.sh <workspace-id>        (ALERT_EMAIL must be set: Activator recipient, kept out of the repo)
# Order matters: the dashboard needs the KQL functions; the model needs lh_gold tables, which the smoke run creates
# on a fresh stage; the Activator reads the dashboard tile. Every step is idempotent, so a failed deploy is re-run.
set -euo pipefail
ws="$1"
ops() { uv run --no-project python tools/fabric_ops.py "$@"; }

ops upload-wheel "$ws"
ops push "$ws" fabric/es_station_status.Eventstream
for nb in nb_gbfs_poller nb_bronze_station_information nb_silver_availability nb_gold_dim_station; do
  ops push "$ws" "fabric/$nb.Notebook"
done
ops kql "$ws" kqldb_bikes kql/schema.kql
ops push "$ws" fabric/rtd_bike_availability.KQLDashboard

# Smoke run: snapshot → 3 min of polls → Silver/Gold (availableNow) → SCD2 + DQ gate (fails the deploy on a DQ error)
ops run "$ws" nb_bronze_station_information '{}'
ops run "$ws" nb_gbfs_poller '{"duration_minutes": "3"}'
sleep 150 # Eventstream → lh_bronze flush (≤ 120 s) before Silver reads Bronze
ops run "$ws" nb_silver_availability '{"duration_minutes": "0"}'
ops run "$ws" nb_gold_dim_station '{}'

ops refresh-sql "$ws" lh_gold
ops push "$ws" fabric/sm_bike_availability.SemanticModel
ops refresh-model "$ws" sm_bike_availability
ops push "$ws" fabric/act_bike_alerts.Reflex
