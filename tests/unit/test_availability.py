from datetime import UTC, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")
delta = pytest.importorskip("delta")

from pyspark.sql import SparkSession  # noqa: E402

from divvy_rti.availability import BRONZE_SCHEMA, Paths, run, validate, write_silver_batch  # noqa: E402

T0 = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("test_availability")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def iso(moment):
    return moment.isoformat().replace("+00:00", "Z")


def row(station="s1", minute=0, reported_minute=None, bikes=5, docks=5, **overrides):
    polled = T0 + timedelta(minutes=minute)
    reported = T0 + timedelta(minutes=minute if reported_minute is None else reported_minute)
    base = {
        "poll_id": f"p{minute}",
        "polled_at": iso(polled),
        "feed_last_updated": iso(polled),
        "station_id": station,
        "num_bikes_available": bikes,
        "num_docks_available": docks,
        "num_ebikes_available": 0,
        "num_bikes_disabled": 0,
        "num_docks_disabled": 0,
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
        "last_reported": iso(reported),
        "schema_version": 1,
        "EventProcessedUtcTime": polled + timedelta(seconds=2),
        "PartitionId": 0,
        "EventEnqueuedUtcTime": polled + timedelta(seconds=1),
    }
    return base | overrides


def df(spark, rows):
    return spark.createDataFrame([tuple(r[f.name] for f in BRONZE_SCHEMA.fields) for r in rows], BRONZE_SCHEMA)


def test_validate_reason_codes(spark):
    cases = {
        "ok": row(),
        "unsupported_schema": row(schema_version=2),
        "missing_field": row(station_id=None),
        "bad_timestamp": row(last_reported="not-a-time"),
        "negative_count": row(num_docks_disabled=-1),
        "impossible_count": row(bikes=1, num_ebikes_available=3),
        "future_report": row(reported_minute=10),
        "late": row(EventEnqueuedUtcTime=T0 + timedelta(minutes=11)),
    }
    out = validate(df(spark, cases.values())).select("reject_reason").collect()
    assert [r.reject_reason or "ok" for r in out] == list(cases)


def test_validate_flags_stale_and_fill_rate(spark):
    stale = row(minute=120, reported_minute=0, bikes=0, docks=4)
    (out,) = validate(df(spark, [stale])).collect()
    assert out.reject_reason is None and out.is_stale and out.is_empty and out.fill_rate == 0.0
    (no_docks,) = validate(df(spark, [row(bikes=0, docks=0)])).collect()
    assert no_docks.fill_rate is None


def paths(tmp_path):
    return Paths(
        bronze=str(tmp_path / "bronze"),
        silver=str(tmp_path / "silver"),
        deadletter=str(tmp_path / "deadletter"),
        gold=str(tmp_path / "gold"),
        silver_checkpoint=str(tmp_path / "chk_silver"),
        gold_checkpoint=str(tmp_path / "chk_gold"),
    )


def polls(start, end, stations=("s1", "s2")):
    """One poll per minute; s1 reports a change every poll, s2 is idle (same last_reported, empty)."""
    out = []
    for minute in range(start, end):
        out.append(row("s1", minute, bikes=minute % 4, docks=4 - minute % 4))
        if "s2" in stations:
            out.append(row("s2", minute, reported_minute=0, bikes=0, docks=10))
    return out


def append_bronze(spark, p, rows):
    df(spark, rows).write.format("delta").mode("append").save(p.bronze)


def snapshot(spark, p):
    def rows(path, order):
        table = spark.read.format("delta").load(path)
        return sorted(tuple(r) for r in table.select(*[table[c].cast("string") for c in order]).collect())

    return (
        rows(p.silver, ["station_id", "last_reported", "poll_id"]),
        rows(p.deadletter, ["row_hash", "reject_reason"]),
        rows(p.gold, ["station_id", "window_start", "polls", "minutes_empty", "minutes_full"]),
    )


def test_end_to_end_dedup_deadletter_and_gold(spark, tmp_path):
    p = paths(tmp_path)
    bad = row("s3", 5, bikes=-1)
    append_bronze(spark, p, [*polls(0, 41), bad, bad])  # bad row twice (replayed send) → one dead-letter row

    run(spark, p)
    silver, deadletter, gold = snapshot(spark, p)

    assert len(silver) == 41 + 1  # s1 changes every poll; idle s2 only once
    assert [r[1] for r in deadletter] == ["negative_count"]
    # watermark = max polled_at (minute 40) - 10 min = 12:30 → windows ending by 12:30 are emitted, later ones wait
    assert {r[:2] for r in gold} == {(s, f"2026-11-13 12:{m}:00") for s in ("s1", "s2") for m in ("00", "15")}
    s2 = [r[2:] for r in gold if r[0] == "s2"]
    assert s2 == [("15", "15", "0")] * 2  # idle, empty station: 15 polls, 15 minutes empty per window


def test_restart_and_replay_are_exactly_once(spark, tmp_path):
    one_shot, resumed = paths(tmp_path / "one"), paths(tmp_path / "two")
    first, second = polls(0, 25), [*polls(25, 50), row("s3", 30, schema_version=9)]

    append_bronze(spark, one_shot, [*first, *second])
    run(spark, one_shot)

    # Same data delivered in two parts with a stop in between (resume from the checkpoints) ...
    append_bronze(spark, resumed, first)
    run(spark, resumed)
    append_bronze(spark, resumed, second)
    run(spark, resumed)
    # ... and a micro-batch replayed after a crash before its checkpoint commit: the MERGEs must be no-ops.
    write_silver_batch(df(spark, second), 1, resumed.silver, resumed.deadletter)

    assert snapshot(spark, resumed) == snapshot(spark, one_shot)
    silver, deadletter, gold = snapshot(spark, resumed)
    assert len(silver) == 50 + 1 and len(deadletter) == 1 and len(gold) == 2 * 2


def test_bounded_run_with_processing_time_trigger(spark, tmp_path):
    p = paths(tmp_path)
    append_bronze(spark, p, polls(0, 3))
    run(spark, p, duration_s=8, processing_time="2 seconds")
    silver, _, _ = snapshot(spark, p)
    assert len(silver) == 3 + 1
