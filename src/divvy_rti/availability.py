"""Warm path: Bronze `station_status_events` → Silver `station_availability` (+ dead-letter) and Gold
`fact_availability_15min`, with Spark Structured Streaming over Delta. Used by the `nb_silver_availability` notebook.

Silver: one row per `(station_id, last_reported)`, the first poll that saw it. `foreachBatch` MERGEs insert-only on that
key, so replaying a micro-batch after a crash is a no-op (exactly-once with the checkpoint). Rows that break the event
contract go to `availability_deadletter` with a reason code, keyed by a hash of the Bronze row (also replay-safe).

Gold: 15-min windows per station from every valid poll (not the deduped Silver: an idle station is still observed
every minute). Watermark 10 min on `polled_at`, dedup of `(station_id, poll_id)` within the watermark, append mode;
finalized windows get the as-of `dim_station` version and are MERGEd insert-only (replay-safe).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SCHEMA_VERSION = 1
WATERMARK = "10 minutes"
LATE_AFTER_S = 600  # enqueued more than 10 min after the poll → dead-letter `late` (same bound as the watermark)
STALE_AFTER_S = 3600  # same rule as KQL `is_stale`: flagged, not dropped
FUTURE_TOLERANCE_S = 300  # station clock ahead of the poller by more than this → `future_report`
WINDOW = "15 minutes"

COUNTS = (
    "num_bikes_available",
    "num_docks_available",
    "num_ebikes_available",
    "num_bikes_disabled",
    "num_docks_disabled",
)
REQUIRED = ("poll_id", "station_id", "num_bikes_available", "num_docks_available", "is_installed", "is_renting")

# What Eventstream lands in lh_bronze dbo.station_status_events (read from dev 2026-11-12).
BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("poll_id", T.StringType()),
        T.StructField("polled_at", T.StringType()),
        T.StructField("feed_last_updated", T.StringType()),
        T.StructField("station_id", T.StringType()),
        *[T.StructField(c, T.LongType()) for c in COUNTS],
        T.StructField("is_installed", T.BooleanType()),
        T.StructField("is_renting", T.BooleanType()),
        T.StructField("is_returning", T.BooleanType()),
        T.StructField("last_reported", T.StringType()),
        T.StructField("schema_version", T.LongType()),
        T.StructField("EventProcessedUtcTime", T.TimestampType()),
        T.StructField("PartitionId", T.LongType()),
        T.StructField("EventEnqueuedUtcTime", T.TimestampType()),
    ]
)

SILVER_SCHEMA = T.StructType(
    [
        T.StructField("station_id", T.StringType(), False),
        T.StructField("last_reported", T.TimestampType(), False),
        T.StructField("report_date", T.DateType(), False),  # UTC date of last_reported; partition + MERGE pruning
        T.StructField("poll_id", T.StringType(), False),
        T.StructField("polled_at", T.TimestampType(), False),
        T.StructField("feed_last_updated", T.TimestampType()),
        *[T.StructField(c, T.IntegerType()) for c in COUNTS],
        T.StructField("is_installed", T.BooleanType()),
        T.StructField("is_renting", T.BooleanType()),
        T.StructField("is_returning", T.BooleanType()),
        T.StructField("fill_rate", T.DoubleType()),
        T.StructField("is_empty", T.BooleanType()),
        T.StructField("is_full", T.BooleanType()),
        T.StructField("is_offline", T.BooleanType()),
        T.StructField("report_age_s", T.LongType()),
        T.StructField("is_stale", T.BooleanType()),
        T.StructField("enqueued_at", T.TimestampType()),
        T.StructField("silver_batch_id", T.LongType()),
        T.StructField("processed_at", T.TimestampType()),
    ]
)

DEADLETTER_SCHEMA = T.StructType(
    [
        T.StructField("row_hash", T.StringType(), False),
        T.StructField("reject_reason", T.StringType(), False),
        T.StructField("station_id", T.StringType()),
        T.StructField("poll_id", T.StringType()),
        T.StructField("raw", T.StringType()),  # the Bronze row as JSON
        T.StructField("enqueued_at", T.TimestampType()),
        T.StructField("silver_batch_id", T.LongType()),
        T.StructField("processed_at", T.TimestampType()),
    ]
)

GOLD_SCHEMA = T.StructType(
    [
        T.StructField("station_id", T.StringType(), False),
        T.StructField("window_start", T.TimestampType(), False),
        T.StructField("window_end", T.TimestampType(), False),
        T.StructField("polls", T.LongType()),
        T.StructField("avg_fill_rate", T.DoubleType()),
        T.StructField("min_bikes_available", T.IntegerType()),
        T.StructField("max_bikes_available", T.IntegerType()),
        T.StructField("minutes_empty", T.LongType()),  # polls are 60 s apart: one poll ≈ one minute
        T.StructField("minutes_full", T.LongType()),
        T.StructField("minutes_offline", T.LongType()),
        T.StructField("station_version_key", T.LongType()),  # dim_station version valid at window_start (as-of)
        T.StructField("date_key", T.IntegerType()),  # Chicago date of window_start, yyyyMMdd (medallion dim_date)
    ]
)


def _seconds(later: Column, earlier: Column) -> Column:
    return later.cast("long") - earlier.cast("long")


def validate(bronze: DataFrame) -> DataFrame:
    """Typed columns, derived flags and `reject_reason` (null = valid; first failing rule wins). Stateless and
    deterministic, so a replayed row always gets the same verdict."""
    ts = {c: F.to_timestamp(c) for c in ("polled_at", "feed_last_updated", "last_reported")}
    bikes, docks = F.col("num_bikes_available"), F.col("num_docks_available")
    missing = F.lit(False)
    for name in REQUIRED:
        missing = missing | F.col(name).isNull()
    negative = F.lit(False)
    for name in COUNTS:
        negative = negative | (F.col(name) < 0)
    reason = (
        F.when(F.col("schema_version").isNull() | (F.col("schema_version") != SCHEMA_VERSION), "unsupported_schema")
        .when(missing, "missing_field")
        .when(ts["polled_at"].isNull() | ts["last_reported"].isNull(), "bad_timestamp")
        .when(negative, "negative_count")
        .when(F.col("num_ebikes_available") > bikes, "impossible_count")
        .when(_seconds(ts["last_reported"], ts["polled_at"]) > FUTURE_TOLERANCE_S, "future_report")
        .when(_seconds(F.col("EventEnqueuedUtcTime"), ts["polled_at"]) > LATE_AFTER_S, "late")
    )
    usable = bikes + docks
    report_age_s = _seconds(ts["polled_at"], ts["last_reported"])
    return bronze.select(
        F.sha2(F.to_json(F.struct(*[F.col(f.name) for f in BRONZE_SCHEMA.fields])), 256).alias("row_hash"),
        F.to_json(F.struct(*[F.col(f.name) for f in BRONZE_SCHEMA.fields])).alias("raw"),
        reason.alias("reject_reason"),
        F.col("station_id"),
        ts["last_reported"].alias("last_reported"),
        F.to_date(ts["last_reported"]).alias("report_date"),
        F.col("poll_id"),
        ts["polled_at"].alias("polled_at"),
        ts["feed_last_updated"].alias("feed_last_updated"),
        *[F.col(c).cast("int").alias(c) for c in COUNTS],
        F.col("is_installed"),
        F.col("is_renting"),
        F.col("is_returning"),
        F.when(usable > 0, bikes / usable).alias("fill_rate"),
        (bikes == 0).alias("is_empty"),
        (docks == 0).alias("is_full"),
        (~F.col("is_installed") | ~F.col("is_renting")).alias("is_offline"),
        report_age_s.alias("report_age_s"),
        (report_age_s > STALE_AFTER_S).alias("is_stale"),
        F.col("EventEnqueuedUtcTime").alias("enqueued_at"),
    )


def ensure_table(spark: SparkSession, path: str, schema: T.StructType, partition_by: tuple[str, ...] = ()) -> None:
    from delta.tables import DeltaTable

    # Check first: on Fabric, createIfNotExists over an existing OneLake path fails with SCHEMA_NOT_FOUND (2026-11-13).
    if DeltaTable.isDeltaTable(spark, path):
        return
    builder = DeltaTable.create(spark).location(path).addColumns(schema)
    if partition_by:
        builder = builder.partitionedBy(*partition_by)
    builder.execute()


def write_silver_batch(batch: DataFrame, batch_id: int, silver_path: str, deadletter_path: str) -> None:
    """`foreachBatch` body: insert-only MERGEs, so running the same batch twice changes nothing."""
    from delta.tables import DeltaTable

    spark = batch.sparkSession
    checked = (
        validate(batch).withColumn("silver_batch_id", F.lit(batch_id)).withColumn("processed_at", F.current_timestamp())
    )
    checked.persist()
    try:
        first_seen = Window.partitionBy("station_id", "last_reported").orderBy("polled_at", "poll_id")
        valid = (
            checked.where(F.col("reject_reason").isNull())
            .withColumn("_n", F.row_number().over(first_seen))
            .where("_n = 1")
            .select(*[f.name for f in SILVER_SCHEMA.fields])
        )
        dates = sorted(r[0].isoformat() for r in valid.select("report_date").distinct().collect())
        if dates:  # literal partition list → MERGE reads only the partitions this batch can match
            pruning = ", ".join(f"DATE'{d}'" for d in dates)
            (
                DeltaTable.forPath(spark, silver_path)
                .alias("t")
                .merge(
                    valid.alias("s"),
                    f"t.report_date IN ({pruning}) AND t.station_id = s.station_id"
                    " AND t.last_reported = s.last_reported",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        rejected = (
            checked.where(F.col("reject_reason").isNotNull())
            .dropDuplicates(["row_hash"])
            .select(*[f.name for f in DEADLETTER_SCHEMA.fields])
        )
        if not rejected.isEmpty():
            (
                DeltaTable.forPath(spark, deadletter_path)
                .alias("t")
                .merge(rejected.alias("s"), "t.row_hash = s.row_hash")
                .whenNotMatchedInsertAll()
                .execute()
            )
    finally:
        checked.unpersist()


def gold_windows(bronze_stream: DataFrame) -> DataFrame:
    """15-min availability per station from every valid poll; append mode emits a window once the watermark passes."""
    valid = validate(bronze_stream).where(F.col("reject_reason").isNull())
    window = F.window("polled_at", WINDOW)
    return (
        valid.withWatermark("polled_at", WATERMARK)
        .dropDuplicatesWithinWatermark(["station_id", "poll_id"])  # replays and overlapping poller runs
        .groupBy(F.col("station_id"), window)
        .agg(
            F.count(F.lit(1)).alias("polls"),
            F.avg("fill_rate").alias("avg_fill_rate"),
            F.min("num_bikes_available").alias("min_bikes_available"),
            F.max("num_bikes_available").alias("max_bikes_available"),
            F.sum(F.col("is_empty").cast("long")).alias("minutes_empty"),
            F.sum(F.col("is_full").cast("long")).alias("minutes_full"),
            F.sum(F.col("is_offline").cast("long")).alias("minutes_offline"),
        )
        .select(
            "station_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "polls",
            "avg_fill_rate",
            "min_bikes_available",
            "max_bikes_available",
            "minutes_empty",
            "minutes_full",
            "minutes_offline",
        )
    )


def write_gold_batch(batch: DataFrame, batch_id: int, gold_path: str, dim_path: str) -> None:
    """`foreachBatch` body for the finalized windows: as-of station version + date key, insert-only MERGE on
    `(station_id, window_start)` → a replayed batch changes nothing."""
    from delta.tables import DeltaTable

    from divvy_rti.dim_station import as_of_version_key

    spark = batch.sparkSession
    dim = spark.read.format("delta").load(dim_path) if DeltaTable.isDeltaTable(spark, dim_path) else None
    local_date = F.from_utc_timestamp("window_start", "America/Chicago")
    rows = as_of_version_key(batch, dim).withColumn("date_key", F.date_format(local_date, "yyyyMMdd").cast("int"))
    (
        DeltaTable.forPath(spark, gold_path)
        .alias("t")
        .merge(
            rows.select(*[f.name for f in GOLD_SCHEMA.fields]).alias("s"),
            "t.station_id = s.station_id AND t.window_start = s.window_start",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


@dataclass(frozen=True)
class Paths:
    bronze: str  # Bronze station_status_events table
    silver: str
    deadletter: str
    gold: str
    silver_checkpoint: str
    gold_checkpoint: str
    dim_station: str  # lh_gold dim_station (SCD2), for the as-of station version of each window


def run(spark: SparkSession, paths: Paths, duration_s: float = 0, processing_time: str = "1 minute") -> dict:
    """Run both streams. `duration_s == 0` → `availableNow` (process what is in Bronze, then stop: dev/test/CI);
    otherwise trigger every `processing_time` for `duration_s`, then stop between batches (prod, hourly)."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # report_date and windows in UTC
    ensure_table(spark, paths.silver, SILVER_SCHEMA, ("report_date",))
    ensure_table(spark, paths.deadletter, DEADLETTER_SCHEMA)
    ensure_table(spark, paths.gold, GOLD_SCHEMA)

    def bronze() -> DataFrame:
        return spark.readStream.format("delta").load(paths.bronze)

    def trigger(writer):
        return writer.trigger(availableNow=True) if duration_s == 0 else writer.trigger(processingTime=processing_time)

    silver = trigger(
        bronze()
        .writeStream.queryName("silver_station_availability")
        .foreachBatch(lambda df, batch_id: write_silver_batch(df, batch_id, paths.silver, paths.deadletter))
        .option("checkpointLocation", paths.silver_checkpoint)
    ).start()
    gold = trigger(
        gold_windows(bronze())
        .writeStream.queryName("gold_fact_availability_15min")
        .outputMode("append")
        .foreachBatch(lambda df, batch_id: write_gold_batch(df, batch_id, paths.gold, paths.dim_station))
        .option("checkpointLocation", paths.gold_checkpoint)
    ).start()

    queries = (silver, gold)
    if duration_s == 0:
        for query in queries:
            query.awaitTermination()
    else:
        deadline = time.monotonic() + duration_s
        while time.monotonic() < deadline and all(q.isActive for q in queries):
            time.sleep(5)
        for query in queries:  # stop between batches; a stop mid-batch is also safe (replay is idempotent)
            while query.isActive and query.status["isTriggerActive"]:
                time.sleep(1)
            query.stop()
    for query in queries:
        if query.exception():
            raise query.exception()
    return {q.name: (q.lastProgress or {}).get("batchId") for q in queries}
