"""CDC / SCD2: daily Bronze `station_information` snapshots → `lh_gold` `dim_station`, dated by the feed.

Each snapshot is compared with the current versions through a hash of the type 2 attributes (name, short_name,
capacity) plus a relocation test (> 100 m). Changes become one Delta MERGE: close the old version, open a new one from
the snapshot's GBFS `last_updated` (not load time); type 1 attributes (small coordinate drift, region, address,
charging) are updated in place; stations missing from a snapshot are closed with `end_reason = 'removed'`.

Keys: `station_key` is durable (hash of GBFS `station_id`); `station_version_key` = hash(station_id, valid_from);
`trips_station_key` = the medallion `station_key` of the trip side (hash of `short_name`, the trips' station id), so
`fact_trips.start_station_key` joins without a lookup table. Lightweight stations have no `short_name` → null.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

OPEN_END = datetime(9999, 12, 31, tzinfo=UTC)
RELOCATION_M = 100.0
TYPE2 = ("name", "short_name", "capacity")
TYPE1 = ("lat", "lon", "region_id", "address", "is_charging")
_NON_NEGATIVE = 0x7FFFFFFFFFFFFFFF  # same masking as the medallion repo's station_key()

DIM_SCHEMA = T.StructType(
    [
        T.StructField("station_version_key", T.LongType(), False),
        T.StructField("station_key", T.LongType(), False),
        T.StructField("station_id", T.StringType(), False),
        T.StructField("trips_station_key", T.LongType()),
        T.StructField("name", T.StringType()),
        T.StructField("short_name", T.StringType()),
        T.StructField("station_type", T.StringType()),
        T.StructField("capacity", T.LongType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("region_id", T.StringType()),
        T.StructField("address", T.StringType()),
        T.StructField("is_charging", T.BooleanType()),
        T.StructField("type2_hash", T.StringType()),
        T.StructField("valid_from", T.TimestampType(), False),
        T.StructField("valid_to", T.TimestampType(), False),
        T.StructField("is_current", T.BooleanType(), False),
        T.StructField("change_reason", T.StringType()),  # why this version opened: initial, new, renamed+capacity, …
        T.StructField("end_reason", T.StringType()),  # why it closed: changed, removed
        T.StructField("snapshot_date", T.DateType()),  # Bronze snapshot that opened it (lineage)
    ]
)
DIM_COLUMNS = [f.name for f in DIM_SCHEMA.fields]


def hash_key(*columns: Column) -> Column:
    return F.xxhash64(*columns).bitwiseAND(F.lit(_NON_NEGATIVE))


def haversine_m(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return F.lit(2 * 6_371_000.0) * F.asin(F.sqrt(a))


def members(snapshot: DataFrame) -> DataFrame:
    """Bronze snapshot rows → dimension attributes (no validity columns yet)."""
    short_name = F.when(F.trim("short_name") != "", F.col("short_name"))
    return snapshot.select(
        hash_key(F.col("station_id")).alias("station_key"),
        "station_id",
        F.when(short_name.isNotNull(), hash_key(short_name)).alias("trips_station_key"),
        "name",
        short_name.alias("short_name"),
        F.when(short_name.isNotNull(), "classic").otherwise("lightweight").alias("station_type"),
        F.col("capacity").cast("long").alias("capacity"),
        *TYPE1,
        F.sha2(F.to_json(F.struct(*TYPE2)), 256).alias("type2_hash"),  # to_json keeps null ≠ ""
        "snapshot_date",
    )


def _open(rows: DataFrame, effective_at: datetime, reason: Column) -> DataFrame:
    return rows.select(
        hash_key(F.col("station_id"), F.lit(effective_at)).alias("station_version_key"),
        *[c for c in DIM_COLUMNS if c in rows.columns and c != "station_version_key"],
        F.lit(effective_at).alias("valid_from"),
        F.lit(OPEN_END).alias("valid_to"),
        F.lit(True).alias("is_current"),
        reason.alias("change_reason"),
        F.lit(None).cast("string").alias("end_reason"),
    ).select(*DIM_COLUMNS)


def scd2_changes(dim: DataFrame, snapshot: DataFrame, effective_at: datetime) -> DataFrame:
    """Staged MERGE rows: `DIM_COLUMNS` + `action` (insert | close | type1) + `merge_key` (version key to update,
    null for inserts)."""
    cur = dim.where("is_current").alias("c")
    snap = members(snapshot).alias("s")
    joined = snap.join(cur, F.col("s.station_key") == F.col("c.station_key"), "full_outer")
    present, existing = F.col("s.station_key").isNotNull(), F.col("c.station_key").isNotNull()
    relocated = F.coalesce(
        haversine_m(F.col("c.lat"), F.col("c.lon"), F.col("s.lat"), F.col("s.lon")) > RELOCATION_M, F.lit(False)
    )
    type2 = (F.col("s.type2_hash") != F.col("c.type2_hash")) | relocated
    type1 = F.lit(False)
    for column in TYPE1:
        type1 = type1 | ~F.col(f"s.{column}").eqNullSafe(F.col(f"c.{column}"))
    reason = F.concat_ws(
        "+",
        *[F.when(~F.col(f"s.{c}").eqNullSafe(F.col(f"c.{c}")), F.lit("renamed" if c == "name" else c)) for c in TYPE2],
        F.when(relocated, F.lit("relocated")),
    )
    change = (
        F.when(present & ~existing, "new")
        .when(~present & existing, "removed")
        .when(present & existing & type2, "changed")
        .when(present & existing & type1, "type1")
    )
    classified = joined.select(
        F.struct("s.*").alias("s"), F.struct("c.*").alias("c"), change.alias("change"), reason.alias("reason")
    ).where(F.col("change").isNotNull())
    first_load = dim.limit(1).count() == 0

    def snap_rows(kind: str) -> DataFrame:
        return classified.where(F.col("change") == kind).select("s.*", "reason")

    inserts = _open(snap_rows("new"), effective_at, F.lit("initial" if first_load else "new")).unionByName(
        _open(snap_rows("changed"), effective_at, F.col("reason"))
    )
    closes = (
        classified.where(F.col("change").isin("changed", "removed"))
        .select("c.*", F.when(F.col("change") == "removed", "removed").otherwise("changed").alias("_end"))
        .withColumn("valid_to", F.lit(effective_at))
        .withColumn("is_current", F.lit(False))
        .withColumn("end_reason", F.col("_end"))
        .select(*DIM_COLUMNS)
    )
    type1_rows = classified.where(F.col("change") == "type1").select(
        *[F.col(f"s.{c}" if c in TYPE1 else f"c.{c}").alias(c) for c in DIM_COLUMNS]
    )

    def staged(rows: DataFrame, action: str, merge_key: Column) -> DataFrame:
        return rows.withColumn("action", F.lit(action)).withColumn("merge_key", merge_key)

    version = F.col("station_version_key")
    return (
        staged(inserts, "insert", F.lit(None).cast("long"))
        .unionByName(staged(closes, "close", version))
        .unionByName(staged(type1_rows, "type1", version))
    )


def merge_changes(spark: SparkSession, dim_path: str, staged: DataFrame) -> None:
    """One MERGE per snapshot: close + type 1 updates on the version key, inserts for new versions."""
    from delta.tables import DeltaTable

    (
        DeltaTable.forPath(spark, dim_path)
        .alias("t")
        .merge(staged.alias("s"), "t.station_version_key = s.merge_key")
        .whenMatchedUpdate(
            condition="s.action = 'close'",
            set={"valid_to": "s.valid_to", "is_current": "false", "end_reason": "s.end_reason"},
        )
        .whenMatchedUpdate(condition="s.action = 'type1'", set={c: f"s.{c}" for c in TYPE1})
        .whenNotMatchedInsert(condition="s.action = 'insert'", values={c: f"s.{c}" for c in DIM_COLUMNS})
        .execute()
    )


def applied_until(dim: DataFrame) -> datetime | None:
    """Feed time of the last applied snapshot: the latest valid_from or (non-open) valid_to in the dimension."""
    closed = F.when(F.col("valid_to") < F.lit(OPEN_END), F.col("valid_to"))
    return dim.agg(F.greatest(F.max("valid_from"), F.max(closed))).first()[0]


def apply_snapshots(spark: SparkSession, bronze_info_path: str, dim_path: str) -> list[dict]:
    """Apply every Bronze snapshot newer than the dimension, oldest first. Rerunning is a no-op; an older snapshot
    is never applied after a newer one (history only moves forward)."""
    from delta.tables import DeltaTable

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if not DeltaTable.isDeltaTable(spark, dim_path):  # createIfNotExists fails on existing Fabric paths
        DeltaTable.create(spark).location(dim_path).addColumns(DIM_SCHEMA).execute()
    bronze = spark.read.format("delta").load(bronze_info_path)
    snapshots = bronze.groupBy("snapshot_date").agg(F.max("feed_last_updated").alias("effective_at"))
    last = applied_until(spark.read.format("delta").load(dim_path))
    if last is not None:
        snapshots = snapshots.where(F.col("effective_at") > F.lit(last))
    applied = []
    for snapshot_date, effective_at in sorted(snapshots.collect(), key=lambda r: r.effective_at):
        dim = spark.read.format("delta").load(dim_path)
        staged = scd2_changes(dim, bronze.where(F.col("snapshot_date") == F.lit(snapshot_date)), effective_at)
        staged = staged.persist()
        counts = {r.action: r["count"] for r in staged.groupBy("action").count().collect()}
        merge_changes(spark, dim_path, staged)
        staged.unpersist()
        applied.append({"snapshot_date": snapshot_date.isoformat(), "effective_at": effective_at.isoformat(), **counts})
    return applied
