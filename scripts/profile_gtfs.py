"""Cross-check an HSL GTFS feed against an HFP capture (Phase 1.3).

Usage:
    python scripts/profile_gtfs.py <extracted_gtfs_dir> <hfp_capture.jsonl>

G1 feed coverage | G2 routes | G3 stops | G4 direction mapping
G5 trip_id structure | G6 trial mapping of HFP journeys to GTFS trips
"""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

gtfs_dir, hfp_path = sys.argv[1], sys.argv[2]

spark = (
    SparkSession.builder.master("local[*]")
    .appName("profile-gtfs")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def read_gtfs(name: str) -> DataFrame:
    """Read a GTFS file with every column as text (like raw_gtfs in Phase 5)."""
    df = spark.read.option("header", True).csv(f"{gtfs_dir}/{name}.txt")
    # Some files start with a UTF-8 BOM, which would end up inside the first column name.
    return df.toDF(*[c.lstrip("﻿").strip() for c in df.columns])


routes = read_gtfs("routes").cache()
stops = read_gtfs("stops").cache()
trips = read_gtfs("trips").cache()
calendar = read_gtfs("calendar")
calendar_dates = read_gtfs("calendar_dates")
feed_info = read_gtfs("feed_info")
stop_times = read_gtfs("stop_times")

# --- HFP side: deduplicated events from the capture ---
SCHEMA = MapType(StringType(), MapType(StringType(), StringType()))
raw = spark.read.json(hfp_path)
parsed = raw.select("topic", F.from_json("payload", SCHEMA).alias("p"))
f = F.map_values("p")[0]
hfp = (
    parsed.select(
        F.map_keys("p")[0].alias("event"),
        F.split("topic", "/")[11].alias("headsign"),
        *[f[c].alias(c) for c in ["oper", "veh", "tst", "route", "desi", "dir", "oday", "start", "stop"]],
    )
    .dropDuplicates(["event", "oper", "veh", "tst"])
    .cache()
)
print(f"HFP rows (deduped): {hfp.count()}")

# ---------------------------------------------------------------- G1
print("\n=== G1. Feed coverage ===")
feed_info.select("feed_start_date", "feed_end_date", "feed_version").show(truncate=False)
calendar.agg(
    F.min("start_date").alias("calendar_min_start"),
    F.max("end_date").alias("calendar_max_end"),
    F.count("*").alias("service_ids"),
).show()
calendar_dates.agg(F.min("date"), F.max("date"), F.count("*").alias("exceptions")).show()

# ---------------------------------------------------------------- G2
print("=== G2. Routes: HFP vs routes.txt ===")
hfp_routes = hfp.groupBy("route", "desi").agg(
    F.countDistinct("oper", "veh").alias("vehicles"), F.count("*").alias("rows")
)
missing = hfp_routes.join(routes, hfp_routes.route == routes.route_id, "left_anti")
print(f"HFP routes: {hfp_routes.count()} | missing from GTFS: {missing.count()}")
missing.orderBy("route").show(truncate=False)
print("Tram routes in GTFS (route_type 0) never seen in the HFP sample:")
(
    routes.filter("route_type = '0'")
    .join(hfp_routes, routes.route_id == hfp_routes.route, "left_anti")
    .select("route_id", "route_short_name", "route_long_name")
    .orderBy("route_id")
    .show(30, truncate=False)
)
print("desi vs route_short_name for matched routes (mismatches only):")
(
    hfp_routes.join(routes, hfp_routes.route == routes.route_id)
    .filter(F.col("desi") != F.col("route_short_name"))
    .select("route", "desi", "route_short_name")
    .show(truncate=False)
)

# ---------------------------------------------------------------- G3
print("=== G3. Stops: HFP ARR/DEP vs stops.txt ===")
hfp_stops = hfp.filter(F.col("event").isin("ARR", "DEP")).select("stop").distinct()
missing_stops = hfp_stops.join(stops, hfp_stops.stop == stops.stop_id, "left_anti")
print(f"HFP stops: {hfp_stops.count()} | missing from GTFS: {missing_stops.count()}")
missing_stops.show(truncate=False)
stops.agg(
    F.count("*").alias("stops"),
    F.sum((F.col("parent_station") == " ").cast("int")).alias("parent_is_one_space"),
    F.sum(F.col("parent_station").isNull().cast("int")).alias("parent_null"),
    F.sum((F.trim("parent_station") != "").cast("int")).alias("parent_real"),
    F.sum((F.col("zone_id").isNull() | (F.trim("zone_id") == "")).cast("int")).alias("zone_empty"),
).show()

# ---------------------------------------------------------------- G4
print("=== G4. Direction: HFP dir (1/2) vs GTFS direction_id (0/1), matched by headsign ===")
hfp_dirs = hfp.select("route", "dir", "headsign").distinct()
gtfs_dirs = trips.select("route_id", "direction_id", "trip_headsign").distinct()
dir_map = (
    hfp_dirs.join(
        gtfs_dirs,
        (hfp_dirs.route == gtfs_dirs.route_id) & (hfp_dirs.headsign == gtfs_dirs.trip_headsign),
    )
    .groupBy("dir", "direction_id")
    .agg(F.countDistinct("route", "headsign").alias("route_headsign_pairs"))
    .orderBy("dir", "direction_id")
)
dir_map.show()
print("    Expected if dir = direction_id + 1: only (1,0) and (2,1).")

# ---------------------------------------------------------------- G5
print("=== G5. trip_id structure and first departure ===")
# First stop of each trip. stop_sequence must be compared as a NUMBER ("10" < "9" as text).
first_stop = (
    stop_times.groupBy("trip_id")
    .agg(
        F.min(F.struct(F.col("stop_sequence").cast("int").alias("seq"), "departure_time", "stop_id"))
        .alias("first")
    )
    .select("trip_id", "first.departure_time", F.col("first.stop_id").alias("first_stop_id"))
    .cache()
)
print(f"trips.txt: {trips.count()} | trips with stop_times: {first_stop.count()}")
parts = F.split("trip_id", "_")
t5 = trips.join(first_stop, "trip_id").select(
    "trip_id", "route_id", "direction_id", "departure_time",
    parts[0].alias("tid_route"), parts[1].alias("tid_date"), parts[2].alias("tid_day"),
    parts[3].alias("tid_dir"), parts[4].alias("tid_hhmm"), F.size(parts).alias("n_parts"),
)
t5.groupBy("n_parts").count().orderBy("n_parts").show()
t5.agg(
    F.sum((F.col("tid_route") != F.col("route_id")).cast("int")).alias("route_differs"),
    F.sum((F.col("tid_dir").cast("int") != F.col("direction_id").cast("int") + 1).cast("int"))
    .alias("dir_not_direction_id_plus_1"),
    F.sum((F.col("tid_hhmm") != F.regexp_replace(F.substring("departure_time", 1, 5), ":", ""))
          .cast("int")).alias("hhmm_differs_from_first_departure"),
    F.sum((F.substring("departure_time", 1, 2).cast("int") >= 24).cast("int")).alias("first_dep_ge_24h"),
).show()
print("    tid_day values:", sorted(r[0] for r in t5.select("tid_day").distinct().collect()))

# ---------------------------------------------------------------- G6
print("=== G6. Trial mapping: HFP journey -> GTFS trip ===")
journeys = (
    hfp.filter(F.col("start").isNotNull())
    .select("oday", "route", "dir", "start")
    .distinct()
    .withColumn("op_date", F.to_date("oday"))
)
days = journeys.select("op_date").distinct()
dow_cols = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
# dayofweek(): 1 = Sunday ... 7 = Saturday  ->  index into dow_cols
dow_col = F.element_at(F.array(*[F.col(c) for c in dow_cols]), ((F.dayofweek("op_date") + 5) % 7) + 1)
regular = (
    calendar.crossJoin(days)
    .filter(
        (F.col("op_date") >= F.to_date("start_date", "yyyyMMdd"))
        & (F.col("op_date") <= F.to_date("end_date", "yyyyMMdd"))
        & (dow_col == "1")
    )
    .select("service_id", "op_date")
)
exc = calendar_dates.withColumn("op_date", F.to_date("date", "yyyyMMdd")).join(days, "op_date")
active = (
    regular.union(exc.filter("exception_type = '1'").select("service_id", "op_date"))
    .exceptAll(exc.filter("exception_type = '2'").select("service_id", "op_date"))
    .distinct()
)
candidates = (
    trips.join(first_stop, "trip_id")
    .withColumn("start_hhmm", F.substring("departure_time", 1, 5))
    .join(active, "service_id")
)
mapped = (
    journeys.join(
        candidates,
        (journeys.op_date == candidates.op_date)
        & (journeys.route == candidates.route_id)
        & (journeys.dir.cast("int") == candidates.direction_id.cast("int") + 1)
        & (journeys.start == candidates.start_hhmm),
        "left",
    )
    .groupBy(journeys.oday, "route", "dir", "start")
    .agg(F.count("trip_id").alias("candidates"))
    .cache()
)
total = mapped.count()
mapped.groupBy("candidates").count().orderBy("candidates").show()
ok = mapped.filter("candidates = 1").count()
print(f"Journeys: {total} | mapped to exactly 1 trip: {ok} ({ok / total:.1%})")
print("Unmapped journeys:")
mapped.filter("candidates = 0").orderBy("route", "start").show(20, truncate=False)
print("Ambiguous journeys (>1 trip):")
mapped.filter("candidates > 1").orderBy("route", "start").show(10, truncate=False)
