import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

spark = SparkSession.builder.master("local[*]").appName("profile-hfp").config("spark.sql.session.timeZone", "UTC").getOrCreate()

spark.sparkContext.setLogLevel("WARN")
path = sys.argv[1]
# Optional 2nd arg: only keep rows received at/after this UTC time,
# e.g. "2026-11-23 13:52:20" to skip a window where the capture client lagged.
min_received = sys.argv[2] if len(sys.argv) > 2 else None
raw = spark.read.json(path)

SCHEMA = MapType(StringType(), MapType(StringType(), StringType()))

FIELDS = [
    "oper", "veh", "tst", "route", "desi", "dir", "oday", "start",
    "stop", "loc", "lat", "long", "dl", "acc", "spd", "hdg", "occu", "jrn", "line",
    "ttarr", "ttdep",  # scheduled arrival/departure at the stop (ARR/DEP only)
]

parsed = raw.select(
    "topic",
    "payload",
    "received_at",
    F.from_json("payload", SCHEMA).alias("parsed"),
)

fields = F.map_values("parsed")[0]

df = (
    parsed.select(
        "topic",
        "payload",
        F.map_keys("parsed")[0].alias("event"),
        *[fields.getItem(field).alias(field) for field in FIELDS],
        F.to_timestamp("received_at").alias("received_ts"),
        F.to_timestamp(fields["tst"]).alias("tst_ts"),
    )
    .withColumn(
        "lateness_s",
        (F.unix_micros("received_ts") - F.unix_micros("tst_ts")) / 1e6
    )
)
if min_received:
    df = df.filter(F.col("received_ts") >= F.to_timestamp(F.lit(min_received)))
    print(f"Filter: received_ts >= {min_received} UTC")
df = df.cache()

df.printSchema()
df.select("event", "oper", "veh", "tst_ts", "received_ts", "lateness_s", "route", "stop").show(5, truncate=False)
print("rows:", df.count())

print("\n=== B.Volume & vehicles ===")

print("B1. Rows per event type")
df.groupBy("event").count().orderBy("event").show()

print("B2. Distinct vehicles (oper + veh) per event type")
(
    df.groupBy("event")
    .agg(F.countDistinct("oper", "veh").alias("vehicles"))
    .orderBy("event")
    .show()
)

# B3: seconds that actually contain data (automatically excludes disconnect gaps)
time_stats = df.agg(
    (F.max("received_ts").cast("long") - F.min("received_ts").cast("long")).alias("span_s"),
    F.countDistinct(F.date_trunc("second", "received_ts")).alias("active_s"),
).first()
span_s, active_s = time_stats["span_s"], time_stats["active_s"]
print(f"B3. Time span: {span_s}s | seconds with data: {active_s}s | ~gap: {span_s - active_s + 1}s")

# B4: VP rate before/after naive dedup (exact dedup key is studied in part C)
vp = df.filter(F.col("event") == "VP")
vp_raw = vp.count()
vp_dedup = vp.dropDuplicates(["topic", "payload"]).count()
vp_vehicles = vp.select(F.countDistinct("oper", "veh")).first()[0]

print("B4. VP rate")
print(f"    raw        : {vp_raw:>8} rows | {vp_raw / active_s:7.1f} msg/s")
print(f"    deduped    : {vp_dedup:>8} rows | {vp_dedup / active_s:7.1f} msg/s")
print(f"    vehicles   : {vp_vehicles}")
print(f"    per vehicle: {vp_dedup / active_s / vp_vehicles:.2f} VP/s/vehicle (after dedup)")
print(f"    dup factor : {vp_raw / vp_dedup:.2f}x")

# B5: per-vehicle rate within each vehicle's OWN presence window.
# Tests the hypothesis "rate < 1 because vehicles join/leave mid-sample".
per_vehicle = (
    vp.dropDuplicates(["topic", "payload"])
    .groupBy("oper", "veh")
    .agg(
        F.count("*").alias("msgs"),
        (F.max("tst_ts").cast("long") - F.min("tst_ts").cast("long") + 1).alias("present_s"),
    )
    .withColumn("rate", F.col("msgs") / F.col("present_s"))
    .cache()
)
print("B5. Per-vehicle VP (deduped): messages, presence window, rate within presence")
per_vehicle.describe("msgs", "present_s", "rate").show()
full_window = per_vehicle.filter(F.col("present_s") >= span_s * 0.95).count()
print(f"    vehicles present >=95% of the sample: {full_window}/{vp_vehicles}")
print("    rate distribution (rounded to 0.1):")
(
    per_vehicle.groupBy(F.round("rate", 1).alias("rate"))
    .count()
    .orderBy("rate")
    .show(30)
)

# --- C: duplicates & dedup key ---
print("\n=== C. Duplicates & dedup key ===")
PCTS = [0.5, 0.9, 0.99, 1.0]

# C1: exact duplicates = same (topic, payload)
exact = (
    df.groupBy("event", "topic", "payload")
    .agg(
        F.count("*").alias("copies"),
        # C3 input: how far apart the copies of one message arrive
        ((F.unix_micros(F.max("received_ts")) - F.unix_micros(F.min("received_ts"))) / 1e6)
        .alias("spread_s"),
    )
    .cache()
)
n_rows, n_exact = df.count(), exact.count()
print(f"C1. rows: {n_rows} | distinct (topic, payload): {n_exact} | dup factor: {n_rows / n_exact:.3f}x")
print("    How many copies does each distinct message have?")
exact.groupBy("event", "copies").count().orderBy("event", "copies").show()

# C2: candidate dedup key = (event, oper, veh, tst)
KEY = ["event", "oper", "veh", "tst"]
by_key = (
    df.groupBy(*KEY)
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("payload").alias("payloads"),
        F.countDistinct("topic").alias("topics"),
    )
    .cache()
)
n_key = by_key.count()
print(f"C2. distinct key {tuple(KEY)}: {n_key} | distinct (topic, payload): {n_exact}")
print("    -> equal means the key identifies a message exactly" if n_key == n_exact
      else "    -> NOT equal: same key carries different content, inspect below")
conflicts = by_key.filter((F.col("payloads") > 1) | (F.col("topics") > 1))
print(f"    keys with >1 distinct payload or topic: {conflicts.count()}")
conflicts.orderBy(F.desc("payloads")).show(5, truncate=False)

# C2b: is `event` needed in the key? Same vehicle + same tst across event types?
cross = df.groupBy("oper", "veh", "tst").agg(F.countDistinct("event").alias("events"))
cross_n = cross.filter("events > 1").count()
print(f"C2b. (oper, veh, tst) shared by >1 event type: {cross_n}")
if cross_n:
    (
        df.join(cross.filter("events > 1"), ["oper", "veh", "tst"])
        .select("event", "oper", "veh", "tst", "stop", "route")
        .dropDuplicates()
        .orderBy("oper", "veh", "tst", "event")
        .show(6, truncate=False)
    )

# C3: arrival spread between first and last copy -> lower bound for the dedup watermark
spread = exact.filter("copies > 1").agg(
    F.percentile_approx("spread_s", PCTS).alias("p50_p90_p99_max")
).first()[0]
print(f"C3. spread first->last copy (s), p50/p90/p99/max: {spread}")
print("    By event type:")
(
    exact.filter("copies > 1")
    .groupBy("event")
    .agg(F.percentile_approx("spread_s", PCTS).alias("p50_p90_p99_max"))
    .orderBy("event")
    .show(truncate=False)
)

# --- E: field quality (on deduplicated rows: copies would just multiply every count) ---
print("\n=== E. Field quality (deduplicated) ===")
LOCAL_TZ = "Europe/Helsinki"
topic_parts = F.split("topic", "/")
q = (
    df.dropDuplicates(["event", "oper", "veh", "tst"])
    .withColumn("topic_oper", topic_parts[7])
    .withColumn("topic_veh", topic_parts[8])
    .withColumn("topic_route", topic_parts[9])
    .withColumn("topic_dir", topic_parts[10])
    .withColumn("topic_next_stop", topic_parts[13])
    .cache()
)

print("E1. Positioning source `loc` and missing coordinates")
(
    q.groupBy("event", "loc")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("lat").isNull().cast("int")).alias("lat_null"),
        F.sum(F.col("long").isNull().cast("int")).alias("long_null"),
    )
    .orderBy("event", "loc")
    .show()
)

print("E2. Numeric ranges (min / p1 / p50 / p99 / max)")
for name in ["spd", "acc", "hdg", "dl"]:
    stats = q.select(
        F.percentile_approx(F.col(name).cast("double"), [0.0, 0.01, 0.5, 0.99, 1.0]).alias("p"),
        F.sum(F.col(name).isNull().cast("int")).alias("nulls"),
    ).first()
    print(f"    {name:<4} {stats['p']}  nulls={stats['nulls']}")
print("    occu distinct values:", [r[0] for r in q.select("occu").distinct().collect()])

print("E3. Route / designation: values containing letters")
(
    q.filter(F.col("route").rlike("[A-Za-z]") | F.col("desi").rlike("[A-Za-z]"))
    .groupBy("route", "desi")
    .agg(F.countDistinct("oper", "veh").alias("vehicles"), F.count("*").alias("rows"))
    .orderBy("route")
    .show(truncate=False)
)
print(f"    distinct routes: {q.select('route').distinct().count()}")

print("E4. Payload vs topic consistency")
(
    q.agg(
        F.sum((F.col("oper").cast("int") != F.col("topic_oper").cast("int")).cast("int")).alias("oper_diff"),
        F.sum((F.col("veh").cast("int") != F.col("topic_veh").cast("int")).cast("int")).alias("veh_diff"),
        F.sum((F.col("route") != F.col("topic_route")).cast("int")).alias("route_diff"),
        F.sum((F.col("dir") != F.col("topic_dir")).cast("int")).alias("dir_diff"),
    )
    .show()
)
print("    dir values:", sorted(r[0] for r in q.select("dir").distinct().collect() if r[0]))
print("    topic oper/veh are zero-padded? sample:",
      q.select("topic_oper", "oper", "topic_veh", "veh").first())

print("E5. ARR/DEP: stop in payload vs next_stop in topic")
(
    q.filter(F.col("event").isin("ARR", "DEP"))
    .groupBy("event")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("stop").isNull().cast("int")).alias("stop_null"),
        F.sum((F.col("stop") == F.col("topic_next_stop")).cast("int")).alias("same_as_topic"),
        F.sum((F.col("stop") != F.col("topic_next_stop")).cast("int")).alias("differs_from_topic"),
    )
    .orderBy("event")
    .show()
)

print("E6. Time zones: `start` (Helsinki local) vs `tst` (UTC)")
# Interpret oday + start as Helsinki wall-clock time, convert to UTC, compare with tst.
start_utc = F.to_utc_timestamp(F.concat_ws(" ", "oday", "start"), LOCAL_TZ)
mins_since_start = (F.unix_timestamp("tst_ts") - F.unix_timestamp(start_utc)) / 60
(
    q.filter("event = 'VP'")
    .select(F.percentile_approx(mins_since_start, [0.0, 0.01, 0.5, 0.99, 1.0]).alias("p"))
    .show(truncate=False)
)
print("    (min since scheduled trip start, as Helsinki time. ~0..90 = correct;"
      " values near -120 would mean start was wrongly read as UTC)")
print("    oday values:", sorted(r[0] for r in q.select("oday").distinct().collect() if r[0]))

print("E7. DEP: delay from schedule (tst - ttdep) vs field dl")
dep = (
    q.filter("event = 'DEP'")
    .withColumn("delay_sched_s",
                F.unix_timestamp("tst_ts") - F.unix_timestamp(F.to_timestamp("ttdep")))
    .withColumn("delay_dl_s", -F.col("dl").cast("int"))  # dl < 0 means late
    .withColumn("diff_s", F.col("delay_sched_s") - F.col("delay_dl_s"))
)
print(f"    DEP rows: {dep.count()} | ttdep null: {dep.filter('ttdep is null').count()}")
dep.select(
    F.percentile_approx("delay_sched_s", [0.01, 0.5, 0.99]).alias("sched_delay_p1_p50_p99"),
    F.percentile_approx("diff_s", [0.01, 0.5, 0.99]).alias("diff_vs_dl_p1_p50_p99"),
).show(truncate=False)
