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
    "stop", "loc", "lat", "long", "dl", "acc", "spd", "occu", "jrn", "line",
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
