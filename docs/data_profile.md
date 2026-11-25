# Data profile: HSL HFP (trams) and HSL GTFS

This document records what the source data actually looks like, measured before any table was designed.
Every design decision in later phases (dedup key, primary keys, watermark, column types, trip mapping)
points back to a number here.

Status: Phase 1, profiled on 2026-11-23/24/25. Night-time behaviour (trips past midnight) is
**not yet measured**. See [Open questions](#open-questions).

## Samples

| Sample | Source | When | Size |
|---|---|---|---|
| HFP day capture | `mqtt.hsl.fi:8883`, topics `vp`, `arr`, `dep` for `tram` (3 non-overlapping subscriptions) | 2026-11-23 13:46:22–13:56:21 UTC (15:46 Helsinki, Monday) | 190,176 messages, 100.7 MB JSONL |
| HFP clean window | Last 241 s of the day capture (see [Measurement caveats](#measurement-caveats)) | 13:52:20–13:56:21 UTC | 111,709 messages |
| HFP second capture | same topics | 2026-11-25 15:18:35–15:28:35 UTC (17:18 Helsinki, Wednesday) | 198,958 messages; 3 disconnects, ~73 s without data. Used for duplicates and ordering (part D) only |
| GTFS | `https://infopalvelut.storage.hsldev.com/gtfs/hsl.zip`, `feed_version` 2026-11-21 07:23:06 | downloaded 2026-11-24 | 91 MB zip, 1.36 GB unzipped (`stop_times.txt` 1.15 GB) |

Each captured line is `{"topic", "payload" (raw text, unparsed), "received_at" (UTC, ms)}`.
Unless stated otherwise, HFP numbers come from the clean window.

## Volume

| Metric | Value |
|---|---|
| Trams sending VP | 118 (peak afternoon) |
| VP after dedup | 0.98 msg/s per tram (HSL documents 1 Hz) |
| Raw VP rate (with duplicates) | 461.5 msg/s |
| ARR / DEP | 241 / 236 events in 241 s (about 1/s each, network-wide) |
| Routes seen | 16 |
| Size per captured line | ~530 B (topic + payload + receive time) |

Sizing estimate for Kafka (raw, before dedup): about 250 KB/s at peak, on the order of 10 GB/day
uncompressed. This is an estimate from one 4-minute peak window, not a measurement over a day.

## Duplicates and the dedup key

| Check | Result |
|---|---|
| Copies per distinct VP | exactly **4** for 27,772 of 27,844 (99.7%); the 72 others all sit in the first ~2 s of the filtered window (edge effect) |
| Copies per distinct ARR / DEP | **1** (241/241, 236/236) |
| ARR/DEP reported twice for the same visit, key `(oper, veh, oday, start, stop)` | 0 of 477 |
| Distinct `(event, oper, veh, tst)` vs distinct `(topic, payload)` | 28,321 = 28,321, 0 keys with more than one payload or topic |
| `(oper, veh, tst)` shared by two event types | 0 |
| Time between first and last copy of a VP | clean window: p50 0.037 s, p90 0.194 s, p99 0.32 s, max 1.015 s. Second capture: p99 1.19 s, **max 28.6 s** |
| Second capture (2026-11-25) | VP: 4 copies for 49,499 of 49,554 (99.9%); ARR 439 and DEP 413, all with 1 copy; key check again 50,406 = 50,406, 0 conflicts |

The duplication comes from the source, not from the subscription: the three subscriptions do not
overlap, and all copies carry the same topic. Across both captures, 1,329 ARR/DEP events had no
duplicate.

## Lateness, ordering and the watermark

Spark's watermark does not use the wall clock. It is `max(tst seen so far) − delay`, and a row whose
`tst` is older than that is dropped as late. So the number that matters is **how far a row's `tst` is
behind the largest `tst` already received** ("behind"), computed from arrival order only. It does not
depend on the local clock, and a capture client that lags uniformly (one TCP connection, FIFO) does not
change it.

| Metric | Clean window 2026-11-23 (241 s) | Second capture 2026-11-25 (600 s) |
|---|---|---|
| Behind stream max `tst`, VP: p50 / p99 / p99.9 / max | 0.005 / 0.22 / 12.8 / **28.1 s** | 0.005 / 0.16 / 6.1 / **20.5 s** |
| Behind, ARR: p99 / max | 3.0 / 10.0 s | 0.28 / 4.7 s |
| Behind, DEP: p99 / max | 0.67 / 6.4 s | 0.28 / 0.49 s |
| Rows behind by more than 10 s / 30 s / 60 s | VP 172 / 0 / 0; ARR, DEP 0 | VP 76 / 0 / 0; ARR, DEP 0 |
| Out-of-order per vehicle (deduped): rows / max step back | ARR 0, DEP 0, VP 32 / 4.0 s | ARR 0, DEP 0, VP 34 / 5.0 s |

The rows far behind come in **bursts from a single vehicle**: e.g. tram 40/624 delivered VP 28 s old
in one batch (2026-11-23), tram 40/412 20.5 s old (2026-11-25). This looks like an on-board buffer
flushed after a connectivity gap. ARR and DEP were never more than 10 s behind.

Wall-clock lateness (`received_at − tst`) could not be measured reliably: it was dominated by the
capture client's own lag and the WSL clock offset (see caveats). In the clean window its p50 was
0.66 s (VP) including the clock offset of that day; its minimum on 2026-11-25 (5.2 s) matches that
day's clock offset (4.4–5.2 s), i.e. the fastest messages arrive within about a second.

## Field quality (HFP, deduplicated)

| Field | Finding |
|---|---|
| `loc`, `lat`, `long` | 122 of 27,765 VP with `loc = GPS` have no coordinates (0.44%). 79 VP use `loc = DR` (dead reckoning) and do have coordinates |
| `spd`, `acc`, `hdg` | null on ~123 rows, the same rows that lost GPS. `acc` ranges from −6.61 to **11.11 m/s²** (a tram does not accelerate at 1 g) |
| `occu` | always `0` |
| `dl` | −386 to +659 s, 160 nulls |
| `route`, `desi` | `route` contains letters (`1009TX`, `1013E`); for these two, `desi` is `000` and the topic headsign is always `Ei linjalla` (Finnish: "not on a line"), i.e. **non-revenue runs** (1,496 and 657 raw messages) |
| `oper`, `veh` | zero-padded in the topic (`0040`, `00458`), plain in the payload (`40`, `458`); otherwise topic and payload agree on oper, veh, route, dir |
| `dir` | values `1`, `2` |
| `stop` (ARR) | equals the topic's next stop in 241/241 |
| `stop` (DEP) | differs from the topic's next stop in **217/236 (92%)**: the payload has the stop just left, the topic has already moved on |
| `oday`, `start` | Helsinki local time. Read as Helsinki, VP fall between −10.6 and +59 min of the scheduled trip start; read as UTC they would be ~120 min off |
| `dl` vs schedule | DEP delay `tst − ttdep`: p50 **+42 s**. It exceeds `−dl` by p50 24 s (p1 0 s, p99 54 s) |

## GTFS

| Check | Result |
|---|---|
| Validity | `feed_start_date` 2026-11-20 to `feed_end_date` 2027-01-18 (about **2 months**); 8,882 service_ids, 4,158 exceptions |
| HFP routes in `routes.txt` | 14 of 16. `1009TX` and `1013E` are absent, also as variants |
| Route variants | `100HF4`, `100HG3`, `100HI9`, … are **variants of `route_id` 100H**: they appear as the prefix of `trip_id`/`service_id`, not in `routes.txt`. Prefixes can contain a space (`31M1 7`) |
| HFP stops in `stops.txt` | 320 of 323. The 3 missing (`1140443`, `1140444`, `1100301`) are served only by `1009TX`/`1013E` and never appear in `stop_times.txt` |
| `desi` vs `route_short_name` | equal for every matched route |
| Direction | HFP `dir` 1/2 = GTFS `direction_id` 0/1 (14 + 12 route/headsign pairs agree). Route 5 uses the headsign `Katajanokan term.` in both directions |
| `trip_id` format | `<route variant>_<date>_<day type>_<dir 1/2>_<HHMM>`, optional suffix such as `K20261225`. The dir part equals `direction_id + 1` in all 444,330 trips |
| Times past midnight | 14,551 trips start at or after `24:00:00` |
| `parent_station` | a single space in 7,977 of 8,413 stops (436 real values, 0 null) |
| `zone_id` | never empty in this feed |
| BOM | `stop_times.txt` starts with a UTF-8 BOM (`﻿trip_id`) |

**Trial trip mapping.** Key: operating day (`oday`), route, `dir = direction_id + 1`,
`start = first departure HH:MM`, with service active on that day from `calendar` plus `calendar_dates`.
Result: **142 of 146 journeys map to exactly one trip, 0 ambiguous.** The 4 unmapped journeys are
exactly the `1009TX`/`1013E` ones.

## Issues and handling

| # | Issue | Evidence | Impact if ignored | Handling | Where |
|---|---|---|---|---|---|
| 1 | VP arrives 4 times | 99.7% of VP have 4 copies | 4× rows written, inflated counts | Dedup on `(event, oper, veh, tst)` in Spark; primary key + `ON CONFLICT DO NOTHING` | Spark (P4), silver |
| 2 | ARR/DEP not duplicated at source, but replays happen on restart | 1 copy each, 0/477 repeats | Duplicates after a Spark restart | Primary key `(event_type, oper, veh, tst)` is the safety net | silver (P2) |
| 3 | A slow consumer is silently dropped by the broker | client lag grew to 30–70 s, 4 disconnects, ~1/3 of data lost in 6 min | Data loss with only a vague `Unspecified error` in logs | Do minimal work in `on_message`; log lateness as a metric; alert on growth | bridge (P3), P11 |
| 4 | MQTT QoS 0 has no replay | gaps after disconnects | Anything missed while the bridge is down is gone | Accept and measure; Kafka protects everything after the bridge | P3, P11 |
| 5 | `loc = GPS` without coordinates | 0.44% of VP | Points at (null, null) on the map | Keep the row; filter `lat IS NOT NULL` in the map panel | silver, Grafana |
| 6 | Implausible `acc` | max 11.11 m/s² | Misleading analysis | Keep raw in silver, do not use in gold | gold |
| 7 | `occu` always 0 | 1 distinct value | No information | Ignore | gold |
| 8 | `dl` disagrees with schedule-based delay | p50 24 s apart | KPI that cannot be explained | Delay = `tst − ttarr` / `tst − ttdep` | gold (P8) |
| 9 | DEP `stop` differs from topic | 217/236 | Departure attributed to the wrong stop | Always read `stop` from the payload | Spark (P4) |
| 10 | Zero-padded oper/veh in topic | `0040` vs `40` | Two keys for one vehicle | Kafka key `<oper>/<veh>` without leading zeros | bridge (P3) |
| 11 | Mixed time zones | `tst` UTC, `oday`/`start` Helsinki | Wrong trip mapping, DST bugs | All timestamps UTC; keep `oday`/`start` as local text | silver |
| 12 | VP before the trip starts | down to −10.6 min | False "early" at the first stop | Punctuality from ARR/DEP at stops, not from `dl` | gold, viz |
| 13 | Routes not in GTFS | `1009TX`, `1013E`, headsign `Ei linjalla` (non-revenue) | Facts dropped silently by an INNER JOIN, or non-revenue runs counted as late service | `route` as text; unknown member (key −1); show the raw route code in marts; flag `Ei linjalla` as non-revenue and exclude it from punctuality KPIs | silver, gold (P8) |
| 14 | Route variants | `100HF4` → `100H` | Variant journeys become "unknown" | Map via `trip_id` prefix before falling back to unknown | intermediate (P7) |
| 15 | Stops not in GTFS | 3 stops, all on non-GTFS routes | Missing stop dimension | Unknown member | gold |
| 16 | `parent_station` is a space | 7,977 stops | Looks non-null | `nullif(trim(...), '')` | staging (P6) |
| 17 | BOM in `stop_times.txt` | first header `﻿trip_id` | First column not found | Strip BOM from header names | GTFS loader (P5) |
| 18 | Times ≥ 24:00 | 14,551 trips | Parsing as a time fails | Keep as text, normalise for matching, flag `is_after_midnight` | staging (P6) |
| 19 | Feed is replaced by newer versions | 2-month validity, new version published regularly | Old trips can vanish from the current feed | Archive the zip daily; mapping and `dim_trip` incremental | P5, P7 |
| 20 | Headsign does not identify direction | route 5 | Wrong direction if derived from headsign | Use `dir` | — |
| 21 | Single vehicles deliver old VP in bursts | up to 28.1 s behind the stream | Rows dropped as late with a short watermark | Watermark 2 minutes (> 4× the largest observed) | Spark (P4) |
| 22 | Copies of one VP can arrive far apart | max 28.6 s between first and last copy | A copy outside the dedup state slips through | Watermark 2 minutes covers it; primary key catches anything left | Spark (P4), silver |

## Decisions taken from this profile

| Decision | Based on |
|---|---|
| Dedup key `(event, oper, veh, tst)` | Duplicates and the dedup key |
| `silver.stop_event` primary key `(event_type, oper, veh, tst)`, `silver.vehicle_position` primary key `(oper, veh, tst)` | same |
| `route` and `stop` stored as text | Field quality, GTFS |
| Kafka key `<oper>/<veh>` from payload values | Field quality (zero padding); 118 trams spread well over 3 partitions |
| Delay measured against `ttarr`/`ttdep`, not `dl` | Field quality |
| Trip mapping key `(oday, route, dir − 1, start HH:MM)` with variant fallback | GTFS trial mapping |
| Watermark **2 minutes** for dedup (`dropDuplicatesWithinWatermark`). Largest observed: 28.1 s behind the stream, 28.6 s between copies; 0 rows beyond 30 s in either capture. State stays small: ~115 deduped VP/s × 120 s ≈ 14k keys | Lateness, ordering and the watermark |

## Measurement caveats

**The capture client fell behind.** In the first ~6 minutes of the day capture the client received
about 200 msg/s while the source sent about 470 msg/s. Lateness grew from about 0 to 30–70 s, the broker
disconnected the client four times (paho reported `Unspecified error`), and each time the queued backlog
was discarded. About a third of the data in that window is missing, although `received_at` shows only
about 12 s of gaps. After 13:52:20 UTC the client kept up (p50 lateness 0.6 s). The cause of the slow
start is not known. Only the clean window is used for the numbers above. The capture script now prints
throughput and lateness every 30 s and warns when p50 lateness exceeds 5 s.

**The second capture lagged too.** On 2026-11-25 the client again fell behind in its first minute
(188 msg/s, p50 lateness ~17 s), caught up, then stalled: for about 90 s it received 2–22 msg/s, paho
reported `Keep alive timeout` twice and `Unspecified error` once, and lateness stayed between 15 and
60 s for most of the run. Since the main thread kept printing on time, the process was not frozen;
the stall was in receiving. Whether the cause is the network path (Vietnam to Helsinki, WSL NAT) or
processing in the client is not known yet. Volume and per-vehicle rates from this capture are
therefore not used; duplicates and ordering are (see above).

**The WSL clock runs fast, and drifts.** The WSL clock was 1.1–1.4 s ahead of a public server's `Date`
header on 2026-11-24 and 4.4–5.2 s ahead on 2026-11-25, although `timedatectl` reported it as
synchronized. Every wall-clock lateness value measured on this machine is inflated by that amount, and
the capture script's "falling behind" warning (p50 > 5 s) fires falsely when the offset is that large.

## Open questions

- **Why the capture client lags:** network path or processing? Test: run the capture with an
  `on_message` that only counts (no JSON, no file write). If the lag remains, it is the network.
  This decides how the bridge must be built in Phase 3.
- **Wall-clock lateness of the source:** needs a synchronized clock and a client that keeps up.
- **Trips past midnight:** how HFP reports `oday` and `start` for trips scheduled at `24:xx`. Needs a
  capture between 00:00 and 02:00 Helsinki time.
- **ARR/DEP duplicates over a longer period:** 477 events is a small sample.
- **Variation by time and day:** one Monday afternoon peak so far.
- **Other non-revenue codes:** `1009TX` and `1013E` are non-revenue runs (headsign `Ei linjalla`).
  Are there other route codes or headsigns for this, e.g. at night or at depot hours?

## Reproduce

```bash
# capture (prints [stats] every 30 s)
python bridge/capture_sample.py 600 samples/tram_day_$(date +%Y%m%d_%H%M).jsonl

# HFP profile, parts A–E (2nd argument: optional minimum received time, UTC)
python scripts/profile_hfp.py samples/tram_day_20261123_1346.jsonl "2026-11-23 13:52:20"

# GTFS cross-check, G1–G6 (unzip the feed first)
python scripts/profile_gtfs.py samples/gtfs/dl_20261124/extracted samples/tram_day_20261123_1346.jsonl
```
