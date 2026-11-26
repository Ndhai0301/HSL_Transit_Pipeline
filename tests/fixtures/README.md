# Test fixtures

Real HFP messages captured from `mqtt.hsl.fi` on 2026-11-23 (clean window 13:52:23–13:56:21 UTC),
selected to cover the data issues in [docs/data_profile.md](../../docs/data_profile.md).

Format: one JSON object per line, exactly as written by `bridge/capture_sample.py`:
`{"topic": ..., "payload": <raw MQTT payload as text>, "received_at": <UTC ISO 8601>}`.
This is also the envelope the bridge will put on Kafka, so Spark tests can read these lines as
Kafka values.

Every line is copied verbatim from the capture, **except the last line of `hfp_vp_sample.txt`**
(see below). Line numbers below are 1-based; keep them stable, tests refer to them.

## `hfp_vp_sample.txt` (17 lines)

| Lines | Case | What a test can check |
|---|---|---|
| 1–4 | One VP (route 1006, veh 40/445) received **4 times**, identical topic and payload | Dedup keeps 1 row |
| 5–8 | The same vehicle's next 4 VP, 1 s apart, 1 copy each | Dedup does not drop distinct messages; ordering by `tst` |
| 9–12 | VP with `loc = GPS` but **no `lat`/`long`/`spd`/`acc`**, 4 copies (route 2015, veh 40/607) | Nulls survive parsing; row is kept, not sent to DLQ |
| 13 | VP with `loc = DR` (dead reckoning) and `stop` set | Accepted `loc` values |
| 14 | Route **`1009TX`**, `desi = "000"`, headsign `Ei linjalla` (non-revenue, not in GTFS) | `route` parsed as text; unknown-member handling |
| 15 | `acc = 11.11` (implausible) | Value kept as-is in silver |
| 16 | Received **29 s after `tst`** (shortly after a reconnect) | Watermark behaviour for late events |
| 17 | **DERIVED:** line 14 with the payload cut after 120 characters; the envelope is still valid JSON | Parse failure goes to the DLQ, the stream keeps running |

## `hfp_arrdep_sample.txt` (8 lines)

| Lines | Case | What a test can check |
|---|---|---|
| 1–6 | One journey (route 1011, veh 40/632, start 15:41): ARR, DEP at 3 consecutive stops | Stop visit = ARR + DEP; dwell time; ARR `stop` equals the topic's next stop, **DEP `stop` does not** |
| 7 | DEP without ARR (route 1003, veh 40/434; the ARR happened before the capture window) | A visit with only DEP still produces a row |
| 8 | ARR on route `1009TX` at stop **`1140444`, which is not in GTFS** | Unknown stop handling |

ARR/DEP have no duplicated copies here because the source did not duplicate them in the sample
(see the data profile). Duplicates for ARR/DEP tests come from replaying a line, not from the source.

## Notes for tests

- `oper`, `veh`, `stop` are JSON numbers in the payload, but zero-padded strings in the topic.
- `tst` is UTC; `oday` and `start` are Helsinki local time.
- No GTFS fixtures yet; they belong to Phase 5 (stop_sequence as number, times ≥ 24:00, BOM).
