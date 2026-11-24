"""Capture raw HSL HFP tram messages to a JSONL file (Phase 1 profiling).

Usage:
    python bridge/capture_sample.py 600 samples/tram_day.jsonl
"""
import argparse
import json
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import paho.mqtt.client as mqtt

BROKER = "mqtt.hsl.fi"
PORT = 8883
# Three non-overlapping subscriptions: overlapping ones (e.g. "#" + "vp/#")
# would deliver the same message twice and distort the duplicate rate.
TOPICS = [
    "/hfp/v2/journey/ongoing/vp/tram/#",
    "/hfp/v2/journey/ongoing/arr/tram/#",
    "/hfp/v2/journey/ongoing/dep/tram/#",
]
STATS_INTERVAL_S = 30
# Healthy lateness is ~0.6s. A growing value means WE are slower than the source:
# messages pile up at the broker, which eventually drops us and discards the backlog.
LAG_WARN_S = 5.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture raw HFP tram messages to JSONL.")
    parser.add_argument("duration", type=int, help="capture duration in seconds")
    parser.add_argument("output", type=Path, help="output JSONL file")
    return parser.parse_args()


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Connect failed: {reason_code}")
        return
    userdata["connects"] += 1
    # Subscribe here (not after connect()) so we re-subscribe on every reconnect.
    client.subscribe([(topic, 0) for topic in TOPICS])
    print(f"Connected, subscribed to {len(TOPICS)} topics")


def on_message(client, userdata, msg):
    now = datetime.now(UTC)
    payload = msg.payload.decode("utf-8", errors="replace")
    record = {
        "topic": msg.topic,
        # Payload is stored as raw text: bronze must stay untouched.
        "payload": payload,
        "received_at": now.isoformat(timespec="milliseconds"),
    }
    userdata["file"].write(json.dumps(record, ensure_ascii=False) + "\n")
    # "/hfp/v2/journey/ongoing/vp/tram/..." -> ["", "hfp", "v2", "journey", "ongoing", "vp", ...]
    userdata["events"][msg.topic.split("/")[5]] += 1

    # Lateness = received - sent (tst). Parsed only for monitoring, never written back.
    try:
        tst = next(iter(json.loads(payload).values()))["tst"]
        sent = datetime.fromisoformat(tst)  # py3.11+ accepts the trailing "Z"
        userdata["lags"].append((now - sent).total_seconds())
    except (ValueError, KeyError, TypeError, AttributeError, StopIteration):
        userdata["bad_tst"] += 1


def report_stats(userdata, interval_s: float) -> None:
    # Swap the list instead of clearing it: the network thread keeps appending meanwhile.
    lags, userdata["lags"] = userdata["lags"], []
    if not lags:
        print(f"[stats] 0 msgs in last {interval_s:.0f}s")
        return
    lags.sort()
    p50, p99, worst = lags[len(lags) // 2], lags[int(len(lags) * 0.99)], lags[-1]
    warn = "  <-- FALLING BEHIND" if p50 > LAG_WARN_S else ""
    print(
        f"[stats] {len(lags) / interval_s:6.1f} msg/s | lateness p50={p50:5.1f}s "
        f"p99={p99:5.1f}s max={worst:5.1f}s{warn}"
    )


def on_disconnect(client, userdata, flags, reason_code, properties):
    if userdata["stopping"]:
        return  # our own disconnect() at shutdown, not a network problem
    userdata["disconnects"] += 1
    print(f"Unexpected disconnect: {reason_code} (paho will reconnect)")


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with args.output.open("w", encoding="utf-8") as f:
        userdata = {
            "file": f,
            "events": Counter(),
            "connects": 0,
            "disconnects": 0,
            "stopping": False,
            "lags": [],
            "bad_tst": 0,
        }
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            userdata=userdata,
        )
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        client.tls_set()  # system CA certificates

        client.connect(BROKER, PORT, keepalive=60)
        client.loop_start()  # network loop runs in a background thread
        start = time.monotonic()
        deadline = start + args.duration
        try:
            while (remaining := deadline - time.monotonic()) > 0:
                interval = min(STATS_INTERVAL_S, remaining)
                time.sleep(interval)
                report_stats(userdata, interval)
        except KeyboardInterrupt:
            print("Interrupted, stopping early")
        finally:
            elapsed = time.monotonic() - start
            userdata["stopping"] = True
            client.disconnect()
            client.loop_stop()

    events = userdata["events"]
    total = sum(events.values())
    rate = total / elapsed if elapsed > 0 else 0
    print(f"Done: {total} messages in {elapsed:.0f}s ({rate:.1f} msg/s) -> {args.output}")
    print("  " + "  ".join(f"{k}: {v}" for k, v in sorted(events.items())))
    print(f"  connects: {userdata['connects']}  unexpected disconnects: {userdata['disconnects']}")
    print(f"  messages with unreadable tst: {userdata['bad_tst']}")


if __name__ == "__main__":
    main()