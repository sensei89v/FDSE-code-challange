"""
MQTT producer that simulates test rig measurements.

Usage:
    python producer.py [--loop] [--accuracy 0.05] [--frequency 200] [--id-count 10]
"""

import argparse
import hashlib
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Range = tuple[float, float]


@dataclass
class Tick:
    datetime: datetime
    rig_id: str
    metric_name: str
    value: float


@dataclass
class Rig:
    name: str
    temp_inlet: Range
    temp_outlet: Range
    pressure: Range
    flow_rate: Range
    voltage: Range
    current: Range

    def generate_ticks(self, now: datetime, jitter_ms: float = 0.0) -> list[Tick]:
        timestamp = now + timedelta(milliseconds=random.uniform(0, jitter_ms))
        return [
            Tick(
                datetime=timestamp,
                rig_id=self.name,
                metric_name=metric,
                value=round(random.uniform(lo, hi), 3),
            )
            for metric, (lo, hi) in [
                ("temp_inlet",  self.temp_inlet),
                ("temp_outlet", self.temp_outlet),
                ("pressure",    self.pressure),
                ("flow_rate",   self.flow_rate),
                ("voltage",     self.voltage),
                ("current",     self.current),
            ]
        ]


METRICS = ["temp_inlet", "temp_outlet", "pressure", "flow_rate", "voltage", "current"]


def calculate_hash(*args) -> float:
    key = "|".join(map(str, args)).encode()
    raw = hashlib.md5(key).digest()
    return (int.from_bytes(raw[:4], "big") % 10000) / 100


def calculate_sensor_range(i: int, metric: str) -> tuple[float, float]:
    center = calculate_hash(i)
    half = calculate_hash(i, metric)
    return (center - half, center + half)


def build_rigs(id_count: int) -> list[Rig]:
    return [
        Rig(
            name=f"RIG-{i:02d}",
            temp_inlet=calculate_sensor_range(i, "temp_inlet"),
            temp_outlet=calculate_sensor_range(i, "temp_outlet"),
            pressure=calculate_sensor_range(i, "pressure"),
            flow_rate=calculate_sensor_range(i, "flow_rate"),
            voltage=calculate_sensor_range(i, "voltage"),
            current=calculate_sensor_range(i, "current"),
        )
        for i in range(1, id_count + 1)
    ]


def build_client(host: str, port: int) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Connected to broker %s:%d", host, port)
        else:
            logger.error("Connection failed, reason code %s", reason_code)

    client.on_connect = on_connect
    client.connect(host, port, keepalive=60)
    client.loop_start()
    return client


def publish_tick(client: mqtt.Client, accuracy: float, rigs: list[Rig], jitter_ms: float = 0.0) -> int:
    """Publish one measurement for every sensor on every rig. Returns message count sent."""
    now = datetime.now(timezone.utc)
    sent = 0

    ticks = [tick for rig in rigs for tick in rig.generate_ticks(now, jitter_ms)]
    random.shuffle(ticks)

    for tick in ticks:
        if random.random() > accuracy:
            logger.info("Dropped %s/%s (accuracy loss)", tick.rig_id, tick.metric_name)
            continue

        topic = f"rigs/{tick.rig_id}/measurements/{tick.metric_name}"
        payload = json.dumps({
            "timestamp": tick.datetime.isoformat(),
            "value": tick.value,
        })
        client.publish(topic, payload, qos=1)
        logger.info("→ %s  %s", topic, payload)
        sent += 1

    return sent


def run(args: argparse.Namespace) -> None:
    client = build_client(args.host, args.port)
    interval = args.frequency / 1000.0  # ms → seconds
    rigs = build_rigs(args.id_count)
    logger.info("Simulating %d rig(s): %s", args.id_count, ", ".join(r.name for r in rigs))

    try:
        if args.loop:
            logger.info("Running in loop mode (frequency=%dms, accuracy=%.2f). Ctrl+C to stop.",
                     args.frequency, args.accuracy)
            while True:
                sent = publish_tick(client, args.accuracy, rigs, args.jitter)
                logger.info("Published %d messages", sent)
                time.sleep(interval)
        else:
            sent = publish_tick(client, args.accuracy, rigs, args.jitter)
            logger.info("Published %d messages (single tick)", sent)
            time.sleep(0.5)  # let paho flush before exit
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down.")
    finally:
        client.loop_stop()
        client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MQTT test-rig producer")
    parser.add_argument(
        "--loop",
        action="store_true",
        default=False,
        help="Publish continuously until interrupted (default: single tick)",
    )
    parser.add_argument(
        "--accuracy",
        type=float,
        default=1.0,
        metavar="0..1",
        help="Probability of dropping a message (0 = always drop, 1 = never drop). Default: 1",
    )
    parser.add_argument(
        "--frequency",
        type=int,
        default=100,
        metavar="MS",
        help="Interval between publish ticks in milliseconds. Default: 100",
    )
    parser.add_argument("--host", default="localhost", help="MQTT broker host. Default: localhost")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port. Default: 1883")
    parser.add_argument(
        "--jitter",
        type=float,
        default=0.0,
        metavar="MS",
        help="Max random milliseconds added to each metric's timestamp. Default: 0",
    )
    parser.add_argument(
        "--id-count",
        type=int,
        default=10,
        metavar="N",
        help="Number of simulated rig IDs (RIG-01 … RIG-N). Default: 10",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if not (0.0 <= args.accuracy <= 1.0):
        raise SystemExit("--accuracy must be between 0 and 1")
    if args.frequency <= 0:
        raise SystemExit("--frequency must be a positive integer")
    if args.id_count <= 0:
        raise SystemExit("--id-count must be a positive integer")

    run(args)
