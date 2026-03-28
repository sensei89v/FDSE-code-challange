#!/usr/bin/env python3
"""
Ingestion service: subscribes to all MQTT rig measurement topics, prints received values,
and forwards each metric to the transformation service.

Usage:
    python ingestion.py --target-url <url> [--host <host>] [--port <port>]

Example:
    python ingestion.py --target-url http://localhost:8081/measures

Subscribes to: rigs/<rig_id>/measurements/<metric>
Message format: {"timestamp": "<iso>", "value": <number>}
"""

import argparse
import json
import logging
import signal
import time
import urllib.error
import urllib.request

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TOPIC = "rigs/#"
RETRY_BACKOFF = 2.0  # seconds; wait = min(RETRY_BACKOFF ** attempt, MAX_BACKOFF)
MAX_BACKOFF = 15.0


class IngestionContext:
    def __init__(self, target_url: str, mqtt_host: str, mqtt_port: int):
        self.target_url = target_url
        self._pending: dict | None = None
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata=self, client_id="ingestion", clean_session=False)
        self._client.on_connect = on_connect
        self._client.on_message = on_message
        self._client.connect(mqtt_host, mqtt_port, keepalive=60)

    def send_to_target(self, payload: dict) -> None:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            self.target_url,
            data=body,
            headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
            method="POST",
        )
        attempt = 0

        while True:
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    logger.debug("Sent to target: status=%d", resp.status)
                    return
            except urllib.error.HTTPError as exc:
                logger.warning("Target returned HTTP %d (attempt %d)", exc.code, attempt)
                if exc.code < 500 and exc.code != 429:  
                    logger.error("Target returned HTTP %d (not retrying): %s", exc.code, exc.read().decode(errors="replace"))
                    return
                
            except Exception as exc:
                logger.warning("Failed to send to target (attempt %d): %s", attempt, exc)
            attempt += 1
            time.sleep(min(RETRY_BACKOFF ** attempt, MAX_BACKOFF))

    def _republish_pending(self) -> None:
        if self._pending is None:
            return
        logger.info("Republishing unsent message back to MQTT: %s", self._pending)
        topic = f"rigs/{self._pending['rig_id']}/measurements/{self._pending['metric']}"
        body = json.dumps({"timestamp": self._pending["timestamp"], "value": self._pending["value"]})
        self._client.publish(topic, body, qos=1)

    def save_pending(self, payload: dict) -> None:
        self._pending = payload

    def clear_pending(self) -> None:
        self._pending = None

    def run(self) -> None:
        self._client.loop_forever()

    def shutdown(self) -> None:
        logger.info("Shutting down ingestion...")
        self._republish_pending()
        self._client.disconnect()


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe(TOPIC, qos=1)
        logger.info("Connected and subscribed to %s", TOPIC)
    else:
        logger.error("Connection failed, reason code %s", reason_code)


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON on topic %s: %s", msg.topic, exc)
        return

    # Topic pattern: rigs/<rig_id>/measurements/<metric>
    parts = msg.topic.split("/")
    if len(parts) != 4:
        logger.warning("Unexpected topic format: %s", msg.topic)
        return

    _, rig_id, _, metric = parts
    timestamp = data.get("timestamp")
    value = data.get("value")

    logger.info("topic=%-45s  timestamp=%s  value=%s", msg.topic, timestamp, value)

    payload = {"rig_id": rig_id, "metric": metric, "timestamp": timestamp, "value": value}
    userdata.save_pending(payload)
    userdata.send_to_target(payload)
    userdata.clear_pending()


def main():
    parser = argparse.ArgumentParser(description="MQTT ingestion: subscribe and forward rig measurements.")
    parser.add_argument("--host", default="localhost", help="MQTT broker host. Default: localhost")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port. Default: 1883")
    parser.add_argument(
        "--target-url", required=True,
        help="Transformation service endpoint, e.g. http://transformation:8081/measures",
    )
    args = parser.parse_args()

    ctx = IngestionContext(target_url=args.target_url, mqtt_host=args.host, mqtt_port=args.port)
    logger.info("Connecting to broker %s:%d  target=%s", args.host, args.port, args.target_url)

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, lambda *_: ctx.shutdown())

    try:
        ctx.run()
    finally:
        ctx.shutdown()


if __name__ == "__main__":
    main()
