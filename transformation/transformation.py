#!/usr/bin/env python3
"""
Transformation service: HTTP server that collects vertical metric messages,
groups them by (rig_id, timestamp), and after a configurable window flushes
a horizontal row to a downstream target URL.

Usage:
    python transformation.py --target-url <url> [--host <host>] [--port <port>] [--window <seconds>]

Example:
    python transformation.py --target-url http://localhost:8080/measures

POST /measures
    Body (JSON) with fields:
        timestamp  - required (ISO 8601 string)
        metric     - required (e.g. "temp_inlet", "pressure")
        value      - required (decimal number)
        rig_id     - required (string)
"""

import argparse
import signal
import json
import logging
import os
import socket
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from decimal import Decimal
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import pydantic
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KNOWN_METRICS = {"temp_inlet", "temp_outlet", "pressure", "flow_rate", "voltage", "current"}


class SendResult(Enum):
    Succeed = "succeed"
    FailedTimeout = "failed_timeout"
    FailedOther = "failed_other"


@dataclass
class HorizontalMetric:
    rig_id: str
    timestamp: datetime
    temp_inlet: Optional[Decimal] = field(default=None)
    temp_outlet: Optional[Decimal] = field(default=None)
    pressure: Optional[Decimal] = field(default=None)
    flow_rate: Optional[Decimal] = field(default=None)
    voltage: Optional[Decimal] = field(default=None)
    current: Optional[Decimal] = field(default=None)
    processing_timestamp: datetime = field(default_factory=datetime.now)

    @property
    def is_complete(self) -> bool:
        return all(getattr(self, m) is not None for m in KNOWN_METRICS)

    def set_metric(self, metric: str, value: Decimal) -> None:
        setattr(self, metric, value)

    def to_payload(self) -> dict:
        return {
            "rig_id": self.rig_id,
            "timestamp": self.timestamp.isoformat(),
            "temp_inlet": str(self.temp_inlet) if self.temp_inlet is not None else None,
            "temp_outlet": str(self.temp_outlet) if self.temp_outlet is not None else None,
            "pressure": str(self.pressure) if self.pressure is not None else None,
            "flow_rate": str(self.flow_rate) if self.flow_rate is not None else None,
            "voltage": str(self.voltage) if self.voltage is not None else None,
            "current": str(self.current) if self.current is not None else None,
        }

    def to_json(self) -> dict:
        return {
            "rig_id": self.rig_id,
            "timestamp": self.timestamp.isoformat(),
            "processing_timestamp": self.processing_timestamp.isoformat(),
            "temp_inlet": str(self.temp_inlet) if self.temp_inlet is not None else None,
            "temp_outlet": str(self.temp_outlet) if self.temp_outlet is not None else None,
            "pressure": str(self.pressure) if self.pressure is not None else None,
            "flow_rate": str(self.flow_rate) if self.flow_rate is not None else None,
            "voltage": str(self.voltage) if self.voltage is not None else None,
            "current": str(self.current) if self.current is not None else None,
        }

    @classmethod
    def from_json(cls, data: dict) -> "HorizontalMetric":
        return cls(
            rig_id=data["rig_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            temp_inlet=Decimal(data["temp_inlet"]) if data["temp_inlet"] is not None else None,
            temp_outlet=Decimal(data["temp_outlet"]) if data["temp_outlet"] is not None else None,
            pressure=Decimal(data["pressure"]) if data["pressure"] is not None else None,
            flow_rate=Decimal(data["flow_rate"]) if data["flow_rate"] is not None else None,
            voltage=Decimal(data["voltage"]) if data["voltage"] is not None else None,
            current=Decimal(data["current"]) if data["current"] is not None else None,
            processing_timestamp=datetime.fromisoformat(data["processing_timestamp"]) if "processing_timestamp" in data else datetime.now(),
        )


class MetricMessage(BaseModel):
    timestamp: datetime
    metric: str
    value: Decimal
    rig_id: str

    @pydantic.field_validator("metric")
    @classmethod
    def validate_metric(cls, v: str) -> str:
        if v not in KNOWN_METRICS:
            raise ValueError(f"Unknown metric '{v}'. Must be one of: {sorted(KNOWN_METRICS)}")
        return v


class TransformationContext:
    def __init__(self, target_url: str, window_seconds: float, state_file: str = "", max_buffer_size: int = 10000, processing_window_seconds: float = 120.0):
        self.target_url = target_url
        self.window_seconds = window_seconds
        self.state_file = state_file
        self.max_buffer_size = max_buffer_size
        self.processing_window_seconds = processing_window_seconds
        # buffer: (rig_id, timestamp) -> HorizontalMetric
        self._buffer: dict[tuple[str, datetime], HorizontalMetric] = {}
        if self.state_file:
            self._load_state()

    def _load_state(self) -> None:
        if not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file, "r") as f:
                records = json.load(f)
            for record in records:
                hm = HorizontalMetric.from_json(record)
                self._buffer[(hm.rig_id, hm.timestamp)] = hm
            logger.info("Restored %d buffered window(s) from %s", len(self._buffer), self.state_file)
        except Exception as exc:
            logger.error("Failed to load state from %s: %s", self.state_file, exc)

    def _save_state(self) -> None:
        logger.info("Saving state with %d buffered window(s)...", len(self._buffer))
        if not self.state_file:
            return
        try:
            with open(self.state_file, "w") as f:
                json.dump([hm.to_json() for hm in self._buffer.values()], f)

            logger.info("Saved %d buffered window(s) to %s", len(self._buffer), self.state_file)
        except Exception as exc:
            logger.error("Failed to save state to %s: %s", self.state_file, exc)

    def process_received_message(self, msg: MetricMessage) -> bool:
        key = (msg.rig_id, msg.timestamp)

        if key not in self._buffer:
            if len(self._buffer) >= self.max_buffer_size:
                logger.warning("Buffer full (%d/%d), rejecting rig_id=%s timestamp=%s",
                               len(self._buffer), self.max_buffer_size, msg.rig_id, msg.timestamp)
                self._flush_needed(msg.timestamp) 
                return False
            
            self._buffer[key] = HorizontalMetric(rig_id=msg.rig_id, timestamp=msg.timestamp)
            logger.info("New window opened for rig_id=%s timestamp=%s", msg.rig_id, msg.timestamp)

        horizontal_metric = self._buffer[key]
        horizontal_metric.set_metric(msg.metric, msg.value)
        self._flush_needed(msg.timestamp)
        return True

    def _flush_needed(self, current_timestamp: datetime) -> None:
        now = datetime.now()
        needed_to_flush = [
            key for key, horizontal_metric in self._buffer.items()
            if (current_timestamp - horizontal_metric.timestamp).total_seconds() >= self.window_seconds
            or (now - horizontal_metric.processing_timestamp).total_seconds() >= self.processing_window_seconds
            or horizontal_metric.is_complete
        ]
        for key in needed_to_flush:
            result = self._flush(key)
            if result == SendResult.FailedTimeout:
                logger.warning("Target timed out — aborting remaining %d flush(es) in this batch.",
                               needed_to_flush.index(key) - len(needed_to_flush) + 1)
                break

    def _flush(self, key: tuple[str, str]) -> SendResult:
        horizontal_metric = self._buffer.pop(key, None)

        if horizontal_metric is None:
            return SendResult.Succeed

        result = self._send_to_target(horizontal_metric.to_payload())

        if result != SendResult.Succeed:
            logger.warning("Failed to send window for rig_id=%s timestamp=%s (%s). Re-buffering.",
                           horizontal_metric.rig_id, horizontal_metric.timestamp, result.value)
            self._buffer[key] = horizontal_metric  # re-buffer for retry on next flush

        return result

    def _send_to_target(self, payload: dict) -> SendResult:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            self.target_url,
            data=body,
            headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info("Forwarded to sink: status=%d", resp.status)
            return SendResult.Succeed
        except urllib.error.HTTPError as exc:
            logger.error("Target returned HTTP %d: %s", exc.code, exc.read().decode(errors="replace"))
            return SendResult.FailedOther
        except (socket.timeout, TimeoutError) as exc:
            logger.warning("Target timed out: %s", exc)
            return SendResult.FailedTimeout
        except Exception as exc:
            logger.error("Failed to forward to target: %s", exc)
            return SendResult.FailedOther

    def shutdown(self) -> None:
        self._save_state()

class TransformationHandler(BaseHTTPRequestHandler):
    def __init__(self, ctx: TransformationContext, *args, **kwargs):
        self.ctx = ctx
        super().__init__(*args, **kwargs)

    def log_message(self, fmt, *args):
        logger.info(fmt, *args)

    def send_json(self, status: int, body: dict):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        logger.info("do_POST thread_id=%d", id(self))
        if self.path != "/measures":
            self.send_json(404, {"error": "Not found. Use POST /measures"})
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON: %s", exc)
            self.send_json(400, {"error": f"Invalid JSON: {exc}"})
            return

        try:
            msg = MetricMessage.model_validate(data)
        except Exception as exc:
            logger.error("Validation error: %s", exc)
            self.send_json(400, {"error": str(exc)})
            return

        if not self.ctx.process_received_message(msg):
            self.send_json(429, {"error": "Buffer full, try again later"})
            return
        logger.info("Received metric: rig_id=%s timestamp=%s metric=%s value=%s",
                    msg.rig_id, msg.timestamp, msg.metric, msg.value)
        self.send_json(202, {"ok": True, "buffered": True})


def main():
    parser = argparse.ArgumentParser(
        description="Transformation service: collect vertical metrics and forward horizontal rows."
    )
    parser.add_argument(
        "--target-url", required=True,
        help="URL of the sink endpoint, e.g. http://sink:8080/measures",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Address to listen on (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8081, help="Port to listen on (default: 8081)")
    parser.add_argument(
        "--window", type=float, default=30.0,
        help="Seconds of inactivity before flushing a (rig_id, timestamp) window (default: 30) in a metric timestamp",
    )
    parser.add_argument(
        "--state-file", default="",
        help="Path to a JSON file for persisting the buffer across restarts (default: disabled)",
    )
    parser.add_argument(
        "--max-buffer-size", type=int, default=10000,
        help="Maximum number of (rig_id, timestamp) windows in the buffer; must be > 0 (default: 100)",
    )
    parser.add_argument(
        "--processing-window", type=float, default=120.0,
        help="Seconds since first message received before flushing a window by wall-clock time (default: 120)",
    )
    args = parser.parse_args()

    if args.max_buffer_size <= 0:
        parser.error("--max-buffer-size must be greater than 0")

    if args.window <= 0:
        parser.error("--window must be greater than 0")

    if args.processing_window <= 0:
        parser.error("--processing-window must be greater than 0")
        
    ctx = TransformationContext(
        target_url=args.target_url,
        window_seconds=args.window,
        state_file=args.state_file,
        max_buffer_size=args.max_buffer_size,
        processing_window_seconds=args.processing_window,
    )

    handler = partial(TransformationHandler, ctx)
    server = HTTPServer((args.host, args.port), handler)

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, lambda *_: ctx.shutdown())
    
    logger.info(
        "Transformation listening on %s:%d  window=%.1fs  target=%s",
        args.host, args.port, args.window, args.target_url,
    )

    try:
        server.serve_forever()
    finally:
        logger.info("Saving state and closing server.")
        ctx.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
