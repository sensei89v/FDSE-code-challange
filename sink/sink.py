#!/usr/bin/env python3
"""
Sink tool: HTTP server that receives measurement data and stores it in PostgreSQL.

Usage:
    python sink.py --url <postgres-connection-url> [--host <host>] [--port <port>]

Example:
    python sink.py --url "postgresql://postgres:postgres@localhost:5432/fdse"

POST /measures
    Body (JSON object or array of objects) with fields:
        rig_id        - required
        timestamp     - required
        temp_inlet    - optional
        temp_outlet   - optional
        pressure      - optional
        flow_rate     - optional
        voltage       - optional
        current       - optional
"""

import argparse
import json
import logging
from datetime import datetime
from decimal import Decimal
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import psycopg2

from pydantic import BaseModel, model_validator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS measures (
    rig_id       VARCHAR(50)  NOT NULL,
    timestamp    TIMESTAMP    NOT NULL,
    temp_inlet   DECIMAL,
    temp_outlet  DECIMAL,
    pressure     DECIMAL,
    flow_rate    DECIMAL,
    voltage      DECIMAL,
    current      DECIMAL,
    UNIQUE (rig_id, timestamp)
);
"""

INSERT_SQL = """
INSERT INTO measures (rig_id, timestamp, temp_inlet, temp_outlet, pressure, flow_rate, voltage, current)
VALUES (%(rig_id)s, %(timestamp)s, %(temp_inlet)s, %(temp_outlet)s, %(pressure)s, %(flow_rate)s, %(voltage)s, %(current)s)
ON CONFLICT (rig_id, timestamp) DO UPDATE SET
    temp_inlet  = COALESCE(measures.temp_inlet,  EXCLUDED.temp_inlet),
    temp_outlet = COALESCE(measures.temp_outlet, EXCLUDED.temp_outlet),
    pressure    = COALESCE(measures.pressure,    EXCLUDED.pressure),
    flow_rate   = COALESCE(measures.flow_rate,   EXCLUDED.flow_rate),
    voltage     = COALESCE(measures.voltage,     EXCLUDED.voltage),
    current     = COALESCE(measures.current,     EXCLUDED.current)
RETURNING (xmax = 0) AS inserted;
"""

class Measure(BaseModel):
    rig_id: str
    timestamp: datetime
    temp_inlet: Optional[Decimal] = None
    temp_outlet: Optional[Decimal] = None
    pressure: Optional[Decimal] = None
    flow_rate: Optional[Decimal] = None
    voltage: Optional[Decimal] = None
    current: Optional[Decimal] = None

    @model_validator(mode="after")
    def at_least_one_metric(self) -> "Measure":
        METRIC_FIELDS = {"temp_inlet", "temp_outlet", "pressure", "flow_rate", "voltage", "current"}

        if all(getattr(self, f) is None for f in METRIC_FIELDS):
            raise ValueError("At least one metric field must be non-null")
        return self


class SinkContext:
    def __init__(self, db_url: str):
        self._conn = psycopg2.connect(db_url)

    def _ensure_connection(self):
        if self._conn.closed:
            raise RuntimeError("Database connection is closed")
        
    def ensure_table(self):
        self._ensure_connection()
        with self._conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        self._conn.commit()
        logger.info("Table 'measures' is ready.")

    def insert_record(self, record: Measure) -> bool:
        """Insert or patch a record. Returns True if inserted, False if updated."""
        self._ensure_connection()
        with self._conn.cursor() as cur:
            cur.execute(INSERT_SQL, record.model_dump())
            row = cur.fetchone()
        self._conn.commit()
        return bool(row[0]) if row else True

    def close(self):
        if not self._conn.closed:
            self._conn.close()


class MeasuresHandler(BaseHTTPRequestHandler):
    def __init__(self, ctx: SinkContext, *args, **kwargs):
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
        if self.path != "/measures":
            self.send_json(404, {"error": "Not found. Use POST /measures"})
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        try:
            data = json.loads(raw)
            logger.info("Received data: %s", data)
        except json.JSONDecodeError as exc:
            logger.error(f"Invalid JSON: {exc}")
            self.send_json(400, {"error": f"Invalid JSON: {exc}"})
            return

        try:
            record = Measure.model_validate(data) 
        except Exception as exc:
            logger.error(f"Validation error: {exc}")
            self.send_json(400, {"error": str(exc)})
            return

        try:
            inserted = self.ctx.insert_record(record)
            if inserted:
                logger.info("Inserted record: rig_id=%s, timestamp=%s", record.rig_id, record.timestamp)
            else:
                logger.info("Patched null fields for rig_id=%s, timestamp=%s", record.rig_id, record.timestamp)
            
            self.send_json(200, {"ok": True})
                
        except Exception as exc:
            logger.exception(f"Database error: {exc}")
            self.send_json(500, {"error": str(exc)})


def main():
    parser = argparse.ArgumentParser(
        description="HTTP sink: receive measurements and store them in PostgreSQL."
    )
    parser.add_argument(
        "--db-url", required=True,
        help="PostgreSQL connection URL, e.g. postgresql://user:pass@host:5432/db",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Address to listen on (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on (default: 8080)")
    args = parser.parse_args()

    ctx = SinkContext(db_url=args.db_url)
    ctx.ensure_table()

    handler = partial(MeasuresHandler, ctx)
    server = HTTPServer((args.host, args.port), handler)
    logger.info("Sink listening on %s:%d", args.host, args.port)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        ctx.close()
        server.server_close()


if __name__ == "__main__":
    main()
