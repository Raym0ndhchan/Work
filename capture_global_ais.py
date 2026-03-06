#!/usr/bin/env python3
"""Capture AIS data from AISStream.io using a JSON config file.

Usage:
  export AISSTREAM_API_KEY="your_api_key"
  python capture_global_ais.py --config config.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any



DEFAULT_CONFIG_PATH = "config.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture AIS data from AISStream.io using a JSON config file."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config JSON file (default: {DEFAULT_CONFIG_PATH}).",
    )
    return parser.parse_args()


def load_config(path: str) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file '{path}' not found. Copy and edit config.json.example first."
        )

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    required = ["capture_minutes", "bounding_boxes", "output_jsonl", "output_csv"]
    missing = [key for key in required if key not in config]
    if missing:
        raise ValueError(f"Missing required config key(s): {', '.join(missing)}")

    if config["capture_minutes"] <= 0:
        raise ValueError("capture_minutes must be greater than 0")

    return config


def extract_csv_row(message: dict[str, Any], received_at: str) -> dict[str, Any]:
    metadata = message.get("MetaData", {})
    msg_wrapper = message.get("Message", {})

    msg_type = ""
    msg_payload: dict[str, Any] = {}
    if isinstance(msg_wrapper, dict) and msg_wrapper:
        msg_type = next(iter(msg_wrapper.keys()))
        payload = msg_wrapper.get(msg_type, {})
        msg_payload = payload if isinstance(payload, dict) else {}

    return {
        "received_at": received_at,
        "message_type": msg_type,
        "mmsi": metadata.get("MMSI") or msg_payload.get("UserID"),
        "latitude": msg_payload.get("Latitude"),
        "longitude": msg_payload.get("Longitude"),
        "sog": msg_payload.get("Sog"),
        "cog": msg_payload.get("Cog"),
        "heading": msg_payload.get("TrueHeading"),
        "ship_name": metadata.get("ShipName"),
        "raw_json": json.dumps(message, separators=(",", ":")),
    }


def main() -> int:
    args = parse_args()
    api_key = os.environ.get("AISSTREAM_API_KEY")

    if not api_key:
        print("Error: AISSTREAM_API_KEY environment variable is not set.", file=sys.stderr)
        return 1

    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"Error loading config: {exc}", file=sys.stderr)
        return 1

    capture_minutes = float(config["capture_minutes"])

    try:
        import websocket as ws_client
    except ModuleNotFoundError:
        print("Error: missing dependency `websocket-client`. Install with `pip install -r requirements.txt`.", file=sys.stderr)
        return 1
    output_jsonl = str(config["output_jsonl"])
    output_csv = str(config["output_csv"])
    bounding_boxes = config["bounding_boxes"]

    end_time = time.time() + (capture_minutes * 60)

    subscribe_message = {
        "APIKey": api_key,
        "BoundingBoxes": bounding_boxes,
    }

    print(
        f"Starting capture for {capture_minutes} minute(s).\n"
        f"JSONL: {output_jsonl}\n"
        f"CSV:   {output_csv}\n"
        f"BoundingBoxes: {bounding_boxes}\n"
        "This script only runs when you execute it manually."
    )

    csv_columns = [
        "received_at",
        "message_type",
        "mmsi",
        "latitude",
        "longitude",
        "sog",
        "cog",
        "heading",
        "ship_name",
        "raw_json",
    ]

    message_count = 0

    with (
        open(output_jsonl, "w", encoding="utf-8") as jsonl_out,
        open(output_csv, "w", newline="", encoding="utf-8") as csv_out,
    ):
        csv_writer = csv.DictWriter(csv_out, fieldnames=csv_columns)
        csv_writer.writeheader()

        ws = ws_client.create_connection("wss://stream.aisstream.io/v0/stream")
        try:
            ws.send(json.dumps(subscribe_message))

            while time.time() < end_time:
                timeout_remaining = max(1, int(end_time - time.time()))
                ws.settimeout(timeout_remaining)
                try:
                    message_json = ws.recv()
                except ws_client.WebSocketTimeoutException:
                    break

                received_at = datetime.now(timezone.utc).isoformat()
                jsonl_out.write(message_json + "\n")

                try:
                    message = json.loads(message_json)
                except json.JSONDecodeError:
                    message = {"raw": message_json}
                csv_writer.writerow(extract_csv_row(message, received_at))

                message_count += 1

        finally:
            ws.close()

    print(f"Done. Captured {message_count} message(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
