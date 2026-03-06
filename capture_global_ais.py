#!/usr/bin/env python3
"""Capture AIS data from AISStream.io using a JSON config file.

Usage:
  export AISSTREAM_API_KEY="your_api_key"
  python capture_global_ais.py --config config.json
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Union


DEFAULT_CONFIG_PATH = "config.json"


def normalize_message_payload(payload: Union[str, bytes]) -> str:
def normalize_message_payload(payload: str | bytes) -> str:
    """Normalize websocket payload to text for file output and JSON parsing."""
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    return payload

DEFAULT_CONFIG_PATH = "config.json"

 main

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


def load_config(path: str) -> Dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(
            "Config file '{}' not found. Copy and edit config.json.example first.".format(
                path
            )
        )

    with config_path.open("r", encoding="utf-8") as file_obj:
        config = json.load(file_obj)

    required = ["capture_minutes", "bounding_boxes", "output_jsonl", "output_csv"]
    missing = [key for key in required if key not in config]
    if missing:
        raise ValueError("Missing required config key(s): {}".format(", ".join(missing)))

    if config["capture_minutes"] <= 0:
        raise ValueError("capture_minutes must be greater than 0")

    return config


def extract_csv_row(message: Dict[str, Any], received_at: str) -> Dict[str, Any]:
    metadata = message.get("MetaData", {})
    msg_wrapper = message.get("Message", {})

    msg_type = ""
    msg_payload = {}
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
        print("Error loading config: {}".format(exc), file=sys.stderr)
        return 1

    capture_minutes = float(config["capture_minutes"])

    try:
        import websocket as ws_client
    except ModuleNotFoundError:
        print(
            "Error: missing dependency `websocket-client`. Install with `pip install -r requirements.txt`.",
            file=sys.stderr,
        )
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
        "Starting capture for {} minute(s).\n"
        "JSONL: {}\n"
        "CSV:   {}\n"
        "BoundingBoxes: {}\n"
        "This script only runs when you execute it manually.".format(
            capture_minutes,
            output_jsonl,
            output_csv,
            bounding_boxes,
        )
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

    with open(output_jsonl, "w", encoding="utf-8") as jsonl_out, open(
        output_csv, "w", newline="", encoding="utf-8"
    ) as csv_out:
        csv_writer = csv.DictWriter(csv_out, fieldnames=csv_columns)
        csv_writer.writeheader()

        ws = ws_client.create_connection("wss://stream.aisstream.io/v0/stream")
        try:
            ws.send(json.dumps(subscribe_message))

            while time.time() < end_time:
                timeout_remaining = max(1, int(end_time - time.time()))
                ws.settimeout(timeout_remaining)
                try:
                    message_raw = ws.recv()
                except ws_client.WebSocketTimeoutException:
                    break

                message_json = normalize_message_payload(message_raw)
 main
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

    print("Done. Captured {} message(s).".format(message_count))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
