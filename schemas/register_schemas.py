"""Register protobuf event schema in Redpanda Schema Registry."""
import argparse
import json
from pathlib import Path

import requests

SUBJECTS = [
    "livreurs-gps-value",
    "order-offers-v1-value",
    "order-events-v1-value",
    "context-signals-v1-value",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Register protobuf schema in Schema Registry")
    parser.add_argument("--registry", default="http://localhost:18081", help="Schema Registry base URL")
    parser.add_argument("--proto", default="schemas/copilot_events.proto", help="Path to proto schema")
    args = parser.parse_args()

    schema_text = Path(args.proto).read_text(encoding="utf-8")

    for subject in SUBJECTS:
        payload = {
            "schemaType": "PROTOBUF",
            "schema": schema_text,
        }
        response = requests.post(
            f"{args.registry}/subjects/{subject}/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps(payload),
            timeout=20,
        )
        if response.status_code >= 300:
            raise SystemExit(f"failed for {subject}: {response.status_code} {response.text}")
        version = response.json().get("id")
        print(f"registered subject={subject} id={version}")


if __name__ == "__main__":
    main()
