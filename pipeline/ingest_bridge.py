from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any

import httpx

LOGGER = logging.getLogger(__name__)


def ingest_jsonl(
    file_path: str | Path,
    api_url: str = "http://api:8000",
    batch_size: int = 250,
) -> dict[str, int]:
    """Read pipeline-generated JSONL events and ingest them into the API database.

    Returns a summary dict with accepted, rejected, duplicate counts.
    """
    jsonl_path = Path(file_path)
    if not jsonl_path.exists():
        LOGGER.warning("ingest_bridge_no_file", extra={"path": str(jsonl_path)})
        return {"accepted": 0, "rejected": 0, "duplicates": 0, "total": 0}

    total = 0
    accepted = 0
    rejected = 0
    duplicates = 0
    batch: list[dict[str, Any]] = []

    try:
        with jsonl_path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    LOGGER.error(
                        "ingest_bridge_json_parse_error",
                        extra={"line": line_number, "error": str(exc)},
                    )
                    rejected += 1
                    continue
                if not isinstance(event, dict):
                    LOGGER.error(
                        "ingest_bridge_line_not_object",
                        extra={"line": line_number},
                    )
                    rejected += 1
                    continue
                batch.append(event)
                total += 1

                if len(batch) >= batch_size:
                    result = _post_batch(api_url, batch)
                    accepted += result.get("accepted", 0)
                    rejected += result.get("rejected", 0)
                    duplicates += result.get("duplicates", 0)
                    batch = []

        # Flush remaining
        if batch:
            result = _post_batch(api_url, batch)
            accepted += result.get("accepted", 0)
            rejected += result.get("rejected", 0)
            duplicates += result.get("duplicates", 0)

    except Exception as exc:
        LOGGER.exception("ingest_bridge_failed", extra={"error": str(exc)})
        raise

    LOGGER.info(
        "ingest_bridge_complete",
        extra={
            "total": total,
            "accepted": accepted,
            "rejected": rejected,
            "duplicates": duplicates,
        },
    )
    return {"total": total, "accepted": accepted, "rejected": rejected, "duplicates": duplicates}


def _post_batch(api_url: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    endpoint = f"{api_url.rstrip('/')}/events/ingest"
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(endpoint, json={"events": events})
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            LOGGER.error("ingest_bridge_non_object_response", extra={"url": endpoint})
            return {"accepted": 0, "rejected": len(events), "duplicates": 0}
    except httpx.HTTPStatusError as exc:
        LOGGER.error(
            "ingest_bridge_http_error",
            extra={
                "url": endpoint,
                "status": exc.response.status_code,
                "body": exc.response.text[:500],
            },
        )
        return {"accepted": 0, "rejected": len(events), "duplicates": 0}
    except httpx.RequestError as exc:
        LOGGER.error(
            "ingest_bridge_connection_error",
            extra={"url": endpoint, "error": str(exc)},
        )
        return {"accepted": 0, "rejected": len(events), "duplicates": 0}


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest pipeline-generated JSONL events into the API."
    )
    parser.add_argument(
        "--file",
        default="data/events.jsonl",
        help="Path to pipeline-generated JSONL events file.",
    )
    parser.add_argument("--api-url", default="http://api:8000")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    LOGGER.info("ingest_bridge_started", extra={"file": args.file, "api_url": args.api_url})
    report = ingest_jsonl(
        file_path=args.file,
        api_url=args.api_url,
        batch_size=args.batch_size,
    )
    LOGGER.info("ingest_bridge_report", extra=report)
    return 0 if report.get("rejected", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())