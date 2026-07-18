#!/usr/bin/env python3
"""Smoke-test go-ingestion-api HTTP endpoints.

Usage:
  python3 scripts/test_api.py
  python3 scripts/test_api.py --base-url http://157.245.246.228
  BASE_URL=http://localhost python3 scripts/test_api.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any


DEFAULT_BASE = os.environ.get("BASE_URL", "http://157.245.246.228")


class APIError(RuntimeError):
    pass


def request(
    method: str,
    url: str,
    *,
    body: Any | None = None,
    timeout: float = 30.0,
) -> tuple[int, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            payload: Any = json.loads(raw) if raw else None
            return resp.status, payload
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            payload = raw
        raise APIError(f"{method} {url} -> HTTP {e.code}: {payload}") from e
    except urllib.error.URLError as e:
        raise APIError(f"{method} {url} -> {e.reason}") from e


def step(title: str) -> None:
    print(f"\n== {title} ==")


def expect(cond: bool, msg: str) -> None:
    if not cond:
        raise APIError(msg)


def test_health(base: str) -> None:
    step("GET /health")
    status, body = request("GET", f"{base}/health")
    print(json.dumps(body, indent=2))
    expect(status == 200, f"health status={status}")
    expect(body.get("status") == "healthy", f"unexpected health body: {body}")
    expect(body.get("plane") == "control", f"unexpected plane: {body}")


def test_pool(base: str) -> None:
    step("GET /api/v1/pool")
    status, body = request("GET", f"{base}/api/v1/pool")
    print(json.dumps(body, indent=2))
    expect(status == 200, f"pool status={status}")
    expect(body.get("plane") == "data", f"unexpected plane: {body}")
    for key in ("queue_length", "queue_cap", "retry_queue_length", "max_workers"):
        expect(key in body, f"missing {key} in pool response")


def test_ingest_and_results(base: str, timeout_s: float) -> str:
    step("POST /api/v1/ingest")
    prompts = ["hello from python", "second prompt", {"prompt": "object form"}]
    status, body = request("POST", f"{base}/api/v1/ingest", body=prompts)
    print(json.dumps(body, indent=2))
    expect(status == 202, f"ingest status={status}, want 202")
    expect(body.get("status") == "accepted", f"unexpected ingest: {body}")
    batch_id = body.get("batch_id")
    expect(isinstance(batch_id, str) and batch_id, "missing batch_id")
    expect(body.get("total") == 3, f"total={body.get('total')}, want 3")

    step(f"GET /api/v1/ingest/batches/{batch_id} (poll)")
    deadline = time.monotonic() + timeout_s
    last: Any = None
    while time.monotonic() < deadline:
        status, last = request("GET", f"{base}/api/v1/ingest/batches/{batch_id}")
        print(json.dumps(last, indent=2))
        expect(status == 200, f"poll status={status}")
        if last.get("status") == "completed":
            break
        if last.get("status") == "failed":
            raise APIError(f"batch failed: {last}")
        time.sleep(0.5)
    else:
        raise APIError(f"timeout waiting for batch completion: {last}")

    expect(last.get("processed") == 3, f"processed={last.get('processed')}, want 3")
    expect(last.get("failed", 0) == 0, f"failed={last.get('failed')}")

    step(f"GET /api/v1/ingest/batches/{batch_id}/results")
    status, results = request("GET", f"{base}/api/v1/ingest/batches/{batch_id}/results")
    print(json.dumps(results, indent=2))
    expect(status == 200, f"results status={status}")
    expect(results.get("batch_id") == batch_id, "batch_id mismatch in results")
    expect(results.get("count") == 3, f"count={results.get('count')}, want 3")
    inferences = results.get("inferences") or []
    expect(len(inferences) == 3, f"len(inferences)={len(inferences)}")
    for item in inferences:
        expect("prompt" in item and "inference" in item, f"bad inference item: {item}")
        expect(item["inference"], "empty inference")

    return batch_id


def test_not_found(base: str) -> None:
    step("GET unknown batch (expect 404)")
    url = f"{base}/api/v1/ingest/batches/00000000-0000-0000-0000-000000000000"
    try:
        request("GET", url)
        raise APIError("expected 404 for unknown batch")
    except APIError as e:
        if "HTTP 404" not in str(e):
            raise
        print(str(e))


def main() -> int:
    parser = argparse.ArgumentParser(description="Test go-ingestion-api endpoints")
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE,
        help=f"API base URL (default: {DEFAULT_BASE} or $BASE_URL)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Seconds to wait for batch completion (default: 60)",
    )
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    print(f"BASE_URL={base}")
    try:
        test_health(base)
        test_pool(base)
        batch_id = test_ingest_and_results(base, args.timeout)
        test_pool(base)
        test_not_found(base)
    except APIError as e:
        print(f"\nFAIL: {e}", file=sys.stderr)
        return 1

    print(f"\nOK — all endpoint checks passed (batch_id={batch_id})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
