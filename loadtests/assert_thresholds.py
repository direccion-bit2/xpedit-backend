#!/usr/bin/env python3
"""
Assert Locust CSV results against Sprint 1 SLO thresholds.

Used as a pre-deploy gate. Exit code != 0 fails CI.

Usage:
    locust -f locustfile_autocomplete_burst.py --csv stress --headless ...
    python assert_thresholds.py stress_stats.csv
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

# Sprint 1 SLO thresholds. Tighten when Sprint 2 finishes.
P95_MAX_MS = 1500
P99_MAX_MS = 3000
ERROR_RATE_MAX = 0.01  # 1%


def main(csv_path: str) -> int:
    p = Path(csv_path)
    if not p.exists():
        print(f"FAIL: {csv_path} not found", file=sys.stderr)
        return 2

    failures: list[str] = []
    rows: list[dict[str, str]] = []
    with p.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    aggregated = next((r for r in rows if r.get("Name") == "Aggregated"), None)
    if not aggregated:
        print(
            "FAIL: no 'Aggregated' row in CSV — locust didn't run "
            "or csv is malformed",
            file=sys.stderr,
        )
        return 2

    n_requests = int(aggregated.get("Request Count", "0") or "0")
    n_failures = int(aggregated.get("Failure Count", "0") or "0")
    p95 = float(aggregated.get("95%", "0") or "0")
    p99 = float(aggregated.get("99%", "0") or "0")

    if n_requests < 10:
        failures.append(f"too few requests ({n_requests}) — locust didn't warm up")
    if n_requests > 0:
        err_rate = n_failures / n_requests
        if err_rate > ERROR_RATE_MAX:
            failures.append(
                f"error rate {err_rate:.2%} > {ERROR_RATE_MAX:.2%} "
                f"({n_failures}/{n_requests})"
            )
    if p95 > P95_MAX_MS:
        failures.append(f"p95 {p95:.0f}ms > {P95_MAX_MS}ms")
    if p99 > P99_MAX_MS:
        failures.append(f"p99 {p99:.0f}ms > {P99_MAX_MS}ms")

    if failures:
        print(f"❌ {len(failures)} threshold(s) violated:")
        for fail in failures:
            print(f"  - {fail}")
        return 1

    print(
        f"✅ thresholds passed: {n_requests} requests, "
        f"{n_failures} failures, p95={p95:.0f}ms, p99={p99:.0f}ms"
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: assert_thresholds.py <stress_stats.csv>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
