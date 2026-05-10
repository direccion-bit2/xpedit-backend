"""
Locust CI smoke — 30 s probe of /health/loop.

Purpose: catch event-loop blocking regressions in PRs before merge.
The 5 may 2026 incident was caused by sync supabase-py calls inside async
handlers; this smoke would have failed because /health/loop yielded_ms
spikes whenever the loop is starved by sync code anywhere in the worker.

Why this and not the autocomplete burst:
  - CI has no real Supabase / Google API keys, so /places/* would 502.
  - /health/loop is auth-free, dependency-free, and exposes the exact
    metric that distinguishes "blocked" from "slow" — yielded_ms.

Usage (matches the GitHub Action):

    LOCUST_HOST=http://localhost:8001 \\
      locust -f loadtests/locustfile_ci_smoke.py \\
        --headless --users 5 --spawn-rate 5 --run-time 30s \\
        --csv smoke

    python loadtests/assert_thresholds.py smoke_stats.csv

CI thresholds (assert_thresholds.py SLOs apply):
  - p95  < 1500 ms
  - p99  < 3000 ms
  - error rate < 1 %
A failure marks every yielded_ms > 100 ms as a request error so the
CSV picks it up — locust counts it in error rate, not just latency.
"""

from __future__ import annotations

import os

from locust import HttpUser, between, task

HOST = os.getenv("LOCUST_HOST", "http://localhost:8001")


class SmokeUser(HttpUser):
    """One virtual driver hammering /health/loop every ~1 s.

    Five users × 30 s = 150 probes. Enough samples for p95/p99 to be
    meaningful, short enough that the CI step stays under a minute.
    """

    wait_time = between(0.5, 1.5)
    host = HOST

    @task
    def health_loop(self):
        with self.client.get(
            "/health/loop",
            name="/health/loop",
            catch_response=True,
        ) as r:
            if r.status_code != 200:
                r.failure(f"http {r.status_code}")
                return
            try:
                payload = r.json()
            except Exception:
                r.failure("not json")
                return
            lag = float(payload.get("yielded_ms", 0))
            # Treat >100 ms event-loop lag as a hard failure — that's exactly
            # the "the loop is blocked by sync code" signal we want to catch.
            if lag > 100:
                r.failure(f"event loop blocked {lag:.1f}ms")
