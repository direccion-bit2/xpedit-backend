"""
Locust scenario A — autocomplete burst.

Reproduces the 5 may 2026 incident: 22 drivers FORCE-OTA reload at once,
each types 5 letters in 5s, then idle 30s. Targets the backend
`/places/autocomplete` endpoint.

Usage (headless, against staging or local backend):

    cd backend
    pip install locust  # one-time

    # Local backend (must run with --workers 1 to reproduce single-worker bug):
    LOCUST_HOST=http://localhost:8000 LOCUST_JWT=$(cat .jwt-staging-test) \\
      locust -f loadtests/locustfile_autocomplete_burst.py \\
        --headless --users 22 --spawn-rate 22 --run-time 2m \\
        --csv stress --html stress-report.html

    # Staging backend:
    LOCUST_HOST=https://web-staging-5f41.up.railway.app LOCUST_JWT=... \\
      locust -f loadtests/locustfile_autocomplete_burst.py ...

The CSV thresholds are validated by `loadtests/assert_thresholds.py`.

Targets after Sprint 1 (pg_trgm + asyncio.to_thread + httpx singleton):
  - p95 < 1500 ms
  - error rate < 1 %
  - any /health/loop yielded_ms > 100 ms = warning

Pre-Sprint 1 baseline (the bug as it manifested):
  - p99 ≥ 8000 ms intermittent
  - 5-30 % errors when worker stalled
"""

from __future__ import annotations

import os
import random
import string
import uuid

from locust import HttpUser, between, task

# Spanish address prefixes that match a wide chunk of the address_cache
# trigram index — exercises the GIN index path post-migration.
QUERY_PREFIXES = [
    "calle ",
    "avenida ",
    "plaza ",
    "paseo ",
    "ronda ",
    "carretera ",
    "camino ",
    "carrer ",
    "rua ",
]

JWT = os.getenv("LOCUST_JWT", "")
HOST = os.getenv("LOCUST_HOST", "http://localhost:8000")


class AutocompleteUser(HttpUser):
    """Each virtual user simulates one driver typing a Spanish address.

    Per-user wait_time is a random 25-35 s pause so 22 drivers don't
    all hit at exactly the same instant — the realistic FORCE-OTA pattern
    is a tight burst, then drift apart.
    """

    wait_time = between(25, 35)
    host = HOST

    def on_start(self):
        if not JWT:
            print("[locust] LOCUST_JWT empty; requests will get 401")
        self.headers = {"Authorization": f"Bearer {JWT}"} if JWT else {}
        # Each driver gets a stable session token for the whole run, mimicking
        # the Google Places billing pattern (one session per address picked).
        self.session_token = str(uuid.uuid4())

    @task(weight=10)
    def keystroke_burst(self):
        """5 keystrokes in ~5s. Each one is a real backend GET."""
        prefix = random.choice(QUERY_PREFIXES)
        for i in range(1, 6):
            q = prefix + "".join(
                random.choices(string.ascii_lowercase, k=i)
            )
            with self.client.get(
                "/places/autocomplete",
                params={"input": q, "sessiontoken": self.session_token},
                headers=self.headers,
                name="/places/autocomplete",
                catch_response=True,
            ) as r:
                # Treat anything > 2s as a failure even if HTTP 200 — that's
                # the user-perceived "se queda cargando" symptom.
                if r.elapsed.total_seconds() > 2.0:
                    r.failure(f"slow {r.elapsed.total_seconds():.2f}s")
                elif r.status_code != 200:
                    r.failure(f"http {r.status_code}")

    @task(weight=1)
    def health_loop(self):
        """Cheap event-loop-lag probe. Will surface blocking inside the
        worker independently of the autocomplete path."""
        with self.client.get(
            "/health/loop",
            name="/health/loop",
            catch_response=True,
        ) as r:
            if r.status_code != 200:
                r.failure(f"http {r.status_code}")
                return
            try:
                lag = r.json().get("yielded_ms", 0)
            except Exception:
                r.failure("not json")
                return
            if lag > 100:
                r.failure(f"event loop blocked {lag:.1f}ms")
