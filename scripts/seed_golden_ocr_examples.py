"""Idempotent seeder for golden OCR examples.

Reads `golden_ocr_seeds.json` and upserts each example into
`ocr_corrections` with `source='seed'`, `driver_id=NULL`,
`is_golden_example=TRUE`. The row UUID is derived deterministically from
`seed_key` via uuid5, so re-running the script never duplicates rows — it
updates the existing seeds in place if the JSON has changed.

Usage:
    SUPABASE_URL=... SUPABASE_SERVICE_KEY=... \\
        python3 scripts/seed_golden_ocr_examples.py [--env prod|staging] [--dry-run]

Required env (or pick up from .env via python-dotenv):
    SUPABASE_URL
    SUPABASE_SERVICE_KEY   # service_role, needed to bypass RLS
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

try:
    from supabase import create_client
except ImportError:
    print("ERROR: supabase-py not installed. Run from backend/ with the venv active.", file=sys.stderr)
    sys.exit(2)


SEEDS_FILE = Path(__file__).parent / "golden_ocr_seeds.json"


def load_seeds() -> dict[str, Any]:
    with SEEDS_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_row(example: dict[str, Any], namespace: uuid.UUID) -> dict[str, Any]:
    """Translate one JSON example into an ocr_corrections row.

    The row id is uuid5(namespace, seed_key) — deterministic and stable, so
    re-running the script over the same seed_key always updates the same
    row instead of creating duplicates.
    """
    seed_key = example["seed_key"]
    row_id = str(uuid.uuid5(namespace, seed_key))

    return {
        "id": row_id,
        "driver_id": None,
        "source": "seed",
        "model_name": example.get("model_name", "gemini-2.5-pro"),
        "prompt_version": example.get("prompt_version", "v1"),
        "model_extracted_address": example["model_extracted_address"],
        "model_extracted_parts": example["model_extracted_parts"],
        "model_confidence": example.get("model_confidence"),
        "user_final_address": example["user_final_address"],
        "user_action": example["user_action"],
        "was_corrected": example["was_corrected"],
        "corrected_fields": example.get("corrected_fields", []),
        "carrier_hint": example.get("carrier_hint"),
        "country_iso": example.get("country_iso"),
        "user_consented_training": True,
        "consent_version": "seed-v1",
        "redaction_status": "not_required",
        "is_golden_example": True,
        "is_in_training_set": True,
        "notes": f"{example.get('lesson', '')}\n\nseed_key={seed_key}\nuser_final_parts={json.dumps(example.get('user_final_parts', {}), ensure_ascii=False)}",
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--env",
        choices=("prod", "staging"),
        default=None,
        help="Which env-prefixed creds to use (SUPABASE_URL_PROD / SUPABASE_URL_STG). "
        "If omitted, falls back to plain SUPABASE_URL + SUPABASE_SERVICE_KEY.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print rows without writing.")
    args = ap.parse_args()

    if args.env == "prod":
        url = os.environ.get("SUPABASE_URL_PROD") or os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY_PROD") or os.environ.get("SUPABASE_SERVICE_KEY")
    elif args.env == "staging":
        url = os.environ.get("SUPABASE_URL_STG") or os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY_STG") or os.environ.get("SUPABASE_SERVICE_KEY")
    else:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not args.dry_run and (not url or not key):
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.", file=sys.stderr)
        return 2

    payload = load_seeds()
    namespace = uuid.UUID(payload["namespace_uuid"])
    examples: list[dict[str, Any]] = payload["examples"]
    print(f"Loaded {len(examples)} examples from {SEEDS_FILE.name}.")

    rows = [build_row(ex, namespace) for ex in examples]

    if args.dry_run:
        print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
        return 0

    assert url is not None and key is not None
    client = create_client(url, key)

    res = client.table("ocr_corrections").upsert(rows, on_conflict="id").execute()
    n = len(res.data) if getattr(res, "data", None) else 0
    print(f"Upserted {n} rows into ocr_corrections (env={args.env or 'default'}).")

    # Sanity check: count seeds in DB after upsert.
    check = (
        client.table("ocr_corrections")
        .select("id", count="exact")
        .eq("source", "seed")
        .execute()
    )
    total = getattr(check, "count", None)
    print(f"Total seed rows now in DB: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
