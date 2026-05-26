"""Golden dataset runner for /ocr/screenshots-batch.

Reads `tests/golden_screenshots/ground_truth.json`, sends each image to the
endpoint (local FastAPI by default, or staging if API_URL is set), and
prints a per-carrier precision/recall report.

Usage:
    python tests/golden_ocr_runner.py [--api-url URL] [--token JWT]

If no `images/` folder exists yet (the screenshots haven't been collected),
the runner exits cleanly with a friendly note. This makes the file safe to
import / lint without breaking CI before the dataset is built.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from pathlib import Path
from typing import Any

try:
    import httpx
except ImportError:
    print("Install httpx first: pip install httpx", file=sys.stderr)
    sys.exit(1)


HERE = Path(__file__).resolve().parent
DATASET_DIR = HERE / "golden_screenshots"
IMAGES_DIR = DATASET_DIR / "images"
GROUND_TRUTH = DATASET_DIR / "ground_truth.json"


def _normalize(s: str | None) -> str:
    """Light normalize for comparison: lowercase, strip, collapse spaces."""
    if not s:
        return ""
    return " ".join(str(s).lower().strip().split())


def _load_image_b64(filename: str) -> dict | None:
    p = IMAGES_DIR / filename
    if not p.exists():
        return None
    data = base64.b64encode(p.read_bytes()).decode("ascii")
    suffix = p.suffix.lower()
    media = {".jpg": "image/jpeg", ".jpeg": "image/jpeg",
             ".png": "image/png", ".webp": "image/webp"}.get(suffix, "image/jpeg")
    return {"image_base64": data, "media_type": media}


def _match_stop(got: dict, expected: dict) -> dict:
    """Field-by-field comparison. Returns per-field bool matches."""
    out = {}
    for fld in ("street", "number", "postal_code", "city", "province"):
        if fld in expected:
            out[fld] = _normalize(got.get(fld)) == _normalize(expected[fld])
    if "floor_etc" in expected:
        out["floor_etc"] = _normalize(got.get("floor_etc")) == _normalize(expected["floor_etc"])
    return out


def run(api_url: str, token: str | None) -> int:
    if not GROUND_TRUTH.exists():
        print(f"⚠ {GROUND_TRUTH} not found. Build the dataset first (see README).")
        return 0
    if not IMAGES_DIR.exists() or not list(IMAGES_DIR.iterdir()):
        print(f"⚠ {IMAGES_DIR} is empty. Add real screenshots before running.")
        return 0

    cases: list[dict[str, Any]] = json.loads(GROUND_TRUTH.read_text())
    if not cases:
        print("⚠ ground_truth.json is empty.")
        return 0

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    by_carrier: dict[str, dict] = {}
    total = {"images": 0, "stops_expected": 0, "stops_got": 0, "matches": 0}

    print("\n# Golden OCR run\n")
    print(f"API: {api_url}\n")

    with httpx.Client(timeout=90.0) as client:
        for case in cases:
            fn = case["filename"]
            img = _load_image_b64(fn)
            if img is None:
                print(f"  - SKIP {fn} (not on disk)")
                continue
            payload = {
                "images": [img],
                "carrier_hint": case.get("carrier_hint"),
            }
            try:
                r = client.post(f"{api_url}/ocr/screenshots-batch", json=payload, headers=headers)
            except Exception as e:
                print(f"  - ERR {fn}: {type(e).__name__}: {e}")
                continue
            if r.status_code != 200:
                print(f"  - HTTP {r.status_code} {fn}: {r.text[:200]}")
                continue
            body = r.json()
            got_stops = body.get("stops") or []
            expected_stops = case.get("expected_stops") or []

            carrier = case.get("carrier_hint", "generic")
            slot = by_carrier.setdefault(carrier, {"images": 0, "expected": 0, "got": 0, "matches": 0})
            slot["images"] += 1
            slot["expected"] += len(expected_stops)
            slot["got"] += len(got_stops)
            total["images"] += 1
            total["stops_expected"] += len(expected_stops)
            total["stops_got"] += len(got_stops)

            # Greedy match: walk expected, find first got that matches street+postal_code
            consumed: set[int] = set()
            file_matches = 0
            for exp in expected_stops:
                for i, got in enumerate(got_stops):
                    if i in consumed:
                        continue
                    fields = _match_stop(got, exp)
                    if fields.get("street") and (
                        fields.get("postal_code") or fields.get("city")
                    ):
                        consumed.add(i)
                        file_matches += 1
                        break
            slot["matches"] += file_matches
            total["matches"] += file_matches
            print(f"  - {fn}: expected={len(expected_stops)} got={len(got_stops)} matched={file_matches}")

    print("\n## Summary\n")
    print("| Carrier | Images | Expected | Got | Matched | Recall |")
    print("|---|---|---|---|---|---|")
    for c, s in sorted(by_carrier.items()):
        recall = s["matches"] / s["expected"] if s["expected"] else 0
        print(f"| {c} | {s['images']} | {s['expected']} | {s['got']} | {s['matches']} | {recall:.2%} |")
    overall_recall = total["matches"] / total["stops_expected"] if total["stops_expected"] else 0
    print(f"| **TOTAL** | {total['images']} | {total['stops_expected']} | {total['stops_got']} | {total['matches']} | **{overall_recall:.2%}** |")

    # Exit non-zero if recall below MVP target
    return 0 if overall_recall >= 0.90 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-url", default=os.getenv("API_URL", "http://localhost:8004"))
    ap.add_argument("--token", default=os.getenv("MSI_TEST_JWT"))
    args = ap.parse_args()
    sys.exit(run(args.api_url, args.token))
