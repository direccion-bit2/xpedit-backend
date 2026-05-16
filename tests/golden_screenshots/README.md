# Golden screenshots — Multi-Screenshot Importer regression dataset

This directory holds **real screenshots** from Spanish carrier apps (CTT,
MRW, Seur, GLS, NACEX, Correos Express, TIPSA) plus their hand-labeled
ground truth, used to validate `/ocr/screenshots-batch` end-to-end.

## Folder layout

```
golden_screenshots/
├── README.md                 # this file
├── ground_truth.json         # array of ground-truth records (one per screenshot)
├── images/                   # raw .jpg/.png files referenced by ground_truth
│   ├── ctt_001.jpg
│   ├── mrw_002.jpg
│   └── …
└── _archive/                 # legacy / poor-quality samples kept for diversity
```

The `images/` folder is **gitignored** — Spanish carrier UIs may contain
delivery addresses that are real personal data. Only `ground_truth.json`
(redacted of the lat/lng of the actual addresses if needed) lives in git.

To populate it locally:

1. Take screenshots with Miguel's drivers' permission (or use anonymized
   ones from public delivery driver YouTube tutorials / forum posts).
2. Drop them in `images/`. Naming: `<carrier>_<seq>.jpg` (zero-padded).
3. Add an entry to `ground_truth.json` per the schema below.
4. Run the runner: `python tests/golden_ocr_runner.py`.

## ground_truth.json schema

```json
[
  {
    "filename": "ctt_001.jpg",
    "carrier_hint": "ctt",
    "expected_stops": [
      {
        "street": "Calle Mayor",
        "number": "5",
        "floor_etc": "4ºB",
        "postal_code": "11630",
        "city": "Arcos de la Frontera",
        "province": "Cádiz",
        "name_optional": "Juan García",
        "tracking_number_optional": "ABC123"
      }
    ],
    "notes": "Standard CTT list view, 1 stop visible, full address legible."
  }
]
```

Required fields per stop: `street`, `postal_code` OR `city`. Everything
else `name_*` and `tracking_*` are optional and only counted if visible.

## Targets (MVP → V1)

| Metric                            | MVP target | V1 target |
|-----------------------------------|------------|-----------|
| Recall (stops extracted / actual) | ≥0.90      | ≥0.98     |
| Precision street                  | ≥0.85      | ≥0.95     |
| Precision city                    | ≥0.90      | ≥0.97     |
| Precision postal_code             | ≥0.90      | ≥0.97     |
| Avg processing_ms (5-image batch) | ≤8000      | ≤6000     |

## Running the runner

```bash
# From repo root with .env loaded (GOOGLE_AI_API_KEY + GOOGLE_API_KEY)
python tests/golden_ocr_runner.py
```

The runner:
1. Loads `ground_truth.json`
2. POSTs each image's batch to a local FastAPI instance OR the staging URL
3. Compares each extracted stop against ground truth field-by-field
4. Prints a markdown report with per-carrier precision/recall

## Privacy

Screenshots may contain real addresses + recipient names. Treat as PII.
- Do **not** commit `images/` (already gitignored).
- Do **not** post screenshots in Slack/Discord/issues.
- Discard from local disk once the runner is validated — they are not
  needed long-term for regression (the LLM is the regression target).
