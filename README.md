# Andhra Pradesh Property Damage Crawler (Local Tool)

This project is a **local-use research crawler** that collects public web reports and estimates property-damage amounts connected to terrorism-related incidents in **Andhra Pradesh** for the **2020-2025** period.

## What it does

- Queries article metadata from multiple connectors:
  - GDELT DOC API
  - Indian news RSS sources
  - Indian government/public-information RSS feeds
- Pulls article text for candidate links.
- Filters content for:
  - Andhra Pradesh location mention
  - terrorism/militancy signals
  - property-damage signals
- Extracts monetary amount mentions (INR/USD patterns).
- Merges likely duplicates (same URL or highly similar headline + nearby date).
- Tags Andhra Pradesh district from title/body (when detected).
- Computes confidence per incident from source quality + extraction strength.
- Outputs:
  - `andhra_damage_timeline.csv` (incident-level rows)
  - `andhra_damage_summary.json` (total estimate + counts)
  - review-ready columns (`needs_review`, `include_in_total`, `reviewer_amount_in_inr`)

## Important limitations

- It does **not** crawl "all internet websites". That is not practically feasible.
- Amount extraction is heuristic and can miss or misread values.
- Some pages block bots or remove old content.
- This output is for **research support only**, not legal/official reporting.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python crawler.py --start-year 2020 --end-year 2025 --max-records 300
```

Optional output paths:

```bash
python crawler.py --csv-out my_timeline.csv --summary-out my_summary.json
```

## Review dashboard (manual verification)

Use Streamlit to verify each incident amount before final totals:

```bash
streamlit run review_dashboard.py
```

In the dashboard:

- edit `reviewer_amount_in_inr` when extraction is wrong
- toggle `include_in_total` for non-financial/noisy rows
- mark `needs_review = false` after validation
- review `district_tag` and confidence fields before approving
- save:
  - `andhra_damage_reviewed.csv`
  - `andhra_damage_review_summary.json`

## Included connectors

- GDELT
- The Hindu (National RSS)
- Indian Express (India feed)
- Hindustan Times (India feed)
- Times of India (India feed)
- PIB (Press Information Bureau)
- Ministry of Home Affairs (RSS)

## Suggested next improvements

- Add Telugu-language sources and translation normalization.
- Add NER-based district extraction to improve tagging coverage.
