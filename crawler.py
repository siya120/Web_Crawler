import argparse
import csv
import datetime as dt
import json
import re
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
USER_AGENT = "AndhraPropertyDamageCrawler/2.0 (+local-research)"

RSS_CONNECTORS = {
    "The Hindu (National)": "https://www.thehindu.com/news/national/feeder/default.rss",
    "Indian Express (India News)": "https://indianexpress.com/section/india/feed/",
    "Hindustan Times (India News)": "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
    "Times of India (India)": "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
    "PIB (Press Information Bureau)": "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",
    "MHA (Home Ministry)": "https://www.mha.gov.in/en/rss.xml",
}

SOURCE_QUALITY_SCORES = {
    "GDELT": 0.65,
    "The Hindu (National)": 0.85,
    "Indian Express (India News)": 0.84,
    "Hindustan Times (India News)": 0.8,
    "Times of India (India)": 0.78,
    "PIB (Press Information Bureau)": 0.92,
    "MHA (Home Ministry)": 0.95,
}

ANDHRA_DISTRICTS = [
    "Anakapalli",
    "Anantapur",
    "Annamayya",
    "Bapatla",
    "Chittoor",
    "Dr. B.R. Ambedkar Konaseema",
    "East Godavari",
    "Eluru",
    "Guntur",
    "Kakinada",
    "Krishna",
    "Kurnool",
    "Nandyal",
    "NTR",
    "Palnadu",
    "Parvathipuram Manyam",
    "Prakasam",
    "SPSR Nellore",
    "Srikakulam",
    "Sri Sathya Sai",
    "Tirupati",
    "Visakhapatnam",
    "Vizianagaram",
    "West Godavari",
    "YSR Kadapa",
]


@dataclass
class CandidateArticle:
    title: str
    source_url: str
    domain: str
    source_connector: str
    date_text: str


@dataclass
class IncidentRecord:
    incident_id: str
    date: str
    title: str
    source_url: str
    domain: str
    source_connector: str
    extracted_amount_in_inr: Optional[float]
    extracted_currency_text: Optional[str]
    snippet: str
    merge_key: str
    duplicate_count: int
    duplicate_urls: str
    needs_review: bool
    include_in_total: bool
    reviewer_amount_in_inr: Optional[float]
    reviewer_notes: str
    district_tag: str
    source_quality_score: float
    extraction_confidence_score: float
    incident_confidence_score: float


def build_gdelt_query(start_year: int, end_year: int) -> str:
    terms = [
        '"Andhra Pradesh"',
        "(terrorism OR terror OR militant OR bombing OR extremist OR Naxal OR Maoist)",
        '("property damage" OR "damage to property" OR arson OR blast OR explosion OR vandalism)',
    ]
    date_part = f"date>={start_year}0101000000 AND date<={end_year}1231235959"
    return " AND ".join(terms) + f" AND sourcelang:english AND {date_part}"


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_title_for_match(title: str) -> str:
    title = clean_text(title).lower()
    title = re.sub(r"[^a-z0-9 ]", " ", title)
    title = re.sub(r"\b(andhra pradesh|ap|india|news|update|breaking)\b", " ", title)
    return re.sub(r"\s+", " ", title).strip()


def canonicalize_url(url: str) -> str:
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")
    except Exception:
        return url


def try_parse_date(date_value: str) -> Optional[dt.date]:
    if not date_value:
        return None
    try:
        parsed = date_parser.parse(date_value, fuzzy=True)
        return parsed.date()
    except Exception:
        return None


def fetch_gdelt_articles(start_year: int, end_year: int, max_records: int = 250) -> List[CandidateArticle]:
    params = {
        "query": build_gdelt_query(start_year, end_year),
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "sort": "DateAsc",
    }
    response = requests.get(
        GDELT_DOC_API,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=45,
    )
    response.raise_for_status()
    payload = response.json()
    articles = payload.get("articles", [])
    out: List[CandidateArticle] = []
    for item in articles:
        out.append(
            CandidateArticle(
                title=clean_text(item.get("title", "")),
                source_url=item.get("url", ""),
                domain=item.get("domain", ""),
                source_connector="GDELT",
                date_text=item.get("seendate", "") or item.get("date", ""),
            )
        )
    return out


def fetch_rss_articles(start_year: int, end_year: int, max_per_feed: int = 200) -> List[CandidateArticle]:
    out: List[CandidateArticle] = []
    for connector_name, rss_url in RSS_CONNECTORS.items():
        feed = feedparser.parse(rss_url, request_headers={"User-Agent": USER_AGENT})
        entries = feed.entries[:max_per_feed]
        for entry in entries:
            date_text = (
                entry.get("published")
                or entry.get("updated")
                or entry.get("pubDate")
                or ""
            )
            parsed_date = try_parse_date(date_text)
            if not parsed_date or parsed_date.year < start_year or parsed_date.year > end_year:
                continue
            url = entry.get("link", "")
            out.append(
                CandidateArticle(
                    title=clean_text(entry.get("title", "")),
                    source_url=url,
                    domain=urlparse(url).netloc,
                    source_connector=connector_name,
                    date_text=date_text,
                )
            )
    return out


def fetch_page_text(session: requests.Session, url: str) -> str:
    res = session.get(url, timeout=30)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    for bad in soup(["script", "style", "noscript"]):
        bad.decompose()
    return " ".join(soup.stripped_strings)[:200000]


def is_relevant_incident(text: str) -> bool:
    low = text.lower()
    location_signals = ["andhra pradesh", "visakhapatnam", "vijayawada", "tirupati", "kurnool", "kadapa"]
    terrorism_signals = ["terror", "terrorism", "militant", "extremist", "naxal", "maoist", "ied", "blast"]
    damage_signals = ["property damage", "damage to property", "arson", "destroyed", "loss", "vandal", "set ablaze"]
    return (
        any(word in low for word in location_signals)
        and any(word in low for word in terrorism_signals)
        and any(word in low for word in damage_signals)
    )


def detect_district_tag(title: str, text: str) -> str:
    low = f"{title} {text}".lower()
    normalized = {
        "ysr kadapa": ["kadapa", "ysr kadapa", "cuddapah"],
        "dr. b.r. ambedkar konaseema": ["konaseema"],
        "spsr nellore": ["nellore", "spsr nellore"],
        "ntr": ["ntr district"],
        "sri sathya sai": ["sri sathya sai", "sathya sai"],
    }
    for district in ANDHRA_DISTRICTS:
        alias = [district.lower()]
        alias.extend(normalized.get(district.lower(), []))
        if any(name in low for name in alias):
            return district
    return "Unknown"


def extract_amount_mentions(text: str) -> Iterable[tuple[float, str]]:
    patterns = [
        r"(₹|Rs\.?|INR)\s?([\d,]+(?:\.\d+)?)\s?(crore|lakh|million|billion)?",
        r"(\$|USD)\s?([\d,]+(?:\.\d+)?)\s?(million|billion)?",
    ]
    multiplier = {
        "lakh": 100_000.0,
        "crore": 10_000_000.0,
        "million": 1_000_000.0,
        "billion": 1_000_000_000.0,
        "": 1.0,
    }
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            currency = match.group(1) or ""
            amount_str = (match.group(2) or "").replace(",", "")
            unit = (match.group(3) or "").lower()
            try:
                raw = float(amount_str) * multiplier.get(unit, 1.0)
            except ValueError:
                continue
            yield raw, f"{currency} {match.group(2)} {unit}".strip()


def normalize_amount_to_inr(raw_amount: float, currency_hint: str) -> Optional[float]:
    hint = (currency_hint or "").lower()
    if "inr" in hint or "rupee" in hint or "rs" in hint or "₹" in hint:
        return raw_amount
    if "usd" in hint or "$" in hint or "dollar" in hint:
        return raw_amount * 83.0
    return None


def compute_extraction_confidence(page_text: str, mentions: List[tuple[float, str]]) -> float:
    if not mentions:
        return 0.2
    amount_hint = mentions[0][1].lower()
    confidence = 0.45
    if any(k in amount_hint for k in ["₹", "rs", "inr"]):
        confidence += 0.25
    elif any(k in amount_hint for k in ["$", "usd"]):
        confidence += 0.15

    low = page_text.lower()
    if "property damage" in low or "damage to property" in low:
        confidence += 0.15
    if any(k in low for k in ["estimated loss", "loss of", "worth", "valued at"]):
        confidence += 0.1
    return min(confidence, 1.0)


def compute_incident_confidence(source_connector: str, extraction_confidence: float, district_tag: str) -> float:
    source_quality = SOURCE_QUALITY_SCORES.get(source_connector, 0.7)
    district_bonus = 0.05 if district_tag != "Unknown" else 0.0
    score = source_quality * 0.6 + extraction_confidence * 0.4 + district_bonus
    return round(min(score, 1.0), 3)


def dedupe_records(records: List[IncidentRecord]) -> List[IncidentRecord]:
    grouped: List[List[IncidentRecord]] = []
    for record in records:
        placed = False
        record_title_norm = normalize_title_for_match(record.title)
        record_date = try_parse_date(record.date)
        for bucket in grouped:
            seed = bucket[0]
            seed_title_norm = normalize_title_for_match(seed.title)
            seed_date = try_parse_date(seed.date)
            ratio = SequenceMatcher(None, record_title_norm, seed_title_norm).ratio()
            same_url = canonicalize_url(record.source_url) == canonicalize_url(seed.source_url)
            near_date = (
                record_date is not None
                and seed_date is not None
                and abs((record_date - seed_date).days) <= 3
            )
            if same_url or (ratio >= 0.78 and near_date):
                bucket.append(record)
                placed = True
                break
        if not placed:
            grouped.append([record])

    merged: List[IncidentRecord] = []
    for idx, bucket in enumerate(grouped, start=1):
        bucket = sorted(
            bucket,
            key=lambda r: (
                (r.extracted_amount_in_inr is None),
                -r.incident_confidence_score,
                r.date,
            ),
        )
        primary = bucket[0]
        urls = sorted({item.source_url for item in bucket if item.source_url})
        primary.merge_key = f"INC-{idx:04d}"
        primary.incident_id = primary.merge_key
        primary.duplicate_count = len(bucket)
        primary.duplicate_urls = " | ".join(urls)
        merged.append(primary)
    merged.sort(key=lambda r: r.date)
    return merged


def process_articles(candidates: List[CandidateArticle], start_year: int, end_year: int) -> List[IncidentRecord]:
    rows: List[IncidentRecord] = []
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    seen_urls: Dict[str, bool] = {}

    for item in candidates:
        source_url = item.source_url
        if not source_url:
            continue
        canonical = canonicalize_url(source_url)
        if canonical in seen_urls:
            continue
        seen_urls[canonical] = True

        incident_date = try_parse_date(item.date_text)
        if not incident_date or incident_date.year < start_year or incident_date.year > end_year:
            continue

        title = clean_text(item.title)
        try:
            page_text = fetch_page_text(session, source_url)
        except Exception:
            page_text = title

        if not is_relevant_incident(f"{title} {page_text}"):
            continue

        mentions = list(extract_amount_mentions(page_text))
        amount_inr = None
        amount_text = None
        if mentions:
            raw_amount, currency_text = mentions[0]
            amount_inr = normalize_amount_to_inr(raw_amount, currency_text)
            amount_text = currency_text
        extraction_confidence = compute_extraction_confidence(page_text, mentions)
        district_tag = detect_district_tag(title, page_text)
        source_quality = SOURCE_QUALITY_SCORES.get(item.source_connector, 0.7)
        incident_confidence = compute_incident_confidence(
            item.source_connector,
            extraction_confidence,
            district_tag,
        )

        rows.append(
            IncidentRecord(
                incident_id="",
                date=str(incident_date),
                title=title,
                source_url=source_url,
                domain=item.domain,
                source_connector=item.source_connector,
                extracted_amount_in_inr=amount_inr,
                extracted_currency_text=amount_text,
                snippet=" ".join(page_text.split()[:60]),
                merge_key="",
                duplicate_count=1,
                duplicate_urls=source_url,
                needs_review=True,
                include_in_total=True,
                reviewer_amount_in_inr=amount_inr,
                reviewer_notes="",
                district_tag=district_tag,
                source_quality_score=source_quality,
                extraction_confidence_score=round(extraction_confidence, 3),
                incident_confidence_score=incident_confidence,
            )
        )
    return dedupe_records(rows)


def write_outputs(records: List[IncidentRecord], csv_path: str, summary_path: str) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(records[0]).keys()) if records else [
            "incident_id",
            "date",
            "title",
            "source_url",
            "domain",
            "source_connector",
            "extracted_amount_in_inr",
            "extracted_currency_text",
            "snippet",
            "merge_key",
            "duplicate_count",
            "duplicate_urls",
            "needs_review",
            "include_in_total",
            "reviewer_amount_in_inr",
            "reviewer_notes",
            "district_tag",
            "source_quality_score",
            "extraction_confidence_score",
            "incident_confidence_score",
        ])
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))

    total_known = sum((r.reviewer_amount_in_inr or 0.0) for r in records if r.include_in_total)
    count_with_amount = sum(1 for r in records if r.reviewer_amount_in_inr is not None)
    summary = {
        "record_count": len(records),
        "records_with_amounts": count_with_amount,
        "total_estimated_damage_in_inr": round(total_known, 2),
        "average_incident_confidence_score": round(
            sum(r.incident_confidence_score for r in records) / len(records), 3
        ) if records else 0.0,
        "generated_at_utc": dt.datetime.utcnow().isoformat() + "Z",
        "connectors_used": ["GDELT"] + list(RSS_CONNECTORS.keys()),
        "warning": "Heuristic estimate. Use the review dashboard before final reporting.",
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl Andhra Pradesh terrorism-linked property damage incidents and estimate financial impact."
    )
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--max-records", type=int, default=250)
    parser.add_argument("--csv-out", default="andhra_damage_timeline.csv")
    parser.add_argument("--summary-out", default="andhra_damage_summary.json")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        raise ValueError("start-year must be <= end-year")

    gdelt = fetch_gdelt_articles(args.start_year, args.end_year, args.max_records)
    rss = fetch_rss_articles(args.start_year, args.end_year)
    all_candidates = gdelt + rss
    records = process_articles(all_candidates, args.start_year, args.end_year)
    write_outputs(records, args.csv_out, args.summary_out)

    print(f"Candidates fetched: {len(all_candidates)}")
    print(f"Relevant incidents after dedupe: {len(records)}")
    print(f"Wrote timeline to: {args.csv_out}")
    print(f"Wrote summary to: {args.summary_out}")


if __name__ == "__main__":
    main()
