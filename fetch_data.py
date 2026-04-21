import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from supabase import Client, create_client

ARCHIVE_DIR = "data/archive"
OUTPUT_FILE = "data/daily_data.json"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "sentiment-archive")

HEADERS = {
    "User-Agent": "daily-report-bot/1.0 (+https://github.com/uptonke/daily-update)"
}

POLYMARKET_URL = "https://gamma-api.polymarket.com/markets"
POLYMARKET_LIMIT = 30

# Google Trends: 每個 chunk 都帶同一個 anchor keyword，做粗略跨 chunk 校正
ANCHOR_KEYWORD = "Federal Reserve"
TRENDS_KEYWORDS = [
    "Iran war",
    "Ukraine war",
    "NATO",
    "oil price",
    "gold price",
    "recession",
    "Trump tariffs",
    "interest rate",
    "inflation",
    "AI regulation",
    "chip export",
    "China economy",
    "China US",
    "OPEC",
]

# 建議只放長期穩定、頁名明確的頁面
WIKI_PAGES = [
    "Strait_of_Hormuz",
    "Russo-Ukrainian_War",
    "Federal_Reserve",
    "Donald_Trump",
    "OPEC",
    "NATO",
    "Artificial_intelligence_regulation",
    "Kevin_Warsh",
    "China%E2%80%93United_States_trade_war",
    "Semiconductor",
]

REQUEST_TIMEOUT = 15
REQUEST_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.5


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def request_json(
    method: str,
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = REQUEST_TIMEOUT,
    retries: int = REQUEST_RETRIES,
) -> Any:
    last_error: Optional[Exception] = None
    merged_headers = {**HEADERS, **(headers or {})}

    for attempt in range(retries + 1):
        try:
            logging.info("HTTP %s %s | attempt=%s", method, url, attempt + 1)
            resp = requests.request(
                method=method,
                url=url,
                params=params,
                headers=merged_headers,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_error = e
            logging.warning(
                "Request failed | url=%s | attempt=%s | error=%s",
                url,
                attempt + 1,
                str(e),
            )
            if attempt < retries:
                sleep_seconds = BACKOFF_BASE_SECONDS * (2 ** attempt)
                time.sleep(sleep_seconds)

    raise RuntimeError(f"Request failed after {retries + 1} attempts: {url}") from last_error


def build_source_result(
    name: str,
    data: List[Dict[str, Any]],
    errors: List[Dict[str, Any]],
    *,
    meta: Optional[Dict[str, Any]] = None,
    summary: Optional[Dict[str, Any]] = None,
    status: Optional[str] = None,
) -> Dict[str, Any]:
    final_status = status
    if final_status is None:
        if errors and not data:
            final_status = "error"
        elif errors:
            final_status = "partial"
        else:
            final_status = "ok"

    return {
        "status": final_status,
        "meta": meta or {},
        "summary": summary or {},
        "data": data,
        "errors": errors,
    }


# ----------------------------
# Polymarket
# ----------------------------
def normalize_outcomes(raw_outcomes: Any) -> List[str]:
    if isinstance(raw_outcomes, list):
        return [str(x).strip() for x in raw_outcomes]

    if isinstance(raw_outcomes, str):
        try:
            parsed = json.loads(raw_outcomes)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed]
        except json.JSONDecodeError:
            pass

    return []


def normalize_outcome_prices(raw_prices: Any) -> List[Optional[float]]:
    if isinstance(raw_prices, list):
        return [safe_float(x) for x in raw_prices]

    if isinstance(raw_prices, str):
        try:
            parsed = json.loads(raw_prices)
            if isinstance(parsed, list):
                return [safe_float(x) for x in parsed]
        except json.JSONDecodeError:
            pass

    return []


def extract_yes_no_prices(
    outcomes: List[str],
    prices: List[Optional[float]],
) -> Tuple[Optional[float], Optional[float], str]:
    if not outcomes or not prices or len(outcomes) != len(prices):
        return None, None, "unknown"

    outcome_map = {}
    for name, price in zip(outcomes, prices):
        outcome_map[str(name).strip().lower()] = price

    yes_price = outcome_map.get("yes")
    no_price = outcome_map.get("no")

    if yes_price is not None or no_price is not None:
        return yes_price, no_price, "binary"

    return None, None, "non_binary"


def fetch_polymarket() -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    try:
        raw_markets = request_json(
            "GET",
            POLYMARKET_URL,
            params={
                "limit": POLYMARKET_LIMIT,
                "active": "true",
                "order": "volume24hr",
                "ascending": "false",
            },
        )

        if not isinstance(raw_markets, list):
            raise ValueError("Polymarket response is not a list")

        for idx, market in enumerate(raw_markets[:POLYMARKET_LIMIT]):
            try:
                question = str(market.get("question", "")).strip()
                slug = market.get("slug")
                volume_24h = safe_float(market.get("volume24hr"))
                liquidity = safe_float(market.get("liquidity"))
                end_date = market.get("endDate")
                active = market.get("active")
                closed = market.get("closed")

                outcomes = normalize_outcomes(market.get("outcomes"))
                outcome_prices = normalize_outcome_prices(market.get("outcomePrices"))
                yes_price, no_price, market_type = extract_yes_no_prices(outcomes, outcome_prices)

                rows.append(
                    {
                        "source": "polymarket",
                        "id": market.get("id"),
                        "question": question,
                        "slug": slug,
                        "market_type": market_type,
                        "yes_price": yes_price,
                        "no_price": no_price,
                        "outcomes": outcomes,
                        "outcome_prices": outcome_prices,
                        "volume_24h": volume_24h,
                        "liquidity": liquidity,
                        "active": active,
                        "closed": closed,
                        "end_date": end_date,
                        "status": "ok",
                    }
                )
            except Exception as e:
                errors.append(
                    {
                        "source": "polymarket",
                        "index": idx,
                        "status": "error",
                        "error_type": type(e).__name__,
                        "message": str(e),
                    }
                )

        top_by_volume = sorted(
            [x for x in rows if x.get("volume_24h") is not None],
            key=lambda x: x["volume_24h"],
            reverse=True,
        )[:5]

        summary = {
            "total_rows": len(rows),
            "top_questions_by_volume_24h": [
                {
                    "question": x["question"],
                    "volume_24h": x["volume_24h"],
                    "yes_price": x["yes_price"],
                    "market_type": x["market_type"],
                }
                for x in top_by_volume
            ],
            "binary_markets": sum(1 for x in rows if x.get("market_type") == "binary"),
            "non_binary_markets": sum(1 for x in rows if x.get("market_type") == "non_binary"),
        }

        return build_source_result(
            "polymarket",
            rows,
            errors,
            meta={"requested_limit": POLYMARKET_LIMIT},
            summary=summary,
        )
    except Exception as e:
        errors.append(
            {
                "source": "polymarket",
                "status": "error",
                "error_type": type(e).__name__,
                "message": str(e),
            }
        )
        return build_source_result(
            "polymarket",
            [],
            errors,
            meta={"requested_limit": POLYMARKET_LIMIT},
            summary={},
            status="error",
        )


# ----------------------------
# Google Trends
# ----------------------------
def chunk_keywords_with_anchor(
    keywords: List[str],
    anchor_keyword: str,
    max_terms_per_payload: int = 5,
) -> List[List[str]]:
    """
    pytrends 每次 build_payload 最多 5 個字。
    這裡保留 1 個位置給 anchor，所以每 chunk = 4 個目標詞 + 1 個 anchor。
    """
    if max_terms_per_payload < 2:
        raise ValueError("max_terms_per_payload must be >= 2")

    target_chunk_size = max_terms_per_payload - 1
    chunks: List[List[str]] = []
    for i in range(0, len(keywords), target_chunk_size):
        chunk = keywords[i: i + target_chunk_size]
        if anchor_keyword not in chunk:
            chunk = chunk + [anchor_keyword]
        chunks.append(chunk)
    return chunks


def fetch_google_trends() -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    try:
        from pytrends.request import TrendReq
    except Exception as e:
        errors.append(
            {
                "source": "google_trends",
                "status": "error",
                "error_type": type(e).__name__,
                "message": f"pytrends import failed: {str(e)}",
            }
        )
        return build_source_result("google_trends", [], errors, status="error")

    try:
        pytrends = TrendReq(hl="en-US", tz=0)
        chunks = chunk_keywords_with_anchor(TRENDS_KEYWORDS, ANCHOR_KEYWORD, max_terms_per_payload=5)

        for chunk_index, chunk in enumerate(chunks):
            try:
                logging.info("Google Trends payload | chunk_index=%s | chunk=%s", chunk_index, chunk)
                pytrends.build_payload(chunk, timeframe="now 1-d", geo="")
                df = pytrends.interest_over_time()

                if df.empty:
                    for keyword in chunk:
                        if keyword == ANCHOR_KEYWORD:
                            continue
                        rows.append(
                            {
                                "source": "google_trends",
                                "keyword": keyword,
                                "anchor_keyword": ANCHOR_KEYWORD,
                                "chunk_index": chunk_index,
                                "score_mean_24h_raw": None,
                                "score_last_raw": None,
                                "anchor_mean_24h_raw": None,
                                "anchor_last_raw": None,
                                "score_mean_24h_anchor_normalized": None,
                                "score_last_anchor_normalized": None,
                                "status": "empty",
                                "error": None,
                                "notes": "No data returned for this chunk.",
                            }
                        )
                    continue

                anchor_mean = None
                anchor_last = None
                if ANCHOR_KEYWORD in df.columns:
                    anchor_series = df[ANCHOR_KEYWORD]
                    anchor_mean = float(anchor_series.mean())
                    anchor_last = float(anchor_series.iloc[-1])

                for keyword in chunk:
                    if keyword == ANCHOR_KEYWORD:
                        continue

                    if keyword not in df.columns:
                        rows.append(
                            {
                                "source": "google_trends",
                                "keyword": keyword,
                                "anchor_keyword": ANCHOR_KEYWORD,
                                "chunk_index": chunk_index,
                                "score_mean_24h_raw": None,
                                "score_last_raw": None,
                                "anchor_mean_24h_raw": anchor_mean,
                                "anchor_last_raw": anchor_last,
                                "score_mean_24h_anchor_normalized": None,
                                "score_last_anchor_normalized": None,
                                "status": "empty",
                                "error": None,
                                "notes": "Keyword column missing in payload result.",
                            }
                        )
                        continue

                    series = df[keyword]
                    score_mean = float(series.mean())
                    score_last = float(series.iloc[-1])

                    norm_mean = None
                    norm_last = None
                    if anchor_mean and anchor_mean > 0:
                        norm_mean = round((score_mean / anchor_mean) * 100, 2)
                    if anchor_last and anchor_last > 0:
                        norm_last = round((score_last / anchor_last) * 100, 2)

                    rows.append(
                        {
                            "source": "google_trends",
                            "keyword": keyword,
                            "anchor_keyword": ANCHOR_KEYWORD,
                            "chunk_index": chunk_index,
                            "score_mean_24h_raw": round(score_mean, 2),
                            "score_last_raw": round(score_last, 2),
                            "anchor_mean_24h_raw": round(anchor_mean, 2) if anchor_mean is not None else None,
                            "anchor_last_raw": round(anchor_last, 2) if anchor_last is not None else None,
                            "score_mean_24h_anchor_normalized": norm_mean,
                            "score_last_anchor_normalized": norm_last,
                            "status": "ok",
                            "error": None,
                            "notes": (
                                "Anchor-normalized scores are rough cross-chunk calibration, "
                                "not official globally comparable Google Trends scores."
                            ),
                        }
                    )

            except Exception as e:
                errors.append(
                    {
                        "source": "google_trends",
                        "chunk_index": chunk_index,
                        "chunk": chunk,
                        "status": "error",
                        "error_type": type(e).__name__,
                        "message": str(e),
                    }
                )
                for keyword in chunk:
                    if keyword == ANCHOR_KEYWORD:
                        continue
                    rows.append(
                        {
                            "source": "google_trends",
                            "keyword": keyword,
                            "anchor_keyword": ANCHOR_KEYWORD,
                            "chunk_index": chunk_index,
                            "score_mean_24h_raw": None,
                            "score_last_raw": None,
                            "anchor_mean_24h_raw": None,
                            "anchor_last_raw": None,
                            "score_mean_24h_anchor_normalized": None,
                            "score_last_anchor_normalized": None,
                            "status": "error",
                            "error": str(e),
                            "notes": "Chunk-level request failed.",
                        }
                    )

        normalized_ok = [
            x for x in rows
            if x.get("status") == "ok" and x.get("score_mean_24h_anchor_normalized") is not None
        ]
        top_keywords = sorted(
            normalized_ok,
            key=lambda x: x["score_mean_24h_anchor_normalized"],
            reverse=True,
        )[:5]

        summary = {
            "anchor_keyword": ANCHOR_KEYWORD,
            "total_rows": len(rows),
            "top_keywords_by_anchor_normalized_mean_24h": [
                {
                    "keyword": x["keyword"],
                    "score_mean_24h_anchor_normalized": x["score_mean_24h_anchor_normalized"],
                    "score_last_anchor_normalized": x["score_last_anchor_normalized"],
                }
                for x in top_keywords
            ],
        }

        return build_source_result(
            "google_trends",
            rows,
            errors,
            meta={
                "timeframe": "now 1-d",
                "geo": "global",
                "anchor_keyword": ANCHOR_KEYWORD,
            },
            summary=summary,
        )
    except Exception as e:
        errors.append(
            {
                "source": "google_trends",
                "status": "error",
                "error_type": type(e).__name__,
                "message": str(e),
            }
        )
        return build_source_result("google_trends", [], errors, status="error")


# ----------------------------
# Wikipedia pageviews
# ----------------------------
def fetch_wikipedia_pageviews() -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    yesterday = (utc_now() - timedelta(days=1)).strftime("%Y%m%d")

    for page in WIKI_PAGES:
        url = (
            "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
            f"/en.wikipedia/all-access/all-agents/{page}/daily/{yesterday}/{yesterday}"
        )
        try:
            payload = request_json("GET", url)
            items = payload.get("items", []) if isinstance(payload, dict) else []
            views = items[0]["views"] if items else None

            rows.append(
                {
                    "source": "wikipedia_pageviews",
                    "page": page,
                    "date": yesterday,
                    "views": safe_int(views),
                    "status": "ok" if views is not None else "empty",
                    "error": None,
                }
            )
        except Exception as e:
            errors.append(
                {
                    "source": "wikipedia_pageviews",
                    "page": page,
                    "status": "error",
                    "error_type": type(e).__name__,
                    "message": str(e),
                }
            )
            rows.append(
                {
                    "source": "wikipedia_pageviews",
                    "page": page,
                    "date": yesterday,
                    "views": None,
                    "status": "error",
                    "error": str(e),
                }
            )

    top_pages = sorted(
        [x for x in rows if x.get("views") is not None],
        key=lambda x: x["views"],
        reverse=True,
    )[:5]

    summary = {
        "date": yesterday,
        "total_rows": len(rows),
        "top_pages_by_views": [
            {"page": x["page"], "views": x["views"]}
            for x in top_pages
        ],
    }

    return build_source_result(
        "wikipedia_pageviews",
        rows,
        errors,
        meta={"date": yesterday},
        summary=summary,
    )


# ----------------------------
# Delta helpers
# ----------------------------
def load_previous_output(output_file: str = OUTPUT_FILE) -> Optional[Dict[str, Any]]:
    if not os.path.exists(output_file):
        logging.info("No previous output file found: %s", output_file)
        return None

    try:
        with open(output_file, "r", encoding="utf-8") as f:
            payload = json.load(f)
        logging.info("Loaded previous output file: %s", output_file)
        return payload
    except Exception as e:
        logging.warning("Failed to load previous output | file=%s | error=%s", output_file, str(e))
        return None


def classify_direction(delta: Optional[float], *, epsilon: float = 1e-9) -> str:
    if delta is None:
        return "unknown"
    if delta > epsilon:
        return "up"
    if delta < -epsilon:
        return "down"
    return "flat"


def calc_delta(current: Optional[float], previous: Optional[float]) -> Dict[str, Any]:
    if current is None or previous is None:
        return {
            "current": current,
            "previous": previous,
            "absolute_change": None,
            "pct_change": None,
            "direction": "unknown",
        }

    absolute_change = round(current - previous, 4)

    pct_change = None
    if previous != 0:
        pct_change = round(((current - previous) / abs(previous)) * 100, 2)

    return {
        "current": current,
        "previous": previous,
        "absolute_change": absolute_change,
        "pct_change": pct_change,
        "direction": classify_direction(absolute_change),
    }


def index_by_key(rows: List[Dict[str, Any]], key_field: str) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = row.get(key_field)
        if key is None:
            continue
        indexed[str(key)] = row
    return indexed


def build_polymarket_delta(
    current_source: Dict[str, Any],
    previous_source: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    current_rows = current_source.get("data", [])
    previous_rows = (previous_source or {}).get("data", [])

    current_index = index_by_key(current_rows, "question")
    previous_index = index_by_key(previous_rows, "question")

    rows: List[Dict[str, Any]] = []

    for question, current_row in current_index.items():
        previous_row = previous_index.get(question)

        volume_delta = calc_delta(
            current_row.get("volume_24h"),
            previous_row.get("volume_24h") if previous_row else None,
        )
        yes_price_delta = calc_delta(
            current_row.get("yes_price"),
            previous_row.get("yes_price") if previous_row else None,
        )

        rows.append(
            {
                "question": question,
                "market_type": current_row.get("market_type"),
                "volume_24h_delta": volume_delta,
                "yes_price_delta": yes_price_delta,
                "is_new_market_vs_previous": previous_row is None,
            }
        )

    biggest_volume_up = sorted(
        [x for x in rows if x["volume_24h_delta"]["absolute_change"] is not None],
        key=lambda x: x["volume_24h_delta"]["absolute_change"],
        reverse=True,
    )[:5]

    biggest_volume_down = sorted(
        [x for x in rows if x["volume_24h_delta"]["absolute_change"] is not None],
        key=lambda x: x["volume_24h_delta"]["absolute_change"],
    )[:5]

    return {
        "comparison_key": "question",
        "row_count": len(rows),
        "data": rows,
        "summary": {
            "top_volume_risers": [
                {
                    "question": x["question"],
                    "absolute_change": x["volume_24h_delta"]["absolute_change"],
                    "pct_change": x["volume_24h_delta"]["pct_change"],
                }
                for x in biggest_volume_up
            ],
            "top_volume_fallers": [
                {
                    "question": x["question"],
                    "absolute_change": x["volume_24h_delta"]["absolute_change"],
                    "pct_change": x["volume_24h_delta"]["pct_change"],
                }
                for x in biggest_volume_down
            ],
            "new_markets_vs_previous": sum(1 for x in rows if x["is_new_market_vs_previous"]),
        },
    }


def build_google_trends_delta(
    current_source: Dict[str, Any],
    previous_source: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    current_rows = current_source.get("data", [])
    previous_rows = (previous_source or {}).get("data", [])

    current_index = index_by_key(current_rows, "keyword")
    previous_index = index_by_key(previous_rows, "keyword")

    rows: List[Dict[str, Any]] = []

    for keyword, current_row in current_index.items():
        previous_row = previous_index.get(keyword)

        mean_norm_delta = calc_delta(
            current_row.get("score_mean_24h_anchor_normalized"),
            previous_row.get("score_mean_24h_anchor_normalized") if previous_row else None,
        )
        last_norm_delta = calc_delta(
            current_row.get("score_last_anchor_normalized"),
            previous_row.get("score_last_anchor_normalized") if previous_row else None,
        )

        rows.append(
            {
                "keyword": keyword,
                "mean_anchor_normalized_delta": mean_norm_delta,
                "last_anchor_normalized_delta": last_norm_delta,
                "is_new_keyword_vs_previous": previous_row is None,
            }
        )

    biggest_risers = sorted(
        [x for x in rows if x["mean_anchor_normalized_delta"]["absolute_change"] is not None],
        key=lambda x: x["mean_anchor_normalized_delta"]["absolute_change"],
        reverse=True,
    )[:5]

    biggest_fallers = sorted(
        [x for x in rows if x["mean_anchor_normalized_delta"]["absolute_change"] is not None],
        key=lambda x: x["mean_anchor_normalized_delta"]["absolute_change"],
    )[:5]

    return {
        "comparison_key": "keyword",
        "row_count": len(rows),
        "data": rows,
        "summary": {
            "top_risers": [
                {
                    "keyword": x["keyword"],
                    "absolute_change": x["mean_anchor_normalized_delta"]["absolute_change"],
                    "pct_change": x["mean_anchor_normalized_delta"]["pct_change"],
                }
                for x in biggest_risers
            ],
            "top_fallers": [
                {
                    "keyword": x["keyword"],
                    "absolute_change": x["mean_anchor_normalized_delta"]["absolute_change"],
                    "pct_change": x["mean_anchor_normalized_delta"]["pct_change"],
                }
                for x in biggest_fallers
            ],
            "new_keywords_vs_previous": sum(1 for x in rows if x["is_new_keyword_vs_previous"]),
        },
    }


def build_wikipedia_delta(
    current_source: Dict[str, Any],
    previous_source: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    current_rows = current_source.get("data", [])
    previous_rows = (previous_source or {}).get("data", [])

    current_index = index_by_key(current_rows, "page")
    previous_index = index_by_key(previous_rows, "page")

    rows: List[Dict[str, Any]] = []

    for page, current_row in current_index.items():
        previous_row = previous_index.get(page)

        views_delta = calc_delta(
            current_row.get("views"),
            previous_row.get("views") if previous_row else None,
        )

        rows.append(
            {
                "page": page,
                "views_delta": views_delta,
                "is_new_page_vs_previous": previous_row is None,
            }
        )

    biggest_risers = sorted(
        [x for x in rows if x["views_delta"]["absolute_change"] is not None],
        key=lambda x: x["views_delta"]["absolute_change"],
        reverse=True,
    )[:5]

    biggest_fallers = sorted(
        [x for x in rows if x["views_delta"]["absolute_change"] is not None],
        key=lambda x: x["views_delta"]["absolute_change"],
    )[:5]

    return {
        "comparison_key": "page",
        "row_count": len(rows),
        "data": rows,
        "summary": {
            "top_risers": [
                {
                    "page": x["page"],
                    "absolute_change": x["views_delta"]["absolute_change"],
                    "pct_change": x["views_delta"]["pct_change"],
                }
                for x in biggest_risers
            ],
            "top_fallers": [
                {
                    "page": x["page"],
                    "absolute_change": x["views_delta"]["absolute_change"],
                    "pct_change": x["views_delta"]["pct_change"],
                }
                for x in biggest_fallers
            ],
            "new_pages_vs_previous": sum(1 for x in rows if x["is_new_page_vs_previous"]),
        },
    }


def build_delta_vs_previous_day(
    current_output: Dict[str, Any],
    previous_output: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not previous_output:
        return {
            "available": False,
            "previous_date_utc": None,
            "comparison_type": "none",
            "sources": {},
            "summary": {
                "notes": [
                    "No previous output file was available, so no day-over-day delta was computed."
                ]
            },
        }

    previous_date = previous_output.get("date_utc")
    current_date = current_output.get("date_utc")

    comparison_type = "previous_snapshot"
    if previous_date == current_date:
        comparison_type = "same_day_rerun"

    poly_delta = build_polymarket_delta(
        current_output["sources"]["polymarket"],
        previous_output.get("sources", {}).get("polymarket"),
    )
    trends_delta = build_google_trends_delta(
        current_output["sources"]["google_trends"],
        previous_output.get("sources", {}).get("google_trends"),
    )
    wiki_delta = build_wikipedia_delta(
        current_output["sources"]["wikipedia_pageviews"],
        previous_output.get("sources", {}).get("wikipedia_pageviews"),
    )

    return {
        "available": True,
        "previous_date_utc": previous_date,
        "current_date_utc": current_date,
        "comparison_type": comparison_type,
        "sources": {
            "polymarket": poly_delta,
            "google_trends": trends_delta,
            "wikipedia_pageviews": wiki_delta,
        },
        "summary": {
            "notes": [
                "Comparison is against the previously committed output file in the repo.",
                "If the workflow is re-run on the same UTC date, comparison_type may be same_day_rerun instead of true previous-day.",
                "Use rise/fall signals as relative momentum indicators, not as causal proof."
            ]
        },
    }


# ----------------------------
# Archive / Supabase
# ----------------------------
def build_archive_snapshot(output: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": output.get("schema_version"),
        "date_utc": output.get("date_utc"),
        "generated_at_utc": output.get("generated_at_utc"),
        "source_health": output.get("source_health", {}),
        "summary": output.get("summary", {}),
        "top_signals": {
            "polymarket": output.get("sources", {}).get("polymarket", {}).get("summary", {}).get("top_questions_by_volume_24h", [])[:5],
            "google_trends": output.get("sources", {}).get("google_trends", {}).get("summary", {}).get("top_keywords_by_anchor_normalized_mean_24h", [])[:5],
            "wikipedia_pageviews": output.get("sources", {}).get("wikipedia_pageviews", {}).get("summary", {}).get("top_pages_by_views", [])[:5],
        },
        "delta_summary": output.get("delta_vs_previous_day", {}).get("summary", {}),
        "delta_top_signals": {
            "polymarket": output.get("delta_vs_previous_day", {}).get("sources", {}).get("polymarket", {}).get("summary", {}),
            "google_trends": output.get("delta_vs_previous_day", {}).get("sources", {}).get("google_trends", {}).get("summary", {}),
            "wikipedia_pageviews": output.get("delta_vs_previous_day", {}).get("sources", {}).get("wikipedia_pageviews", {}).get("summary", {}),
        },
    }


def upload_archive_to_supabase(output: Dict[str, Any]) -> Optional[str]:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logging.info("Supabase env vars not set; skipping archive upload")
        return None

    try:
        client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

        date_str = output.get("date_utc", "unknown-date")
        object_path = f"archive/{date_str}.json"

        archive_snapshot = build_archive_snapshot(output)
        payload_bytes = json.dumps(
            archive_snapshot,
            ensure_ascii=False,
            indent=2
        ).encode("utf-8")

        client.storage.from_(SUPABASE_BUCKET).upload(
            path=object_path,
            file=payload_bytes,
            file_options={
                "content-type": "application/json",
                "upsert": "true",
            },
        )

        logging.info("Uploaded archive to Supabase Storage: %s/%s", SUPABASE_BUCKET, object_path)
        return object_path

    except Exception as e:
        logging.warning("Supabase upload failed: %s", str(e))
        return None


# ----------------------------
# Data quality / summaries for LLM
# ----------------------------
def source_health(source_result: Dict[str, Any]) -> Dict[str, Any]:
    data = source_result.get("data", [])
    errors = source_result.get("errors", [])

    ok_count = sum(1 for x in data if x.get("status") == "ok")
    empty_count = sum(1 for x in data if x.get("status") == "empty")
    error_count = sum(1 for x in data if x.get("status") == "error") + len(errors)

    return {
        "status": source_result.get("status"),
        "row_count": len(data),
        "ok_count": ok_count,
        "empty_count": empty_count,
        "error_count": error_count,
    }


def build_llm_summary(
    polymarket: Dict[str, Any],
    google_trends: Dict[str, Any],
    wikipedia_pageviews: Dict[str, Any],
    delta_vs_previous_day: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    poly_top = polymarket.get("summary", {}).get("top_questions_by_volume_24h", [])[:3]
    trends_top = google_trends.get("summary", {}).get("top_keywords_by_anchor_normalized_mean_24h", [])[:5]
    wiki_top = wikipedia_pageviews.get("summary", {}).get("top_pages_by_views", [])[:5]

    recurring_entities: List[str] = []
    trend_keywords = {x.get("keyword", "").lower() for x in trends_top}
    wiki_pages = {x.get("page", "").lower() for x in wiki_top}
    poly_questions = " ".join(x.get("question", "").lower() for x in poly_top)

    candidate_map = {
        "nato": "NATO",
        "opec": "OPEC",
        "ukraine": "Ukraine",
        "trump": "Trump",
        "federal reserve": "Federal Reserve",
        "ai": "AI",
        "china": "China",
        "oil": "Oil",
        "iran": "Iran",
    }

    for key, label in candidate_map.items():
        matched = (
            any(key in kw for kw in trend_keywords)
            or any(key in page for page in wiki_pages)
            or key in poly_questions
        )
        if matched:
            recurring_entities.append(label)

    recurring_entities = sorted(set(recurring_entities))

    signal_strength = {
        "polymarket": "strong" if len(poly_top) >= 3 else "weak",
        "google_trends": "strong" if len(trends_top) >= 3 else "weak",
        "wikipedia_pageviews": "strong" if len(wiki_top) >= 3 else "weak",
    }

    narrative_hints = []
    if any("oil" in str(x).lower() for x in trends_top) or "Oil" in recurring_entities:
        narrative_hints.append("Energy / oil-related attention is present.")
    if "Trump" in recurring_entities:
        narrative_hints.append("Trump-related political / tariff attention is present.")
    if "Ukraine" in recurring_entities or "NATO" in recurring_entities:
        narrative_hints.append("Geopolitical / security attention is present.")
    if "Federal Reserve" in recurring_entities:
        narrative_hints.append("Macro / rates attention is present.")
    if "AI" in recurring_entities:
        narrative_hints.append("AI / regulation / chip attention is present.")
    if "China" in recurring_entities:
        narrative_hints.append("China / US-China attention is present.")
    if "Iran" in recurring_entities:
        narrative_hints.append("Iran-related geopolitical attention is present.")

    momentum_hints: List[str] = []
    if delta_vs_previous_day and delta_vs_previous_day.get("available"):
        trend_risers = (
            delta_vs_previous_day.get("sources", {})
            .get("google_trends", {})
            .get("summary", {})
            .get("top_risers", [])[:3]
        )
        wiki_risers = (
            delta_vs_previous_day.get("sources", {})
            .get("wikipedia_pageviews", {})
            .get("summary", {})
            .get("top_risers", [])[:3]
        )
        poly_risers = (
            delta_vs_previous_day.get("sources", {})
            .get("polymarket", {})
            .get("summary", {})
            .get("top_volume_risers", [])[:3]
        )

        trend_parts = [
            f"{x['keyword']} ({x['absolute_change']:+})"
            for x in trend_risers
            if x.get("absolute_change") is not None
        ]
        wiki_parts = [
            f"{x['page']} ({x['absolute_change']:+})"
            for x in wiki_risers
            if x.get("absolute_change") is not None
        ]
        poly_parts = [
            f"{x['question']} ({x['absolute_change']:+})"
            for x in poly_risers
            if x.get("absolute_change") is not None
        ]

        if trend_parts:
            momentum_hints.append("Google Trends risers: " + ", ".join(trend_parts))
        if wiki_parts:
            momentum_hints.append("Wikipedia risers: " + ", ".join(wiki_parts))
        if poly_parts:
            momentum_hints.append("Polymarket volume risers: " + ", ".join(poly_parts))

    return {
        "top_polymarket_questions": poly_top,
        "top_google_trends_keywords": trends_top,
        "top_wikipedia_pages": wiki_top,
        "recurring_entities_across_sources": recurring_entities,
        "signal_strength_by_source": signal_strength,
        "narrative_hints": narrative_hints,
        "momentum_hints": momentum_hints,
        "prompt_ready_notes": [
            "Use recurring_entities_across_sources to identify themes confirmed by multiple datasets.",
            "Treat Google Trends anchor-normalized values as rough cross-chunk calibration, not exact absolute ranking.",
            "Prefer entities that appear across at least 2 sources when writing the final sentiment brief.",
            "Use delta_vs_previous_day to identify rising vs cooling attention.",
            "If source health is partial or weak, explicitly say evidence is limited.",
        ],
    }


def build_output(
    polymarket: Dict[str, Any],
    google_trends: Dict[str, Any],
    wikipedia_pageviews: Dict[str, Any],
    previous_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    generated_at = iso_now()
    date_utc = utc_now().strftime("%Y-%m-%d")

    source_health_map = {
        "polymarket": source_health(polymarket),
        "google_trends": source_health(google_trends),
        "wikipedia_pageviews": source_health(wikipedia_pageviews),
    }

    output = {
        "schema_version": "1.1.0",
        "date_utc": date_utc,
        "generated_at_utc": generated_at,
        "pipeline": {
            "name": "daily_sentiment_data_fetch",
            "anchor_keyword": ANCHOR_KEYWORD,
            "request_timeout_seconds": REQUEST_TIMEOUT,
            "request_retries": REQUEST_RETRIES,
        },
        "source_health": source_health_map,
        "sources": {
            "polymarket": polymarket,
            "google_trends": google_trends,
            "wikipedia_pageviews": wikipedia_pageviews,
        },
    }

    delta_vs_previous_day = build_delta_vs_previous_day(output, previous_output)
    output["delta_vs_previous_day"] = delta_vs_previous_day
    output["summary"] = build_llm_summary(
        polymarket,
        google_trends,
        wikipedia_pageviews,
        delta_vs_previous_day,
    )

    return output


def save_output(output: Dict[str, Any], output_file: str = OUTPUT_FILE) -> None:
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # 1. 覆蓋最新版本（給下一次 delta 用）
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # 2. 存本地 archive 精簡版（可選，但保留方便 debug）
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    date_str = output.get("date_utc", "unknown-date")
    archive_file = os.path.join(ARCHIVE_DIR, f"{date_str}.json")
    archive_snapshot = build_archive_snapshot(output)

    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(archive_snapshot, f, ensure_ascii=False, indent=2)

    logging.info("Saved archive snapshot: %s", archive_file)


def main() -> None:
    setup_logging()
    logging.info("Starting daily sentiment data fetch pipeline")

    previous_output = load_previous_output(OUTPUT_FILE)

    polymarket = fetch_polymarket()
    google_trends = fetch_google_trends()
    wikipedia_pageviews = fetch_wikipedia_pageviews()

    output = build_output(
        polymarket,
        google_trends,
        wikipedia_pageviews,
        previous_output=previous_output,
    )
    save_output(output, OUTPUT_FILE)
    upload_archive_to_supabase(output)

    logging.info("Finished pipeline | output_file=%s", OUTPUT_FILE)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
