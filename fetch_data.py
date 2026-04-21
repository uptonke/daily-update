import json
import os
import time
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

OUTPUT_FILE = "data/daily_data.json"
HEADERS = {"User-Agent": "daily-report-bot/1.0"}

TRENDS_KEYWORDS = [
    "Iran war", "Ukraine war", "NATO",
    "Federal Reserve", "oil price", "gold price",
    "recession", "Trump tariffs",
    "interest rate", "inflation",
    "AI regulation", "chip export",
    "China economy", "China US",
    "OPEC"
]

WIKI_PAGES = [
    "Strait_of_Hormuz",
    "Russo-Ukrainian_War",
    "Federal_Reserve_System",
    "Donald_Trump",
    "OPEC",
    "NATO",
    "Artificial_intelligence_regulation",
    "Kevin_Warsh",
    "China%E2%80%93United_States_trade_war",
    "Semiconductor"
]


def safe_get(url: str, params: Optional[dict] = None, timeout: int = 10, retries: int = 2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout, headers=HEADERS)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
    raise last_err


def fetch_polymarket() -> Dict[str, Any]:
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "limit": 20,
        "active": "true",
        "order": "volume24hr",
        "ascending": "false"
    }

    try:
        resp = safe_get(url, params=params)
        markets = resp.json()
        cleaned = []

        for m in markets[:20]:
            question = m.get("question", "")
            volume_24h = m.get("volume24hr")
            outcomes = m.get("outcomes") or []
            outcome_prices = m.get("outcomePrices") or []

            yes_price = None
            market_type = "unknown"

            if len(outcomes) == len(outcome_prices) and outcomes:
                market_type = "binary" if set(outcomes) >= {"Yes", "No"} else "non_binary"
                for name, price in zip(outcomes, outcome_prices):
                    if str(name).strip().lower() == "yes":
                        try:
                            yes_price = float(price)
                        except Exception:
                            yes_price = None
                        break

            try:
                volume_24h = float(volume_24h) if volume_24h is not None else None
            except Exception:
                volume_24h = None

            cleaned.append({
                "question": question,
                "yes_price": yes_price,
                "volume_24h": volume_24h,
                "market_type": market_type,
                "status": "ok"
            })

        return {"status": "ok", "data": cleaned, "errors": []}

    except Exception as e:
        return {"status": "error", "data": [], "errors": [str(e)]}


def fetch_google_trends() -> Dict[str, Any]:
    try:
        from pytrends.request import TrendReq
        pytrends = TrendReq(hl="en-US", tz=0)

        rows = []
        errors = []

        for i in range(0, len(TRENDS_KEYWORDS), 5):
            chunk = TRENDS_KEYWORDS[i:i + 5]
            try:
                pytrends.build_payload(chunk, timeframe="now 1-d", geo="")
                data = pytrends.interest_over_time()

                for kw in chunk:
                    if not data.empty and kw in data.columns:
                        series = data[kw]
                        rows.append({
                            "keyword": kw,
                            "score_mean_24h": float(series.mean()),
                            "score_last": int(series.iloc[-1]),
                            "status": "ok",
                            "note": "Scores are comparable within the same payload chunk, not perfectly across different chunks."
                        })
                    else:
                        rows.append({
                            "keyword": kw,
                            "score_mean_24h": None,
                            "score_last": None,
                            "status": "empty",
                            "note": "No data returned."
                        })
            except Exception as e:
                errors.append(f"chunk {chunk}: {str(e)}")
                for kw in chunk:
                    rows.append({
                        "keyword": kw,
                        "score_mean_24h": None,
                        "score_last": None,
                        "status": "error",
                        "note": str(e)
                    })

        return {"status": "ok", "data": rows, "errors": errors}

    except Exception as e:
        return {"status": "error", "data": [], "errors": [str(e)]}


def fetch_wikipedia_pageviews() -> Dict[str, Any]:
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    rows = []
    errors = []

    for page in WIKI_PAGES:
        url = (
            "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
            f"/en.wikipedia/all-access/all-agents/{page}/daily/{yesterday}/{yesterday}"
        )
        try:
            resp = safe_get(url)
            items = resp.json().get("items", [])
            views = items[0]["views"] if items else None

            rows.append({
                "page": page,
                "views": views,
                "date": yesterday,
                "status": "ok" if views is not None else "empty"
            })
        except Exception as e:
            errors.append(f"{page}: {str(e)}")
            rows.append({
                "page": page,
                "views": None,
                "date": yesterday,
                "status": "error",
                "error": str(e)
            })

    return {"status": "ok", "data": rows, "errors": errors}


def main():
    generated_at = datetime.utcnow().isoformat() + "Z"

    polymarket = fetch_polymarket()
    trends = fetch_google_trends()
    wiki = fetch_wikipedia_pageviews()

    output = {
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "generated_at": generated_at,
        "sources": {
            "polymarket": polymarket,
            "google_trends": trends,
            "wikipedia_pageviews": wiki
        },
        "data_quality": {
            "polymarket_ok": sum(1 for x in polymarket["data"] if x.get("status") == "ok"),
            "trends_ok": sum(1 for x in trends["data"] if x.get("status") == "ok"),
            "wiki_ok": sum(1 for x in wiki["data"] if x.get("status") == "ok")
        }
    }

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
