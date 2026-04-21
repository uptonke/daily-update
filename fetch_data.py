import json
import os
import requests
from datetime import datetime, timedelta

OUTPUT_FILE = "data/daily_data.json"

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
    "Federal_Reserve",
    "Donald_Trump",
    "2026_Iran-United_States_relations",
    "OPEC",
    "NATO",
    "Artificial_intelligence_regulation",
    "Kevin_Warsh",
    "China-United_States_trade_war",
    "Global_recession",
    "Semiconductor"
]

# --- 1. Polymarket ---
def fetch_polymarket():
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "limit": 20,
        "active": "true",
        "order": "volume24hr",
        "ascending": "false"
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    markets = resp.json()
    return [
        {
            "question": m.get("question", ""),
            "yes_price": m.get("outcomePrices", ["?"])[0] if m.get("outcomePrices") else "?",
            "volume_24h": m.get("volume24hr", 0),
        }
        for m in markets[:10]
    ]

# --- 2. Google Trends ---
def fetch_google_trends():
    from pytrends.request import TrendReq
    pytrends = TrendReq(hl='en-US', tz=0)
    results = {}
    for i in range(0, len(TRENDS_KEYWORDS), 5):
        chunk = TRENDS_KEYWORDS[i:i+5]
        try:
            pytrends.build_payload(chunk, timeframe='now 1-d', geo='')
            data = pytrends.interest_over_time()
            if not data.empty:
                for kw in chunk:
                    if kw in data.columns:
                        results[kw] = int(data[kw].mean())
                    else:
                        results[kw] = 0
        except Exception as e:
            for kw in chunk:
                results[kw] = f"error: {str(e)}"
    return results

# --- 3. Wikimedia pageviews ---
def fetch_wikipedia_pageviews():
    results = {}
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    headers = {"User-Agent": "daily-report-bot/1.0 (uptonke@github)"}
    for page in WIKI_PAGES:
        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
            f"/en.wikipedia/all-access/all-agents/{page}/daily/{yesterday}/{yesterday}"
        )
        try:
            resp = requests.get(url, timeout=10, headers=headers)
            if resp.status_code == 200:
                results[page] = resp.json()["items"][0]["views"]
            else:
                results[page] = f"error: HTTP {resp.status_code}"
        except Exception as e:
            results[page] = f"error: {str(e)}"
    return results

# --- Main ---
def main():
    print("=== Step 1: Fetching Polymarket ===")
    polymarket = fetch_polymarket()
    print(f"Got {len(polymarket)} markets")

    print("=== Step 2: Fetching Google Trends ===")
    trends = fetch_google_trends()
    print(f"Got {len(trends)} keywords")

    print("=== Step 3: Fetching Wikipedia pageviews ===")
    pageviews = fetch_wikipedia_pageviews()
    print(f"Got {len(pageviews)} pages")

    output = {
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "polymarket_top_markets": polymarket,
        "google_trends": trends,
        "wikipedia_pageviews": pageviews,
    }

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n=== Done. Saved to {OUTPUT_FILE} ===")
    print(json.dumps(output, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
