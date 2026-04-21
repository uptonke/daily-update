import json
import os
import requests
from datetime import datetime, timedelta
import anthropic

OUTPUT_FILE = "data/daily_data.json"

# --- 1. Polymarket: 抓當日交易量最高的公開市場 ---
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

# --- 2. Claude 動態決定今天要查什麼關鍵字 ---
def get_dynamic_queries(polymarket_data):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    prompt = f"""Based on these top Polymarket prediction markets today:
{json.dumps(polymarket_data, ensure_ascii=False, indent=2)}

Return ONLY a valid JSON object (no markdown, no explanation) with:
{{
  "google_trends_keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "wikipedia_pages": ["Page_Title_1", "Page_Title_2", "Page_Title_3", "Page_Title_4", "Page_Title_5", "Page_Title_6", "Page_Title_7", "Page_Title_8", "Page_Title_9", "Page_Title_10"]
}}

Rules:
- google_trends_keywords: English, short (1-3 words), currently newsworthy based on the markets
- wikipedia_pages: exact English Wikipedia article titles (use underscores not spaces), currently relevant topics
"""
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = message.content[0].text.strip()
    # 防禦性解析：去除可能的 markdown fences
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())

# --- 3. Google Trends (pytrends) ---
def fetch_google_trends(keywords):
    from pytrends.request import TrendReq
    pytrends = TrendReq(hl='en-US', tz=0)
    results = {}
    # pytrends 每次最多 5 個關鍵字
    for i in range(0, len(keywords), 5):
        chunk = keywords[i:i+5]
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

# --- 4. Wikimedia pageviews ---
def fetch_wikipedia_pageviews(pages):
    results = {}
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    headers = {"User-Agent": "daily-report-bot/1.0 (your-email@example.com)"}
    for page in pages:
        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
            f"/en.wikipedia/all-access/all-agents/{page}/daily/{yesterday}/{yesterday}"
        )
        try:
            resp = requests.get(url, timeout=10, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                results[page] = data["items"][0]["views"]
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

    print("=== Step 2: Getting dynamic queries from Claude ===")
    queries = get_dynamic_queries(polymarket)
    keywords = queries["google_trends_keywords"]
    wiki_pages = queries["wikipedia_pages"]
    print(f"Keywords: {keywords}")
    print(f"Wiki pages: {wiki_pages}")

    print("=== Step 3: Fetching Google Trends ===")
    trends = fetch_google_trends(keywords)

    print("=== Step 4: Fetching Wikipedia pageviews ===")
    pageviews = fetch_wikipedia_pageviews(wiki_pages)

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
