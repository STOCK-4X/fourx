#!/usr/bin/env python3
"""
Refreshes the public Firestore mirror (project stock-4x) for the
Ханшийн Тэмдэглэл dashboard. Runs on a GitHub Actions schedule,
independent of any local machine.

Reads its own tracked-ticker list from Firestore's `watchlist` and
`holdings` collections (this mirror is self-sufficient — it never
touches the private Claude Artifact). Writes: quotes, indices, news,
and (once per UTC calendar day) history, analytics, profiles,
dividends, fx, portfolio_history.

Required environment variables:
  FIREBASE_SERVICE_ACCOUNT_JSON  - the service account key, as raw JSON text
  FINNHUB_API_KEY                - Finnhub API key
"""
import os
import json
import time
import datetime
import urllib.request
import urllib.error

import firebase_admin
from firebase_admin import credentials, firestore

FINNHUB_KEY = os.environ["FINNHUB_API_KEY"]
DEFAULT_TICKERS = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]
UA = {"User-Agent": "Mozilla/5.0"}


def http_get_json(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def finnhub(path, **params):
    params["token"] = FINNHUB_KEY
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return http_get_json(f"https://finnhub.io/api/v1/{path}?{qs}")


def yahoo_chart(symbol, rng="1d", events=None):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range={rng}"
    if events:
        url += f"&events={events}"
    return http_get_json(url, headers=UA)


def now_iso():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")


def today_str():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")


def main():
    cred = credentials.Certificate(json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"]))
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    # Step 1: tracked tickers, read from Firestore itself
    tickers = set()
    for col in ("watchlist", "holdings"):
        for doc in db.collection(col).stream():
            tickers.add(doc.id)
    if not tickers:
        tickers = set(DEFAULT_TICKERS)
    tickers = sorted(tickers)
    print("Tracked tickers:", tickers)

    # Step 2: quotes + profiles
    quotes_written = 0
    profiles_cache = {}
    for t in tickers:
        try:
            q = finnhub("quote", symbol=t)
        except Exception as e:
            print(f"  {t}: quote fetch failed: {e}")
            continue
        price = q.get("c")
        if not price:
            print(f"  {t}: no price, skipping")
            continue
        try:
            p = finnhub("stock/profile2", symbol=t)
        except Exception:
            p = {}
        profiles_cache[t] = p
        name = p.get("name", "")
        prev = q.get("pc")
        chg = q.get("dp")
        db.collection("quotes").document(t).set({
            "ticker": t, "name": name, "price": price, "previousClose": prev,
            "changePercent": chg, "currency": "USD", "updatedAt": now_iso(),
            "source": "Finnhub (GitHub Actions)",
        }, merge=True)
        quotes_written += 1
        time.sleep(0.15)  # gentle on the free-tier rate limit

    # Step 3: indices from Yahoo
    for sym, doc_id, label in [("%5EGSPC", "SPX", "S&P 500"),
                                ("%5EIXIC", "IXIC", "NASDAQ Composite"),
                                ("%5EDJI", "DJI", "Dow Jones Industrial Average")]:
        try:
            d = yahoo_chart(sym, "1d")
            meta = d["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev = meta.get("chartPreviousClose") or meta.get("previousClose")
            chg = (price - prev) / prev * 100 if prev else None
            db.collection("indices").document(doc_id).set({
                "name": label, "price": price, "previousClose": prev,
                "changePercent": chg, "updatedAt": now_iso(),
            }, merge=True)
        except Exception as e:
            print(f"  index {doc_id} failed: {e}")

    # Step 4: news (general + per ticker)
    news_written = 0
    try:
        general = finnhub("news", category="general")
        general.sort(key=lambda x: -x.get("datetime", 0))
        for item in general[:5]:
            _write_news(db, item, "general")
            news_written += 1
    except Exception as e:
        print("  general news failed:", e)

    to_date = today_str()
    from_date = (datetime.datetime.utcnow() - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    for t in tickers:
        try:
            items = finnhub("company-news", symbol=t, **{"from": from_date, "to": to_date})
            items.sort(key=lambda x: -x.get("datetime", 0))
            for item in items[:2]:
                _write_news(db, item, t)
                news_written += 1
        except Exception as e:
            print(f"  {t} news failed: {e}")
        time.sleep(0.1)

    # prune news beyond 50 newest by fetchedAt
    all_news = list(db.collection("news").stream())
    all_news.sort(key=lambda d: d.to_dict().get("fetchedAt", ""), reverse=True)
    for doc in all_news[50:]:
        doc.reference.delete()

    # Step 5: daily-gated data
    gate_doc = None
    if tickers:
        gate_doc = db.collection("analytics").document(tickers[0]).get()
    already_today = gate_doc and gate_doc.exists and gate_doc.to_dict().get("updatedAt", "").startswith(today_str())

    daily_done = False
    if not already_today:
        daily_done = True
        for t in tickers:
            # history: 2 years, Firestore-shaped points ({t,c} objects)
            try:
                d = yahoo_chart(t, "2y")
                res = d["chart"]["result"][0]
                ts = res["timestamp"]
                closes = res["indicators"]["quote"][0]["close"]
                points = [{"t": a, "c": round(b, 4)} for a, b in zip(ts, closes) if b is not None]
                db.collection("history").document(t).set({
                    "ticker": t, "range": "2y", "points": points, "updatedAt": now_iso(),
                }, merge=True)
            except Exception as e:
                print(f"  {t} history failed: {e}")

            # analytics
            try:
                metric = finnhub("stock/metric", symbol=t, metric="all").get("metric", {})
                rec_list = finnhub("stock/recommendation", symbol=t)
                rec0 = rec_list[0] if rec_list else {}
                db.collection("analytics").document(t).set({
                    "ticker": t,
                    "pe": metric.get("peNormalizedAnnual"),
                    "beta": metric.get("beta"),
                    "week52High": metric.get("52WeekHigh"),
                    "week52Low": metric.get("52WeekLow"),
                    "dividendYield": metric.get("dividendYieldIndicatedAnnual"),
                    "marketCap": metric.get("marketCapitalization"),
                    "rec": {
                        "period": rec0.get("period"),
                        "strongBuy": rec0.get("strongBuy", 0),
                        "buy": rec0.get("buy", 0),
                        "hold": rec0.get("hold", 0),
                        "sell": rec0.get("sell", 0),
                        "strongSell": rec0.get("strongSell", 0),
                    },
                    "updatedAt": now_iso(),
                }, merge=True)
            except Exception as e:
                print(f"  {t} analytics failed: {e}")

            # profile (reuse cached profile2 from Step 2 if we have it)
            p = profiles_cache.get(t, {})
            db.collection("profiles").document(t).set({
                "ticker": t, "name": p.get("name"), "industry": p.get("finnhubIndustry"),
                "exchange": p.get("exchange"), "ipo": p.get("ipo"), "country": p.get("country"),
                "shareOutstanding": p.get("shareOutstanding"), "weburl": p.get("weburl"),
                "logo": p.get("logo"), "updatedAt": now_iso(),
            }, merge=True)

            # dividends (Yahoo, 1y events)
            try:
                d = yahoo_chart(t, "1y", events="div")
                divs = d["chart"]["result"][0].get("events", {}).get("dividends", {})
                if divs:
                    last = max(divs.values(), key=lambda x: x["date"])
                    est_next = datetime.datetime.utcfromtimestamp(last["date"] + 91 * 86400).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    db.collection("dividends").document(t).set({
                        "ticker": t, "paysDividend": True, "lastAmount": last["amount"],
                        "lastDate": datetime.datetime.utcfromtimestamp(last["date"]).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                        "estimatedNextDate": est_next, "updatedAt": now_iso(),
                    }, merge=True)
                else:
                    db.collection("dividends").document(t).set({
                        "ticker": t, "paysDividend": False, "updatedAt": now_iso(),
                    }, merge=True)
            except Exception as e:
                print(f"  {t} dividends failed: {e}")
            time.sleep(0.2)

        # FX (once)
        try:
            fx = http_get_json("https://open.er-api.com/v6/latest/USD")
            db.collection("fx").document("USDMNT").set({
                "rate": fx["rates"]["MNT"], "updatedAt": now_iso(), "source": "open.er-api.com",
            }, merge=True)
        except Exception as e:
            print("  fx failed:", e)

        # portfolio value snapshot (once)
        try:
            holdings = [d.to_dict() for d in db.collection("holdings").stream()]
            mv = 0.0
            cost = 0.0
            quotes_now = {d.id: d.to_dict() for d in db.collection("quotes").stream()}
            for h in holdings:
                tkr = h.get("ticker")
                shares = float(h.get("shares") or 0)
                avg_cost = float(h.get("avgCost") or 0)
                price = (quotes_now.get(tkr) or {}).get("price", avg_cost)
                mv += price * shares
                cost += avg_cost * shares
            db.collection("portfolio_history").document(today_str()).set({
                "date": today_str(), "marketValue": mv, "costBasis": cost, "updatedAt": now_iso(),
            }, merge=True)
        except Exception as e:
            print("  portfolio snapshot failed:", e)

    print(f"Done. Quotes: {quotes_written}, news: {news_written}, daily refresh: {daily_done}")


def _write_news(db, item, ticker):
    fh_id = item.get("id")
    if not fh_id:
        return
    dt = item.get("datetime")
    published = datetime.datetime.utcfromtimestamp(dt).strftime("%Y-%m-%dT%H:%M:%S.000Z") if dt else None
    db.collection("news").document(f"n-fh-{fh_id}").set({
        "headline": item.get("headline"), "url": item.get("url"), "source": item.get("source"),
        "ticker": ticker, "publishedAt": published, "fetchedAt": now_iso(),
    }, merge=True)


if __name__ == "__main__":
    main()
