"""
gatherer.py — collects NBIS market intelligence from yfinance
"""

import time
import concurrent.futures
import urllib.request
import xml.etree.ElementTree as ET
import html
import re as _re
import yfinance as yf
import numpy as np
import pandas as pd

_RSS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}
_RSS_TIMEOUT = 8  # seconds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def _macd(series: pd.Series):
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line
    cross = "none"
    if len(macd_line) >= 2:
        prev_above = macd_line.iloc[-2] > signal_line.iloc[-2]
        curr_above = macd_line.iloc[-1] > signal_line.iloc[-1]
        if not prev_above and curr_above:
            cross = "bullish"
        elif prev_above and not curr_above:
            cross = "bearish"
    return {
        "macd_value": round(float(macd_line.iloc[-1]), 4),
        "macd_signal": round(float(signal_line.iloc[-1]), 4),
        "macd_histogram": round(float(histogram.iloc[-1]), 4),
        "macd_cross": cross,
    }


def _bollinger(series: pd.Series, window: int = 20, num_std: float = 2.0):
    ma = series.rolling(window).mean()
    std = series.rolling(window).std()
    upper = ma + num_std * std
    lower = ma - num_std * std
    price = series.iloc[-1]
    band_width = float(upper.iloc[-1] - lower.iloc[-1])
    pct_b = float((price - lower.iloc[-1]) / band_width) if band_width != 0 else 0.5
    if pct_b > 0.8:
        bb_position = "overbought"
    elif pct_b < 0.2:
        bb_position = "oversold"
    else:
        bb_position = "neutral"
    return {
        "bb_upper": round(float(upper.iloc[-1]), 4),
        "bb_lower": round(float(lower.iloc[-1]), 4),
        "bb_pct_b": round(pct_b, 4),
        "bb_position": bb_position,
    }


def _ma_cross(series: pd.Series):
    ma20 = series.rolling(20).mean()
    ma50 = series.rolling(50).mean()
    cross = "none"
    if len(ma20) >= 2 and len(ma50) >= 2:
        prev_above = ma20.iloc[-2] > ma50.iloc[-2]
        curr_above = ma20.iloc[-1] > ma50.iloc[-1]
        if not prev_above and curr_above:
            cross = "golden"
        elif prev_above and not curr_above:
            cross = "death"
    price = series.iloc[-1]
    return {
        "ma_20": round(float(ma20.iloc[-1]), 4) if not np.isnan(ma20.iloc[-1]) else None,
        "ma_50": round(float(ma50.iloc[-1]), 4) if not np.isnan(ma50.iloc[-1]) else None,
        "ma_cross": cross,
        "price_vs_ma20": round(float((price / ma20.iloc[-1] - 1) * 100), 2) if not np.isnan(ma20.iloc[-1]) else None,
        "price_vs_ma50": round(float((price / ma50.iloc[-1] - 1) * 100), 2) if not np.isnan(ma50.iloc[-1]) else None,
    }


# ---------------------------------------------------------------------------
# Individual fetchers
# ---------------------------------------------------------------------------

def _fetch_nbis_technicals() -> dict:
    try:
        ticker = yf.Ticker("NBIS")
        hist = ticker.history(period="3mo", interval="1d")
        if hist.empty:
            return {"error": "no history data"}

        close = hist["Close"]
        volume = hist["Volume"]

        price = float(close.iloc[-1])
        prev_close = float(close.iloc[-2]) if len(close) >= 2 else price
        change_pct = round((price / prev_close - 1) * 100, 2)

        vol_avg20 = float(volume.rolling(20).mean().iloc[-1])
        volume_ratio = round(float(volume.iloc[-1]) / vol_avg20, 2) if vol_avg20 > 0 else 1.0

        rsi_val = _rsi(close)
        if rsi_val > 70:
            rsi_signal = "overbought"
        elif rsi_val < 30:
            rsi_signal = "oversold"
        else:
            rsi_signal = "neutral"

        macd_data = _macd(close)
        bb_data = _bollinger(close)
        ma_data = _ma_cross(close)

        info = ticker.fast_info
        high_52w = getattr(info, "year_high", None)
        low_52w = getattr(info, "year_low", None)

        return {
            "price": round(price, 4),
            "change_pct": change_pct,
            "volume_ratio": volume_ratio,
            "rsi_14": round(rsi_val, 2),
            "rsi_signal": rsi_signal,
            **macd_data,
            **bb_data,
            **ma_data,
            "high_52w": round(float(high_52w), 4) if high_52w else None,
            "low_52w": round(float(low_52w), 4) if low_52w else None,
        }
    except Exception as e:
        return {"error": str(e)}


def _fetch_correlated() -> dict:
    symbols = ["NVDA", "SMCI", "CRWV", "SPY", "QQQ", "^VIX"]
    try:
        df = yf.download(symbols, period="6d", interval="1d", progress=False, auto_adjust=True)
        close = df["Close"] if "Close" in df.columns else df.xs("Close", level=1, axis=1)
        result = {}
        for sym in symbols:
            key = sym.replace("^", "")
            try:
                col = close[sym]
                col = col.dropna()
                if len(col) < 2:
                    result[key] = {"error": "insufficient data"}
                    continue
                today_chg = round(float((col.iloc[-1] / col.iloc[-2] - 1) * 100), 2)
                five_d = col.iloc[-min(5, len(col)):]
                five_d_ret = round(float((five_d.iloc[-1] / five_d.iloc[0] - 1) * 100), 2) if len(five_d) >= 2 else None
                result[key] = {
                    "price": round(float(col.iloc[-1]), 4),
                    "change_pct": today_chg,
                    "5d_return": five_d_ret,
                }
            except Exception as e:
                result[key] = {"error": str(e)}
        return result
    except Exception as e:
        return {"error": str(e)}


def _fetch_options() -> dict:
    try:
        ticker = yf.Ticker("NBIS")
        expirations = ticker.options
        if not expirations:
            return {"available": False, "reason": "no expirations"}
        chain = ticker.option_chain(expirations[0])
        calls = chain.calls
        puts = chain.puts
        total_call_vol = int(calls["volume"].sum()) if "volume" in calls.columns else 0
        total_put_vol = int(puts["volume"].sum()) if "volume" in puts.columns else 0
        pcr = round(total_put_vol / total_call_vol, 4) if total_call_vol > 0 else None
        return {
            "available": True,
            "expiration": expirations[0],
            "put_call_ratio": pcr,
            "total_call_volume": total_call_vol,
            "total_put_volume": total_put_vol,
        }
    except Exception as e:
        return {"available": False, "reason": str(e)}


POSITIVE_WORDS = {
    "surge", "soar", "rally", "gain", "rise", "beat", "record", "strong",
    "profit", "growth", "bullish", "upgrade", "outperform", "positive",
    "partnership", "contract", "win", "launch", "expand", "milestone",
}
NEGATIVE_WORDS = {
    "fall", "drop", "decline", "loss", "miss", "weak", "bearish", "downgrade",
    "underperform", "negative", "risk", "concern", "fear", "sell", "cut",
    "warn", "layoff", "lawsuit", "delay", "failure", "crash",
}


def _score_headline(title: str) -> float:
    words = set(title.lower().split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    if pos + neg == 0:
        return 0.0
    return round((pos - neg) / (pos + neg), 2)


def _rss_fetch(url: str, publisher_fallback: str = "", limit: int = 15) -> list:
    """Fetch and parse an RSS feed, returning a list of headline dicts."""
    try:
        req = urllib.request.Request(url, headers=_RSS_HEADERS)
        with urllib.request.urlopen(req, timeout=_RSS_TIMEOUT) as resp:
            raw = resp.read()
        root = ET.fromstring(raw)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)
        results = []
        for item in items[:limit]:
            def _t(tag):
                el = item.find(tag)
                return html.unescape(el.text.strip()) if el is not None and el.text else ""
            title = _t("title") or _t("atom:title")
            link  = _t("link")  or _t("atom:link") or (item.find("atom:link", ns).get("href","") if item.find("atom:link", ns) is not None else "")
            pub   = _t("pubDate") or _t("published") or _t("atom:published")
            source_el = item.find("source")
            publisher = (source_el.text.strip() if source_el is not None and source_el.text else "") or publisher_fallback
            # Parse pub date to unix timestamp
            pub_ts = 0
            if pub:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_ts = int(parsedate_to_datetime(pub).timestamp())
                except Exception:
                    try:
                        from datetime import datetime, timezone
                        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
                            try:
                                pub_ts = int(datetime.strptime(pub[:25], fmt).replace(tzinfo=timezone.utc).timestamp())
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pub_ts = 0
            if title:
                results.append({
                    "title": title,
                    "publisher": publisher,
                    "url": link,
                    "published_at": pub_ts,
                    "sentiment_score": _score_headline(title),
                })
        return results
    except Exception:
        return []


def _finviz_news(symbol: str, limit: int = 10) -> list:
    """Scrape Finviz news table for a ticker (no API key required)."""
    try:
        url = f"https://finviz.com/quote.ashx?t={symbol}&p=d"
        req = urllib.request.Request(url, headers={**_RSS_HEADERS, "Referer": "https://finviz.com/"})
        with urllib.request.urlopen(req, timeout=_RSS_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        # Extract rows from the news table
        pattern = _re.compile(
            r'class="news-link-left[^"]*"[^>]*href="([^"]+)"[^>]*>([^<]+)</a>.*?'
            r'<span[^>]*>([^<]+)</span>',
            _re.DOTALL
        )
        results = []
        for m in pattern.finditer(body):
            url_val, title, publisher = m.group(1), m.group(2).strip(), m.group(3).strip()
            title = html.unescape(title)
            results.append({
                "title": title,
                "publisher": publisher,
                "url": url_val,
                "published_at": 0,
                "sentiment_score": _score_headline(title),
            })
            if len(results) >= limit:
                break
        return results
    except Exception:
        return []


def _yf_news(symbol: str, limit: int = 10) -> list:
    """Pull news from yfinance (Yahoo Finance internal API)."""
    try:
        ticker = yf.Ticker(symbol)
        items = ticker.news or []
        results = []
        for item in items[:limit]:
            title = item.get("title", "")
            link = item.get("link", "") or item.get("url", "")
            publisher = item.get("publisher", "")
            if not publisher and isinstance(item.get("source"), dict):
                publisher = item["source"].get("name", "")
            pub_time = item.get("providerPublishTime") or item.get("pubDate") or item.get("timestamp", 0)
            try:
                pub_time = int(pub_time)
            except (TypeError, ValueError):
                pub_time = 0
            if title:
                results.append({
                    "title": title,
                    "publisher": publisher or "Yahoo Finance",
                    "url": link,
                    "published_at": pub_time,
                    "sentiment_score": _score_headline(title),
                })
        return results
    except Exception:
        return []


def _dedupe(headlines: list) -> list:
    """Remove duplicate headlines by normalised title."""
    seen, out = set(), []
    for h in headlines:
        key = _re.sub(r"[^a-z0-9]", "", h.get("title", "").lower())[:60]
        if key and key not in seen:
            seen.add(key)
            out.append(h)
    return out


def _fetch_news() -> dict:
    YF_RSS_NBIS  = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=NBIS&region=US&lang=en-US"
    YF_RSS_NVDA  = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=NVDA&region=US&lang=en-US"
    GNEWS_NBIS   = "https://news.google.com/rss/search?q=Nebius+NBIS+stock&hl=en-US&gl=US&ceid=US:en"
    GNEWS_NVDA   = "https://news.google.com/rss/search?q=NVDA+Nvidia+AI+chips&hl=en-US&gl=US&ceid=US:en"
    GNEWS_AI     = "https://news.google.com/rss/search?q=AI+chips+semiconductor+GPU&hl=en-US&gl=US&ceid=US:en"
    REUTERS_TECH = "https://feeds.reuters.com/reuters/technologyNews"
    MARKETWATCH  = "https://feeds.marketwatch.com/marketwatch/topstories/"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            "yf_nbis":   ex.submit(_yf_news, "NBIS", 10),
            "yf_nvda":   ex.submit(_yf_news, "NVDA", 10),
            "rss_nbis":  ex.submit(_rss_fetch, YF_RSS_NBIS, "Yahoo Finance", 15),
            "rss_nvda":  ex.submit(_rss_fetch, YF_RSS_NVDA, "Yahoo Finance", 15),
            "g_nbis":    ex.submit(_rss_fetch, GNEWS_NBIS,  "Google News",   15),
            "g_nvda":    ex.submit(_rss_fetch, GNEWS_NVDA,  "Google News",   15),
            "g_ai":      ex.submit(_rss_fetch, GNEWS_AI,    "Google News",   10),
            "reuters":   ex.submit(_rss_fetch, REUTERS_TECH,"Reuters",       10),
            "mw":        ex.submit(_rss_fetch, MARKETWATCH, "MarketWatch",   10),
            "fviz_nbis": ex.submit(_finviz_news, "NBIS", 10),
            "fviz_nvda": ex.submit(_finviz_news, "NVDA", 10),
        }
        results = {k: f.result() for k, f in futs.items()}

    nbis_raw = (results["yf_nbis"] + results["rss_nbis"] +
                results["g_nbis"] + results["fviz_nbis"])
    nvda_raw = (results["yf_nvda"] + results["rss_nvda"] +
                results["g_nvda"] + results["fviz_nvda"])
    sector_raw = results["g_ai"] + results["reuters"] + results["mw"]

    nbis_headlines = sorted(_dedupe(nbis_raw), key=lambda h: h["published_at"], reverse=True)[:15]
    nvda_headlines = sorted(_dedupe(nvda_raw), key=lambda h: h["published_at"], reverse=True)[:15]
    sector_headlines = sorted(_dedupe(sector_raw), key=lambda h: h["published_at"], reverse=True)[:10]

    def _avg(items):
        scores = [h["sentiment_score"] for h in items if "sentiment_score" in h]
        return round(sum(scores) / len(scores), 2) if scores else 0.0

    nbis_avg   = _avg(nbis_headlines)
    nvda_avg   = _avg(nvda_headlines)
    sector_avg = _avg(sector_headlines)
    overall    = round((nbis_avg + nvda_avg + sector_avg) / 3, 2)

    return {
        "nbis_headlines":    nbis_headlines,
        "nvda_headlines":    nvda_headlines,
        "sector_headlines":  sector_headlines,
        "nbis_sentiment_avg":   nbis_avg,
        "nvda_sentiment_avg":   nvda_avg,
        "sector_sentiment_avg": sector_avg,
        "overall_sentiment":    overall,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def gather_all() -> dict:
    """Gather all market intelligence concurrently and return a nested dict."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        fut_nbis = executor.submit(_fetch_nbis_technicals)
        fut_corr = executor.submit(_fetch_correlated)
        fut_opts = executor.submit(_fetch_options)
        fut_news = executor.submit(_fetch_news)

        nbis = fut_nbis.result()
        correlated = fut_corr.result()
        options = fut_opts.result()
        news = fut_news.result()

    return {
        "nbis": nbis,
        "correlated": correlated,
        "options": options,
        "news": news,
    }


if __name__ == "__main__":
    import json
    print(json.dumps(gather_all(), indent=2))
