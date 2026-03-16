"""
gatherer.py — collects NBIS market intelligence from yfinance
"""

import time
import concurrent.futures
import yfinance as yf
import numpy as np
import pandas as pd


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


def _fetch_news() -> dict:
    def _get_headlines(symbol: str, limit: int = 10):
        try:
            ticker = yf.Ticker(symbol)
            items = ticker.news or []
            headlines = []
            for item in items[:limit]:
                title = item.get("title", "")
                link = item.get("link", "") or item.get("url", "")
                publisher = item.get("publisher", "") or item.get("source", {}).get("name", "") if isinstance(item.get("source"), dict) else item.get("publisher", "")
                pub_time = item.get("providerPublishTime") or item.get("pubDate") or item.get("timestamp", 0)
                try:
                    pub_time = int(pub_time)
                except (TypeError, ValueError):
                    pub_time = 0
                score = _score_headline(title)
                headlines.append({
                    "title": title,
                    "publisher": publisher,
                    "url": link,
                    "published_at": pub_time,
                    "sentiment_score": score,
                })
            return headlines
        except Exception as e:
            return [{"error": str(e)}]

    nbis_headlines = _get_headlines("NBIS")
    nvda_headlines = _get_headlines("NVDA")

    nbis_scores = [h["sentiment_score"] for h in nbis_headlines if "sentiment_score" in h]
    nvda_scores = [h["sentiment_score"] for h in nvda_headlines if "sentiment_score" in h]
    nbis_avg = round(sum(nbis_scores) / len(nbis_scores), 2) if nbis_scores else 0.0
    nvda_avg = round(sum(nvda_scores) / len(nvda_scores), 2) if nvda_scores else 0.0
    overall = round((nbis_avg + nvda_avg) / 2, 2)

    return {
        "nbis_headlines": nbis_headlines,
        "nvda_headlines": nvda_headlines,
        "nbis_sentiment_avg": nbis_avg,
        "nvda_sentiment_avg": nvda_avg,
        "overall_sentiment": overall,
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
