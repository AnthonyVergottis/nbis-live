import json
import time
import threading
from datetime import datetime, timezone
from flask import Flask, Response, jsonify, render_template_string, request
import yfinance as yf

app = Flask(__name__)

TICKER = "NBIS"

# Cache: {timeframe: {"data": {...}, "expires": float}}
_history_cache: dict = {}
_history_lock = threading.Lock()

# Live price cache
_quote = {"price": None, "change": None, "change_pct": None, "prev_close": None,
          "market_open": False, "stale": False, "timestamp": None}
_quote_lock = threading.Lock()

TIMEFRAME_CONFIG = {
    "1D": {"period": "1d",  "interval": "1m",  "ttl": 60},
    "1W": {"period": "5d",  "interval": "5m",  "ttl": 300},
    "1M": {"period": "1mo", "interval": "1h",  "ttl": 3600},
    "1Y": {"period": "1y",  "interval": "1d",  "ttl": 86400},
}


def _fetch_history(timeframe: str) -> dict:
    cfg = TIMEFRAME_CONFIG[timeframe]
    ticker = yf.Ticker(TICKER)
    df = ticker.history(period=cfg["period"], interval=cfg["interval"])

    if df.empty:
        # Return last available close
        fallback = ticker.history(period="5d", interval="1d")
        last_close = float(fallback["Close"].iloc[-1]) if not fallback.empty else 0.0
        return {"labels": [], "prices": [], "last_close": last_close, "market_open": False}

    labels = [str(idx) for idx in df.index]
    prices = [round(float(v), 4) for v in df["Close"]]
    return {"labels": labels, "prices": prices, "last_close": prices[-1], "market_open": True}


def _get_history(timeframe: str) -> dict:
    now = time.time()
    with _history_lock:
        cached = _history_cache.get(timeframe)
        if cached and cached["expires"] > now:
            return cached["data"]

    try:
        data = _fetch_history(timeframe)
    except Exception:
        with _history_lock:
            cached = _history_cache.get(timeframe)
            if cached:
                return cached["data"]
        return {"labels": [], "prices": [], "last_close": 0.0, "market_open": False}

    ttl = TIMEFRAME_CONFIG[timeframe]["ttl"]
    with _history_lock:
        _history_cache[timeframe] = {"data": data, "expires": now + ttl}
    return data


def _refresh_quote():
    global _quote
    while True:
        try:
            ticker = yf.Ticker(TICKER)
            info = ticker.fast_info
            price = float(info.last_price) if info.last_price else None
            prev_close = float(info.previous_close) if info.previous_close else None

            if price is not None and prev_close is not None:
                change = round(price - prev_close, 4)
                change_pct = round((change / prev_close) * 100, 4)
            else:
                change = change_pct = None

            # Determine market open via regular market hours
            try:
                market_open = bool(info.regular_market_open)
            except Exception:
                market_open = False

            new_quote = {
                "price": round(price, 4) if price else None,
                "change": change,
                "change_pct": change_pct,
                "prev_close": round(prev_close, 4) if prev_close else None,
                "market_open": market_open,
                "stale": False,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        except Exception:
            new_quote = dict(_quote)
            new_quote["stale"] = True
            new_quote["timestamp"] = datetime.now(timezone.utc).isoformat()

        with _quote_lock:
            _quote = new_quote

        time.sleep(15)


# Start background thread
_bg_thread = threading.Thread(target=_refresh_quote, daemon=True)
_bg_thread.start()


@app.route("/")
def index():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.route("/api/history")
def api_history():
    tf = request.args.get("timeframe", "1D").upper()
    if tf not in TIMEFRAME_CONFIG:
        return jsonify({"error": "Invalid timeframe"}), 400
    data = _get_history(tf)
    return jsonify(data)


@app.route("/api/stream")
def api_stream():
    def generate():
        # Send initial quote immediately
        with _quote_lock:
            q = dict(_quote)
        yield f"data: {json.dumps(q)}\n\n"

        while True:
            time.sleep(5)
            with _quote_lock:
                q = dict(_quote)
            yield f"data: {json.dumps(q)}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
