"""
app.py — Flask server for NBIS Market Intelligence Dashboard
"""

import json
import queue
import threading
import time
from datetime import datetime, timezone

from flask import Flask, Response, jsonify

from gatherer import gather_all
from predictor import predict

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Module-level analysis cache
# ---------------------------------------------------------------------------
_cache = {
    "data": None,
    "expires": 0.0,
    "status": "idle",   # idle | running | ready | error
    "progress": [],     # list of str messages
    "error": None,
}
_cache_lock = threading.Lock()
CACHE_TTL = 300  # seconds


# ---------------------------------------------------------------------------
# Background analysis worker
# ---------------------------------------------------------------------------

def _run_analysis():
    def log(msg: str):
        with _cache_lock:
            _cache["progress"].append(msg)

    try:
        log("Gathering NBIS technical data...")
        with _cache_lock:
            _cache["status"] = "running"
            _cache["error"] = None

        raw = gather_all()
        log("Data gathered. Running AI analysis...")

        prediction = predict(raw)
        log("AI analysis complete. Rendering results...")

        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "nbis": raw.get("nbis", {}),
            "correlated": raw.get("correlated", {}),
            "options": raw.get("options", {}),
            "news": raw.get("news", {}),
            "prediction": prediction,
        }

        with _cache_lock:
            _cache["data"] = result
            _cache["expires"] = time.time() + CACHE_TTL
            _cache["status"] = "ready"
            _cache["progress"].append("done")

    except Exception as e:
        with _cache_lock:
            _cache["status"] = "error"
            _cache["error"] = str(e)
            _cache["progress"].append(f"error: {e}")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    with open("index.html", encoding="utf-8") as f:
        return f.read()


@app.route("/api/latest")
def api_latest():
    with _cache_lock:
        return jsonify({
            "status": _cache["status"],
            "data": _cache["data"],
            "error": _cache["error"],
        })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    with _cache_lock:
        if _cache["status"] == "running":
            return jsonify({"status": "running", "message": "Analysis already in progress"}), 409
        if _cache["status"] == "ready" and time.time() < _cache["expires"]:
            return jsonify({"status": "ready", "data": _cache["data"]})
        # Reset and start fresh
        _cache["status"] = "running"
        _cache["progress"] = []
        _cache["error"] = None

    t = threading.Thread(target=_run_analysis, daemon=True)
    t.start()
    return jsonify({"status": "started"})


@app.route("/api/stream")
def api_stream():
    def generate():
        sent = 0
        last_keepalive = time.time()
        while True:
            with _cache_lock:
                progress = list(_cache["progress"])
                status = _cache["status"]
                error = _cache["error"]

            # Send any new progress messages
            while sent < len(progress):
                msg = progress[sent]
                sent += 1
                if msg == "done":
                    yield f"event: done\ndata: ready\n\n"
                    return
                elif msg.startswith("error:"):
                    yield f"event: done\ndata: error\n\n"
                    return
                else:
                    yield f"data: {json.dumps({'message': msg})}\n\n"

            # Terminal states with no pending messages
            if status in ("ready", "error") and sent >= len(progress):
                yield f"event: done\ndata: {status}\n\n"
                return

            # Keepalive comment every 30s
            if time.time() - last_keepalive > 30:
                yield ": keepalive\n\n"
                last_keepalive = time.time()

            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
