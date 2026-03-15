# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
python app.py
# Server starts at http://localhost:5000

# Stop the server (Windows)
cmd.exe /C "taskkill /F /IM python3.13.exe"
```

## Architecture

Two-file app: a Flask backend (`app.py`) and a single-page frontend (`index.html`).

**`app.py`**
- `/` — serves `index.html` directly via `open("index.html")`
- `/api/history?timeframe=1D|1W|1M|1Y` — returns `{labels, prices, last_close, market_open}` from yfinance; results are cached in-memory with per-timeframe TTLs (60s / 300s / 3600s / 86400s)
- `/api/stream` — Server-Sent Events endpoint; yields a JSON quote payload every 5 seconds
- A daemon thread (`_refresh_quote`) calls `yf.Ticker("NBIS").fast_info` every 15 s and writes into the module-level `_quote` dict; SSE clients read from that dict
- On yfinance errors the thread sets `stale: true` and serves the last known quote; history endpoints fall back to cached data silently

**`index.html`**
- Chart.js (CDN) renders a line chart in dark theme (`#131722`)
- `EventSource("/api/stream")` drives the live price ticker; auto-reconnects on error
- `fetch("/api/history?timeframe=...")` loads chart data on page load and on timeframe button clicks
- Only the 1D chart appends live SSE price points in real time (capped at 500 points); 1W / 1M / 1Y charts are static after load

## Git / GitHub

Every change must be committed with a clear message and pushed to `origin master`:
```bash
git add <files>
git commit -m "short description"
git push
```
Remote: https://github.com/AnthonyVergottis/nbis-live
