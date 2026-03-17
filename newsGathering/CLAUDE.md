# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app (requires ANTHROPIC_API_KEY env var)
ANTHROPIC_API_KEY=... python app.py
# Server starts at http://localhost:5001

# Run the gatherer standalone (prints raw JSON to stdout)
python gatherer.py

# Stop the server (Windows)
cmd.exe /C "taskkill /F /IM python3.13.exe"
```

## Architecture

Three-file app: Flask backend (`app.py`), data collection (`gatherer.py`), AI analysis (`predictor.py`), and a single-page frontend (`index.html`).

### Data flow

1. Frontend POSTs to `/api/refresh` to trigger an analysis run.
2. `app.py` spawns a daemon thread that calls `gather_all()` then `predict()`.
3. Frontend opens `/api/stream` (SSE) to receive progress messages; when the `done` event fires, it GETs `/api/latest` for the full result.
4. Results are cached in-memory for 300 s (`CACHE_TTL`); a second `/api/refresh` within TTL returns the cached data immediately.

### `gatherer.py`

Collects four data categories concurrently via `ThreadPoolExecutor`:
- **NBIS technicals** — yfinance 3-month daily history; computes RSI-14, MACD, Bollinger Bands, MA-20/50 cross, volume ratio, 52-week high/low.
- **Correlated assets** — daily close data for NVDA, SMCI, CRWV, SPY, QQQ, VIX; today's % change and 5-day return.
- **Options flow** — nearest expiry put/call ratio and volumes from yfinance.
- **News** — 11 concurrent sources (Yahoo Finance RSS, yfinance news API, Google News RSS, Reuters, MarketWatch, Finviz HTML scrape) for NBIS, NVDA, and sector; deduped and scored with a keyword sentiment function (`_score_headline`).

### `predictor.py`

Formats gathered data into a structured prompt and calls `claude-sonnet-4-6` (`anthropic` SDK). Returns a parsed JSON prediction with fields: `direction`, `confidence`, `time_horizon_days`, `summary`, `key_drivers`, `risk_factors`, `technical_bias`, `sentiment_bias`, `sector_bias`, `price_target_low`, `price_target_high`.

Requires `ANTHROPIC_API_KEY` in the environment.

## Git / GitHub

Every change must be committed and pushed to `origin master`.
