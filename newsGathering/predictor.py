"""
predictor.py — Claude API integration for NBIS trading predictions
"""

import os
import json
import re
import anthropic


_CLIENT = None


def _get_client() -> anthropic.Anthropic:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _CLIENT


def _build_prompt(data: dict) -> str:
    nbis = data.get("nbis", {})
    corr = data.get("correlated", {})
    opts = data.get("options", {})
    news = data.get("news", {})

    nbis_headlines = news.get("nbis_headlines", [])[:5]
    nvda_headlines = news.get("nvda_headlines", [])[:3]

    headline_text = "\n".join(
        f"  - [{h.get('publisher','')}] {h.get('title','')} (sentiment: {h.get('sentiment_score',0)})"
        for h in nbis_headlines if "title" in h
    )
    nvda_headline_text = "\n".join(
        f"  - [{h.get('publisher','')}] {h.get('title','')} (sentiment: {h.get('sentiment_score',0)})"
        for h in nvda_headlines if "title" in h
    )

    corr_text = "\n".join(
        f"  {sym}: price={v.get('price')}, change={v.get('change_pct')}%, 5d_return={v.get('5d_return')}%"
        for sym, v in corr.items() if isinstance(v, dict) and "price" in v
    )

    opts_text = (
        f"Put/Call ratio: {opts.get('put_call_ratio')}, call_vol={opts.get('total_call_volume')}, put_vol={opts.get('total_put_volume')}"
        if opts.get("available")
        else "Options data unavailable"
    )

    return f"""You are a quantitative analyst providing a structured trading outlook for Nebius (NBIS) stock.

## NBIS Technical Data
- Price: {nbis.get('price')} | Change: {nbis.get('change_pct')}%
- RSI(14): {nbis.get('rsi_14')} → {nbis.get('rsi_signal')}
- MACD: value={nbis.get('macd_value')}, signal={nbis.get('macd_signal')}, histogram={nbis.get('macd_histogram')}, cross={nbis.get('macd_cross')}
- Bollinger %B: {nbis.get('bb_pct_b')} → {nbis.get('bb_position')}
- MA20: {nbis.get('ma_20')} ({nbis.get('price_vs_ma20')}% vs price) | MA50: {nbis.get('ma_50')} ({nbis.get('price_vs_ma50')}% vs price)
- MA Cross: {nbis.get('ma_cross')}
- Volume ratio vs 20d avg: {nbis.get('volume_ratio')}x
- 52w High: {nbis.get('high_52w')} | 52w Low: {nbis.get('low_52w')}

## Correlated Assets
{corr_text}

## Options Flow
{opts_text}

## News Sentiment
NBIS headlines:
{headline_text}

NVDA headlines:
{nvda_headline_text}

Overall sentiment: NBIS avg={news.get('nbis_sentiment_avg')}, NVDA avg={news.get('nvda_sentiment_avg')}, combined={news.get('overall_sentiment')}

## Instructions
Analyze all of the above and return ONLY a JSON object with exactly these fields:
{{
  "direction": "bullish" | "bearish" | "neutral",
  "confidence": <integer 0-100>,
  "time_horizon_days": <integer, e.g. 5>,
  "summary": "<2-3 sentence plain English outlook>",
  "key_drivers": ["<driver1>", "<driver2>", "<driver3>"],
  "risk_factors": ["<risk1>", "<risk2>"],
  "technical_bias": "bullish" | "bearish" | "neutral",
  "sentiment_bias": "bullish" | "bearish" | "neutral",
  "sector_bias": "bullish" | "bearish" | "neutral",
  "price_target_low": <number>,
  "price_target_high": <number>
}}

Return ONLY the JSON. No markdown, no explanation, no code fences."""


def _parse_response(text: str) -> dict:
    text = text.strip()
    # Strip markdown code fences if present
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Regex fallback: find first {...} block
        match = re.search(r"\{[\s\S]+\}", text)
        if match:
            return json.loads(match.group())
        raise ValueError(f"Could not parse Claude response as JSON: {text[:200]}")


def predict(data: dict) -> dict:
    """Call Claude API with gathered data and return parsed prediction dict."""
    client = _get_client()
    prompt = _build_prompt(data)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = message.content[0].text
    return _parse_response(raw)
