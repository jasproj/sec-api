import os
import time
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

import httpx
from fastapi import FastAPI, HTTPException, Path, Query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sec-api")

SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "SEC-API Jason Dudney jasondudney@gmail.com")
SEC_HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}

TICKER_MAP_TTL = 24 * 3600
FACTS_TTL = 6 * 3600
CONCEPT_TTL = 6 * 3600
HTTP_TIMEOUT = 10.0

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

client: Optional[httpx.AsyncClient] = None
ticker_map: Dict[str, Dict[str, Any]] = {}
ticker_map_loaded_at: float = 0.0
data_cache: Dict[str, Dict[str, Any]] = {}


def pad_cik(cik: str) -> str:
    digits = "".join(filter(str.isdigit, str(cik)))
    if not digits:
        raise HTTPException(status_code=400, detail="Invalid CIK: must contain digits.")
    return digits.zfill(10)


async def load_ticker_map(force: bool = False) -> Dict[str, Dict[str, Any]]:
    global ticker_map, ticker_map_loaded_at
    now = time.time()
    if ticker_map and not force and (now - ticker_map_loaded_at) < TICKER_MAP_TTL:
        return ticker_map
    try:
        resp = await client.get(TICKERS_URL)
        resp.raise_for_status()
        raw = resp.json()
        new_map: Dict[str, Dict[str, Any]] = {}
        for item in raw.values():
            t = str(item["ticker"]).upper().strip()
            new_map[t] = {"cik": int(item["cik_str"]), "name": item.get("title", "")}
        ticker_map = new_map
        ticker_map_loaded_at = now
        logger.info("Loaded %d tickers from SEC.", len(ticker_map))
    except Exception as e:
        logger.error("Ticker map load failed: %s", e)
    return ticker_map


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = httpx.AsyncClient(headers=SEC_HEADERS, timeout=HTTP_TIMEOUT)
    await load_ticker_map(force=True)
    yield
    await client.aclose()


app = FastAPI(
    title="SEC Company Facts API",
    version="1.0.0",
    description="Ticker-to-CIK lookup and SEC EDGAR company financials in clean JSON.",
    lifespan=lifespan,
)


async def fetch_sec(url: str, ttl: int) -> Dict[str, Any]:
    now = time.time()
    hit = data_cache.get(url)
    if hit and hit["expiry"] > now:
        return hit["data"]
    try:
        resp = await client.get(url)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="SEC EDGAR request timed out.")
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Not found at SEC EDGAR.")
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=resp.status_code, detail=f"SEC EDGAR error: {e}")
    data = resp.json()
    data_cache[url] = {"expiry": now + ttl, "data": data}
    return data


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "ticker_map_loaded": len(ticker_map) > 0,
        "tickers": len(ticker_map),
        "cache_entries": len(data_cache),
    }


@app.get("/lookup")
async def lookup(ticker: str = Query(..., description="Stock ticker symbol, e.g. AAPL")):
    t = ticker.upper().strip()
    if not ticker_map:
        await load_ticker_map(force=True)
        if not ticker_map:
            raise HTTPException(status_code=503, detail="Ticker map unavailable; try again shortly.")
    entry = ticker_map.get(t)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Ticker '{t}' not found.")
    padded = str(entry["cik"]).zfill(10)
    return {"ticker": t, "cik": entry["cik"], "cik_padded": f"CIK{padded}", "name": entry["name"]}


@app.get("/facts/{cik}")
async def company_facts(cik: str = Path(..., description="Raw or padded CIK, e.g. 320193")):
    padded = pad_cik(cik)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json"
    return await fetch_sec(url, FACTS_TTL)


@app.get("/concept/{cik}/{tag}")
async def company_concept(
    cik: str = Path(..., description="Raw or padded CIK, e.g. 320193"),
    tag: str = Path(..., description="US-GAAP XBRL tag, e.g. Revenues, Assets, EarningsPerShareBasic"),
):
    padded = pad_cik(cik)
    url = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{padded}/us-gaap/{tag.strip()}.json"
    return await fetch_sec(url, CONCEPT_TTL)
