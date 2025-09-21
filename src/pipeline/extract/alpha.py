import os
import time
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

BASE = "https://www.alphavantage.co/query"
BENCHMARK_SYMBOL = "SPY"


def _alpha_get(params, max_retries=5, backoff=2):
    headers = {"User-Agent": "uk-portfolio-health/1.0"}
    for attempt in range(max_retries):
        resp = requests.get(BASE, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if not data or "Note" in data or "Error Message" in data:
                # API limit hit or invalid request
                if attempt < max_retries - 1:
                    time.sleep(backoff * (attempt + 1))
                    continue
                raise RuntimeError(f"Alpha Vantage error: {data}")
            return data
        time.sleep(backoff * (attempt + 1))
    resp.raise_for_status()


def fetch_symbol_daily(sym: str, start_date: str) -> pd.DataFrame:
    load_dotenv()
    API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": sym.upper(),
        "outputsize": "full",
        "apikey": API_KEY,
    }

    data = _alpha_get(params)
    if "Time Series (Daily)" not in data:
        raise RuntimeError(f"Unexpected Alpha Vantage response for {sym}: {data}")

    records = []
    for date_str, values in data["Time Series (Daily)"].items():
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        if sym == BENCHMARK_SYMBOL:
            records.append(
                {
                    "symbol": sym.upper(),
                    "date": date_obj,
                    "close": float(values["4. close"]),
                }
            )
        else:
            records.append(
                {
                    "symbol": sym.upper(),
                    "date": date_obj,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["6. volume"]),
                    "source": "alphavantage",
                }
            )

    df = pd.DataFrame(records)

    # Filter by start_date
    sd = pd.to_datetime(start_date).date()
    df = df[df["date"] >= sd].reset_index(drop=True)

    return (
        df[["symbol", "date", "close"]]
        if sym == BENCHMARK_SYMBOL
        else df[["symbol", "date", "open", "high", "low", "close", "volume", "source"]]
    )


def fetch_equities(symbols, start_date, last_loaded=None):
    parts = [fetch_symbol_daily(s, start_date) for s in symbols]
    df = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["symbol", "date"])

    if last_loaded:
        df = df[df["date"] > pd.to_datetime(last_loaded).date()]

    return df


def fetch_fx_pair(pair: str, start_date: str) -> pd.DataFrame:
    load_dotenv()
    API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
    pair = pair.strip().upper()
    if len(pair) != 6:
        raise ValueError(f"Invalid FX pair format: {pair}, expected like 'USDGBP'")

    from_curr, to_curr = pair[0:3], pair[3:6]

    params = {
        "function": "FX_DAILY",
        "from_symbol": from_curr,
        "to_symbol": to_curr,
        "outputsize": "full",
        "apikey": API_KEY,
    }

    data = _alpha_get(params)
    if "Time Series FX (Daily)" not in data:
        raise RuntimeError(f"Unexpected Alpha Vantage FX response for {pair}: {data}")

    records = []
    for date_str, values in data["Time Series FX (Daily)"].items():
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        records.append(
            {
                "pair": pair,
                "date": date_obj,
                "rate": float(values["4. close"]),  # use close price as standard
                "source": "alphavantage",
            }
        )

    df = pd.DataFrame(records)

    sd = pd.to_datetime(start_date).date()
    df = df[df["date"] >= sd].reset_index(drop=True)

    return df[["pair", "date", "rate", "source"]]
