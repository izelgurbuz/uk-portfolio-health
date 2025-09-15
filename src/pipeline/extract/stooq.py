import io
import time
from typing import List

import pandas as pd
import requests

BASE = "https://stooq.com/q/d/l/"


def _stooq_symbol(sym: str) -> str:
    s = sym.strip().lower()
    if not s.endswith(".us"):
        s = f"{s}.us"
    return s


def fetch_symbol_daily(sym: str, start_date: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV for a US equity from Stooq.
    URL pattern: ?s=aapl.us&i=d
    """
    s = _stooq_symbol(sym)
    url = f"{BASE}?s={s}&i=d"
    headers = {"User-Agent": "uk-portfolio-health/1.0"}

    for attempt in range(5):
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200 and r.text.strip():
            break
        time.sleep(1 + attempt)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text))

    df.rename(columns=str.lower, inplace=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    num_cols = ["open", "high", "low", "close"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["symbol"] = sym.upper()
    df["source"] = "stooq"

    sd = pd.to_datetime(start_date).date()
    df = df[df["date"] >= sd].reset_index(drop=True)
    return df[["symbol", "date", "open", "high", "low", "close", "volume", "source"]]


def fetch_equities(symbols: List[str], start_date: str) -> pd.DataFrame:
    parts = [fetch_symbol_daily(s, start_date) for s in symbols]
    df = pd.concat(parts, ignore_index=True)
    # de-dupe just in case
    df = df.drop_duplicates(subset=["symbol", "date"]).reset_index(drop=True)
    return df
