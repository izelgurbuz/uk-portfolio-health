import io
import time
from typing import Tuple

import pandas as pd
import requests

ECB_HIST = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.csv"


def fetch_eur_hist() -> pd.DataFrame:
    headers = {"User-Agent": "uk-portfolio-health/1.0"}
    for attempt in range(5):
        r = requests.get(ECB_HIST, headers=headers, timeout=30)
        if r.status_code == 200 and r.text.strip():
            break
        time.sleep(1 + attempt)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text))
    df.rename(columns={"Date": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    return df


def usd_gbp_series(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.sort_values("date").reset_index(drop=True)

    eur_usd = pd.to_numeric(df["USD"], errors="coerce")
    eur_gbp = pd.to_numeric(df["GBP"], errors="coerce")

    out = df[["date"]].copy()
    out["EURUSD"] = eur_usd
    out["EURGBP"] = eur_gbp

    out["USDGBP"] = out["EURGBP"] / out["EURUSD"]

    usd_eur = out[["date", "EURUSD"]].rename(columns={"EURUSD": "rate"})
    usd_eur["pair"] = "USD/EUR"

    gbp_eur = out[["date", "EURGBP"]].rename(columns={"EURGBP": "rate"})
    gbp_eur["pair"] = "GBP/EUR"

    usd_gbp = out[["date", "USDGBP"]].rename(columns={"USDGBP": "rate"})
    usd_gbp["pair"] = "USD/GBP"

    def tidy(x):
        x["source"] = "ecb"
        # drop NaNs
        x = x.dropna(subset=["rate"]).copy()
        return x[["pair", "date", "rate", "source"]]

    return tidy(usd_eur), tidy(gbp_eur), tidy(usd_gbp)


def fetch_fx_all() -> pd.DataFrame:
    hist = fetch_eur_hist()
    a, b, c = usd_gbp_series(hist)
    return (
        pd.concat([a, b, c], ignore_index=True)
        .sort_values(["pair", "date"])
        .reset_index(drop=True)
    )
