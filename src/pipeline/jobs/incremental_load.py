import os

import pandas as pd
from dotenv import load_dotenv

from pipeline.load.local import write_partitioned

from ..extract.ecb_fx import fetch_fx_all
from ..extract.stooq import fetch_equities
from ..load.snowflake_loader import (
    get_last_loaded_date,
    update_last_loaded_date,
    write_df,
)
from ..transform.cleaning import clean_equities, clean_fx
from ..utils.logging import log


def run_incremental():
    load_dotenv()
    symbols = [
        s.strip()
        for s in os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL").split(",")
        if s.strip()
    ]
    start = os.getenv("START_DATE", "2018-01-01")

    # -----Equities-----

    last_eq_date = get_last_loaded_date("equities")
    log(f"Last equities load date: {last_eq_date}")
    eq_df = clean_equities(fetch_equities(symbols, start, last_loaded=last_eq_date))
    if len(eq_df) == 0:
        log("No new equities to load.")
    else:
        write_partitioned(eq_df, "equity_daily", ["symbol", "date"])
        write_df(eq_df, table="EQUITY_DAILY", schema="RAW")
        max_date = eq_df["DATE"].max().strftime("%Y-%m-%d")
        update_last_loaded_date("equities", max_date)
        log(f"Equities loaded through {max_date}")

    # -------FX-------
    last_fx = get_last_loaded_date("fx")
    log(f"Last FX load date: {last_fx}")
    fx_df = clean_fx(fetch_fx_all())

    if last_fx:
        fx_df = fx_df[fx_df["DATE"] > pd.to_datetime(last_fx).date()]

    if len(fx_df) == 0:
        log("No new FX data to load.")
    else:
        write_partitioned(fx_df, "fx_daily", ["pair", "date"])
        write_df(fx_df, table="FX_DAILY", schema="RAW")
        max_date_fx = fx_df["DATE"].max().strftime("%Y-%m-%d")
        update_last_loaded_date("fx", max_date_fx)
        log(f"FX loaded through {max_date_fx}")


def main():
    log("Starting incremental load job")
    run_incremental()
    log("Incremental load job completed")


if __name__ == "__main__":
    main()
