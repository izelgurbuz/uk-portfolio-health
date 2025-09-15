import json
import os
from typing import List

from dotenv import load_dotenv

from ..extract.ecb_fx import fetch_fx_all
from ..extract.stooq import fetch_equities
from ..load.local import write_parquet
from ..transform.cleaning import clean_equities, clean_fx
from ..utils.dq import dq_report
from ..utils.logging import log


def parse_symbols(env: str) -> List[str]:
    return [s.strip() for s in env.split(",") if s.strip()]


def main():
    load_dotenv()
    symbols = parse_symbols(os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL"))
    start = os.getenv("START_DATE", "2018-01-01")

    log(f"fetching equities for {symbols} from {start}")
    eq = fetch_equities(symbols, start)
    eqc = clean_equities(eq)
    eq_p = write_parquet(eqc, "equity_daily")

    log("fetching fx from ECB")
    fx = fetch_fx_all()
    fxc = clean_fx(fx)
    fx_p = write_parquet(fxc, "fx_daily")

    log(
        f"[DQ] equities: {json.dumps(dq_report(eqc, pk_cols=['SYMBOL', 'DATE']), indent=2)}"
    )
    log(f"[DQ] fx: {json.dumps(dq_report(fxc, pk_cols=['PAIR', 'DATE']), indent=2)}")
    log(f"Parquet written: {eq_p} and {fx_p}")


if __name__ == "__main__":
    main()
