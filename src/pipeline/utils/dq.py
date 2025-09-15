from typing import Dict

import pandas as pd


def dq_report(df: pd.DataFrame, pk_cols=None) -> Dict:
    rep = {
        "rows": len(df),
        "cols": list(df.columns),
        "null_counts": df.isna().sum().to_dict(),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
    }
    if pk_cols:
        rep["dupe_pk_rows"] = int(df.duplicated(pk_cols).sum())
    return rep
