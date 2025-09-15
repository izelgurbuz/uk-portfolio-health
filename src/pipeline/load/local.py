import pandas as pd

from ..utils.io import processed_dir


def write_parquet(df: pd.DataFrame, name: str):
    path = processed_dir() / f"{name}.parquet"
    df.to_parquet(path, index=False)
    return path
