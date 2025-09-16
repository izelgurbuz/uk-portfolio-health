import pandas as pd

from ..utils.io import processed_dir


def write_parquet(df: pd.DataFrame, name: str):
    path = processed_dir() / f"{name}.parquet"
    df.to_parquet(path, index=False)
    return path


def write_partitioned(df: pd.DataFrame, base_name: str, partition_cols: list[str]):
    """
    Write DataFrame to a partitioned directory tree based on partition_cols.
    """
    base = processed_dir() / base_name
    # combo, group : (key, sub-DataFrame) , combo: ("AAPL", 2024)
    # combo is tuple of values for the partition columns
    for combo, group in df.groupby(partition_cols):
        parts = [f"{col}={val}" for col, val in zip(partition_cols, combo)]
        path = base.joinpath(*parts)
        path.mkdir(parents=True, exist_ok=True)
        filename = f"{base_name}.parquet"
        group.to_parquet(path / filename, index=False)
    return base
