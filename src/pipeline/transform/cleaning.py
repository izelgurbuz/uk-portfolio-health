import pandas as pd


def clean_equities(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Basic sanity: drop rows with missing close or date
    df = df.dropna(subset=["symbol", "date", "close"]).copy()
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    # Deduplicate
    df = df.drop_duplicates(subset=["symbol", "date"]).reset_index(drop=True)
    df.rename(columns=str.upper, inplace=True)
    return df


def clean_fx(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.dropna(subset=["pair", "date", "rate"]).copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df = df.dropna(subset=["rate"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["pair", "date"]).reset_index(drop=True)
    df.rename(columns=str.upper, inplace=True)
    return df
