import os
from pathlib import Path


def data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "./data")).resolve()


def raw_dir() -> Path:
    d = data_dir() / "raw"
    d.mkdir(parents=True, exist_ok=True)
    return d


def processed_dir() -> Path:
    d = data_dir() / "processed"
    d.mkdir(parents=True, exist_ok=True)
    return d
