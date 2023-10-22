"""

    bookscrape.utils.py
    ~~~~~~~~~~~~~~~~~~~
    Projec's utilities

    @author: z33k

"""
from functools import wraps
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

import pandas as pd
import requests
from bs4 import BeautifulSoup
from contexttimer import Timer

from bookscrape.constants import PathLike, REQUEST_TIMOUT, T
from bookscrape.utils.check_type import type_checker


def timed(func: Callable) -> Callable:
    """Add time measurement to the decorated operation.

    Returns:
        the decorated function
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        with Timer() as t:
            result = func(*args, **kwargs)
        print(f"Completed in {t.elapsed:.3f} seconds")
        return result
    return wrapper


@timed
@type_checker(str)
def getsoup(url: str) -> BeautifulSoup:
    """Return BeautifulSoup object based on ``url``.

    Args:
        url: URL string

    Returns:
        a BeautifulSoup object
    """
    print(f"Requesting: {url!r}")
    markup = requests.get(url, timeout=REQUEST_TIMOUT).text
    return BeautifulSoup(markup, "lxml")


@type_checker(pd.DataFrame)
def first_df_row_as_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make first row of ``df`` its columns.
    """
    return df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)


@type_checker(str)
def extract_float(text: str) -> float:
    text = "".join([char for char in text if char.isdigit() or char in ",."])
    return float(text.replace(",", "."))


@type_checker(str)
def extract_int(text: str) -> int:
    text = "".join([char for char in text if char.isdigit()])
    return int(text)


def from_iterable(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Optional[T]:
    """Return item from ``iterable`` based on ``predicate`` or ``None``, if it cannot be found.
    """
    return next((item for item in iterable if predicate(item)), None)


@type_checker(PathLike)
def getdir(path: PathLike) -> Path:
    """Return a directory at ``path`` creating it (and all its needed parents) if missing.
    """
    dir_ = Path(path)
    if not dir_.exists():
        print(f"Creating missing directory at: '{dir_.resolve()}'...")
        dir_.mkdir(parents=True, exist_ok=True)
    else:
        if dir_.is_file():
            raise NotADirectoryError(f"Not a directory: '{dir_.resolve()}'")
    return dir_


@type_checker(PathLike)
def getfile(path: PathLike, ext="") -> Path:
    """Return a file at ``path``.
    """
    f = Path(path)
    if not f.is_file():
        raise FileNotFoundError(f"Not a file: '{f.resolve()}'")
    if ext and not f.suffix.lower() == ext.lower():
        raise ValueError(f"Not a {ext!r} file")
    return f


def is_increasing(seq: Sequence[int | float]) -> bool:
    if len(seq) < 2:
        return False
    return all(seq[i] > seq[i-1] for i, _ in enumerate(seq, start=1) if i < len(seq))
