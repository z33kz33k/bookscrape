"""

    bookscrape.utils.py
    ~~~~~~~~~~~~~~~~~~~
    Script's utilities

    @author: z33k

"""
from functools import wraps
from typing import Callable, Iterable, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from contexttimer import Timer

from bookscrape.constants import REQUEST_TIMOUT, T


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


def first_df_row_as_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make first row of ``df`` its columns.
    """
    return df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)


def extract_float(text: str) -> float:
    text = "".join([char for char in text if char.isdigit() or char in ",."])
    return float(text.replace(",", "."))


def extract_int(text: str) -> int:
    text = "".join([char for char in text if char.isdigit()])
    return int(text)


def from_iterable(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Optional[T]:
    """Return item from ``iterable`` based on ``predicate`` or ``None``, if it cannot be found.
    """
    return next((item for item in iterable if predicate(item)), None)


