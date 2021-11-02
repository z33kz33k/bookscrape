"""

    utils.py
    ~~~~~~~~~~~~
    Script's utilities.

    @author: z33k

"""
import pandas as pd
import requests
from bs4 import BeautifulSoup
from contexttimer import Timer

from constants import REQUEST_TIMOUT


def non_ascii_index(text: str) -> int:
    """Returm index of the first non-ASCII character in ``text` or ``-1``.
    """
    for i, char in enumerate(text):
        if ord(char) not in range(128):
            return i
    return -1


def getsoup(url: str) -> BeautifulSoup:
    """Return BeautifulSoup object based on ``url``.

    :param url: URL string
    :return: BeautifulSoup object
    """
    print(f"Requesting: {url!r}")
    with Timer() as t:
        markup = requests.get(url, timeout=REQUEST_TIMOUT).text
    print(f"Request completed in {t.elapsed:3f} seconds.")
    return BeautifulSoup(markup, "lxml")


def first_df_row_as_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make first row of ``df`` its columns.
    """
    return df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)


