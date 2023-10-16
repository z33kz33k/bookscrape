"""

    bookscrape.utils.py
    ~~~~~~~~~~~~~~~~~~~
    Script's utilities

    @author: z33k

"""
from typing import Callable, Iterable, Iterator, List, Optional

import gspread
import pandas as pd
import requests
from bs4 import BeautifulSoup
from contexttimer import Timer

from bookscrape.constants import REQUEST_TIMOUT, T


def non_ascii_indices(text: str) -> Iterator[int]:
    """Generate indices of non-ASCII characters in ``text``.
    """
    for i, char in enumerate(text):
        if ord(char) not in range(128):
            yield i


def getsoup(url: str) -> BeautifulSoup:
    """Return BeautifulSoup object based on ``url``.

    Args:
        url: URL string

    Returns:

        a BeautifulSoup object
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


def retrieve_gsheets_col(spreadsheet: str, worksheet: str,
                            col=1, start_row=1, ignore_none=True) -> List[str]:
    if col < 1 or start_row < 1:
        raise ValueError("Column and start row must be positive integers")
    creds_file = "scraping_service_account.json"
    client = gspread.service_account(filename=creds_file)
    spreadsheet = client.open(spreadsheet)
    worksheet = spreadsheet.worksheet(worksheet)
    values = worksheet.col_values(col, value_render_option="UNFORMATTED_VALUE")[start_row-1:]
    if ignore_none:
        return [value for value in values if value is not None]
    return values


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