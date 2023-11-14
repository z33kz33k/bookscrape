"""

    bookscrape.sheets.py
    ~~~~~~~~~~~~~~~~~~~~
    Google Sheets as backend/frontend.

    @author: z33k

"""
import logging
from typing import List

import gspread

from bookscrape.constants import BookRecord
from bookscrape.scrape.provider.goodreads import url2id
from bookscrape.utils import timed
from bookscrape.utils.check_type import generic_iterable_type_checker, type_checker

_log = logging.getLogger(__name__)


@type_checker(str, str)
def _worksheet(spreadsheet: str, worksheet: str) -> gspread.Worksheet:
    creds_file = "scraping_service_account.json"
    client = gspread.service_account(filename=creds_file)
    spreadsheet = client.open(spreadsheet)
    worksheet = spreadsheet.worksheet(worksheet)
    return worksheet


@timed("retrieving from Google Sheets")
def retrieve_from_gsheets_col(spreadsheet: str, worksheet: str, col=1, start_row=1,
                              ignore_none=True) -> List[str]:
    """Retrieve a list of string values from a Google Sheets worksheet.
    """
    if col < 1 or start_row < 1:
        raise ValueError("Column and start row must be positive integers")
    worksheet = _worksheet(spreadsheet, worksheet)
    values = worksheet.col_values(col, value_render_option="UNFORMATTED_VALUE")[start_row-1:]
    if ignore_none:
        return [value for value in values if value is not None]
    return values


def retrieve_yt_sf_authors() -> List[str]:
    """Retrieve a list of author names from my private youtubers ranking.
    """
    _log.info("Retrieving author names from Google Sheets...")
    items = retrieve_from_gsheets_col("sf_books", "books", 4, 4)
    authors = set()
    for item in items:
        if "," in item:
            names = [token.strip() for token in item.split(",")]
            authors.update(names)
        else:
            authors.add(item.strip())
    authors = sorted(authors)
    _log.info(f"Retrieved {len(authors)} author names")
    return authors


def retrieve_yt_sf_book_records() -> List[BookRecord]:
    """Retrieve a list of book records from my private youtubers ranking.
    """
    _log.info("Retrieving book records from Google Sheets...")
    titles = [title.strip() for title in retrieve_from_gsheets_col("sf_books", "books", 2, 4)]
    author_tokens = retrieve_from_gsheets_col("sf_books", "books", 4, 4)
    authors = []
    for token in author_tokens:
        if "," in token:
            names = [token.strip() for token in token.split(",")]
            authors.append(names[0])
        else:
            authors.append(token.strip())
    records = [BookRecord(title, author) for title, author in zip(titles, authors)]
    _log.info(f"Retrieved {len(records)} book records")
    return records


def retrieve_yt_sf_book_ids() -> List[str]:
    """Retrieve a list of book IDs from my private youtubers ranking.
    """
    _log.info("Retrieving book IDs from Google Sheets...")
    ids = [url2id(url) for url in retrieve_from_gsheets_col("sf_books", "books", 3, 4)]
    _log.info(f"Retrieved {len(ids)} book IDs")
    return ids


def retrieve_sf_lists_book_ids() -> List[str]:
    """Retrieve a list of book IDs from 'sf_lists_ids' worksheet.
    """
    _log.info("Retrieving book IDs from Google Sheets...")
    ids = retrieve_from_gsheets_col("sf_books", "sf_lists_ids", 1, 2)
    _log.info(f"Retrieved {len(ids)} book IDs")
    return ids


@timed("saving to Google Sheets")
@generic_iterable_type_checker(str)
def save_to_gsheets_col(values: List[str], spreadsheet: str, worksheet: str, col=1,
                        start_row=1) -> None:
    """Save a list of strings to a Google Sheets worksheet.
    """
    if col < 1 or start_row < 1:
        raise ValueError("Column and start row must be positive integers")
    worksheet = _worksheet(spreadsheet, worksheet)
    worksheet.insert_rows([[value] for value in values], row=start_row)


def save_ids_to_sf_lists_sheet(book_ids: List[str], col: int) -> None:
    """Save a list of Goodreads book IDs to 'sf_lists_ids' worksheet.
    """
    _log.info("Saving book IDs to Google Sheets...")
    save_to_gsheets_col(book_ids, "sf_books", "sf_lists_ids", col, 2)
    _log.info(f"Saved {len(book_ids)} book IDs")
