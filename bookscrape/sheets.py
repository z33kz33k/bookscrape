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
from bookscrape.scrape.goodreads.utils import url2id
from bookscrape.utils import timed


_log = logging.getLogger(__name__)


@timed("retrieving from Google Sheets")
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


def retrieve_sf_authors() -> List[str]:
    """Retrieve a list of author names from 'sf_books' private Google sheet.
    """
    _log.info("Retrieving author names from gsheets...")
    items = retrieve_gsheets_col("sf_books", "books", 4, 4)
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


def retrieve_sf_book_records() -> List[BookRecord]:
    """Retrieve a list of book records from 'sf_books' private Google sheet.
    """
    _log.info("Retrieving book records from gsheets...")
    titles = [title.strip() for title in retrieve_gsheets_col("sf_books", "books", 2, 4)]
    author_tokens = retrieve_gsheets_col("sf_books", "books", 4, 4)
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


@timed
def retrieve_sf_book_ids() -> List[str]:
    """Retrieve a list of book IDs from 'sf_books' private Google sheet.
    """
    _log.info("Retrieving book IDs from gsheets...")
    ids = [url2id(url) for url in retrieve_gsheets_col("sf_books", "books", 3, 4)]
    _log.info(f"Retrieved {len(ids)} book IDs")
    return ids
