"""

    bookscrape.sheets.py
    ~~~~~~~~~~~~~~~~~~~~
    Google Sheets as backend/frontend.

    @author: z33k

"""
from typing import List

import gspread

from bookscrape.utils import timed


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


@timed
def retrieve_sf_books_authors() -> List[str]:
    """Retrieve a list of author names from 'sf_books' private Google sheet.
    """
    print("Retrieving data from gsheets...")
    items = retrieve_gsheets_col("sf_books", "books", 3, 4)
    authors = set()
    for item in items:
        if "," in item:
            names = [token.strip() for token in item.split(",")]
            authors.update(names)
        else:
            authors.add(item.strip())
    return sorted(authors)

