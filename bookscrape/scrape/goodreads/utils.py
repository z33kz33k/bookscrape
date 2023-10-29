"""

    bookscrape.goodreads.utils.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Utilities specific to Goodreads scraping.

    @author: z33k

"""
import re

from bookscrape.utils import type_checker


@type_checker(str)
def numeric_id(text_id: str) -> int:
    """Extract numeric part of Goodreads ID and return it.

    Example of possible formats:
        '625094.The_Leopard'
        '9969571-ready-player-one'
    """
    match = re.search(r"\d+", text_id)
    if not match:
        raise ValueError(f"Unable to extract numeric part of Goodread ID: {text_id!r}")
    return int(match.group())


@type_checker(str)
def is_goodreads_id(text: str) -> bool:
    if len(text) <= 2:
        return False
    if "." in text and "-" in text:
        return False
    if "." in text:
        sep = "."
    elif "-" in text:
        sep = "-"
    else:
        return False
    left, *right = text.split(sep)
    right = "".join(right)
    if not all(char.isdigit() for char in left):
        return False
    if not all((char.isalnum() and char.isascii()) or char in "_-" for char in right):
        return False
    return True
