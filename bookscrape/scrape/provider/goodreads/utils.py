"""

    bookscrape.scrape.goodreads.utils.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
        raise ValueError(f"Could not extract numeric part of Goodread ID: {text_id!r}")
    return int(match.group())


@type_checker(str)
def is_goodreads_id(text: str) -> bool:
    """Check if text is a Goodreads ID.

    Example of possible formats:
        '625094.The_Leopard'
        '9969571-ready-player-one'
        '40982390'

    The numeric only variation occurs in cases when the book's title is written in a foreign
    script (e.g. 'Трилогия о Хане Соло'). This can also happen with books for which Goodreads
    uses an English translation as title but their original title uses foreign script (
    e.g.'Roadside Picnic'). In such a case, the book ID would be rendered in regular format,
    but its so-called work ID is going to be numeric only.
    """
    if all(char.isdigit() for char in text):
        return True
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


def url2id(url: str) -> str | None:
    """Extract Goodreads ID from URL.
    """
    if "/" not in url:
        return None
    *_, id_ = url.split("/")
    if is_goodreads_id(id_):
        return id_
    return None
