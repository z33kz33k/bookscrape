"""

    bookscrape.scrape.goodreads.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape Goodreads for data on authors and books.

    @author: z33k

"""
import logging
import traceback
from typing import Generator, Iterable, Tuple

from bookscrape.scrape.goodreads.data import Author, DetailedBook, PROVIDER
from bookscrape.scrape.goodreads.scrapers import AuthorScraper, BookScraper


_log = logging.getLogger(__name__)
_BORKED = [
    "44037.Vernor_Vinge",  # works only with ID,
]


def scrape_authors(*cues: str | Tuple[str, str]) -> Generator[Author, None, None]:
    """Scrape Goodreads for authors data according to the parameters provided.

    Cues can be either full author names or Goodreads author IDs.
    For book scraping cues can be either (title, author) tuples or Goodreads book IDs.

    Args:
        cues: variable number of author full names or Goodread author IDs
    """
    for i, cue in enumerate(cues, start=1):
        _log.info(f"Scraping {PROVIDER} for item #{i}: '{cue}'...")
        try:
            scraper = AuthorScraper(cue)
            yield scraper.scrape()
        except Exception as e:
            _log.error(f"{type(e).__qualname__}. Skipping...\n{traceback.format_exc()}")
            continue


def scrape_books(*cues: str | Tuple[str, str], authors_data: Iterable[Author] | None = None
                 ) -> Generator[DetailedBook, None, None]:
    """Scrape Goodreads for books data according to the parameters provided.

    Cues can be either (title, author) tuples or Goodreads book IDs.

    When book IDs aren't supplied in the input, previously scraped Author data
    objects can be provided to speed up book IDs derivation. Also, expressing 'author' as an
    author ID makes it even faster.

    Args:
        cues: variable number of either Goodreads book IDs or (title, author) tuples
        authors_data: optionally, iterable of Author data objects
    """
    for i, cue in enumerate(cues, start=1):
        _log.info(f"Scraping {PROVIDER} for item #{i}: '{cue}'...")
        try:
            scraper = BookScraper(cue, authors_data=authors_data)
            yield scraper.scrape()
        except Exception as e:
            _log.error(f"{type(e).__qualname__}. Skipping...\n{traceback.format_exc()}")
            continue


def scrape_book_ids(*book_records: Tuple[str, str], authors_data: Iterable[Author] | None = None
                    ) -> Generator[str, None, None]:
    """Scrape Goodreads for books IDs according to the (title, author) records provided.

    Previously scraped Author data objects can be provided to speed up book IDs derivation. Also,
    expressing 'author' as an author ID makes it even faster.

    Args:
        book_records: variable number of (title, author) records
        authors_data: optionally, iterable of Author data objects
    """
    for i, record in enumerate(book_records, start=1):
        _log.info(f"Scraping {PROVIDER} for item #{i}: '{record}'...")
        try:
            scraper = BookScraper(record, authors_data=authors_data)
            yield scraper.book_id
        except Exception as e:
            _log.error(f"{type(e).__qualname__}. Skipping...\n{traceback.format_exc()}")
            continue
