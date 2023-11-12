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
PROPER_AUTHORS = {
    "Mary Shelley": "Mary Wollstonecraft Shelley",
    "Stanislaw Lem": "Stanisław Lem",
}
PROPER_TITLES = {
    "The Island of Doctor Moreau": "The Island of Dr. Moreau",
    "Planet of the Apes (aka Monkey Planet)": "Planet of the Apes",
    "The Songs of Distant Earth": "Songs of Distant Earth",
    "Galapagos": "Galápagos",
    "How to Live Safely in a Sci-Fi Universe": "How to Live Safely in a Science Fictional Universe",
    "Readme": "Reamde",
}


def scrape_authors(*cues: str | Tuple[str, str]) -> Generator[Author | DetailedBook, None, None]:
    """Scrape Goodreads for authors data according to the parameters provided.

    Cues can be either full author names or Goodreads author IDs.
    For book scraping cues can be either (title, author) tuples or Goodreads book IDs.

    Args:
        cues: variable number of input arguments for the scraping specified
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
                 ) -> Generator[Author | DetailedBook, None, None]:
    """Scrape Goodreads for books data according to the parameters provided.

    Cues can be either (title, author) tuples or Goodreads book IDs.

    When book IDs aren't supplied in the input, previously scraped Author data
    objects can be provided to speed up book IDs derivation.

    Args:
        cues: variable number of input arguments for the scraping specified
        authors_data: optionally, iterable of Author data objects
    """
    for i, cue in enumerate(cues, start=1):
        _log.info(f"Scraping {PROVIDER} for item #{i}: '{cue}'...")
        try:
            if isinstance(cue, str):
                scraper = BookScraper(cue)
            else:
                scraper = BookScraper(*cue, authors_data=authors_data)
            yield scraper.scrape()
        except Exception as e:
            _log.error(f"{type(e).__qualname__}. Skipping...\n{traceback.format_exc()}")
            continue
