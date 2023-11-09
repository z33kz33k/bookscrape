"""

    bookscrape.scrape.goodreads.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Handle data scraped from Goodreads.

    @author: z33k

"""
import json
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Tuple, Type

from bookscrape.constants import (Json, OUTPUT_DIR, PathLike, READABLE_TIMESTAMP_FORMAT,
                                  FILENAME_TIMESTAMP_FORMAT)
from bookscrape.scrape.goodreads.scrapers import AuthorScraper, BookScraper, _AuthorsData
from bookscrape.scrape.goodreads.utils import is_goodreads_id, numeric_id, url2id
from bookscrape.utils import getdir, getfile, timed
from bookscrape.scrape.goodreads.data import (Author, AuthorStats, Book, BookDetails, BookSeries,
                                              BookStats, DetailedBook,
                                              MainEdition, BookAward, BookSetting, SimpleAuthor,
                                              _ScriptTagData)

_log = logging.getLogger(__name__)
PROVIDER = "www.goodreads.com"
_BORKED = [
    "44037.Vernor_Vinge",  # works only with ID,
]


def load_authors(authors_json: PathLike) -> _AuthorsData:
    """Load ``authors_json`` into a dictionary containg a list of Author objects and return it.

    Args:
        authors_json: path to a JSON file saved earlier by dump_authors()
    """
    authors_json = getfile(authors_json, ext=".json")
    data = json.loads(authors_json.read_text(encoding="utf8"))
    data["timestamp"] = datetime.strptime(data["timestamp"], READABLE_TIMESTAMP_FORMAT)
    data["authors"] = [Author.from_dict(item) for item in data["authors"]]
    return data


def load_books(books_json: PathLike) -> Json:
    """Load ``books_json`` into a dictionary containg a list of DetailedBook objects and return it.

    Args:
        books_json: path to a JSON file saved earlier by dump_books()
    """
    books_json = getfile(books_json, ext=".json")
    data = json.loads(books_json.read_text(encoding="utf8"))
    data["timestamp"] = datetime.strptime(data["timestamp"], READABLE_TIMESTAMP_FORMAT)
    data["books"] = [DetailedBook.from_dict(item) for item in data["books"]]
    return data


def scrape_data(*cues: str | Tuple[str, str],
                scraper_type: Type[AuthorScraper | BookScraper] = AuthorScraper,
                **kwargs: Any) -> Generator[Author | DetailedBook, None, None]:
    """Scrape data according to the parameters provided.

    See dump_authors() and dump_books() for more details.

    Recognized optional arguments:
        authors_data: authors' data as read from JSON saved by dump_authors()

    Args:
        cues: variable number of input arguments for the scraper specified
        scraper_type: a type of scraper to be used
        kwargs: optional arguments
    """
    for i, cue in enumerate(cues, start=1):
        _log.info(f"Scraping item #{i}: '{cue}'...")
        try:
            if scraper_type is AuthorScraper:
                scraper = scraper_type(cue)
            elif scraper_type is BookScraper:
                if isinstance(cue, str):
                    scraper = scraper_type(cue)
                else:
                    scraper = scraper_type(*cue, authors_data=kwargs.get("authors_data"))
            else:
                break

            yield scraper.scrape()
        except Exception as e:
            _log.error(f"{type(e).__qualname__}. Skipping...\n{traceback.format_exc()}")
            continue


def _dump_data(*cues: str | Tuple[str, str],
               scraper_type: Type[AuthorScraper | BookScraper] = AuthorScraper,
               **kwargs: Any) -> None:
    """Scrape data and dump it to JSON.
    """
    if scraper_type is AuthorScraper:
        scraped = [s.as_dict for s in scrape_data(*cues, scraper_type=scraper_type)]
        scraped = sorted(scraped, key=lambda item: item["name"].casefold())
        dataname = "authors"
    elif scraper_type is BookScraper:
        scraped = [s.as_dict for s in scrape_data(
            *cues, scraper_type=scraper_type, authors_data=kwargs.get("authors_data"))]
        scraped = sorted(scraped, key=lambda item: item["title"].casefold())
        dataname = "books"
    else:
        return

    timestamp = datetime.now()
    data = {
        "timestamp": timestamp.strftime(READABLE_TIMESTAMP_FORMAT),
        "provider": PROVIDER,
        dataname: scraped,
    }
    # kwargs
    prefix = kwargs.get("prefix") or ""
    prefix = f"{prefix}_" if prefix else ""
    use_timestamp = kwargs.get("use_timestamp") if kwargs.get("use_timestamp") is not None else \
        True
    timestamp = f"_{timestamp.strftime(FILENAME_TIMESTAMP_FORMAT)}" if use_timestamp else ""
    output_dir = kwargs.get("output_dir") or kwargs.get("outputdir") or OUTPUT_DIR
    output_dir = getdir(output_dir)
    filename = kwargs.get("filename")
    if filename:
        filename = filename
    else:
        filename = f"{prefix}dump{timestamp}.json"

    dest = output_dir / filename
    with dest.open("w", encoding="utf8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    if dest.exists():
        _log.info(f"Successfully dumped '{dest}'")


@timed("author data dump", precision=1)
def dump_authors(*authors: str, prefix="authors", **kwargs: Any) -> None:
    """Scrape data on ``authors`` and dump it to JSON.

    Providing Goodreads authors IDs as 'authors' cuts the needed number of requests by half.

    Recognized optional arguments:
        use_timestamp: whether to append a timestamp to the dumpfile's name (default: True)
        filename: a complete filename for the dumpfile (renders moot other filename-concerned arguments)
        output_dir: an output directory (if not provided, defaults to OUTPUT_DIR)

    Args:
        authors: variable number of author full names or Goodread author IDs
        prefix: a prefix for a dumpfile's name
        kwargs: optional arguments
    """
    try:
        _dump_data(*authors, scraper_type=AuthorScraper, prefix=prefix, **kwargs)
    except Exception as e:
        _log.critical(f"{type(e).__qualname__}: {e}:\n{traceback.format_exc()}")


@timed("book data dump", precision=1)
def dump_books(*book_cues: str | Tuple[str, str], prefix="books", **kwargs: Any) -> None:
    """Scrape data on books specified by provided cues and dump it to JSON.

    Providing Goodreads book IDs as 'book_cues' cuts the needed number of requests considerably.
    If not provided, specifying 'authors_data' in optional arguments speeds up derivation of the
    needed IDs.

    Recognized optional aguments:
        authors_data: authors' data as read from JSON saved by dump_authors()
        use_timestamp: whether to append a timestamp to the dumpfile's name
        filename: a complete filename for the dumpfile (renders moot other filename-concerned arguments)
        output_dir: an output directory (if not provided, defaults to OUTPUT_DIR)

    Args:
        book_cues: variable number of either Goodreads book IDs or (book's title, book's author) tuples
        prefix: a prefix for a dumpfile's name
        kwargs: optional arguments
    """
    try:
        _dump_data(*book_cues, scraper_type=BookScraper, prefix=prefix, **kwargs)
    except Exception as e:
        _log.critical(f"{type(e).__qualname__}: {e}:\n{traceback.format_exc()}")


def update_authors(authors_json: PathLike) -> None:
    """Load authors from ``authors_json``, scrape them again and save at the same location (
    with updated file timestamp).

    Args:
        authors_json: path to a JSON file saved earlier by dump_authors()
    """
    output_dir = getfile(authors_json).parent
    data = load_authors(authors_json)
    authors = data["authors"]
    ids = [author.id for author in authors]
    dump_authors(*ids, output_dir=output_dir)


def update_books(books_json: PathLike) -> None:
    """Load books from ``books_json``, scrape them again and save at the same location (
    with updated file timestamp).

    Args:
        books_json: path to a JSON file saved earlier by dump_books()
    """
    output_dir = getfile(books_json).parent
    data = load_books(books_json)
    books = data["books"]
    ids = [book.book_id for book in books]
    dump_books(*ids, output_dir=output_dir)


def update_tolkien() -> None:
    """Update J.R.R. Tolkien's data that is used as a measuring stick for calculating renown in
    author/book stats.
    """
    outputdir = Path(__file__).parent.parent.parent / "data"
    dump_authors("656983.J_R_R_Tolkien", outputdir=outputdir, filename="tolkien.json")
    dump_books("5907.The_Hobbit", outputdir=outputdir, filename="hobbit.json")

