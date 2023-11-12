"""

    bookscrape.scrape.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape data on authors and books from various sites.

    @author: z33k

"""
import logging
import json
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Iterable, List, Tuple

from bookscrape.constants import BookRecord, FILENAME_TIMESTAMP_FORMAT, Json, OUTPUT_DIR, PathLike, \
    READABLE_TIMESTAMP_FORMAT
from bookscrape.scrape.goodreads import scrape_authors as scrape_goodreads_authors
from bookscrape.scrape.goodreads import scrape_books as scrape_goodreads_books
from bookscrape.scrape.goodreads.data import PROVIDER as GOODREADS
from bookscrape.scrape.goodreads.data import Author as GoodreadsAuthor
from bookscrape.scrape.goodreads.data import DetailedBook as GoodreadsBook
from bookscrape.utils import getdir, getfile, timed

_log = logging.getLogger(__name__)


@dataclass
class AuthorData:
    # there's room for more
    goodreads: GoodreadsAuthor

    @property
    def as_dict(self) -> Json:
        return {GOODREADS: self.goodreads.as_dict}

    @classmethod
    def from_dict(cls, data: Json) -> "AuthorData":
        return cls(GoodreadsAuthor.from_dict(data[GOODREADS]))


@dataclass
class BookData:
    # there's room for more
    goodreads: GoodreadsBook

    @property
    def as_dict(self) -> Json:
        return {GOODREADS: self.goodreads.as_dict}

    @classmethod
    def from_dict(cls, data: Json) -> "BookData":
        return cls(GoodreadsBook.from_dict(data[GOODREADS]))


@dataclass
class AuthorDump:
    timestamp: datetime
    authors: List[AuthorData]

    @property
    def as_dict(self) -> Json:
        return {
            "timestamp": self.timestamp.strftime(READABLE_TIMESTAMP_FORMAT),
            "authors": [author.as_dict for author in self.authors],
        }

    @classmethod
    def from_dict(cls, data: Json) -> "AuthorDump":
        return cls(
            datetime.strptime(data["timestamp"], READABLE_TIMESTAMP_FORMAT),
            [AuthorData.from_dict(author) for author in data["authors"]],
        )


@dataclass
class BookDump:
    timestamp: datetime
    books: List[BookData]

    @property
    def as_dict(self) -> Json:
        return {
            "timestamp": self.timestamp.strftime(READABLE_TIMESTAMP_FORMAT),
            "books": [book.as_dict for book in self.books],
        }

    @classmethod
    def from_dict(cls, data: Json) -> "BookDump":
        return cls(
            datetime.strptime(data["timestamp"], READABLE_TIMESTAMP_FORMAT),
            [BookData.from_dict(book) for book in data["books"]],
        )


def load_authors(*author_jsons: PathLike) -> List[AuthorDump]:
    """Deserialize JSON files with author data into a list of data objects.
    """
    authors = []
    for author_json in author_jsons:
        author_json = getfile(author_json, ext=".json")
        data = json.loads(author_json.read_text(encoding="utf8"))
        authors.append(AuthorDump.from_dict(data))
    return authors


def load_books(*book_jsons: PathLike) -> List[BookDump]:
    """Deserialize JSON files with book data into a list of data objects.
    """
    books = []
    for book_json in book_jsons:
        books_json = getfile(book_json, ext=".json")
        data = json.loads(books_json.read_text(encoding="utf8"))
        books.append(BookDump.from_dict(data))
    return books


def _dump_data(data: AuthorDump | BookDump, **kwargs: Any) -> None:
    """Dump the provided data to a JSON file.

    Recognized optional arguments:
        use_timestamp: whether to append a timestamp to the dumpfile's name (default: True)
        prefix: a prefix for a dumpfile's name
        filename: a complete filename for the dumpfile (renders moot other filename-concerned arguments)
        output_dir: an output directory (if not provided, defaults to OUTPUT_DIR)

    Args:
        data: the data to dump
        kwargs: optional arguments
    """
    prefix = kwargs.get("prefix") or ""
    prefix = f"{prefix}_" if prefix else ""
    use_timestamp = kwargs.get("use_timestamp") if kwargs.get("use_timestamp") is not None else \
        True
    timestamp = f"_{data.timestamp.strftime(FILENAME_TIMESTAMP_FORMAT)}" if use_timestamp else ""
    output_dir = kwargs.get("output_dir") or kwargs.get("outputdir") or OUTPUT_DIR
    output_dir = getdir(output_dir)
    filename = kwargs.get("filename")
    if filename:
        filename = filename
    else:
        filename = f"{prefix}dump{timestamp}.json"

    dest = output_dir / filename
    with dest.open("w", encoding="utf8") as f:
        json.dump(data.as_dict, f, indent=4, ensure_ascii=False)

    if dest.exists():
        _log.info(f"Successfully dumped '{dest}'")


@timed("authors data dump", precision=1)
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
        scraped = sorted(scrape_goodreads_authors(*authors),
                         key=lambda author: author.name.casefold())
        if scraped:
            data = AuthorDump(datetime.now(), [AuthorData(author) for author in scraped])
            _dump_data(data, prefix=prefix, **kwargs)
        else:
            _log.warning("Nothing has beenb scraped")
    except Exception as e:
        _log.critical(f"{type(e).__qualname__}: {e}:\n{traceback.format_exc()}")


@timed("books data dump", precision=1)
def dump_books(*book_cues: str | Tuple[str, str], prefix="books", **kwargs: Any) -> None:
    """Scrape data on books specified by provided cues and dump it to JSON.

    Providing Goodreads book IDs as 'book_cues' cuts the needed number of requests considerably.
    If not provided, specifying 'authors_data' in optional arguments speeds up derivation of the
    needed IDs.

    Recognized optional aguments:
        authors_data: iterable of previously scraped data objects containing Goodreads author IDs
        use_timestamp: whether to append a timestamp to the dumpfile's name
        filename: a complete filename for the dumpfile (renders moot other filename-concerned arguments)
        output_dir: an output directory (if not provided, defaults to OUTPUT_DIR)

    Args:
        book_cues: variable number of either Goodreads book IDs or (title, author) tuples
        prefix: a prefix for a dumpfile's name
        kwargs: optional arguments
    """
    try:
        scraped = scrape_goodreads_books(
            *book_cues, authors_data=kwargs.get("authors_data") or kwargs.get("author_data"))
        if scraped:
            scraped = sorted(scraped, key=lambda book: book.title.casefold())
            data = BookDump(datetime.now(), [BookData(book) for book in scraped])
            _dump_data(data, prefix=prefix, **kwargs)
        else:
            _log.warning("Nothing has been scraped")
    except Exception as e:
        _log.critical(f"{type(e).__qualname__}: {e}:\n{traceback.format_exc()}")


def update_authors(*authors_jsons: PathLike) -> None:
    """For each ``authors_json`` specified, deserialize it, extract Goodreads author IDs,
    scrape those authors again and save the scraped data at the previous location (with updated
    file timestamp).

    Args:
        authors_jsons: vairable number of paths to a JSON files saved earlier by dump_authors()
    """
    for authors_json in authors_jsons:
        output_dir = getfile(authors_json).parent
        _log.info(f"Updating '{authors_json}'...")
        data = load_authors(authors_json)
        if not data:
            continue
        data = data[0]
        authors = data.authors
        ids = [author.goodreads.id for author in authors]
        dump_authors(*ids, output_dir=output_dir)


def update_books(*books_jsons: PathLike) -> None:
    """For each ``books_json`` specified, deserialize it, extract Goodreads book IDs,
    scrape those books again and save the scraped data at the previous location (with updated
    file timestamp).

    Args:
        books_jsons: vairable number of paths to a JSON files saved earlier by dump_books()
    """
    for books_json in books_jsons:
        output_dir = getfile(books_json).parent
        _log.info(f"Updating '{books_json}'...")
        data = load_books(books_json)
        if not data:
            continue
        data = data[0]
        books = data.books
        ids = [book.goodreads.book_id for book in books]
        dump_books(*ids, output_dir=output_dir)


def update_tolkien() -> None:
    """Update J.R.R. Tolkien's data that is used as a measuring stick for calculating renown in
    author/book stats.
    """
    outputdir = Path(__file__).parent.parent / "data"
    dump_authors("656983.J_R_R_Tolkien", outputdir=outputdir, filename="tolkien.json")
    dump_books("5907.The_Hobbit", outputdir=outputdir, filename="hobbit.json")


def authors_data(*author_jsons: PathLike) -> List[GoodreadsAuthor]:
    dumps = load_authors(*author_jsons)
    return [author.goodreads for dump in dumps for author in dump.authors]


def books_data(*book_jsons: PathLike) -> List[GoodreadsBook]:
    dumps = load_books(*book_jsons)
    return [book.goodreads for dump in dumps for book in dump.books]


def extract_matching_ids(book_records: Iterable[Tuple[str, str]],
                         *book_jsons: PathLike,
                         blank: str | None = None) -> Generator[str, None, None]:
    """Yield Goodreads book IDs from book JSON dumps that match the provided book records.

    If blank is not None, it is yielded instead of an ID that cannot be matched.

    Args:
        book_records: (title, author) tuples to match
        *book_jsons: JSON files saved earlier by dump_books()
        blank: a value to yield instead of an ID that cannot be matched

    Returns:
        a generator of Goodreads book IDs
    """
    books = books_data(*book_jsons)
    ids_map = {}
    for book in books:
        ids_map[(book.title.casefold(), book.authors[0].name.casefold())] = book.book_id
    for title, author in book_records:
        if book_id := ids_map.get((title.casefold(), author.casefold())):
            yield book_id
        else:
            if blank is not None:
                yield blank
