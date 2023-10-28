"""

    bookscrape.goodreads.py
    ~~~~~~~~~~~~~~~~~~~~~~~
    Scrape and parse Goodreads data

    @author: z33k

"""
import json
import re
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

import backoff
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests import Timeout

from bookscrape.constants import (Json, OUTPUT_DIR, PathLike, READABLE_TIMESTAMP_FORMAT,
                                  FILNAME_TIMESTAMP_FORMAT)
from bookscrape.utils import getdir, getfile, extract_int, extract_float, from_iterable, \
    type_checker
from bookscrape.scrape import ParsingError, Renown, getsoup, throttled, FiveStars

PROVIDER = "goodreads.com"
# the unofficially known enforced throttling delay
# between requests to Goodreads servers is 1 s
# we're choosing to be safe here
THROTTLING_DELAY = 1.1  # seconds


def _load_tolkien() -> Tuple[int, int]:
    source = getfile(Path(__file__).parent.parent / "data" / "tolkien.json")
    with source.open(encoding="utf8") as f:
        data = json.load(f)

    if not data:
        raise ValueError(f"No data in '{source}'")

    try:
        tolkien_ratings = data["authors"][0]["stats"]["ratings"]
        hobbit_ratings = data["authors"][0]["books"][0]["ratings"]
    except (KeyError, IndexError):
        raise ValueError(f"Invalid data in '{source}'")
    return tolkien_ratings, hobbit_ratings


TOLKIEN_RATINGS, HOBBIT_RATINGS = _load_tolkien()
# TOLKIEN_RATINGS, HOBBIT_RATINGS = 10_674_789, 3_779_353  # on 18th Oct 2023


@dataclass
class AuthorStats:
    avg_rating: float
    ratings: int
    reviews: int
    shelvings: int

    @property
    def as_dict(self) -> Dict[str, int | float]:
        return {
            "avg_rating": self.avg_rating,
            "ratings": self.ratings,
            "reviews": self.reviews,
            "shelvings": self.shelvings,
            "r2r": self.r2r_percent,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, int | float]) -> "AuthorStats":
        return cls(
            data["avg_rating"],
            data["ratings"],
            data["reviews"],
            data["shelvings"],
        )

    @property
    def r2r(self) -> float:
        return self.reviews / self.ratings if self.ratings else 0

    @property
    def r2r_percent(self) -> str:
        r2r = self.r2r * 100
        return f"{r2r:.2f} %"


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


@dataclass
class Book:
    title: str
    id: str
    avg_rating: float
    ratings: int
    published_in: Optional[datetime]
    editions: Optional[int]

    @property
    def as_dict(self) -> Dict[str, int | float | str]:
        data = {
            "title": self.title,
            "id": self.id,
            "avg_rating": self.avg_rating,
            "ratings": self.ratings,
        }
        if self.published_in is not None:
            data.update({
                "published_in": self.published_in.year,
            })
        if self.editions is not None:
            data.update({
                "editions": self.editions,
            })
        data.update({"renown": self.renown.name})
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, int | float | str]) -> "Book":
        published_in = data.get("published_in")
        return cls(
            data["title"],
            data["id"],
            data["avg_rating"],
            data["ratings"],
            datetime(published_in, 1, 1) if published_in is not None else None,
            data.get("editions"),
        )

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.ratings, HOBBIT_RATINGS)

    @property
    def numeric_id(self) -> int:
        return numeric_id(self.id)


@dataclass
class Author:
    name: str
    id: str
    stats: AuthorStats
    books: List[Book]

    @property
    def as_dict(self) -> Json:
        return {
            "name": self.name,
            "id": self.id,
            "stats": self.stats.as_dict,
            "total_editions": self.total_editions,
            "renown": self.renown.name,
            "books": [b.as_dict for b in self.books],
        }

    @classmethod
    def from_dict(cls, data: Json) -> "Author":
        return cls(
            data["name"],
            data["id"],
            AuthorStats.from_dict(data["stats"]),
            [Book.from_dict(book) for book in data["books"]]
        )

    @property
    def total_editions(self) -> int:
        return sum(book.editions for book in self.books if book.editions)

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.stats.ratings, TOLKIEN_RATINGS)

    @property
    def numeric_id(self) -> int:
        return numeric_id(self.id)


class AuthorParser:
    """Goodreads author page parser.
    """
    URL_TEMPLATE = "https://www.goodreads.com/author/list/{}"

    @property
    def author_name(self) -> str | None:
        return self._author_name

    @property
    def author_id(self) -> str | None:
        return self._author_id

    def __init__(self, author: str) -> None:
        """Initialize.

        Args:
            author: author's full name or Goodreads author ID (one fewer sever request)
        """
        self._author_id, self._author_name = None, None
        if is_goodreads_id(author):
            self._author_id = author
        else:
            self._author_name = author

    @staticmethod
    def normalize_name(author_name: str) -> str:
        """Return 'author_name' as rendered in Goodreads author ID.

        Goodreads logic that formulates author ID:

        1) replace any whitespace with underscore
            Example: '589.Orson_Scott_Card'

        2) replace any non-alphabetic character with underscore
            Apostrophe: 'Madeleine L'Engle' ==> '106.Madeleine_L_Engle'
            Hyphen: 'Chi Ta-wei' ==> '14640243.Chi_Ta_wei'
            Dot: 'George R.R. Martin' ==> '346732.George_R_R_Martin'

        3) replace any non-ASCII character with underscore
            Example: 'Stanisław Lem' ==> '10991.Stanis_aw_Lem'

        4) replace any immediately repeated underscore with only one instance
            Example: 'Ewa Białołęcka' ==> '554577.Ewa_Bia_o_cka'
        """
        chars, underscore_appended = [], False
        for char in author_name:
            if not (char.isalpha() and char.isascii()):
                if not underscore_appended:
                    chars.append("_")
                    underscore_appended = True
                else:
                    continue
            else:
                chars.append(char)
                underscore_appended = False
        return "".join(chars)

    @classmethod
    @throttled(THROTTLING_DELAY)
    def find_author_id(cls, author_name: str) -> str:
        """Find Goodreads author ID by quering a Goodreads search with ``author_name``.

        Args:
            author_name: full author's name

        Example:
            '7415.Harlan_Ellison'
        """
        def parse_spans(spans_: List[Tag]) -> Optional[Tag]:
            for span in spans_:
                a_ = span.find(
                    lambda t: t.name == "a" and cls.normalize_name(author_name) in t.attrs.get(
                        "href"))
                if a_ is not None:
                    return a_
            return None

        query = "+".join(author_name.split())
        url_template = "https://www.goodreads.com/search?q={}"
        url = url_template.format(query)
        soup = getsoup(url)
        spans = soup.find_all("span", itemprop="author")
        if not spans:
            raise ParsingError(f"No 'span' tags with queried author's data")

        a = parse_spans(spans)
        if not a:
            raise ParsingError(f"No 'a' tag containing the queried author's URL")

        url = a.attrs.get("href")
        # URL now ought to look like this:
        # 'https://www.goodreads.com/author/show/7415.Harlan_Ellison?from_search=true&from_srp=true'
        url, _ = url.split("?")  # stripping the trash part
        *_, id_ = url.split("/")
        return id_

    @staticmethod
    def _parse_author_stats(parts: List[str]) -> AuthorStats:
        """Parse author stats string parts extracted from the author list page.

        Example string parts:
            ['Average rating 4.17',
            '197,169 ratings',
            '12,120 reviews',
            'shelved 428,790 times']
        """
        if len(parts) != 4:
            raise ParsingError(f"Invalid author stats parts: {parts}")
        avg_rating = extract_float(parts[0])
        ratings = extract_int(parts[1])
        reviews = extract_int(parts[2])
        shelvings = extract_int(parts[3])
        return AuthorStats(avg_rating, ratings, reviews, shelvings)

    @staticmethod
    def _parse_published(row: Tag) -> Optional[int]:
        tag = row.find(lambda t: t.name == "span" and "published" in t.text)
        if tag is None:
            return None
        text = tag.text.strip()
        parts = text.split("\n")
        part = from_iterable(parts, lambda p: "published" in p)
        if part is None:
            return None
        idx = parts.index(part)
        idx += 1
        try:
            published = parts[idx]
            return extract_int(published)
        except (IndexError, ValueError):
            return None

    @staticmethod
    def _parse_editions(row: Tag) -> Optional[int]:
        editions = row.find(lambda t: t.name == "a" and "edition" in t.text)
        if editions is None:
            return None
        editions = editions.text.strip()
        try:
            return extract_int(editions)
        except ValueError:
            return None

    @classmethod
    def _parse_book_table_row(cls, row: Tag) -> Book:
        """Parse a book table row of the author page's book list.

        Args:
            row: a BeautifulSoup Tag object representing the row

        Returns:
            a Book object
        """
        a = row.find("a")
        if not a:
            raise ParsingError(f"No 'a' tag with title data in a row: {row}")
        title = a.attrs.get("title")
        if not title:
            raise ParsingError(f"No 'title' attribute in 'a' title tag within a row: {row}")
        href = a.attrs.get("href")
        if not href:
            raise ParsingError(f"No 'href' attribute in 'a' title' tag within a row: {row}")
        id_ = href.replace("/book/show/", "")
        ratings_text = row.find("span", class_="minirating").text.strip()
        avg, ratings = ratings_text.split(" — ")
        avg = extract_float(avg)
        ratings = extract_int(ratings)
        published = cls._parse_published(row)
        published = datetime(published, 1, 1) if published is not None else None
        editions = cls._parse_editions(row)

        return Book(title, id_, avg, ratings, published, editions)

    @throttled(THROTTLING_DELAY)
    def _parse_author_page(self) -> Author:
        """Scrape and parse Goodreads author page and return an Author object.

        Example URL:
            https://www.goodreads.com/author/list/7415.Harlan_Ellison

        Returns:
            an Author object
        """
        url = self.URL_TEMPLATE.format(self.author_id)
        soup = getsoup(url)
        container = soup.find("div", class_="leftContainer")
        name_tag = container.find("a", class_="authorName")
        if name_tag is None:
            raise ParsingError("No author name tag")
        self._author_name = name_tag.text.strip()

        # author stats
        div = container.find("div", class_="")
        text = div.text.strip()
        parts = [part.strip() for part in text.split("\n")[1:]]
        parts = [part.strip(" ·") for part in parts]
        stats = self._parse_author_stats(parts)

        # books
        table = container.find("table", class_="tableList")
        rows = table.find_all("tr")
        books = [self._parse_book_table_row(row) for row in rows]

        return Author(self.author_name, self.author_id, stats, books)

    def fetch_data(self) -> Author:
        try:
            if not self.author_id:
                self._author_id = self.find_author_id(self.author_name)
            author = self._parse_author_page()
        except Timeout:
            print("Goodreads doesn't play nice. Timeout exceeded. Retrying with backoff "
                  "(60 seconds max)...")
            return self.fetch_data_with_backoff()
        return author

    @backoff.on_exception(backoff.expo, Timeout, max_time=60)
    def fetch_data_with_backoff(self) -> Author:
        if not self.author_id:
            self._author_id = self.find_author_id(self.author_name)
        return self._parse_author_page()


@dataclass
class RatingStats:
    distribution: FiveStars
    reviews: int

    @property
    def r2r(self) -> float:
        return self.reviews / self.ratings

    @property
    def r2r_percent(self) -> str:
        r2r = self.r2r * 100
        return f"{r2r:.2f} %"

    @property
    def ratings(self) -> int:
        return self.distribution.total


@dataclass
class DetailedBook:
    title: str
    authors: List[str]  # list of author ID's
    series: List[str]  # list of book ID's
    first_published: datetime
    ratings_stats: RatingStats
    shelves: Dict[str, int]  # TODO: extract data on genres only
    titles: Dict[str, str]  # TODO: scraped from editions page

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.ratings_stats.ratings, HOBBIT_RATINGS)


@dataclass
class MainEdition:
    publisher: str
    publication_time: datetime
    format: str
    pages: int
    language: str
    isbn: str
    isbn13: str
    asin: str


class _ScriptTagParser:
    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data
        self._book_data = self._item("Book:kca://")
        self._work_data = self._item("Work:kca://")
        if self._book_data is None:
            raise ParsingError("No 'Book:kca://' data on the 'script' tag")
        if self._work_data is None:
            raise ParsingError("No 'Work:kca://' data on the 'script' tag")
        pass

    def _item(self, key_part: str) -> Any | None:
        key = from_iterable(self._data, lambda k: key_part in k)
        if not key:
            return None
        return self._data[key]

    def parse(self):
        try:
            complete_title = self._book_data["titleComplete"]
            details = self._book_data["details"]
            timestamp = details["publicationTime"]
            if len(str(timestamp)) == 13:
                timestamp /= 1000
            main_edition = MainEdition(
                publisher=details["publisher"],
                publication_time=datetime.utcfromtimestamp(timestamp),
                format=details["format"],
                pages=details["numPages"],
                language=details["language"]["name"],
                isbn=details["isbn"],
                isbn13=details["isbn13"],
                asin=details["asin"],
            )
            blurb = self._book_data['description({"stripped":true})']
            genres = []
            for item in self._book_data["bookGenres"]:
                genre = item["genre"]
                genres.append(genre["name"])
        except KeyError as ke:
            raise ParsingError(f"Data unavailable on the 'script' tag: {ke}")
        pass


_AuthorsData = Dict[str, datetime | str | List[Author]]
_Contributor = namedtuple("_Contributor", "author_id has_role")


# TODO: scrape more data
class BookParser:
    """Goodreads book page parser.
    """
    URL_TEMPLATE = "https://www.goodreads.com/book/show/{}"
    DATE_FORMAT = "%B %d, %Y"  # datetime.strptime("August 16, 2011", "%B %d, %Y")

    @property
    def book_id(self) -> str:
        return self._book_id

    def __init__(self, book: str, author: str | None,
                 authors_data: _AuthorsData | None = None) -> None:
        """Initialize.

        Args:
            book: book's title or book ID
            author: optionally (if book ID was not provided), book author's full name or Goodreads author ID
            authors_data: optionally, data as read from JSON saved by dump_authors()
        """
        if is_goodreads_id(book):
            self._book_id = book
        else:
            if not author:
                raise ValueError("Author must be specified when Book ID not provided")
            self._book_id = self.find_book_id(book, author, authors_data)
        if not self._book_id:
            raise ValueError("Unable to derive Goodreads book ID from provided input")
        self._url = self.URL_TEMPLATE.format(self.book_id)
        self._other_stats_url = f"https://www.goodreads.com/book/stats?id={numeric_id(self.book_id)}"
        self._series_url = f"https://www.goodreads.com/series/{self.book_id}"
        self._shelves_url = f"https://www.goodreads.com/work/shelves/{self.book_id}"
        self._editions_url = f"https://www.goodreads.com/work/editions/{numeric_id(self.book_id)}"

    @staticmethod
    def book_id_from_data(title: str, author: str, authors_data: _AuthorsData) -> str | None:
        """Derive Goodreads book ID from provided authors data.

        Args:
            title: book's title
            author: book author's full name or Goodreads author ID
            authors_data: data as read from JSON saved by dump_authors()

        Returns:
            derived book ID or None
        """
        authors: List[Author] = authors_data["authors"]
        if is_goodreads_id(author):
            author = from_iterable(authors, lambda a: a.book_id == author)
        else:
            author = from_iterable(authors, lambda a: a.name == author)
        if not author:
            return None
        book = from_iterable(author.books, lambda b: b.title == title)
        if not book:
            # Goodreads gets fancy with their apostrophes...
            book = from_iterable(author.books,
                                 lambda b: b.title == title.replace("'", "’"))
            if not book:
                return None
        return book.id

    @staticmethod
    def fetch_book_id(title: str, author: str) -> str | None:
        """Scrape author data and extract Goodreads book ID from it according to arguments passed.

        Args:
            title: book's title
            author: book author's name or author ID

        Returns:
            fetched book ID or None
        """
        author = AuthorParser(author).fetch_data()
        book = from_iterable(author.books, lambda b: b.title == title)
        if not book:
            return None
        return book.id

    @classmethod
    def find_book_id(cls, title: str, author: str,
                     authors_data: _AuthorsData | None = None) -> str | None:
        """Find Goodreads book ID based on provided arguments.

        Performs the look-up on ``authors_data`` if provided. Otherwise, scrapes Goodreads author
        page for the ID.

        Args:
            title: book's title
            author: book's author or author ID
            authors_data: data as read from JSON saved by dump_authors() (if not provided, Goodreads author page is scraped)

        Returns:
            book ID found or None
        """
        id_ = None
        if authors_data:
            id_ = cls.book_id_from_data(title, author, authors_data)
        if not id_:
            id_ = cls.fetch_book_id(title, author)
        return id_

    @staticmethod
    def _parse_first_published(soup: BeautifulSoup) -> datetime:
        p_tag = soup.find(
            lambda t: t.name == "p" and t.attrs.get("data-testid") == "publicationInfo")
        if p_tag is None:
            raise ParsingError("No tag with first publication data")
        *_, text = p_tag.text.split("published")
        return datetime.strptime(text.strip(), "%B %d, %Y")

    @staticmethod
    def _parse_contributor(a_tag: Tag) -> _Contributor:
        url = a_tag.attrs.get("href")
        if not url:
            raise ParsingError("No 'href' attribute on contributor 'a' tag")
        *_, id_ = url.split("/")
        return _Contributor(id_, a_tag.find("span", class_="ContributorLink__role") is not None)

    @classmethod
    def _parse_authors_line(cls, soup: BeautifulSoup) -> List[str]:
        contributor_div = soup.find("div", class_="ContributorLinksList")
        if contributor_div is None:
            raise ParsingError("No 'div' tag with contributors data")
        contributor_tags = contributor_div.find_all("a")
        if not contributor_tags:
            raise ParsingError("No contributor data 'a' tags")
        contributors = [cls._parse_contributor(tag) for tag in contributor_tags]
        authors = [c for c in contributors if not c.has_role]
        if not authors:
            return [contributors[0].author_id]
        return [a.author_id for a in authors]

    @staticmethod
    def _parse_specifics_row(row: Tag) -> Tuple[int, int]:
        label = row.attrs.get("aria-label")
        if not label:
            raise ParsingError("No label for detailed ratings")
        ratings_div = row.find("div", class_="RatingsHistogram__labelTotal")
        if ratings_div is None:
            raise ParsingError("No 'div' tag with detailed ratings data")
        text, _ = ratings_div.text.split("(")
        ratings = extract_int(text)
        return extract_int(label), ratings

    def _parse_ratings_stats(self, soup: BeautifulSoup) -> RatingStats:
        ratings_div: Tag | None = soup.find("div", class_="ReviewsSectionStatistics")
        if ratings_div is None:
            raise ParsingError(f"This book ID: {self.book_id!r} doesn't produce a parseable markup")
        general_div = ratings_div.find("div", class_="RatingStatistics__meta")
        if general_div is None:
            raise ParsingError("No detailed ratings pane")
        text = general_div.attrs.get("aria-label")
        if not text:
            raise ParsingError("No ratings/reviews text")
        _, reviews_text = text.split("ratings")
        reviews = extract_int(reviews_text)
        specifics_rows = ratings_div.find_all("div", class_="RatingsHistogram__bar")
        if not specifics_rows:
            raise ParsingError("No rows with detailed ratings")
        dist = FiveStars(dict(self._parse_specifics_row(row) for row in specifics_rows))
        return RatingStats(dist, reviews)

    @staticmethod
    def _parse_meta_script_tag(soup: BeautifulSoup):
        t = soup.find("script", id="__NEXT_DATA__")
        try:
            parser = _ScriptTagParser(json.loads(t.text)["props"]["pageProps"]["apolloState"])
        except KeyError:
            raise ParsingError("No valid meta 'script' tag to parse")
        parser.parse()

    @throttled(THROTTLING_DELAY)
    def fetch_data(self) -> DetailedBook:
        soup = getsoup(self._url)
        first_published = self._parse_first_published(soup)
        authors = self._parse_authors_line(soup)
        ratings_stats = self._parse_ratings_stats(soup)
        d2 = self._parse_meta_script_tag(soup)
        pass


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


def dump_authors(*authors: str, **kwargs: Any) -> None:
    """Fetch data on ``authors`` and dump it to JSON.

    Example authors: [
        "Isaac Asimov",
        "Frank Herbert",
        "Jacek Dukaj",
        "Andrzej Sapkowski",
        "J.R.R. Tolkien",
        "C.S. Lewis",
        "Cordwainer Smith",
        "Michael Moorcock",
        "Clifford D. Simak",
        "George R.R. Martin",
        "Joe Abercrombie",
        "Ursula K. Le Guin",
    ]

    Args:
        authors: variable number of author full names or Goodread author IDs (in case of the latter there's one request fewer)
        kwargs: optional arguments (e.g. a prefix for a dumpfile's name, an output directory, etc.)
    """
    timestamp = datetime.now()
    data = {
        "timestamp": timestamp.strftime(READABLE_TIMESTAMP_FORMAT),
        "provider": PROVIDER,
        "authors": [],
    }
    for i, author in enumerate(authors, start=1):
        print(f"Scraping author #{i}: {author!r}...")
        parser = AuthorParser(author)
        try:
            fetched = parser.fetch_data_with_backoff()
        except ParsingError as e:
            print(f"{e}. Skipping...")
            continue
        data["authors"].append(fetched.as_dict)

        if i != len(authors):
            print()

    data["authors"] = sorted(data["authors"], key=lambda item: item["name"].casefold())

    # kwargs
    prefix = kwargs.get("prefix") or ""
    prefix = f"{prefix}_" if prefix else ""
    use_timestamp = kwargs.get("use_timestamp") if kwargs.get("use_timestamp") is not None else \
        True
    timestamp = f"_{timestamp.strftime(FILNAME_TIMESTAMP_FORMAT)}" if use_timestamp else ""
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
        print(f"Successfully dumped '{dest}'")


def update_authors(authors_json: PathLike) -> None:
    """Load authors from ``authors_json``, scrape them again and save at the same location (
    with updated file timestamp).

    Args:
        authors_json: path to a JSON file saved earlier by dump_authors()
    """
    output_dir = Path(authors_json).parent
    data = load_authors(authors_json)
    authors = data["authors"]
    ids = [author.id for author in authors]
    dump_authors(*ids, prefix="authors", output_dir=output_dir)


def update_tolkien() -> None:
    outputdir = Path(__file__).parent.parent / "data"
    dump_authors("J.R.R. Tolkien", use_timestamp=False, outputdir=outputdir,
                 filename="tolkien.json")


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
