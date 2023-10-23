"""

    bookscrape.goodreads.py
    ~~~~~~~~~~~~~~~~~~~~~~~
    Scrape and parse Goodreads data

    @author: z33k

"""
import json
import re
import time
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

import backoff
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests import Timeout

from bookscrape.constants import (DELAY, Json, OUTPUT_DIR, PathLike, READABLE_TIMESTAMP_FORMAT,
                                  FILNAME_TIMESTAMP_FORMAT)
from bookscrape.utils import getdir, getfile, getsoup, extract_int, extract_float, from_iterable, \
    throttle
from bookscrape.data import ParsingError, Renown

PROVIDER = "goodreads.com"


def _load_tolkien() -> Tuple[int, int]:
    source = Path(__file__).parent / "data" / "tolkien.json"
    if not source.exists():
        raise FileNotFoundError(f"'{source}' not found")

    with source.open(encoding="utf8") as f:
        data = json.load(f)

    if not data:
        raise ValueError(f"No data in '{source}'")

    try:
        tolkien_ratings = data["authors"][0]["stats"]["ratings"]
        fellowship_ratings = data["authors"][0]["books"][1]["ratings"]
    except (KeyError, IndexError):
        raise ValueError(f"Invalid data in '{source}'")
    return tolkien_ratings, fellowship_ratings


try:
    TOLKIEN_RATINGS, FELLOWSHIP_RATINGS = _load_tolkien()  # 10_674_789, 2_736_955 on 18th Oct 2023
except ValueError:
    TOLKIEN_RATINGS, FELLOWSHIP_RATINGS = 10_674_789, 2_736_955


@dataclass
class AuthorStats:
    avg_rating: float
    ratings: int
    reviews: int
    shelvings: int

    @property
    def as_dict(self) -> Dict[str, Union[float, int]]:
        return {
            "avg_rating": self.avg_rating,
            "ratings": self.ratings,
            "reviews": self.reviews,
            "shelvings": self.shelvings,
            "r2r": self.r2r_percent,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Union[float, int]]) -> "AuthorStats":
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
    published_in: Optional[int]
    editions: Optional[int]

    @property
    def as_dict(self) -> Dict[str, Union[str, int, float]]:
        data = {
            "title": self.title,
            "id": self.id,
            "avg_rating": self.avg_rating,
            "ratings": self.ratings,
        }
        if self.published_in is not None:
            data.update({
                "published_in": self.published_in,
            })
        if self.editions is not None:
            data.update({
                "editions": self.editions,
            })
        data.update({"renown": self.renown.name})
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Union[str, int, float]]) -> "Book":
        return cls(
            data["title"],
            data["id"],
            data["avg_rating"],
            data["ratings"],
            data.get("published_in"),
            data.get("editions"),
        )

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.ratings, FELLOWSHIP_RATINGS)

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
    def fullname(self) -> str:
        return self._fullname

    @property
    def normalized_name(self) -> str:
        # Goodreads logic that formulates author ID:

        # 1) replace any whitespace with underscore
        # Example: '589.Orson_Scott_Card'

        # 2) replace any non-alphabetic character with underscore
        # Apostrophe: 'Madeleine L'Engle' ==> '106.Madeleine_L_Engle'
        # Hyphen: 'Chi Ta-wei' ==> '14640243.Chi_Ta_wei'
        # Dot: 'George R.R. Martin' ==> '346732.George_R_R_Martin'

        # 3) replace any non-ASCII character with underscore
        # Example: 'Stanisław Lem' ==> '10991.Stanis_aw_Lem'

        # 4) replace any immediately repeated underscore with only one instance
        # Example: 'Ewa Białołęcka' ==> '554577.Ewa_Bia_o_cka'
        chars, underscore_appended = [], False
        for char in self.fullname:
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

    def __init__(self, fullname: str) -> None:
        self._fullname = fullname

    def find_author_url(self) -> str:
        """Find Goodreads author URL.

        Example:
            'https://www.goodreads.com/author/show/7415.Harlan_Ellison'
        """
        def parse_spans(spans_: List[Tag]) -> Optional[Tag]:
            for span in spans_:
                a_ = span.find(
                    lambda t: t.name == "a" and self.normalized_name in t.attrs.get("href"))
                if a_ is not None:
                    return a_
            return None

        query = "+".join(self.fullname.split())
        url_template = "https://www.goodreads.com/search?q={}"
        url = url_template.format(query)
        soup = getsoup(url)
        spans = soup.find_all("span", itemprop="author")
        if not spans:
            raise ParsingError(f"No 'span' tags with queried authors data")

        a = parse_spans(spans)
        if not a:
            raise ParsingError(f"No 'a' tag with author URL")

        url = a.attrs.get("href")
        # URL now ought to look like this:
        # 'https://www.goodreads.com/author/show/7415.Harlan_Ellison?from_search=true&from_srp=true'
        url, _ = url.split("?")  # stripping the trash part
        return url

    @staticmethod
    def extract_id(author_url: str) -> str:
        """Extract Goodreads author ID from ``author_url``.

        Args:
            author_url: Goodreads author URL, e.g.: 'https://www.goodreads.com/author/show/7415.Harlan_Ellison'

        Returns:
            an author ID, e.g.: '7415.Harlan_Ellison'
        """
        *_, id_ = author_url.split("/")
        return id_

    def parse_author_page(self, author_id: str) -> Tuple[AuthorStats, List[Book]]:
        """Parse Goodreads author page.

        Example URL:
            https://www.goodreads.com/author/list/7415.Harlan_Ellison

        Args:
            author_id: last part of the URL, e.g.: '7415.Harlan_Ellison'

        Returns:
            an AuthorStats object and a list of Book objects
        """
        url = self.URL_TEMPLATE.format(author_id)
        soup = getsoup(url)
        container = soup.find("div", class_="leftContainer")

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

        return stats, books

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

    def _parse_book_table_row(self, row: Tag) -> Book:
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
        published = self._parse_published(row)
        editions = self._parse_editions(row)

        return Book(title, id_, avg, ratings, published, editions)

    def fetch_data(self) -> Author:
        try:
            url = self.find_author_url()
            author_id = self.extract_id(url)
            stats, books = self.parse_author_page(author_id)
        except Timeout:
            print("Goodreads doesn't play nice. Timeout exceeded. Retrying with backoff "
                  "(60 seconds max)...")
            return self.fetch_data_with_backoff()
        return Author(self.fullname, author_id, stats, books)

    @backoff.on_exception(backoff.expo, Timeout, max_time=60)
    def fetch_data_with_backoff(self) -> Author:
        link = self.find_author_url()
        author_id = self.extract_id(link)
        stats, books = self.parse_author_page(author_id)
        return Author(self.fullname, author_id, stats, books)


@dataclass
class RatingStats:
    ratings: int
    one_star_ratings: int
    two_stars_ratings: int
    three_stars_ratings: int
    four_stars_ratings: int
    five_stars_ratings: int
    reviews: int

    def __post_init__(self) -> None:
        total = sum([self.one_star_ratings, self.two_stars_ratings, self.three_stars_ratings,
                     self.four_stars_ratings, self.five_stars_ratings])
        if total != self.ratings:
            print("WARNING: Total ratings counted from partial ones different than the parsed "
                  "total")

    @property
    def avg_rating(self) -> float:
        return sum([
            self.one_star_ratings, self.two_stars_ratings * 2, self.three_stars_ratings * 3,
            self.four_stars_ratings * 4, self.five_stars_ratings * 5
        ]) / self.ratings

    @property
    def one_star_percent(self) -> str:
        percent = self.one_star_ratings * 100 / self.ratings
        return f"{percent:.2f} %"

    @property
    def two_stars_percent(self) -> str:
        percent = self.two_stars_ratings * 100 / self.ratings
        return f"{percent:.2f} %"

    @property
    def three_stars_percent(self) -> str:
        percent = self.three_stars_ratings * 100 / self.ratings
        return f"{percent:.2f} %"

    @property
    def four_stars_percent(self) -> str:
        percent = self.four_stars_ratings * 100 / self.ratings
        return f"{percent:.2f} %"

    @property
    def five_stars_percent(self) -> str:
        percent = self.five_stars_ratings * 100 / self.ratings
        return f"{percent:.2f} %"

    @property
    def r2r(self) -> float:
        return self.reviews / self.ratings if self.ratings else 0

    @property
    def r2r_percent(self) -> str:
        r2r = self.r2r * 100
        return f"{r2r:.2f} %"


@dataclass
class OtherStats:
    want_to_read: int
    shelvings: int
    editions: int


@dataclass
class DetailedBook:
    title: str
    authors: List[str]  # list of author ID's
    series: List[str]  # list of book ID's
    first_published: datetime
    ratings_stats: RatingStats
    other_stats: OtherStats
    shelves: Dict[str, int]  # TODO: extract data on genres only
    titles: Dict[str, str]  # TODO: scraped from editions page

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.ratings_stats.ratings, FELLOWSHIP_RATINGS)


AuthorsData = Dict[str, datetime | str | List[Author]]
_Contributor = namedtuple("_Contributor", "author_id has_role")


# TODO: scrape more data
class BookParser:
    """Goodreads book page parser.
    """
    URL_TEMPLATE = "https://www.goodreads.com/book/show/{}"
    DATE_FORMAT = "%B %d, %Y"  # datetime.strptime("August 16, 2011", "%B %d, %Y")

    @property
    def id(self) -> str:
        return self._id

    def __init__(self, title: str, author: str,
                 authors_data: AuthorsData | None = None) -> None:
        self._id = self.find_id(title, author, authors_data)
        if not self._id:
            raise ValueError("Unable to derive Goodreads book ID from provided input")
        self._url = self.URL_TEMPLATE.format(self.id)
        self._other_stats_url = f"https://www.goodreads.com/book/stats?id={numeric_id(self.id)}"
        self._series_url = f"https://www.goodreads.com/series/{self.id}"
        self._shelves_url = f"https://www.goodreads.com/work/shelves/{self.id}"
        self._editions_url = f"https://www.goodreads.com/work/editions/{numeric_id(self.id)}"

    @staticmethod
    def id_from_data(title: str, author: str, authors_data: AuthorsData) -> str | None:
        """Derive Goodreads book ID from provided authors data.

        Args:
            title: book's title
            author: book's author
            authors_data: data as read from JSON saved by dump_authors()

        Returns:
            derived ID or None
        """
        authors: List[Author] = authors_data["authors"]
        author = from_iterable(authors, lambda a: a.name == author)
        if not author:
            return None
        book = from_iterable(author.books, lambda b: b.title == title)
        if not book:
            return None
        return book.id

    @staticmethod
    def fetch_id(title: str, author: str) -> str | None:
        """Scrape author data and extract Goodreads book ID from it according to arguments passed.

        Args:
            title: book's title
            author: book's author

        Returns:
            fetched ID or None
        """
        author = AuthorParser(author).fetch_data()
        book = from_iterable(author.books, lambda b: b.title == title)
        if not book:
            return None
        return book.id

    @classmethod
    def find_id(cls, title: str, author: str,
                authors_data: AuthorsData | None = None) -> str | None:
        """Find Goodreads book ID based on provided arguments.

        Performs the look-up on ``authors_data`` if provided. Otherwise, scrapes Goodreads author
        page for the ID.

        Args:
            title: book's title
            author: book's author
            authors_data: data as read from JSON saved by dump_authors() (if not provided, Goodreads author page is scraped)

        Returns:
            book ID found or None
        """
        id_ = None
        if authors_data:
            id_ = cls.id_from_data(title, author, authors_data)
        if not id_:
            id_ = cls.fetch_id(title, author)
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

    def _parse_authors_line(self, soup: BeautifulSoup) -> List[str]:
        contributor_div = soup.find("div", class_="ContributorLinksList")
        if contributor_div is None:
            raise ParsingError("No 'div' tag with contributors data")
        contributor_tags = contributor_div.find_all("a")
        if not contributor_tags:
            raise ParsingError("No contributor data 'a' tags")
        contributors = [self._parse_contributor(tag) for tag in contributor_tags]
        authors = [c for c in contributors if not c.has_role]
        if not authors:
            return [contributors[0].author_id]
        return [a.author_id for a in authors]

    @staticmethod
    def _parse_specifics_row(row: Tag) -> Tuple[str, int]:
        label = row.attrs.get("aria-label")
        if not label:
            raise ParsingError("No label for detailed ratings")
        ratings_div = row.find("div", class_="RatingsHistogram__labelTotal")
        if ratings_div is None:
            raise ParsingError("No 'div' tag with detailed ratings data")
        text, _ = ratings_div.text.split("(")
        ratings = extract_int(text)
        return label, ratings

    def _parse_ratings_stats(self, soup: BeautifulSoup) -> RatingStats:
        ratings_div: Tag | None = soup.find("div", class_="ReviewsSectionStatistics")
        if ratings_div is None:
            raise ParsingError(f"This book ID: {self.id!r} doesn't produce a parseable markup")
        general_div = ratings_div.find("div", class_="RatingStatistics__meta")
        if general_div is None:
            raise ParsingError("No detailed ratings pane")
        text = general_div.attrs.get("aria-label")
        if not text:
            raise ParsingError("No ratings/reviews text")
        ratings_text, reviews_text = text.split("ratings")
        ratings, reviews = extract_int(ratings_text), extract_int(reviews_text)
        specifics_rows = ratings_div.find_all("div", class_="RatingsHistogram__bar")
        if not specifics_rows:
            raise ParsingError("No rows with detailed ratings")
        specific_ratings = dict(self._parse_specifics_row(row) for row in specifics_rows)
        return RatingStats(
            ratings,
            specific_ratings["1 star"],
            specific_ratings["2 stars"],
            specific_ratings["3 stars"],
            specific_ratings["4 stars"],
            specific_ratings["5 stars"],
            reviews
        )

    def _parse_other_stats(self) -> OtherStats:
        pass

    def fetch_data(self) -> DetailedBook:
        soup = getsoup(self._url)
        first_published = self._parse_first_published(soup)
        authors = self._parse_authors_line(soup)
        ratings_stats = self._parse_ratings_stats(soup)
        other_stats = self._parse_other_stats()
        pass


def load_authors(authors_json: PathLike) -> AuthorsData:
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
        authors: variable number of author full names
        kwargs: optional arguments (e.g. a prefix for a dumpfile's name, an output directory, etc.)
    """
    timestamp = datetime.now()
    data = {
        "timestamp": timestamp.strftime(READABLE_TIMESTAMP_FORMAT),
        "provider": PROVIDER,
        "authors": [],
    }
    delay = kwargs.get("delay") or DELAY
    for i, author in enumerate(authors, start=1):
        print(f"Scraping author #{i}: {author!r}...")
        parser = AuthorParser(fullname=author)
        try:
            fetched = parser.fetch_data_with_backoff()
        except ParsingError as e:
            print(f"{e}. Skipping...")
            continue
        data["authors"].append(fetched.as_dict)

        if len(authors) > 1:
            throttle(delay)
            print()

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


def update_tolkien() -> None:
    outputdir = Path(__file__).parent / "data"
    dump_authors("J.R.R. Tolkien", use_timestamp=False, outputdir=outputdir,
                 filename="tolkien.json")
