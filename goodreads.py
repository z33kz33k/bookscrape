"""

    goodreads.py
    ~~~~~~~~~~~~
    Goodreads scraping and parsing.

    @author: z33k

"""
import json
import re
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

from bs4.element import Tag
from requests import Timeout

from constants import DELAY, Json, TIMESTAMP_FORMAT
from utils import getsoup, non_ascii_index

TOLKIEN_RATINGS_COUNT = 9_323_827


class Renown(Enum):
    SUPERSTAR = int(TOLKIEN_RATINGS_COUNT / 3)
    STAR = range(int(TOLKIEN_RATINGS_COUNT / 10), int(TOLKIEN_RATINGS_COUNT / 3))
    FAMOUS = range(int(TOLKIEN_RATINGS_COUNT / 30), int(TOLKIEN_RATINGS_COUNT / 10))
    POPULAR = range(int(TOLKIEN_RATINGS_COUNT / 60), int(TOLKIEN_RATINGS_COUNT / 30))
    WELL_KNOWN = range(int(TOLKIEN_RATINGS_COUNT / 100), int(TOLKIEN_RATINGS_COUNT / 60))
    KNOWN = range(int(TOLKIEN_RATINGS_COUNT / 400), int(TOLKIEN_RATINGS_COUNT / 100))
    SOMEWHAT_KNOWN = range(int(TOLKIEN_RATINGS_COUNT / 200), int(TOLKIEN_RATINGS_COUNT / 400))
    LITTLE_KNOWN = range(int(TOLKIEN_RATINGS_COUNT / 1000), int(TOLKIEN_RATINGS_COUNT / 200))
    OBSCURE = range(int(TOLKIEN_RATINGS_COUNT / 1000))

    @property
    def priority(self) -> int:
        if self is Renown.SUPERSTAR:
            return 8
        elif self is Renown.STAR:
            return 7
        elif self is Renown.FAMOUS:
            return 6
        elif self is Renown.POPULAR:
            return 5
        elif self is Renown.WELL_KNOWN:
            return 4
        elif self is Renown.KNOWN:
            return 3
        elif self is Renown.SOMEWHAT_KNOWN:
            return 2
        elif self is Renown.LITTLE_KNOWN:
            return 1
        else:
            return 0


@dataclass
class AuthorStats:
    avg_rating: float
    ratings_count: int
    reviews_count: int
    shelvings_count: int

    @property
    def as_dict(self) -> Dict[str, Union[float, int]]:
        return {
            "avg_rating": self.avg_rating,
            "ratings_count": self.ratings_count,
            "reviews_count": self.reviews_count,
            "shelvings_count": self.shelvings_count,
        }

    @staticmethod
    def from_dict(data: Dict[str, Union[float, int]]) -> "AuthorStats":
        return AuthorStats(
            data["avg_rating"],
            data["ratings_count"],
            data["reviews_count"],
            data["shelvings_count"],
        )

    @property
    def r2r(self) -> float:
        return self.reviews_count / self.ratings_count if self.ratings_count else 0

    @property
    def r2r_percent(self) -> str:
        r2r = self.r2r * 100
        return f"{r2r:.2f} %"

    @property
    def renown(self) -> Renown:
        if self.ratings_count >= Renown.SUPERSTAR.value:
            return Renown.SUPERSTAR
        elif self.ratings_count in Renown.STAR.value:
            return Renown.STAR
        elif self.ratings_count in Renown.FAMOUS.value:
            return Renown.FAMOUS
        elif self.ratings_count in Renown.POPULAR.value:
            return Renown.POPULAR
        elif self.ratings_count in Renown.WELL_KNOWN.value:
            return Renown.WELL_KNOWN
        elif self.ratings_count in Renown.KNOWN.value:
            return Renown.KNOWN
        elif self.ratings_count in Renown.SOMEWHAT_KNOWN.value:
            return Renown.SOMEWHAT_KNOWN
        elif self.ratings_count in Renown.LITTLE_KNOWN.value:
            return Renown.LITTLE_KNOWN
        elif self.ratings_count in Renown.OBSCURE.value:
            return Renown.OBSCURE
        else:
            raise ValueError(f"Invalid ratings count: {self.ratings_count:,}.")


@dataclass
class Book:
    title: str
    avg_rating: float
    ratings_count: int
    id: str

    @property
    def as_dict(self) -> Dict[str, Union[str, int, float]]:
        return {
            "title": self.title,
            "avg_rating": self.avg_rating,
            "ratings_count": self.ratings_count,
            "id": self.id,
        }

    @staticmethod
    def from_dict(data: Dict[str, Union[str, int, float]]) -> "Book":
        return Book(
            data["title"],
            data["avg_rating"],
            data["ratings_count"],
            data["id"],
        )


class AuthorParser:
    """Goodreads author page parser.
    """
    URL_TEMPLATE = "https://www.goodreads.com/author/list/{}"

    def __init__(self, surname="", *names: str, **kwargs: Any) -> None:
        if "fullname" in kwargs:
            if surname:
                raise ValueError(f"Invalid argument 'surname'={surname!r} when 'fullname' "
                                 f"specified.")
            if names:
                raise ValueError(f"Invalid argument 'names'={names!r} when 'fullname' "
                                 f"specified.")
            *names, surname = kwargs["fullname"].split()
        self.surname = surname
        self.names = names
        self.stats: Optional[AuthorStats] = None
        self.books: List[Book] = []

    @property
    def allnames(self) -> List[str]:
        return [*self.names, self.surname]

    @property
    def fullname(self) -> str:
        return " ".join(self.allnames)

    def find_author_link(self) -> str:
        """Find Goodreads author link.

        Example:
            'https://www.goodreads.com/author/show/7415.Harlan_Ellison'
        """
        def parse_spans(spans_: List[Tag], *author_names: str) -> Optional[Tag]:
            i, result = 0, None
            while not result:
                if i == len(spans_):
                    break
                span = spans_[i]
                re_parts = [f"(?=.*{name})" for name in author_names]
                result = span.find(href=re.compile("".join(re_parts)))
                i += 1

            return result

        # Goodreads replaces a non-ASCII character with "_" in the author ID.
        # Example: '10991.Stanis_aw_Lem'
        # As the example above shows, underline is also used as a separator.
        # If a non-ASCII character occurs at the name's limit, underlines ARE NOT doubled
        # Example: '10089.Philip_Jos_Farmer'
        def handle_non_ascii(*author_names) -> List[str]:
            handled_names = []
            for name in author_names:
                idx = non_ascii_index(name)
                if idx != -1:
                    name = name[:idx] + "_" + name[idx + 1:]
                handled_names.append(name)
            return handled_names

        query = "+".join(self.allnames)
        url_template = "https://www.goodreads.com/search?q={}"
        url = url_template.format(query)
        soup = getsoup(url)
        spans = soup.find_all("span", itemprop="author")
        if not spans:
            raise ValueError(f"{self.allnames!r} are not valid Goodreads author names.")

        names = handle_non_ascii(*self.allnames)

        a = parse_spans(spans, *names)

        if not a:
            if len(names) > 2:
                a = parse_spans(spans, names[0], names[-1])
            if not a:
                raise ValueError(f"{self.allnames!r} are not valid Goodreads author names.")

        link = a.attrs.get("href")
        if not link:
            raise ValueError(f"{self.allnames!r} are not valid Goodreads author names.")
        # link now ought to look like this:
        # 'https://www.goodreads.com/author/show/7415.Harlan_Ellison?from_search=true&from_srp=true'
        link, _ = link.split("?")  # stripping the trash part
        return link

    @staticmethod
    def extract_id(author_link: str) -> str:
        """Extract Goodreads author ID from ``author_link``.

        :param author_link: Goodreads author link, e.g.: 'https://www.goodreads.com/author/show/7415.Harlan_Ellison'
        :return: author ID, e.g.: '7415.Harlan_Ellison'
        """
        *_, id_ = author_link.split("/")
        return id_

    def parse_author_page(self, author_id: str) -> Tuple[AuthorStats, List[Book]]:
        """Parse Goodreads author page.

        Example URL:
            https://www.goodreads.com/author/list/7415.Harlan_Ellison

        :param author_id: last part of the URL, e.g.: '7415.Harlan_Ellison'
        :return: AuthorStats object and a list of Book objects
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
            raise ValueError(f"Invalid parts: {parts}.")
        avg_rating = float(parts[0].replace("Average rating ", "").replace(" ·", ""))
        ratings_count = int(parts[1].replace(",", "").replace(" ratings", ""))
        reviews_count = int(parts[2].replace(",", "").replace(" reviews", ""))
        shelvings_count = int(parts[3]
                              .replace(",", "")
                              .replace("shelved ", "")
                              .replace(" times", ""))
        return AuthorStats(avg_rating, ratings_count, reviews_count, shelvings_count)

    @staticmethod
    def _parse_book_table_row(row: Tag) -> Book:
        """Parse a book table row of the author page's book list.

        :param row: a BeautifulSoup Tag object representing the row
        :return: Book object
        """
        a = row.find("a")
        if not a:
            raise ValueError(f"Invalid row: {row}.")
        title = a.attrs.get("title")
        if not title:
            raise ValueError(f"Invalid row: {row}.")
        href = a.attrs.get("href")
        if not href:
            raise ValueError(f"Invalid row: {row}.")
        id_ = href.replace("/book/show/", "")
        text = row.find("span", class_="minirating").text.strip()
        trash = ("liked it ", "really liked it ", "really ", "it was amazing ", "it was ok ",
                 "didn't like it ")
        for t in trash:
            if t in text:
                text = text.replace(t, "")
        avg, count = text.split(" — ")
        avg = float(avg.replace(" avg rating", ""))
        trash = (" ratings", " rating")
        t = next((t for t in trash if t in count), None)
        if not t:
            raise ValueError(f"Invalid row: {row}.")
        count = int(count.replace(",", "").replace(t, ""))
        return Book(title, avg, count, id_)

    def fetch_stats_and_books(self) -> None:
        link = self.find_author_link()
        author_id = self.extract_id(link)
        self.stats, self.books = self.parse_author_page(author_id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(stats={self.stats}, books={self.books[:5]})"

    def __str__(self) -> str:
        return self.fullname

    @property
    def as_dict(self) -> Json:
        return {
            self.fullname: {
                "stats": self.stats.as_dict,
                "books": [b.as_dict for b in self.books]
            }
        }


# TODO
class BookParser:
    """Goodreads book page parser.
    """
    URL_TEMPLATE = "https://www.goodreads.com/book/show/{}"


def dump(*authors: str, **kwargs: Any) -> None:
    """Dump ``authors`` to JSON.

    Example authors: [
        "Isaac Asimov",
        "Frank Herbert",
        "Jacek Dukaj",
        "Andrzej Sapkowski",
        "J. R. R. Tolkien",
        "C. S. Lewis",
        "Cordwainer Smith",
        "Michael Moorcock",
        "Clifford D. Simak",
        "George R. R. Martin",
        "Joe Abercrombie",
        "Ursula K. Le Guin",
    ]

    :param authors: variable number of author full names
    :param kwargs: optional arguments (e.g. a prefix for a dumpfile's name)
    """
    prefix = kwargs["prefix"] if "prefix" in kwargs else ""
    data = {}
    for i, author in enumerate(authors, start=1):
        print(f"Scraping author #{i}: {author!r}...")
        parser = AuthorParser(fullname=author)
        try:
            parser.fetch_stats_and_books()
        except Timeout:
            print("Goodreads doesn't play nice. Timeout exceeded. Exiting.")
            break
        data.update(parser.as_dict)

        print(f"Throttling for {DELAY} seconds...")
        time.sleep(DELAY)
        print()

    prefix = f"{prefix}_" if prefix else ""
    dest = Path("output") / f"{prefix}dump_{datetime.now().strftime(TIMESTAMP_FORMAT)}.json"
    with dest.open("w", encoding="utf8") as f:
        json.dump(data, f, indent=4)


