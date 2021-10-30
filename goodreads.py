"""

    goodreads.py
    ~~~~~~~~~~~~
    Goodreads scraping and parsing.

    @author: z33k

"""
import re
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from contexttimer import Timer

from constants import REQUEST_TIMOUT


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


def getsoup(url: str) -> BeautifulSoup:
    print(f"Requesting: {url!r}")
    with Timer() as t:
        markup = requests.get(url, timeout=REQUEST_TIMOUT).text
    print(f"Request completed in {t.elapsed:3f} seconds.")
    return BeautifulSoup(markup, "lxml")


class AuthorParser:
    """Goodreads author page parser.
    """
    LIST_URL_TEMPLATE = "https://www.goodreads.com/author/list/{}"

    def __init__(self, surname: str, *names: str) -> None:
        self.surname = surname
        self.names = names
        self.stats: Optional[AuthorStats] = None
        self.books: List[Book] = []

    @property
    def allnames(self) -> List[str]:
        return [*self.names, self.surname]

    def find_author_link(self) -> str:
        """Find Goodreads author link.

        Example:
            'https://www.goodreads.com/author/show/7415.Harlan_Ellison'
        """
        def parse_spans(spans_: List[Tag], *names: str) -> Optional[Tag]:
            i, result = 0, None
            while not result:
                if i == len(spans_):
                    break
                span = spans_[i]
                re_parts = [f"(?=.*{name})" for name in names]
                result = span.find(href=re.compile("".join(re_parts)))
                i += 1

            return result

        query = "+".join(self.allnames)
        url_template = "https://www.goodreads.com/search?q={}"
        url = url_template.format(query)
        soup = getsoup(url)
        spans = soup.find_all("span", itemprop="author")
        if not spans:
            raise ValueError(f"{self.allnames!r} are not valid Goodreads author names.")

        a = parse_spans(spans, *self.allnames)

        if not a:
            if len(self.allnames) > 2:
                a = parse_spans(spans, self.allnames[0], self.allnames[-1])
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

    def parse_author_list(self, author_id: str) -> Tuple[AuthorStats, List[Book]]:
        """Parse Goodreads author list page.

        Example URL:
            https://www.goodreads.com/author/list/7415.Harlan_Ellison

        :param author_id: last part of the URL, e.g.: '7415.Harlan_Ellison'
        :return: AuthorStats object and a list of Book objects
        """
        url = self.LIST_URL_TEMPLATE.format(author_id)
        soup = getsoup(url)
        container = soup.find("div", class_="leftContainer")

        # author stats
        div = container.find("div", class_="")
        text = div.text.strip()
        parts = [part.strip() for part in text.split("\n")[1:]]
        parts = [part.strip(" ·") for part in parts]
        stats = self.parse_author_stats(parts)

        # books
        table = container.find("table", class_="tableList")
        rows = table.find_all("tr")
        books = [self.parse_book_table_row(row) for row in rows]

        return stats, books

    @staticmethod
    def parse_author_stats(parts: List[str]) -> AuthorStats:
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
    def parse_book_table_row(row: Tag) -> Book:
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

    def get_stats_and_books(self) -> None:
        link = self.find_author_link()
        author_id = self.extract_id(link)
        self.stats, self.books = self.parse_author_list(author_id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(stats={self.stats}, books={self.books[:5]})"


class BookParser:
    """Goodreads book page parser.
    """
    URL_TEMPLATE = "https://www.goodreads.com/book/show/{}"


