"""

    bookscrape.scrape.provider.amazon.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape Amazon for data on books.

    @author: z33k

"""
import random
from collections import OrderedDict
from dataclasses import dataclass
from typing import Tuple

from bs4 import BeautifulSoup

from bookscrape.constants import Json
from bookscrape.scrape.stats import FiveStars
from bookscrape.scrape.utils import getsoup, throttled
from bookscrape.utils import extract_int


# the unofficially known enforced throttling delay
# between requests to Amazon servers is 0.5 s
# we're choosing to be safe here
def throttling_delay() -> float:
    return random.uniform(0.5, 1.0)


@dataclass
class Book:
    ratings: FiveStars
    total_reviews: int
    bestseller_ranks: OrderedDict[int, str]

    @property
    def r2r(self) -> float:
        return self.total_reviews / self.ratings.total if self.ratings.ratings else 0

    @property
    def r2r_percent(self) -> str:
        r2r = self.r2r * 100
        return f"{r2r:.2f} %"

    @property
    def as_dict(self) -> Json:
        return {
            "ratings": self.ratings.as_dict,
            "total_reviews": self.total_reviews,
            "bestseller_ranks": self.bestseller_ranks,
        }

    @classmethod
    def from_dict(cls, data: Json) -> "Book":
        return cls(
            FiveStars({int(k): v for k, v in data["ratings"].items()}),
            data["total_reviews"],
            OrderedDict(sorted([(int(k), v) for k, v
                                in data["bestseller_ranks"].items()])))


class Scraper:
    URL_TEMPLATE = "https://www.amazon.com/gp/product/{}"
    REVIEWS_URL_TEMPLATE = ("https://www.amazon.com/product-reviews/{}"
                            "/ref=cm_cr_arp_d_show_all?ie=UTF8")
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, '
                      'like Gecko) Chrome/54.0.2840.71 Safari/537.36'
    }

    @property
    def id(self) -> str:
        return self._id

    def __init__(self, book_cue: str | Tuple[str, str]) -> None:
        if isinstance(book_cue, tuple):
            book, author = book_cue  # TODO: tbc.
        elif not isinstance(book_cue, str) or not is_amazon_id(book_cue):
            raise ValueError("Book ID or (title, author) tuple must be provided")
        else:
            self._id = book_cue

    @staticmethod
    @throttled(throttling_delay)
    def _parse_bestseller_ranks(soup: BeautifulSoup) -> OrderedDict[int, str]:
        ul = soup.find("ul", class_="a-unordered-list a-nostyle a-vertical a-spacing-none "
                                    "detail-bullet-list")
        span = ul.find("span", class_="a-list-item")
        tokens = [span.find("span").text.strip()]
        for li_tag in span.find_all("li"):
            span_tag = li_tag.find("span")
            token = span_tag.text
            token += span_tag.find("a").text.string()
            tokens.append(token)
        ranks = []
        for token in tokens:
            number, category = token.split(" in ")
            ranks.append((extract_int(number), category))
        return OrderedDict(ranks)

    @staticmethod
    @throttled(throttling_delay)
    def _parse_reviews_page(soup: BeautifulSoup) -> Tuple[FiveStars, int]:
        pass

    def scrape(self) -> Book:
        url = self.URL_TEMPLATE.format(self.id)
        soup = getsoup(url, headers=self.HEADERS)
        bestseller_ranks = self._parse_bestseller_ranks(soup)
        url = self.REVIEWS_URL_TEMPLATE.format(self.id)
        soup = getsoup(url, headers=self.HEADERS)
        ratings, total_reviews = self._parse_reviews_page(soup)
        return Book(ratings, total_reviews, bestseller_ranks)

    @staticmethod
    def url2id(url: str) -> str | None:
        """Extract amazon ID from URL.
        """
        if "/" not in url:
            return None
        *_, id_ = url.split("/")
        if id_.isalnum() and id_.isupper():
            return id_
        return None


def is_amazon_id(id_: str) -> bool:
    return id_.isalnum() and id_.isupper()
