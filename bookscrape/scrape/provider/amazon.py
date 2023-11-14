"""

    bookscrape.scrape.provider.amazon.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape Amazon for data on books.

    @author: z33k

"""
from collections import OrderedDict
from dataclasses import dataclass

from bookscrape.constants import Json
from bookscrape.scrape.stats import FiveStars
from bookscrape.scrape.utils import getsoup


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


class UrlScraper:
    REVIEWS_URL_TEMPLATE = ("https://www.amazon.com/product-reviews/{}"
                            "/ref=cm_cr_arp_d_show_all?ie=UTF8")
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, '
                      'like Gecko) Chrome/54.0.2840.71 Safari/537.36'
    }

    @property
    def url(self) -> str:
        return self._url

    @property
    def id(self) -> str:
        return self._id

    def __init__(self, url: str) -> None:
        self._url = url
        self._id = url2id(url)

    def scrape(self) -> Book:
        soup = getsoup(self._url, headers=self.HEADERS)
        pass


def url2id(url: str) -> str | None:
    """Extract amazon ID from URL.
    """
    if "/" not in url:
        return None
    *_, id_ = url.split("/")
    if id_.isalnum() and id_.isupper():
        return id_
    return None

