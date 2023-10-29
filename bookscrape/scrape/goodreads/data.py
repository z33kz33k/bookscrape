"""

    bookscrape.goodreads.data.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Data objects for Goodreads scraping.

    @author: z33k

"""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from bookscrape.constants import Json
from bookscrape.scrape import FiveStars, LangReviewsDistribution, Renown
from bookscrape.scrape.goodreads import HOBBIT_RATINGS, TOLKIEN_RATINGS, numeric_id


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


@dataclass
class Book:
    title: str
    id: str
    avg_rating: float
    ratings: int
    publication_year: Optional[datetime]
    editions: Optional[int]

    @property
    def as_dict(self) -> Dict[str, int | float | str]:
        data = {
            "title": self.title,
            "id": self.id,
            "avg_rating": self.avg_rating,
            "ratings": self.ratings,
        }
        if self.publication_year is not None:
            data.update({
                "publication_year": self.publication_year.year,
            })
        if self.editions is not None:
            data.update({
                "editions": self.editions,
            })
        data.update({"renown": self.renown.name})
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, int | float | str]) -> "Book":
        publication_year = data.get("publication_year")
        return cls(
            data["title"],
            data["id"],
            data["avg_rating"],
            data["ratings"],
            datetime(publication_year, 1, 1) if publication_year is not None else None,
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


@dataclass
class DetailedBook:
    title: str
    id: str
    authors: List[str]  # list of author ID's
    series: List[str]  # list of book ID's
    first_publication: datetime
    ratings: FiveStars
    reviews: int | LangReviewsDistribution
    shelves: Dict[str, int]  # TODO: extract data on genres only
    titles: Dict[str, str]  # TODO: scraped from editions page

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.ratings.total, HOBBIT_RATINGS)

    @property
    def r2r(self) -> float:
        return self.reviews / self.ratings.total

    @property
    def r2r_percent(self) -> str:
        r2r = self.r2r * 100
        return f"{r2r:.2f} %"


@dataclass
class MainEdition:
    publisher: str
    publication: datetime
    format: str
    pages: int
    language: str
    isbn: str
    isbn13: str
    asin: str

