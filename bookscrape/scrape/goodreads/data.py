"""

    bookscrape.goodreads.data.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Data objects for Goodreads scraping.

    @author: z33k

"""
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, OrderedDict, Tuple

from bookscrape.constants import Json, READABLE_TIMESTAMP_FORMAT
from bookscrape.scrape import FiveStars, LangReviewsDistribution, Renown
from bookscrape.scrape.goodreads.utils import numeric_id
from bookscrape.utils import getfile


def _load_tolkien() -> Tuple[int, int]:
    source = getfile(Path(__file__).parent.parent.parent / "data" / "tolkien.json")
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
class MainEdition:
    publisher: str
    publication: datetime
    format: str
    pages: int
    language: str
    isbn: str
    isbn13: str
    asin: str

    @property
    def as_dict(self) -> Dict[str, str | int]:
        return {
            "publisher": self.publisher,
            "publication": self.publication.strftime(READABLE_TIMESTAMP_FORMAT),
            "format": self.format,
            "pages": self.pages,
            "language": self.language,
            "isbn": self.isbn,
            "isbn13": self.isbn13,
            "asin": self.asin,
        }


@dataclass
class BookAward:
    name: str
    id: str
    date: datetime
    category: Optional[str]
    designation: str

    @property
    def as_dict(self) -> Dict[str, str]:
        data = {
            "name": self.name,
            "id": self.id,
            "date": self.date.strftime(READABLE_TIMESTAMP_FORMAT),
            "designation": self.designation,
        }
        if self.category:
            data["category"] = self.category

        return data


@dataclass
class BookSetting:
    name: str
    id: str
    country: Optional[str]
    year: Optional[datetime]

    @property
    def as_dict(self) -> Dict[str, str]:
        data = {
            "name": self.name,
            "id": self.id,
        }
        if self.country:
            data["country"] = self.country
        if self.year is not None:
            data["year"] = self.year.strftime(READABLE_TIMESTAMP_FORMAT)

        return data


@dataclass
class BookDetails:
    description: str
    main_edition: MainEdition
    genres: List[str]
    awards: List[BookAward]
    places: List[BookSetting]
    characters: List[str]

    @property
    def as_dict(self) -> Dict[str, Any]:
        data = {
            "description": self.description,
            "main_edition": self.main_edition.as_dict,
            "genres": self.genres,
            "characters": self.characters
        }
        if self.awards:
            data["awards"] = [award.as_dict for award in self.awards]
        if self.places:
            data["places"] = [place.as_dict for place in self.places]
        if self.characters:
            data["characters"] = self.characters

        return data


@dataclass
class _ScriptTagData:
    title: Optional[str]
    complete_title: str
    work_id: str
    ratings: FiveStars
    reviews: LangReviewsDistribution
    total_reviews: int
    first_publication: Optional[datetime]
    details: BookDetails


@dataclass
class BookSeries:
    title: str
    id: str
    layout: Dict[float, str]  # numberings to book IDs

    @property
    def as_dict(self) -> Dict[str, str | Dict[float, str]]:
        return asdict(self)


@dataclass
class DetailedBook:
    title: str
    complete_title: str
    book_id: str
    work_id: str
    series: Optional[BookSeries]
    authors: List[str]  # list of author ID's
    first_publication: datetime
    ratings: FiveStars
    reviews: LangReviewsDistribution
    total_reviews: int  # this is different from total calculated from 'reviews' dict
    details: BookDetails
    shelves: OrderedDict[int, str]  # number of shelvings to shelves, only the first page is scraped
    editions: OrderedDict[str, List[str]]
    total_editions: int

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.ratings.total, HOBBIT_RATINGS)

    @property
    def r2r(self) -> float:
        return self.total_reviews / self.ratings.total

    @property
    def r2r_percent(self) -> str:
        r2r = self.r2r * 100
        return f"{r2r:.2f} %"

    @property
    def shelvings(self) -> int:
        return sum(s for s in self.shelves)

    @property
    def as_dict(self) -> Dict[str, Any]:
        data = {
            "title": self.title,
            "complete_title": self.complete_title,
            "book_id": self.book_id,
            "work_id": self.work_id,
            "authors": self.authors,
            "first_publication": self.first_publication.strftime(READABLE_TIMESTAMP_FORMAT),
            "ratings": self.ratings.as_dict,
            "reviews": self.reviews.as_dict,
            "total_reviews": self.total_reviews,
            "details": self.details.as_dict,
            "shelves": self.shelves,
            "editions": self.editions,
            "total_editions": self.total_editions,
        }
        if self.series:
            data["series"] = self.series.as_dict
        return data
