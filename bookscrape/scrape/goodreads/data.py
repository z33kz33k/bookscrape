"""

    bookscrape.goodreads.data.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Data objects for Goodreads scraping.

    @author: z33k

"""
import json
from collections import OrderedDict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bookscrape.constants import Json, READABLE_TIMESTAMP_FORMAT
from bookscrape.scrape import FiveStars, ReviewsDistribution, Renown
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
            "reviews_to_ratings": self.r2r_percent,
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

    @property
    def sh2r(self) -> float:
        return self.shelvings / self.ratings if self.ratings else 0

    @property
    def sh2r_percent(self) -> str:
        sh2r = self.sh2r * 100
        return f"{sh2r:.2f} %"


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


@dataclass
class Author:
    name: str
    id: str
    stats: AuthorStats
    top_books: List[Book]

    @property
    def as_dict(self) -> Json:
        return {
            "name": self.name,
            "id": self.id,
            "stats": self.stats.as_dict,
            "total_editions": self.total_editions,
            "renown": self.renown.name,
            "top_books": [b.as_dict for b in self.top_books],
        }

    @classmethod
    def from_dict(cls, data: Json) -> "Author":
        return cls(
            data["name"],
            data["id"],
            AuthorStats.from_dict(data["stats"]),
            [Book.from_dict(book) for book in data["top_books"]]
        )

    @property
    def total_editions(self) -> int:
        return sum(book.editions for book in self.top_books if book.editions)

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.stats.ratings, TOLKIEN_RATINGS)


@dataclass
class SimpleAuthor(Author):
    """An author with books only as a list of IDs.
    """
    top_books: List[str]  # overriden

    @property
    def as_dict(self) -> Json:  # overriden
        return {
            "name": self.name,
            "id": self.id,
            "stats": self.stats.as_dict,
            "renown": self.renown.name,
            "top_books": self.top_books,
        }

    @classmethod
    def from_dict(cls, data: Json) -> "SimpleAuthor":  # overriden
        return cls(
            data["name"],
            data["id"],
            AuthorStats.from_dict(data["stats"]),
            data["top_books"],
        )

    @property
    def total_editions(self) -> None:  # overriden
        return None


@dataclass
class MainEdition:
    publisher: str
    publication: datetime
    format: str
    pages: Optional[int]
    language: Optional[str]
    isbn: Optional[str]
    isbn13: Optional[str]
    asin: Optional[str]

    @property
    def as_dict(self) -> Dict[str, str | int]:
        data = {
            "publisher": self.publisher,
            "publication": self.publication.strftime(READABLE_TIMESTAMP_FORMAT),
            "format": self.format,
        }
        if self.pages is not None:
            data["pages"] = self.pages
        if self.language:
            data["language"] = self.language
        if self.isbn:
            data["isbn"] = self.isbn
        if self.isbn13:
            data["isbn13"] = self.isbn13
        if self.asin:
            data["asin"] = self.asin
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, str | int]) -> "MainEdition":
        return cls(
            data["publisher"],
            datetime.strptime(data["publication"], READABLE_TIMESTAMP_FORMAT),
            data["format"],
            data.get("pages"),
            data.get("language"),
            data.get("isbn"),
            data.get("isbn13"),
            data.get("asin"),
        )


@dataclass
class BookAward:
    name: str
    id: str
    date: Optional[datetime]
    category: Optional[str]
    designation: str

    @property
    def as_dict(self) -> Dict[str, str]:
        data = {
            "name": self.name,
            "id": self.id,
            "designation": self.designation,
        }
        if self.date is not None:
            data["date"] = self.date.strftime(READABLE_TIMESTAMP_FORMAT)
        if self.category:
            data["category"] = self.category

        return data

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "BookAward":
        return cls(
            data["name"],
            data["id"],
            datetime.strptime(
                data["date"], READABLE_TIMESTAMP_FORMAT) if data.get("date") else None,
            data.get("category"),
            data["designation"],
        )


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

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "BookSetting":
        return cls(
            data["name"],
            data["id"],
            data.get("country"),
            datetime.strptime(
                data["year"], READABLE_TIMESTAMP_FORMAT) if data.get("year") else None,
        )


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
        }
        if self.genres:
            data["genres"] = self.genres
        if self.awards:
            data["awards"] = [award.as_dict for award in self.awards]
        if self.places:
            data["places"] = [place.as_dict for place in self.places]
        if self.characters:
            data["characters"] = self.characters

        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BookDetails":
        return cls(
            data["description"],
            MainEdition.from_dict(data["main_edition"]),
            data.get("genres") or [],
            [BookAward.from_dict(award) for award in data["awards"]] if data.get("awards") else [],
            [BookSetting.from_dict(place) for place in data["places"]] if data.get(
                "places") else [],
            data.get("characters") or [],
        )


@dataclass
class _ScriptTagData:
    title: Optional[str]
    complete_title: str
    work_id: str
    ratings: FiveStars
    reviews: ReviewsDistribution
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

    @classmethod
    def from_dict(cls, data: Dict[str, str | Dict[float, str]]) -> "BookSeries":
        return cls(
            data["title"],
            data["id"],
            {float(k): v for k, v in data["layout"].items()} if data.get("layout") else {},
        )


@dataclass
class BookStats:
    ratings: FiveStars
    reviews: ReviewsDistribution
    total_reviews: int  # this is different from total calculated from 'reviews' dict
    shelves: OrderedDict[int, str]  # number of shelvings to shelves, only the first page is scraped
    editions: OrderedDict[str, List[str]]  # iso lang codes to editions' titles
    total_editions: int

    @property
    def avg_rating(self) -> float:
        return self.ratings.avg_rating

    @property
    def total_ratings(self) -> int:
        return self.ratings.total

    @property
    def renown(self) -> Renown:
        return Renown.calculate(self.ratings.total, HOBBIT_RATINGS)

    @property
    def r2r(self) -> float:
        return self.total_reviews / self.total_ratings if self.total_ratings else 0

    @property
    def r2r_percent(self) -> str:
        r2r = self.r2r * 100
        return f"{r2r:.2f} %"

    @property
    def total_shelvings(self) -> int:
        return sum(s for s in self.shelves)

    @property
    def sh2r(self) -> float:
        return self.total_shelvings / self.total_ratings if self.total_ratings else 0

    @property
    def sh2r_percent(self) -> str:
        sh2r = self.sh2r * 100
        return f"{sh2r:.2f} %"

    @property
    def e2r(self) -> float:
        return self.total_editions / self.total_ratings if self.total_ratings else 0

    @property
    def e2r_percent(self) -> str:
        e2r = self.e2r * 100
        return f"{e2r:.3f} %"

    @property
    def as_dict(self) -> Dict[str, Any]:
        return {
            "ratings": self.ratings.as_dict,
            "avg_rating": round(self.avg_rating, 4),
            "total_ratings": self.total_ratings,
            "renown": self.renown.name,
            "reviews": self.reviews.as_dict,
            "total_reviews": self.total_reviews,
            "reviews_to_ratings": self.r2r_percent,
            "shelves": self.shelves,
            "total_shelvings": self.total_shelvings,
            "shelvings_to_ratings": self.sh2r_percent,
            "editions": self.editions,
            "total_editions": self.total_editions,
            "editions_to_ratings": self.e2r_percent,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BookStats":
        return cls(
            FiveStars({int(k): v for k, v in data["ratings"].items()}),
            ReviewsDistribution(data["reviews"]),
            data["total_reviews"],
            OrderedDict(sorted([(int(k), v) for k, v in data["shelves"].items()], reverse=True)),
            OrderedDict(sorted((k, v) for k, v in data["editions"].items())),
            data["total_editions"],
        )


@dataclass
class DetailedBook:
    title: str
    complete_title: str
    book_id: str
    work_id: str
    authors: List[SimpleAuthor]
    first_publication: datetime
    series: Optional[BookSeries]
    details: BookDetails
    stats: BookStats

    @property
    def as_dict(self) -> Dict[str, Any]:
        data = {
            "title": self.title,
            "complete_title": self.complete_title,
            "book_id": self.book_id,
            "work_id": self.work_id,
            "authors": [author.as_dict for author in self.authors],
            "first_publication": self.first_publication.strftime(READABLE_TIMESTAMP_FORMAT),
            "details": self.details.as_dict,
            "stats": self.stats.as_dict,
        }
        if self.series:
            data["series"] = self.series.as_dict
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DetailedBook":
        return cls(
            data["title"],
            data["complete_title"],
            data["book_id"],
            data["work_id"],
            [SimpleAuthor.from_dict(author) for author in data["authors"]],
            datetime.strptime(
                data["first_publication"], READABLE_TIMESTAMP_FORMAT),
            BookSeries.from_dict(data["series"]) if data.get("series") else None,
            BookDetails.from_dict(data["details"]),
            BookStats.from_dict(data["stats"]),
        )
