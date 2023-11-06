"""

    bookscrape.scrape.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Common scraping-related logic.

    @author: z33k

"""
import logging
import time
from collections import OrderedDict
from enum import Enum, auto
from functools import wraps
from typing import Callable, Dict, Iterable, Tuple

import requests
from bs4 import BeautifulSoup
from langcodes import tag_is_valid

from bookscrape.constants import REQUEST_TIMOUT
from bookscrape.utils import is_increasing, timed, type_checker, langcode2name, name2langcode


_log = logging.getLogger(__name__)


class ParsingError(ValueError):
    """Raised whenever parser's assumptions are not met.
    """


class Renown(Enum):
    SUPERSTAR = auto()
    STAR = auto()
    FAMOUS = auto()
    POPULAR = auto()
    WELL_KNOWN = auto()
    KNOWN = auto()
    SOMEWHAT_KNOWN = auto()
    LITTLE_KNOWN = auto()
    OBSCURE = auto()

    @staticmethod
    def calculate(ratings: int, model_ratings: int,
                  fractions=(3, 11, 29, 66, 141, 291, 591, 1191)) -> "Renown":
        # fraction differences: 3, 8, 18, 37, 75, 150, 300, 600
        if len(fractions) != len(Renown) - 1:
            raise ValueError(f"Fractions must have exactly {len(Renown) - 1} items, "
                             f"got: {len(fractions)}")
        if not is_increasing(fractions):
            raise ValueError(f"Fractions must be an increasing sequence, got: {fractions}")
        if ratings >= int(model_ratings * 1 / fractions[0]):
            return Renown.SUPERSTAR
        elif int(model_ratings * 1 / fractions[1]) <= ratings < int(
                model_ratings * 1 / fractions[0]):
            return Renown.STAR
        elif int(model_ratings * 1 / fractions[2]) <= ratings < int(
                model_ratings * 1 / fractions[1]):
            return Renown.FAMOUS
        elif int(model_ratings * 1 / fractions[3]) <= ratings < int(
                model_ratings * 1 / fractions[2]):
            return Renown.POPULAR
        elif int(model_ratings * 1 / fractions[4]) <= ratings < int(
                model_ratings * 1 / fractions[3]):
            return Renown.WELL_KNOWN
        elif int(model_ratings * 1 / fractions[5]) <= ratings < int(
                model_ratings * 1 / fractions[4]):
            return Renown.KNOWN
        elif int(model_ratings * 1 / fractions[6]) <= ratings < int(
                model_ratings * 1 / fractions[5]):
            return Renown.SOMEWHAT_KNOWN
        elif int(model_ratings * 1 / fractions[7]) <= ratings < int(
                model_ratings * 1 / fractions[6]):
            return Renown.LITTLE_KNOWN
        elif 0 <= ratings < int(model_ratings * 1 / fractions[7]):
            return Renown.OBSCURE
        else:
            raise ValueError(f"Invalid ratings count: {ratings:,}")


@timed("request")
@type_checker(str)
def getsoup(url: str) -> BeautifulSoup:
    """Return BeautifulSoup object based on ``url``.

    Args:
        url: URL string

    Returns:
        a BeautifulSoup object
    """
    _log.info(f"Requesting: {url!r}")
    markup = requests.get(url, timeout=REQUEST_TIMOUT).text
    return BeautifulSoup(markup, "lxml")


def throttle(delay: float) -> None:
    _log.info(f"Throttling for {delay} seconds...")
    time.sleep(delay)


def throttled(delay: float) -> Callable:
    """Add throttling delay after the decorated operation.

    Args:
        throttling delay in fraction of seconds

    Returns:
        the decorated function
    """
    def decorate(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            throttle(delay)
            return result
        return wrapper
    return decorate


class RatingsDistribution:
    """Ratings distribution that rescales itself to any given rank scheme.
    """
    @property
    def dist(self) -> OrderedDict[int | float, int]:
        return self._dist

    @property
    def rank_scheme(self) -> Tuple[int | float, ...]:
        return self._rank_scheme

    @property
    def total(self) -> int:
        return sum(votes for _, votes in self.dist.items())

    @property
    def avg_rating(self) -> float:
        return sum(rank * votes for rank, votes in self.dist.items()) / self.total

    @property
    def scaled_dist(self) -> OrderedDict[int | float, int]:
        if self.rank_scheme == tuple(sorted(self.dist)):
            return self.dist

        pairs, max_rank = [], max(self.rank_scheme)
        for i, rank in enumerate(self.rank_scheme):
            if i == 0:
                ratings = self._span_ratings(0, round(rank / max_rank, 3))
                pairs.append((rank, ratings))
            elif i == len(self.rank_scheme) - 1:
                previous_rank = self.rank_scheme[i - 1]
                ratings = self._span_ratings(round(previous_rank / max_rank, 3), 1)
                pairs.append((rank, ratings))
            else:
                previous_rank = self.rank_scheme[i - 1]
                min_ = round(previous_rank / max_rank, 3)
                max_ = round(rank / max_rank, 3)
                pair = rank, self._span_ratings(min_, max_)
                pairs.append(pair)
        return OrderedDict(pairs)

    def __init__(self, distribution: Dict[int | float, int],
                 rank_scheme: Iterable[int | float] = ()) -> None:
        """Initialize.

        If no rank scheme is supplied the keys of supplied distribution dict are assumed as
        such and no effective scaling is perfomed.

        Args:
            distribution: a mapping of (at least three) non-negative rating ranks to number of votes for them
            rank_scheme: iterable of (at least three) non-negative numbers to rescale supplied distribution to
        """
        if not rank_scheme:
            rank_scheme = [*distribution]
        self._dist = OrderedDict(sorted([(r, v) for r, v in distribution.items()]))
        self._rank_scheme = tuple(sorted(set(rank_scheme)))
        if any(rank < 0 for rank in distribution) or len(distribution) < 3:
            raise ValueError("Ratings distribution must be a mapping of at least three "
                             "non-negative rating ranks to number of votes fot them, got: "
                             f"{distribution}")
        if any(rank < 0 for rank in self.rank_scheme) or len(self.rank_scheme) < 3:
            raise ValueError("Rank scheme must be an iterable of at least three non-negative "
                             f"numbers, got: {rank_scheme}")
        self._normalized = [(rank / max(self.dist), votes) for rank, votes in self.dist.items()]

    def _span_ratings(self, min_: float, max_: float) -> int:
        return sum(votes for rank, votes in self._normalized if min_ < rank <= max_)

    def ratings(self, rank: int | float) -> int:
        if rank not in self.rank_scheme:
            raise ValueError(f"Rank must be defined in the rank scheme: '{self.rank_scheme}'")
        return self.scaled_dist[rank]

    def ratings_percent(self, rank: int | float) -> str:
        if rank not in self.rank_scheme:
            raise ValueError(f"Rank must be defined in the rank scheme: '{self.rank_scheme}'")
        percent = self.ratings(rank) * 100 / self.total
        return f"{percent:.2f} %"

    def __repr__(self) -> str:
        return repr(self.scaled_dist).replace("OrderedDict", self.__class__.__name__)

    def as_dict(self) -> Dict[str, OrderedDict[int | float, int] | Tuple[int | float, ...]]:
        return {
            "dist": self.dist,
            "rank_scheme": self.rank_scheme,
        }


class FiveStars(RatingsDistribution):
    """A rating distribution with pre-defined (1, 2, 3, 4, 5) rank scheme and some convenience
    properties.
    """
    def __init__(self, distribution: Dict[int | float, int]) -> None:
        super().__init__(distribution=distribution, rank_scheme=(1, 2, 3, 4, 5))

    @property
    def one_star_ratings(self) -> int:
        return self.ratings(1)

    @property
    def one_star_percent(self) -> str:
        return self.ratings_percent(1)

    @property
    def two_stars_ratings(self) -> int:
        return self.ratings(2)

    @property
    def two_star_percent(self) -> str:
        return self.ratings_percent(2)

    @property
    def three_stars_ratings(self) -> int:
        return self.ratings(3)

    @property
    def three_star_percent(self) -> str:
        return self.ratings_percent(3)

    @property
    def four_stars_ratings(self) -> int:
        return self.ratings(4)

    @property
    def four_star_percent(self) -> str:
        return self.ratings_percent(4)

    @property
    def five_stars_ratings(self) -> int:
        return self.ratings(5)

    @property
    def five_star_percent(self) -> str:
        return self.ratings_percent(5)

    @property
    def as_dict(self) -> OrderedDict[int | float, int]:
        return self.scaled_dist


class ReviewsDistribution:
    """Language based reviews distribution.
    """
    @property
    def dist(self) -> OrderedDict[str, int]:
        return self._dist

    @property
    def dist_by_reviews(self) -> OrderedDict[str, int]:
        return OrderedDict(sorted([(lang, r) for lang, r in self.dist.items()],
                                  key=lambda pair: pair[1], reverse=True))

    @property
    def langnames_dist(self) -> OrderedDict[str, int]:
        return OrderedDict(sorted([(langcode2name(lang), r) for lang, r in self.dist.items()]))

    @property
    def alpha3_dist(self) -> OrderedDict[str, int]:
        return OrderedDict(sorted([(name2langcode(langcode2name(lang), alpha3=True), r)
                                   for lang, r in self.dist.items()]))

    @property
    def total(self) -> int:
        return sum(reviews for _, reviews in self.dist.items())

    def __init__(self, distribution: Dict[str, int]) -> None:
        """Initialize.

        Args:
            distribution: a mapping of 2-letter ISO language codes to number of reviews written in that language
        """
        self._dist = OrderedDict(
            sorted([(lang, r) for lang, r in distribution.items() if tag_is_valid(lang)]))

    def reviews(self, lang: str) -> int | None:
        """Return number of reviews for the language specified or `None`.

        Args:
            lang: either a language code or language name
        """
        reviews = self.dist.get(lang)
        if reviews is None:
            reviews = self.langnames_dist.get(lang)
            if reviews is None:
                reviews = self.alpha3_dist.get(lang)
                if reviews is None:
                    return None
        return reviews

    def reviews_percent(self, lang: str) -> str:
        reviews = self.reviews(lang)
        if reviews is None:
            raise ValueError(f"Unrecognized language: {lang!r}")
        percent = reviews * 100 / self.total
        return f"{percent:.2f} %"

    def __repr__(self) -> str:
        return repr(self.dist).replace("OrderedDict", self.__class__.__name__)

    @property
    def as_dict(self) -> OrderedDict[str, int]:
        return self.dist
