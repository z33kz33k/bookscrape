"""

    bookscrape.scrape.py
    ~~~~~~~~~~~~~~~~~~~~
    Common scraping-related logic.

    @author: z33k

"""
import time
from enum import Enum, auto

import requests
from bs4 import BeautifulSoup

from bookscrape.constants import DELAY, REQUEST_TIMOUT
from bookscrape.utils import is_increasing, timed, type_checker


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


@timed
@type_checker(str)
def getsoup(url: str) -> BeautifulSoup:
    """Return BeautifulSoup object based on ``url``.

    Args:
        url: URL string

    Returns:
        a BeautifulSoup object
    """
    print(f"Requesting: {url!r}")
    markup = requests.get(url, timeout=REQUEST_TIMOUT).text
    return BeautifulSoup(markup, "lxml")


def throttle(delay: float = DELAY) -> None:
    print(f"Throttling for {delay} seconds...")
    time.sleep(delay)
