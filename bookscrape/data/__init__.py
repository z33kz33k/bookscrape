"""

    bookscrape.data.py
    ~~~~~~~~~~~~~~~~~~~
    Project's data and data-related logic.

    @author: z33k

"""
from enum import Enum, auto

import pandas as pd

from bookscrape.utils import type_checker, is_increasing


@type_checker(pd.DataFrame)
def first_df_row_as_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make first row of ``df`` its columns.
    """
    return df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)


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
                  fractions=(3, 10, 30, 60, 100, 200, 400, 1000)) -> "Renown":
        if len(fractions) != len(Renown) - 1:
            raise ValueError(f"Fractions must have exactly {len(Renown) - 1} items, "
                             f"got: {len(fractions)}")
        if not is_increasing(fractions):
            raise ValueError(f"Fractions must be an increasing sequence, got: {fractions}")
        if ratings >= int(model_ratings * 1 / fractions[0]):
            return Renown.SUPERSTAR
        elif ratings in range(
                int(model_ratings * 1 / fractions[1]), int(model_ratings * 1 / fractions[0])):
            return Renown.STAR
        elif ratings in range(
                int(model_ratings * 1 / fractions[2]), int(model_ratings * 1 / fractions[1])):
            return Renown.FAMOUS
        elif ratings in range(
                int(model_ratings * 1 / fractions[3]), int(model_ratings * 1 / fractions[2])):
            return Renown.POPULAR
        elif ratings in range(
                int(model_ratings * 1 / fractions[4]), int(model_ratings * 1 / fractions[3])):
            return Renown.WELL_KNOWN
        elif ratings in range(
                int(model_ratings * 1 / fractions[5]), int(model_ratings * 1 / fractions[4])):
            return Renown.KNOWN
        elif ratings in range(
                int(model_ratings * 1 / fractions[6]), int(model_ratings * 1 / fractions[5])):
            return Renown.SOMEWHAT_KNOWN
        elif ratings in range(
                int(model_ratings * 1 / fractions[7]), int(model_ratings * 1 / fractions[6])):
            return Renown.LITTLE_KNOWN
        elif ratings in range(int(model_ratings * 1 / fractions[7])):
            return Renown.OBSCURE
        else:
            raise ValueError(f"Invalid ratings count: {ratings:,}")
