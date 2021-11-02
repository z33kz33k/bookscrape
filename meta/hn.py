"""

    meta.hn.py
    ~~~~~~~~~~
    Scrape Hugo and Nebula awards meta data.

    @author: z33k

"""
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, NamedTuple, Union

import numpy as np
import pandas as pd

from utils import first_df_row_as_columns, getsoup

URL = "http://www.nicholaswhyte.info/sf/nh2.htm"
DEFAULT_JSON = Path("input") / "hugo_nebula.json"


def scrape(dump_json=False, dest: Optional[Path] = None) -> pd.DataFrame:
    """Scrape Hugo and Nebula winners data from ``URL`` ito a pandas dataframe.

    :param dump_json: flag for dumping data to JSON (default: do not dump)
    :param dest: optional destination for dumping the data as JSON
    """
    soup = getsoup(URL)
    table = soup.find("table")
    df = pd.read_html(str(table))[0]
    df = first_df_row_as_columns(df)
    # replace np.nan as the first column's label with 'authors'
    df.rename({np.nan: "author"}, axis=1, inplace=True)
    # improve other column names
    df.rename({"Hugo  awards": "hugo_awards"}, axis=1, inplace=True)
    df.rename({"Nebula  awards": "nebula_awards"}, axis=1, inplace=True)
    df.rename({"double wins": "double_wins"}, axis=1, inplace=True)
    # split first column into two
    df[["author", "life_span"]] = df["author"].str.rsplit(" ", 1, expand=True)
    # replace &nbsp; (non-breaking space) with regular space
    for col in df.columns:
        df[col] = df[col].str.replace("\xa0", " ")
    # replace the original source's non-ascii placeholder with the underline
    df["author"] = df["author"].str.replace("�", "_")
    # JSON dumping
    if dump_json:
        dest = dest if dest else DEFAULT_JSON
        with dest.open("w", encoding="utf8") as f:
            df.to_json(f, orient="records", force_ascii=False, indent=4)

    return df


class Category(Enum):
    """Enumeration of award categories.
    """
    SHORT_STORY = "Best Short Story"
    NOVELETTE = "Best Novelette"
    NOVELLA = "Best Novella"
    NOVEL = "Best Novel"
    # blanket Hugo category for SHORT_STORY, NOVELETTE and NOVELLA in yrs 1960-66
    SHORT_FICTION = "Best Short Fiction"

    @property
    def weight(self) -> int:
        """Return weight of the category.

        Based on:
        https://literature.stackexchange.com/questions/674/what-is-the-difference-between-a-novelette-novella-and-novel
        """
        if self is Category.SHORT_FICTION:
            return 1
        elif self is Category.NOVELETTE:
            return 3
        elif self is Category.NOVELLA:
            return 7
        elif self is Category.NOVEL:
            return 10
        elif self is Category.SHORT_FICTION:
            return 6


@dataclass(frozen=True)
class Work:
    """Awarded work data.
    """
    year: datetime
    category: Category
    title: str

    def __eq__(self, other: "Work") -> Union[bool, "NotImplemented"]:
        """Overload '==' operator.

        NOTE: this solution was based on:
        https://stackoverflow.com/questions/390250/elegant-ways-to-support-equivalence-equality-in-python-classes)
        """
        if isinstance(self, other.__class__):
            return self.title == other.title
        return NotImplemented


LifeSpan = NamedTuple("LifeSpan", [("birth", datetime), ("death", Optional[datetime])])


@dataclass(frozen=True)
class Author:
    """Awarded author data.
    """
    name: str
    hugos: List[Work]
    nebulas: List[Work]
    life_span: LifeSpan

    @property
    def double_wins(self) -> List[Work]:
        return [*set(self.hugos).intersection(set(self.nebulas))]


# def parse_json(dest: Optional[Path] = None) -> List[Author]:
#     dest = dest if dest else DEFAULT_JSON
#     with dest.open() as f:
#         data = json.load(f)

