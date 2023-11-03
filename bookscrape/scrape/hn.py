"""

    bookscrape.hn.py
    ~~~~~~~~~~~~~~~~
    Scrape Hugo and Nebula awards data

    @author: z33k

"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
import re
from typing import List, Optional, NamedTuple, Tuple, Union

import numpy as np
import pandas as pd

from bookscrape.constants import Json
from bookscrape.utils import first_df_row_as_columns, getfile
from bookscrape.scrape import getsoup

_log = logging.getLogger(__name__)
URL = "http://www.nicholaswhyte.info/sf/nh2.htm"
DEFAULT_JSON = getfile(Path(__file__).parent.parent / "data" / "hugo_nebula.json")


def scrape(dump_json=False, dest: Optional[Path] = None) -> pd.DataFrame:
    """Scrape Hugo and Nebula winners data from ``URL`` into a pandas dataframe.

    Args:
        dump_json: flag for dumping data to JSON (default: do not dump)
        dest: optional destination for dumping the data as JSON
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
    for col in df.columns[:4]:
        df[col] = df[col].str.replace("�", "_")
    # remove trash strings
    trash = (" [review]", " [see review]", " [discussion]", " (tie)", " (declined)")
    for col in df.columns[1:4]:
        for t in trash:
            df[col] = df[col].str.replace(t, "", regex=False)
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
    NOVEL_OR_NOVELETTE = "Best Novel or Best Novelette"
    # blanket Hugo category for SHORT_STORY, NOVELETTE and NOVELLA in yrs 1960-66
    SHORT_FICTION = "Best Short Fiction"

    @property
    def weight(self) -> int:
        """Return weight of the category.

        Based on:
        https://literature.stackexchange.com/questions/674/what-is-the-difference-between-a-novelette-novella-and-novel
        """
        if self is Category.SHORT_STORY:
            return 1
        elif self is Category.NOVELETTE:
            return 3
        elif self is Category.NOVELLA:
            return 7
        elif self is Category.NOVEL:
            return 10
        elif self is Category.NOVEL_OR_NOVELETTE:
            return 6
        elif self is Category.SHORT_FICTION:
            return 4


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
            return self.title.lower() == other.title.lower()
        return NotImplemented

    def __hash__(self) -> int:
        """Make this object hashable
        """
        return hash(self.title.lower())


Lifespan = NamedTuple("Lifespan", [("birth", Optional[datetime]), ("death", Optional[datetime])])


@dataclass(frozen=True)
class Author:
    """Awarded author data.
    """
    name: str
    hugos: List[Work]
    nebulas: List[Work]
    lifespan: Lifespan

    @property
    def double_wins(self) -> List[Work]:
        return [*set(self.hugos).intersection(set(self.nebulas))]

    @property
    def awards(self) -> List[Work]:
        return [*{work for lst in (self.hugos, self.nebulas) for work in lst}]

    @property
    def rank(self) -> int:
        hugorank = round(sum(work.category.weight for work in self.hugos) * 0.4, 2)
        nebrank = round(sum(work.category.weight for work in self.nebulas) * 0.6, 2)
        return round((hugorank + nebrank) * 10)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, rank={self.rank}, hugos=" \
               f"{len(self.hugos)}, nebulas={len(self.nebulas)}, doubles={len(self.double_wins)}," \
               f" lifespan=({self.lifespan.birth.year if self.lifespan.birth else '?'}-" \
               f"{self.lifespan.death.year if self.lifespan.death else ''}))"


class Parser:
    """Parse data scraped from ``URL``.
    """
    def __init__(self, data: Optional[pd.DataFrame] = None) -> None:
        if not data:
            with DEFAULT_JSON.open() as f:
                data = json.load(f)
            self.author_names, self.hugo_data, self.nebula_data, self.double_wins_data, \
                self.life_spans = self._parse_json(data)
        else:
            if isinstance(data, pd.DataFrame):
                self.author_names = [*data.author]
                self.hugo_data = [*data.hugo_awards]
                self.nebula_data = [*data.nebula_awards]
                self.double_wins_data = [*data.double_wins]
                self.life_spans = [*data.life_span]
            elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
                self.author_names, self.hugo_data, self.nebula_data, self.double_wins_data, \
                    self.life_spans = self._parse_json(data)
            else:
                TypeError(f"Invalid input data type: {type(data)!r}.")
        self.authors: List[Author] = self._getauthors()

    @staticmethod
    def _parse_json(
            json_data: List[Json]) -> Tuple[List[str], List[str], List[str], List[str], List[str]]:
        author_names = [item["author"] for item in json_data]
        hugo_data = [item["hugo_awards"] for item in json_data]
        nebula_data = [item["nebula_awards"] for item in json_data]
        double_wins_data = [item["double_wins"] for item in json_data]
        life_spans = [item["life_span"] for item in json_data]
        return author_names, hugo_data, nebula_data, double_wins_data, life_spans

    def _parse_hn_data(self, input_text: str) -> List[Work]:
        count, *rest = input_text.split()
        if "*" in count:
            count = count.replace("*", "")
        count = int(count)
        if not rest:
            if count != 0:
                raise ValueError(f"Invalid data: {input_text!r}.")
            return []

        text, regex = " ".join(rest), r"\d\d\d\d,\s"
        if "*" in text:
            text = text.replace("*", "")

        cat_title_parts = [part.strip() for part in re.split(regex, text) if part]
        # expected result:
        # ['Best Novelette, Fire Watch',
        # 'Best Novella, The Last of the Winnebagos',
        # 'Best Novel, Doomsday Book (tie)',
        # 'Best Short Story, Even the Queen',
        # 'Best Short Story, Death on the Nile',
        # 'Best Short Story, The Soul Selects Her Own  Society ...',
        # 'Best Novel, To Say Nothing of the Dog',
        # 'Best Novelette, The Winds of Marble Arch',
        # 'Best Novella, Inside Job',
        # 'Best Novella, All Seated on the Ground',
        # 'Best Novel, Blackout  \\/ All Clear']

        years_parts = [part.strip(", ") for part in re.findall(regex, text)]
        # expected result:
        # ['1983',
        #  '1989',
        #  '1993',
        #  '1993',
        #  '1994',
        #  '1997',
        #  '1999',
        #  '2000',
        #  '2006',
        #  '2008',
        #  '2011']

        if len(cat_title_parts) != len(years_parts) or count != len(years_parts):
            raise ValueError(f"Invalid input data: {input_text!r}")

        cat_parts, title_parts = [], []
        for part in cat_title_parts:
            cat, *rest = part.split(", ")
            if not rest:
                cat, *rest = part.split(",")
            cat_parts.append(cat)
            title_parts.append(", ".join(rest))

        works = []
        for year, cat, title in zip(years_parts, cat_parts, title_parts):
            year = datetime(int(year), 1, 1)
            cat = Category(cat)
            title = self._process_title(title)
            works.append(Work(year, cat, title))

        return works

    @classmethod
    def _process_title(cls, title: str) -> str:
        mangled_dash = r"\/"
        if mangled_dash in title:
            title = title.replace(mangled_dash, "/")
        title = re.sub(r"\s\(with[\w|\s]+\)", "", title)
        title = re.sub(r"\s\(as[\w|\s]+\)", "", title)
        return title

    def _getauthors(self) -> List[Author]:
        authors = []
        for name, hugo, nebula, dw, ls in zip(self.author_names, self.hugo_data, self.nebula_data,
                                              self.double_wins_data, self.life_spans):
            hugos = self._parse_hn_data(hugo)
            nebulas = self._parse_hn_data(nebula)
            dw_count, *_ = dw.split()
            if "*" in dw_count:
                dw_count = dw_count.replace("*", "")
            dw_count = int(dw_count)
            ls = ls[1:-1]
            birth, death = ls.split("-")
            birth = datetime(int(birth), 1, 1) if birth != "?" else None
            death = datetime(int(death), 1, 1) if death else None
            lifespan = Lifespan(birth, death)
            author = Author(name.strip(), hugos, nebulas, lifespan)
            if len(author.double_wins) != dw_count:
                _log.warning(f"Inconsistent double wins data: {len(author.double_wins)}"
                             f"!={dw_count}")
            authors.append(author)

        return sorted(authors, key=lambda a: (a.rank, len(a.double_wins), len(a.nebulas),
                                              len(a.hugos)), reverse=True)

    def dump_reprs(self, dest: Optional[Path] = None) -> None:
        dest = dest if dest else Path("output") / "hugo_nebula.txt"
        text = "\n".join(f"{('#'+str(i)).rjust(4, ' ')}: {repr(author)}" for i, author
                         in enumerate(self.authors, start=1))
        dest.write_text(text, encoding="utf8")
