"""

    bookscrape.utils.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Project's utilities.

    @author: z33k

"""
import logging
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol, Sequence

import langcodes
import pandas as pd
from contexttimer import Timer
from langcodes import Language, tag_is_valid

from bookscrape.constants import PathLike, T, SECONDS_IN_YEAR
from bookscrape.utils.check_type import type_checker


_log = logging.getLogger(__name__)


def timed(operation="", precision=3) -> Callable:
    """Add time measurement to the decorated operation.

    Args:
        operation: optionally, name of the time-measured operation
        precision: precision of the time measurement in seconds

    Returns:
        the decorated function
    """
    if precision < 0:
        precision = 0

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with Timer() as t:
                result = func(*args, **kwargs)
            _log.info(f"Completed {operation} in {t.elapsed:.{precision}f} seconds")
            return result
        return wrapper
    return decorator


@type_checker(pd.DataFrame)
def first_df_row_as_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make first row of ``df`` its columns.
    """
    return df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)


@type_checker(str)
def extract_float(text: str) -> float:
    """Extract floating point number from text.
    """
    text = "".join([char for char in text if char.isdigit() or char in ",."])
    return float(text.replace(",", "."))


@type_checker(str)
def extract_int(text: str) -> int:
    """Extract an integer text.
    """
    text = "".join([char for char in text if char.isdigit()])
    return int(text)


def from_iterable(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Optional[T]:
    """Return item from ``iterable`` based on ``predicate`` or ``None``, if it cannot be found.
    """
    return next((item for item in iterable if predicate(item)), None)


@type_checker(PathLike)
def getdir(path: PathLike, create_missing=True) -> Path:
    """Return a directory at ``path`` creating it (and all its needed parents) if missing.
    """
    dir_ = Path(path)
    if not dir_.exists() and create_missing:
        _log.warning(f"Creating missing directory at: '{dir_.resolve()}'...")
        dir_.mkdir(parents=True, exist_ok=True)
    else:
        if dir_.is_file():
            raise NotADirectoryError(f"Not a directory: '{dir_.resolve()}'")
    return dir_


@type_checker(PathLike)
def getfile(path: PathLike, ext="") -> Path:
    """Return an existing file at ``path``.
    """
    f = Path(path)
    if not f.is_file():
        raise FileNotFoundError(f"Not a file: '{f.resolve()}'")
    if ext and not f.suffix.lower() == ext.lower():
        raise ValueError(f"Not a {ext!r} file")
    return f


class Comparable(Protocol):
    """Protocol for annotating comparable types.
    """
    def __lt__(self, other) -> bool:
        ...


def is_increasing(seq: Sequence[Comparable]) -> bool:
    if len(seq) < 2:
        return False
    return all(seq[i] > seq[i-1] for i, _ in enumerate(seq, start=1) if i < len(seq))


@type_checker(str)
def langcode2name(langcode: str) -> str | None:
    """Convert ``langcode`` to language name or `None` if it cannot be converted.
    """
    if not tag_is_valid(langcode):
        return None
    lang = Language.get(langcode)
    return lang.display_name()


@type_checker(str)
def name2langcode(langname: str, alpha3=False) -> str | None:
    """Convert supplied language name to a 2-letter ISO language code or `None` if it cannot be
    converted. Optionally, convert it to 3-letter ISO code (aka "alpha3").
    """
    try:
        lang = langcodes.find(langname)
    except LookupError:
        return None
    if alpha3:
        return lang.to_alpha3()
    return str(lang)


def init_log() -> None:
    """Initialize logging.
    """
    output_dir = Path(__file__).parent.parent.parent / "temp" / "logs"
    if output_dir.exists():
        logfile = output_dir / "bookscrape.log"
    else:
        logfile = "bookscrape.log"

    log_format = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    log_level = logging.INFO

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    formatter = logging.Formatter(log_format)
    handler = RotatingFileHandler(logfile, maxBytes=1024*1024*10, backupCount=10)
    handler.setFormatter(formatter)
    handler.setLevel(log_level)
    root_logger.addHandler(handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(log_level)
    root_logger.addHandler(stream_handler)


@type_checker(datetime, datetime)
def timedelta2years(start: datetime, stop: datetime) -> float:
    delta = stop - start
    return delta.total_seconds() / SECONDS_IN_YEAR

