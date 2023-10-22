"""

    bookscrape.constants.py
    ~~~~~~~~~~~~~~~~~~~~~~~
    Script's constants

    @author: z33k

"""
from pathlib import Path
from typing import Any, Callable, Dict, Tuple, TypeVar

# type hints
T = TypeVar("T")
Json = Dict[str, Any]
PathLike = str | Path
Method = Callable[[Any, Tuple[Any, ...]], Any]  # method with signature def methodname(self, *args)
Function = Callable[[Tuple[Any, ...]], Any]  # function with signature def funcname(*args)

DELAY = 1.1  # seconds
REQUEST_TIMOUT = 5  # seconds
FILNAME_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
READABLE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

OUTPUT_DIR = Path("temp") / "output"
if not OUTPUT_DIR.is_dir():
    print(f"Creating missing output directory at: '{OUTPUT_DIR.resolve()}'")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
