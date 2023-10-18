"""

    bookscrape.constants.py
    ~~~~~~~~~~~~~~~~~~~~~~~
    Script's constants

    @author: z33k

"""
from pathlib import Path
from typing import Any, Dict, TypeVar

Json = Dict[str, Any]
DELAY = 1.1  # seconds
REQUEST_TIMOUT = 5  # seconds
FILNAME_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
READABLE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
T = TypeVar("T")

OUTPUT_DIR = Path("temp") / "output"
if not OUTPUT_DIR.is_dir():
    print(f"Creating missing output directory at: '{OUTPUT_DIR.resolve()}'")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
