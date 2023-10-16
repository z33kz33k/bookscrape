"""

    bookscrape.constants.py
    ~~~~~~~~~~~~~~~~~~~~~~~
    Script's constants

    @author: z33k

"""
from typing import Any, Dict, TypeVar

Json = Dict[str, Any]
DELAY = 1.1  # seconds
REQUEST_TIMOUT = 5  # seconds
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
T = TypeVar("T")
