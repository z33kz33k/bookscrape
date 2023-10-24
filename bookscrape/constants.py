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

REQUEST_TIMOUT = 5  # seconds
FILNAME_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
READABLE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

OUTPUT_DIR = Path("temp") / "output"
if not OUTPUT_DIR.is_dir():
    print(f"Creating missing output directory at: '{OUTPUT_DIR.resolve()}'")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DANGEROUS_VISIONS_AUTHORS = [
    "Lester del Rey",
    "Robert Silverberg",
    "Frederik Pohl",
    "Philip Jose Farmer",
    "Miriam Allen deFord",
    "Robert Bloch",
    "Harlan Ellison",
    "Brian W. Aldiss",
    "Howard Rodman",
    "Philip K. Dick",
    "Larry Niven",
    "Fritz Leiber",
    "Joe L. Hensley",
    "Poul Anderson",
    "David R. Bunch",
    "Hugh J. Parry",
    "Carol Emshwiller",
    "Damon Knight",
    "Theodore Sturgeon",
    "Larry Eisenberg",
    "Henry Slesar",
    "Sonya Dorman",
    "John Sladek",
    "Jonathan Brand",
    "Kris Neville",
    "R.A. Lafferty",
    "J.G. Ballard",
    "John Brunner",
    "Keith Laumer",
    "Norman Spinrad",
    "Roger Zelazny",
    "Samuel R. Delany",
]

BORKED = [
    "Daniel Keyes",
    "44037.Vernor_Vinge",  # only with ID,
    "Karin Tidbeck",
    "Mary Wollstonecraft Shelley",
]
