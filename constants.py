"""

    constants.py
    ~~~~~~~~~~~~
    Script's constants.

    @author: z33k

"""
from enum import Enum
from typing import Any, Dict

Json = Dict[str, Any]
DELAY = 1.1  # seconds
REQUEST_TIMOUT = 5  # seconds
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"


DANGEROUS_VISIONS_AUTHORS = [
    ("Lester", "del", "Rey"),
    ("Robert", "Silverberg"),
    ("Frederik", "Pohl"),
    ("Philip", "Jose", "Farmer"),
    ("Miriam", "Allen", "deFord"),
    ("Robert", "Bloch"),
    ("Harlan", "Ellison"),
    ("Brian", "W.", "Aldiss"),
    ("Howard", "Rodman"),
    ("Philip", "K.", "Dick"),
    ("Larry", "Niven"),
    ("Fritz", "Leiber"),
    ("Joe", "L.", "Hensley"),
    ("Poul", "Anderson"),
    ("David", "R.", "Bunch"),
    ("Hugh", "J.", "Parry"),
    ("Carol", "Emshwiller"),
    ("Damon", "Knight"),
    ("Theodore", "Sturgeon"),
    ("Larry", "Eisenberg"),
    ("Henry", "Slesar"),
    ("Sonya", "Dorman"),
    ("John", "Sladek"),
    ("Jonathan", "Brand"),
    ("Kris", "Neville"),
    ("R.", "A.", "Lafferty"),
    ("J.", "G.", "Ballard"),
    ("John", "Brunner"),
    ("Keith", "Laumer"),
    ("Norman", "Spinrad"),
    ("Roger", "Zelazny"),
    ("Samuel", "R.", "Delany"),
]

OTHER_AUTHORS = [
    ("Isaac", "Asimov"),
    ("Frank", "Herbert"),
    ("Jacek", "Dukaj"),
    ("Andrzej", "Sapkowski"),
    ("J.", "R.", "R.", "Tolkien"),
    ("C.", "S.", "Lewis"),
    ("Cordwainer", "Smith"),
    ("Michael", "Moorcock"),
    ("Clifford", "D.", "Simak"),
    ("George", "R.", "R.", "Martin"),
    ("Joe", "Abercrombie"),
    ("Ursula", "K.", "Le", "Guin"),
]


TOLKIEN_RATINGS_COUNT = 9_323_827


class Renown(Enum):
    SUPERSTAR = int(TOLKIEN_RATINGS_COUNT / 3)
    STAR = range(int(TOLKIEN_RATINGS_COUNT / 10), int(TOLKIEN_RATINGS_COUNT / 3))
    FAMOUS = range(int(TOLKIEN_RATINGS_COUNT / 30), int(TOLKIEN_RATINGS_COUNT / 10))
    POPULAR = range(int(TOLKIEN_RATINGS_COUNT / 60), int(TOLKIEN_RATINGS_COUNT / 30))
    WELL_KNOWN = range(int(TOLKIEN_RATINGS_COUNT / 100), int(TOLKIEN_RATINGS_COUNT / 60))
    KNOWN = range(int(TOLKIEN_RATINGS_COUNT / 400), int(TOLKIEN_RATINGS_COUNT / 100))
    SOMEWHAT_KNOWN = range(int(TOLKIEN_RATINGS_COUNT / 200), int(TOLKIEN_RATINGS_COUNT / 400))
    LITTLE_KNOWN = range(int(TOLKIEN_RATINGS_COUNT / 1000), int(TOLKIEN_RATINGS_COUNT / 200))
    OBSCURE = range(int(TOLKIEN_RATINGS_COUNT / 1000))

    @property
    def priority(self) -> int:
        if self is Renown.SUPERSTAR:
            return 8
        elif self is Renown.STAR:
            return 7
        elif self is Renown.FAMOUS:
            return 6
        elif self is Renown.POPULAR:
            return 5
        elif self is Renown.WELL_KNOWN:
            return 4
        elif self is Renown.KNOWN:
            return 3
        elif self is Renown.SOMEWHAT_KNOWN:
            return 2
        elif self is Renown.LITTLE_KNOWN:
            return 1
        elif self is Renown.OBSCURE:
            return 0


