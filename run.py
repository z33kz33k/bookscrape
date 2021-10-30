"""

    run.py
    ~~~~~~
    Run the code.

    @author: z33k

"""
import time
from datetime import datetime
from pathlib import Path
from pprint import pprint
import json

from requests import Timeout

from goodreads import AuthorParser
from constants import DELAY, DANGEROUS_VISIONS_AUTHORS, TIMESTAMP_FORMAT


data = {}
for i, author in enumerate(DANGEROUS_VISIONS_AUTHORS, start=1):
    print(f"Scraping author #{i}: {' '.join(author)!r}...")
    *names, surname = author
    parser = AuthorParser(surname, *names)
    try:
        parser.get_stats_and_books()
    except Timeout:
        print("Goodreads doesn't play nice. Timeout exceeded. Exiting.")
        break
    data.update({
        " ".join(parser.allnames): {
            "stats": parser.stats.as_dict,
            "books": [b.as_dict for b in parser.books]
        }
    })

    print(f"Throttling for {DELAY} seconds...")
    time.sleep(DELAY)
    print()

pprint(data)

dest = Path("output") / f"dump_{datetime.now().strftime(TIMESTAMP_FORMAT)}.json"
with dest.open("w", encoding="utf8") as f:
    json.dump(data, f, indent=4)

