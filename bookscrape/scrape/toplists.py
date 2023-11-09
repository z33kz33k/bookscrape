"""

    bookscrape.scrape.toplists.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape internet lists of top SF books for book (title, author) data.

    @author: z33k

"""
from typing import List

from bs4 import Tag

from bookscrape.constants import BookRecord
from bookscrape.scrape import getsoup

LISTS = [
    "http://scifilists.sffjazz.com/lists_books_rank1.html",
    "http://scifilists.sffjazz.com/lists_books_rank2.html",
    "http://scifilists.sffjazz.com/lists_books_rank3.html",
    "https://bestwriting.com/best-books/sci-fi",
    "https://blamcast.net/articles/best-science-fiction-books/",
    "https://bookriot.com/best-science-fiction-books-of-all-time/",
    "https://booksofbrilliance.com/2023/08/26/the-25-best-science-fiction-books-of-all-time/",
    "https://reedsy.com/discovery/blog/best-sci-fi-books",
    "https://stacker.com/art-culture/100-best-science-fiction-novels-all-time",
    "https://theplanets.org/100-best-science-fiction-books/",
    "https://vsbattle.com/battle/110304-what-is-the-greatest-science-fiction-novel-of-all-time",
    "https://www.esquire.com/entertainment/books/g39358054/best-sci-fi-books/",
    "https://www.expertreviews.co.uk/books/1418066/best-science-fiction-books",
    "https://www.forbes.com/sites/paultassi/2019/07/31/the-best-science-fiction-books-of-all-time",
    "https://www.goodhousekeeping.com/life/entertainment/g41777590/best-sci-fi-books/",
    "https://www.goodreads.com/blog/show/1874-the-100-most-popular-sci-fi-books-on-goodreads",
    "https://www.goodreads.com/list/show/114163",
    "https://www.goodreads.com/list/show/19341.Best_Science_Fiction",
    "https://www.goodreads.com/list/show/35776",
    "https://www.goodreads.com/list/show/72370._r_PrintSF_Recommends_Science_Fiction_Novels",
    "https://www.nerdmuch.com/books/best-sci-fi-books/",
    "https://www.npr.org/2011/08/11/139085843/your-picks-top-100-science-fiction-fantasy-books",
    "https://www.penguinrandomhouse.com/the-read-down/best-sci-fi-books/",
    "https://www.shortform.com/best-books/genre/best-classic-sci-fi-books-of-all-time",
    "https://www.wired.co.uk/article/best-sci-fi-books",
]


class SffJazzScraper:
    HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
                  "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Host": "scifilists.sffjazz.com",
        "If-Modified-Since": "Sat, 28 Jan 2023 00:11:29 GMT",
        "If-None-Match": "\"1b80d-5f347d2e22021-gzip\"",
        "Referer": "http://scifilists.sffjazz.com/lists_books_rank2.html",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/118.0.0.0 Safari/537.36"
    }

    @staticmethod
    def _parse_row(row: Tag) -> BookRecord | None:
        colums = row.find_all("td")[1:3]
        if len(colums) != 2:
            return None
        author_col, title_col = colums
        author = author_col.text.strip()
        title = title_col.find("a").text
        if "[" in title:
            title, *_ = title.split("[")
            title = title.strip()
        return BookRecord(title, author)

    @classmethod
    def _scrape_list(cls, list_num: int) -> List[BookRecord]:
        url = LISTS[list_num]
        soup = getsoup(url, headers=cls.HEADERS)
        table = soup.find("table", {"cellpadding": "1"})
        rows = table.find_all("tr")
        records = []
        for row in rows:
            record = cls._parse_row(row)
            if record:
                records.append(record)
        return records

    @classmethod
    def scrape_pre_2000_first(cls) -> List[BookRecord]:
        return cls._scrape_list(0)

    @classmethod
    def scrape_pre_2000_second(cls) -> List[BookRecord]:
        return cls._scrape_list(1)

    @classmethod
    def scrape_post_2000(cls) -> List[BookRecord]:
        return cls._scrape_list(2)

