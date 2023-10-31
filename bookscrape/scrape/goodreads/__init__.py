"""

    bookscrape.goodreads.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape and parse Goodreads data

    @author: z33k

"""
import itertools
import json
from collections import OrderedDict, defaultdict, namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple

import backoff
import pytz
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests import Timeout

from bookscrape.constants import (OUTPUT_DIR, PathLike, READABLE_TIMESTAMP_FORMAT,
                                  FILNAME_TIMESTAMP_FORMAT)
from bookscrape.scrape.goodreads.utils import is_goodreads_id, numeric_id
from bookscrape.utils import getdir, getfile, extract_int, extract_float, from_iterable, \
    name2langcode
from bookscrape.scrape import ParsingError, getsoup, throttled, FiveStars, LangReviewsDistribution
from bookscrape.scrape.goodreads.data import (Author, AuthorStats, Book, BookDetails, BookSeries,
                                              BookStats, DetailedBook,
                                              MainEdition, BookAward, BookSetting, _ScriptTagData)

PROVIDER = "www.goodreads.com"
# the unofficially known enforced throttling delay
# between requests to Goodreads servers is 1 s
# we're choosing to be safe here
THROTTLING_DELAY = 1.2  # seconds


class AuthorParser:
    """Goodreads author page parser.
    """
    URL_TEMPLATE = "https://www.goodreads.com/author/list/{}"

    @property
    def author_name(self) -> str | None:
        return self._author_name

    @property
    def author_id(self) -> str | None:
        return self._author_id

    def __init__(self, author: str) -> None:
        """Initialize.

        Args:
            author: author's full name or Goodreads author ID (one fewer sever request)
        """
        self._author_id, self._author_name = None, None
        if is_goodreads_id(author):
            self._author_id = author
        else:
            self._author_name = author

    @staticmethod
    def normalize_name(author_name: str) -> str:
        """Return 'author_name' as rendered in Goodreads author ID.

        Goodreads logic that formulates author ID:

        1) replace any whitespace with underscore
            Example: '589.Orson_Scott_Card'

        2) replace any non-alphabetic character with underscore
            Apostrophe: 'Madeleine L'Engle' ==> '106.Madeleine_L_Engle'
            Hyphen: 'Chi Ta-wei' ==> '14640243.Chi_Ta_wei'
            Dot: 'George R.R. Martin' ==> '346732.George_R_R_Martin'

        3) replace any non-ASCII character with underscore
            Example: 'Stanisław Lem' ==> '10991.Stanis_aw_Lem'

        4) replace any immediately repeated underscore with only one instance
            Example: 'Ewa Białołęcka' ==> '554577.Ewa_Bia_o_cka'
        """
        chars, underscore_appended = [], False
        for char in author_name:
            if not (char.isalpha() and char.isascii()):
                if not underscore_appended:
                    chars.append("_")
                    underscore_appended = True
                else:
                    continue
            else:
                chars.append(char)
                underscore_appended = False
        return "".join(chars)

    @classmethod
    @throttled(THROTTLING_DELAY)
    def find_author_id(cls, author_name: str) -> str:
        """Find Goodreads author ID by quering a Goodreads search with ``author_name``.

        Args:
            author_name: full author's name

        Example:
            '7415.Harlan_Ellison'
        """
        def parse_spans(spans_: List[Tag]) -> Optional[Tag]:
            for span in spans_:
                a_ = span.find(
                    lambda t: t.name == "a" and cls.normalize_name(author_name) in t.attrs.get(
                        "href"))
                if a_ is not None:
                    return a_
            return None

        query = "+".join(author_name.split())
        url_template = "https://www.goodreads.com/search?q={}"
        url = url_template.format(query)
        soup = getsoup(url)
        spans = soup.find_all("span", itemprop="author")
        if not spans:
            raise ParsingError(f"No 'span' tags with author's data according to query: "
                               f"{query!r}")

        a = parse_spans(spans)
        if not a:
            raise ParsingError(f"No 'a' tag containing the queried author's URL")

        url = a.attrs.get("href")
        # URL now ought to look like this:
        # 'https://www.goodreads.com/author/show/7415.Harlan_Ellison?from_search=true&from_srp=true'
        url, _ = url.split("?")  # stripping the trash part
        *_, id_ = url.split("/")
        return id_

    @staticmethod
    def _parse_author_stats(parts: List[str]) -> AuthorStats:
        """Parse author stats string parts extracted from the author list page.

        Example string parts:
            ['Average rating 4.17',
            '197,169 ratings',
            '12,120 reviews',
            'shelved 428,790 times']
        """
        if len(parts) != 4:
            raise ParsingError(f"Invalid author stats parts: {parts}")
        avg_rating = extract_float(parts[0])
        ratings = extract_int(parts[1])
        reviews = extract_int(parts[2])
        shelvings = extract_int(parts[3])
        return AuthorStats(avg_rating, ratings, reviews, shelvings)

    @staticmethod
    def _parse_published(row: Tag) -> Optional[int]:
        tag = row.find(lambda t: t.name == "span" and "published" in t.text)
        if tag is None:
            return None
        text = tag.text.strip()
        parts = text.split("\n")
        part = from_iterable(parts, lambda p: "published" in p)
        if part is None:
            return None
        idx = parts.index(part)
        idx += 1
        try:
            published = parts[idx]
            return extract_int(published)
        except (IndexError, ValueError):
            return None

    @staticmethod
    def _parse_editions(row: Tag) -> Optional[int]:
        editions = row.find(lambda t: t.name == "a" and "edition" in t.text)
        if editions is None:
            return None
        editions = editions.text.strip()
        try:
            return extract_int(editions)
        except ValueError:
            return None

    @classmethod
    def _parse_book_table_row(cls, row: Tag) -> Book:
        """Parse a book table row of the author page's book list.

        Args:
            row: a BeautifulSoup Tag object representing the row

        Returns:
            a Book object
        """
        a = row.find("a")
        if not a:
            raise ParsingError(f"No 'a' tag with title data in a row: {row}")
        title = a.attrs.get("title")
        if not title:
            raise ParsingError(f"No 'title' attribute in 'a' title tag within a row: {row}")
        href = a.attrs.get("href")
        if not href:
            raise ParsingError(f"No 'href' attribute in 'a' title' tag within a row: {row}")
        id_ = href.replace("/book/show/", "")
        ratings_text = row.find("span", class_="minirating").text.strip()
        avg, ratings = ratings_text.split(" — ")
        avg = extract_float(avg)
        ratings = extract_int(ratings)
        published = cls._parse_published(row)
        published = datetime(published, 1, 1) if published is not None else None
        editions = cls._parse_editions(row)

        return Book(title, id_, avg, ratings, published, editions)

    @throttled(THROTTLING_DELAY)
    def _parse_author_page(self) -> Author:
        """Scrape and parse Goodreads author page and return an Author object.

        Example URL:
            https://www.goodreads.com/author/list/7415.Harlan_Ellison

        Returns:
            an Author object
        """
        url = self.URL_TEMPLATE.format(self.author_id)
        soup = getsoup(url)
        container = soup.find("div", class_="leftContainer")
        name_tag = container.find("a", class_="authorName")
        if name_tag is None:
            raise ParsingError("No author name tag")
        self._author_name = name_tag.text.strip()

        # author stats
        div = container.find("div", class_="")
        text = div.text.strip()
        parts = [part.strip() for part in text.split("\n")[1:]]
        parts = [part.strip(" ·") for part in parts]
        stats = self._parse_author_stats(parts)

        # books
        table = container.find("table", class_="tableList")
        rows = table.find_all("tr")
        books = [self._parse_book_table_row(row) for row in rows]

        return Author(self.author_name, self.author_id, stats, books)

    def fetch_data(self) -> Author:
        try:
            if not self.author_id:
                self._author_id = self.find_author_id(self.author_name)
            author = self._parse_author_page()
        except Timeout:
            print("Goodreads doesn't play nice. Timeout exceeded. Retrying with backoff "
                  "(60 seconds max)...")
            return self.fetch_data_with_backoff()
        return author

    @backoff.on_exception(backoff.expo, Timeout, max_time=60)
    def fetch_data_with_backoff(self) -> Author:
        if not self.author_id:
            self._author_id = self.find_author_id(self.author_name)
        return self._parse_author_page()


class _ScriptTagParser:
    """Sub-parser of data contained in Goodreads book page's meta 'script' tag obtained by
    `soup.find("script", id="__NEXT_DATA__")`.
    """
    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data
        self._book_data = self._item("Book:kca://")
        self._work_data = self._item("Work:kca://")
        if self._book_data is None:
            raise ParsingError("No 'Book:kca://' data on the 'script' tag")
        if self._work_data is None:
            raise ParsingError("No 'Work:kca://' data on the 'script' tag")

    def _item(self, key_part: str) -> Any | None:
        key = from_iterable(self._data, lambda k: key_part in k)
        if not key:
            return None
        return self._data[key]

    @staticmethod
    def _parse_timestamp(timestamp: int) -> datetime:  # GPT3
        # assuming the timestamp is in PST
        # parse the timestamp into a datetime object in the local time zone (CET)
        dt = datetime.fromtimestamp(timestamp / 1000)   # from milliseconds to seconds
        # convert the datetime object to UTC
        cet_tz = pytz.timezone('CET')
        dt_utc = cet_tz.localize(dt).astimezone(pytz.UTC)
        # apply the -8 hour offset
        offset = timedelta(hours=-8)
        dt_offset = dt_utc + offset
        # convert the datetime object back to the CET time zone
        dt_cet = dt_offset.astimezone(cet_tz)
        return dt_cet

    def parse(self) -> _ScriptTagData:
        try:
            complete_title = self._book_data["titleComplete"]
            details = self._book_data["details"]
            main_edition = MainEdition(
                publisher=details["publisher"],
                publication=self._parse_timestamp(details["publicationTime"]),
                format=details["format"],
                pages=details["numPages"],
                language=name2langcode(details["language"]["name"]),
                isbn=details["isbn"],
                isbn13=details["isbn13"],
                asin=details["asin"],
            )
            blurb = self._book_data['description({"stripped":true})'].strip()
            genres = []
            for item in self._book_data["bookGenres"]:
                genre = item["genre"]
                genres.append(genre["name"])
            *_, work_id = self._work_data["details"]["webUrl"].split("/")
            original_title = self._work_data["details"]["originalTitle"]
            first_publication = self._work_data["details"]["publicationTime"]
            first_publication = self._parse_timestamp(
                first_publication) if first_publication is not None else None
            ratings = self._work_data["stats"]["ratingsCountDist"]
            ratings = FiveStars({i: votes for i, votes in enumerate(ratings, start=1)})
            reviews = []
            for item in self._work_data["stats"]["textReviewsLanguageCounts"]:
                reviews.append((item["isoLanguageCode"], item["count"]))
            reviews = LangReviewsDistribution(dict(reviews))
            # this is always greater than the distribution's total
            total_reviews = self._work_data["stats"]["textReviewsCount"]
            awards = []
            for item in self._work_data["details"]["awardsWon"]:
                *_, id_ = item["webUrl"].split("/")
                award = BookAward(
                    name=item["name"],
                    id=id_,
                    date=self._parse_timestamp(item["awardedAt"]),
                    category=item["category"],
                    designation=item["designation"],
                )
                awards.append(award)
            places = []
            for item in self._work_data["details"]["places"]:
                year = item["year"]
                year = datetime(int(year), 1, 1) if year else None
                *_, id_ = item["webUrl"].split("/")
                place = BookSetting(
                    name=item["name"],
                    id=id_,
                    country=item["countryName"],
                    year=year
                )
                places.append(place)
            characters = [item["name"] for item in self._work_data["details"]["characters"]]
        except KeyError as ke:
            raise ParsingError(f"A key on 'script' tag data is unavailable: {ke}")

        return _ScriptTagData(
            title=original_title,
            complete_title=complete_title,
            work_id=work_id,
            ratings=ratings,
            reviews=reviews,
            total_reviews=total_reviews,
            first_publication=first_publication,
            details=BookDetails(
                description=blurb,
                main_edition=main_edition,
                genres=genres,
                awards=awards,
                places=places,
                characters=characters,
            )
        )


_AuthorsData = Dict[str, datetime | str | List[Author]]
_Contributor = namedtuple("_Contributor", "author_id has_role")


class BookParser:
    """Goodreads book page parser.
    """
    URL_TEMPLATE = "https://www.goodreads.com/book/show/{}"
    EDITIONS_URL_TEMPLATE = "https://www.goodreads.com/work/editions/{}?page={}&per_page=100"
    DATE_FORMAT = "%B %d, %Y"  # datetime.strptime("August 16, 2011", "%B %d, %Y")

    @property
    def book_id(self) -> str:
        return self._book_id

    @property
    def work_id(self) -> str | None:
        return self._work_id

    @property
    def series_id(self) -> str | None:
        return self._series_id

    def __init__(self, book: str, author: str | None,
                 authors_data: _AuthorsData | None = None) -> None:
        """Initialize.

        Args:
            book: book's title or book ID
            author: optionally (if book ID was not provided), book author's full name or Goodreads author ID
            authors_data: optionally, data as read from JSON saved by dump_authors()
        """
        if is_goodreads_id(book):
            self._book_id = book
        else:
            if not author:
                raise ValueError("Author must be specified when Book ID not provided")
            self._book_id = self.find_book_id(book, author, authors_data)
        if not self._book_id:
            raise ValueError("Unable to derive Goodreads book ID from provided input")
        self._work_id = None  # this is different than book_id
        self._series_id = None
        self._url = self.URL_TEMPLATE.format(self.book_id)
        self._other_stats_url = None
        self._series_url = None
        self._shelves_url = None

    def _set_secondary_urls(self) -> None:
        self._other_stats_url = (f"https://www.goodreads.com/book/stats"
                                 f"?id={numeric_id(self.book_id)}")
        if self.series_id:
            self._series_url = f"https://www.goodreads.com/series/{self.series_id}"
        self._shelves_url = f"https://www.goodreads.com/work/shelves/{self.work_id}"

    def _editions_url(self, page: int) -> str:
        return self.EDITIONS_URL_TEMPLATE.format(numeric_id(self.work_id), page)

    @staticmethod
    def _find_book_in_author_books(author: Author, title: str) -> Book | None:
        book = from_iterable(author.books, lambda b: b.title.casefold() == title.casefold())
        if not book:
            # Goodreads gets fancy with their apostrophes...
            book = from_iterable(
                author.books, lambda b: b.title.casefold() == title.replace("'", "’").casefold())
            # let's be even less strict...
            if not book:
                book = from_iterable(author.books, lambda b: title.casefold() in b.title.casefold())
                if not book:
                    book = from_iterable(
                        author.books, lambda b: title.replace(
                            "'", "’").casefold() in b.title.casefold())
        return book

    @classmethod
    def book_id_from_data(cls, title: str, author: str, authors_data: _AuthorsData) -> str | None:
        """Derive Goodreads book ID from provided authors data.

        Args:
            title: book's title
            author: book author's full name or Goodreads author ID
            authors_data: data as read from JSON saved by dump_authors()

        Returns:
            derived book ID or None
        """
        authors: List[Author] = authors_data["authors"]
        if is_goodreads_id(author):
            author = from_iterable(authors, lambda a: a.id == author)
        else:
            author = from_iterable(authors, lambda a: a.name.casefold() == author.casefold())
        if not author:
            return None
        book = cls._find_book_in_author_books(author, title)
        if not book:
            return None
        return book.id

    @classmethod
    def fetch_book_id(cls, title: str, author: str) -> str | None:
        """Scrape author data and extract Goodreads book ID from it according to arguments passed.

        Args:
            title: book's title
            author: book author's name or author ID

        Returns:
            fetched book ID or None
        """
        author = AuthorParser(author).fetch_data()
        book = cls._find_book_in_author_books(author, title)
        if not book:
            return None
        return book.id

    @classmethod
    def find_book_id(cls, title: str, author: str,
                     authors_data: _AuthorsData | None = None) -> str | None:
        """Find Goodreads book ID based on provided arguments.

        Performs the look-up on ``authors_data`` if provided. Otherwise, scrapes Goodreads author
        page for the ID.

        Args:
            title: book's title
            author: book's author or author ID
            authors_data: data as read from JSON saved by dump_authors() (if not provided, Goodreads author page is scraped)

        Returns:
            book ID found or None
        """
        id_ = None
        if authors_data:
            id_ = cls.book_id_from_data(title, author, authors_data)
        if not id_:
            id_ = cls.fetch_book_id(title, author)
        return id_

    @staticmethod
    def _parse_title(soup: BeautifulSoup) -> str:  # not used
        tag = soup.find(
            lambda t: t.name == "h1" and t.attrs.get("data-testid") == "bookTitle")
        if tag is None:
            raise ParsingError("No tag with title data")
        return tag.text

    @staticmethod
    def _parse_first_publication(soup: BeautifulSoup) -> datetime:  # not used
        p_tag = soup.find(
            lambda t: t.name == "p" and t.attrs.get("data-testid") == "publicationInfo")
        if p_tag is None:
            raise ParsingError("No tag with first publication data")
        # p_tag.text can be either 'First published October 1, 1967' or 'Published October 1, 1967'
        *_, text = p_tag.text.split("ublished")
        return datetime.strptime(text.strip(), "%B %d, %Y")

    @staticmethod
    def _parse_contributor(a_tag: Tag) -> _Contributor:
        url = a_tag.attrs.get("href")
        if not url:
            raise ParsingError("No 'href' attribute on contributor 'a' tag")
        *_, id_ = url.split("/")
        return _Contributor(id_, a_tag.find("span", class_="ContributorLink__role") is not None)

    @classmethod
    def _parse_authors_line(cls, soup: BeautifulSoup) -> List[str]:
        contributor_div = soup.find("div", class_="ContributorLinksList")
        if contributor_div is None:
            raise ParsingError("No 'div' tag with contributors data")
        contributor_tags = contributor_div.find_all("a")
        if not contributor_tags:
            raise ParsingError("No contributor data 'a' tags")
        contributors = [cls._parse_contributor(tag) for tag in contributor_tags]
        authors = [c for c in contributors if not c.has_role]
        if not authors:
            return [contributors[0].author_id]
        return [a.author_id for a in authors]

    @staticmethod
    def _parse_specifics_row(row: Tag) -> Tuple[int, int]:  # not used
        label = row.attrs.get("aria-label")
        if not label:
            raise ParsingError("No label for detailed ratings")
        ratings_div = row.find("div", class_="RatingsHistogram__labelTotal")
        if ratings_div is None:
            raise ParsingError("No 'div' tag with detailed ratings data")
        text, _ = ratings_div.text.split("(")
        ratings = extract_int(text)
        return extract_int(label), ratings

    def _parse_ratings_stats(self, soup: BeautifulSoup) -> Tuple[FiveStars, int]:  # not used
        ratings_div: Tag | None = soup.find("div", class_="ReviewsSectionStatistics")
        if ratings_div is None:
            raise ParsingError(f"This book ID: {self.book_id!r} doesn't produce a parseable markup")
        general_div = ratings_div.find("div", class_="RatingStatistics__meta")
        if general_div is None:
            raise ParsingError("No detailed ratings pane")
        text = general_div.attrs.get("aria-label")
        if not text:
            raise ParsingError("No ratings/reviews text")
        _, reviews_text = text.split("ratings")
        reviews = extract_int(reviews_text)
        specifics_rows = ratings_div.find_all("div", class_="RatingsHistogram__bar")
        if not specifics_rows:
            raise ParsingError("No rows with detailed ratings")
        dist = FiveStars(dict(self._parse_specifics_row(row) for row in specifics_rows))
        return dist, reviews

    @staticmethod
    def _parse_meta_script_tag(soup: BeautifulSoup) -> _ScriptTagData:
        t = soup.find("script", id="__NEXT_DATA__")
        try:
            parser = _ScriptTagParser(json.loads(t.text)["props"]["pageProps"]["apolloState"])
        except (KeyError, AttributeError):
            raise ParsingError("No valid meta 'script' tag to parse")
        return parser.parse()

    @staticmethod
    def _parse_series_id(soup: BeautifulSoup) -> str | None:
        tag = soup.find("div", class_="BookPageTitleSection__title")
        a = tag.find("a")
        if a is None:
            return None
        *_, id_ = a.attrs.get("href").split("/")
        return id_

    @throttled(THROTTLING_DELAY)
    def _parse_book_page(self) -> Tuple[_ScriptTagData, List[str], str, BeautifulSoup]:
        soup = getsoup(self._url)
        script_data = self._parse_meta_script_tag(soup)
        authors = self._parse_authors_line(soup)
        series_id = self._parse_series_id(soup)
        return script_data, authors, series_id, soup

    @staticmethod
    def _validate_series_div(div: Tag) -> bool:
        h3 = div.find("h3")
        if h3 is None:
            return False
        if any(char in h3.text for char in ",-"):
            return False
        if "BOOK" not in h3.text.upper():
            return False
        return True

    @throttled(THROTTLING_DELAY)
    def _parse_series_page(self) -> BookSeries | None:
        soup = getsoup(self._series_url)
        # title
        title_tag = soup.find("div", class_="responsiveSeriesHeader__title")
        if title_tag is None:
            raise ParsingError("No tag with series title data")
        title = title_tag.find("h1").text
        if "by" in title:
            title, *_ = title.split("by")
            title = title.strip()
        elif "Series" in title:
            title, *_ = title.split("Series")
            title = title.strip()
        # layout
        items = soup.find_all("div", class_="listWithDividers__item")
        items = [item for item in items if self._validate_series_div(item)]
        if not items:
            return None  # 'Dangerous Visions' by Harlan Ellison case
        series = OrderedDict()
        for i, item in enumerate(items, start=1):
            numbering = extract_float(item.find("h3").text)
            a_tag = item.find("a", href=lambda href: href and "/book/show/" in href)
            if a_tag is None:
                raise ParsingError(f"No book ID data on #{i} series item")
            book_id = a_tag.attrs.get("href").replace("/book/show/", "")
            series[numbering] = book_id
        return BookSeries(title, self.series_id, series)

    @throttled(THROTTLING_DELAY)
    def _parse_shelves_page(self) -> OrderedDict[int, str]:
        soup = getsoup(self._shelves_url)
        shelf_tags = soup.find_all("div", class_="shelfStat")
        shelves = OrderedDict()
        for tag in shelf_tags:
            name = tag.find("a").text
            shelvings_tag = tag.find(lambda t: t.name == "div" and "people" in t.text)
            if shelvings_tag is None:
                continue
            shelvings = extract_int(shelvings_tag.text)
            shelves[shelvings] = name
        return shelves

    @throttled(THROTTLING_DELAY)
    def _parse_editions_page(
            self, page: int,
            editions: DefaultDict[str, Set[str]] | None = None
    ) -> Tuple[DefaultDict[str, Set[str]], int]:
        soup = getsoup(self._editions_url(page))
        items = soup.find_all("div", class_="elementList clearFix")
        editions = editions or defaultdict(set)
        count = 0
        for item in items:
            count += 1
            title = item.find("a", class_="bookTitle").text
            if "(" in title:
                title, *_ = title.split("(")
                title = title.strip()
            hidden_tag = item.find("div", class_="moreDetails hideDetails")
            data_rows = hidden_tag.find_all("div", class_="dataRow")
            data_row = from_iterable(
                data_rows, lambda dr: dr.find(
                    lambda t: t.name == "div" and "Edition language:" in t.text) is not None)
            if data_row is None:
                continue
            lang = data_row.find("div", class_="dataValue").text.strip()
            editions[lang].add(title)

        return editions, count

    def _scrape_editions(self) -> Tuple[OrderedDict[str, List[str]], int]:
        counter = itertools.count(1)
        editions, total, next_page = None, 0,  True
        for i in counter:
            editions, editions_count = self._parse_editions_page(i, editions)
            total += editions_count
            if editions_count < 100 or i > 10:
                break
        ordered = OrderedDict(sorted([(name2langcode(lang), sorted(titles))
                              for lang, titles in editions.items()]))
        return ordered, total

    def _scrape_book(self) -> DetailedBook:
        script_data, authors, self._series_id, soup = self._parse_book_page()
        if not script_data.title:
            script_data.title = self._parse_title(soup)
        if script_data.first_publication is None:
            script_data.first_publication = self._parse_first_publication(soup)
        self._work_id = script_data.work_id
        self._set_secondary_urls()
        series = self._parse_series_page() if self.series_id else None
        shelves = self._parse_shelves_page()
        editions, total_editions = self._scrape_editions()
        stats = BookStats(
            ratings=script_data.ratings,
            reviews=script_data.reviews,
            total_reviews=script_data.total_reviews,
            shelves=shelves,
            editions=editions,
            total_editions=total_editions,
        )
        return DetailedBook(
            title=script_data.title,
            complete_title=script_data.complete_title,
            book_id=self.book_id,
            work_id=self.work_id,
            authors=authors,
            first_publication=script_data.first_publication,
            series=series,
            details=script_data.details,
            stats=stats,
        )

    def fetch_data(self) -> DetailedBook:
        try:
            book = self._scrape_book()
        except Timeout:
            print("Goodreads doesn't play nice. Timeout exceeded. Retrying with backoff "
                  "(60 seconds max)...")
            return self.fetch_data_with_backoff()
        return book

    @backoff.on_exception(backoff.expo, Timeout, max_time=60)
    def fetch_data_with_backoff(self) -> DetailedBook:
        return self._scrape_book()


def load_authors(authors_json: PathLike) -> _AuthorsData:
    """Load ``authors_json`` into a dictionary containg a list of Author objects and return it.

    Args:
        authors_json: path to a JSON file saved earlier by dump_authors()
    """
    authors_json = getfile(authors_json, ext=".json")
    data = json.loads(authors_json.read_text(encoding="utf8"))
    data["timestamp"] = datetime.strptime(data["timestamp"], READABLE_TIMESTAMP_FORMAT)
    data["authors"] = [Author.from_dict(item) for item in data["authors"]]
    return data


def dump_authors(*authors: str, **kwargs: Any) -> None:
    """Fetch data on ``authors`` and dump it to JSON.

    Args:
        authors: variable number of author full names or Goodread author IDs (in case of the latter there's one request fewer)
        kwargs: optional arguments (e.g. a prefix for a dumpfile's name, an output directory, etc.)
    """
    timestamp = datetime.now()
    data = {
        "timestamp": timestamp.strftime(READABLE_TIMESTAMP_FORMAT),
        "provider": PROVIDER,
        "authors": [],
    }
    for i, author in enumerate(authors, start=1):
        print(f"Scraping author #{i}: {author!r}...")
        parser = AuthorParser(author)
        try:
            fetched = parser.fetch_data_with_backoff()
        except ParsingError as e:
            print(f"{e}. Skipping...")
            continue
        data["authors"].append(fetched.as_dict)

        if i != len(authors):
            print()

    data["authors"] = sorted(data["authors"], key=lambda item: item["name"].casefold())

    # kwargs
    prefix = kwargs.get("prefix") or ""
    prefix = f"{prefix}_" if prefix else ""
    use_timestamp = kwargs.get("use_timestamp") if kwargs.get("use_timestamp") is not None else \
        True
    timestamp = f"_{timestamp.strftime(FILNAME_TIMESTAMP_FORMAT)}" if use_timestamp else ""
    output_dir = kwargs.get("output_dir") or kwargs.get("outputdir") or OUTPUT_DIR
    output_dir = getdir(output_dir)
    filename = kwargs.get("filename")
    if filename:
        filename = filename
    else:
        filename = f"{prefix}dump{timestamp}.json"

    dest = output_dir / filename
    with dest.open("w", encoding="utf8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    if dest.exists():
        print(f"Successfully dumped '{dest}'")


def update_authors(authors_json: PathLike) -> None:
    """Load authors from ``authors_json``, scrape them again and save at the same location (
    with updated file timestamp).

    Args:
        authors_json: path to a JSON file saved earlier by dump_authors()
    """
    output_dir = Path(authors_json).parent
    data = load_authors(authors_json)
    authors = data["authors"]
    ids = [author.id for author in authors]
    dump_authors(*ids, prefix="authors", output_dir=output_dir)


def update_tolkien() -> None:
    outputdir = Path(__file__).parent.parent.parent / "data"
    dump_authors("J.R.R. Tolkien", use_timestamp=False, outputdir=outputdir,
                 filename="tolkien.json")


