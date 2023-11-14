"""

    bookscrape.scrape.goodreads.scrapers.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Goodreads scraper objects.

    @author: z33k

"""
import itertools
import json
import logging
import re
from collections import OrderedDict, defaultdict, namedtuple
from datetime import datetime, timedelta
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Set, Tuple

import backoff
import pytz
from bs4 import BeautifulSoup, Tag
from requests import HTTPError, Timeout

from bookscrape.scrape.provider.goodreads.utils import is_goodreads_id, numeric_id, url2id
from bookscrape.scrape.provider.goodreads.data import Author, AuthorStats, Book, BookAward, BookDetails, \
    BookSeries, BookSetting, BookStats, DetailedBook, MainEdition, SimpleAuthor, _ScriptTagData
from bookscrape.scrape.stats import FiveStars, ReviewsDistribution
from bookscrape.scrape.utils import getsoup, throttled, ParsingError
from bookscrape.utils import extract_float, extract_int, from_iterable, name2langcode, timed

_log = logging.getLogger(__name__)
# the unofficially known enforced throttling delay
# between requests to Goodreads servers is 1 s
# we're choosing to be safe here
THROTTLING_DELAY = 1.2  # seconds


class AuthorScraper:
    """Scraper of Goodreads author data.

    Scrapes Goodreads 'Books by author' page that displays the author's stats and a list of their 30
    most popular books.
    """
    URL_TEMPLATE = "https://www.goodreads.com/author/list/{}"
    EXTENEDED_URL_TEMPLATE = "https://www.goodreads.com/author/list/{}?page=1&per_page=100"

    @property
    def author_name(self) -> str | None:
        return self._author_name

    @property
    def author_id(self) -> str | None:
        return self._author_id

    def __init__(self, author: str) -> None:
        """Provide author's full name (or Goodreads author ID) to scrape their data.

        When Goodreads author ID is provided, there's one less server request (no need to derive
        an ID from the provided name).

        Args:
            author: author's full name or Goodreads author ID
        """
        self._author_id, self._author_name = None, None
        if is_goodreads_id(author):
            self._author_id = author
        else:
            self._author_name = sanitize_input(author)

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
                    lambda t: t.name == "a" and cls.normalize_name(
                        author_name).casefold() in t.attrs["href"].casefold())
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
        id_ = url2id(url)
        if not id_:
            raise ParsingError(f"Could not extract author's ID from URL: {url!r}")
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
        id_ = url2id(href)
        if not id_:
            raise ParsingError(f"Could not extract book's ID from URL: {href!r}")
        ratings_text = row.find("span", class_="minirating").text.strip()
        avg, ratings = ratings_text.split(" — ")
        avg = extract_float(avg)
        ratings = extract_int(ratings)
        published = cls._parse_published(row)
        published = datetime(published, 1, 1) if published is not None else None
        editions = cls._parse_editions(row)

        return Book(sanitize_output(title), id_, avg, ratings, published, editions)

    @throttled(THROTTLING_DELAY)
    def _parse_author_page_contents(self, url: str) -> Tuple[List[Tag], AuthorStats]:
        soup = getsoup(url)
        container = soup.find("div", class_="leftContainer")
        name_tag = container.find("a", class_="authorName")
        if name_tag is None:
            raise ParsingError("No author name tag")
        self._author_name = sanitize_output(name_tag.text)
        # author stats
        div = container.find("div", class_="")
        text = div.text.strip()
        parts = [part.strip() for part in text.split("\n")[1:]]
        parts = [part.strip(" ·") for part in parts]
        stats = self._parse_author_stats(parts)
        # books
        table = container.find("table", class_="tableList")
        rows = table.find_all("tr")
        return rows, stats

    def _parse_author_page(self, extended_top_books=False) -> Author:
        """Parse Goodreads 'Books by author' page and return an author object.

        Example URLs:
            https://www.goodreads.com/author/list/7415.Harlan_Ellison
            https://www.goodreads.com/author/list/7415.Harlan_Ellison?page=1&per_page=100

        Returns:
            an Author object
        """
        if extended_top_books:
            url = self.EXTENEDED_URL_TEMPLATE.format(self.author_id)
        else:
            url = self.URL_TEMPLATE.format(self.author_id)
        rows, stats = self._parse_author_page_contents(url)
        top_books = [self._parse_book_table_row(row) for row in rows]
        return Author(self.author_name, self.author_id, stats, top_books)

    @timed("author scraping")
    def scrape(self, extended_top_books=False) -> Author | SimpleAuthor:
        """Scrape Goodreads for author data.

        Args:
            extended_top_books: if True, scrape 100 top books instead of Goodreads's default of 30

        Returns:
            an Author or SimpleAuthor (if called on SimpleAuthorParser) object
        """
        try:
            if not self.author_id:
                self._author_id = self.find_author_id(self.author_name)
            author = self._parse_author_page(extended_top_books)
        except HTTPError as e:
            _log.warning(f"Goodreads had a hiccup ({e}). Retrying with backoff "
                         "(60 seconds max)...")
            return self.scrape_with_backoff()
        except Timeout:
            _log.warning("Goodreads doesn't play nice. Timeout exceeded. Retrying with backoff "
                         "(60 seconds max)...")
            return self.scrape_with_backoff(extended_top_books)
        return author

    @timed("author scraping (with backoff)")
    @backoff.on_exception(backoff.expo, (Timeout, HTTPError), max_time=60)
    def scrape_with_backoff(self, extended_top_books=False) -> Author | SimpleAuthor:
        """Scrape Goodreads for either full or simplified author data with (one minute max)
        backoff on timeout.
        """
        if not self.author_id:
            self._author_id = self.find_author_id(self.author_name)
        return self._parse_author_page(extended_top_books)


class SimpleAuthorScraper(AuthorScraper):
    """Scraper of Goodreads author data in a simplified form.

    As the superclass does, it scrapes Goodreads 'Books by author' page that displays the author's
    stats and a list of their 30 most popular books. The difference is that instead of an Author
    object with full data on author's books it returns a SimpleAuthor object that holds only book
    IDs as the books data.
    """
    @classmethod
    def _parse_book_table_row(cls, row: Tag) -> str:  # overridden
        """Parse a book table row of the author page's book list for Goodreads book ID only.

        Args:
            row: a BeautifulSoup Tag object representing the row

        Returns:
            a Goodreads book ID
        """
        a = row.find("a")
        if not a:
            raise ParsingError(f"No 'a' tag with title data in a row: {row}")
        href = a.attrs.get("href")
        if not href:
            raise ParsingError(f"No 'href' attribute in 'a' title' tag within a row: {row}")
        id_ = url2id(href)
        if not id_:
            raise ParsingError(f"Could not extract book's ID from URL: {href!r}")
        return id_

    def _parse_author_page(self, extended_top_books=False) -> SimpleAuthor:  # overridden
        """Parse Goodreads 'Books by author' page and return an author object.

        Example URLs:
            https://www.goodreads.com/author/list/7415.Harlan_Ellison
            https://www.goodreads.com/author/list/7415.Harlan_Ellison?page=1&per_page=100

        Returns:
            a SimpleAuthor object
        """
        if extended_top_books:
            url = self.EXTENEDED_URL_TEMPLATE.format(self.author_id)
        else:
            url = self.URL_TEMPLATE.format(self.author_id)
        rows, stats = self._parse_author_page_contents(url)
        top_book_ids = [self._parse_book_table_row(row) for row in rows]
        return SimpleAuthor(self.author_name, self.author_id, stats, top_book_ids)


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
        key_items = sorted([v for k, v in self._data.items() if key_part in k], key=len,
                           reverse=True)
        if not key_items:
            return None
        return key_items[0]

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

    def _parse_blurb(self) -> str:
        blurb = self._book_data.get('description({"stripped":true})')
        if blurb:
            return blurb.strip()
        blurb = self._book_data.get('description')
        return blurb.strip() if blurb is not None else ""

    def _parse_affiliate_links(self) -> Tuple[str, str]:
        links = self._book_data['links({})']
        amazon = links['primaryAffiliateLink']["url"]
        *amazon, _ = amazon.split("/")
        amazon = "/".join(amazon)
        bn = from_iterable(links['secondaryAffiliateLinks'],
                           lambda item: item["name"] == "Barnes & Noble")
        if not bn:
            raise ParsingError("Could not parse Barnes & Noble affiliate link")
        return amazon, bn["url"]

    def parse(self) -> _ScriptTagData:
        try:
            details = self._book_data["details"]
            lang = details["language"]["name"]
            main_edition = MainEdition(
                publisher=details["publisher"],
                format=details["format"],
                publication=self._parse_timestamp(
                    details["publicationTime"]) if details.get("publicationTime") else None,
                pages=details["numPages"],
                language=name2langcode(lang) if lang else None,
                isbn=details["isbn"],
                isbn13=details["isbn13"],
                asin=details["asin"],
            )
            blurb = self._parse_blurb()
            genres = []
            for item in self._book_data["bookGenres"]:
                genre = item["genre"]
                genres.append(genre["name"])
            work_id = url2id(self._work_data["details"]["webUrl"])
            if not work_id:
                raise ParsingError(
                    f"Could not parse work ID from URL: {self._work_data['details']['webUrl']}")
            original_title = self._work_data["details"]["originalTitle"].strip()
            # sanitize "Dauntless (The Lost Fleet, #1)" cases
            original_title = re.sub(r"\s\(.+#\d{1,2}\)$", "", original_title)
            first_publication = self._work_data["details"]["publicationTime"]
            first_publication = self._parse_timestamp(
                first_publication) if first_publication is not None else None
            ratings = self._work_data["stats"]["ratingsCountDist"]
            ratings = FiveStars({i: votes for i, votes in enumerate(ratings, start=1)})
            reviews = []
            for item in self._work_data["stats"]["textReviewsLanguageCounts"]:
                reviews.append((item["isoLanguageCode"], item["count"]))
            reviews = ReviewsDistribution(dict(reviews))
            # this is always greater than the distribution's total
            total_reviews = self._work_data["stats"]["textReviewsCount"]
            awards = []
            for item in self._work_data["details"]["awardsWon"]:
                id_ = url2id(item["webUrl"])
                if not id_:
                    continue
                timestamp = item["awardedAt"]
                award = BookAward(
                    name=item["name"],
                    id=id_,
                    date=self._parse_timestamp(timestamp) if timestamp else None,
                    category=item["category"],
                    designation=item["designation"],
                )
                awards.append(award)
            places = []
            for item in self._work_data["details"]["places"]:
                year = item["year"]
                year = datetime(int(year), 1, 1) if year else None
                id_ = url2id(item["webUrl"])
                if not id_:
                    continue
                place = BookSetting(
                    name=item["name"],
                    id=id_,
                    country=item["countryName"],
                    year=year
                )
                places.append(place)
            characters = [item["name"] for item in self._work_data["details"]["characters"]]
            amazon, bn = self._parse_affiliate_links()
        except KeyError as ke:
            raise ParsingError(f"A key on 'script' tag data is unavailable: {ke}")

        return _ScriptTagData(
            original_title=original_title,
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
            ),
            amazon_url=amazon,
            barnes_and_noble_url=bn,
        )


_AuthorsData = Dict[str, datetime | str | List[Author]]
_Contributor = namedtuple("_Contributor", "author_id has_role")


class BookScraper:
    """Scraper of Goodreads book data.

    Scrapes detailed data on a book contained within a Goodreads book page and several connected
    pages that detail the book's series, shelves and editions.
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

    def __init__(self, book_cue: str | Tuple[str, str],
                 authors_data: Iterable[Author] | None = None) -> None:
        """Provide either a Goodreads book ID or book's title and author (either their full
        name or their Goodreads ID) to scrape detailed data on it.

        Information on the book's author is only needed to determine the book's Goodreads ID.
        The fastest route is to provide both the author's Goodreads ID and Author objects iterable.
        Otherwise, the missing pieces are scraped, with additional requests.

        Args:
            book_cue: Goodreads book ID or (title, author) tuple
            authors_data: optionally, previously scraped authors data to speed up book IDs derivation
        """
        if isinstance(book_cue, tuple):
            book, author = book_cue
            book, author = sanitize_input(book), sanitize_input(author)
            self._book_id = self.find_book_id(book, author, authors_data)
            if not self._book_id:
                raise ValueError(f"Could not derive Goodreads book ID from {book_cue}")
        elif not isinstance(book_cue, str) or not is_goodreads_id(book_cue):
            raise ValueError("Book ID or (title, author) tuple must be provided")
        else:
            self._book_id = book_cue
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
        book = from_iterable(author.top_books, lambda b: b.title.casefold() == title.casefold())
        if not book:
            # let's be even less strict...
            book = from_iterable(
                author.top_books, lambda b: title.casefold() in b.title.casefold())
        return book

    @classmethod
    def book_id_from_data(cls, title: str, author: str,
                          authors_data: Iterable[Author]) -> str | None:
        """Derive Goodreads book ID from provided authors data.

        Args:
            title: book's title
            author: book author's full name or Goodreads author ID
            authors_data: data as read from JSON saved by dump_authors()

        Returns:
            derived book ID or None
        """
        if is_goodreads_id(author):
            author = from_iterable(authors_data, lambda a: a.id == author)
        else:
            author = from_iterable(
                authors_data, lambda a: a.name.casefold() == author.casefold())
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
            author: book author's full name or Goodreads author ID

        Returns:
            fetched book ID or None
        """
        scraper = AuthorScraper(author)
        author = scraper.scrape()
        book = cls._find_book_in_author_books(author, title)
        if not book:
            author = scraper.scrape(extended_top_books=True)
            book = cls._find_book_in_author_books(author, title)
            if not book:
                return None
        return book.id

    @classmethod
    def find_book_id(cls, title: str, author: str,
                     authors_data: Iterable[Author] | None = None) -> str | None:
        """Find Goodreads book ID based on provided arguments.

        Performs the look-up on ``authors_data`` if provided. Otherwise, scrapes Goodreads author
        page for the ID.

        Args:
            title: book's title
            author: book's author or author ID
            authors_data: iterable of Author data objects or None

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
        return sanitize_output(tag.text)

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
        id_ = url2id(url)
        if not id_:
            raise ParsingError(f"Could not extract Goodreads ID from '{url}'")
        return _Contributor(id_, a_tag.find("span", class_="ContributorLink__role") is not None)

    @classmethod
    def _parse_authors_line(cls, soup: BeautifulSoup) -> List[SimpleAuthor]:
        contributor_div = soup.find("div", class_="ContributorLinksList")
        if contributor_div is None:
            raise ParsingError("No 'div' tag with contributors data")
        contributor_tags = contributor_div.find_all("a")
        if not contributor_tags:
            raise ParsingError("No contributor data 'a' tags")
        contributors = [cls._parse_contributor(tag) for tag in contributor_tags]
        authors = [c for c in contributors if not c.has_role]
        if not authors:
            return [SimpleAuthorScraper(contributors[0].author_id).scrape()]
        return [SimpleAuthorScraper(a.author_id).scrape() for a in authors]

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
        id_ = url2id(a.attrs.get("href"))
        if not id_:
            raise ParsingError(f"Could not extract Goodreads ID from '{a.attrs.get('href')}'")
        return id_

    # response is so slow it doesn't need throttling
    # besides, _parse_authors_line() calls is already throttled
    def _parse_book_page(self) -> Tuple[_ScriptTagData, str, List[SimpleAuthor], str]:
        soup = getsoup(self._url)
        script_data = self._parse_meta_script_tag(soup)
        title = self._parse_title(soup)
        authors = self._parse_authors_line(soup)
        series_id = self._parse_series_id(soup)
        if script_data.first_publication is None:
            script_data.first_publication = self._parse_first_publication(soup)
        return script_data, title, authors, series_id

    @staticmethod
    def _validate_series_div(div: Tag) -> bool:
        h3 = div.find("h3")
        if h3 is None:
            return False
        if " " not in h3.text:
            return False
        first, second = h3.text.split(maxsplit=1)
        if first.upper() != "BOOK":
            return False
        try:
            float(second)
        except ValueError:
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
    def _parse_shelves_page(self) -> Tuple[OrderedDict[int, str], int]:
        soup = getsoup(self._shelves_url)
        lc_tag = soup.find("div", class_="leftContainer")
        if lc_tag is None:
            raise ParsingError("No 'leftContainer' tag with total shelves created data")
        span_tag = lc_tag.find("span", class_="smallText")
        if span_tag is None:
            raise ParsingError("No 'span' tag with total shelves created data")
        *_, text = span_tag.text.split()
        total_shelves_created = extract_int(text)

        shelf_tags = soup.find_all("div", class_="shelfStat")
        shelves = OrderedDict()
        for tag in shelf_tags:
            name = tag.find("a").text
            shelvings_tag = tag.find(lambda t: t.name == "div" and "people" in t.text)
            if shelvings_tag is None:
                continue
            shelvings = extract_int(shelvings_tag.text)
            shelves[shelvings] = name
        return shelves, total_shelves_created

    @throttled(THROTTLING_DELAY)
    def _parse_editions_page(
            self, page: int,
            editions: DefaultDict[str, Set[str]] | None = None
    ) -> Tuple[DefaultDict[str, Set[str]], int, int | None]:
        soup = getsoup(self._editions_url(page))
        total_editions = None
        if page == 1:
            wi_tag = soup.find("div", class_="left workInfo")
            if wi_tag is None:
                raise ParsingError("No 'left workInfo' tag with total editions data")
            span_tag = wi_tag.find("span", class_="smallText")
            if span_tag is None:
                raise ParsingError("No 'span' tag with total editions data")
            *_, text = span_tag.text.split()
            total_editions = extract_int(text)

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
            if not lang or not title or name2langcode(lang) is None:
                continue
            editions[lang].add(title)

        return editions, count, total_editions

    # capped at 10 pages as, for older books, there are cases of more almost 600 pages (!)
    def _scrape_editions(self) -> Tuple[OrderedDict[str, List[str]], int]:
        counter = itertools.count(1)
        editions, total_editions, next_page = None, None, True
        for i in counter:
            editions, editions_count, total = self._parse_editions_page(i, editions)
            if i == 1:
                total_editions = total
            if editions_count < 100 or i > 10:
                break
        ordered = OrderedDict(sorted([(name2langcode(lang), sorted(titles))
                              for lang, titles in editions.items()]))
        if total_editions is None:
            raise ParsingError("Failed to parse total editions data")
        return ordered, total_editions

    def _scrape_book(self) -> DetailedBook:
        script_data, title, authors, self._series_id = self._parse_book_page()
        self._work_id = script_data.work_id
        self._set_secondary_urls()
        series = self._parse_series_page() if self.series_id else None
        shelves, total_shelves = self._parse_shelves_page()
        editions, total_editions = self._scrape_editions()
        stats = BookStats(
            ratings=script_data.ratings,
            reviews=script_data.reviews,
            total_reviews=script_data.total_reviews,
            top_shelves=shelves,
            total_shelves=total_shelves,
            editions=editions,
            total_editions=total_editions,
        )
        return DetailedBook(
            title=title,
            original_title=script_data.original_title,
            book_id=self.book_id,
            work_id=self.work_id,
            authors=authors,
            first_publication=script_data.first_publication,
            series=series,
            details=script_data.details,
            stats=stats,
        )

    @timed("book scraping", precision=2)
    def scrape(self) -> DetailedBook:
        """Scrape detailed book data from Goodreads.
        """
        try:
            book = self._scrape_book()
        except HTTPError as e:
            _log.warning(f"Goodreads had a hiccup ({e}). Retrying with backoff "
                         "(60 seconds max)...")
            return self.scrape_with_backoff()
        except Timeout:
            _log.warning("Goodreads doesn't play nice. Timeout exceeded. Retrying with backoff "
                         "(60 seconds max)...")
            return self.scrape_with_backoff()

        return book

    @timed("book scraping (with backoff)", precision=2)
    @backoff.on_exception(backoff.expo, (Timeout, HTTPError), max_time=60)
    def scrape_with_backoff(self) -> DetailedBook:
        """Scrape detailed book data from Goodreads with (one minute max) backoff on timeout.
        """
        return self._scrape_book()


PROPER_AUTHORS = {
    "Mary Shelley": "Mary Wollstonecraft Shelley",
    "Stanislaw Lem": "Stanisław Lem",
}
PROPER_TITLES = {
    "Galapagos": "Galápagos",
    "How to Live Safely in a Sci-Fi Universe": "How to Live Safely in a Science Fictional Universe",
    "Planet of the Apes (aka Monkey Planet)": "Planet of the Apes",
    "Readme": "Reamde",
    "The Island of Doctor Moreau": "The Island of Dr. Moreau",
    "The Long Way to a Small Angry Planet": "The Long Way to a Small, Angry Planet",
    "The Real Story": "The Gap Into Conflict",
    "The Songs of Distant Earth": "Songs of Distant Earth",
    "The Word for World is Forest": "The Word for World Is Forest",
    "Restaurant at the End of the Universe": "The Restaurant at the End of the Universe",
}


def sanitize_output(text: str) -> str:
    """Sanitize scraper's output text.
    """
    text = text.strip()
    text = re.sub(r"\s{2,}", " ", text)
    text = text.replace("’", "'")
    return text


def sanitize_input(text: str) -> str:
    """Sanitize scraper's input text.
    """
    proper_authors = {k.casefold(): v for k, v in PROPER_AUTHORS.items()}
    proper_titles = {k.casefold(): v for k, v in PROPER_TITLES.items()}
    text = text.strip()
    if text.casefold() in proper_authors:
        return proper_authors[text.casefold()]
    if text.casefold() in proper_titles:
        return proper_titles[text.casefold()]
    return sanitize_output(text)
