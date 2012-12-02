"""
Tools for retrieving data with requests, extracting it with extraction,
and storing it all in RethinkDB.
"""
import logging
import requests
import feedparser
from extraction import Extractor
from rethinkdb import r
from rethinkdb.net import ExecutionError


class RSSLinkExtractor(object):
    """
    Extract links from an RSS feed. Not very sophisticated. :)

        >>> x = RSSLinkExtractor()
        >>> links = x.links("http://lethain.com/feeds/")
        >>> links[:1]
        ["http://lethain.com/doing-it-harder-and-hero-programming/"]

    """
    def links(self, url):
        "Extract links from an RSS feed at a given URL."
        feed = feedparser.parse(url)
        print feed
        print dir(feed)

class Crawler(object):
    "Crawlers pass data retrieved by requests to extraction and stoer it in RethinkDB."
    db = "crawl"
    tables = ['pages', 'html']

    def __init__(self, client=None, host='localhost', port=28015):
        "Initialize a Crawler."
        self._client = client
        self.host = host
        self.port = port
        self.extractor = Extractor()
        self.log = logging.getLogger(self.__class__.__name__)

    def crawl(self, url):
        # print r.table('tv_shows').insert({ 'name': 'Star Trek TNG' }).run()
        pass

    def ensure_db_and_tables(self):
        "Create database and tables if they don't exist."
        try:
            self.client.db_create(self.db).run()
        except ExecutionError, ee:
            self.log.info("Database already existed %s: %s", self.db, ee)

        for table in self.tables:
            try:
                self.client.db(self.db).table_create(table).run()
            except ExecutionError, ee:
                self.log.info("Table already existed %s: %s", table, ee)

    @property
    def client(self):
        "Retrieve crawler's RethinkDB client or create a new one."
        if self._client is None:
            self._client = r
            self._client.connect(self.host, self.port)
        return self._client


feed_url = "http://lethain.com/feeds/"
links = RSSLinkExtractor().links(feed_url)

print links

c = Crawler()
c.ensure_db_and_tables()

