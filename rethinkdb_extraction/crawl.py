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
        return [x.link for x in feed.entries]


class Crawler(object):
    "Crawlers pass data retrieved by requests to extraction and stoer it in RethinkDB."
    log_content_len = 50
    db = "crawl"
    page_table = "pages"
    html_table = "html"
    tables = [page_table, html_table]

    def __init__(self, client=None, host='localhost', port=28015, overwrite=False):
        "Initialize a Crawler."
        self._client = client
        self.host = host
        self.port = port
        self.overwrite = overwrite
        self.extractor = Extractor()
        self.log = logging.getLogger(self.__class__.__name__)

    def retrieve(self, url, force_crawl=False):
        """
        Retrieve a URL's contents from RethinkDB if possible,
        or crawl the source URL directly if not.

        Specify force_crawl as True to bypass the RethinkDB
        cache entirely.
        """
        contents = None
        if not force_crawl:
            try:
                found = self.client.db(self.db).table(self.html_table).get(url).run()
                if found and 'contents' in found:
                    self.log.info("%s:Crawler.retrieve found contents in RethinkDB.", url)
                    contents = found['contents']
            except ExecutionError, ee:
                self.log.info("%s:Crawler.retrieve contents missing from RethinkDB: %s", url, ee)

        if not contents:
            self.log.info("%s:Crawler.retrieve crawling URL", url)
            contents = requests.get(url).text

        self.log.info("%s:Crawler.retrieve contents '%s'.", url, contents[:self.log_content_len])
        return contents

    def extract(self, url, contents):
        "Extract data from a crawl."
        metadata = self.extractor.extract(contents, source_url=url)
        self.log.info("%s:Crawler.extract metadata '%s'.", url, metadata.title)
        return metadata

    def store(self, url, metadata, contents):
        "Store retrieved data into RethinkDB."
        self.log.info("%s:Crawler.store storing %s and '%s'.", url, metadata.title, contents[:self.log_content_len])
        success = self.client.db(self.db).table(self.html_table).insert({'id':url, 'contents':contents}, self.overwrite).run()
        # the code will error here on the second successful run because it will detect that
        # it's attempting to create a duplicate primary key, to change this behavior, instantiate
        # the crawler with overwrite=True:
        #
        #    crawler = Crawler(overwrite=True)
        #
        # which will allow it to overwrite
        
        if 'first_error' in success :
            raise Exception("%s:Crawler.store had %s errors storing data, first error was: %s" % (url, success['errors'], success['first_error']))

        success = self.client.db(self.db).table(self.page_table).insert({'id': url, 'metadata': {
                    'urls': metadata.urls,
                    'titles': metadata.titles,
                    'images': metadata.images,
                    'descriptions': metadata.descriptions,
                    }}, self.overwrite).run()

        if 'first_error' in success :
            raise Exception("%s:Crawler.store had %s errors storing data, first error was: %s" % (url, success['errors'], success['first_error']))

    def crawl(self, url, contents=None):
        """
        Retrieve a URL, save it's body into RethinkDB,
        extract its metadata, save that into RethinKDB
        as well.
        """
        self.log.error("%s:Crawler.crawl starting crawl", url)
        self.log.info("%s:Crawler.crawl starting crawl", url)
        contents = contents or self.retrieve(url)
        metadata = self.extract(url, contents)
        self.store(url, metadata, contents)
        if metadata and contents:        
            self.log.info("%s:Crawler.crawl ending crawl, appears successful.", url)
        else:
            self.log.warning("%s:Crawler.crawl ending crawl, appears to have failed.", url)

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


def main():
    "Extract some links and such."
    feed_url = "http://lethain.com/feeds/"
    link_extractor = RSSLinkExtractor()
    crawler = Crawler(overwrite=False)
    crawler = Crawler(overwrite=True)

    # setup loggers to capture output
    log = logging.getLogger('Crawler')
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    log.addHandler(ch)

    # crawl the data, load it into rethinkdb
    crawler.ensure_db_and_tables()
    links = link_extractor.links(feed_url)
    for link in links:
        crawler.crawl(link)

    # retrieve the crawled data from rethinkdb
    pass


if __name__ == "__main__":
    main()

