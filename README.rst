====================
RethinkDB-Extraction
====================

An extremely simple example of using `RethinkDB <http://www.rethinkdb.com/>`_ in
Python, along with the `extraction <https://github.com/lethain/extraction>`_ module
to create a database of crawled HTML pages and the extracted data.


Usage
=====

There isn't really very much here beyond syntax examples, but to run those, first install
RethinkDB (`maybe using these very easy instructions <https://github.com/RyanAmos/rethinkdb-vagrant/>`_),
and then do this::

    git clone https://github.com/lethain/rethinkdb-extraction.git
    cd rethinkdb-extraction
    virtualenv .
    . ./bin/activate
    pip install -r requirements.txt
    python rethinkgdb_extraction/crawl.py

That will crawl a few pages, and load them into a local RethinkDB instance.
