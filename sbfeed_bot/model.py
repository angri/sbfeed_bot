import sqlite3
import functools
import time
import logging
import threading

from sbfeed_bot import exceptions


class SbFeedModel:
    UPDATE_EVERY = 10

    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.connections = {}
        self.logger = logging.getLogger("sbfeed.model")

    def _get_connection(self):
        tid = threading.get_ident()
        if tid not in self.connections:
            conn = sqlite3.connect(self.dbfile, isolation_level=None)
            conn.execute("PRAGMA foreign_keys = ON;")
            self.connections[tid] = conn
        return self.connections[tid]

    def create_db(self):
        conn = self._get_connection()
        conn.execute("""
            CREATE TABLE feed (
                slug varchar(255),
                last_modified integer,
                last_tried_to_fetch integer,
                PRIMARY KEY (slug)
            );
        """)
        conn.execute("""
            CREATE TABLE feed_item (
                feed varchar(255),
                title text,
                link varchar(255),
                text text,
                pubdate integer,
                PRIMARY KEY (feed, pubdate),
                FOREIGN KEY (feed) REFERENCES feed (slug) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE TABLE subscription (
                chat_id integer,
                feed varchar(255),
                last_notified integer,
                PRIMARY KEY (feed, chat_id),
                FOREIGN KEY (feed) REFERENCES feed (slug) ON DELETE RESTRICT
            );
        """)
        conn.execute("""
            CREATE INDEX idx_subscription_chat_id ON subscription(chat_id);
        """)

    def transaction(*, readonly):
        def wrapper(meth):
            @functools.wraps(meth)
            def wrapped(self, *args, **kwargs):
                cursor = self._get_connection().cursor()
                self.logger.debug("trying to %s(*%r, **%r)",
                                  meth.__name__, args, kwargs)
                cursor.execute("BEGIN TRANSACTION")
                try:
                    result = meth(self, cursor, *args, **kwargs)
                except Exception as exc:
                    self.logger.info("%s() failed with: %s",
                                     meth.__name__, exc)
                    cursor.execute("ROLLBACK")
                    raise
                else:
                    self.logger.debug("%s(*%r, **%r) -> %r",
                                      meth.__name__, args, kwargs, result)
                    cursor.execute("COMMIT")
                    return result
            return wrapped
        return wrapper

    @transaction(readonly=True)
    def check_feed_is_known(self, cursor, feed):
        cursor.execute("SELECT 1 FROM feed WHERE slug = ?", [feed])
        return bool(cursor.fetchone())

    @transaction(readonly=False)
    def init_feed(self, cursor, feed):
        cursor.execute("SELECT 1 FROM feed WHERE slug = ?", [feed])
        if cursor.fetchone():
            raise exceptions.AlreadyExistsError()
        cursor.execute(
            "INSERT INTO feed (slug, last_modified, last_tried_to_fetch) "
            "VALUES (?, NULL, NULL)",
            [feed]
        )

    @transaction(readonly=False)
    def store_item(self, cursor, feed, item_title, item_link, item_text,
                   item_pub_date):
        cursor.execute("SELECT 1 FROM feed WHERE slug = ?", [feed])
        if not cursor.fetchone():
            raise exceptions.NotExistError()
        cursor.execute(
            "INSERT INTO feed_item (feed, title, link, text, pubdate) "
            "VALUES (?, ?, ?, ?, ?)",
            [feed, item_title, item_link, item_text, item_pub_date]
        )

    @transaction(readonly=False)
    def mark_feed_as_processed(self, cursor, feed, *, last_modified):
        now = time.time()
        if last_modified is None:
            cursor.execute(
                "UPDATE feed SET last_tried_to_fetch = ? "
                "WHERE slug = ?",
                [now, feed]
            )
        else:
            cursor.execute(
                "UPDATE feed SET last_modified = ?, last_tried_to_fetch = ? "
                "WHERE slug = ? ",
                [last_modified, now, feed]
            )
        if not cursor.rowcount:
            raise exceptions.NotExistError()

    @transaction(readonly=False)
    def subscribe(self, cursor, chat_id, feed):
        cursor.execute("SELECT 1 FROM subscription WHERE "
                       "chat_id = ? AND feed = ?", [chat_id, feed])
        if cursor.fetchone():
            raise exceptions.AlreadyExistsError()
        cursor.execute("SELECT 1 FROM feed WHERE slug = ?", [feed])
        if not cursor.fetchone():
            raise exceptions.NotExistError()
        cursor.execute(
            "INSERT INTO subscription (chat_id, feed, last_notified) "
            "VALUES (?, ?, ?)",
            [chat_id, feed, int(time.time())]
        )

    @transaction(readonly=True)
    def list_subscriptions(self, cursor, chat_id):
        cursor.execute("SELECT feed FROM subscription WHERE "
                       "chat_id = ? ORDER BY feed", [chat_id])
        feeds = [row[0] for row in cursor.fetchall()]
        return feeds

    @transaction(readonly=False)
    def unsubscribe(self, cursor, chat_id, feed):
        cursor.execute("SELECT 1 FROM subscription WHERE "
                       "chat_id = ? AND feed = ?", [chat_id, feed])
        if not cursor.fetchone():
            raise exceptions.NotExistError()
        cursor.execute("DELETE FROM subscription WHERE "
                       "chat_id = ? AND feed = ?", [chat_id, feed])

    @transaction(readonly=False)
    def unsubscribe_all(self, cursor, chat_id):
        cursor.execute("SELECT 1 FROM subscription WHERE "
                       "chat_id = ? LIMIT 1", [chat_id])
        if not cursor.fetchone():
            raise exceptions.NotExistError()
        cursor.execute("DELETE FROM subscription WHERE "
                       "chat_id = ?", [chat_id])

    @transaction(readonly=True)
    def get_fetches_needed(self, cursor):
        cursor.execute(
            "SELECT slug, last_modified, last_tried_to_fetch "
            "FROM feed "
            "WHERE last_tried_to_fetch IS NULL OR last_tried_to_fetch + ? < ?",
            [self.UPDATE_EVERY, time.time()]
        )
        return cursor.fetchall()

    @transaction(readonly=True)
    def check_notifications_needed(self, cursor):
        cursor.execute("""
            SELECT su.chat_id, fi.feed, fi.title, fi.link, fi.text, fi.pubdate
            FROM subscription AS su
            JOIN feed_item AS fi ON (fi.feed = su.feed)
            WHERE su.last_notified < fi.pubdate
            ORDER BY fi.feed, fi.pubdate
            LIMIT 10
        """)
        return [{'chat_id': row[0],
                 'feed': row[1],
                 'item_title': row[2],
                 'item_link': row[3],
                 'item_text': row[4],
                 'item_pub_date': row[5]}
                for row in cursor.fetchall()]

    @transaction(readonly=False)
    def mark_notification_as_sent(self, cursor, chat_id, feed, item_pub_date):
        cursor.execute("UPDATE subscription SET last_notified = ? "
                       "WHERE feed = ? AND chat_id = ? AND last_notified < ?",
                       [item_pub_date, feed, chat_id, item_pub_date])
        if not cursor.rowcount:
            raise exceptions.NotExistError()

    del transaction
