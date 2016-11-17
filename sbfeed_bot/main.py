import argparse
import logging
import time
import threading
import sys

from sbfeed_bot.model import SbFeedModel
from sbfeed_bot.feed import SbFeedFeeder
from sbfeed_bot.bot import SbFeedBot


def setup_logging(logfile, level):
    handler = logging.StreamHandler(logfile)
    logging.basicConfig(
        level=level, handlers=[handler],
        format='%(asctime)-15s %(name)s %(levelname)s %(message)s'
    )
    logging.captureWarnings(capture=True)


def peridodic_fetcher(feeder, model):
    logger = logging.getLogger('sbfeed.fetcher')
    while True:
        for slug, last_fetched, last_tried in model.get_fetches_needed():
            now = time.time()
            logger.info('need to fetch %r: last fetched %ds ago '
                        'and last tried %ds ago',
                        slug, now - (last_fetched or now + 1),
                        now - (last_tried or now + 1))
            try:
                last_modified, items = feeder.fetch(slug, last_fetched)
            except Exception:
                logger.exception('failed to fetch')
                model.mark_feed_as_processed(slug, last_modified=None)
                time.sleep(10)
                continue

            for item in items:
                logger.info('feed %r: new item %r, see %r',
                            slug, item['title'], item['link'])
                try:
                    model.store_item(slug, item['title'], item['link'],
                                     item['text'], item['pubdate'])
                except Exception:
                    logger.exception('failed to store item %r', item)
            model.mark_feed_as_processed(slug, last_modified=last_modified)

        time.sleep(10)


def periodic_notifier(model, bot):
    logger = logging.getLogger('sbfeed.notifier')
    while True:
        for item in model.check_notifications_needed():
            logger.info('need to notify %d about feed %r item %r, see %r',
                        item['chat_id'], item['feed'], item['item_title'],
                        item['item_link'])
            try:
                bot.notify(item['chat_id'], item['feed'], item['item_title'],
                           item['item_link'], item['item_text'],
                           item['item_pub_date'])
                logger.info('notified successfully')
            except Exception:
                logger.exception('failed to notify')
            finally:
                model.mark_notification_as_sent(item['chat_id'], item['feed'],
                                                item['item_pub_date'])
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log-file", dest='logfile', metavar='FILE',
                        default=sys.stderr, type=argparse.FileType('a'),
                        help="write logs to FILE")
    parser.add_argument("-t", "--telegram-token", dest='token',
                        required=True, help="telegram bot api token")
    parser.add_argument("-d", "--database", metavar='FILE', required=True,
                        type=argparse.FileType('a'),
                        help="sqlite3 database file")
    parser.add_argument("-s", "--songbook-url",
                        default='https://songbook.angri.ru',
                        help='songbook url, with no trailing slash')
    parser.add_argument('--create-db', action='store_true',
                        help='only initialize database structure and exit')
    args = parser.parse_args()

    setup_logging(args.logfile, level=logging.INFO)

    args.database.close()
    model = SbFeedModel(args.database.name)
    if args.create_db:
        model.create_db()
        sys.exit(0)

    feeder = SbFeedFeeder(args.songbook_url)
    bot = SbFeedBot(args.token, model, feeder)

    logger = logging.getLogger('sbfeed.main')
    workers = [
        threading.Thread(
            name='fetcher', target=peridodic_fetcher, args=(feeder, model)
        ),
        threading.Thread(
            name='notifier', target=periodic_notifier, args=(model, bot)
        ),
    ]
    for worker in workers:
        logger.info('spawning %r', (worker, ))
        worker.daemon = True
        worker.start()
    logger.info('starting bot updater')
    bot.start()
    try:
        while True:
            time.sleep(1)
            for worker in workers:
                if not worker.is_alive():
                    logger.critical('%r died, exiting', (worker, ))
                    sys.exit(1)
    except KeyboardInterrupt:
        return
    finally:
        bot.stop()


if __name__ == '__main__':
    main()
