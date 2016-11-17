import re
import logging

from telegram.ext import CommandHandler, Updater, MessageHandler, Filters

from sbfeed_bot import exceptions


class SbFeedBot:
    NOTIFICATION_TMPL = """\
{title}

{text}

{link}\
"""

    def __init__(self, token, model, feeder):
        self.logger = logging.getLogger('sbfeed.bot')
        self.model = model
        self.feeder = feeder
        self.token = token
        self.slug_re = re.compile(r'^[-a-zA-Z0-9_]{,50}\Z')
        self.updater = Updater(token=token)
        self.dispatcher = self.updater.dispatcher

        self.dispatcher.add_handler(
            CommandHandler('subscribe', self._handle_subscribe, pass_args=True)
        )
        self.dispatcher.add_handler(
            CommandHandler('unsubscribe',
                           self._handle_unsubscribe, pass_args=True)
        )
        self.dispatcher.add_handler(
            CommandHandler('unsubscribe_all', self._handle_unsubscribe_all)
        )
        self.dispatcher.add_handler(
            CommandHandler('list_subscriptions',
                           self._handle_list_subscriptions)
        )
        self.dispatcher.add_handler(
            CommandHandler('start', self._handle_start)
        )
        self.dispatcher.add_handler(
            MessageHandler(Filters.command, self._handle_unknown)
        )

    def start(self):
        self.logger.info('starting')
        self.updater.start_polling()

    def stop(self):
        self.logger.info('stopping')
        self.updater.stop()

    def _handle_start(self, bot, update):
        self.logger.info('got /start')
        bot.sendMessage(chat_id=update.message.chat_id,
                        text="I can notify you on songbook changes. "
                             "The following commands are supported:\n\n"
                             " /start - this message\n"
                             " /subscribe <gig-slug> - start receiving "
                             "notifications on gig\n"
                             " /list_subscriptions - show what you are "
                             "subscribed to\n"
                             " /unsubscribe <gig-slug> - remove subscription "
                             "for a gig\n"
                             " /unsubscribe_all - remove all subscriptions")

    def _handle_subscribe(self, bot, update, args):
        self.logger.info('got /subscribe %r', args)
        if not args or len(args) != 1:
            bot.sendMessage(chat_id=update.message.chat_id,
                            text='syntax: /subscribe <slug>')
            return
        [slug] = args
        if not self.slug_re.match(slug):
            bot.sendMessage(chat_id=update.message.chat_id, text='broken slug')
            return
        if not self.model.check_feed_is_known(slug):
            try:
                self.feeder.fetch(slug, not_before=None)
                self.model.init_feed(slug)
            except exceptions.NotExistError:
                bot.sendMessage(chat_id=update.message.chat_id,
                                text='failed to fetch gig, does it exist?')
                return
            except Exception as exc:
                self.logger.exception('failed to fetch gig %r', slug)
                bot.sendMessage(chat_id=update.message.chat_id,
                                text='failed to fetch gig: %s' % (exc, ))
                return
        self.model.subscribe(chat_id=update.message.chat_id, feed=slug)
        bot.sendMessage(
            chat_id=update.message.chat_id,
            text='congratulations! you subscribed to %s' % (slug, )
        )

    def _handle_unsubscribe(self, bot, update, args):
        self.logger.info('got /unsubscribe %r', args)
        if not args or len(args) != 1:
            bot.sendMessage(chat_id=update.message.chat_id,
                            text='syntax: /unsubscribe <slug>')
            return
        [slug] = args
        if not self.slug_re.match(slug):
            bot.sendMessage(chat_id=update.message.chat_id, text='broken slug')
            return
        try:
            self.model.unsubscribe(update.message.chat_id, slug)
        except exceptions.NotExistError:
            bot.sendMessage(chat_id=update.message.chat_id,
                            text="you were not subscribed to %s" % (slug, ))
        else:
            bot.sendMessage(chat_id=update.message.chat_id,
                            text="you were unsubscribed from %s" % (slug, ))

    def _handle_unknown(self, bot, update):
        self.logger.info('got unknown command/message')
        bot.sendMessage(chat_id=update.message.chat_id,
                        text="command is not supported. try /start")

    def _handle_list_subscriptions(self, bot, update):
        self.logger.info('got list_subscriptions')
        slugs = self.model.list_subscriptions(update.message.chat_id)
        if not slugs:
            bot.sendMessage(chat_id=update.message.chat_id,
                            text="you're not subscribed to gigs")
            return
        bot.sendMessage(chat_id=update.message.chat_id,
                        text="you are subscribed to these gigs:\n%s" % (
                            '\n'.join(' * %s' % (slug, ) for slug in slugs),
                        ))

    def _handle_unsubscribe_all(self, bot, update):
        self.logger.info('got unsubscribe_all')
        try:
            self.model.unsubscribe_all(update.message.chat_id)
        except exceptions.NotExistError:
            bot.sendMessage(chat_id=update.message.chat_id,
                            text="you were not subscribed to anything")
        else:
            bot.sendMessage(chat_id=update.message.chat_id,
                            text="you were unsubscribed from everything")

    def notify(self, chat_id, feed,
               item_title, item_link, item_text, item_pubdate):
        text = self.NOTIFICATION_TMPL.format(
            title=item_title, link=item_link, text=item_text, slug=feed,
        )
        self.dispatcher.bot.send_message(chat_id=chat_id, text=text,
                                         disable_web_page_preview=True)
