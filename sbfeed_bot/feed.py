import time
import logging
import calendar
from urllib.parse import quote as urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from xml.etree.cElementTree import parse as etree_parse

from sbfeed_bot import exceptions


class SbFeedFeeder:
    DATETIME_FMT = "%a, %d %b %Y %H:%M:%S %z"

    def __init__(self, songbook_url):
        self.url_tmpl = songbook_url + '/comments/feeds/%s'
        self.logger = logging.getLogger('sbfeed.feeder')

    def _atom_date(self, string):
        return int(calendar.timegm(time.strptime(string, self.DATETIME_FMT)))

    def fetch(self, feed, not_before):
        url = self.url_tmpl % (urlencode(feed), )
        request = Request(url)
        if not_before:
            request.add_header('If-Modified-Since',
                               time.strftime("%a, %d %b %Y %H:%M:%S GMT",
                                             time.gmtime(not_before)))
        self.logger.info('fetching %r (%r)', url, request.headers)
        try:
            response = urlopen(request)
        except HTTPError as exc:
            if exc.code == 304:
                # not modified
                return not_before, []
            elif exc.code == 404:
                raise exceptions.NotExistError()
            raise
        tree = etree_parse(response)
        build_date = self._atom_date(tree.find('./channel/lastBuildDate').text)
        result = []
        for item in tree.findall('./channel/item')[::-1]:
            pubdate = self._atom_date(item.find('pubDate').text)
            if not_before and pubdate <= not_before:
                continue
            itemd = {'pubdate': pubdate}
            itemd['title'] = item.find('title').text
            itemd['link'] = item.find('link').text
            text = item.find('description').text
            if text.startswith('<pre>') and text.endswith('</pre>'):
                text = text[len('<pre>'):-len('</pre>')]
            itemd['text'] = text.strip()
            result.append(itemd)
        return build_date, result
