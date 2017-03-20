import requests
import sys
import time
import csv
import os
import sqlite3
from dotenv import Dotenv
from xml.etree import ElementTree
# from oauth2client.service_account import ServiceAccountCredentials


class CrawlerError(Exception):
    def __init__(self, message):
        super(CrawlerError, self).__init__(message)


class Crawler:

    results = []
    urls = []
    path = path = os.path.dirname(os.path.realpath(__file__))
    headers = {'User-Agent':
               'Mozilla/5.0 (compatible; Cachoid; +https://www.cachoid.com/)'}

    def warm_url(self, url):
        try:
            delay = float(os.environ.get('DELAY', 500))
            time.sleep(delay / 1000.0)
            warmer = requests.get(url, headers=self.headers)
            result = [url.encode("utf-8"), warmer.status_code,
                      (warmer.elapsed.microseconds / 1000), warmer.is_redirect]
            self.results.append(result)
        except:
            print 'ERROR - Could not crawl %s' % url
            pass

    def sitemap_crawler(self, sitemap, limit, offset):
        limit = limit if (limit >= 0 and limit <= 10000) else 1000
        req = requests.get(sitemap, headers=self.headers, stream=True)
        req.raw.decode_content = True
        if req.status_code != 200:
            raise CrawlerError('ERROR - Invalid Sitemap.XML file!')
        try:
            total = 0
            count = 0
            for event, elem in ElementTree.iterparse(req.raw):
                if elem.tag == ('{http://www.sitemaps.org/schemas/'
                                'sitemap/0.9}loc'):
                    count = count + 1
                    if count > offset:
                        if total < limit or limit == 0:
                            self.urls.append(elem.text)
                            total = total + 1
                        else:
                            break
                    else:
                        continue
            return self.urls
        except Exception as e:
            raise CrawlerError('ERROR - Could not parse the Sitemap.XML file!')

    def google_crawler(self, gaid, limit):
        limit = limit if (limit > 0 and limit < 1000) else 10
        domain = os.environ.get('DOMAIN', False)
        protocol = os.environ.get('PROTOCOL', 'https')
        protocol = 'https' if protocol == 'https' else 'http'
        scope = 'https://www.googleapis.com/auth/analytics.readonly'
        token = ServiceAccountCredentials.from_json_keyfile_name(
                ('%s/key.json' % self.path),
                scope).get_access_token().access_token
        url = ('https://www.googleapis.com/analytics/v3/data/ga?'
               'ids=%s&start-date=3daysAgo&end-date=yesterday&metrics'
               '=ga:entrances&dimensions=ga:landingPagePath'
               '&sort=-ga:entrances&max-results=%i&access_token=%s'
               ) % (gaid, limit, token)
        req = requests.get(url, headers=self.headers)
        rows = req.json().get('rows')
        if rows:
            if domain:
                for row in rows:
                    url = row[0]
                    if url[:len(domain)] == domain:
                        url = '%s://%s' % (protocol, row[0])
                    else:
                        url = '%s://%s%s' % (protocol, domain, row[0])
                    self.urls.append(url)
                return self.urls
            else:
                raise CrawlerError('ERROR - Invalid DOMAIN!')
        else:
            raise CrawlerError('ERROR - Could not successfully autheticate'
                               ' to Google!')
