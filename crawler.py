import requests
import sys
import time
import csv
import os
import zlib
import sqlite3
import configparser
from dotenv import Dotenv
from xml.etree import ElementTree
from warmer_utils import isSQLite3
# from oauth2client.service_account import ServiceAccountCredentials


cachoid_conf_file = "/etc/cachoid/config"
config = configparser.ConfigParser()
config.readfp(open(cachoid_conf_file))

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

    def getSitemapTxtFomSQLite(self, fqdn_id, sitemap_db):
	if not isSQLite3(db_name):
		syslog.syslog("Sitemap DB does not exist. Exiting...")
		raise CrawlerError('ERROR - Invalid SQLite DB')
        try:
		con = sqlite.connect(sitemap_db) 
		con.row_factory = sqlite3.Row
        except:
		raise CrawlerError('ERROR - Unable to connect to SQLite DB')
	c = con.cursor()
	cursor = c.execute("select sitemap from sitemap where fqdn_id=? limit 1", [fqdn_id])
	if cursor.rowcount < 1:
		raise CrawlerError('ERROR - not sitemap exists for ' + fqdn_id)
	try:
		sitemap_txt_gz = cursor.fetchone()['sitemap'];	
	except:
		raise CrawlerError('ERROR - unable to pull sitemap blog for ' + fqdn_id)
	try:
		sitemap_txt = zlib.decompress(sitemap_txt_gz)
	except:
		raise CrawlerError('ERROR - unable to decompress compress sitemap blog')
	return sitemap_txt

    def sqlite_sitemap_crawler(self, fqdn_id, limit, offset):
        limit = limit if (limit >= 0 and limit <= 10000) else 1000
	db_name = config['node']['sitemap_db']
	sitemap_txt = self.getSitemapTxtFromSQLite(db_name)
	try:
		total = 0
		count = 0
		for event, elem in ElementTree.iterparse(sitemap_txt):	
			if elem.tag == ('{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
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
