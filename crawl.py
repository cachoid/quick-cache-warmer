import requests
import sys
import time
import csv
import os
import argparse
import threading
import Queue
from dotenv import Dotenv
from crawler import Crawler, CrawlerError
from emailer import Emailer, EmailerError
# from oauth2client.service_account import ServiceAccountCredentials


if __name__ == "__main__":

    # get arguments for ID and count
    parser = argparse.ArgumentParser(
        description='Warm the cache of highly trafficed pages.')
    parser.add_argument(
        '-sdb', '--sitemapdb', metavar='', type=str, required=True,
        help='The location of the sitemap SQLite DB.')
    parser.add_argument(
        '-sid', '--sitemapid', metavar='', type=str, required=True,
        help='The FQDN ID of the sitemap entry in SQLite DB.')

    req_parser = parser.add_argument_group('required arguments')
    req_parser.add_argument(
        '-c', '--count', metavar='', type=int, required=True,
        help='The number of pages to warm.')
    args = parser.parse_args()

    # main variables
    QUEUE = Queue.Queue(maxsize=0)
    PATH = os.path.dirname(os.path.realpath(__file__))

    # check if warming sitemap or google analytics
    if args.sitemapid is None and args.sitemapdb is None:
        print ('ERROR! You must specify the FQDN ID and '
               'sitemap.db file argument to continue.')
        sys.exit(-1)

    # check that .env exits and load variables
    if not os.path.isfile('%s/.env' % PATH):
        print ('ERROR! The .env file could not be found in %s\n'
               'Review README.md for more information.') % PATH
        sys.exit(-1)
    else:
        dotenv = Dotenv(os.path.join(PATH, '.env'))
        os.environ.update(dotenv)

    # check that key exists
    if args.id is not None and not os.path.isfile('%s/key.json' % PATH):
        print ("ERROR! The key.json file could not be found in %s\n"
               "Review README.md for more information.") % PATH
        sys.exit(-1)

    # add to queue
    def add_to_queue(i, q):
        while True:
            url = q.get()
            crawler.warm_url(url)
            q.task_done()

    # create threads
    threads = int(os.environ.get('THREADS', 5))
    threads = threads if (threads > 0 and threads < 10) else 5
    for i in range(threads):
        worker = threading.Thread(target=add_to_queue, args=(i, QUEUE))
        worker.daemon = True
        worker.start()

    # start crawling
    started = time.time()
    crawler = Crawler()
    if args.sitemap is not None:
        offset = args.offset if (args.offset is not None) else 0
        crawler.sitemap_crawler(args.sitemap, args.count, offset)
    else:
	sys.exit(-1)	

    # multithreaded cache warmer
    delay = float(os.environ.get('DELAY', 500))
    for url in crawler.urls:
        QUEUE.put(url)
        time.sleep(delay / 1000.0)

    # finsih the queue/threads
    try:
        term = threading.Thread(target=QUEUE.join)
        term.daemon = True
        term.start()
        while (term.isAlive()):
            term.join(600)
    except:
        print 'ERROR - Could not join the thread pool!'
        sys.exit(-1)

    # finished crawling
    num_warmed = len(crawler.results)
    finished = (float(time.time() - started) / 60)

    # save the results to a csv file
    with open('%s/crawler-log.csv' % PATH, 'wb') as f:
        headers = ['URL', 'Status Code', 'Elapsed (ms)', 'Redirect']
        logger = csv.writer(f)
        logger.writerow(headers)
        logger.writerows(crawler.results)

    # prepare email
    emailer = Emailer()
    subject = "Warmed %i Pages" % num_warmed
    html = ("Warmed %i pages in %i minutes. Please review the attached"
            " log for more information on the pages crawled."
            ) % (num_warmed, finished)
    attachments = ['%s/crawler-log.csv' % PATH]

    # Send the Email
    try:
        emailer.send_email(html, subject, attachments)
        sys.exit(0)
    except EmailerError as e:
        print e
        sys.exit(-1)

    # safely exit
    sys.exit(0)
