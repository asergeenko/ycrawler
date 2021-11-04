import aiohttp
import asyncio

import logging
from lxml.html import fromstring
from urllib.parse import urlparse,urljoin
from mimetypes import guess_extension
from collections import namedtuple
import os

BASE_URL = 'https://news.ycombinator.com/'
MAX_FETCHES = 5
CONNECT_TIMEOUT = 60 # Set this so large because of Twitter troubles in Russia
NUM_ITEMS_TO_FETCH = 30
OUTPUT_DIR = './output/'
PERIOD = 30

Item = namedtuple("Item", "id url comment_url")

def get_absolute_url(url):
    '''
    Retrurns absolute url
    :param url: url to check
    :return: Absoulute url
    '''
    if bool(urlparse(url).netloc): # url is absolute
        return url
    return urljoin(BASE_URL,url) # url is local

def write_file(source_path, content_type, body):
    '''
    Write contents to a file, based on its content type
    :param source_path: Source path of the file without an extension
    :param content_type: File content type
    :param body: File contents
    '''
    ext = guess_extension(content_type) or ''
    if content_type.startswith('text/'):
        kwargs = {'mode':'w', 'encoding':'utf-8'}
    else:
        kwargs = {'mode':'wb'}
    with open(source_path + ext, **kwargs) as fp:
        fp.write(body)


async def fetch(session, sem, url):
    '''
    Fetch an URL
    :param session: aiohttp.ClientSession
    :param sem: asyncio.Semaphore
    :param url: url to fetch
    :return: tuple of (content_type, body)
    '''
    async with sem:
        async with session.get(get_absolute_url(url)) as response:
            content_type = response.content_type
            if content_type.startswith('text/'):
                body = await response.text()
            else:
                body = await response.read()
            return content_type, body

async def check_news(sem):
    '''
    Check and download latest NUM_ITEMS news
    :param sem: asyncio.Semaphore
    '''
    logging.info('Checking news...')
    async with aiohttp.ClientSession() as session:
        _, body = await fetch(session, sem, BASE_URL)
        tree = fromstring(body)
        rows = tree.xpath('//tr[@class="athing"]')[:NUM_ITEMS_TO_FETCH]
        items = []
        for row in rows:
            id = row.attrib['id']
            if os.path.exists(os.path.join(OUTPUT_DIR,id)):
                continue

            url = row.xpath('td[@class="title"]/a[@class="titlelink"]/@href')[0]
            comment_url = row.xpath('following-sibling::tr[1]/td[@class="subtext"]/a[contains(text(),"comment")]/@href')
            items.append(Item(id, url, comment_url[0] if comment_url else ''))

        responses = await asyncio.gather(*(
            asyncio.wait_for(fetch(session, sem, item.url), CONNECT_TIMEOUT)
            for item in items
        ), return_exceptions=True)

        logging.info('found {} new(s)'.format(len(items)))
        for item, (content_type, body) in zip(items, responses):
            logging.info("Crawling {} {}".format(item.id,item.url))
            item_dir = os.path.join(OUTPUT_DIR, item.id)
            if not os.path.exists(item_dir):
                os.mkdir(item_dir)

            if isinstance(body,asyncio.TimeoutError):
                logging.error('Timeout error')
                continue

            source_path = os.path.join(item_dir, 'page')
            write_file(source_path, content_type, body)

            if item.comment_url:
                _, comment_body = await fetch(session, sem, item.comment_url)
                comment_tree = fromstring(comment_body)
                comment_links = comment_tree.xpath('//div[@class="comment"]//a[@rel="nofollow"]/@href')

                logging.info('found {} comment(s)'.format(len(comment_links)))
                comment_responses = await asyncio.gather(*(
                    asyncio.wait_for(fetch(session, sem, url), CONNECT_TIMEOUT)
                    for url in comment_links
                ), return_exceptions=True)


                for idx, (c_url, resp) in enumerate(zip(comment_links,comment_responses)):
                    logging.info("Crawling comment {} {}".format(idx, c_url))
                    if isinstance(resp,tuple):
                        c_type, c_body = resp
                        if isinstance(c_body, asyncio.TimeoutError):
                            logging.error('Timeout error')
                            continue
                        c_path = os.path.join(item_dir, '{:03d}_comment_page'.format(idx))
                        write_file(c_path, c_type, c_body)
    logging.info('Done')

async def periodic(timeout):
    '''
    Check news with period
    :param timeout: Timeout in seconds
    '''

    # Create directory for the output
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    sem = asyncio.Semaphore(MAX_FETCHES)
    while True:
        await check_news(sem)
        await asyncio.sleep(timeout)


logging.basicConfig(filename=None, level='INFO',
                    format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

loop = asyncio.get_event_loop()
task = loop.create_task(periodic(PERIOD))
try:
    loop.run_until_complete(task)
except KeyboardInterrupt:
    logging.info("Keyboard interrupt. Stopping...")
    task.cancel()