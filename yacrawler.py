import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor

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

def blocking_write_to_file(path,body,kwargs):
    '''
    :param path: File path
    :param body: File content
    :param kwargs: File open kwargs
    '''
    with open(path, **kwargs) as fp:
        fp.write(body)


class Crawler:

    def __init__(self):
        self.news_queue = asyncio.Queue()
        self.download_queue = asyncio.Queue()
        self.sem = asyncio.Semaphore(MAX_FETCHES)

    async def fetch(self, session, url):
        '''
        Fetch an URL
        :param session: aiohttp.ClientSession
        :param url: url to fetch
        :return: tuple of (content_type, body)
        '''
        async with self.sem:
            absolute_url = get_absolute_url(url)
            async with session.get(absolute_url, verify_ssl=False) as response:
                if response.status != 200:
                    logging.error(f'Invalid response status: {response.status} {absolute_url}')
                    return '', None
                content_type = response.content_type
                if content_type.startswith('text/'):
                    body = await response.text()
                else:
                    body = await response.read()
                return content_type, body

    async def check_latest_news(self):
        '''
        Check latest NUM_ITEMS news
        '''
        logging.info('Checking news...')
        async with aiohttp.ClientSession() as session:
            _, body = await self.fetch(session, BASE_URL)
            tree = fromstring(body)
            rows = tree.xpath('//tr[@class="athing"]')[:NUM_ITEMS_TO_FETCH]
            i=0
            for row in rows:
                id = row.attrib['id']
                if os.path.exists(os.path.join(OUTPUT_DIR, id)):
                    continue

                url = row.xpath('td[@class="title"]/a[@class="titlelink"]/@href')[0]
                comment_url = row.xpath(
                    'following-sibling::tr[1]/td[@class="subtext"]/a[contains(text(),"comment")]/@href')
                i+=1
                self.news_queue.put_nowait(Item(id, url, comment_url[0] if comment_url else ''))

            logging.info(f'found {i} new(s)')
        logging.info('Done')

    async def periodic(self, timeout):
        '''
        Check news with period
        :param timeout: Timeout in seconds
        '''
        asyncio.create_task(self.process_news_queue())
        asyncio.create_task(self.process_download_queue())
        # Create directory for the output
        if not os.path.exists(OUTPUT_DIR):
            os.mkdir(OUTPUT_DIR)

        while True:
            await self.check_latest_news()
            await self.news_queue.join()
            await asyncio.sleep(timeout)


    async def process_news_queue(self):
        while True:
            item = await self.news_queue.get()
            await self.process_news_item(item)
            self.news_queue.task_done()

    async def process_download_queue(self):
        while True:
            args = await self.download_queue.get()
            await self.write_content_to_file(*args)
            self.download_queue.task_done()


    def run(self):
        asyncio.run(self.periodic(PERIOD))


    async def process_news_item(self, item):
        '''
        Process news by given list of links
        :param item: list of url
        '''
        item_dir = os.path.join(OUTPUT_DIR, item.id)
        if not os.path.exists(item_dir):
            os.mkdir(item_dir)

        async with aiohttp.ClientSession() as session:
            content_type, body = await self.fetch(session, item.url)
            if not content_type:
                return
            logging.info("Crawling {} {}".format(item.id, item.url))


            if isinstance(body, asyncio.TimeoutError):
                logging.error('Timeout error')
                return

            source_path = os.path.join(item_dir, 'page')
            self.download_queue.put_nowait((source_path, content_type, body))

            if item.comment_url:
                await self.process_comments(session, item.comment_url, item_dir)

    async def write_content_to_file(self,source_path, content_type, body):
        '''
        Write content to a file, based on its content type
        :param source_path: Source path of the file without an extension
        :param content_type: File content type
        :param body: File contents   '''

        if not content_type or not body:
            return
        ext = guess_extension(content_type) or ''
        kwargs = {'mode': 'w', 'encoding': 'utf-8'} if content_type.startswith('text/') else {'mode': 'wb'}
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, blocking_write_to_file, source_path + ext, body, kwargs)

    async def process_comments(self, session, comment_url, item_dir):
        '''
        Process comments for comment_url
        :param session: Aiohttp session
        :param comment_url: url to check
        :param item_dir: output directory
        '''

        _, comment_body = await self.fetch(session, comment_url)
        if not comment_body:
            return

        comment_tree = fromstring(comment_body)
        comment_links = comment_tree.xpath('//div[@class="comment"]//a[@rel="nofollow"]/@href')

        logging.info(f'found {len(comment_links)} comment(s)')

        comment_responses = await asyncio.gather(*(
            asyncio.wait_for(self.fetch(session, url), CONNECT_TIMEOUT)
            for url in comment_links
        ), return_exceptions=True)

        for idx, (c_url, resp) in enumerate(zip(comment_links, comment_responses)):
            logging.info("Crawling comment {} {}".format(idx, c_url))
            if isinstance(resp, tuple):
                c_type, c_body = resp
                if isinstance(c_body, asyncio.TimeoutError):
                    logging.error('Timeout error')
                    continue
                c_path = os.path.join(item_dir, '{:03d}_comment_page'.format(idx))
                self.download_queue.put_nowait((c_path, c_type, c_body))


if __name__ == '__main__':

    logging.basicConfig(filename=None, level='INFO',
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    # Windows workaround for a problem with EventLoopPolicy
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    crawler = Crawler()
    try:
        crawler.run()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt. Stopping...")

