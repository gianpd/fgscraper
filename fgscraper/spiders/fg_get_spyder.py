import os
import sys
import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                    level=logging.DEBUG, datefmt=None)
logger = logging.getLogger("region-id-spyder")


from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
import pandas as pd
import scrapy

from scrapy_playwright.page import PageMethod


ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../')
BASE_URL = 'https://www.fgas.it/RicercaSezC/DettaglioImpresa?id='


def get_start_urls(ids_list, base_url):
    return list(map(lambda x: base_url + x, ids_list))


with open(ROOT_PATH + 'data/202207191651_Abruzzo_AQUILA.txt') as f:
    txt = f.read()

ids_list = txt.splitlines(keepends=True)
logger.info(f'len {len(ids_list)}')
logger.debug(f'example: {ids_list[0]}')

start_urls = get_start_urls(ids_list, BASE_URL)
logger.debug(f'{start_urls[0]}')

# div with class name -> fgas-header-body


class FGGetSpyder(scrapy.Spider):
    name = 'FgGetSpyder'
    # start_urls = start_urls
    start_urls = ['https://www.fgas.it/RicercaSezC/DettaglioImpresa?id=EzQg0vVdxwYSGXlOmFnBVA%3d%3d']
    logger.info(f'Loading {name} with {len(start_urls)} start urls.')

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            meta=dict(
                playwright = True,
                playwright_include_page = True,
                playwright_page_methods = {
                    'pdf': PageMethod('pdf', path='./out.pdf')
                }
            )
        )

    async def parse(self, response: scrapy.http.Response):
        pdf_bytes = response.meta['playwright_page_methods']['pdf'].result
        with open('example.pdf', 'wb') as f:
            f.write(pdf_bytes)
        
        yield {
            'url': response.url
        }
        
        # soup = BeautifulSoup(response.text, 'lxml')
        # strongs = soup.find('table', 'editor-field').find_all('strong')

        # for strong in strongs:
        #     yield {
        #         'strong': strong.text
        #     }


if __name__ == "__main__":
    process = CrawlerProcess(
        settings={
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "DOWNLOAD_HANDLERS": {
                "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            },
            # "PLAYWRIGHT_LAUNCH_OPTIONS": {
            #     'timeout': 0
            # }
        }
    )
    process.crawl(FGGetSpyder)
    process.start()
