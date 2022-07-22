"""
Questo spyder serve per estrarre tutte le imprese nazionali suddivise per regioni e provincie. Il suo output è un txt, uno per ogni provincia (107), il quale contiene il campo Imprese|id, dove id, è l'id 
da dover usare successivamente in una GET per estrarre l'info richiesta.
"""

from datetime import datetime
from scrapy.crawler import CrawlerProcess
import scrapy
import json
import os
import sys

from wasabi import msg

import utils as spyder_utils


ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../')

DICT_PAYLOAD = {
    'FormatoReport': '3',
    'GeneraReport': 'false',
    'displayReport': 'display%3Anone%3B',
    'flagAreaPub': 'True',
    'DataFromSession': 'false',
    'DownloadToken': '',
    'NumRecord': '160',
    'Nazionalita': 'I',
    'IDRegione': '',  # field to be fill in the loop
    'IstatProv': '',  # field to be fill in the loop
    'Identificativo': '',
    'RadioBtnDenominazione': 'C',
    'Denominazione': '',
    'NumCertProv': '',
    'TipoSoggetto': 'I',
    'IDAttivita_I': '',
    'IDAttivita': ''}



class FGPOSTSpyder(scrapy.Spider):
    name = 'fgpostspyder'
    # starts_urls = ['https://www.fgas.it/RicercaSezC/RicercaSezC_PostResult?_=1657727080235', 'https://www.fgas.it/RicercaSezC/RicercaSezC_PostResult?_=1658155872384']
    base_url = 'https://www.fgas.it/RicercaSezC/RicercaSezC_PostResult?_='
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US, en;q = 0.9',
        # 'content-length': 284,
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'cookie': '',
        'dnt': 1,
        'origin': 'https://www.fgas.it',
        'referer': 'https://www.fgas.it/RicercaSezC',
        'request-context': 'appId = cid-v1: 81c9feee-b38a-4c67-a311-ddfbb9d069b5',
        'request-id': '| 0tRPH.e+OnD',
        'sec-ch-ua': '.Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': "Windows",
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0 Win64 x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    # custom scraper settings
    custom_settings = {
        # pass cookies along with headers
        'COOKIES_ENABLED': False,
        'DOWNLOAD_DELAY': 2
    }

    # cookies = {
    #     'CookieOptIn': 'true',
    #     'luckynumber': '1896761001',
    #     'MpSession': '9ff31f05-36fd-4570-9cdc-e1800bf682fe',
    # }

    dict_payload = DICT_PAYLOAD

    with open(f'{ROOT_PATH}/assets/regions-ids.json') as f:
        regions = json.load(f)
    with open(f'{ROOT_PATH}/assets/provinces-ids.json') as f:
        provinces = json.load(f)

    def start_requests(self):
        for region_name, region_id in self.regions.items():
            self.dict_payload.update({'IDRegione': region_id})
            # get a list which elements are dictionaries {PROV_NAME: PROV_ID}
            provs = self.provinces.get(region_name)
            for prov in provs:
                prov_name, prov_id = prov.popitem()
                msg.info(
                    f'Requesting POST for {region_name, prov_name} with prov_id: {prov_id}')
                self.dict_payload.update({'IstatProv': prov_id})
                body = spyder_utils.get_raw_payload(self.dict_payload) # POST request requires raw payload
                msg.info(f'Requesting a POST with raw body: {body}')
                yield scrapy.Request(
                    url=self.base_url,
                    method='POST',
                    headers=self.headers,
                    # cookies=self.cookies,
                    body=body,
                    callback=self.parse_enterprise,
                    cb_kwargs=dict(region_name=region_name,
                                   prov_name=prov_name)
                )

    def parse_enterprise(self, response, region_name: str, prov_name: str):
        resp = json.loads(response.text)
        dataTable_ids = list(map(lambda x: x[-1].split('|')[-1], resp['Result']['DataTable']))
        assert len(dataTable_ids) == resp['Result']['TotalRecords'], 'Ids must be equal to the total records.'
        now = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
        fname = now + f'_{region_name}_{prov_name}.txt'
        with open(f'{ROOT_PATH}/data/enterprise_ids/{fname}', '+w') as f:
            for i in dataTable_ids:
                f.write(i + '\n')


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(FGPOSTSpyder)
    process.start()
