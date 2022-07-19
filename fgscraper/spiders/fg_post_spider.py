# se hai l'id della regione con questa GET ottieni tutte e sole le provincie associate a questa regione
# Regione AQUILA -> id=13
# fgas.it/RicercaCommon/LoadProvinceByRegione?idRegione=13&_=1657788838798
# per fare la prima POST (ottenere tutti i link delle varie imprese) nel payload devi inserire id regione e value provincia
"""
STEP necessari:
1. ottenere tutti gli id delle regioni
2. ottenere tutti i value per ogni provincia associata ad una regione
3. costruire base dati (dict) con le due info precedenti da usare per il for loop successivo
4. eseguire le R x P chiamate POST, dove R è il numero delle regioni e P è il numero delle provincie totali (doppio ciclo for su regioni e provincie)
5. per ogni richiesta post, bisogna "seguire" tutti i links associati ed estrarre le info richieste e scriverle in un csv
"""

from typing import Dict, List, Union, Optional
from datetime import datetime
import time
from scrapy.crawler import CrawlerProcess
import scrapy
import json
import os
import sys
import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                    level=logging.INFO, datefmt=None)
logger = logging.getLogger("region-id-spyder")


ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../')
print(ROOT_PATH)

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


def get_raw_payload(dict_payload: Dict[str, str]) -> str:
    """
    Util method for parsing the dict_payload in a raw string payload.
    """
    assert isinstance(
        dict_payload, dict), 'The provided payload must be a Python Dict.'
    ss = [key + '=' + value for key, value in dict_payload.items()]
    return '&'.join(ss)


class FGSpyder(scrapy.Spider):
    name = 'fgspyder'
    # starts_urls = ['https://www.fgas.it/RicercaSezC/RicercaSezC_PostResult?_=1657727080235', 'https://www.fgas.it/RicercaSezC/RicercaSezC_PostResult?_=1658155872384']
    base_url = 'https://www.fgas.it/RicercaSezC/RicercaSezC_PostResult?_='
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US, en;q = 0.9',
        'content-length': 284,
        'content-type': 'application/x-www-form-urlencoded',
        'cookie': '',
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
                logger.info(
                    f'Requesting POST for {region_name, prov_name} with prov_id: {prov_id}')
                self.dict_payload.update({'IstatProv': prov_id})
                body = get_raw_payload(self.dict_payload)
                logger.info(f'Requesting a POST with body: {body}')
                # time.sleep(3)
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
        resp = json.dumps(json.loads(response.text), indent=4)
        now = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
        with open(f'{ROOT_PATH}/data/{now}_{region_name}_{prov_name}.json', '+w') as f:
            json.dump(resp, f)


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(FGSpyder)
    process.start()
