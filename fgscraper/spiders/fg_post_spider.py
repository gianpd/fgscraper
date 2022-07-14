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

import os
import sys
import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("region-id-spyder")


import csv
import json

import scrapy
from scrapy.crawler import CrawlerProcess


class FGSpyder(scrapy.Spider):
    name = 'fgspyder'

    base_url = 'https://www.fgas.it/RicercaSezC/RicercaSezC_PostResult?_=1657727080235'

    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US, en;q = 0.9',
        'content-length': 284,
        'content-type': 'application/x-www-form-urlencoded',
        'cookie': 'ASP.NET_SessionId=wuufmyjfo2kg3tjqr50puheo; ai_user=M9M1U|2022-07-13T13:45:14.695Z; ai_session=6fqj/|1657719914802|1657727070692.1',
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

    body = 'FormatoReport=3&GeneraReport=false&displayReport=display%3Anone%3B&flagAreaPub=True&DataFromSession=false&DownloadToken=&NumRecord=160&Nazionalita=I&IDRegione=19&IstatProv=083&Identificativo=&RadioBtnDenominazione=C&Denominazione=&NumCertProv=&TipoSoggetto=I&IDAttivita_I=&IDAttivita='
    
    #TODO: payload is obtained by parsing the body, however the scrapy post request does not work if payload is used, it requires raw payload.
    # payload = {
    #     'FormatoReport': '3', 
    #     'GeneraReport': 'false', 
    #     'displayReport': 'display%3Anone%3B', 
    #     'flagAreaPub': 'True', 
    #     'DataFromSession': 'false', 
    #     'DownloadToken': '', 
    #     'NumRecord': '160', 
    #     'Nazionalita': 'I', 
    #     'IDRegione': '19', 
    #     'IstatProv': '083', 
    #     'Identificativo': '', 
    #     'RadioBtnDenominazione': 'C', 
    #     'Denominazione': '', 
    #     'NumCertProv': '', 
    #     'TipoSoggetto': 'I', 
    #     'IDAttivita_I': '', 
    #     'IDAttivita': ''}

    # custom scraper settings
    custom_settings = {
        # pass cookies along with headers
        'COOKIES_ENABLED': False
    }

    def start_requests(self):
        yield scrapy.Request(
            url=self.base_url,
            method='POST',
            headers=self.headers,
            body=self.body,
            # body=json.dumps(self.payload),
            callback=self.parse
            )

    def parse(self, response):
        resp = json.dumps(json.loads(response.text), indent=4)
        print(resp)
        with open('resp.json', '+w') as f:
            json.dump(resp, f)


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(FGSpyder)
    process.start()