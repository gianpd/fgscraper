import os
import sys
import json
import logging

logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("region-id-spyder")

import requests
from bs4 import BeautifulSoup

def get_select_options_dict(response, attrs={'name': 'IDRegione'}) -> dict:
    soup = BeautifulSoup(response.text, 'lxml')
    options = soup.find('select', attrs=attrs).find_all('option')
    logger.info(f'Founded {len(options)} options in select with attrs {attrs}')
    regionsIDs = {option.text: option['value'] for option in options if option['value']}
    return regionsIDs


url = "https://www.fgas.it/RicercaSezC#"
response = requests.get(url=url)
regionsIDs = get_select_options_dict(response=response)
province_ids = get_select_options_dict(response=response, attrs={'name': 'IstatProv'})

with open('assets/regions-ids.json', '+w') as f:
    json.dump(regionsIDs, f)
    logger.info('Regions IDs json written.')

with open('assets/provincie-ids.json', '+w') as f:
    json.dump(province_ids, f)
    logger.info('Province IDs json written.')

