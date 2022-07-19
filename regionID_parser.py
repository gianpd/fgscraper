import os
import sys
import json
import logging

logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("region-id-spyder")

import requests
from bs4 import BeautifulSoup

from typing import Dict, List, Union, Optional

def get_select_options_dict(response: requests.Response, attrs: Optional[Dict[str, str]]={'name': 'IDRegione'}) -> dict:
    """Util for getting the list of all regions and their own IDs.
    
    --Parameters
     response: requests response
     attrs: dict (the attributes to use for the finding procedures.
     
    --return
      dict (a dictionary where keys are the region name and values the region id"""

    soup = BeautifulSoup(response.text, 'lxml')
    options = soup.find('select', attrs=attrs).find_all('option')
    logger.info(f'Founded {len(options)} options in select with attrs {attrs}')
    regionsIDs = {option.text: option['value'] for option in options if option['value']}
    return regionsIDs

def get_provinces_json(region_id_dict: Dict[str, str]) -> Dict[str, str]:
    """Get the provinces list for each region"""
    assert isinstance(region_id_dict, dict), 'The region_id_dict must be a Python Dictionary.'
    pr = {}
    for region_name, region_id in region_id_dict.items():
        url = 'https://www.fgas.it/RicercaCommon/LoadProvinceByRegione?idRegione=' + region_id
        logger.info(f'Requesting a GET at {url}')
        response_jsons = requests.get(url).json()
        logger.info(f'Response content: {response_jsons}')
        pr[region_name] = []
        for resp in response_jsons:
            if resp['Value']:
                pr[region_name].append({resp['Text']: resp['Value']})
    return pr


### Get the regions IDs
url = "https://www.fgas.it/RicercaSezC#"
response = requests.get(url=url)
regionsIDs = get_select_options_dict(response=response)
### Get the provinces IDs per regions
provincesIDs = get_provinces_json(regionsIDs)

with open('./fgscraper/assets/regions-ids.json', '+w') as f:
    json.dump(regionsIDs, f)
    logger.info('Regions IDs json written.')


with open('./fgscraper/assets/provinces-ids.json', '+w') as f:
    json.dump(provincesIDs, f)
    logger.info('Provinces IDs json written.')

