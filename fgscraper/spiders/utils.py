import os
import sys
import logging

logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                    level=logging.DEBUG, datefmt=None)
logger = logging.getLogger("fg-get-spyder")

from typing import List, Dict, Union, Optional, Tuple

###### CONSTANT
BASE_URL = 'https://www.fgas.it/RicercaSezC/DettaglioImpresa?id='

def get_full_url(enterprise_id: str) -> str:
    return BASE_URL + enterprise_id

def gen_dataset(file_paths: Union[List[str], str]) -> Tuple[str, str]:
    """
    Dataset Generator:

    --Parameters
     - file_paths: list (or string) contained the full paths of the txt files contained the enterprise IDs

    --Return 
     - Tuple(str, str): file path, txt file content
    """
    if not isinstance(file_paths, list):
        raise ValueError('Generator input must be a List containing the enterprise IDs.')
    for f_path in file_paths:
        with open(f_path) as f:
            logger.info(f'yielding {f_path}')
            yield (f_path, f.read())

def get_raw_payload(dict_payload: Dict[str, str]) -> str:
    """
    Util method for parsing the dict_payload in a raw string payload.
    """
    assert isinstance(
        dict_payload, dict), 'The provided payload must be a Python Dict.'
    ss = [key + '=' + value for key, value in dict_payload.items()]
    return '&'.join(ss)