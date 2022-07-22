import os
import sys

from wasabi import msg

from typing import List, Dict, Union, Optional, Tuple

from bs4 import BeautifulSoup

# CONSTANT
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
        raise ValueError(
            'Generator input must be a List containing the enterprise IDs.')
    for f_path in file_paths:
        with open(f_path) as f:
            msg.info(f'yielding {f_path}')
            yield (f_path, f.read())


def get_raw_payload(dict_payload: Dict[str, str]) -> str:
    """
    Util method for parsing the dict_payload in a raw string payload.
    """
    assert isinstance(
        dict_payload, dict), 'The provided payload must be a Python Dict.'
    ss = [key + '=' + value for key, value in dict_payload.items()]
    return '&'.join(ss)


def extract_table1_values(_soup: BeautifulSoup):
    table1 = _soup.find('table', 'editor-field')
    if not table1:
        msg.warn('Table 1 not founded.')
        return None
    strongs = table1.find_all('strong')
    cols1 = [strong.text for strong in strongs]
    values1 = [strong.find_next('td').text.replace(
        '\t', '').replace('\n', '') for strong in strongs]
    return (cols1, values1)


def extract_table2_values(_soup: BeautifulSoup):
    table2 = _soup.find('table', 'dataTable dtCertificati')
    if not table2:
        msg.warn('Table 2 not found.')
        return None
    cols2 = list(filter(lambda x: x, [div.text.replace('\n', '').replace('\t', '')
                                      .replace('  ', '') for div in table2.find_all('div', 'DataTables_sort_wrapper') if div.text]))
    values2 = list(filter(lambda x: x, [td.text.replace('\n', '')
                                        .replace('\t', '').replace('  ', '') for td in table2.find('tr', 'odd').find_all('td') if td.text]))
    return (cols2, values2)


def get_full_dict(data1, data2) -> Dict[str, str]:
    d1 = {k: v for k, v in zip(*data1)} if data1 else None
    d2 = {k: v for k, v in zip(*data2)} if data2 else None
    if not d1 and d2:
        msg.warn('Just Table 2 founded.')
        return d2
    elif d1 and not d2:
        msg.warn('Just Table 1 founded.')
        return d1
    else:
        msg.good('Founded both table 1 and table 2.')
        d1.update(d2)
        return d1
