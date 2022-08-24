import os
import pathlib

import re

from datetime import datetime

from wasabi import msg

import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 80)

from fgscraper.common.data_ingest_manager import DataIngestManager

ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')
DATA_PATH = pathlib.Path(ROOT_PATH)/'data'

data_manager = DataIngestManager(DATA_PATH/'raw_enterprise')

ENTI_CERTIFICATO_DICT = {
    'AICQ-SICEV': re.compile(r'(\S+)\SICEV'),
    'AJA EUROPE': re.compile(r'(XPERT\S+)|(AJA\S+)'),
    'APAVE': re.compile(r'FGAS\S+'),
    'BUREAU VERITAS_CEPAS': re.compile(r'IT\d+'),
    'CERTIQUALITY': re.compile(r'\S+Certiquality'),
    'DEKRA': 'DTC',
    'DI.QU.': re.compile(r'\S+-\d{2}$'),
    'ICMQ': re.compile(r'FG0\S+'),
    'IMQ': re.compile(r'303I\S+'),
    'INTERTEK': re.compile(r'ITK\S+'),
    'RINA': re.compile(r'\S+/\d{2}'),
    'SGS': re.compile(r'IT\d{2}/\S+'),
    'STS': re.compile(r'FGI\.\S+'),
    'ITEC': re.compile(r'FGI\d+'),
    'TEC-EUROLAB': re.compile(r'F-\S+'),
    'TUV': re.compile(r'FLI\S+'),
    'KIWA': re.compile(r'(KI\S+)|(ACVPR\s\S+)'),
    'VERIGAS.IT': re.compile(r'VG\S+')
}


def main():
    dfs = []
    ent_json_paths = data_manager.get_file_paths(data_path=data_manager._raw_enterprise_path, file_prefix='json')
    if not len(ent_json_paths):
        raise ValueError('No raw enterprise to process. Be sure to run fg_get_playwright.py before.')
    for ent_json_path in ent_json_paths:
        ent_json = data_manager.read_json(ent_json_path)
        dfs.append(pd.DataFrame(ent_json, index=[0]))
    
    df = pd.concat(dfs, ignore_index=True)
    msg.info(f'Created a DF with {len(df)} rows')
    now = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
    fpath = DATA_PATH / \
        f'processed_enterprise/{now}_processed_enterprise.parquet'
    msg.info(f'Saving a DF as parquet file to {fpath}')

    # ENTI - certificati mapping
    to_replace = list(ENTI_CERTIFICATO_DICT.values())
    replace_with = list(ENTI_CERTIFICATO_DICT.keys())
    df['ENTE'] = df['Numero certificato'].replace(to_replace, replace_with, regex=True)
    msg.good(f'Created df with columns: {df.columns}')
    msg.good('df head:\n{df}'.format(df=df.head(1)))

    ### Save df
    df.to_parquet(fpath, engine='pyarrow')
    msg.good('Done.')
