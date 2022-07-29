import os
import json
import pathlib

from datetime import datetime

from wasabi import msg

import pandas as pd

from fgscraper.common.data_ingest_manager import DataIngestManager

ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')
DATA_PATH = pathlib.Path(ROOT_PATH)/'data'

data_manager = DataIngestManager(DATA_PATH/'raw_enterprise')


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
    df.to_parquet(fpath, engine='pyarrow')
    msg.good('Done.')
