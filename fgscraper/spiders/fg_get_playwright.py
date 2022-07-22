import utils as spyder_utils
# import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import asyncio
from pathlib import Path
import os
from datetime import datetime

from wasabi import msg

from data_ingest_manager import DataIngestManager


ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')
DATA_PATH = Path(ROOT_PATH)/'data'
data_manager = DataIngestManager(raw_enterprise_path=DATA_PATH/'raw_enterprise')
file_paths = data_manager.get_file_paths(DATA_PATH/'enterprise_ids', 'txt')
dataset = spyder_utils.gen_dataset(file_paths)


async def run(playwright):
    # dfs = []
    for data in dataset:
        chromium = playwright.chromium
        browser = await chromium.launch()
        fpath = Path(data[0])
        regione, provincia = fpath.stem.split('_')[1:]
        id_lines = data[1].splitlines()
        msg.info(f'===== Processing files with {len(id_lines)} Ids')
        msg.info(f'regione: {regione}, provincia: {provincia}')
        for id_line in id_lines:
            ### check if raw enterprise json already exist
            if data_manager.check_raw_enterprise_exist(id_line):
                msg.good(f'{id_line} already present, skipping ...')
                continue
            _url = spyder_utils.get_full_url(id_line)
            msg.info(f'requesting page at {_url}')
            try:
                # chromium = playwright.chromium
                # browser = await chromium.launch()
                context = await browser.new_context()  # create a new incognite mode context
                page = await context.new_page()
                await page.goto(_url)
                html = await page.inner_html('#main_content')
                soup = BeautifulSoup(html, 'html.parser')
            except PlaywrightTimeoutError as e:
                msg.fail(e)
                continue

            ### get cols and values
            data1 = spyder_utils.extract_table1_values(_soup=soup)
            data2 = spyder_utils.extract_table2_values(_soup=soup)
            if not data1 and not data2:
                msg.warn(f'Problems with {_url}: not table1 and table2 founded.')
                continue
    

            full_dict = spyder_utils.get_full_dict(data1, data2)
            # Make two dictionaries and join them
            full_dict.update({'Regione': regione.upper(),
                             'Provincia': provincia.upper(),
                             'source_file': fpath.name,
                             'EnterpriseID': id_line})
            msg.info(f'Retrived full dict: {full_dict}')
            ### write the raw enterprise info to the disk 
            now = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
            fname_raw = f'{now}_{regione}_{provincia}_{id_line}'
            data_manager.write_json(full_dict=full_dict, dir_path=DATA_PATH/'raw_enterprise', fname=fname_raw)
            # df = pd.DataFrame(full_dict, index=[0])
            # dfs.append(df)
            await context.close()

        await browser.close()

    # final_df = pd.concat(dfs, ignore_index=True)
    # msg.info(f'Writing a parquet file with {len(final_df)} rows')
    # final_df.to_parquet(f'{DATA_PATH}/final_df_prova.parquet', engine='pyarrow')


async def main():
    async with async_playwright() as playwright:
        await run(playwright)


asyncio.run(main())
