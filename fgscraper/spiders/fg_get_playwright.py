from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import asyncio
from pathlib import Path
import os
from datetime import datetime

from wasabi import msg

import fgscraper.common.utils as spyder_utils
from fgscraper.common.data_ingest_manager import DataIngestManager


ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')
DATA_PATH = Path(ROOT_PATH)/'data'
data_manager = DataIngestManager(
    raw_enterprise_path=DATA_PATH/'raw_enterprise')
file_paths = data_manager.get_file_paths(DATA_PATH/'enterprise_ids', 'txt')
dataset = spyder_utils.gen_dataset(file_paths)


async def run(playwright):
    for data in dataset:
        chromium = playwright.chromium
        browser = await chromium.launch()
        fpath = Path(data[0])
        regione, provincia = fpath.stem.split('_')[1:]
        id_lines = data[1].splitlines()
        start_n_lines = len(id_lines)
        msg.info(f'===== Processing files with {start_n_lines} Ids')
        msg.info(f'regione: {regione}, provincia: {provincia}')
        # check if raw enterprise json already exist
        already_ids = data_manager.get_raw_enterprise_paths()
        msg.good(f'Checking if current TXT contains already processed enterprise ...')
        id_lines = list(set(id_lines).difference(already_ids))  # ids are unique, so sets could be used. 
        msg.good(f'id_lines reduced of {start_n_lines - len(id_lines)}')

        for id_line in id_lines:
            _url = spyder_utils.get_full_url(id_line)
            msg.info(f'requesting page at {_url}')
            try:
                context = await browser.new_context()  # create a new incognite mode context
                page = await context.new_page()
                await page.goto(_url)
                html = await page.inner_html('#main_content')
                soup = BeautifulSoup(html, 'html.parser')
            except PlaywrightTimeoutError as e:
                msg.fail(e)
                continue

            # get cols and values
            data1 = spyder_utils.extract_table1_values(_soup=soup)
            data2 = spyder_utils.extract_table2_values(_soup=soup)
            if not data1 and not data2:
                msg.warn(
                    f'Problems with {_url}: neither table1 nor table2 founded.')
                continue

            full_dict = spyder_utils.get_full_dict(data1, data2)
            # Make two dictionaries and join them
            full_dict.update({'Regione': regione.upper(),
                             'Provincia': provincia.upper(),
                              'source_file': fpath.name,
                              'EnterpriseID': id_line})
            msg.info(f'Retrived full dict: {full_dict}')
            # write the raw enterprise info to the disk
            now = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
            fname_raw = f'{now}_{regione}_{provincia}_{id_line}'
            data_manager.write_json(
                full_dict=full_dict, dir_path=DATA_PATH/'raw_enterprise', fname=fname_raw)
            await context.close()
            
        await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)

