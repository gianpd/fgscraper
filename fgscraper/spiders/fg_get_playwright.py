import utils as spyder_utils
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import asyncio
from pathlib import Path
import os
import sys
import logging

logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                    level=logging.DEBUG, datefmt=None)
logger = logging.getLogger("fg-get-spyder")


ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')
DATA_PATH = Path(ROOT_PATH)/'data'
file_paths = sorted(list(map(lambda x: str(x), Path(DATA_PATH).glob('*.txt'))))
logger.info(f'Retrived {len(file_paths)} txt files in {DATA_PATH}')

dataset = spyder_utils.gen_dataset(file_paths)


async def run(playwright):
    dfs = []
    for data in dataset:
        chromium = playwright.chromium
        browser = await chromium.launch()
        fpath = Path(data[0])
        regione, provincia = fpath.stem.split('_')[1:]
        id_lines = data[1].splitlines()
        logger.info(f'===== Processing files with {len(id_lines)} Ids')
        logger.info(f'regione: {regione}, provincia: {provincia}')
        for id_line in id_lines:
            _url = spyder_utils.get_full_url(id_line)
            logger.info(f'requesting page at {_url}')
            try:
                # chromium = playwright.chromium
                # browser = await chromium.launch()
                context = await browser.new_context()  # create a new incognite mode context
                page = await context.new_page()
                await page.goto(_url)
                html = await page.inner_html('#main_content')
                soup = BeautifulSoup(html, 'html.parser')
            except PlaywrightTimeoutError as e:
                logger.info(e)
                continue

            def extract_table1_values(soap: BeautifulSoup):
                table1 = soup.find('table', 'editor-field')
                if not table1:
                    logger.info('Table 1 not founded.')
                    return None
                strongs = table1.find_all('strong')
                cols1 = [strong.text for strong in strongs]
                values1 = [strong.find_next('td').text.replace('\t', '').replace('\n', '') for strong in strongs]
                return (cols1, values1)

            def extract_table2_values(soap: BeautifulSoup):
                table2 = soup.find('table', 'dataTable dtCertificati')
                if not table2:
                    logger.info('Table 2 not found.')
                    return None
                cols2 = list(filter(lambda x: x, [div.text.replace('\n', '').replace('\t', '')\
                    .replace('  ', '') for div in table2.find_all('div', 'DataTables_sort_wrapper') if div.text]))
                values2 = list(filter(lambda x: x, [td.text.replace('\n', '')\
                    .replace('\t', '').replace('  ', '') for td in table2.find('tr', 'odd').find_all('td') if td.text]))
                return (cols2, values2)

            ### get cols and values
            data1 = extract_table1_values(soup)
            data2 = extract_table2_values(soup)
            if not data1 and not data2:
                logger.info(f'Problems with {_url}: not table1 and table2 founded.')
                continue
            
            full_dict = None
            d1 = {k: v for k, v in zip(*data1)} if data1 else None
            d2 = {k: v for k, v in zip(*data2)} if data2 else None
            if not d1 and d2:
                logger.info('Just Table 2 founded.')
                full_dict = d2
            elif d1 and not d2:
                logger.info('Just Table 1 founded.')
                full_dict = 1
            else:
                logger.info('Founded both table 1 and table 2.')
                d1.update(d2)
                full_dict = d1
            

            # Make two dictionaries and join them
            logger.info(f'Retrived full dict: {full_dict}')
            full_dict.update({'Regione': regione.upper(),
                             'Provincia': provincia.upper()})
            df = pd.DataFrame(full_dict, index=[0])
            dfs.append(df)
            await context.close()

        await browser.close()

    final_df = pd.concat(dfs, ignore_index=True)
    logger.info(f'Writing a csv file with {len(final_df)} rows')
    final_df.to_csv(f'{DATA_PATH}/final_df_prova.csv')


async def main():
    async with async_playwright() as playwright:
        await run(playwright)


asyncio.run(main())
