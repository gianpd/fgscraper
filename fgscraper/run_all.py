import os
from pathlib import Path
from wasabi import msg
import asyncio
from fgscraper.common.data_ingest_manager import DataIngestManager
from fgscraper.spiders.regionID_parser import main as regionID_main
ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')
DATA_PATH = Path(ROOT_PATH)/'data'
data_manager = DataIngestManager(DATA_PATH/'raw_enterprise')

if not data_manager.check_if_regionIds():
    msg.good('##### Running regionID main script')
    regionID_main()

from fgscraper.spiders.fg_post_spider import main as fg_post_main
from fgscraper.spiders.fg_get_playwright import main as fg_get_main
from fgscraper.post_processing.processing_raw_enterprise import main as processing_main

loop = asyncio.get_event_loop()

def main():
    if data_manager.check_if_must_run_id_and_post():
        msg.info('Running entire pipeline')
        fg_post_main()
        loop.run_until_complete(fg_get_main())
        processing_main()
    else:
        msg.good('Running just get and processing')
        loop.run_until_complete(fg_get_main())
        processing_main()

def run_post_processing():
    msg.good('Running post-processing job ...')
    processing_main()
