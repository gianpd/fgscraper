import asyncio
from fgscraper.spiders.regionID_parser import main as regionID_main
from fgscraper.spiders.fg_post_spider import main as fg_post_main
from fgscraper.spiders.fg_get_playwright import main as fg_get_main
from fgscraper.post_processing.processing_raw_enterprise import main as processing_main


def main():
    regionID_main()
    fg_post_main()
    asyncio.run(fg_get_main())
    processing_main()