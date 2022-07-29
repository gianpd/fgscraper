from fgscraper import run_all
import argparse
import asyncio

async def run_conditional():
    if args.post_processing:
        run_all.run_post_processing()
    elif args.fg_post_spyder:
        run_all.run_fg_post_main()
    elif args.fg_get_playwright:
        await run_all.run_fg_get_main()
    else:
       run_all.main()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FGScraper.')
    parser.add_argument('--post-processing', required=False, action='store_true',
                    help='If running just post-processing.')
    parser.add_argument('--fg-post-spyder', required=False, action='store_true',
                    help='If running just post-spyder.')
    parser.add_argument('--fg-get-playwright', required=False, action='store_true',
                    help='If running just post-spyder.')
    args = parser.parse_args()

    asyncio.run(run_conditional())

