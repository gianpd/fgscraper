from fgscraper import run_all
import argparse
import asyncio

from wasabi import msg

async def async_run_conditional(args):
    if args.post_processing:
        run_all.run_post_processing()
    elif args.fg_post_spyder:
        run_all.run_fg_post_main()
    elif args.fg_get_playwright:
        await run_all.run_fg_get_main()


def synch_run_main():
    run_all.main()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FGScraper.')
    parser.add_argument('--run', required=False, action='store_true',
                    help='If running just post-processing.')
    parser.add_argument('--post-processing', required=False, action='store_true',
                    help='If running just post-processing.')
    parser.add_argument('--fg-post-spyder', required=False, action='store_true',
                    help='If running just post-spyder.')
    parser.add_argument('--fg-get-playwright', required=False, action='store_true',
                    help='If running just post-spyder.')
    args = parser.parse_args()

    if not args.run:
        msg.good('Running ASYNC run conditional ...')
        asyncio.run(async_run_conditional(args))
    else:
        msg.good('Running SYNC run conditional ...')
        synch_run_main()

