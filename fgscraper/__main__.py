from fgscraper import run_all
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FGScraper.')
    parser.add_argument('--post-processing', required=False, action='store_true',
                    help='If running just post-processing.')
    args = parser.parse_args()
    if args.post_processing:
        run_all.run_post_processing()
    else:
        run_all.main()

