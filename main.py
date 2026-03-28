#!/usr/bin/env python3
"""
Qualitative Research Data Crawler

Usage:
    python main.py <mode> <crawler> [crawler2 ...]

Arguments:
    mode        'new' or 'resume'
    crawler     One or more crawler names: ukdataservice, databrary

Examples:
    python main.py new ukdataservice
    python main.py resume ukdataservice
    python main.py new ukdataservice databrary
"""

import argparse
import shutil
from pathlib import Path

from database import Database
from crawler import UKDataServiceCrawler

# Map crawler names to their classes and download directories
CRAWLERS = {
    "ukdataservice": {
        "class": UKDataServiceCrawler,
        "download_dir": "archive_root/downloads/ukdataservice",
        "repository_id": 3,
    },
    # "databrary": {
    #     "class": DataBraryCrawler,
    #     "download_dir": "archive_root/downloads/databrary",
    #     "repository_id": ...,
    # },
}


def run_new(crawler_name: str, db: Database):
    """Delete old data and run a fresh crawl."""
    config = CRAWLERS[crawler_name]
    download_dir = Path(config["download_dir"])

    # Clean up downloads
    if download_dir.exists():
        shutil.rmtree(download_dir)
        print(f"  Deleted: {download_dir}")
    download_dir.mkdir(parents=True, exist_ok=True)

    # Clean up database entries
    db.delete_by_repository(config["repository_id"])
    print(f"  Deleted DB records for repository_id: {config['repository_id']}")

    # Run crawler
    crawler = config["class"](db, download_dir=str(download_dir))
    crawler.crawl()


def run_resume(crawler_name: str, db: Database):
    """Resume incomplete downloads only."""
    config = CRAWLERS[crawler_name]
    download_dir = Path(config["download_dir"])
    download_dir.mkdir(parents=True, exist_ok=True)

    crawler = config["class"](db, download_dir=str(download_dir))
    crawler.resume()


def main():
    parser = argparse.ArgumentParser(
        description="Qualitative Research Data Crawler"
    )
    parser.add_argument(
        "mode",
        choices=["new", "resume"],
        help="'new' = fresh crawl (deletes old data), 'resume' = retry incomplete downloads"
    )
    parser.add_argument(
        "crawlers",
        nargs="+",
        choices=list(CRAWLERS.keys()),
        help="Crawler(s) to run"
    )
    args = parser.parse_args()

    db = Database()

    for crawler_name in args.crawlers:
        print(f"\n=== {crawler_name} ({args.mode}) ===\n")

        if args.mode == "new":
            run_new(crawler_name, db)
        else:
            run_resume(crawler_name, db)

    db.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
