"""Collect all qualitative study IDs from ICPSR search."""

from playwright.sync_api import sync_playwright
import time
import re
import json

SEARCH_URL = (
    "https://www.icpsr.umich.edu/web/ICPSR/search/studies?"
    "start=0&sort=score%20desc%2CTITLE_SORT%20asc"
    "&ARCHIVE=ICPSR&PUBLISH_STATUS=PUBLISHED"
    "&DATAKIND_FACET=qualitative&rows=50"
)
OUTPUT_FILE = "archive_root/metadata/icpsr/qualitative_study_ids.json"


def collect_ids():
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"
    )

    print(f"Loading search page...")
    page.goto(SEARCH_URL, wait_until="networkidle", timeout=60000)
    time.sleep(3)

    # Click "Show all" to load all results on one page
    show_all = page.locator('a:has-text("Show all")')
    if show_all.count() > 0:
        print("Clicking 'Show all'...")
        show_all.first.click()
        time.sleep(5)
        page.wait_for_load_state("networkidle", timeout=120000)

    # Extract study IDs from links
    links = page.locator('a[href*="/studies/"]').all()
    study_ids = set()
    for link in links:
        href = link.get_attribute("href") or ""
        m = re.search(r"/studies/(\d+)", href)
        if m:
            study_ids.add(m.group(1))

    browser.close()
    p.stop()

    study_ids = sorted(study_ids, key=int)
    print(f"Found {len(study_ids)} qualitative study IDs")

    from pathlib import Path
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(study_ids, f, indent=2)

    print(f"Saved to {OUTPUT_FILE}")
    return study_ids


if __name__ == "__main__":
    collect_ids()
