"""UK Data Service crawler for qualitative research datasets.

Simplified architecture using pre-harvested OAI-PMH metadata:
1. Load OAI metadata index (all ~10K datasets, pre-filtered by type)
2. For each qualitative dataset:
   - Visit page directly (using dataset ID)
   - Check if open access
   - Download files if available
   - Detect QDA files

No web search needed - full metadata already in OAI batches.
"""

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Optional, Tuple, List
import json
import time
import logging
from datetime import datetime
from tqdm import tqdm

from .oai_index import load_oai_metadata_index, save_index_to_json, load_index_from_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("archive_root/logs/crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

EXTENSIONS_FILE = Path(__file__).parent.parent.parent / "extensions.csv"

REPOSITORY_ID = 3
REPOSITORY_URL = "https://datacatalogue.ukdataservice.ac.uk"
DOWNLOAD_REPOSITORY_FOLDER = "ukdataservice"
DOWNLOAD_METHOD = "SCRAPING"


def load_qda_extensions() -> set:
    """Load QDA file extensions from extensions.csv (lowercase, with leading dot)."""
    extensions = set()
    if EXTENSIONS_FILE.exists():
        with open(EXTENSIONS_FILE) as f:
            for line in f:
                ext = line.strip()
                if ext and ext != "types":
                    ext = ext if ext.startswith(".") else "." + ext
                    extensions.add(ext.lower())
    return extensions


class UKDataServiceCrawler:
    """Crawler for UK Data Service using OAI-PMH metadata index."""

    BASE_URL = "https://datacatalogue.ukdataservice.ac.uk"

    def __init__(self, db, download_dir: str = "archive_root/downloads/ukdataservice"):
        self.db = db
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.qda_extensions = load_qda_extensions()
        self.browser = None
        self.page = None

    def _start_browser(self):
        if not self.browser:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(headless=True)
            self.page = self.browser.new_page(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"
            )
            logger.info("Browser started")

    def _stop_browser(self):
        if self.browser:
            self.browser.close()
            self.playwright.stop()
            self.browser = None
            logger.info("Browser stopped")

    def _check_access_level(self, soup: BeautifulSoup) -> Tuple[bool, Optional[str]]:
        """Check access level. Returns (is_open, license_string)."""
        license_str = None
        access_span = soup.find("span", string=lambda value: value and value.strip() == "Access")
        if not access_span:
            return False, None

        parent = access_span.find_parent("div")
        if parent:
            container = parent.find_parent("div")
            if container:
                access_p = container.find("p")
                if access_p:
                    access_text = access_p.get_text(" ", strip=True).lower()

                    if "open" in access_text:
                        access_section = soup.find("div", {"data-testid": "access-section"})
                        if access_section:
                            license_links = access_section.find_all("a")
                            for link in license_links:
                                link_text = link.get_text(strip=True)
                                if link_text:
                                    license_str = link_text
                                    break
                        return True, license_str

                    if "safeguarded" in access_text:
                        return False, "Safeguarded"
                    elif "controlled" in access_text:
                        return False, "Controlled"

        return False, None

    def _extract_doi(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract DOI URL from the page."""
        doi_link = soup.find("a", href=lambda h: h and "doi.org/" in h)
        if doi_link:
            return doi_link["href"]
        return None

    def _download_via_buttons(self, dataset_dir: Path) -> List[Tuple[str, str]]:
        """Download files via buttons. Returns list of (filename, status) tuples."""
        results = []
        self.page.context.set_default_timeout(60000)

        buttons = self.page.locator("div[data-testid='access-section'] button").all()

        for button in buttons:
            try:
                with self.page.expect_download(timeout=15000) as download_info:
                    button.click()

                download = download_info.value
                filename = download.suggested_filename
                filepath = dataset_dir / filename
                download.save_as(filepath)

                results.append((filename, "SUCCEEDED"))
                logger.info(f"Downloaded: {filename}")
                time.sleep(1)

            except Exception as error:
                logger.warning(f"Failed to download from button: {error}")
                results.append(("unknown", "FAILED_SERVER_UNRESPONSIVE"))

        return results

    def crawl(self, limit: int = None):
        index_file = Path("archive_root/metadata/ukdataservice/oai_metadata_index.json")
        if index_file.exists():
            logger.info("Loading metadata index from cache...")
            metadata_index = load_index_from_json(index_file)
        else:
            logger.info("Building metadata index from OAI batch files...")
            metadata_index = load_oai_metadata_index()
            if metadata_index:
                save_index_to_json(metadata_index, index_file)

        if not metadata_index:
            logger.error("No metadata in index")
            return

        all_ids = list(metadata_index.keys())
        if limit:
            all_ids = all_ids[:limit]

        logger.info(f"Processing {len(all_ids)} datasets")
        self._start_browser()

        processed = 0
        for dataset_id in tqdm(all_ids, desc="Processing datasets", unit="dataset"):
            oai_metadata = metadata_index[dataset_id]
            project_url = f"{self.BASE_URL}/studies/study/{dataset_id}"

            if self.db.project_exists(project_url):
                continue

            try:
                self.page.goto(project_url, wait_until="networkidle", timeout=30000)
                time.sleep(1)

                html = self.page.content()
                soup = BeautifulSoup(html, "html.parser")

                is_open, license_str = self._check_access_level(soup)
                doi = self._extract_doi(soup)

                dataset_dir = self.download_dir / dataset_id
                dataset_dir.mkdir(exist_ok=True)

                download_date = datetime.now().isoformat()

                project_data = {
                    "repository_id": REPOSITORY_ID,
                    "repository_url": REPOSITORY_URL,
                    "project_url": project_url,
                    "title": oai_metadata.get("title", ""),
                    "description": oai_metadata.get("description", ""),
                    "language": oai_metadata.get("language"),
                    "doi": doi,
                    "upload_date": oai_metadata.get("datestamp"),
                    "download_date": download_date,
                    "download_repository_folder": DOWNLOAD_REPOSITORY_FOLDER,
                    "download_project_folder": dataset_id,
                    "download_method": DOWNLOAD_METHOD,
                }

                project_id = self.db.insert_project(**project_data)

                # Insert keywords from OAI subjects
                for subject in oai_metadata.get("subjects", []):
                    self.db.insert_keyword(project_id, subject)

                # Insert creators as AUTHOR
                for creator in oai_metadata.get("creators", []):
                    self.db.insert_person_role(project_id, creator, "AUTHOR")

                # Insert license
                if license_str:
                    self.db.insert_license(project_id, license_str)

                if is_open:
                    file_results = self._download_via_buttons(dataset_dir)

                    for filename, status in file_results:
                        file_type = Path(filename).suffix.lstrip(".").lower() if Path(filename).suffix else ""
                        self.db.insert_file(project_id, filename, file_type, status)

                    succeeded = [f for f, s in file_results if s == "SUCCEEDED"]
                    if succeeded:
                        tqdm.write(f"  ✓ {len(succeeded)} files: {oai_metadata.get('title', dataset_id)[:60]}")
                        for filename in succeeded:
                            ext = Path(filename).suffix.lower()
                            if ext in self.qda_extensions:
                                tqdm.write(f"  ★ QDA: {filename}")
                                break
                else:
                    # Restricted access - record as FAILED_LOGIN_REQUIRED
                    self.db.insert_file(project_id, "", "", "FAILED_LOGIN_REQUIRED")

                processed += 1

            except Exception as error:
                logger.error(f"Failed to process dataset {dataset_id}: {error}")

            time.sleep(0.5)

        self._stop_browser()
        logger.info(f"Crawl complete: {processed} datasets processed")

    def resume(self, delay: float = 0.5):
        incomplete = self.db.get_incomplete_downloads(REPOSITORY_ID)
        no_files = self.db.get_projects_without_files(REPOSITORY_ID)
        to_retry = incomplete + no_files

        if not to_retry:
            logger.info("Nothing to resume")
            return

        logger.info(f"Resuming {len(to_retry)} datasets")
        self._start_browser()

        for index, record in enumerate(to_retry, 1):
            project_url = record["project_url"]
            project_id = record["id"]
            dataset_id = record["download_project_folder"]

            logger.info(f"[{index}/{len(to_retry)}] Resuming: {project_url}")

            try:
                dataset_dir = self.download_dir / dataset_id
                dataset_dir.mkdir(parents=True, exist_ok=True)

                self.page.goto(project_url, wait_until="networkidle", timeout=30000)
                time.sleep(2)

                # Remove old failed file records
                self.db.delete_files_for_project(project_id)

                file_results = self._download_via_buttons(dataset_dir)

                for filename, status in file_results:
                    file_type = Path(filename).suffix.lstrip(".").lower() if Path(filename).suffix else ""
                    self.db.insert_file(project_id, filename, file_type, status)

                succeeded_count = sum(1 for _, s in file_results if s == "SUCCEEDED")
                if succeeded_count:
                    logger.info(f"[{index}/{len(to_retry)}] Downloaded {succeeded_count} files")
                else:
                    logger.info(f"[{index}/{len(to_retry)}] No files")

            except Exception as error:
                logger.error(f"[{index}/{len(to_retry)}] Failed: {error}")

            time.sleep(delay)

        self._stop_browser()
