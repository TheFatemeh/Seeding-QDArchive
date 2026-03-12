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
import json
import time
import logging
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

    def _check_access_level(self, soup: BeautifulSoup, metadata: dict) -> bool:
        access_span = soup.find("span", string=lambda value: value and value.strip() == "Access")
        if not access_span:
            metadata["download_status"] = "unknown"
            return False

        parent = access_span.find_parent("div")
        if parent:
            container = parent.find_parent("div")
            if container:
                access_p = container.find("p")
                if access_p:
                    access_text = access_p.get_text(" ", strip=True).lower()

                    if "open" in access_text:
                        metadata["download_status"] = "open"

                        access_section = soup.find("div", {"data-testid": "access-section"})
                        if access_section:
                            license_links = access_section.find_all("a")
                            for link in license_links:
                                link_text = link.get_text(strip=True)
                                if any(token in link_text for token in ["Licence", "License", "Creative Commons"]):
                                    metadata["license"] = link_text
                                    break

                        return True

                    if "safeguarded" in access_text:
                        metadata["download_status"] = "restricted_safeguarded"
                        metadata["license"] = "Safeguarded"
                    elif "controlled" in access_text:
                        metadata["download_status"] = "restricted_controlled"
                        metadata["license"] = "Controlled"

        metadata["download_status"] = "unknown"
        return False

    def _download_via_buttons(self, dataset_dir: Path) -> list:
        filenames = []
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

                filenames.append(filename)
                logger.info(f"Downloaded: {filename}")
                time.sleep(1)

            except Exception as error:
                logger.warning(f"Failed to download from button: {error}")

        return filenames

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
            dataset_url = f"{self.BASE_URL}/studies/study/{dataset_id}"

            if self.db.dataset_exists(dataset_url):
                continue

            metadata = {
                "dataset_page_url": dataset_url,
                "repository_name": "UK Data Service",
                "dataset_title": oai_metadata.get("title", ""),
                "dataset_description": oai_metadata.get("description", ""),
            }

            if oai_metadata.get("creators"):
                metadata["author_name"] = json.dumps(oai_metadata["creators"])

            if oai_metadata.get("subjects"):
                metadata["keywords"] = json.dumps(oai_metadata["subjects"])

            try:
                self.page.goto(dataset_url, wait_until="networkidle", timeout=30000)
                time.sleep(1)

                html = self.page.content()
                soup = BeautifulSoup(html, "html.parser")

                is_open = self._check_access_level(soup, metadata)

                dataset_dir = self.download_dir / f"ukds_{dataset_id}"
                dataset_dir.mkdir(exist_ok=True)
                metadata["local_directory"] = str(dataset_dir)

                if is_open:
                    downloaded_files = self._download_via_buttons(dataset_dir)
                    metadata["file_count"] = len(downloaded_files)

                    if downloaded_files:
                        metadata["download_status"] = "successful"
                        file_types = list({Path(name).suffix.lower() for name in downloaded_files if Path(name).suffix})
                        metadata["file_types"] = json.dumps(sorted(file_types))

                        for filename in downloaded_files:
                            ext = Path(filename).suffix.lower()
                            if ext in self.qda_extensions:
                                metadata["qda_file_url"] = dataset_url
                                metadata["qda_local_filename"] = filename
                                break
                    else:
                        metadata["download_status"] = "no_files"
                else:
                    metadata["file_count"] = 0

                (dataset_dir / "metadata.json").write_text(
                    json.dumps(metadata, indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )

                self.db.insert_dataset(**metadata)
                processed += 1

                if metadata.get("download_status") == "successful":
                    tqdm.write(f"  ✓ {metadata.get('file_count', 0)} files: {metadata.get('dataset_title', dataset_id)[:60]}")
                if metadata.get("qda_local_filename"):
                    tqdm.write(f"  ★ QDA: {metadata['qda_local_filename']}")

            except Exception as error:
                logger.error(f"Failed to process dataset {dataset_id}: {error}")
                metadata["download_status"] = "failed"
                try:
                    self.db.insert_dataset(**metadata)
                except Exception:
                    pass

            time.sleep(0.5)

        self._stop_browser()
        logger.info(f"Crawl complete: {processed} datasets processed")

    def resume(self, delay: float = 0.5):
        incomplete = self.db.get_incomplete_downloads("UK Data Service")

        if not incomplete:
            logger.info("Nothing to resume")
            return

        logger.info(f"Resuming {len(incomplete)} datasets")
        self._start_browser()

        for index, record in enumerate(incomplete, 1):
            dataset_url = record["dataset_page_url"]
            dataset_id = record["id"]

            logger.info(f"[{index}/{len(incomplete)}] Resuming: {dataset_url}")

            try:
                local_dir = record.get("local_directory")
                if local_dir:
                    dataset_dir = Path(local_dir)
                else:
                    study_id = dataset_url.rstrip("/").split("/")[-1]
                    dataset_dir = self.download_dir / f"ukds_{study_id}"
                dataset_dir.mkdir(parents=True, exist_ok=True)

                self.page.goto(dataset_url, wait_until="networkidle", timeout=30000)
                time.sleep(2)

                downloaded_files = self._download_via_buttons(dataset_dir)

                if downloaded_files:
                    file_types = list({Path(name).suffix.lower() for name in downloaded_files if Path(name).suffix})
                    updates = {
                        "download_status": "successful",
                        "file_count": len(downloaded_files),
                        "file_types": json.dumps(sorted(file_types)),
                    }
                    for filename in downloaded_files:
                        ext = Path(filename).suffix.lower()
                        if ext in self.qda_extensions:
                            updates["qda_file_url"] = dataset_url
                            updates["qda_local_filename"] = filename
                            break
                    self.db.update_download(dataset_id, **updates)
                    logger.info(f"[{index}/{len(incomplete)}] Downloaded {len(downloaded_files)} files")
                else:
                    logger.info(f"[{index}/{len(incomplete)}] No files")

            except Exception as error:
                logger.error(f"[{index}/{len(incomplete)}] Failed: {error}")

            time.sleep(delay)

        self._stop_browser()
