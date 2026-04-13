"""ICPSR crawler for qualitative research datasets.

Flow:
1. Load pre-collected qualitative study IDs (from search page scrape)
2. For each study, fetch DCAT JSON metadata via /pcms/dcat/{id}
3. Insert metadata into DB (PROJECTS, KEYWORDS, PERSON_ROLE, LICENSES)
4. Download files using format priority: Qualitative Data > Delimited > ASCII > first available
"""

from playwright.sync_api import sync_playwright
from pathlib import Path
import zipfile
import json
import time
import re
import logging
import getpass
from typing import Optional, List, Tuple
from datetime import datetime
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("archive_root/logs/crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

REPOSITORY_ID = 15
REPOSITORY_URL = "https://www.icpsr.umich.edu"
DOWNLOAD_REPOSITORY_FOLDER = "icpsr"
DOWNLOAD_METHOD = "API-CALL"
DCAT_URL = "https://pcms.icpsr.umich.edu/pcms/dcat/{study_id}"
STUDY_IDS_FILE = Path("archive_root/metadata/icpsr/qualitative_study_ids.json")

LOGIN_URL = (
    "https://login.icpsr.umich.edu/realms/icpsr/protocol/openid-connect/auth?"
    "client_id=icpsr-archonnex-prod-authx&response_type=code&login=true"
    "&redirect_uri=https://www.icpsr.umich.edu/web/oauth/callback"
)

SEARCH_URL = (
    "https://www.icpsr.umich.edu/web/ICPSR/search/studies?"
    "start=0&sort=score%20desc%2CTITLE_SORT%20asc"
    "&ARCHIVE=ICPSR&PUBLISH_STATUS=PUBLISHED"
    "&DATAKIND_FACET=qualitative&rows=50"
)

# Priority order for download format selection
FORMAT_PRIORITY = ["Qualitative Data", "Delimited", "ASCII"]


def collect_study_ids(output_file: Path = STUDY_IDS_FILE) -> List[str]:
    """Scrape qualitative study IDs from ICPSR search page."""
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"
    )

    logger.info("Loading ICPSR search page...")
    page.goto(SEARCH_URL, wait_until="networkidle", timeout=60000)
    time.sleep(3)

    show_all = page.locator('a:has-text("Show all")')
    if show_all.count() > 0:
        logger.info("Clicking 'Show all'...")
        show_all.first.click()
        time.sleep(5)
        page.wait_for_load_state("networkidle", timeout=120000)

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
    logger.info(f"Found {len(study_ids)} qualitative study IDs")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(study_ids, f, indent=2)

    return study_ids


def load_study_ids(filepath: Path = STUDY_IDS_FILE) -> List[str]:
    if not filepath.exists():
        return []
    with open(filepath) as f:
        return json.load(f)


def _pick_distribution(distributions: list) -> Optional[dict]:
    """Pick the best distribution based on format priority."""
    if not distributions:
        return None

    format_map = {d.get("format", ""): d for d in distributions}

    for fmt in FORMAT_PRIORITY:
        if fmt in format_map:
            return format_map[fmt]

    # Fallback: first available
    return distributions[0]


class ICPSRCrawler:
    """Crawler for ICPSR using DCAT JSON metadata API."""

    def __init__(self, db, download_dir: str = "archive_root/downloads/icpsr"):
        self.db = db
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.browser = None
        self.page = None
        self.logged_in = False

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
            self.logged_in = False
            logger.info("Browser stopped")

    def _login(self):
        """Log in to ICPSR using credentials prompted at runtime."""
        print("\n--- ICPSR Login ---")
        username = input("ICPSR username (email): ")
        password = getpass.getpass("ICPSR password: ")

        self.page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
        time.sleep(2)

        # Click "Sign in with email" to reveal the username/password form
        sign_in_email = self.page.locator('#kc-emaillogin')
        if sign_in_email.is_visible():
            sign_in_email.click()
            time.sleep(2)

        self.page.fill("#username", username)
        self.page.fill("#password", password)
        self.page.locator("input[type=submit]").click()

        time.sleep(5)
        self.page.wait_for_load_state("networkidle", timeout=30000)

        # Check if login succeeded (should redirect away from login page)
        if "login.icpsr" in self.page.url:
            logger.error("Login failed — still on login page")
            self.logged_in = False
        else:
            logger.info("Login successful")
            self.logged_in = True

    def _fetch_dcat(self, study_id: str) -> Optional[dict]:
        """Fetch DCAT JSON for a single study."""
        url = DCAT_URL.format(study_id=study_id)
        try:
            self.page.goto(url, wait_until="networkidle", timeout=30000)
            time.sleep(1)
            text = self.page.locator("body").inner_text()
            return json.loads(text)
        except Exception as e:
            logger.error(f"Failed to fetch DCAT for {study_id}: {e}")
            return None

    def _extract_version(self, dcat: dict) -> Optional[str]:
        """Extract version from DOI URL like https://doi.org/10.3886/ICPSR34347.v1"""
        doi = dcat.get("@id", "")
        m = re.search(r"\.(v\d+)$", doi, re.IGNORECASE)
        return m.group(1).upper() if m else None

    def _download_file(self, download_url: str, dataset_dir: Path) -> Tuple[str, Path, str]:
        """Download a file. Returns (filename, filepath, status)."""
        try:
            with self.page.expect_download(timeout=60000) as dl_info:
                self.page.goto(download_url, timeout=30000)

            download = dl_info.value
            filename = download.suggested_filename
            filepath = dataset_dir / filename
            download.save_as(filepath)

            logger.info(f"Downloaded: {filename}")
            return filename, filepath, "SUCCEEDED"

        except Exception as e:
            if "login.icpsr" in self.page.url:
                logger.warning(f"Login required for download: {download_url}")
                return "", Path(), "FAILED_LOGIN_REQUIRED"
            else:
                logger.warning(f"Download failed: {e}")
                return "", Path(), "FAILED_SERVER_UNRESPONSIVE"

    def _insert_files_from_zip(self, project_id: int, zip_path: Path):
        """List files inside a zip and insert each as a FILES row."""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for entry in zf.infolist():
                    if not entry.is_dir():
                        file_type = Path(entry.filename).suffix.lstrip(".").lower()
                        self.db.insert_file(project_id, entry.filename, file_type, "SUCCEEDED")
        except zipfile.BadZipFile:
            logger.warning(f"Not a valid zip file: {zip_path}")

    def _accept_terms_if_needed(self):
        """Click 'I Agree' on terms page if it appears."""
        try:
            agree_btn = self.page.locator("button:has-text('I Agree'), input[value='I Agree']")
            if agree_btn.count() > 0:
                agree_btn.first.click()
                time.sleep(3)
                self.page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass

    def crawl(self, limit: int = None):
        # Load or collect study IDs
        study_ids = load_study_ids()
        if not study_ids:
            logger.info("No cached study IDs found, collecting from search...")
            study_ids = collect_study_ids()

        if not study_ids:
            logger.error("No study IDs found")
            return

        if limit:
            study_ids = study_ids[:limit]

        logger.info(f"Processing {len(study_ids)} studies")
        self._start_browser()
        self._login()

        if not self.logged_in:
            logger.error("Cannot proceed without login")
            self._stop_browser()
            return

        processed = 0
        for study_id in tqdm(study_ids, desc="Processing studies", unit="study"):
            project_url = f"{REPOSITORY_URL}/web/ICPSR/studies/{study_id}"

            if self.db.project_exists(project_url):
                continue

            dcat = self._fetch_dcat(study_id)
            if not dcat:
                continue

            try:
                download_date = datetime.now().isoformat()
                version = self._extract_version(dcat)

                project_data = {
                    "repository_id": REPOSITORY_ID,
                    "repository_url": REPOSITORY_URL,
                    "project_url": project_url,
                    "title": dcat.get("title", ""),
                    "description": dcat.get("description", ""),
                    "doi": dcat.get("@id"),
                    "upload_date": dcat.get("issued"),
                    "version": version,
                    "download_date": download_date,
                    "download_repository_folder": DOWNLOAD_REPOSITORY_FOLDER,
                    "download_project_folder": study_id,
                    "download_version_folder": version,
                    "download_method": DOWNLOAD_METHOD,
                }

                project_id = self.db.insert_project(**project_data)

                # Keywords
                for keyword in dcat.get("keywords", []):
                    self.db.insert_keyword(project_id, keyword)

                # Creators as AUTHOR
                for creator in dcat.get("creator", []):
                    name = creator.get("name", "")
                    if name:
                        self.db.insert_person_role(project_id, name, "AUTHOR")

                # License
                self.db.insert_license(project_id, "ICPSR Terms of Use")

                # Download file
                distributions = dcat.get("distribution", [])
                dist = _pick_distribution(distributions)

                if dist and dist.get("downloadURL"):
                    dataset_dir = self.download_dir / study_id
                    dataset_dir.mkdir(exist_ok=True)

                    # Navigate to download URL — may hit terms page first
                    self.page.goto(dist["downloadURL"], timeout=30000)
                    time.sleep(2)
                    self._accept_terms_if_needed()

                    filename, filepath, status = self._download_file(
                        dist["downloadURL"], dataset_dir
                    )

                    if status == "SUCCEEDED" and filepath.suffix.lower() == ".zip":
                        # Insert the zip file itself
                        self.db.insert_file(project_id, filename, "zip", "SUCCEEDED")
                        # Also list files inside zip, insert each as a row
                        self._insert_files_from_zip(project_id, filepath)
                        tqdm.write(f"  ✓ [{dist.get('format', '?')}] {filename}")
                    elif status == "SUCCEEDED":
                        file_type = filepath.suffix.lstrip(".").lower()
                        self.db.insert_file(project_id, filename, file_type, status)
                        tqdm.write(f"  ✓ [{dist.get('format', '?')}] {filename}")
                    else:
                        self.db.insert_file(project_id, "", "", status)
                        tqdm.write(f"  ✗ [{status}] {dcat.get('title', study_id)[:60]}")
                else:
                    # No distribution links at all
                    self.db.insert_file(project_id, "", "", "FAILED_SERVER_UNRESPONSIVE")
                    tqdm.write(f"  ✗ [no download links] {dcat.get('title', study_id)[:60]}")

                processed += 1

            except Exception as e:
                logger.error(f"Failed to process study {study_id}: {e}")

            time.sleep(0.5)

        self._stop_browser()
        logger.info(f"Crawl complete: {processed} studies processed")

    def resume(self, delay: float = 0.5):
        """Resume incomplete downloads."""
        incomplete = self.db.get_incomplete_downloads(REPOSITORY_ID)
        no_files = self.db.get_projects_without_files(REPOSITORY_ID)
        to_retry = incomplete + no_files

        if not to_retry:
            logger.info("Nothing to resume")
            return

        logger.info(f"Resuming {len(to_retry)} studies")
        self._start_browser()
        self._login()

        if not self.logged_in:
            logger.error("Cannot proceed without login")
            self._stop_browser()
            return

        for index, record in enumerate(to_retry, 1):
            project_url = record["project_url"]
            project_id = record["id"]
            study_id = record["download_project_folder"]

            logger.info(f"[{index}/{len(to_retry)}] Resuming: {project_url}")

            try:
                dcat = self._fetch_dcat(study_id)
                if not dcat:
                    continue

                distributions = dcat.get("distribution", [])
                dist = _pick_distribution(distributions)

                if dist and dist.get("downloadURL"):
                    dataset_dir = self.download_dir / study_id
                    dataset_dir.mkdir(parents=True, exist_ok=True)

                    self.db.delete_files_for_project(project_id)

                    self.page.goto(dist["downloadURL"], timeout=30000)
                    time.sleep(2)
                    self._accept_terms_if_needed()

                    filename, filepath, status = self._download_file(
                        dist["downloadURL"], dataset_dir
                    )

                    if status == "SUCCEEDED" and filepath.suffix.lower() == ".zip":
                        self.db.insert_file(project_id, filename, "zip", "SUCCEEDED")
                        self._insert_files_from_zip(project_id, filepath)
                        logger.info(f"[{index}/{len(to_retry)}] Downloaded: {filename}")
                    elif status == "SUCCEEDED":
                        file_type = filepath.suffix.lstrip(".").lower()
                        self.db.insert_file(project_id, filename, file_type, status)
                        logger.info(f"[{index}/{len(to_retry)}] Downloaded: {filename}")
                    else:
                        self.db.insert_file(project_id, "", "", status)
                        logger.info(f"[{index}/{len(to_retry)}] {status}")

            except Exception as e:
                logger.error(f"[{index}/{len(to_retry)}] Failed: {e}")

            time.sleep(delay)

        self._stop_browser()
