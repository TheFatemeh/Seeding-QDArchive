"""Build metadata index from OAI-PMH XML batches.

Reads all harvested batch files and extracts qualified (non-numeric-only)
dataset metadata. Returns a dict indexed by dataset ID.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
import json
import logging
from urllib.parse import urlencode
from urllib.request import urlopen

logger = logging.getLogger(__name__)

NAMESPACES = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
}

OAI_ENDPOINT = "https://oai.ukdataservice.ac.uk/oai/provider"
DEFAULT_BATCH_DIR = Path("archive_root/metadata/ukdataservice/oai_batches")
DEFAULT_INDEX_FILE = Path("archive_root/metadata/ukdataservice/oai_metadata_index.json")
LEGACY_BATCH_DIR = Path("ukds_metadata")


def _resolve_batch_dir(batch_dir: Path | None) -> Path:
    if batch_dir is not None:
        return batch_dir

    if DEFAULT_BATCH_DIR.exists():
        return DEFAULT_BATCH_DIR

    if LEGACY_BATCH_DIR.exists():
        return LEGACY_BATCH_DIR

    return DEFAULT_BATCH_DIR


def _extract_resumption_token(xml_content: bytes) -> str | None:
    root = ET.fromstring(xml_content)
    token_elem = root.find(".//oai:resumptionToken", NAMESPACES)
    if token_elem is None or token_elem.text is None:
        return None

    token = token_elem.text.strip()
    return token or None


def harvest_oai_batches(batch_dir: Path = None, overwrite: bool = False) -> int:
    """Download OAI-PMH ListRecords batches to local XML files.

    Returns number of batch files written.
    """
    batch_dir = _resolve_batch_dir(batch_dir)
    batch_dir.mkdir(parents=True, exist_ok=True)

    existing = sorted(batch_dir.glob("batch_*.xml"))
    if existing and not overwrite:
        return len(existing)

    if overwrite:
        for file_path in existing:
            file_path.unlink()

    params = {
        "verb": "ListRecords",
        "metadataPrefix": "oai_dc",
    }

    written_count = 0
    batch_number = 0

    while True:
        query = urlencode(params)
        url = f"{OAI_ENDPOINT}?{query}"

        try:
            with urlopen(url, timeout=60) as response:
                xml_content = response.read()
        except Exception as error:
            logger.error(f"Failed to fetch OAI batch {batch_number}: {error}")
            break

        output_file = batch_dir / f"batch_{batch_number}.xml"
        output_file.write_bytes(xml_content)
        written_count += 1

        try:
            token = _extract_resumption_token(xml_content)
        except Exception as error:
            logger.error(f"Failed to parse OAI batch {batch_number}: {error}")
            break

        if not token:
            break

        params = {
            "verb": "ListRecords",
            "resumptionToken": token,
        }
        batch_number += 1

    return written_count


def load_oai_metadata_index(batch_dir: Path = None) -> dict:
    """
    Load and parse all OAI-PMH batch XML files.

    Returns dict: {dataset_id: {title, description, creators, subjects, types, ...}}
    Only includes datasets that pass the type filter (not only-numeric).
    """
    batch_dir = _resolve_batch_dir(batch_dir)

    index = {}
    batch_files = sorted(batch_dir.glob("batch_*.xml"))

    if not batch_files:
        logger.info(f"No OAI batch files found at {batch_dir}. Downloading from OAI-PMH...")
        harvest_oai_batches(batch_dir=batch_dir)
        batch_files = sorted(batch_dir.glob("batch_*.xml"))

    if not batch_files:
        logger.warning(f"Could not load or download OAI batches in {batch_dir}")
        return {}

    logger.info(f"Loading OAI metadata from {len(batch_files)} batch files...")

    for batch_file in batch_files:
        try:
            tree = ET.parse(batch_file)
            root = tree.getroot()

            for record in root.findall(".//oai:record", NAMESPACES):
                header = record.find("oai:header", NAMESPACES)
                if header is not None and header.get("status") == "deleted":
                    continue

                id_elem = header.find("oai:identifier", NAMESPACES)
                if id_elem is None or not id_elem.text:
                    continue
                dataset_id = id_elem.text.strip()

                dc = record.find(".//oai_dc:dc", NAMESPACES)
                if dc is None:
                    continue

                metadata = _extract_dublin_core(dc)
                metadata["dataset_id"] = dataset_id

                if not _should_include_by_type(metadata.get("types", [])):
                    continue

                index[dataset_id] = metadata

        except ET.ParseError as error:
            logger.error(f"Failed to parse {batch_file}: {error}")
        except Exception as error:
            logger.error(f"Error processing {batch_file}: {error}")

    logger.info(f"Loaded {len(index)} qualified datasets (numeric-only excluded)")
    return index


def _extract_dublin_core(dc_elem) -> dict:
    metadata = {}

    title = dc_elem.find("dc:title", NAMESPACES)
    if title is not None and title.text:
        metadata["title"] = title.text.strip()

    description = dc_elem.find("dc:description", NAMESPACES)
    if description is not None and description.text:
        metadata["description"] = description.text.strip()

    date = dc_elem.find("dc:date", NAMESPACES)
    if date is not None and date.text:
        metadata["date"] = date.text.strip()

    language = dc_elem.find("dc:language", NAMESPACES)
    if language is not None and language.text:
        metadata["language"] = language.text.strip()

    rights = dc_elem.find("dc:rights", NAMESPACES)
    if rights is not None and rights.text:
        metadata["rights"] = rights.text.strip()

    metadata["creators"] = [
        element.text.strip()
        for element in dc_elem.findall("dc:creator", NAMESPACES)
        if element.text and element.text.strip()
    ]

    metadata["subjects"] = [
        element.text.strip()
        for element in dc_elem.findall("dc:subject", NAMESPACES)
        if element.text and element.text.strip()
    ]

    metadata["types"] = [
        element.text.strip()
        for element in dc_elem.findall("dc:type", NAMESPACES)
        if element.text and element.text.strip()
    ]

    metadata["formats"] = [
        element.text.strip()
        for element in dc_elem.findall("dc:format", NAMESPACES)
        if element.text and element.text.strip()
    ]

    metadata["relations"] = [
        element.text.strip()
        for element in dc_elem.findall("dc:relation", NAMESPACES)
        if element.text and element.text.strip()
    ]

    metadata["coverage"] = [
        element.text.strip()
        for element in dc_elem.findall("dc:coverage", NAMESPACES)
        if element.text and element.text.strip()
    ]

    return metadata


def _should_include_by_type(types: list) -> bool:
    if not types:
        return True

    normalized_types = [item.strip().lower() for item in types]
    if len(normalized_types) == 1 and normalized_types[0] == "numeric":
        return False

    return True


def save_index_to_json(index: dict, filepath: Path = None):
    if filepath is None:
        filepath = DEFAULT_INDEX_FILE

    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w") as file_handle:
        json.dump(index, file_handle, indent=2, ensure_ascii=False)


def load_index_from_json(filepath: Path = None) -> dict:
    if filepath is None:
        filepath = DEFAULT_INDEX_FILE

    if not filepath.exists():
        return {}

    with open(filepath) as file_handle:
        return json.load(file_handle)
