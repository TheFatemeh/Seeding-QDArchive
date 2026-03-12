# QDArchive – Part 1: Data Acquisition

Python crawler for qualitative research datasets.

Currently supported repositories:
- UK Data Service (`datacatalogue.ukdataservice.ac.uk`)

## Project Structure

```
├── main.py
├── crawler/
│   ├── __init__.py
│   └── ukdataservice/
│       ├── __init__.py
│       ├── crawler.py          # crawler logic for UK Data Service
│       └── oai_index.py        # OAI-PMH XML parsing + qualitative filter
├── database/
│   └── db.py
├── archive_root/
│   ├── downloads/
│   ├── metadata/
│   │   └── ukdataservice/
│   │       ├── oai_batches/          # auto-downloaded OAI batch_*.xml files
│   │       └── oai_metadata_index.json
│   └── logs/
├── extensions.csv
├── environment.yml
└── README.md
```

## Setup

```bash
conda env create -f environment.yml
conda activate qda-crawler
playwright install chromium
```

## Usage

```bash
# Fresh crawl (deletes old UKDS downloads + DB rows)
python main.py new ukdataservice

# Resume incomplete downloads
python main.py resume ukdataservice
```

## UK Data Service Pipeline

1. Read OAI-PMH batch XML files from `archive_root/metadata/ukdataservice/oai_batches/`.
2. If they are missing, download all batches automatically from the OAI-PMH endpoint.
3. Build an in-memory metadata index.
4. Apply qualitative filter rule on `dc:type`:
	- exclude only when type is exactly `Numeric` and nothing else
	- include mixed types (e.g., `Numeric` + `Text`)
	- include when no type is present
5. For each included dataset ID, visit the dataset page directly.
6. Check access level and download files for open datasets.
7. Store metadata in `archive_root/metadata/crawl.sqlite`.

## Notes

- The crawler does not use query-based website search anymore.
- OAI batches and `oai_metadata_index.json` are generated locally during crawling if missing.
