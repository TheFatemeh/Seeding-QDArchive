# QDArchive -- Part 1: Data Acquisition

Python crawler for qualitative research datasets.

Currently supported repositories:
- UK Data Service (`datacatalogue.ukdataservice.ac.uk`)
- ICPSR (`www.icpsr.umich.edu`)

## Project Structure

```
├── main.py
├── crawler/
│   ├── __init__.py
│   ├── workflow.md                 # access conditions and decisions per repository
│   ├── ukdataservice/
│   │   ├── __init__.py
│   │   ├── crawler.py              # crawler logic for UK Data Service
│   │   └── oai_index.py            # OAI-PMH XML parsing + qualitative filter
│   └── icpsr/
│       ├── __init__.py
│       └── crawler.py              # crawler logic for ICPSR
├── database/
│   └── db.py                       # multi-table SQLite schema (PROJECTS, FILES, KEYWORDS, PERSON_ROLE, LICENSES)
├── archive_root/
│   ├── downloads/
│   │   ├── ukdataservice/
│   │   └── icpsr/
│   ├── metadata/
│   │   ├── ukdataservice/
│   │   │   ├── oai_batches/        # auto-downloaded OAI batch_*.xml files
│   │   │   └── oai_metadata_index.json
│   │   └── icpsr/
│   │       └── qualitative_study_ids.json
│   └── logs/
├── scripts/
│   └── icpsr_collect_ids.py        # standalone script to collect ICPSR qualitative study IDs
├── docs/
│   └── crawling-strategy.md
├── 23724707-seeding.sqlite
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
# Fresh crawl (deletes old data for that crawler only)
python main.py new ukdataservice
python main.py new icpsr

# Resume incomplete downloads
python main.py resume ukdataservice
python main.py resume icpsr
```

ICPSR requires login -- you will be prompted for your credentials when the crawler starts.

## Access Conditions

See [crawler/workflow.md](crawler/workflow.md) for details on which access levels are used per repository and which ones are included in theis project.

## UK Data Service Pipeline

1. Read OAI-PMH batch XML files from `archive_root/metadata/ukdataservice/oai_batches/`.
2. If they are missing, download all batches automatically from the OAI-PMH endpoint.
3. Build an in-memory metadata index.
4. Apply qualitative filter rule on `dc:type`:
	- exclude only when type is exactly `Numeric` and nothing else
	- include mixed types (e.g., `Numeric` + `Text`)
5. For each included dataset ID, visit the dataset page directly.
6. Check access level and download files for open datasets.
7. Store metadata in `23724707-seeding.sqlite`.

## ICPSR Pipeline

1. Load pre-collected qualitative study IDs (filtered via ICPSR search with `dataType=qualitative`).
2. For each study, fetch metadata via the DCAT JSON API (`/pcms/dcat/{id}`).
3. Log in with ICPSR credentials (prompted at runtime).
4. Download the file (always a zip).
5. List all files inside the zip and record each in the FILES table — zip is kept on disk, not extracted.
6. Store metadata in `23724707-seeding.sqlite`.
