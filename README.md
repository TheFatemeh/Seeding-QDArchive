# QDArchive – Part 1: Data Acquisition

**Student Name:** Seyedeh Fatemeh Ahmadi

## Overview

Python crawler that discovers qualitative research datasets from online repositories,
extracts metadata, and downloads openly available data files into a local archive.

Currently supported repositories:
- **UK Data Service** (`datacatalogue.ukdataservice.ac.uk`)

## Project Structure

```
├── main.py                  # CLI entry point (new / resume modes)
├── crawler/
│   ├── ukdataservice_crawler.py   # UK Data Service crawler
│   └── search_queries.txt         # search terms (one per line)
├── database/
│   └── db.py                      # SQLite wrapper
├── archive_root/
│   ├── downloads/                 # downloaded dataset files
│   ├── metadata/                  # crawl.sqlite database
│   └── logs/                      # crawler.log
├── environment.yml                # conda environment spec
```

## Setup

```bash
# 1. Create and activate the conda environment
conda env create -f environment.yml
conda activate qda-crawler

# 2. Install the Playwright browser
playwright install chromium
```

## Usage

```bash
# Fresh crawl — clears previous data and runs from scratch
python main.py new ukdataservice

# Resume — retries incomplete downloads only
python main.py resume ukdataservice
```

### Search Queries

Edit `crawler/search_queries.txt` to add or change search terms (one per line, `#` for comments).

## How It Works

1. For each query in `search_queries.txt`, the crawler searches the UK Data Service catalogue and paginates through all result pages.
2. All discovered study URLs are deduplicated into a single set.
3. For each study URL, the crawler visits the page, extracts metadata (title, DOI, authors, access level, abstract, keywords, license), and saves the page HTML.
4. If the dataset has **open** access, it clicks the download buttons (EXCEL, SPSS, STATA, TAB, etc.) and saves the files locally.
5. All metadata is stored in a SQLite database at `archive_root/metadata/crawl.sqlite`.
