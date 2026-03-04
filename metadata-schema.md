# Assignment 1: Proposed Metadata Schema

**Students**:
Seyedeh Fatemeh Ahmadi, Diyar Tulenov


## Metadata Schema
The metadata will be stored in a **SQLite database** using **one table**, where **each row represents one downloaded QDA dataset**.  
The schema records dataset provenance, download information, repository context, and basic contact/licensing metadata.

## Table: `datasets`

| Field | Type | Required | Description |
|------|------|------|-------------|
| id | INTEGER | yes | Primary key |
| qda_file_url | TEXT | yes | URL of the QDA file |
| download_timestamp | TEXT | yes | Timestamp of the most recent download |
| local_directory | TEXT | yes | Name of the local folder containing the dataset files |
| local_filename | TEXT | yes | Name of the downloaded QDA file |
| repository_name | TEXT | no | Name of the repository (e.g., Zenodo, DataverseNO, Dryad) |
| dataset_page_url | TEXT | no | URL of the dataset landing page |
| dataset_title | TEXT | no | Title of the dataset |
| dataset_description | TEXT | no | Short description or abstract of the dataset |
| dataset_doi | TEXT | no | DOI identifier if available |
| keywords | TEXT | no | Dataset keywords or tags |
| download_method | TEXT | no | How the dataset was acquired (API, manual, login-required) |
| download_status | TEXT | no | Status of the download (success, partial, failed) |
| file_count | INTEGER | no | Number of files downloaded into the dataset folder |
| uploader_name | TEXT | no | Name of the dataset uploader |
| uploader_email | TEXT | no | Email of the dataset uploader |
| author_name | TEXT | no | Name(s) of dataset author(s) |
| contact_person | TEXT | no | Contact person for the dataset |
| license | TEXT | no | Dataset license (e.g., CC-BY) |

## Example SQLite Definition

```sql
CREATE TABLE datasets (
    id INTEGER PRIMARY KEY,
    qda_file_url TEXT NOT NULL,
    download_timestamp TEXT NOT NULL,
    local_directory TEXT NOT NULL,
    local_filename TEXT NOT NULL,
    repository_name TEXT,
    dataset_page_url TEXT,
    dataset_title TEXT,
    dataset_description TEXT,
    dataset_doi TEXT,
    keywords TEXT,
    download_method TEXT,
    download_status TEXT,
    file_count INTEGER,
    uploader_name TEXT,
    uploader_email TEXT,
    author_name TEXT,
    contact_person TEXT,
    license TEXT
);