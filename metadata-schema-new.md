# SQLite Metadata Database Schema

Primary rule as discussed: Do not change data when downloading; data quality issues will be resolved in a second step.

---

## PROJECTS table

| Field name | Type | Required / optional | Explanation | Example | Comment |
|------------|------|---------------------|-------------|---------|---------|
| id | integer | r | Primary key / identifier | 1 | |
| query_string | String | o | Query string that led to this project | qdpx | |
| repository_id | integer | r | Based on our own list of repos | 1 | |
| repository_url | URL | r | Top-level URL of repository | https://zenodo.org | |
| project_url | URL | r | Complete URL of repo + project path | https://zenodo.org/records/16082705 | |
| version | String | o | Version string, if any | | |
| title | String | r | Identifiable name of research project, if any | Supporting Data ... | |
| description | TEXT | r | Some description from the project site, if any | | |
| language | BCP 47 | o | Primary language of project; possibly fall back to ISO 639 if BCP 47 isn't provided | en-US | |
| doi | URL | o | DOI URL | https://doi.org/10.5281/zenodo.16082705 | |
| upload_date | DATE | o | The date of the upload (down to the day, but if all you have is the year that works as well) | 2026-01-23 | |
| download_date | TIMESTAMP | r | The timestamp of when your download concluded | | |
| download_repository_folder | String | r | Relative to root repository folder | zenodo | |
| download_project_folder | String | r | Project folder relative to repo folder | 16082705 | Use project id from website and let me know if this is unclear |
| download_version_folder | String | o | Version folder relative to project folder, if any | v1 | |
| download_method | SCRAPING \| API-CALL | r | How the data was downloaded | | |

---

## FILES table

| Field name | Type | Required / optional | Explanation | Example | Comment |
|------------|------|---------------------|-------------|---------|---------|
| id | integer | r | Primary key / identifier of file | 1 | |
| project_id | integer | r | Foreign key pointing back to the project the file belongs to | 1 | |
| file_name | String | r | Name of the file in the folder for the project | Country_Article counts.xlsx | |
| file_type | String | r | Type of file (just extract the extension) | xlsx | |
| status | DOWNLOAD_RESULT | r | Whether the download succeeded or failed | | |

---

## KEYWORDS table

| Field name | Type | Required / optional | Explanation | Example | Comment |
|------------|------|---------------------|-------------|---------|---------|
| id | integer | r | Primary key / identifier of keyword | 1 | |
| project_id | integer | r | Foreign key pointing back to the project the file belongs to | 1 | |
| keyword | String | r | The keyword | EFL learners | |

---

## PERSON_ROLE table

| Field name | Type | Required / optional | Explanation | Example | Comment |
|------------|------|---------------------|-------------|---------|---------|
| id | integer | r | Primary key / identifier | 1 | |
| project_id | integer | r | Foreign key pointing back to the project the file belongs to | 1 | |
| name | String | r | Name string; ignore components | Huaqiang, Li | |
| role | PERSON_ROLE | r | Role the person played, UNKNOWN if unclear (usually the case) | | |

---

## LICENSES table

| Field name | Type | Required / optional | Explanation | Example | Comment |
|------------|------|---------------------|-------------|---------|---------|
| id | integer | r | Primary key / identifier | 1 | |
| project_id | integer | r | Foreign key pointing back to the project the file belongs to | 1 | |
| license | LICENSE | r | | | |

---

## Data Types

**LICENSE** (String):
- CC BY, CC BY-SA, CC BY-NC, CC BY-ND, CC BY-NC-ND, CC0
- Each of these may have a trailing version identifier that also is a valid license, i.e. CC BY 4.0
- ODbL, ODC-By, PDDL
- ODbL-1.0, ODC-By-1.0
- But if there is a different original data string identifying the license, use this and we'll fix later

**PERSON_ROLE** (enum):
- UPLOADER, AUTHOR, OWNER, OTHER, UNKNOWN
- Use unknown if you don't know which role it is; other is intended for involved people

**DOWNLOAD_RESULT** (enum):
- SUCCEEDED, FAILED_SERVER_UNRESPONSIVE, FAILED_LOGIN_REQUIRED, FAILED_TOO_LARGE
