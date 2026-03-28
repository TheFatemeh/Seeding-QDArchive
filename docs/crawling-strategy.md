# Crawling Strategy

## UK Data Service

1. Harvest all dataset metadata via OAI-PMH (`ListRecords` with Dublin Core)
2. Filter locally: exclude datasets whose only type is "numeric"
3. For each remaining dataset, visit the web page to check access level and scrape additional metadata (DOI, license)
4. Download files for open-access datasets

## ICPSR

1. Use the website search with `dataType=qualitative` filter to collect study IDs (paginated)
2. For each study, fetch detailed metadata via the DCAT JSON API (`/pcms/dcat/{id}`)
3. Download files for accessible datasets
