# UK Data Service -- Metadata Extraction Guide

This document defines **how the crawler should extract metadata fields
from UK Data Service study pages** and how those fields map to the
Assignment 1 SQLite schema.

Important constraints:

-   Do NOT rely on dynamic CSS classes (e.g. `css-1hgtbf4`) because the
    site uses React/Material UI.
-   Instead, locate fields by **label text inside `<span>` elements**.
-   Extract the value from the adjacent `<p>` elements.
-   Some fields must be **computed by the crawler** rather than
    extracted from the page.

------------------------------------------------------------------------

# Metadata Mapping

## dataset_title

**Website field:** Title

Example HTML pattern:

    <span>Title</span>
    <p>European Working Conditions Survey, 2024</p>

Extraction idea:

``` python
title = extract_field(soup, "Title")
```

------------------------------------------------------------------------

## dataset_page_url

Source: the page URL currently being crawled.

Example:

    https://datacatalogue.ukdataservice.ac.uk/studies/study?id=9511

Extraction idea:

``` python
dataset_page_url = current_url
```

------------------------------------------------------------------------

## dataset_description

**Website field:** Abstract

Section id:

    #abstract-section

Extraction idea:

``` python
abstract_section = soup.find(id="abstract-section")
dataset_description = abstract_section.get_text(" ", strip=True)
```

------------------------------------------------------------------------

## dataset_doi

**Website field:** Persistent identifier (DOI)

Example HTML:

    <span>Persistent identifier (DOI)</span>
    <p>10.5255/UKDA-SN-9511-1</p>

Extraction idea:

``` python
dataset_doi = extract_field(soup, "DOI")
```

------------------------------------------------------------------------

## author_name

**Website field:** Data creator(s)

Important:

-   Multiple authors may appear
-   Each author appears as a separate `<p>` tag
-   Store authors **comma separated**

Example HTML:

    <span>Data creator(s)</span>
    <p>Baumberg Geiger, B., University of Kent</p>
    <p>Edmiston, D., University of Leeds</p>
    <p>Summers, K., London School of Economics</p>

Extraction idea:

``` python
creator_section = soup.find("span", string=lambda x: x and "Data creator" in x)
creator_container = creator_section.find_parent("div").find_next("div")

authors = [p.get_text(strip=True) for p in creator_container.find_all("p")]

author_name = ", ".join(authors)
```

------------------------------------------------------------------------

## contact_person

Closest equivalent field:

**Depositor**

Example HTML:

    <span>Depositor</span>
    <p>Eurofound</p>

Extraction idea:

``` python
contact_person = extract_field(soup, "Depositor")
```

------------------------------------------------------------------------

## repository_name

Static value for this crawler.

    UK Data Service

------------------------------------------------------------------------

## keywords

**Website field:** Thesaurus search on keywords

Located in section:

    #keyword-section

Example HTML:

    <a>WORKING CONDITIONS</a>
    <a>EMPLOYMENT</a>
    <a>WORK-LIFE BALANCE</a>

Extraction idea:

``` python
keywords = [a.get_text(strip=True) for a in soup.select("#keyword-section a")]
keywords = ", ".join(keywords)
```

------------------------------------------------------------------------

## license

The site does not show explicit licenses, but it classifies datasets
under **Access categories**.

Categories:

-   Open data
-   Safeguarded data
-   Controlled data

These categories correspond to the **Access field on the page**.

Example HTML:

    <span>Access</span>
    <p>These data are safeguarded</p>

Mapping rule:

  Access text   Stored license
  ------------- ------------------
  open          Open data
  safeguarded   Safeguarded data
  controlled    Controlled data

Extraction idea:

``` python
access_text = extract_field(soup, "Access")
license = access_text
```

------------------------------------------------------------------------

## download_status

This field **must be set by the crawler after attempting downloads**.

Values:

    successful
    failed

Example logic:

``` python
if all_files_downloaded:
    download_status = "successful"
else:
    download_status = "failed"
```

------------------------------------------------------------------------

## qda_file_url

This field must store **URLs of files that match QDA extensions**.

Examples of QDA extensions:

    .qdpx
    .qdc
    .mqda
    .mx24
    .nvp
    .nvpx
    .atlasproj
    .hpr7
    .ppj
    .pprj
    .qlt
    .f4p
    .qpd

If multiple QDA files are detected, store them **comma separated**.

Example:

    https://example.org/project.qdpx, https://example.org/project2.qdc

------------------------------------------------------------------------

# File Download Extraction

Files are listed in the **Documentation table**.

Example structure:

    <table>
    <tr>
    <td>Title</td>
    <td><a href="FILE_URL">filename</a></td>
    <td>size</td>
    </tr>
    </table>

Extraction idea:

``` python
rows = soup.select("table tbody tr")

files = []

for row in rows:
    link = row.select_one("a")
    if link:
        url = link["href"]
        filename = link.get_text(strip=True)
        files.append((filename, url))
```

Download all files in this list.

------------------------------------------------------------------------

# File Count

After downloads:

    file_count = number of successfully downloaded files

------------------------------------------------------------------------

# Helper Extraction Function

Use a generic helper to extract labeled fields.

``` python
def extract_field(soup, label):

    span = soup.find("span", string=lambda x: x and label.lower() in x.lower())

    if not span:
        return None

    parent = span.find_parent("div")
    value = parent.find_next("p")

    if value:
        return value.get_text(strip=True)

    return None
```

This approach is robust against layout changes and avoids unstable CSS
classes.
