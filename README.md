# QDArchive Seeding Pipeline

Automated harvesting of qualitative research data (QDA project files and
accompanying source materials) from open repositories. Part of the QDArchive
project at **FAU Erlangen-Nürnberg**.

---

## What Was Actually Collected

This pipeline successfully retrieved data from **two repositories**:

| Repository | Records Collected | Notes |
|---|---|---|
| **Zenodo** | 39,405 projects | Full API crawl across all query tiers |
| **Harvard Dataverse** | 13,650 projects | Search + OAI-PMH crawl |
| Columbia Oral History Archive | 25 projects | Limited (scraper only) |

The **Columbia** crawler is included but collects very few records because the
Columbia Oral History Portal does not expose a public API — the crawler
web-scrapes a limited listing page. For Zenodo and Harvard, both the metadata
and (where applicable) Google Drive upload links are recorded.

**Database file:** `23692652-sq26.db`

**Total records in merged database:**

| Table | Rows |
|---|---|
| `projects` | 53,080 |
| `keywords` | 203,849 |
| `person_role` | 159,344 |
| `licenses` | 49,603 |
| `project_files` | 610,131 |

---

## Project Structure

```
Seeding-QDArchive/
├── 23692652-sq26.db             # Main SQLite database (tracked via Git LFS)
├── qdarchive_pipeline.py        # Entry-point — delegates to the qdarchive package
├── run_until_done.sh            # Shell wrapper to auto-restart on exit
├── merge_databases.py           # One-time script to merge old + new databases
├── README.md
├── .gitignore
├── .gitattributes               # Git LFS configuration
│
├── archive/                     # Downloaded files (not committed)
│
└── qdarchive/                   # Core package
    ├── pipeline.py              # CLI argument parsing and orchestration
    ├── db.py                    # MetadataDB — SQLite wrapper with WAL mode
    ├── queries.py               # QDA_QUERIES and QDA_EXTENSION_QUERIES
    ├── progress.py              # Resume-state serialisation (*.progress.json)
    ├── utils.py                 # File classification, path helpers
    ├── gdrive.py                # Google Drive upload integration
    │
    ├── crawlers/
    │   ├── __init__.py          # Source registry (CRAWLERS dict)
    │   ├── harvard.py           # Harvard Dataverse search loop
    │   ├── harvard_oai.py       # OAI-PMH crawler for Harvard Dataverse
    │   ├── columbia.py          # Columbia Oral History Archive scraper
    │   └── zenodo.py            # Zenodo REST API crawler
    │
    └── fetchers/
        ├── base.py              # Shared Dataverse dataset fetcher
        └── zenodo.py            # Zenodo file metadata fetcher
```

---

## Requirements

- Python 3.9+
- `requests`
- `google-api-python-client`, `google-auth-httplib2`, `google-auth-oauthlib`
  (only needed for Google Drive upload)

```bash
# Create and activate virtual environment
python3 -m venv env
source env/bin/activate          # macOS / Linux
# env\Scripts\activate           # Windows

# Install dependencies
pip install requests google-api-python-client google-auth-httplib2 google-auth-oauthlib
```

---

## Configuration

### 1. No credentials needed for basic crawling

Zenodo and Harvard Dataverse are public APIs — no API key is required for
metadata-only crawls.

### 2. Google Drive upload (optional)

To upload downloaded files to Google Drive you need a Google Cloud OAuth 2.0
credential file:

1. Go to [Google Cloud Console](https://console.cloud.google.com/) → APIs &
   Services → Credentials.
2. Create an **OAuth 2.0 Client ID** (Desktop app type).
3. Download the JSON file and save it as `credentials.json` in the project root.
4. On first run with `--gdrive`, your browser will open for authentication.
   After you approve, a `token.pickle` file is saved automatically.

> **Important:** `credentials.json` and `token.pickle` are listed in
> `.gitignore` and must **never** be committed to the repository.

---

## Running the Pipeline

```bash
# Always activate the virtualenv first
source env/bin/activate
```

### Metadata-only scan (no files downloaded)

```bash
# All sources — Zenodo + Harvard + Harvard-OAI + Columbia
python qdarchive_pipeline.py

# Zenodo only
python qdarchive_pipeline.py --sources zenodo

# Harvard Dataverse only (search-based crawler)
python qdarchive_pipeline.py --sources harvard

# Harvard OAI-PMH full harvest
python qdarchive_pipeline.py --sources harvard-oai

# Columbia Oral History Archive
python qdarchive_pipeline.py --sources columbia

# Custom database path
python qdarchive_pipeline.py --db 23692652-sq26.db
```

### Download files to disk

```bash
# Download everything to ./archive/
python qdarchive_pipeline.py --download

# Download only QDA analysis files (.qdpx, .nvp, .mx24 …)
python qdarchive_pipeline.py --download --extensions .qdpx .nvp .nvpx .mx24

# Limit file size (skip files > 200 MB)
python qdarchive_pipeline.py --download --max-file-size 200MB

# Zenodo only, with file download, custom output directory
python qdarchive_pipeline.py \
    --sources zenodo \
    --download \
    --output-dir /data/qdarchive/files \
    --db 23692652-sq26.db
```

### Google Drive upload

```bash
# Download files and upload to Google Drive (keep local copies)
python qdarchive_pipeline.py --download --gdrive

# Download, upload to Google Drive, then delete local copies
python qdarchive_pipeline.py --download --gdrive-only

# Specific sources with Drive upload
python qdarchive_pipeline.py --sources zenodo harvard --download --gdrive
```

### Resuming an interrupted run

The pipeline saves pagination state to a `*.progress.json` file automatically.
Just re-run the same command to continue from where it stopped:

```bash
python qdarchive_pipeline.py --sources zenodo
# interrupted … re-run the same command:
python qdarchive_pipeline.py --sources zenodo
```

To force a full re-crawl from the beginning:

```bash
rm metadata.*.progress.json
python qdarchive_pipeline.py
```

### Continuous run wrapper

For long overnight runs that should auto-restart on crash:

```bash
bash run_until_done.sh
```

---

## CLI Reference

| Flag | Default | Description |
|---|---|---|
| `--sources` | all | Which repos to crawl: `zenodo` `harvard` `harvard-oai` `columbia` |
| `--db` | `./metadata.sqlite` | SQLite database path |
| `--output-dir` | `./archive` | Root directory for downloaded files |
| `--download` | off | Download files to disk |
| `--extensions` | all | Whitelist of file extensions to download (e.g. `.qdpx .nvp`) |
| `--max-records` | 0 (unlimited) | Max projects to collect per source |
| `--max-file-size` | unlimited | Skip files larger than this (e.g. `200MB`, `512KB`) |
| `--gdrive` | off | Upload downloaded files to Google Drive (keeps local copies) |
| `--gdrive-only` | off | Upload to Google Drive and delete local files after upload |

---

## Database Schema — `23692652-sq26.db`

The database uses a normalised five-table schema. Each **project** (dataset)
has a single row in `projects`; its keywords, authors, licenses, and file list
are in the four related tables.

### `projects`

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-incrementing primary key |
| `query_string` | TEXT | Search query that found this dataset |
| `repository_id` | INTEGER | Source repo identifier |
| `repository_url` | TEXT | Base URL of the repository |
| `project_url` | TEXT | Dataset landing page URL |
| `version` | TEXT | Dataset version string |
| `title` | TEXT | Dataset title |
| `description` | TEXT | Abstract / description |
| `language` | TEXT | Content language |
| `doi` | TEXT | Persistent DOI, e.g. `10.5281/zenodo.14523891` |
| `upload_date` | TEXT | Date deposited |
| `download_date` | TEXT | Date crawled |
| `download_repository_folder` | TEXT | Top-level archive folder (e.g. `zenodo`) |
| `download_project_folder` | TEXT | Project subfolder |
| `download_version_folder` | TEXT | Version subfolder (if any) |
| `download_method` | TEXT | How the dataset was retrieved |
| `gdrive_folder_id` | TEXT | Google Drive folder ID (if uploaded) |
| `gdrive_folder_url` | TEXT | Google Drive folder URL (if uploaded) |

### `keywords`

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | |
| `project_id` | INTEGER FK → `projects.id` | |
| `keyword` | TEXT | One keyword per row |

### `person_role`

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | |
| `project_id` | INTEGER FK → `projects.id` | |
| `name` | TEXT | Person's name |
| `role` | TEXT | Role (e.g. `author`, `uploader`) |

### `licenses`

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | |
| `project_id` | INTEGER FK → `projects.id` | |
| `license` | TEXT | License identifier (e.g. `CC0-1.0`) |

### `project_files`

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | |
| `project_id` | INTEGER FK → `projects.id` | |
| `file_name` | TEXT | Filename |
| `file_type` | TEXT | `analysis` \| `primary` \| `additional` |
| `file_size_bytes` | INTEGER | File size in bytes |
| `status` | TEXT | Download status (e.g. `SUCCEEDED`, `PENDING`) |
| `gdrive_file_id` | TEXT | Google Drive file ID (if uploaded) |
| `gdrive_url` | TEXT | Google Drive file URL (if uploaded) |

---

## File Type Classification

Files are classified by extension at crawl time:

| Type | Extensions | Meaning |
|---|---|---|
| `analysis` | `.qdpx` `.nvp` `.nvpx` `.mx24` `.mx22` `.atlasproj` `.mqda` … | QDA project files — primary archive target |
| `primary` | `.pdf` `.docx` `.mp3` `.mp4` `.txt` `.csv` … | Source data: transcripts, recordings, documents |
| `additional` | `.png` `.zip` `.md` *(everything else)* | Supporting files: READMEs, codebooks, images |

---

## Search Query Strategy

Queries are applied in tiers across all repositories. The `query_string` column
in `projects` records which query found each dataset.

| Tier | Examples | Goal |
|---|---|---|
| 1 — File extensions | `.qdpx` `.nvp` `.mx24` | Highest precision — matches exact files |
| 2 — Software names | `NVivo` `MAXQDA` `ATLAS.ti` `Dedoose` | High precision |
| 3 — Methodology | `interview transcripts` `thematic analysis` | High recall |
| 4–9 | Data collection methods, discipline terms, German/French/Spanish terms | Broad coverage |

---

## Useful SQL Queries

```sql
-- Total projects per repository
SELECT repository_url, COUNT(*) AS projects
FROM   projects
GROUP  BY repository_url;

-- All QDA analysis files
SELECT p.title, p.doi, pf.file_name, pf.file_type, pf.gdrive_url
FROM   projects p
JOIN   project_files pf ON pf.project_id = p.id
WHERE  pf.file_type = 'analysis'
ORDER  BY p.repository_url, p.title;

-- Projects with Google Drive links
SELECT title, doi, gdrive_folder_url
FROM   projects
WHERE  gdrive_folder_id IS NOT NULL
ORDER  BY repository_url;

-- Storage usage by repository
SELECT p.repository_url,
       COUNT(DISTINCT p.id)            AS projects,
       COUNT(pf.id)                    AS files,
       ROUND(SUM(pf.file_size_bytes) / 1e9, 2) AS total_gb
FROM   projects p
JOIN   project_files pf ON pf.project_id = p.id
GROUP  BY p.repository_url;

-- Most productive search queries
SELECT query_string, COUNT(*) AS projects
FROM   projects
GROUP  BY query_string
ORDER  BY projects DESC
LIMIT  20;

-- Authors with the most deposited datasets
SELECT pr.name, COUNT(DISTINCT pr.project_id) AS datasets
FROM   person_role pr
WHERE  pr.role = 'author'
GROUP  BY pr.name
ORDER  BY datasets DESC
LIMIT  20;

-- License distribution
SELECT l.license, COUNT(*) AS count
FROM   licenses l
GROUP  BY l.license
ORDER  BY count DESC;
```

---

## Files Ignored by Git

The following files are listed in `.gitignore` and are **not** committed:

| File / Pattern | Reason |
|---|---|
| `credentials.json` | Google OAuth client secret — never share |
| `token.pickle` | Google OAuth access token — never share |
| `*.progress.json` | Pipeline resume state — ephemeral |
| `pipeline.log` | Runtime log — ephemeral |
| `env/` | Python virtual environment |
| `.env` | Environment variable overrides |

The database `23692652-sq26.db` is committed via **Git LFS** (see
`.gitattributes`).

---

## License

This pipeline code is developed for research purposes at FAU Erlangen-Nürnberg.
The downloaded datasets retain their original licenses as recorded in the
`licenses` table of the database.

---

*FAU Erlangen-Nürnberg · QDArchive Project · 2026*
