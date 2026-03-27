# QDArchive Seeding Pipeline

Automated harvesting of qualitative research data (QDA project files and
accompanying source materials) from open repositories. Part of the QDArchive
project at **FAU Erlangen-Nürnberg**.

---

## Overview

The pipeline crawls multiple open repositories, collects file metadata into an
SQLite database, and optionally downloads the files to a local directory tree.
By default only metadata is recorded — actual file downloads require the
`--download` flag, which makes it easy to do a fast inventory scan before
committing to storage.

**Repositories crawled:**

| Repository | URL | Auth |
|---|---|---|
| Zenodo | https://zenodo.org/ | none (public API) |
| Dryad | https://datadryad.org/ | OAuth2 (auto-refreshed every 10 h) |
| Syracuse QDR | https://data.qdr.syr.edu/ | API token |
| DataverseNO | https://dataverse.no/ | none |
| DANS | https://ssh.datastations.nl/ | none |
| Harvard Dataverse | https://dataverse.harvard.edu/ | none |
| Columbia ICPSR | https://www.openicpsr.org/ | account credentials |

---

## Requirements

- Python 3.9+
- [`requests`](https://pypi.org/project/requests/)

```bash
pip install requests
```

---

## Project Structure

```
Seeding-QDArchive/
├── qdarchive_pipeline.py        # Entry point — delegates to the qdarchive package
├── qdarchive_pipeline_monolith.py  # Legacy single-file version (kept for reference)
├── run_until_done.sh            # Shell wrapper to restart pipeline on exit
├── README.md
├── .gitignore
│
└── qdarchive/                   # Core package
    ├── pipeline.py              # CLI argument parsing and orchestration
    ├── config.py                # Registry of repositories, queries, credentials
    ├── db.py                    # MetadataDB — SQLite wrapper with WAL mode
    ├── http_client.py           # Shared requests.Session with retry / backoff
    ├── queries.py               # QDA_QUERIES and QDA_EXTENSION_QUERIES
    ├── progress.py              # Resume-state serialisation (progress.json)
    ├── utils.py                 # File classification, path helpers
    │
    ├── crawlers/
    │   ├── harvard.py           # Harvard Dataverse search loop (main entry)
    │   ├── harvard_oai.py       # OAI-PMH crawler for Harvard Dataverse
    │   └── columbia.py          # OpenICPSR / Columbia scraper
    │
    └── fetchers/
        ├── base.py              # fetch_dataverse_dataset — handles all Dataverse repos
        ├── zenodo.py            # Zenodo REST API
        ├── dryad.py             # Dryad OAuth2 + REST API
        ├── osf.py               # OSF (Open Science Framework)
        ├── figshare.py          # Figshare REST API
        └── icpsr.py             # ICPSR REST API
```

---

## Credentials

Credentials are read from environment variables. **Never hardcode secrets in
source files.**

```bash
# Dryad OAuth2 — obtain at https://datadryad.org/stash/developer
export DRYAD_CLIENT_ID="your-application-id"
export DRYAD_CLIENT_SECRET="your-client-secret"

# Syracuse QDR API token
export QDR_TOKEN="your-qdr-api-token"
```

Add these to your `~/.bashrc` or `~/.zshrc` for persistence, or use a
`.env` file with a tool like [`direnv`](https://direnv.net/) (the `.env`
file is git-ignored by default).

> **Dryad token rotation:** The pipeline automatically refreshes the OAuth
> token every 10 hours using the `client_credentials` grant. Long overnight
> runs will never hit an expired-token error.

---

## Usage

```bash
# Activate the virtualenv first
source env/bin/activate

# Metadata scan only — no files downloaded (fast, default behaviour)
python qdarchive_pipeline.py

# Full download from all repositories
python qdarchive_pipeline.py --download

# Specific repositories only
python qdarchive_pipeline.py --sources zenodo dryad

# Metadata scan with a higher record limit
python qdarchive_pipeline.py --sources zenodo --max-records 2000

# Full download with custom paths
python qdarchive_pipeline.py \
    --download \
    --output-dir /data/qdarchive/files \
    --db        /data/qdarchive/metadata.sqlite

# Resume an interrupted run — just re-run the same command
# (metadata.progress.json tracks pagination state)
# To force a full re-crawl:
rm metadata.progress.json && python qdarchive_pipeline.py
```

### CLI options

| Flag | Default | Description |
|---|---|---|
| `--output-dir` | `./archive` | Root directory for downloaded files |
| `--db` | `./metadata.sqlite` | SQLite database path |
| `--max-records` | `500` | Max analysis-file records per repository |
| `--download` | off | Download files to disk |
| `--sources` | all | Repositories to crawl (space-separated) |
| `--dryad-client-id` | env | Dryad OAuth client ID |
| `--dryad-client-secret` | env | Dryad OAuth client secret |

---

## Database Schema — `metadata.sqlite`

The database contains a single table `files`. Each row represents **one file**
within a dataset. A dataset with five files produces five rows sharing the same
`doi`, `title`, and `author` but with distinct `url`, `local_filename`, and
`file_type`.

### Identity

| Column | Type | Required | Description |
|---|---|:---:|---|
| `id` | INTEGER | ✓ | Auto-incrementing primary key |
| `url` | TEXT | ✓ | Direct download URL — deduplication key |
| `doi` | TEXT | | Persistent dataset identifier, e.g. `10.5281/zenodo.14523891` |

### File

| Column | Type | Required | Description |
|---|---|:---:|---|
| `local_dir` | TEXT | ✓ | Relative folder path, e.g. `zenodo/14523891` |
| `local_filename` | TEXT | ✓ | Filename, e.g. `main.qdpx` |
| `file_type` | TEXT | ✓ | `analysis` \| `primary` \| `additional` |
| `file_extension` | TEXT | ✓ | Lowercase extension, e.g. `.qdpx` |
| `file_size_bytes` | INTEGER | | File size in bytes |
| `checksum_md5` | TEXT | | MD5 hex digest — empty until downloaded |

### Download

| Column | Type | Required | Description |
|---|---|:---:|---|
| `download_timestamp` | TEXT | ✓ | ISO-8601 UTC timestamp — empty if not yet downloaded |
| `source_name` | TEXT | ✓ | Repository name, e.g. `Zenodo` |
| `source_url` | TEXT | | Dataset landing page URL |

### Dataset

| Column | Type | Required | Description |
|---|---|:---:|---|
| `title` | TEXT | | Dataset title |
| `description` | TEXT | | Abstract, truncated to 500 characters |
| `year` | TEXT | | Publication year (YYYY) |
| `keywords` | TEXT | | Semicolon-separated keywords |
| `language` | TEXT | | Language of the dataset content |

### People

| Column | Type | Required | Description |
|---|---|:---:|---|
| `author` | TEXT | | Semicolon-separated credited author names |
| `uploader_name` | TEXT | | Name of the depositor |
| `uploader_email` | TEXT | | Contact email of the depositor |

### Legal

| Column | Type | Required | Description |
|---|---|:---:|---|
| `license` | TEXT | | SPDX identifier or short name, e.g. `CC0-1.0` |
| `license_url` | TEXT | | URL to full license text |

### Discovery

| Column | Type | Required | Description |
|---|---|:---:|---|
| `matched_query` | TEXT | | The search query that surfaced this dataset |

**Indexes** created automatically: `idx_url`, `idx_doi`, `idx_file_type`,
`idx_source`, `idx_matched_query`.

---

## File Type Classification

Files are classified by extension at crawl time:

| Type | Examples | Meaning |
|---|---|---|
| `analysis` | `.qdpx` `.nvp` `.nvpx` `.mx24` `.atlasproj` `.mqda` | QDA project files — primary archive target |
| `primary` | `.pdf` `.docx` `.mp3` `.mp4` `.txt` `.csv` | Source data: transcripts, recordings, documents |
| `additional` | `.png` `.zip` `.md` *(everything else)* | Supporting files: READMEs, codebooks, images |

---

## Search Query Strategy

Queries are applied in three tiers across all repositories. The
`matched_query` column records which query found each dataset.

| Tier | Examples | Strategy |
|---|---|---|
| 1 — Extensions | `.qdpx` `.nvp` `.mx24` | Highest precision |
| 2 — Software names | `NVivo` `MAXQDA` `ATLAS.ti` | High precision |
| 3 — Methodology | `interview transcripts` `thematic analysis` | High recall |

---

## Output

### File hierarchy — `./archive/`

```
archive/
  zenodo/
    {record_id}/
      main.qdpx
      interview_01.docx
  dryad/
    {doi_safe}/
      data.nvp
  syracuse_qdr/  dataverse_no/  dans/  harvard_dataverse/
    ...
```

### Runtime files

| File | Description |
|---|---|
| `metadata.sqlite` | SQLite database (git-ignored) |
| `metadata.progress.json` | Resume state for interrupted runs (git-ignored) |
| `pipeline.log` | Full execution log (git-ignored) |

---

## Useful Queries

```sql
-- All QDA analysis files
SELECT local_dir, local_filename, source_name, title
FROM   files
WHERE  file_type = 'analysis'
ORDER  BY source_name;

-- Storage usage by repository
SELECT source_name,
       COUNT(*)                              AS files,
       ROUND(SUM(file_size_bytes) / 1e6, 1) AS total_mb
FROM   files
GROUP  BY source_name;

-- Files not yet downloaded
SELECT doi, title, url
FROM   files
WHERE  file_type = 'analysis'
  AND  download_timestamp = '';

-- Most productive search queries
SELECT matched_query,
       SUM(file_type = 'analysis') AS analysis_files
FROM   files
GROUP  BY matched_query
ORDER  BY analysis_files DESC;
```

---

## License

This pipeline code is developed for research purposes at FAU Erlangen-Nürnberg.
The downloaded datasets retain their original licenses as recorded in the
`license` column of the database.

---

*FAU Erlangen-Nürnberg · Open Science Software Lab · 2026*
