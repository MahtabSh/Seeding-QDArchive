# QDArchive Seeding Pipeline

Automated harvesting and ISIC classification of qualitative research data (QDA project files and
accompanying source materials) from open repositories. Part of the QDArchive
project at **FAU Erlangen-Nürnberg**.

---

## Overview

This project is divided into two parts:

| Part | Description |
|---|---|
| **Part 1 — Data Collection** | Crawl Zenodo, Harvard Dataverse, and Columbia to collect 51,818 research dataset projects |
| **Part 2 — Classification** | Classify all projects by economic sector using ISIC Rev.5 via a 4-classifier ensemble |

---

## Part 1 — Data Collection

### What Was Collected

| Repository | Records | Notes |
|---|---|---|
| **Zenodo** | 39,393 | Full API crawl across all query tiers |
| **Harvard Dataverse** | 12,400 | Search + OAI-PMH crawl |
| **Columbia Oral History Archive** | 25 | Scraper only (no public API) |
| **Total** | **51,818** | After DOI-based deduplication |

**Database file:** `23692652-sq26.db`

| Table | Rows |
|---|---|
| `projects` | 51,818 |
| `keywords` | 201,089 |
| `person_role` | 155,917 |
| `licenses` | 48,495 |
| `files` | 610,131 |

### Project Types

Each project is typed based on its file contents:

| Type | Description | Count |
|---|---|---|
| `QD_PROJECT` | Qualitative data files (interviews, transcripts, surveys) | 34,677 |
| `OTHER_PROJECT` | Research datasets of other kinds | 15,295 |
| `NOT_A_PROJECT` | Entries that do not represent meaningful research datasets | 1,757 |
| `QDA_PROJECT` | Contains QDA software project files (.qdpx, .nvpx, .atlproj, .mx22) | 64 |

---

## Part 2 — ISIC Rev.5 Classification

All 51,818 projects were classified according to the **ISIC Rev.5** standard (International Standard Industrial Classification of All Economic Activities, Revision 5 — United Nations 2025).

Each project received:
- A **primary class** — ISIC section (one of 22 letters A–V)
- A **secondary class** — ISIC division (one of 88 two-digit codes)
- A **confidence score** derived from classifier agreement

### Classification Pipeline

A 4-classifier voting ensemble was used. Each classifier contributes an independent signal; the final label is the majority vote with priority tiebreaking.

| Step | Script | Algorithm | Output Table | Provides Division? |
|---|---|---|---|---|
| 1 | `classify_combined.py` | LLM prompt — Qwen2.5:7b / Mistral / Claude Haiku | `classifications_combined` | Yes |
| 2 | `create_training_labels.py` | Silver label extraction + class balancing | `labels_training` | — |
| 3 | `classify_bert.py` | bert-base-uncased fine-tuned on labels_training | `classifications_bert_base` | No |
| 4 | `classify_bert_division.py` | DistilBERT + ISIC PDF anchors (section + division) | `classifications_bert_division` | Yes (100%) |
| 5 | `classify_tfidf.py` | TF-IDF + Logistic Regression | `classifications_tfidf` | No |
| 6 | `classify_vote.py` | Majority vote with priority tiebreak + division fill | `classifications_vote` | Yes (100%) |
| 7 | `classify_files_bert.py` | bert_base_isic_v1 on individual file text snippets | `classifications_files` | No |

### Results Summary

**Project-level (51,793 projects excl. Columbia — 100% coverage):**

| Confidence | Projects | % |
|---|---|---|
| very_high (4/4 agree) | 22,539 | 43.5% |
| high (3/4 agree) | 18,783 | 36.3% |
| medium (2/4 agree) | 9,690 | 18.7% |
| low (1/4 agree) | 781 | 1.5% |

**File-level (17,001 files — QDA + QD projects across all repositories):**

| Confidence | Files | % |
|---|---|---|
| very_high | 11,530 | 67.8% |
| high | 2,716 | 16.0% |
| medium | 2,013 | 11.8% |
| low | 742 | 4.4% |

### Per-Repository Dominant ISIC Sections

| Repo | Name | #1 Section | #2 Section | #3 Section |
|---|---|---|---|---|
| 1 | Zenodo | N — Professional, scientific and technical activities | R — Human health and social work activities | Q — Education |
| 10 | Harvard Dataverse | P — Public administration and defence | R — Human health and social work activities | N — Professional, scientific and technical activities |
| 19 | Columbia | R — Human health and social work activities | N — Professional, scientific and technical activities | F — Construction |

### Submission Deliverables

| File | Description |
|---|---|
| `23692652-sq26-classification.xlsx` | All 51,793 projects (excl. Columbia) with primary + secondary class |
| `23692652-sq26-classification.db` | Full SQLite database with all classification tables |
| `23692652-sq26-classification-methodology.*` | Methodology report (md / docx / pdf) — local only |
| `23692652-sq26-report-repo{1,10,19}.*` | Per-repository reports (docx / pdf) — local only |

### Classification Scripts

| Script | Purpose |
|---|---|
| `classify_combined.py` | Run LLM classification (Qwen / Mistral / Claude) |
| `classify_bert.py` | Fine-tune and run bert-base-uncased |
| `classify_bert_division.py` | Fine-tune and run DistilBERT with division labels |
| `classify_tfidf.py` | Train and run TF-IDF + Logistic Regression |
| `classify_vote.py` | Combine votes into final classification |
| `classify_files_bert.py` | Per-file BERT classification using file snippets |
| `create_training_labels.py` | Build silver training labels from LLM results |
| `enrich_projects.py` | Add keyword and filename enrichment to project text |
| `retrain_all.sh` | Re-run full classifier training pipeline end-to-end |
| `generate_xlsx.py` | Export classification results to Excel |
| `generate_pdf_report.py` | Generate per-repo PDF reports |
| `generate_docx_report.py` | Generate per-repo Word reports |
| `build_docx.py` | Convert methodology markdown to Word |
| `build_pdf_doc.py` | Convert methodology markdown to PDF |

---

## Project Structure

```
Seeding-QDArchive/
├── 23692652-sq26-classification.db   # Submission DB — all classification tables (Git LFS)
├── 23692652-sq26-classification.xlsx # Submission spreadsheet
│
├── classify_*.py                     # Classifier scripts (Part 2)
├── create_training_labels.py         # Silver label builder
├── enrich_projects.py                # Text enrichment
├── retrain_all.sh                    # Full retrain pipeline
├── generate_*.py / build_*.py        # Report generators
│
├── qdarchive_pipeline.py             # Entry-point — data collection (Part 1)
├── run_until_done.sh                 # Auto-restart wrapper
├── merge_databases.py                # One-time DB merge utility
│
├── models/                           # Fine-tuned model weights (not tracked in git)
│   ├── bert_base_isic_v1/            # bert-base-uncased fine-tuned on ISIC sections
│   └── bert_division_isic_v1/        # DistilBERT fine-tuned on ISIC sections + divisions
│
└── qdarchive/                        # Core package (Part 1)
    ├── pipeline.py
    ├── db.py
    ├── queries.py
    ├── ensemble_classifier.py        # LLM-based classifier
    ├── isic_enriched.py              # ISIC Rev.5 division metadata
    ├── nli_classifier.py             # NLI zero-shot classifier
    ├── crawlers/
    │   ├── zenodo.py
    │   ├── harvard.py
    │   ├── harvard_oai.py
    │   └── columbia.py
    └── fetchers/
        ├── base.py
        └── zenodo.py
```

---

## Requirements

```bash
python3 -m venv env
source env/bin/activate

# Part 1 — data collection
pip install requests google-api-python-client google-auth-httplib2 google-auth-oauthlib

# Part 2 — classification
pip install transformers torch scikit-learn openpyxl python-docx matplotlib numpy
```

---

## Running Part 1 — Data Collection

```bash
source env/bin/activate

# All sources
python qdarchive_pipeline.py

# Single source
python qdarchive_pipeline.py --sources zenodo
python qdarchive_pipeline.py --sources harvard
python qdarchive_pipeline.py --sources columbia

# With file download
python qdarchive_pipeline.py --download --max-file-size 200MB
```

Resume an interrupted run by re-running the same command (state is saved automatically).

---

## Running Part 2 — Classification

```bash
source env/bin/activate

# 1. LLM classification (requires Ollama with qwen2.5:7b or mistral)
python classify_combined.py --db 23692652-sq26.db

# 2. Build training labels from LLM results
python create_training_labels.py --db 23692652-sq26.db

# 3. Train and run BERT classifiers
python classify_bert.py --db 23692652-sq26.db
python classify_bert_division.py --db 23692652-sq26.db

# 4. Train and run TF-IDF classifier
python classify_tfidf.py --db 23692652-sq26.db

# 5. Combine votes into final classification
python classify_vote.py --db 23692652-sq26.db

# 6. Classify individual files
python classify_files_bert.py --db 23692652-sq26.db

# Or retrain everything at once
bash retrain_all.sh

# Export results
python generate_xlsx.py --db 23692652-sq26.db
python generate_docx_report.py
```

---

## Database Schema

### Core tables (Part 1)

| Table | Rows | Description |
|---|---|---|
| `projects` | 51,818 | One row per dataset project |
| `keywords` | 201,089 | Author-supplied keywords |
| `person_role` | 155,917 | Authors and uploaders |
| `licenses` | 48,495 | License identifiers |
| `files` | 610,131 | Files deposited with each project |
| `project_knowledge` | 51,818 | Enriched text + file snippets for classification |

### Classification tables (Part 2)

| Table | Description |
|---|---|
| `classifications_combined` | LLM results (Qwen / Mistral / Claude) |
| `classifications_bert_base` | bert-base-uncased — section only |
| `classifications_bert_division` | DistilBERT — section + division (100% division coverage) |
| `classifications_tfidf` | TF-IDF + Logistic Regression — section only |
| `classifications_vote` | **Final**: majority vote ensemble — section + division |
| `classifications_files` | Per-file BERT classifications (QDA + QD projects) |
| `labels_training` | Silver labels used to train BERT and TF-IDF |

---

## Useful SQL Queries

```sql
-- Final classification for all projects
SELECT p.title, p.type, cv.section, cv.section_name,
       cv.division, cv.division_name, cv.confidence
FROM projects p JOIN classifications_vote cv ON p.id = cv.project_id
ORDER BY cv.confidence DESC LIMIT 20;

-- Section distribution across all projects
SELECT section, section_name, COUNT(*) AS n,
       ROUND(COUNT(*)*100.0/51818, 1) AS pct
FROM classifications_vote
GROUP BY section ORDER BY n DESC;

-- Confidence distribution
SELECT confidence, COUNT(*), ROUND(COUNT(*)*100.0/51818,1) AS pct
FROM classifications_vote
GROUP BY confidence
ORDER BY CASE confidence WHEN 'very_high' THEN 1 WHEN 'high' THEN 2
         WHEN 'medium' THEN 3 ELSE 4 END;

-- QDA projects with their ISIC classification
SELECT p.title, p.repository_id, cv.section, cv.section_name,
       cv.division, cv.division_name, cv.confidence
FROM projects p JOIN classifications_vote cv ON p.id = cv.project_id
WHERE p.type = 'QDA_PROJECT';

-- File-level classifications for a specific project
SELECT file_name, section, section_name, confidence, prob_top1
FROM classifications_files WHERE project_id = 1;
```

---

## Git Tags

| Tag | Description |
|---|---|
| `part-1-release` | Completion of Part 1 (data collection) |
| `classification-results` | Completion of Part 2 (classification pipeline) |

---

## License

This pipeline code is developed for research purposes at FAU Erlangen-Nürnberg.
The downloaded datasets retain their original licenses as recorded in the
`licenses` table of the database.

---

*FAU Erlangen-Nürnberg · QDArchive Project · Student ID: 23692652 · July 2026*
