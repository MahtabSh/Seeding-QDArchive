#!/usr/bin/env python3
"""
Fetch file content from Google Drive for Zenodo (repo 1) projects and store
text snippets in project_knowledge.file_snippets.

After this script completes, re-run:
    python3 classify_files_bert.py --db 23692652-sq26.db --type QDA_PROJECT,QD_PROJECT

Supported file types for text extraction:
    .txt .csv .tsv .tab .md .rtf  → read as plain text
    .pdf                           → extract with pdfminer
    .docx                          → extract with python-docx

All other types (audio, QDA binaries, images, etc.) are skipped.

Usage:
    python3 fetch_zenodo_snippets.py --db 23692652-sq26.db
    python3 fetch_zenodo_snippets.py --db 23692652-sq26.db --max-size-mb 5
    python3 fetch_zenodo_snippets.py --db 23692652-sq26.db --limit 500  # test run
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_DB     = "23692652-sq26.db"
SNIPPET_CHARS  = 500   # characters to keep per file
MAX_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB default — skip larger files
REPO_ID        = 1     # Zenodo

TEXT_EXTENSIONS = {".txt", ".csv", ".tsv", ".tab", ".md", ".rtf", ".log", ".xml", ".json"}
PDF_EXTENSIONS  = {".pdf"}
DOCX_EXTENSIONS = {".docx", ".doc"}


# ── Text extractors ────────────────────────────────────────────────────────────

def extract_text_plain(data: bytes) -> str:
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def extract_text_pdf(data: bytes) -> str:
    try:
        from pdfminer.high_level import extract_text as pdf_extract
        return pdf_extract(io.BytesIO(data)) or ""
    except Exception:
        return ""


def extract_text_docx(data: bytes) -> str:
    try:
        from docx import Document
        doc = Document(io.BytesIO(data))
        return " ".join(p.text for p in doc.paragraphs if p.text.strip())
    except Exception:
        return ""


def extract_snippet(data: bytes, ext: str) -> str:
    ext = ext.lower()
    if ext in TEXT_EXTENSIONS:
        text = extract_text_plain(data)
    elif ext in PDF_EXTENSIONS:
        text = extract_text_pdf(data)
    elif ext in DOCX_EXTENSIONS:
        text = extract_text_docx(data)
    else:
        return ""
    return " ".join(text.split())[:SNIPPET_CHARS]


# ── Google Drive download ──────────────────────────────────────────────────────

def get_drive_service():
    import pickle
    import socket
    from pathlib import Path as _Path
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow

    socket.setdefaulttimeout(60)
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

    creds = None
    if _Path("token.pickle").exists():
        with open("token.pickle", "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as f:
            pickle.dump(creds, f)

    return build("drive", "v3", credentials=creds)


def download_file(service, file_id: str) -> bytes | None:
    try:
        from googleapiclient.http import MediaIoBaseDownload
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request, chunksize=1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()
    except Exception as e:
        log.debug(f"Download failed for {file_id}: {e}")
        return None


# ── DB helpers ─────────────────────────────────────────────────────────────────

def load_projects(conn: sqlite3.Connection, limit: int | None) -> list[tuple]:
    """
    Return (project_id, project_type, existing_snippets_json) for Zenodo
    QDA/QD projects that have at least one Drive file with a readable extension.
    """
    query = """
        SELECT DISTINCT p.id, p.type,
               COALESCE(pk.file_snippets, '{}') as existing
        FROM projects p
        JOIN files f ON f.project_id = p.id
        LEFT JOIN project_knowledge pk ON pk.project_id = p.id
        WHERE p.repository_id = ?
          AND p.type IN ('QDA_PROJECT','QD_PROJECT')
          AND f.gdrive_file_id IS NOT NULL AND f.gdrive_file_id != ''
          AND LOWER(f.file_type) IN (
              '.txt','.csv','.tsv','.tab','.md','.rtf',
              '.pdf','.docx','.doc','.log','.xml'
          )
        ORDER BY p.id
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query, (REPO_ID,)).fetchall()


def load_files_for_project(conn: sqlite3.Connection,
                            project_id: int,
                            max_size: int) -> list[tuple]:
    """Return (file_name, file_type, file_size_bytes, gdrive_file_id) for a project."""
    return conn.execute("""
        SELECT file_name, file_type, COALESCE(file_size_bytes, 0), gdrive_file_id
        FROM files
        WHERE project_id = ?
          AND gdrive_file_id IS NOT NULL AND gdrive_file_id != ''
          AND LOWER(file_type) IN (
              '.txt','.csv','.tsv','.tab','.md','.rtf',
              '.pdf','.docx','.doc','.log','.xml'
          )
          AND (file_size_bytes IS NULL OR file_size_bytes <= ?)
        ORDER BY
            CASE LOWER(file_type)
                WHEN '.txt' THEN 1 WHEN '.csv' THEN 2 WHEN '.tsv' THEN 3
                WHEN '.tab' THEN 4 WHEN '.docx' THEN 5 WHEN '.pdf' THEN 6
                ELSE 7 END
        LIMIT 5
    """, (project_id, max_size)).fetchall()


def save_snippets(conn: sqlite3.Connection,
                  project_id: int,
                  snippets: dict[str, str]) -> None:
    conn.execute("""
        UPDATE project_knowledge
        SET file_snippets = ?, enriched_at = datetime('now')
        WHERE project_id = ?
    """, (json.dumps(snippets, ensure_ascii=False), project_id))


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch Zenodo file content from Google Drive → project_knowledge.file_snippets"
    )
    ap.add_argument("--db",           default=DEFAULT_DB)
    ap.add_argument("--max-size-mb",  type=float, default=5.0,
                    help="Skip files larger than this many MB (default: 5)")
    ap.add_argument("--limit",        type=int,   default=None,
                    help="Process at most N projects (for test runs)")
    ap.add_argument("--skip-existing", action="store_true",
                    help="Skip projects that already have non-empty snippets")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}")
        sys.exit(1)

    max_size = int(args.max_size_mb * 1024 * 1024)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    projects = load_projects(conn, args.limit)
    log.info(f"Projects to process: {len(projects):,}")

    if args.skip_existing:
        projects = [
            (pid, ptype, ex) for pid, ptype, ex in projects
            if ex in ("", "{}", None) or json.loads(ex or "{}") == {}
        ]
        log.info(f"After skip-existing filter: {len(projects):,}")

    if not projects:
        log.info("Nothing to do.")
        conn.close()
        return

    log.info("Authenticating with Google Drive…")
    try:
        service = get_drive_service()
    except Exception as e:
        log.error(f"Drive auth failed: {e}")
        sys.exit(1)

    t0 = time.time()
    done = skipped = errors = total_files = 0
    COMMIT_EVERY = 50

    for idx, (project_id, ptype, existing_json) in enumerate(projects):
        files = load_files_for_project(conn, project_id, max_size)
        if not files:
            skipped += 1
            continue

        try:
            existing = json.loads(existing_json or "{}")
        except Exception:
            existing = {}

        new_snippets: dict[str, str] = {}

        for file_name, file_type, file_size, gdrive_id in files:
            data = download_file(service, gdrive_id)
            if data is None:
                errors += 1
                continue

            snippet = extract_snippet(data, file_type)
            if snippet.strip():
                new_snippets[file_name] = snippet
                total_files += 1

        if new_snippets:
            merged = {**existing, **new_snippets}
            save_snippets(conn, project_id, merged)
            done += 1
        else:
            skipped += 1

        if (idx + 1) % COMMIT_EVERY == 0:
            conn.commit()
            elapsed = time.time() - t0
            speed = (idx + 1) / elapsed
            eta = (len(projects) - idx - 1) / speed if speed > 0 else 0
            log.info(
                f"  {idx+1:>6}/{len(projects):,}  "
                f"done={done:,}  skipped={skipped:,}  errors={errors:,}  "
                f"files={total_files:,}  ETA {eta/60:.1f}min"
            )

    conn.commit()
    elapsed = time.time() - t0
    log.info(f"Finished in {elapsed/60:.1f}min")
    log.info(f"  Projects updated:  {done:,}")
    log.info(f"  Projects skipped:  {skipped:,}")
    log.info(f"  Download errors:   {errors:,}")
    log.info(f"  Files with text:   {total_files:,}")
    log.info("")
    log.info("Next step: python3 classify_files_bert.py --db 23692652-sq26.db")
    conn.close()


if __name__ == "__main__":
    main()
