#!/usr/bin/env python3
"""
Phase 1: Project enrichment pipeline.

Collects ALL available information about each project and stores it in
a local `project_knowledge` table.  classify_combined.py then reads
from this table — classification needs no network calls.

What gets fetched per project (tried in priority order)
--------------------------------------------------------
  Metadata     title, description, keywords from our DB (instant)
  File names   readable file names from files table (instant)
  Zenodo API   richer abstract + subject tags + file snippets
  Dataverse    richer abstract + subject tags + file snippets
  Google Drive file snippets (if gdrive_folder_id and token valid)

Result
------
  project_knowledge.enriched_text — one text block per project, ready
  to feed directly to the LLM prompt with no further processing.

Usage
-----
  python3 enrich_projects.py --status          # progress report
  python3 enrich_projects.py --limit 500       # enrich 500 projects
  python3 enrich_projects.py                   # enrich all (resumable)
  python3 enrich_projects.py --redo --limit 100
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_DB   = "23692652-sq26.db"
TABLE        = "project_knowledge"
BATCH_SIZE   = 20
TIMEOUT      = 30
RATE_LIMIT_S = 1.0   # seconds between API calls (Zenodo: 60/min limit)

_READABLE_EXTS = (
    ".txt", ".pdf", ".docx", ".doc", ".csv", ".tab", ".tsv",
    ".atlproj", ".atlproj23", ".nvpx", ".qdpx",
    ".mex", ".mx18", ".mx20", ".mx22", ".mx24", ".mqda",
    ".f4", ".f4a",
)


# ── Schema ────────────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            project_id         INTEGER PRIMARY KEY,
            title              TEXT,
            description        TEXT,
            keywords           TEXT,
            file_names         TEXT,   -- JSON list
            file_snippets      TEXT,   -- JSON {{filename: text}}
            source_description TEXT,   -- richer abstract from Zenodo/Dataverse
            source_subjects    TEXT,   -- subject tags from source
            enriched_text      TEXT,   -- combined text ready for LLM
            file_source        TEXT,   -- "zenodo"|"dataverse"|"drive"|null
            enriched_at        TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


# ── DB helpers ────────────────────────────────────────────────────────────────

def fetch_batch(
    conn: sqlite3.Connection,
    offset: int,
    limit: int,
    redo: bool,
    drive_redo: bool = False,
) -> list[dict]:
    if drive_redo:
        # Only projects with a Drive folder not yet enriched from Drive
        where = f"""
            WHERE p.gdrive_folder_id IS NOT NULL AND p.gdrive_folder_id != ''
              AND p.id NOT IN (
                SELECT project_id FROM {TABLE} WHERE file_source = 'drive'
              )
        """
    elif redo:
        where = ""
    else:
        where = f"WHERE p.id NOT IN (SELECT project_id FROM {TABLE})"

    rows = conn.execute(f"""
        SELECT p.id, p.title, p.description, p.doi,
               p.repository_url, p.project_url,
               COALESCE(p.gdrive_folder_id,'') AS gdrive_folder_id,
               GROUP_CONCAT(DISTINCT k.keyword)   AS keywords,
               GROUP_CONCAT(DISTINCT f.file_type) AS file_types
        FROM projects p
        LEFT JOIN keywords k ON k.project_id = p.id
        LEFT JOIN files    f ON f.project_id  = p.id
        {where}
        GROUP BY p.id
        LIMIT ? OFFSET ?
    """, (limit, offset)).fetchall()
    cols = ["id","title","description","doi","repository_url","project_url",
            "gdrive_folder_id","keywords","file_types"]
    return [dict(zip(cols, r)) for r in rows]


def count_remaining(conn: sqlite3.Connection, redo: bool, drive_redo: bool = False) -> int:
    if drive_redo:
        return conn.execute(f"""
            SELECT COUNT(*) FROM projects
            WHERE gdrive_folder_id IS NOT NULL AND gdrive_folder_id != ''
              AND id NOT IN (SELECT project_id FROM {TABLE} WHERE file_source = 'drive')
        """).fetchone()[0]
    if redo:
        return conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    return conn.execute(f"""
        SELECT COUNT(*) FROM projects
        WHERE id NOT IN (SELECT project_id FROM {TABLE})
    """).fetchone()[0]


def get_file_names(conn: sqlite3.Connection, project_id: int) -> list[str]:
    rows = conn.execute(f"""
        SELECT file_name, file_type
        FROM files
        WHERE project_id = ? AND status = 'SUCCEEDED'
          AND file_type IN ({','.join('?'*len(_READABLE_EXTS))})
        LIMIT 20
    """, (project_id, *_READABLE_EXTS)).fetchall()
    if not rows:
        return []
    from qdarchive.file_reader import _file_priority
    files = sorted(rows, key=lambda r: _file_priority(r[0], r[1]), reverse=True)
    return [r[0] for r in files[:8]]


def save_knowledge(
    conn: sqlite3.Connection,
    project_id: int,
    title: str,
    description: str,
    keywords: str,
    file_names: list[str],
    file_snippets: dict[str, str],
    source_description: str,
    source_subjects: list[str],
    enriched_text: str,
    file_source: str | None,
) -> None:
    conn.execute(f"""
        INSERT INTO {TABLE} (
            project_id, title, description, keywords,
            file_names, file_snippets,
            source_description, source_subjects,
            enriched_text, file_source
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(project_id) DO UPDATE SET
            file_names=excluded.file_names,
            file_snippets=excluded.file_snippets,
            source_description=excluded.source_description,
            source_subjects=excluded.source_subjects,
            enriched_text=excluded.enriched_text,
            file_source=excluded.file_source,
            enriched_at=datetime('now')
    """, (
        project_id, title, description, keywords,
        json.dumps(file_names, ensure_ascii=False),
        json.dumps(file_snippets, ensure_ascii=False),
        source_description,
        json.dumps(source_subjects, ensure_ascii=False),
        enriched_text,
        file_source,
    ))


# ── HTTP helpers ──────────────────────────────────────────────────────────────

_session = requests.Session()
_session.headers["User-Agent"] = "QDArchive-Enricher/1.0 (research use)"
_last_call: float = 0.0


def _get(url: str, max_bytes: int | None = None) -> bytes | None:
    global _last_call
    elapsed = time.time() - _last_call
    if elapsed < RATE_LIMIT_S:
        time.sleep(RATE_LIMIT_S - elapsed)
    _last_call = time.time()
    try:
        r = _session.get(url, timeout=TIMEOUT, stream=True)
        if r.status_code == 429:
            log.warning("Rate limited — sleeping 60s")
            time.sleep(60)
            return _get(url, max_bytes=max_bytes)
        if r.status_code in (401, 403, 404):
            return None
        if r.status_code != 200:
            log.debug(f"HTTP {r.status_code}: {url}")
            return None
        chunks: list[bytes] = []
        total = 0
        for chunk in r.iter_content(chunk_size=65536):
            chunks.append(chunk)
            total += len(chunk)
            if max_bytes and total > max_bytes:
                log.debug(f"Skipping oversized download ({total:,}B > {max_bytes:,}B): {url}")
                return None
        return b"".join(chunks)
    except requests.RequestException as e:
        log.debug(f"Request failed: {e}")
        return None


def _get_json(url: str) -> dict | None:
    data = _get(url)
    if data is None:
        return None
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return None


# ── Text extraction ────────────────────────────────────────────────────────────

def _extract(data: bytes, ext: str) -> str:
    from qdarchive.file_reader import _EXTRACTORS
    extractor = _EXTRACTORS.get(ext)
    if not extractor:
        return ""
    try:
        text = extractor(data)
        return re.sub(r"\s+", " ", text).strip()
    except Exception:
        return ""


# ── Zenodo enrichment ─────────────────────────────────────────────────────────

def enrich_zenodo(doi: str, file_names: list[str]) -> dict:
    """Fetch Zenodo record metadata + download priority file snippets."""
    record_id = _zenodo_id(doi)
    if not record_id:
        return {}

    meta = _get_json(f"https://zenodo.org/api/records/{record_id}")
    if not meta:
        return {}

    m = meta.get("metadata", {})
    source_description = _strip_html(m.get("description", ""))
    source_subjects    = [
        s.get("term", "") for s in m.get("subjects", []) if s.get("term")
    ]
    # Also grab keywords from the record (may differ from our DB keywords)
    zenodo_keywords = m.get("keywords", [])

    # Build filename→size map from API metadata (avoids downloading oversized files)
    api_file_sizes: dict[str, int] = {}
    for f in meta.get("files", []):
        key  = f.get("key", "")
        size = f.get("size", 0)
        if key:
            api_file_sizes[key] = size

    # Download priority files
    snippets: dict[str, str] = {}
    from qdarchive.file_reader import MAX_FILES_PER_PROJECT, SNIPPET_CHARS, MAX_FILE_BYTES, _QDA_ZIP_EXTS
    for fname in file_names[:MAX_FILES_PER_PROJECT * 2]:
        if len(snippets) >= MAX_FILES_PER_PROJECT:
            break
        ext = Path(fname).suffix.lower()
        if not ext:
            continue
        size_limit = 50 * 1024 * 1024 if ext in _QDA_ZIP_EXTS else MAX_FILE_BYTES
        # Skip if the API already told us the file is too large
        if fname in api_file_sizes and api_file_sizes[fname] > size_limit:
            log.debug(f"Skipping {fname} ({api_file_sizes[fname]:,}B > {size_limit:,}B limit)")
            continue
        url  = f"https://zenodo.org/records/{record_id}/files/{fname}/content"
        data = _get(url, max_bytes=size_limit)
        if data is None:
            continue
        text = _extract(data, ext)
        if text:
            snippets[fname] = text[:SNIPPET_CHARS]

    return {
        "source_description": source_description,
        "source_subjects":    source_subjects + zenodo_keywords,
        "file_snippets":      snippets,
        "file_source":        "zenodo" if snippets else None,
    }


def _zenodo_id(doi: str) -> str | None:
    m = re.search(r"zenodo\.(\d+)", doi or "", re.I)
    return m.group(1) if m else None


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()


# ── Dataverse enrichment ──────────────────────────────────────────────────────

def enrich_dataverse(doi: str, project_url: str, file_names: list[str]) -> dict:
    """Fetch Dataverse metadata + download priority file snippets."""
    base = _base_url(project_url or "")
    pid  = _normalise_doi(doi)
    if not base or not pid:
        return {}

    meta = _get_json(f"{base}/api/datasets/:persistentId/?persistentId={pid}")
    if not meta:
        return {}

    source_description = ""
    source_subjects    = []
    file_id_map: dict[str, int] = {}

    try:
        version = meta["data"]["latestVersion"]
        # Subject field
        for block in version.get("metadataBlocks", {}).values():
            for field in block.get("fields", []):
                name = field.get("typeName", "")
                val  = field.get("value")
                if name == "subject" and isinstance(val, list):
                    source_subjects = val
                elif name == "dsDescription" and isinstance(val, list):
                    for v in val:
                        desc = v.get("dsDescriptionValue", {}).get("value", "")
                        if len(desc) > len(source_description):
                            source_description = _strip_html(desc)
                elif name == "keyword" and isinstance(val, list):
                    for v in val:
                        kw = v.get("keywordValue", {}).get("value", "")
                        if kw:
                            source_subjects.append(kw)

        # File ID map for downloads
        for entry in version.get("files", []):
            df = entry.get("dataFile", {})
            file_id_map[df.get("filename", "")] = df.get("id")
    except (KeyError, TypeError):
        pass

    # Download priority files
    snippets: dict[str, str] = {}
    from qdarchive.file_reader import MAX_FILES_PER_PROJECT, SNIPPET_CHARS, MAX_FILE_BYTES, _QDA_ZIP_EXTS
    for fname in file_names[:MAX_FILES_PER_PROJECT * 2]:
        if len(snippets) >= MAX_FILES_PER_PROJECT:
            break
        fid = file_id_map.get(fname)
        if not fid:
            continue
        ext  = Path(fname).suffix.lower()
        size_limit = 50 * 1024 * 1024 if ext in _QDA_ZIP_EXTS else MAX_FILE_BYTES
        data = _get(f"{base}/api/access/datafile/{fid}")
        if data is None:
            continue
        text = _extract(data, ext)
        if text:
            snippets[fname] = text[:SNIPPET_CHARS]

    return {
        "source_description": source_description,
        "source_subjects":    source_subjects,
        "file_snippets":      snippets,
        "file_source":        "dataverse" if snippets else None,
    }


def _normalise_doi(doi: str) -> str | None:
    m = re.search(r"(10\.\d{4,}/\S+)", doi or "")
    return "doi:" + m.group(1).rstrip(".") if m else None


def _base_url(url: str) -> str:
    m = re.match(r"(https?://[^/]+)", url or "")
    return m.group(1) if m else ""


# ── Drive enrichment ──────────────────────────────────────────────────────────

def enrich_drive(project_id: int, folder_id: str, drive_reader) -> dict:
    try:
        snippets = drive_reader.get_snippets(project_id=project_id, folder_id=folder_id)
        return {
            "file_snippets": snippets,
            "file_source":   "drive" if snippets else None,
        }
    except Exception as e:
        log.debug(f"Drive failed for p{project_id}: {e}")
        return {}


# ── Build enriched_text ────────────────────────────────────────────────────────

def build_enriched_text(
    title: str,
    description: str,
    keywords: str,
    file_names: list[str],
    file_snippets: dict[str, str],
    source_description: str,
    source_subjects: list[str],
) -> str:
    parts = []

    if title:
        parts.append(f"Title: {title}")

    # Use richer description when available
    best_desc = (source_description
                 if len(source_description or "") > len(description or "")
                 else (description or ""))
    if best_desc:
        parts.append(f"Description: {best_desc[:800]}")

    if keywords:
        parts.append(f"Keywords: {keywords[:400]}")

    if source_subjects:
        subj = ", ".join(s for s in source_subjects if s)[:300]
        if subj:
            parts.append(f"Subjects: {subj}")

    if file_names:
        parts.append(f"Files: {', '.join(file_names[:8])}")

    if file_snippets:
        for fname, text in list(file_snippets.items())[:3]:
            parts.append(f"[{fname}]: {text[:600]}")

    return "\n".join(parts)


# ── Status ────────────────────────────────────────────────────────────────────

def print_status(conn: sqlite3.Connection) -> None:
    total     = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    enriched  = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    remaining = total - enriched

    src_rows = conn.execute(f"""
        SELECT COALESCE(file_source,'metadata_only'), COUNT(*)
        FROM {TABLE} GROUP BY file_source ORDER BY COUNT(*) DESC
    """).fetchall()

    print(f"\n{'═'*55}")
    print(f"  PROJECT ENRICHMENT STATUS")
    print(f"{'═'*55}")
    print(f"  Total projects   : {total:>8,}")
    print(f"  Enriched         : {enriched:>8,}  ({enriched/total*100:.1f}%)")
    print(f"  Remaining        : {remaining:>8,}")
    print(f"\n  Source breakdown:")
    for src, n in src_rows:
        print(f"    {src:<20}: {n:>7,}  ({n/enriched*100:.1f}%)")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich project_knowledge table with file snippets and source metadata."
    )
    parser.add_argument("--db",     default=DEFAULT_DB)
    parser.add_argument("--limit",  type=int, default=None)
    parser.add_argument("--batch",  type=int, default=BATCH_SIZE)
    parser.add_argument("--redo",       action="store_true")
    parser.add_argument("--drive-redo", action="store_true",
                        help="Re-enrich only projects that have a Drive folder but no Drive "
                             "file content yet (ignores --redo, never skips Drive)")
    parser.add_argument("--status",   action="store_true", help="Show progress and exit")
    parser.add_argument("--no-drive", action="store_true", help="Skip Google Drive")
    args = parser.parse_args()

    drive_redo = args.drive_redo

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    if args.status:
        print_status(conn)
        conn.close()
        return

    # ── Drive reader (optional) ───────────────────────────────────────────────
    drive_reader = None
    if not args.no_drive or drive_redo:
        try:
            from qdarchive.file_reader import FileContentReader
            drive_reader = FileContentReader()
            if drive_reader._service is None:
                log.warning("Drive unavailable — skipping Drive enrichment.")
                drive_reader = None
            else:
                log.info("Google Drive connected.")
        except Exception as e:
            log.warning(f"Drive error: {e}")

    # ── Main loop ─────────────────────────────────────────────────────────────
    total = count_remaining(conn, args.redo, drive_redo=drive_redo)
    if args.limit:
        total = min(total, args.limit)
    if drive_redo:
        log.info(f"Projects to Drive-enrich: {total:,}  (have folder, no Drive files yet)")
    else:
        log.info(f"Projects to enrich: {total:,}")

    if total == 0:
        log.info("Nothing to do. Use --redo to re-enrich.")
        conn.close()
        return

    processed  = 0
    drive_used = 0
    zenodo_used = 0
    dv_used    = 0
    offset     = 0
    t0 = time.time()

    try:
        while True:
            batch = fetch_batch(conn, offset, args.batch, args.redo, drive_redo=drive_redo)
            if not batch:
                break

            for p in batch:
                pid        = p["id"]
                title      = (p["title"] or "").strip()
                description = (p["description"] or "").strip()
                keywords   = (p["keywords"] or "").strip()
                doi        = (p["doi"] or "").strip()
                repo_url   = (p["repository_url"] or "").lower()
                project_url = (p["project_url"] or "")
                folder_id  = (p["gdrive_folder_id"] or "").strip()

                file_names = get_file_names(conn, pid)
                enriched   = {}

                # 1. Try Drive first
                if drive_reader and folder_id and not enriched.get("file_source"):
                    enriched = enrich_drive(pid, folder_id, drive_reader)
                    if enriched.get("file_source"):
                        drive_used += 1

                # 2. Try Zenodo
                if "zenodo.org" in repo_url and doi:
                    z = enrich_zenodo(doi, file_names)
                    if z:
                        # Merge — keep Drive snippets if available, else use Zenodo snippets
                        if not enriched.get("file_snippets"):
                            enriched["file_snippets"] = z.get("file_snippets", {})
                            enriched["file_source"]   = z.get("file_source")
                            if enriched["file_source"]:
                                zenodo_used += 1
                        enriched["source_description"] = z.get("source_description", "")
                        enriched["source_subjects"]    = z.get("source_subjects", [])

                # 3. Try Dataverse
                elif doi and project_url and "zenodo" not in repo_url:
                    d = enrich_dataverse(doi, project_url, file_names)
                    if d:
                        if not enriched.get("file_snippets"):
                            enriched["file_snippets"] = d.get("file_snippets", {})
                            enriched["file_source"]   = d.get("file_source")
                            if enriched["file_source"]:
                                dv_used += 1
                        enriched["source_description"] = d.get("source_description", "")
                        enriched["source_subjects"]    = d.get("source_subjects", [])

                # Build enriched_text
                file_snippets      = enriched.get("file_snippets", {})
                source_description = enriched.get("source_description", "")
                source_subjects    = enriched.get("source_subjects", [])
                file_source        = enriched.get("file_source")

                enriched_text = build_enriched_text(
                    title, description, keywords,
                    file_names, file_snippets,
                    source_description, source_subjects,
                )

                save_knowledge(
                    conn, pid, title, description, keywords,
                    file_names, file_snippets,
                    source_description, source_subjects,
                    enriched_text, file_source,
                )

                processed += 1

                if processed % 50 == 0:
                    elapsed = time.time() - t0
                    rate    = processed / elapsed
                    eta_min = ((total - processed) / rate / 60) if rate > 0 else 0
                    log.info(
                        f"  {processed}/{total}  ({rate:.1f}/s)  "
                        f"drive={drive_used}  zenodo={zenodo_used}  "
                        f"dataverse={dv_used}  ETA {eta_min:.0f} min"
                    )

                if args.limit and processed >= args.limit:
                    break

            conn.commit()

            if args.redo:
                offset += len(batch)

            if args.limit and processed >= args.limit:
                break

    except KeyboardInterrupt:
        log.info(f"Interrupted at {processed}. Committed so far.")
        conn.commit()
    finally:
        conn.close()

    elapsed = time.time() - t0
    log.info(
        f"Done. {processed} projects in {elapsed/60:.1f} min  "
        f"| drive={drive_used}  zenodo={zenodo_used}  dataverse={dv_used}  "
        f"metadata_only={processed - drive_used - zenodo_used - dv_used}"
    )


if __name__ == "__main__":
    main()
