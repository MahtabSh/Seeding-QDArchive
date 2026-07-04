#!/usr/bin/env python3
"""
NLI-based ISIC classifier for QDArchive.

Uses MoritzLaurer/deberta-v3-large-zeroshot-v2 (zero-shot NLI) instead of
an Ollama LLM.  No external API, no Ollama — just:

    pip install transformers torch

Results go into `classifications_nli` (never touches other tables).

File source priority per project
---------------------------------
  1. Pre-enriched knowledge  — project_knowledge table (fastest)
  2. Google Drive            — if project has a gdrive_folder_id
  3. Repo API                — Zenodo or Dataverse direct download
  4. Metadata only           — if no files are accessible

Usage
-----
  # Dry-run — preview 10 results without writing
  python3 classify_nli.py --dry-run --limit 10

  # Classify 500 new projects
  python3 classify_nli.py --limit 500

  # Metadata only (skip all file fetching — fastest)
  python3 classify_nli.py --no-file-content --limit 500

  # Re-classify already-done projects
  python3 classify_nli.py --redo --limit 500

  # Use the smaller/faster model (90 MB instead of 400 MB)
  python3 classify_nli.py --nli-model cross-encoder/nli-deberta-v3-small --limit 500
"""

from __future__ import annotations

import argparse
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

DEFAULT_DB    = "23692652-sq26.db"
TABLE         = "classifications_nli"
BATCH_SIZE    = 50

# Reuse file-fetching utilities that have no table dependency
from classify_combined import get_file_names, get_snippets, _is_generic_title


# ── Table-aware fetch / count (must use TABLE = classifications_nli) ──────────

def fetch_batch(
    conn: sqlite3.Connection,
    offset: int,
    limit: int,
    model_name: str,
    redo: bool,
    redo_with_files: bool = False,
) -> list[dict]:
    if redo:
        where  = ""
        params = (limit, offset)
    elif redo_with_files:
        # Projects with metadata tier done but files tier not yet done
        where = f"""
            WHERE p.id IN (
                SELECT project_id FROM {TABLE}
                WHERE model_name = ? AND tier = 'metadata'
            )
            AND p.id NOT IN (
                SELECT project_id FROM {TABLE}
                WHERE model_name = ? AND tier = 'files'
            )
            AND (
                p.gdrive_folder_id IS NOT NULL AND p.gdrive_folder_id != ''
                OR p.id IN (SELECT project_id FROM project_knowledge)
                OR p.repository_url LIKE '%zenodo%'
                OR p.repository_url LIKE '%dataverse%'
            )
        """
        params = (model_name, model_name, limit, offset)
    else:
        # New projects not yet classified at metadata tier
        where  = f"""
            WHERE p.id NOT IN (
                SELECT project_id FROM {TABLE}
                WHERE model_name = ? AND tier = 'metadata'
            )
        """
        params = (model_name, limit, offset)

    rows = conn.execute(f"""
        SELECT
            p.id,
            p.title,
            p.description,
            p.doi,
            p.repository_url,
            p.project_url,
            COALESCE(p.gdrive_folder_id, '') AS gdrive_folder_id,
            GROUP_CONCAT(DISTINCT k.keyword)   AS keywords,
            GROUP_CONCAT(DISTINCT f.file_type) AS file_types
        FROM projects p
        LEFT JOIN keywords k ON k.project_id = p.id
        LEFT JOIN files    f ON f.project_id  = p.id
        {where}
        GROUP BY p.id
        LIMIT ? OFFSET ?
    """, params).fetchall()
    cols = ["id", "title", "description", "doi", "repository_url", "project_url",
            "gdrive_folder_id", "keywords", "file_types"]
    return [dict(zip(cols, r)) for r in rows]


# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_SQL = f"""
    CREATE TABLE {TABLE} (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id     INTEGER NOT NULL,
        model_name     TEXT    NOT NULL,
        tier           TEXT    NOT NULL DEFAULT 'metadata',
        section        TEXT,
        section_name   TEXT,
        division       TEXT,
        division_name  TEXT,
        confidence     TEXT,
        method         TEXT,
        files_used     TEXT,
        file_source    TEXT,
        classified_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(project_id, model_name, tier)
    )
"""

def ensure_schema(conn: sqlite3.Connection) -> None:
    exists = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)
    ).fetchone()

    if not exists:
        conn.execute(_CREATE_SQL)
        conn.commit()
        return

    # Check whether the existing UNIQUE constraint already covers tier
    table_sql = exists[0] or ""
    if "tier" in table_sql and "UNIQUE(project_id, model_name, tier)" in table_sql.replace(" ", "").replace("\n", ""):
        return  # already up-to-date

    # Migrate: rename old table, create new one, copy data, drop old
    log.info(f"Migrating {TABLE} to add tier-aware UNIQUE constraint …")
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({TABLE})")]
    if "tier" not in cols:
        conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN tier TEXT NOT NULL DEFAULT 'metadata'")

    conn.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_old")
    conn.execute(_CREATE_SQL)
    conn.execute(f"""
        INSERT OR IGNORE INTO {TABLE}
            (project_id, model_name, tier, section, section_name,
             division, division_name, confidence, method,
             files_used, file_source, classified_at)
        SELECT project_id, model_name, tier, section, section_name,
               division, division_name, confidence, method,
               files_used, file_source, classified_at
        FROM {TABLE}_old
    """)
    conn.execute(f"DROP TABLE {TABLE}_old")
    conn.commit()
    log.info("Migration complete.")


def count_remaining(
    conn: sqlite3.Connection,
    model_name: str,
    redo: bool,
    redo_with_files: bool = False,
) -> int:
    if redo:
        return conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    if redo_with_files:
        # Projects that have a metadata-tier result but NOT yet a files-tier result
        return conn.execute(f"""
            SELECT COUNT(*) FROM projects p
            WHERE p.id IN (
                SELECT project_id FROM {TABLE}
                WHERE model_name = ? AND tier = 'metadata'
            )
            AND p.id NOT IN (
                SELECT project_id FROM {TABLE}
                WHERE model_name = ? AND tier = 'files'
            )
            AND (
                p.gdrive_folder_id IS NOT NULL AND p.gdrive_folder_id != ''
                OR p.id IN (SELECT project_id FROM project_knowledge)
                OR p.repository_url LIKE '%zenodo%'
                OR p.repository_url LIKE '%dataverse%'
            )
        """, (model_name, model_name)).fetchone()[0]
    # Normal: projects not yet classified at metadata tier
    return conn.execute(f"""
        SELECT COUNT(*) FROM projects
        WHERE id NOT IN (
            SELECT project_id FROM {TABLE}
            WHERE model_name = ? AND tier = 'metadata'
        )
    """, (model_name,)).fetchone()[0]


def save_result(
    conn: sqlite3.Connection,
    project_id: int,
    model_name: str,
    result: dict,
    files_used: list[str] | None,
    file_source: str | None,
    tier: str = "metadata",
) -> None:
    conn.execute(f"""
        INSERT INTO {TABLE} (
            project_id, model_name, tier,
            section, section_name, division, division_name,
            confidence, method, files_used, file_source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(project_id, model_name, tier) DO UPDATE SET
            section=excluded.section,
            section_name=excluded.section_name,
            division=excluded.division,
            division_name=excluded.division_name,
            confidence=excluded.confidence,
            method=excluded.method,
            files_used=excluded.files_used,
            file_source=excluded.file_source,
            classified_at=datetime('now')
    """, (
        project_id, model_name, tier,
        result["section"], result["section_name"],
        result["division"], result["division_name"],
        result["confidence"], result["method"],
        json.dumps(files_used) if files_used else None,
        file_source,
    ))


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify QDArchive projects using zero-shot NLI (no Ollama)."
    )
    parser.add_argument("--db",              default=DEFAULT_DB)
    parser.add_argument(
        "--nli-model",
        default="facebook/bart-large-mnli",
        help="HuggingFace zero-shot-classification model to use. "
             "Default: facebook/bart-large-mnli (~1.6 GB, no login required). "
             "Multilingual alternative: joeddav/xlm-roberta-large-xnli",
    )
    parser.add_argument("--limit",           type=int, default=None)
    parser.add_argument("--batch",           type=int, default=BATCH_SIZE)
    parser.add_argument("--redo",            action="store_true",
                        help="Re-classify ALL projects already in the table")
    parser.add_argument("--redo-with-files", action="store_true",
                        help="Re-classify only projects that have file content available "
                             "(Drive / Zenodo / Dataverse / project_knowledge). "
                             "Use this for the second pass after --no-file-content.")
    parser.add_argument("--no-file-content", action="store_true",
                        help="Skip all file fetching (metadata only — fastest)")
    parser.add_argument("--no-drive",        action="store_true",
                        help="Skip Google Drive but still use Repo API")
    parser.add_argument("--no-repo",         action="store_true",
                        help="Skip Repo API but still use Google Drive")
    parser.add_argument("--dry-run",         action="store_true",
                        help="Print results without writing to DB")
    parser.add_argument("--verbose",         action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        sys.exit(1)

    # ── Load NLI classifier ───────────────────────────────────────────────────
    log.info(f"Loading NLI classifier: {args.nli_model}")
    from qdarchive.nli_classifier import NLIClassifier
    clf = NLIClassifier(model=args.nli_model)
    model_name = args.nli_model

    # ── Drive reader ──────────────────────────────────────────────────────────
    drive_reader = None
    if not args.no_file_content and not args.no_drive:
        try:
            from qdarchive.file_reader import FileContentReader
            drive_reader = FileContentReader()
            if drive_reader._service is None:
                log.warning("Google Drive unavailable — Drive disabled.")
                drive_reader = None
            else:
                log.info("Google Drive connected.")
        except Exception as e:
            log.warning(f"Drive reader error: {e}")

    # ── Repo reader ───────────────────────────────────────────────────────────
    repo_reader = None
    if not args.no_file_content and not args.no_repo:
        from qdarchive.repo_file_reader import RepoFileReader
        repo_reader = RepoFileReader(db_path=db_path)

    sources = []
    if drive_reader: sources.append("Drive")
    if repo_reader:  sources.append("Repo (Zenodo/Dataverse)")
    if not sources:  sources.append("metadata only")
    log.info(f"File sources: {', '.join(sources)}")

    # ── DB ────────────────────────────────────────────────────────────────────
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    # ── Dry-run ───────────────────────────────────────────────────────────────
    if args.dry_run:
        n = args.limit or 10
        sample = fetch_batch(conn, 0, n, model_name, redo=True)

        print(f"\n{'═'*72}")
        print(f"  DRY-RUN — {len(sample)} projects")
        print(f"  NLI model : {model_name}")
        print(f"  Sources   : {', '.join(sources)}")
        print(f"  Table     : {TABLE}  (not written in dry-run)")
        print(f"{'═'*72}\n")

        for p in sample:
            kw = [k.strip() for k in (p["keywords"] or "").split(",") if k.strip()]
            ft = [f.strip() for f in (p["file_types"] or "").split(",") if f.strip()]
            file_names = get_file_names(conn, p["id"])
            snips, src = get_snippets(p, drive_reader, repo_reader) if not args.no_file_content else ({}, None)

            result = clf.classify(
                title=p["title"] or "",
                description=p["description"] or "",
                keywords=kw,
                file_types=ft,
                snippets=snips or None,
                file_names=file_names,
            )

            src_tag = f"[{src}: {list(snips.keys())}]" if snips else "[metadata only]"
            print(f"[{p['id']}] {(p['title'] or '')[:70]}")
            if args.verbose:
                print(f"  Keywords : {', '.join(kw[:8]) or '—'}")
                print(f"  Files    : {', '.join(file_names[:5]) or '—'}")
            print(f"  → {result['division']} ({result['section']}): "
                  f"{result['division_name'][:48]}"
                  f"  [{result['confidence']}, {result['method']}]  {src_tag}")
            print()

        conn.close()
        return

    # ── Full run ──────────────────────────────────────────────────────────────
    redo_with_files = getattr(args, "redo_with_files", False)

    total = count_remaining(conn, model_name, args.redo, redo_with_files)
    if args.limit:
        total = min(total, args.limit)

    mode = ("redo-all" if args.redo
            else "redo-with-files" if redo_with_files
            else "new-only")
    log.info(f"Projects to classify: {total}  mode={mode}  table={TABLE}")

    if total == 0:
        log.info("Nothing to do. Use --redo or --redo-with-files to re-classify.")
        conn.close()
        return

    offset        = 0
    processed     = 0
    drive_used    = 0
    repo_used     = 0
    t0 = time.time()

    try:
        while True:
            batch = fetch_batch(conn, offset, args.batch, model_name,
                                args.redo, redo_with_files)
            if not batch:
                break

            for p in batch:
                kw = [k.strip() for k in (p["keywords"] or "").split(",") if k.strip()]
                ft = [f.strip() for f in (p["file_types"] or "").split(",") if f.strip()]

                # Use pre-enriched knowledge if available
                knowledge = conn.execute(
                    "SELECT title, keywords, source_description, source_subjects, "
                    "file_snippets, file_names, file_source "
                    "FROM project_knowledge WHERE project_id=?", (p["id"],)
                ).fetchone()

                if knowledge:
                    pk_title    = knowledge[0] or p["title"] or ""
                    pk_keywords = [k.strip() for k in (knowledge[1] or "").split(",") if k.strip()]
                    pk_src_desc = knowledge[2] or ""
                    pk_subjects = json.loads(knowledge[3] or "[]")
                    file_names_list  = json.loads(knowledge[5] or "[]")
                    src              = knowledge[6]

                    # In --no-file-content mode skip snippets even from cache so
                    # tier='metadata' is saved and fetch_batch won't re-fetch this project
                    if args.no_file_content:
                        file_snippets = {}
                        src = None
                    else:
                        file_snippets = json.loads(knowledge[4] or "{}")
                        if src == "drive":   drive_used += 1
                        elif src in ("zenodo", "dataverse"): repo_used += 1

                    if _is_generic_title(pk_title):
                        enriched_desc = pk_src_desc[:400] if pk_src_desc else (p["description"] or "")
                    else:
                        enriched_desc = pk_src_desc or (p["description"] or "")
                        if pk_subjects:
                            enriched_desc += "\nSubjects: " + ", ".join(str(s) for s in pk_subjects[:6])

                    result = clf.classify(
                        title=pk_title,
                        description=enriched_desc[:600],
                        keywords=pk_keywords or kw,
                        file_types=ft,
                        snippets=file_snippets or None,
                        file_names=file_names_list,
                    )
                    snips = file_snippets

                else:
                    file_names = get_file_names(conn, p["id"])

                    if not args.no_file_content:
                        snips, src = get_snippets(p, drive_reader, repo_reader)
                    else:
                        snips, src = {}, None

                    result = clf.classify(
                        title=p["title"] or "",
                        description=p["description"] or "",
                        keywords=kw,
                        file_types=ft,
                        snippets=snips or None,
                        file_names=file_names,
                    )

                    if src == "drive": drive_used += 1
                    if src == "repo":  repo_used  += 1

                files_used = list(snips.keys()) if snips else None
                tier = "files" if files_used else "metadata"
                save_result(conn, p["id"], model_name, result, files_used, src, tier)
                processed += 1

                if processed % 50 == 0:
                    elapsed = time.time() - t0
                    rate    = processed / elapsed
                    eta_min = ((total - processed) / rate / 60) if rate > 0 else 0
                    log.info(
                        f"  {processed}/{total}  ({rate:.2f} proj/s)  "
                        f"drive={drive_used}  repo={repo_used}  ETA {eta_min:.0f} min"
                    )

                if args.limit and processed >= args.limit:
                    break

            conn.commit()

            if args.redo or redo_with_files:
                offset += len(batch)

            if args.limit and processed >= args.limit:
                break

    except KeyboardInterrupt:
        log.info(f"Interrupted at {processed}. Progress saved.")
        conn.commit()
    finally:
        conn.close()

    elapsed = time.time() - t0
    log.info(
        f"Done. {processed} projects in {elapsed/60:.1f} min  |  "
        f"drive={drive_used}  repo={repo_used}  "
        f"metadata_only={processed - drive_used - repo_used}"
    )


if __name__ == "__main__":
    main()
