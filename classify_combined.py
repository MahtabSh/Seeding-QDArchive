#!/usr/bin/env python3
"""
Combined Tier 1 + Tier 2 classifier for QDArchive.

Runs a single mistral classification per project using ALL available
information at once: title, description, keywords, file types, AND
file content — fetched from Google Drive OR directly from the original
repository (Zenodo / Dataverse).

File source priority per project
---------------------------------
  1. Google Drive    — if project has a gdrive_folder_id
  2. Repo API        — Zenodo or Dataverse direct download (no Drive needed)
  3. Metadata only   — if no files are accessible

Results go into `classifications_combined` — the existing `classifications`
table is never touched.

Schema
------
  classifications_combined(
    project_id, model_name,
    section, section_name, division, division_name,
    confidence, method,
    files_used,       ← JSON list of filenames used (or null)
    file_source,      ← "drive" | "repo" | null
    classified_at
  )

Usage
-----
  # Dry-run — preview 10 results without writing
  python3 classify_combined.py --dry-run --limit 10

  # Classify 100 projects (Drive + Repo fallback)
  python3 classify_combined.py --limit 100

  # Metadata only (skip all file fetching)
  python3 classify_combined.py --no-file-content --limit 100

  # Re-classify already-done projects
  python3 classify_combined.py --redo --limit 100
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

DEFAULT_DB = "23692652-sq26.db"
TABLE      = "classifications_combined"
BATCH_SIZE = 50


# ── Schema ────────────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id     INTEGER NOT NULL,
            model_name     TEXT    NOT NULL,
            section        TEXT,
            section_name   TEXT,
            division       TEXT,
            division_name  TEXT,
            confidence     TEXT,
            method         TEXT,
            files_used     TEXT,
            file_source    TEXT,
            classified_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(project_id, model_name)
        )
    """)
    # Migrations for tables created with the old schema
    for col, typedef in [("files_used", "TEXT"), ("file_source", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN {col} {typedef}")
        except Exception:
            pass
    conn.commit()


# ── Data fetching ─────────────────────────────────────────────────────────────

def _confidence_filter_sql(tiers: list[str] | None) -> str:
    """Return a WHERE/AND clause restricting to specific vote confidence tiers."""
    if not tiers:
        return ""
    placeholders = ",".join("?" * len(tiers))
    return f"AND p.id IN (SELECT project_id FROM classifications_vote WHERE confidence IN ({placeholders}))"


def fetch_batch(
    conn: sqlite3.Connection,
    offset: int,
    limit: int,
    model_name: str,
    redo: bool,
    confidence_tiers: list[str] | None = None,
    redo_llm_uncertain: bool = False,
) -> list[dict]:
    tier_clause = _confidence_filter_sql(confidence_tiers)
    tier_params = list(confidence_tiers) if confidence_tiers else []

    if redo:
        where  = f"WHERE 1=1 {tier_clause}"
        params = tier_params + [limit, offset]
    elif redo_llm_uncertain:
        # Include: not yet done, OR already done but LLM gave low/medium confidence
        where = f"""WHERE (
            p.id NOT IN (SELECT project_id FROM {TABLE} WHERE model_name = ?)
            OR p.id IN (SELECT project_id FROM {TABLE}
                        WHERE model_name = ? AND confidence IN ('low','medium'))
        ) {tier_clause}"""
        params = [model_name, model_name] + tier_params + [limit, offset]
    else:
        where  = f"WHERE p.id NOT IN (SELECT project_id FROM {TABLE} WHERE model_name = ?) {tier_clause}"
        params = [model_name] + tier_params + [limit, offset]

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


def count_remaining(
    conn: sqlite3.Connection,
    model_name: str,
    redo: bool,
    confidence_tiers: list[str] | None = None,
    redo_llm_uncertain: bool = False,
) -> int:
    tier_clause = _confidence_filter_sql(confidence_tiers)
    tier_params = list(confidence_tiers) if confidence_tiers else []

    if redo:
        return conn.execute(
            f"SELECT COUNT(*) FROM projects p WHERE 1=1 {tier_clause}", tier_params
        ).fetchone()[0]
    if redo_llm_uncertain:
        return conn.execute(f"""
            SELECT COUNT(*) FROM projects p
            WHERE (
                p.id NOT IN (SELECT project_id FROM {TABLE} WHERE model_name = ?)
                OR p.id IN (SELECT project_id FROM {TABLE}
                            WHERE model_name = ? AND confidence IN ('low','medium'))
            ) {tier_clause}
        """, [model_name, model_name] + tier_params).fetchone()[0]
    return conn.execute(f"""
        SELECT COUNT(*) FROM projects p
        WHERE p.id NOT IN (
            SELECT project_id FROM {TABLE} WHERE model_name = ?
        ) {tier_clause}
    """, [model_name] + tier_params).fetchone()[0]


# ── Generic title detection ───────────────────────────────────────────────────

_GENERIC_TITLE_WORDS = {
    "documents", "document", "data", "dataset", "outputs", "output",
    "images", "image", "activities", "activity", "results", "figures",
    "tables", "cleaned tables", "cleaned data", "processed data",
    "summary tables", "raw tables", "raw data", "field data", "spatial data",
    "code", "python code", "r code", "matlab code", "scripts", "script",
    "appendix", "supplement", "supplementary", "readme", "notes",
    "figures and tables", "supplemental materials",
}
_SKIP_WORDS = {"the", "a", "an", "of", "for", "in", "and", "or",
               "data", "replication", "dataset", "file", "files"}
# Prefixes that add no domain signal — strip before checking
_REPLICATION_PREFIXES = (
    "replication data for:", "replication data for",
    "data for:", "dataset for:", "supplementary data for:",
    "supplemental data for:",
)


def _is_generic_title(title: str) -> bool:
    """Return True if the title carries no domain signal (sub-dataset / stub)."""
    t = (title or "").lower().strip()
    # Strip replication/data prefixes — they add no subject signal
    for prefix in _REPLICATION_PREFIXES:
        if t.startswith(prefix):
            t = t[len(prefix):].strip()
            break
    if t in _GENERIC_TITLE_WORDS:
        return True
    meaningful = [w for w in t.split() if w not in _SKIP_WORDS]
    return len(meaningful) <= 2


# ── File names from DB (free — no network) ────────────────────────────────────

def get_file_names(conn: sqlite3.Connection, project_id: int) -> list[str]:
    """
    Return up to 8 readable file names for a project from the files table.
    Sorted by priority (README > codebook > other). No network call needed.
    """
    rows = conn.execute("""
        SELECT file_name, file_type, file_size_bytes
        FROM files
        WHERE project_id = ?
          AND status = 'SUCCEEDED'
          AND file_type IN ('.txt','.pdf','.docx','.doc','.csv','.tab','.tsv',
                            '.atlproj','.atlproj23','.nvpx','.qdpx','.mex',
                            '.mx18','.mx20','.mx22','.mx24','.mqda','.f4','.f4a')
        LIMIT 20
    """, (project_id,)).fetchall()
    if not rows:
        return []
    from qdarchive.file_reader import _file_priority
    files = sorted(rows, key=lambda r: _file_priority(r[0], r[1]), reverse=True)
    return [r[0] for r in files[:8]]


# ── File snippets — Drive + Repo ──────────────────────────────────────────────

def get_snippets(
    p: dict,
    drive_reader,
    repo_reader,
) -> tuple[dict[str, str], str | None]:
    """
    Return (snippets_dict, source_label).
    source_label is "drive", "repo", or None.
    """
    if drive_reader and (p.get("gdrive_folder_id") or "").strip():
        try:
            snips = drive_reader.get_snippets(
                project_id=p["id"],
                folder_id=p["gdrive_folder_id"].strip(),
            )
            if snips:
                return snips, "drive"
        except Exception as e:
            log.debug(f"Drive failed for p{p['id']}: {e}")

    if repo_reader:
        try:
            snips = repo_reader.get_snippets(
                project_id=p["id"],
                doi=p.get("doi"),
                repo_url=p.get("repository_url"),
                project_url=p.get("project_url"),
            )
            if snips:
                return snips, "repo"
        except Exception as e:
            log.debug(f"Repo fetch failed for p{p['id']}: {e}")

    return {}, None


# ── DB write ──────────────────────────────────────────────────────────────────

def save_result(
    conn: sqlite3.Connection,
    project_id: int,
    model_name: str,
    result: dict,
    files_used: list[str] | None,
    file_source: str | None,
) -> None:
    conn.execute(f"""
        INSERT INTO {TABLE} (
            project_id, model_name,
            section, section_name, division, division_name,
            confidence, method, files_used, file_source
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(project_id, model_name) DO UPDATE SET
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
        project_id, model_name,
        result["section"], result["section_name"],
        result["division"], result["division_name"],
        result["confidence"], result["method"],
        json.dumps(files_used) if files_used else None,
        file_source,
    ))


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify QDArchive projects — metadata + Drive + Repo files."
    )
    parser.add_argument("--db",              default=DEFAULT_DB)
    parser.add_argument("--embed-model",     default="paraphrase-multilingual-MiniLM-L12-v2")
    parser.add_argument("--llm-model",       default="qwen2.5:7b")
    parser.add_argument("--top-k-sections",  type=int, default=3)
    parser.add_argument("--top-k-divisions", type=int, default=5)
    parser.add_argument("--limit",           type=int, default=None)
    parser.add_argument("--batch",           type=int, default=BATCH_SIZE)
    parser.add_argument("--redo",            action="store_true")
    parser.add_argument("--no-file-content", action="store_true",
                        help="Skip all file fetching (metadata only)")
    parser.add_argument("--no-drive",        action="store_true",
                        help="Skip Google Drive but still use Repo API")
    parser.add_argument("--no-repo",         action="store_true",
                        help="Skip Repo API but still use Google Drive")
    parser.add_argument("--dry-run",         action="store_true",
                        help="Print results without writing to DB")
    parser.add_argument("--confidence-tiers", default=None,
                        help="Restrict to projects with these vote-table confidence tiers "
                             "(comma-separated: low, medium, high, very_high). "
                             "Requires classifications_vote to be built first. "
                             "Example: --confidence-tiers low  or  --confidence-tiers low,medium")
    parser.add_argument("--redo-llm-uncertain", action="store_true",
                        help="Also re-run projects already classified by the LLM but where "
                             "the LLM itself returned low or medium confidence")
    parser.add_argument("--verbose",         action="store_true")
    args = parser.parse_args()

    confidence_tiers: list[str] | None = None
    if args.confidence_tiers:
        confidence_tiers = [t.strip() for t in args.confidence_tiers.split(",") if t.strip()]
        log.info(f"Targeting confidence tiers: {confidence_tiers}")

    redo_llm_uncertain: bool = args.redo_llm_uncertain
    if redo_llm_uncertain:
        log.info("Mode: also re-running projects where LLM previously gave low/medium confidence")

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        sys.exit(1)

    # ── Classifier ────────────────────────────────────────────────────────────
    log.info(f"Loading EnsembleClassifier (LLM={args.llm_model})...")
    from qdarchive.ensemble_classifier import EnsembleClassifier
    clf = EnsembleClassifier(
        embed_model=args.embed_model,
        llm_model=args.llm_model,
        top_k_sections=args.top_k_sections,
        top_k_divisions=args.top_k_divisions,
    )
    model_name = args.llm_model

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

    # ── Repo reader (Zenodo / Dataverse) ──────────────────────────────────────
    repo_reader = None
    if not args.no_file_content and not args.no_repo:
        from qdarchive.repo_file_reader import RepoFileReader
        repo_reader = RepoFileReader(db_path=db_path)

    # ── Summary of file sources enabled ──────────────────────────────────────
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
        sample = fetch_batch(conn, 0, n, model_name, redo=True,
                             confidence_tiers=confidence_tiers)

        print(f"\n{'═'*72}")
        print(f"  DRY-RUN — {len(sample)} projects")
        print(f"  LLM     : {'ON' if clf._ollama_ok else 'OFF (embedding only)'}")
        print(f"  Sources : {', '.join(sources)}")
        print(f"  Table   : {TABLE}  (not written in dry-run)")
        print(f"{'═'*72}\n")

        for p in sample:
            kw = [k.strip() for k in (p["keywords"] or "").split(",") if k.strip()]
            ft = [f.strip() for f in (p["file_types"] or "").split(",") if f.strip()]
            file_names = get_file_names(conn, p["id"])

            # Cascade: metadata+names first
            meta_result = clf.classify(
                title=p["title"] or "",
                description=p["description"] or "",
                keywords=kw, file_types=ft,
                snippets=None, file_names=file_names,
            )

            if meta_result["confidence"] == "high" and meta_result["division"] != "72":
                result, snips, src = meta_result, {}, None
                path = "cascade:skip_files"
            else:
                snips, src = get_snippets(p, drive_reader, repo_reader)
                if snips:
                    result = clf.classify(
                        title=p["title"] or "",
                        description=p["description"] or "",
                        keywords=kw, file_types=ft,
                        snippets=snips, file_names=file_names,
                    )
                    path = f"{src}+files"
                else:
                    result, path = meta_result, "metadata_only"

            src_tag = f"[{path}: {list(snips.keys())}]" if snips else f"[{path}]"
            print(f"[{p['id']}] {(p['title'] or '')[:70]}")
            if args.verbose:
                print(f"  Keywords   : {', '.join(kw[:8]) or '—'}")
                print(f"  File names : {', '.join(file_names[:5]) or '—'}")
            print(f"  → {result['division']} ({result['section']}): "
                  f"{result['division_name'][:48]}"
                  f"  [{result['confidence']}, {result['method']}]  {src_tag}")
            print()

        conn.close()
        return

    # ── Full run ──────────────────────────────────────────────────────────────
    total = count_remaining(conn, model_name, args.redo, confidence_tiers=confidence_tiers,
                            redo_llm_uncertain=redo_llm_uncertain)
    if args.limit:
        total = min(total, args.limit)
    log.info(f"Projects to classify: {total}  (model={model_name}, table={TABLE})")

    if total == 0:
        log.info("Nothing to do. Use --redo to re-run.")
        conn.close()
        return

    offset        = 0
    processed     = 0
    drive_used    = 0
    repo_used     = 0
    cascade_saved = 0   # projects that skipped file fetching (metadata was confident)
    t0 = time.time()

    try:
        while True:
            batch = fetch_batch(conn, offset, args.batch, model_name, args.redo,
                               confidence_tiers=confidence_tiers,
                               redo_llm_uncertain=redo_llm_uncertain)
            if not batch:
                break

            for p in batch:
                kw = [k.strip() for k in (p["keywords"] or "").split(",") if k.strip()]
                ft = [f.strip() for f in (p["file_types"] or "").split(",") if f.strip()]

                # ── Use pre-enriched knowledge if available ───────────────────
                knowledge = conn.execute(
                    "SELECT title, keywords, source_description, source_subjects, "
                    "file_snippets, file_names, file_source "
                    "FROM project_knowledge WHERE project_id=?", (p["id"],)
                ).fetchone()

                if knowledge:
                    pk_title       = knowledge[0] or p["title"] or ""
                    pk_keywords    = [k.strip() for k in (knowledge[1] or "").split(",") if k.strip()]
                    pk_source_desc = knowledge[2] or ""
                    pk_subjects    = json.loads(knowledge[3] or "[]")
                    file_snippets  = json.loads(knowledge[4] or "{}")
                    file_names_list = json.loads(knowledge[5] or "[]")
                    src             = knowledge[6]

                    # Build enriched description: source abstract + subject tags.
                    # Skip subject tags for generic titles (sub-datasets that inherit
                    # parent tags and would get mis-classified by them).
                    if _is_generic_title(pk_title):
                        # Generic title — trust file content over subject tags
                        enriched_desc = pk_source_desc[:400] if pk_source_desc else (p["description"] or "")
                    else:
                        enriched_desc = pk_source_desc or (p["description"] or "")
                        if pk_subjects:
                            subj_str = ", ".join(str(s) for s in pk_subjects[:6])
                            enriched_desc += f"\nSubjects: {subj_str}"

                    result = clf.classify(
                        title=pk_title,          # title as title (properly weighted)
                        description=enriched_desc[:600],
                        keywords=pk_keywords or kw,
                        file_types=ft,
                        snippets=file_snippets or None,
                        file_names=file_names_list,
                    )
                    snips = file_snippets
                    if src:
                        if src == "drive":  drive_used += 1
                        if src in ("zenodo", "dataverse"): repo_used += 1
                    cascade_saved += 1   # no new network call needed

                else:
                    # No pre-enrichment — use cascade pipeline
                    file_names = get_file_names(conn, p["id"])

                    # Classify with metadata + file names first
                    meta_result = clf.classify(
                        title=p["title"] or "",
                        description=p["description"] or "",
                        keywords=kw,
                        file_types=ft,
                        snippets=None,
                        file_names=file_names,
                    )

                    # If confident and specific, skip file fetching
                    if (meta_result["confidence"] == "high"
                            and meta_result["division"] != "72"
                            and not args.no_file_content):
                        result   = meta_result
                        src      = None
                        snips    = {}
                        cascade_saved += 1
                    else:
                        # Fetch file content (Drive first, then Repo)
                        if not args.no_file_content:
                            snips, src = get_snippets(p, drive_reader, repo_reader)
                        else:
                            snips, src = {}, None

                        if snips:
                            result = clf.classify(
                                title=p["title"] or "",
                                description=p["description"] or "",
                                keywords=kw,
                                file_types=ft,
                                snippets=snips,
                                file_names=file_names,
                            )
                        else:
                            result = meta_result

                files_used = list(snips.keys()) if snips else None
                if src == "drive": drive_used += 1
                if src == "repo":  repo_used  += 1
                save_result(conn, p["id"], model_name, result, files_used, src)
                processed += 1

                if processed % 50 == 0:
                    elapsed = time.time() - t0
                    rate    = processed / elapsed
                    eta_min = ((total - processed) / rate / 60) if rate > 0 else 0
                    log.info(
                        f"  {processed}/{total}  ({rate:.1f} proj/s)  "
                        f"drive={drive_used}  repo={repo_used}  "
                        f"cascade_saved={cascade_saved}  ETA {eta_min:.0f} min"
                    )

                if args.limit and processed >= args.limit:
                    break

            conn.commit()

            if args.redo:
                offset += len(batch)

            if args.limit and processed >= args.limit:
                break

    except KeyboardInterrupt:
        log.info(f"Interrupted at {processed}. Progress saved to DB.")
        conn.commit()
    finally:
        conn.close()

    elapsed = time.time() - t0
    log.info(
        f"Done. {processed} projects in {elapsed/60:.1f} min  |  "
        f"drive={drive_used}  repo={repo_used}  "
        f"metadata_only={processed - drive_used - repo_used}  "
        f"cascade_saved={cascade_saved} (skipped file fetch)"
    )


if __name__ == "__main__":
    main()
