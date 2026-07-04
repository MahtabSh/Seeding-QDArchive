#!/usr/bin/env python3
"""
Sentence-transformer cosine similarity classifier for ISIC Rev. 5.

Embeds all project texts and ISIC section/division descriptions, picks the
section with the highest cosine similarity (Stage A), then the best division
within that section (Stage B).

No Ollama, no API keys — just:
    pip install sentence-transformers

Results → table: classifications_st  (never touches other tables)

Pass 1 — metadata only (~5 min for 53k projects on CPU):
    python3 classify_st.py --no-file-content

Pass 2 — enrich with file content:
    python3 classify_st.py --redo-with-files

Other:
    python3 classify_st.py --dry-run --limit 20
    python3 classify_st.py --redo
    python3 classify_st.py --model all-mpnet-base-v2   # better quality, slower
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
TABLE         = "classifications_st"
DEFAULT_MODEL = "all-MiniLM-L6-v2"
ENCODE_BATCH  = 512

from classify_combined import get_file_names, get_snippets, _is_generic_title
from qdarchive.nli_classifier import _SECTION_DESCRIPTIONS, _DIVISION_DESCRIPTIONS
from qdarchive.isic import SECTIONS, DIVISIONS, SECTION_MAP


# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_SQL = f"""CREATE TABLE {TABLE} (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id    INTEGER NOT NULL,
    model_name    TEXT    NOT NULL,
    tier          TEXT    NOT NULL DEFAULT 'metadata',
    section       TEXT, section_name  TEXT,
    division      TEXT, division_name TEXT,
    confidence    TEXT, method        TEXT,
    files_used    TEXT, file_source   TEXT,
    classified_at TEXT DEFAULT (datetime('now')),
    UNIQUE(project_id, model_name, tier)
)"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    exists = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)
    ).fetchone()
    if not exists:
        conn.execute(_CREATE_SQL)
        conn.commit()
        return
    sql = (exists[0] or "").replace(" ", "").replace("\n", "")
    if "UNIQUE(project_id,model_name,tier)" in sql:
        return
    log.info(f"Migrating {TABLE}…")
    conn.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_old")
    conn.execute(_CREATE_SQL)
    conn.execute(f"""INSERT OR IGNORE INTO {TABLE}
        (project_id,model_name,tier,section,section_name,division,division_name,
         confidence,method,files_used,file_source,classified_at)
        SELECT project_id,model_name,COALESCE(tier,'metadata'),section,section_name,
               division,division_name,confidence,method,files_used,file_source,classified_at
        FROM {TABLE}_old""")
    conn.execute(f"DROP TABLE {TABLE}_old")
    conn.commit()
    log.info("Migration done.")


# ── DB helpers ─────────────────────────────────────────────────────────────────

_COLS = ["id", "title", "description", "doi", "repository_url", "project_url",
         "gdrive_folder_id", "keywords", "file_types"]


def fetch_projects(
    conn: sqlite3.Connection,
    model_name: str,
    redo: bool,
    redo_with_files: bool,
    limit: int | None = None,
) -> list[dict]:
    if redo:
        where, params = "", ()
    elif redo_with_files:
        where = f"""WHERE p.id IN (
                SELECT project_id FROM {TABLE} WHERE model_name=? AND tier='metadata')
            AND p.id NOT IN (
                SELECT project_id FROM {TABLE} WHERE model_name=? AND tier='files')
            AND (p.gdrive_folder_id IS NOT NULL AND p.gdrive_folder_id != ''
                 OR p.id IN (SELECT project_id FROM project_knowledge)
                 OR p.repository_url LIKE '%zenodo%'
                 OR p.repository_url LIKE '%dataverse%')"""
        params = (model_name, model_name)
    else:
        where = f"""WHERE p.id NOT IN (
                SELECT project_id FROM {TABLE} WHERE model_name=? AND tier='metadata')"""
        params = (model_name,)

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
    """, params).fetchall()

    projects = [dict(zip(_COLS, r)) for r in rows]
    return projects[:limit] if limit else projects


def save_results_bulk(
    conn: sqlite3.Connection,
    projects: list[dict],
    results: list[dict],
    model_name: str,
    tier: str = "metadata",
    files_used_list: list | None = None,
    file_source_list: list | None = None,
) -> None:
    files_used_list  = files_used_list  or [None] * len(projects)
    file_source_list = file_source_list or [None] * len(projects)
    conn.executemany(f"""
        INSERT INTO {TABLE}
            (project_id, model_name, tier, section, section_name,
             division, division_name, confidence, method, files_used, file_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(project_id, model_name, tier) DO UPDATE SET
            section=excluded.section, section_name=excluded.section_name,
            division=excluded.division, division_name=excluded.division_name,
            confidence=excluded.confidence, method=excluded.method,
            files_used=excluded.files_used, file_source=excluded.file_source,
            classified_at=datetime('now')
    """, [
        (p["id"], model_name, tier,
         r["section"], r["section_name"],
         r["division"], r["division_name"],
         r["confidence"], r["method"],
         json.dumps(fu) if fu else None, fs)
        for p, r, fu, fs in zip(projects, results, files_used_list, file_source_list)
    ])
    conn.commit()


# ── Text builder ───────────────────────────────────────────────────────────────

def build_text(p: dict, snippets: dict | None = None) -> str:
    parts: list[str] = []
    t = (p.get("title") or "").strip()
    if t:
        parts.extend([t] * 3)
    kw = [k.strip() for k in (p.get("keywords") or "").split(",") if k.strip()]
    if kw:
        parts.extend([" ".join(kw[:20])] * 2)
    d = (p.get("description") or "").strip()
    if d:
        parts.append(d[:400])
    if snippets:
        for txt in list(snippets.values())[:3]:
            parts.append(str(txt)[:200])
    return " ".join(parts)


# ── Classifier ─────────────────────────────────────────────────────────────────

class STClassifier:
    def __init__(self, model_name: str = DEFAULT_MODEL):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError("Run: pip install sentence-transformers")

        log.info(f"Loading sentence-transformer: {model_name}")
        self._model     = SentenceTransformer(model_name)
        self.model_name = model_name

        self._sec_letters = [s for s, _ in SECTIONS]
        self._sec_embs = self._model.encode(
            [_SECTION_DESCRIPTIONS[s] for s in self._sec_letters],
            normalize_embeddings=True, show_progress_bar=False,
        )  # (22, D)

        self._div_list = list(DIVISIONS)  # [(sec, div, name), ...]
        self._div_embs = self._model.encode(
            [_DIVISION_DESCRIPTIONS.get(div, name) for _, div, name in self._div_list],
            normalize_embeddings=True, show_progress_bar=False,
        )  # (87, D)

        # Pre-build section → division indices lookup
        self._div_by_sec: dict[str, list[int]] = {}
        for i, (s, _, _) in enumerate(self._div_list):
            self._div_by_sec.setdefault(s, []).append(i)

        log.info("Classifier ready.")

    def classify_batch(self, texts: list[str]) -> list[dict]:
        """Encode all texts at once — the fast path."""
        import numpy as np

        proj_embs = self._model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=len(texts) > 500,
            batch_size=ENCODE_BATCH,
        )  # (N, D)

        sec_scores = proj_embs @ self._sec_embs.T  # (N, 22)
        sec_idxs   = sec_scores.argmax(axis=1)     # (N,)

        results = []
        for i, sec_idx in enumerate(sec_idxs):
            section   = self._sec_letters[int(sec_idx)]
            sec_score = float(sec_scores[i, sec_idx])

            d_idxs   = self._div_by_sec.get(section, list(range(len(self._div_list))))
            d_scores = proj_embs[i] @ self._div_embs[d_idxs].T  # (K,)
            best_k   = int(d_scores.argmax())
            _, division, div_name = self._div_list[d_idxs[best_k]]
            div_score = float(d_scores[best_k])

            confidence = (
                "high"   if div_score > 0.40 else
                "medium" if div_score > 0.25 else
                "low"
            )
            results.append({
                "section":       section,
                "section_name":  SECTION_MAP.get(section, ""),
                "division":      division,
                "division_name": div_name,
                "confidence":    confidence,
                "method":        "sentence_transformer_cosine",
            })
        return results


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Classify QDArchive projects using sentence-transformer cosine similarity."
    )
    ap.add_argument("--db",              default=DEFAULT_DB)
    ap.add_argument("--model",           default=DEFAULT_MODEL,
                    help="Sentence-transformer model name (HuggingFace hub or local path)")
    ap.add_argument("--limit",           type=int, default=None)
    ap.add_argument("--redo",            action="store_true",
                    help="Re-classify ALL projects already in the table")
    ap.add_argument("--redo-with-files", action="store_true",
                    help="Second pass: add file-content tier for projects that have sources")
    ap.add_argument("--no-file-content", action="store_true",
                    help="Skip all file fetching — fastest, saves as tier=metadata")
    ap.add_argument("--no-drive",        action="store_true")
    ap.add_argument("--no-repo",         action="store_true")
    ap.add_argument("--dry-run",         action="store_true",
                    help="Print results without writing to DB")
    ap.add_argument("--verbose",         action="store_true")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}"); sys.exit(1)

    clf        = STClassifier(args.model)
    model_name = clf.model_name
    redo_wf    = getattr(args, "redo_with_files", False)

    drive_reader = repo_reader = None
    if not args.no_file_content and not args.no_drive:
        try:
            from qdarchive.file_reader import FileContentReader
            dr = FileContentReader()
            drive_reader = dr if dr._service else None
        except Exception:
            pass
    if not args.no_file_content and not args.no_repo:
        try:
            from qdarchive.repo_file_reader import RepoFileReader
            repo_reader = RepoFileReader(db_path=db_path)
        except Exception:
            pass

    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    projects = fetch_projects(conn, model_name, args.redo, redo_wf, args.limit)
    log.info(f"Projects to classify: {len(projects)}  table={TABLE}")

    if not projects:
        log.info("Nothing to do. Use --redo to re-classify.")
        conn.close()
        return

    # ── Pass 1: metadata-only (batch encode everything at once) ───────────────
    if args.no_file_content:
        log.info("Building texts from metadata + knowledge cache (no file snippets)…")
        texts = []
        for p in projects:
            row = conn.execute(
                "SELECT title, keywords, source_description, source_subjects "
                "FROM project_knowledge WHERE project_id=?", (p["id"],)
            ).fetchone()
            if row:
                pk_title = row[0] or p["title"] or ""
                pk_kw    = row[1] or p.get("keywords") or ""
                pk_desc  = row[2] or p.get("description") or ""
                subj     = json.loads(row[3] or "[]")
                if subj:
                    pk_desc += "\nSubjects: " + ", ".join(str(s) for s in subj[:6])
                aug = {**p, "title": pk_title, "keywords": pk_kw, "description": pk_desc}
                texts.append(build_text(aug))
            else:
                texts.append(build_text(p))

        log.info(f"Encoding {len(texts)} texts in one batch…")
        t0 = time.time()
        results = clf.classify_batch(texts)
        log.info(f"Encoded and classified in {time.time()-t0:.1f}s")

        if args.dry_run:
            for p, r in zip(projects[:20], results[:20]):
                print(f"[{p['id']}] {(p['title'] or '')[:70]}")
                print(f"  → {r['division']} ({r['section']}): "
                      f"{r['division_name'][:50]}  [{r['confidence']}]")
            conn.close()
            return

        log.info("Saving to DB…")
        save_results_bulk(conn, projects, results, model_name, tier="metadata")
        log.info(f"Saved {len(projects)} results to {TABLE} (tier=metadata).")

    # ── Pass 2: per-project with file content ─────────────────────────────────
    else:
        drive_used = repo_used = 0
        t0 = time.time()
        COMMIT_EVERY = 500
        batch_p, batch_r, batch_fu, batch_fs = [], [], [], []

        for i, p in enumerate(projects):
            row = conn.execute(
                "SELECT title, keywords, source_description, source_subjects, "
                "file_snippets, file_names, file_source "
                "FROM project_knowledge WHERE project_id=?", (p["id"],)
            ).fetchone()

            if row:
                pk_title = row[0] or p["title"] or ""
                pk_kw    = row[1] or p.get("keywords") or ""
                pk_desc  = row[2] or p.get("description") or ""
                subj     = json.loads(row[3] or "[]")
                snips    = json.loads(row[4] or "{}")
                src      = row[6]
                if subj:
                    pk_desc += "\nSubjects: " + ", ".join(str(s) for s in subj[:6])
                aug = {**p, "title": pk_title, "keywords": pk_kw, "description": pk_desc}
                text = build_text(aug, snippets=snips or None)
                if src == "drive":                 drive_used += 1
                elif src in ("zenodo","dataverse"): repo_used  += 1
            else:
                snips, src = get_snippets(p, drive_reader, repo_reader)
                text = build_text(p, snippets=snips or None)
                if src == "drive": drive_used += 1
                if src == "repo":  repo_used  += 1

            result = clf.classify_batch([text])[0]
            fu = list(snips.keys()) if snips else None

            batch_p.append(p); batch_r.append(result)
            batch_fu.append(fu); batch_fs.append(src)

            if len(batch_p) >= COMMIT_EVERY:
                save_results_bulk(conn, batch_p, batch_r, model_name,
                                  tier="files", files_used_list=batch_fu,
                                  file_source_list=batch_fs)
                batch_p.clear(); batch_r.clear()
                batch_fu.clear(); batch_fs.clear()
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed
                eta  = (len(projects) - i - 1) / rate / 60
                log.info(f"  {i+1}/{len(projects)} ({rate:.1f}/s)  "
                         f"drive={drive_used} repo={repo_used}  ETA {eta:.0f} min")

        if batch_p:
            save_results_bulk(conn, batch_p, batch_r, model_name,
                              tier="files", files_used_list=batch_fu,
                              file_source_list=batch_fs)

        elapsed = time.time() - t0
        log.info(f"Done. {len(projects)} projects in {elapsed/60:.1f} min | "
                 f"drive={drive_used} repo={repo_used}")

    conn.close()


if __name__ == "__main__":
    main()
