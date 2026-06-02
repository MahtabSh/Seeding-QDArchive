#!/usr/bin/env python3
"""
ISIC Rev. 5 classifier for QDArchive — multi-classifier comparison.

Results stored in `classifications` table, one row per (project × model_name).
Each row has both Tier 1 (metadata) and Tier 2 (+ Drive files) columns.

Usage
-----
  # Dry-run: show results for 10 projects, one classifier
  python3 classify_isic.py --dry-run --limit 10 --verbose

  # Classify 10 projects with the LLM classifier (default: mistral)
  python3 classify_isic.py --limit 10

  # Run ALL 5 classifiers on the same 10 projects and compare
  python3 classify_isic.py --classifier all --limit 10 --redo --no-file-content

  # Dry-run comparison across all classifiers
  python3 classify_isic.py --dry-run --classifier all --limit 10

  # Individual non-LLM classifiers
  python3 classify_isic.py --classifier keyword --limit 10 --redo
  python3 classify_isic.py --classifier bm25    --limit 10 --redo
  python3 classify_isic.py --classifier nli     --limit 10 --redo

  # Different LLM
  python3 classify_isic.py --llm-model llama3.1:8b --limit 10 --redo
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
BATCH_SIZE    = 100
PROGRESS_FILE = "classify_isic.progress.json"


# ── Schema ────────────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS classifications (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id       INTEGER NOT NULL,
            model_name       TEXT    NOT NULL,

            t1_section       TEXT,
            t1_section_name  TEXT,
            t1_division      TEXT,
            t1_division_name TEXT,
            t1_confidence    TEXT,
            t1_method        TEXT,

            t2_section       TEXT,
            t2_section_name  TEXT,
            t2_division      TEXT,
            t2_division_name TEXT,
            t2_confidence    TEXT,
            t2_method        TEXT,
            t2_files         TEXT,

            classified_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(project_id, model_name)
        )
    """)
    conn.commit()


# ── Data fetching ─────────────────────────────────────────────────────────────

def fetch_batch(
    conn: sqlite3.Connection,
    offset: int,
    limit: int,
    model_name: str,
    redo: bool,
) -> list[dict]:
    if redo:
        where = ""
    else:
        where = """
            WHERE p.id NOT IN (
                SELECT project_id FROM classifications WHERE model_name = ?
            )
        """
    params = (limit, offset) if redo else (model_name, limit, offset)
    rows = conn.execute(f"""
        SELECT
            p.id,
            p.title,
            p.description,
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
    cols = ["id", "title", "description", "gdrive_folder_id", "keywords", "file_types"]
    return [dict(zip(cols, r)) for r in rows]


def count_remaining(conn: sqlite3.Connection, model_name: str, redo: bool) -> int:
    if redo:
        return conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    return conn.execute("""
        SELECT COUNT(*) FROM projects
        WHERE id NOT IN (
            SELECT project_id FROM classifications WHERE model_name = ?
        )
    """, (model_name,)).fetchone()[0]


# ── Drive snippets ────────────────────────────────────────────────────────────

def collect_snippets(reader, projects: list[dict]) -> dict[int, dict[str, str]]:
    if reader is None:
        return {}
    result: dict[int, dict[str, str]] = {}
    for p in projects:
        fid = (p.get("gdrive_folder_id") or "").strip()
        if not fid:
            continue
        try:
            snips = reader.get_snippets(project_id=p["id"], folder_id=fid)
            if snips:
                result[p["id"]] = snips
        except Exception as e:
            log.debug(f"Drive snippet failed for project {p['id']}: {e}")
    return result


# ── DB write ──────────────────────────────────────────────────────────────────

def save_result(
    conn: sqlite3.Connection,
    project_id: int,
    model_name: str,
    t1: dict,
    t2: dict | None,
    t2_files: list[str] | None,
) -> None:
    conn.execute("""
        INSERT INTO classifications (
            project_id, model_name,
            t1_section, t1_section_name, t1_division, t1_division_name, t1_confidence, t1_method,
            t2_section, t2_section_name, t2_division, t2_division_name, t2_confidence, t2_method,
            t2_files
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(project_id, model_name) DO UPDATE SET
            t1_section=excluded.t1_section, t1_section_name=excluded.t1_section_name,
            t1_division=excluded.t1_division, t1_division_name=excluded.t1_division_name,
            t1_confidence=excluded.t1_confidence, t1_method=excluded.t1_method,
            t2_section=excluded.t2_section, t2_section_name=excluded.t2_section_name,
            t2_division=excluded.t2_division, t2_division_name=excluded.t2_division_name,
            t2_confidence=excluded.t2_confidence, t2_method=excluded.t2_method,
            t2_files=excluded.t2_files,
            classified_at=datetime('now')
    """, (
        project_id, model_name,
        t1["section"], t1["section_name"], t1["division"], t1["division_name"],
        t1["confidence"], t1["method"],
        t2["section"] if t2 else None,
        t2["section_name"] if t2 else None,
        t2["division"] if t2 else None,
        t2["division_name"] if t2 else None,
        t2["confidence"] if t2 else None,
        t2["method"] if t2 else None,
        json.dumps(t2_files) if t2_files else None,
    ))


# ── Progress ──────────────────────────────────────────────────────────────────

def load_progress(path: Path) -> int:
    if path.exists():
        try:
            return json.loads(path.read_text())["offset"]
        except Exception:
            pass
    return 0


def save_progress(path: Path, offset: int) -> None:
    path.write_text(json.dumps({"offset": offset}))


# ── Classify one project (both tiers) ────────────────────────────────────────

def classify_project(clf, p: dict, snips: dict[str, str] | None):
    """Run classifier twice: once Tier 1 only, once Tier 1 + Tier 2.
    Returns (t1_result, t2_result_or_None)."""
    kw = [k.strip() for k in (p["keywords"] or "").split(",") if k.strip()]
    ft = [f.strip() for f in (p["file_types"] or "").split(",") if f.strip()]

    t1 = clf.classify(
        title=p["title"] or "",
        description=p["description"] or "",
        keywords=kw,
        file_types=ft,
        snippets=None,          # Tier 1: no Drive content
    )

    t2 = None
    if snips:
        t2 = clf.classify(
            title=p["title"] or "",
            description=p["description"] or "",
            keywords=kw,
            file_types=ft,
            snippets=snips,     # Tier 2: with Drive content
        )

    return t1, t2


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify QDArchive projects into ISIC Rev. 5."
    )
    parser.add_argument("--db",              default=DEFAULT_DB)
    parser.add_argument("--embed-model",     default="paraphrase-multilingual-MiniLM-L12-v2")
    parser.add_argument("--llm-model",       default="mistral",
                        help="Ollama model for --classifier llm (default: mistral)")
    parser.add_argument("--classifier",      default="llm",
                        choices=["llm", "nli", "bm25", "keyword", "all"],
                        help="Which classifier(s) to run (default: llm)")
    parser.add_argument("--top-k-sections",  type=int, default=3)
    parser.add_argument("--top-k-divisions", type=int, default=5)
    parser.add_argument("--limit",           type=int, default=None,
                        help="Stop after N projects (for testing)")
    parser.add_argument("--batch",           type=int, default=BATCH_SIZE)
    parser.add_argument("--redo",            action="store_true",
                        help="Re-classify rows already classified by this model")
    parser.add_argument("--no-file-content", action="store_true",
                        help="Skip Tier 2 (no Google Drive)")
    parser.add_argument("--dry-run",         action="store_true",
                        help="Show results without writing to DB")
    parser.add_argument("--verbose",         action="store_true",
                        help="Show keywords/files per project in output")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        sys.exit(1)

    # ── Build classifier list ─────────────────────────────────────────────────
    from qdarchive.ensemble_classifier import EnsembleClassifier
    from qdarchive.classifiers import build_classifier as _build_clf

    classifiers: list[tuple] = []   # (clf_instance, model_name)

    if args.classifier in ("llm", "all"):
        llm_names = ["mistral", "llama3.1:8b"] if args.classifier == "all" else [args.llm_model]
        for llm in llm_names:
            log.info(f"Initialising EnsembleClassifier (LLM={llm})...")
            try:
                clf = EnsembleClassifier(
                    embed_model=args.embed_model,
                    llm_model=llm,
                    top_k_sections=args.top_k_sections,
                    top_k_divisions=args.top_k_divisions,
                )
                classifiers.append((clf, llm))
            except Exception as e:
                log.warning(f"Could not load LLM classifier ({llm}): {e}")

    if args.classifier in ("keyword", "all"):
        try:
            clf = _build_clf("keyword", embed_model=args.embed_model)
            classifiers.append((clf, "keyword"))
        except Exception as e:
            log.warning(f"Could not load keyword classifier: {e}")

    if args.classifier in ("bm25", "all"):
        try:
            log.info("Initialising BM25Classifier...")
            clf = _build_clf("bm25", embed_model=args.embed_model)
            classifiers.append((clf, "bm25"))
        except Exception as e:
            log.warning(f"Could not load BM25 classifier: {e}")

    if args.classifier in ("nli", "all"):
        try:
            log.info("Initialising NLIClassifier...")
            clf = _build_clf("nli", embed_model=args.embed_model)
            classifiers.append((clf, "nli"))
        except Exception as e:
            log.warning(f"Could not load NLI classifier: {e}")

    if not classifiers:
        log.error("No classifiers could be initialized.")
        sys.exit(1)

    log.info(f"Classifiers ready: {[m for _, m in classifiers]}")

    # ── Drive reader (Tier 2) ─────────────────────────────────────────────────
    reader = None
    if not args.no_file_content:
        try:
            from qdarchive.file_reader import FileContentReader
            reader = FileContentReader()
            if reader._service is None:
                log.warning("Google Drive unavailable — running Tier 1 only.")
                reader = None
            else:
                log.info("Google Drive connected — Tier 2 enabled.")
        except Exception as e:
            log.warning(f"Drive reader error: {e} — Tier 1 only.")
    else:
        log.info("--no-file-content: Tier 2 disabled.")

    # ── DB ────────────────────────────────────────────────────────────────────
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    # ── Dry-run ───────────────────────────────────────────────────────────────
    if args.dry_run:
        from collections import Counter
        from qdarchive.isic import DIVISION_MAP

        n = args.limit or 10
        # Always use redo=True for dry-run so we can show any projects
        sample = fetch_batch(conn, 0, n, classifiers[0][1], redo=True)
        snippets_map = collect_snippets(reader, sample)
        multi = len(classifiers) > 1

        print(f"\n{'═'*72}")
        if multi:
            print(f"  COMPARISON DRY-RUN — {len(sample)} projects × {len(classifiers)} classifiers")
        else:
            clf0, m0 = classifiers[0]
            llm_status = getattr(clf0, '_ollama_ok', None)
            llm_info = f"  LLM: {'ON' if llm_status else 'OFF (embedding only)'}" if llm_status is not None else ""
            print(f"  DRY-RUN — {len(sample)} projects  |  {m0}{llm_info}")
        print(f"  Classifiers : {', '.join(m for _, m in classifiers)}")
        print(f"  Tier 2      : {'ON (' + str(len(snippets_map)) + ' projects with Drive files)' if reader else 'OFF'}")
        print(f"{'═'*72}\n")

        for p in sample:
            kw = [k.strip() for k in (p["keywords"] or "").split(",") if k.strip()]
            ft = [f.strip() for f in (p["file_types"] or "").split(",") if f.strip()]
            snips = snippets_map.get(p["id"])

            print(f"[{p['id']}] {(p['title'] or '')[:72]}")
            if args.verbose:
                print(f"  Keywords : {', '.join(kw[:8]) or '—'}")
                print(f"  Files    : {', '.join(sorted(set(ft))) or '—'}")
                if snips:
                    print(f"  Drive    : {list(snips.keys())}")

            t1_divs = []
            for clf, mname in classifiers:
                t1, t2 = classify_project(clf, p, snips)
                t1_divs.append(t1["division"])
                label = f"  {mname:<16}"
                print(f"{label}T1 → {t1['division']} ({t1['section']}): "
                      f"{t1['division_name'][:38]}  [{t1['confidence']}, {t1['method']}]")
                if t2:
                    match = "✓" if t1["division"] == t2["division"] else "≠ DIFFERENT"
                    print(f"{'':18}T2 → {t2['division']} ({t2['section']}): "
                          f"{t2['division_name'][:38]}  [{t2['confidence']}]  {match}")

            if multi:
                top_div, top_count = Counter(t1_divs).most_common(1)[0]
                top_sec = DIVISION_MAP.get(top_div, ("?", ""))[0]
                print(f"  {'─'*62}")
                print(f"  Consensus       → Div {top_div} ({top_sec})  "
                      f"[{top_count}/{len(classifiers)} agree]")
            print()

        conn.close()
        return

    # ── Full run ──────────────────────────────────────────────────────────────
    first_model = classifiers[0][1]
    total = count_remaining(conn, first_model, args.redo)
    if args.limit:
        total = min(total, args.limit)
    log.info(f"Projects to classify: {total}  "
             f"(classifiers={[m for _, m in classifiers]})")

    if total == 0:
        log.info("Nothing to do. Use --redo to re-run.")
        conn.close()
        return

    # ── Resume logic ──────────────────────────────────────────────────────────
    # Non-redo mode: fetch_batch uses NOT IN to skip classified projects, so
    # offset stays at 0 for every batch — after each commit the NOT IN clause
    # naturally moves to the next unclassified batch. No progress file needed.
    #
    # Redo mode: NOT IN is disabled (we iterate the whole table), so we track
    # position with a progress file to allow resuming after interruption.
    if args.redo:
        progress_path = Path(f"{PROGRESS_FILE}.{first_model}")
        offset = load_progress(progress_path)
        if offset:
            log.info(f"Resuming from offset {offset}")
    else:
        progress_path = None
        offset = 0

    processed = 0
    t0 = time.time()

    try:
        while True:
            batch = fetch_batch(conn, offset, args.batch, first_model, args.redo)
            if not batch:
                break

            snippets_map = collect_snippets(reader, batch)
            if snippets_map:
                log.info(f"  Drive content for {len(snippets_map)}/{len(batch)} projects")

            for p in batch:
                snips = snippets_map.get(p["id"])

                for clf, mname in classifiers:
                    t1, t2 = classify_project(clf, p, snips)
                    t2_files = list(snips.keys()) if snips else None
                    save_result(conn, p["id"], mname, t1, t2, t2_files)

                processed += 1

                if processed % 50 == 0:
                    elapsed = time.time() - t0
                    rate = processed / elapsed
                    eta_min = ((total - processed) / rate / 60) if rate > 0 else 0
                    log.info(f"  {processed}/{total}  ({rate:.1f} proj/s, ETA {eta_min:.0f} min)")

                if args.limit and processed >= args.limit:
                    break

            conn.commit()

            if args.redo:
                # Advance the file-based offset only in redo mode
                offset += len(batch)
                save_progress(progress_path, offset)
            # Non-redo: offset stays 0 — NOT IN excludes the just-committed
            # rows so the next fetch naturally returns the next unclassified batch

            if args.limit and processed >= args.limit:
                break

    except KeyboardInterrupt:
        log.info(f"Interrupted at {processed}. Progress saved.")
        conn.commit()
    finally:
        conn.close()

    elapsed = time.time() - t0
    log.info(f"Done. {processed} projects × {len(classifiers)} classifiers "
             f"in {elapsed/60:.1f} min.")
    if progress_path and progress_path.exists():
        progress_path.unlink()


if __name__ == "__main__":
    main()
