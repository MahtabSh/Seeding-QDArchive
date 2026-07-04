#!/usr/bin/env python3
"""
Classify individual files using the trained bert_base_isic_v1 model.

Source of content: project_knowledge.file_snippets (JSON dict: filename → snippet)
Text per file: title + description + keywords + file_name (cleaned) + snippet (first 300 chars)
Scope: QDA_PROJECT and QD_PROJECT only (files in OTHER / NOT_A_PROJECT are not classified)
Output table: classifications_files

Usage:
    python3 classify_files_bert.py --db 23692652-sq26.db
    python3 classify_files_bert.py --db 23692652-sq26.db --batch-size 64
    python3 classify_files_bert.py --db 23692652-sq26.db --stats
    python3 classify_files_bert.py --db 23692652-sq26.db --type QDA_PROJECT
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
MODEL_DIR     = "models/bert_base_isic_v1"
TABLE         = "classifications_files"

ISIC_SECTIONS: dict[str, str] = {
    "A": "Agriculture, forestry and fishing",
    "B": "Mining and quarrying",
    "C": "Manufacturing",
    "D": "Electricity, gas, steam and air conditioning supply",
    "E": "Water supply; sewerage, waste management and remediation activities",
    "F": "Construction",
    "G": "Wholesale and retail trade",
    "H": "Transportation and storage",
    "I": "Accommodation and food service activities",
    "J": "Publishing, broadcasting, and content production and distribution activities",
    "K": "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities",
    "L": "Financial and insurance activities",
    "M": "Real estate activities",
    "N": "Professional, scientific and technical activities",
    "O": "Administrative and support service activities",
    "P": "Public administration and defence; compulsory social security",
    "Q": "Education",
    "R": "Human health and social work activities",
    "S": "Arts, sports and recreation",
    "T": "Other service activities",
    "U": "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use",
    "V": "Activities of extraterritorial organizations and bodies",
}

_CREATE_SQL = f"""CREATE TABLE IF NOT EXISTS {TABLE} (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id    INTEGER NOT NULL,
    project_type  TEXT,
    file_name     TEXT NOT NULL,
    section       TEXT,
    section_name  TEXT,
    confidence    TEXT,
    prob_top1     REAL,
    prob_top2     REAL,
    section_top2  TEXT,
    classified_at TEXT DEFAULT (datetime('now')),
    UNIQUE(project_id, file_name)
)"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_SQL)
    conn.commit()


def make_file_text(
    title: str | None,
    description: str | None,
    keywords: str | None,
    file_name: str,
    snippet: str | None,
) -> str:
    parts = []
    if title:
        parts.append(title.strip())
    if description:
        parts.append(description.strip()[:400])
    if keywords:
        parts.append(keywords.strip()[:150])
    # Clean file name: remove extension, replace _ and - with spaces
    base = file_name.rsplit(".", 1)[0] if "." in file_name else file_name
    cleaned = base.replace("_", " ").replace("-", " ").strip()
    if cleaned:
        parts.append(cleaned[:150])
    if snippet:
        parts.append(snippet.strip()[:300])
    return " ".join(parts) or "untitled"


def confidence_from_prob(p: float) -> str:
    if p >= 0.80:
        return "very_high"
    if p >= 0.60:
        return "high"
    if p >= 0.40:
        return "medium"
    return "low"


def load_file_records(
    conn: sqlite3.Connection,
    project_types: list[str],
) -> list[tuple[int, str, str, str, str, str]]:
    """Return list of (project_id, project_type, title, description, keywords, file_snippets_json)."""
    placeholders = ",".join("?" * len(project_types))
    rows = conn.execute(f"""
        SELECT p.id, p.type, p.title, p.description,
               GROUP_CONCAT(DISTINCT k.keyword) AS keywords,
               pk.file_snippets
        FROM projects p
        JOIN project_knowledge pk ON pk.project_id = p.id
        LEFT JOIN keywords k ON k.project_id = p.id
        WHERE p.type IN ({placeholders})
          AND pk.file_snippets IS NOT NULL
          AND pk.file_snippets != ''
        GROUP BY p.id
    """, project_types).fetchall()
    log.info(f"Loaded {len(rows):,} projects with file snippets")
    return rows


def expand_to_files(
    project_rows: list[tuple],
) -> list[tuple[int, str, str, str]]:
    """
    Expand project rows into individual file records.
    Returns list of (project_id, project_type, file_name, text_for_bert).
    """
    records = []
    for pid, ptype, title, desc, kw, snippets_json in project_rows:
        try:
            snippets: dict[str, str] = json.loads(snippets_json)
        except (json.JSONDecodeError, TypeError):
            continue
        for file_name, snippet in snippets.items():
            text = make_file_text(title, desc, kw, file_name, snippet)
            records.append((pid, ptype, file_name, text))
    return records


def run_inference(
    records: list[tuple[int, str, str, str]],
    model_dir: str,
    batch_size: int,
) -> list[tuple[int, str, str, str, str, float, float, str]]:
    """
    Classify each file record. Returns list of:
    (project_id, project_type, file_name, section, section_name,
     confidence, prob_top1, prob_top2, section_top2)
    """
    try:
        import torch
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        import numpy as np
    except ImportError as e:
        log.error(f"Missing dependency: {e}")
        sys.exit(1)

    model_path = Path(model_dir)
    if not model_path.exists():
        log.error(f"Model not found at {model_dir} — run classify_bert.py first")
        sys.exit(1)

    # Device
    if torch.backends.mps.is_available():
        device = torch.device("mps")
        log.info("Device: Apple Silicon MPS")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
        log.info(f"Device: CUDA ({torch.cuda.get_device_name(0)})")
    else:
        device = torch.device("cpu")
        log.info("Device: CPU")

    log.info(f"Loading model from {model_dir}…")
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir)
    model.to(device)
    model.eval()

    id2label: dict[int, str] = model.config.id2label

    texts      = [r[3] for r in records]
    n          = len(texts)
    results    = []
    t0         = time.time()

    log.info(f"Classifying {n:,} files in batches of {batch_size}…")

    for start in range(0, n, batch_size):
        batch_texts = texts[start:start + batch_size]
        enc = tokenizer(
            batch_texts,
            truncation=True,
            padding=True,
            max_length=256,
            return_tensors="pt",
        )
        enc = {k: v.to(device) for k, v in enc.items()}

        with torch.no_grad():
            logits = model(**enc).logits

        probs = torch.softmax(logits, dim=-1).cpu().numpy()

        for i, (pid, ptype, fname, _) in enumerate(records[start:start + batch_size]):
            row_probs = probs[i]
            top_idx   = int(row_probs.argmax())
            sorted_idx = row_probs.argsort()[::-1]
            top2_idx  = int(sorted_idx[1]) if len(sorted_idx) > 1 else top_idx

            sec       = id2label[top_idx]
            p1        = float(row_probs[top_idx])
            p2        = float(row_probs[top2_idx])
            sec2      = id2label[top2_idx]
            conf      = confidence_from_prob(p1)

            results.append((
                pid, ptype, fname,
                sec, ISIC_SECTIONS.get(sec, ""),
                conf, p1, p2, sec2,
            ))

        done = min(start + batch_size, n)
        elapsed = time.time() - t0
        speed = done / elapsed if elapsed > 0 else 0
        eta = (n - done) / speed if speed > 0 else 0
        if done % (batch_size * 20) == 0 or done == n:
            log.info(f"  {done:>7,}/{n:,}  {speed:.0f} files/s  ETA {eta/60:.1f}min")

    log.info(f"Inference done: {n:,} files in {(time.time()-t0)/60:.1f}min")
    return results


def save_results(
    conn: sqlite3.Connection,
    results: list[tuple],
    replace: bool = False,
) -> None:
    if replace:
        conn.execute(f"DELETE FROM {TABLE}")
    conn.executemany(f"""
        INSERT OR REPLACE INTO {TABLE}
            (project_id, project_type, file_name, section, section_name,
             confidence, prob_top1, prob_top2, section_top2)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, results)
    conn.commit()
    log.info(f"Saved {len(results):,} file classifications to {TABLE}")


def show_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if total == 0:
        print(f"{TABLE} is empty — run without --stats first.")
        return

    print(f"\n{'═'*72}")
    print(f"  {TABLE}  —  {total:,} file classifications")
    print(f"{'═'*72}\n")

    print("By project type:")
    for ptype, n in conn.execute(f"SELECT project_type, COUNT(*) FROM {TABLE} GROUP BY project_type ORDER BY COUNT(*) DESC"):
        print(f"  {ptype or 'NULL':<20} {n:>8,}")

    print("\nConfidence distribution:")
    order = "CASE confidence WHEN 'very_high' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
    for conf, n, pct in conn.execute(f"""
        SELECT confidence, COUNT(*), ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY confidence ORDER BY {order}
    """):
        bar = "█" * int(pct / 2)
        print(f"  {conf:<12} {n:>7,}  {pct:>5}%  {bar}")

    print("\nTop 10 sections:")
    for sec, name, n, pct in conn.execute(f"""
        SELECT section, section_name, COUNT(*), ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY section ORDER BY COUNT(*) DESC LIMIT 10
    """):
        print(f"  {sec:<4} {(name or '')[:48]:<48} {n:>7,}  {pct:>5}%")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Classify individual files using bert_base_isic_v1."
    )
    ap.add_argument("--db",         default=DEFAULT_DB)
    ap.add_argument("--model",      default=MODEL_DIR)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--type",       default="QDA_PROJECT,QD_PROJECT",
                    help="Comma-separated project types to classify (default: QDA_PROJECT,QD_PROJECT)")
    ap.add_argument("--stats",      action="store_true",
                    help="Show stats from existing table (no inference)")
    ap.add_argument("--replace",    action="store_true",
                    help="Delete existing rows before inserting (default: INSERT OR REPLACE)")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    project_types = [t.strip() for t in args.type.split(",")]
    log.info(f"Project types: {project_types}")

    project_rows = load_file_records(conn, project_types)
    if not project_rows:
        log.error("No projects found with file snippets for the given types")
        sys.exit(1)

    file_records = expand_to_files(project_rows)
    log.info(f"Total files to classify: {len(file_records):,}")

    results = run_inference(file_records, args.model, args.batch_size)
    save_results(conn, results, replace=args.replace)
    show_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
