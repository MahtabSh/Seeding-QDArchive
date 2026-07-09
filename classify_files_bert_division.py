#!/usr/bin/env python3
"""
Add ISIC Rev.5 division-level classification to existing file rows in
classifications_files using the trained bert_division_v1 model.

Prerequisite: classify_files_bert.py must have been run first (rows must exist).

This script:
  1. Adds division / division_name columns to classifications_files if absent.
  2. Reads file text from project_knowledge.file_snippets (same as classify_files_bert.py).
  3. Runs bert_division_v1 to predict the ISIC division for each file.
  4. UPDATEs the existing rows in classifications_files.

Usage:
    python3 classify_files_bert_division.py --db 23692652-sq26.db
    python3 classify_files_bert_division.py --db 23692652-sq26.db --batch-size 64
    python3 classify_files_bert_division.py --db 23692652-sq26.db --stats
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

DEFAULT_DB  = "23692652-sq26.db"
MODEL_DIR   = "models/bert_division_v1"
TABLE       = "classifications_files"

DIVISION_TO_SECTION: dict[str, str] = {
    "01": "A", "02": "A", "03": "A",
    "05": "B", "06": "B", "07": "B", "08": "B", "09": "B",
    "10": "C", "11": "C", "12": "C", "13": "C", "14": "C", "15": "C",
    "16": "C", "17": "C", "18": "C", "19": "C", "20": "C", "21": "C",
    "22": "C", "23": "C", "24": "C", "25": "C", "26": "C", "27": "C",
    "28": "C", "29": "C", "30": "C", "31": "C", "32": "C", "33": "C",
    "35": "D",
    "36": "E", "37": "E", "38": "E", "39": "E",
    "41": "F", "42": "F", "43": "F",
    "45": "G", "46": "G", "47": "G",
    "49": "H", "50": "H", "51": "H", "52": "H", "53": "H",
    "55": "I", "56": "I",
    "58": "J", "59": "J", "60": "J",
    "61": "K", "62": "K", "63": "K",
    "64": "L", "65": "L", "66": "L",
    "68": "M",
    "69": "N", "70": "N", "71": "N", "72": "N", "73": "N", "74": "N", "75": "N",
    "77": "O", "78": "O", "79": "O", "80": "O", "81": "O", "82": "O",
    "84": "P",
    "85": "Q",
    "86": "R", "87": "R", "88": "R",
    "90": "S", "91": "S", "92": "S", "93": "S",
    "94": "T", "96": "T",
    "97": "U", "98": "U",
    "99": "V",
}


def ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({TABLE})")}
    for col, coltype in [("division", "TEXT"), ("division_name", "TEXT")]:
        if col not in existing:
            conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN {col} {coltype}")
            log.info(f"Added column {col} to {TABLE}")
    conn.commit()


def load_division_names(conn: sqlite3.Connection) -> dict[str, str]:
    """Pull division→name from project-level classifications if available."""
    rows = conn.execute("""
        SELECT DISTINCT division, division_name
        FROM classifications_bert_division
        WHERE division IS NOT NULL AND division_name IS NOT NULL AND division_name != ''
    """).fetchall()
    return {div: name for div, name in rows}


def make_file_text(title, description, keywords, file_name, snippet) -> str:
    parts = []
    if title:
        parts.append(title.strip())
    if description:
        parts.append(description.strip()[:400])
    if keywords:
        parts.append(keywords.strip()[:150])
    base = file_name.rsplit(".", 1)[0] if "." in file_name else file_name
    cleaned = base.replace("_", " ").replace("-", " ").strip()
    if cleaned:
        parts.append(cleaned[:150])
    if snippet:
        parts.append(snippet.strip()[:300])
    return " ".join(parts) or "untitled"


def confidence_from_prob(p: float) -> str:
    if p >= 0.80: return "very_high"
    if p >= 0.60: return "high"
    if p >= 0.40: return "medium"
    return "low"


def load_file_records(conn: sqlite3.Connection) -> list[tuple]:
    """Return (project_id, project_type, title, description, keywords, file_snippets_json)."""
    rows = conn.execute("""
        SELECT p.id, p.type, p.title, p.description,
               GROUP_CONCAT(DISTINCT k.keyword) AS keywords,
               pk.file_snippets
        FROM projects p
        JOIN project_knowledge pk ON pk.project_id = p.id
        LEFT JOIN keywords k ON k.project_id = p.id
        WHERE p.type IN ('QDA_PROJECT','QD_PROJECT')
          AND pk.file_snippets IS NOT NULL AND pk.file_snippets != ''
        GROUP BY p.id
    """).fetchall()
    log.info(f"Loaded {len(rows):,} projects with file snippets")
    return rows


def expand_to_files(project_rows: list[tuple]) -> list[tuple]:
    """Expand to (project_id, project_type, file_name, text)."""
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


def run_inference(records: list[tuple], model_dir: str, batch_size: int,
                  div_names: dict[str, str]) -> list[tuple]:
    """
    Classify each file. Returns list of:
    (project_id, file_name, division, division_name)
    """
    try:
        import torch
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
    except ImportError as e:
        log.error(f"Missing dependency: {e}")
        sys.exit(1)

    model_path = Path(model_dir)
    if not model_path.exists():
        log.error(f"Model not found at {model_dir}")
        sys.exit(1)

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

    id2label: dict[int, str] = {int(k): v for k, v in model.config.id2label.items()}
    texts = [r[3] for r in records]
    n = len(texts)
    results = []
    t0 = time.time()

    log.info(f"Classifying {n:,} files in batches of {batch_size}…")

    for start in range(0, n, batch_size):
        batch_texts = texts[start:start + batch_size]
        enc = tokenizer(batch_texts, truncation=True, padding=True,
                        max_length=256, return_tensors="pt")
        enc = {k: v.to(device) for k, v in enc.items()}

        with torch.no_grad():
            probs = torch.softmax(model(**enc).logits, dim=-1).cpu().numpy()

        for i, (pid, _ptype, fname, _) in enumerate(records[start:start + batch_size]):
            row_probs = probs[i]
            top_idx = int(row_probs.argmax())
            div = id2label[top_idx]
            div_name = div_names.get(div, "")
            results.append((pid, fname, div, div_name))

        done = min(start + batch_size, n)
        elapsed = time.time() - t0
        speed = done / elapsed if elapsed > 0 else 0
        eta = (n - done) / speed if speed > 0 else 0
        if done % (batch_size * 20) == 0 or done == n:
            log.info(f"  {done:>7,}/{n:,}  {speed:.0f} files/s  ETA {eta/60:.1f}min")

    log.info(f"Inference done: {n:,} files in {(time.time()-t0)/60:.1f}min")
    return results


def update_results(conn: sqlite3.Connection, results: list[tuple]) -> None:
    conn.executemany(f"""
        UPDATE {TABLE}
        SET division = ?, division_name = ?
        WHERE project_id = ? AND file_name = ?
    """, [(div, div_name, pid, fname) for pid, fname, div, div_name in results])
    conn.commit()
    updated = conn.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE division IS NOT NULL"
    ).fetchone()[0]
    log.info(f"Updated {len(results):,} rows; {updated:,} rows now have division")


def show_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    with_div = conn.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE division IS NOT NULL"
    ).fetchone()[0]
    print(f"\n{'═'*72}")
    print(f"  {TABLE}  —  {total:,} total  /  {with_div:,} with division")
    print(f"{'═'*72}\n")
    if with_div == 0:
        print("No division classifications yet — run without --stats first.")
        return
    print("Top 15 divisions:")
    for div, name, n, pct in conn.execute(f"""
        SELECT division, division_name, COUNT(*),
               ROUND(COUNT(*)*100.0/{with_div}, 1)
        FROM {TABLE} WHERE division IS NOT NULL
        GROUP BY division ORDER BY COUNT(*) DESC LIMIT 15
    """):
        print(f"  {div}  {(name or '')[:50]:<50}  {n:>7,}  {pct:>5}%")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Add division-level classification to classifications_files."
    )
    ap.add_argument("--db",         default=DEFAULT_DB)
    ap.add_argument("--model",      default=MODEL_DIR)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--stats",      action="store_true")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    ensure_columns(conn)

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    div_names = load_division_names(conn)
    log.info(f"Loaded {len(div_names)} division names from existing classifications")

    project_rows = load_file_records(conn)
    if not project_rows:
        log.error("No projects with file snippets found")
        sys.exit(1)

    file_records = expand_to_files(project_rows)
    log.info(f"Total files to classify: {len(file_records):,}")

    results = run_inference(file_records, args.model, args.batch_size, div_names)
    update_results(conn, results)
    show_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
