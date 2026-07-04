#!/usr/bin/env python3
"""
Build labels_training table for TF-IDF + LR classifier training.

Phase 1 (always):
    Pull best LLM label per project from classifications_combined.
    Uses highest-confidence classification where a project has multiple rows.

Phase 2 (optional, requires --label-random N):
    Randomly sample N projects not yet in Phase 1, classify them via Ollama,
    and append to labels_training.

Output table: labels_training
    Columns: project_id, section, section_name, division, division_name,
             confidence, source, created_at

Usage:
    python3 create_training_labels.py --db 23692652-sq26.db
    python3 create_training_labels.py --db 23692652-sq26.db --balance 20
    python3 create_training_labels.py --db 23692652-sq26.db --label-random 500
    python3 create_training_labels.py --db 23692652-sq26.db --stats
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import re
import sqlite3
import sys
import time
from pathlib import Path

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_DB    = "23692652-sq26.db"
TABLE         = "labels_training"
OLLAMA_URL    = "http://localhost:11434/api/generate"
OLLAMA_MODEL  = "qwen2.5:7b"

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
    project_id    INTEGER NOT NULL UNIQUE,
    section       TEXT NOT NULL,
    section_name  TEXT,
    division      TEXT,
    division_name TEXT,
    confidence    TEXT,
    source        TEXT,
    created_at    TEXT DEFAULT (datetime('now'))
)"""


# ── Schema ─────────────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_SQL)
    conn.commit()


# ── Phase 1: pull existing LLM labels ─────────────────────────────────────────

_CONF_RANK = {"very_high": 4, "high": 3, "medium": 2, "low": 1}

def load_llm_labels(conn: sqlite3.Connection) -> list[dict]:
    """Return one (best-confidence) label per project from classifications_combined."""
    rows = conn.execute("""
        SELECT project_id, section, section_name, division, division_name, confidence
        FROM classifications_combined
        WHERE section IS NOT NULL
    """).fetchall()

    best: dict[int, dict] = {}
    for pid, sec, sname, div, dname, conf in rows:
        rank = _CONF_RANK.get(conf or "low", 1)
        if pid not in best or rank > best[pid]["rank"]:
            best[pid] = {
                "project_id":   pid,
                "section":      sec,
                "section_name": sname or ISIC_SECTIONS.get(sec, ""),
                "division":     div or "",
                "division_name": dname or "",
                "confidence":   conf or "low",
                "source":       "llm_existing",
                "rank":         rank,
            }

    labels = [{k: v for k, v in d.items() if k != "rank"} for d in best.values()]
    log.info(f"Phase 1: {len(labels):,} projects from classifications_combined")
    return labels


def balance_labels(labels: list[dict], max_pct: float) -> list[dict]:
    """
    Cap any over-represented section so no section exceeds max_pct % of total.
    Under-represented sections are kept in full; excess rows from over-represented
    sections are dropped via random sampling (seed fixed for reproducibility).
    """
    from collections import defaultdict
    import random as _random

    rng = _random.Random(42)

    by_section: dict[str, list[dict]] = defaultdict(list)
    for lbl in labels:
        by_section[lbl["section"]].append(lbl)

    # Calculate cap per section based on the non-capped total
    total = len(labels)
    cap = int(total * max_pct / 100)

    balanced: list[dict] = []
    dropped = 0
    for sec, rows in by_section.items():
        if len(rows) > cap:
            kept = rng.sample(rows, cap)
            dropped += len(rows) - cap
            log.info(f"  Balance: section {sec} capped {len(rows):,} → {cap:,} "
                     f"(dropped {len(rows)-cap:,})")
            balanced.extend(kept)
        else:
            balanced.extend(rows)

    log.info(f"Balance: {total:,} → {len(balanced):,} labels  (dropped {dropped:,} excess N rows)")
    return balanced


# ── Phase 2: Ollama labeling ───────────────────────────────────────────────────

def _build_prompt(title: str, description: str) -> str:
    section_list = "\n".join(f"  {k}: {v}" for k, v in ISIC_SECTIONS.items())
    desc_excerpt = (description or "")[:600].strip()
    return f"""You are an expert at ISIC (International Standard Industrial Classification of All Economic Activities, Rev. 5).

Classify this research dataset by its SUBJECT MATTER — what the data is ABOUT, not that it is scientific research.
Do NOT default to section N just because this is academic work. Only use N when the dataset is specifically about the science/technology industry itself.

Dataset:
  Title: {title}
  Description: {desc_excerpt}

ISIC Sections:
{section_list}

Instructions:
- Choose exactly ONE section letter (A–V) that best matches the topic of the data.
- Examples: ecology data → A; hospital records → R; election data → P; transport logs → H; school surveys → Q.
- Output ONLY valid JSON, nothing else.

Respond with: {{"section": "<letter>", "confidence": "<high|medium|low>"}}"""


def _call_ollama(prompt: str, model: str = OLLAMA_MODEL, timeout: int = 60) -> dict | None:
    if not HAS_REQUESTS:
        log.error("requests library not installed — pip install requests")
        return None
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={"model": model, "prompt": prompt, "stream": False,
                  "options": {"temperature": 0.1, "num_predict": 64}},
            timeout=timeout,
        )
        resp.raise_for_status()
        text = resp.json().get("response", "").strip()
        # Extract JSON from response
        m = re.search(r'\{[^}]+\}', text)
        if not m:
            return None
        data = json.loads(m.group())
        sec = data.get("section", "").strip().upper()
        if sec not in ISIC_SECTIONS:
            return None
        conf = data.get("confidence", "medium").lower()
        if conf not in {"high", "medium", "low"}:
            conf = "medium"
        return {"section": sec, "confidence": conf}
    except Exception as exc:
        log.debug(f"Ollama error: {exc}")
        return None


def label_random_sample(
    conn: sqlite3.Connection,
    n: int,
    existing_pids: set[int],
    model: str = OLLAMA_MODEL,
) -> list[dict]:
    """Sample N projects not yet labeled and classify via Ollama."""
    if not HAS_REQUESTS:
        log.error("requests not available — install with: pip install requests")
        return []

    # Check Ollama is running
    try:
        requests.get("http://localhost:11434/api/tags", timeout=5)
    except Exception:
        log.error("Ollama not reachable at localhost:11434 — start it with: ollama serve")
        return []

    # Pull candidate projects
    placeholders = ",".join("?" * len(existing_pids)) if existing_pids else "0"
    candidates = conn.execute(f"""
        SELECT id, title, description
        FROM projects
        WHERE id NOT IN ({placeholders})
        ORDER BY RANDOM()
        LIMIT ?
    """, (*sorted(existing_pids), n * 3)).fetchall()  # fetch 3× to have buffer for failures

    log.info(f"Phase 2: labeling {n} random projects via Ollama ({model})…")
    log.info(f"  Estimated time: {n * 0.5:.0f}–{n * 1.5:.0f}s depending on model speed")

    results: list[dict] = []
    t0 = time.time()

    for i, (pid, title, desc) in enumerate(candidates):
        if len(results) >= n:
            break
        prompt = _build_prompt(title or "", desc or "")
        parsed = _call_ollama(prompt, model=model)
        if parsed is None:
            log.debug(f"  [{i+1}] project {pid}: no valid response, skipping")
            continue
        sec = parsed["section"]
        results.append({
            "project_id":   pid,
            "section":      sec,
            "section_name": ISIC_SECTIONS[sec],
            "division":     "",
            "division_name": "",
            "confidence":   parsed["confidence"],
            "source":       "llm_random",
        })
        elapsed = time.time() - t0
        rate = (i + 1) / elapsed if elapsed > 0 else 0
        eta = (n - len(results)) / rate if rate > 0 else 0
        log.info(
            f"  [{len(results):>3}/{n}] {title[:55]:55s} → {sec}  "
            f"[{parsed['confidence']}]  ETA {eta:.0f}s"
        )

    log.info(
        f"Phase 2: labeled {len(results):,} projects in {time.time()-t0:.1f}s "
        f"({(time.time()-t0)/max(len(results),1):.1f}s/proj)"
    )
    return results


# ── Save labels ────────────────────────────────────────────────────────────────

def save_labels(conn: sqlite3.Connection, labels: list[dict]) -> None:
    conn.execute(f"DELETE FROM {TABLE}")
    conn.executemany(f"""
        INSERT OR REPLACE INTO {TABLE}
            (project_id, section, section_name, division, division_name, confidence, source)
        VALUES (:project_id, :section, :section_name, :division, :division_name,
                :confidence, :source)
    """, labels)
    conn.commit()
    log.info(f"Saved {len(labels):,} labels to {TABLE}")


# ── Statistics ─────────────────────────────────────────────────────────────────

def show_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if total == 0:
        print("labels_training is empty — run without --stats first.")
        return

    print(f"\n{'═'*70}")
    print(f"  labels_training  —  {total:,} labeled examples")
    print(f"{'═'*70}\n")

    print("Source breakdown:")
    for src, n, pct in conn.execute(f"""
        SELECT source, COUNT(*) as n, ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY source ORDER BY n DESC
    """):
        bar = "█" * int(pct / 2)
        print(f"  {src:<20} {n:>6,}  {pct:>5}%  {bar}")

    print("\nSection distribution:")
    print(f"  {'Sec':<4} {'Name':<52} {'N':>6}  {'%':>5}")
    print(f"  {'─'*68}")
    for sec, name, n, pct in conn.execute(f"""
        SELECT section, section_name, COUNT(*) as n,
               ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY section ORDER BY n DESC
    """):
        bar = "█" * max(1, int(pct / 2))
        print(f"  {sec:<4} {(name or '')[:52]:<52} {n:>6,}  {pct:>5}%  {bar}")

    print("\nConfidence distribution:")
    order = "CASE confidence WHEN 'very_high' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
    for conf, n, pct in conn.execute(f"""
        SELECT confidence, COUNT(*), ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY confidence ORDER BY {order}
    """):
        print(f"  {conf:<12} {n:>6,}  {pct:>5}%")

    # Show sections with very few examples
    sparse = conn.execute(f"""
        SELECT section, section_name, COUNT(*) as n
        FROM {TABLE} GROUP BY section HAVING n < 20 ORDER BY n
    """).fetchall()
    if sparse:
        print("\nWarning: sections with < 20 training examples (may underperform):")
        for sec, name, n in sparse:
            print(f"  {sec}: {name}  → {n} examples")

    print()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build labels_training table from LLM-classified records."
    )
    ap.add_argument("--db",           default=DEFAULT_DB)
    ap.add_argument("--label-random", type=int, default=0, metavar="N",
                    help="Classify N additional random projects via Ollama")
    ap.add_argument("--model",        default=OLLAMA_MODEL,
                    help=f"Ollama model name (default: {OLLAMA_MODEL})")
    ap.add_argument("--balance",      type=float, default=None, metavar="MAX_PCT",
                    help="Cap any single section to at most MAX_PCT%% of total labels "
                         "(e.g. --balance 20 limits section N to 20%%)")
    ap.add_argument("--stats",        action="store_true",
                    help="Show statistics from existing table (no rebuild)")
    args = ap.parse_args()

    model = args.model

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}"); sys.exit(1)

    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    # Phase 1: existing LLM labels
    labels = load_llm_labels(conn)
    existing_pids = {d["project_id"] for d in labels}

    # Phase 2: optional Ollama labeling
    if args.label_random > 0:
        extra = label_random_sample(conn, args.label_random, existing_pids, model=model)
        labels.extend(extra)
        log.info(f"Total labels after Phase 2: {len(labels):,}")

    # Optional: cap over-represented sections
    if args.balance is not None:
        labels = balance_labels(labels, max_pct=args.balance)

    save_labels(conn, labels)
    show_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
