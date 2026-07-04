#!/usr/bin/env python3
"""
Voting ensemble for QDArchive.

Combines all available classifier results into a single majority-vote
classification per project.  Sources used (in tiebreak priority order):

    1. ensemble    — classifications_combined        (qwen2.5 LLM, most accurate)
    2. bert_base   — classifications_bert_base       (bert-base-uncased, LLM labels, section only)
    3. bert_div    — classifications_bert_division   (DistilBERT, PDF descriptions, section+division)
    4. tfidf       — classifications_tfidf           (TF-IDF+LR, LLM labels, section only)

    Removed classifiers (ablation study):
      - nli:        0.2% section change — negligible contribution
      - bert:       DistilBERT section-only, replaced by stronger bert-base-uncased
      - distilbert: old fine-tune covering only 7 sections
      - embed:      31.8% agreement with LLM — weakest classifier, zero-shot cosine similarity
      - st:         41.8% agreement with LLM — weak, zero-shot cosine similarity

Confidence levels:
    very_high  — 4+ classifiers agree
    high       — 3 agree
    medium     — 2 agree
    low        — all disagree (single-source fallback)

Results → table: classifications_vote  (never touches other tables)

Usage:
    python3 classify_vote.py              # build vote table + print stats
    python3 classify_vote.py --stats      # stats only (table already built)
    python3 classify_vote.py --dry-run    # preview 20 projects, no writes
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_DB = "23692652-sq26.db"
TABLE      = "classifications_vote"

# Tiebreak priority: index 0 wins ties
PRIORITY = ["ensemble", "bert_base", "bert_div", "tfidf"]

SOURCES = {
    "ensemble":  "classifications_combined",
    "bert_base": "classifications_bert_base",
    "bert_div":  "classifications_bert_division",
    "tfidf":     "classifications_tfidf",
}


# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_SQL = f"""CREATE TABLE {TABLE} (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id    INTEGER NOT NULL UNIQUE,
    section       TEXT,
    section_name  TEXT,
    division      TEXT,
    division_name TEXT,
    confidence    TEXT,
    vote_count    INTEGER,
    total_voters  INTEGER,
    voters        TEXT,
    classified_at TEXT DEFAULT (datetime('now'))
)"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)
    ).fetchone():
        conn.execute(_CREATE_SQL)
        conn.commit()


# ── Vote loading ───────────────────────────────────────────────────────────────

def load_votes(conn: sqlite3.Connection) -> dict[int, dict[str, dict]]:
    """
    Returns {project_id: {source_name: {section, section_name, division, division_name}}}
    """
    existing_tables = {
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }

    votes: dict[int, dict[str, dict]] = {}

    for source, tbl in SOURCES.items():
        if tbl not in existing_tables:
            log.info(f"  {source:12s}: not found — skipping")
            continue

        # For NLI, ST, TFIDF, BERT, EMBED prefer/require metadata tier
        tier_filter = "AND tier = 'metadata'" if source in ("tfidf", "bert_base", "bert_div") else ""

        # Some tables don't have division columns — use '' as fallback
        tbl_cols = {r[1] for r in conn.execute(f"PRAGMA table_info({tbl})")}
        div_sel      = "division"      if "division"      in tbl_cols else "'' AS division"
        div_name_sel = "division_name" if "division_name" in tbl_cols else "'' AS division_name"

        # For the LLM ensemble, pick the most reliable model per project:
        # priority: qwen2.5:7b (has divisions) > mistral (has divisions) > claude-haiku (section only)
        if source == "ensemble" and "model_name" in tbl_cols:
            rows = conn.execute(f"""
                SELECT project_id, section, section_name, {div_sel}, {div_name_sel}
                FROM {tbl} t1
                WHERE section IS NOT NULL
                  AND model_name = (
                      SELECT model_name FROM {tbl} t2
                      WHERE t2.project_id = t1.project_id
                        AND t2.section IS NOT NULL
                      ORDER BY CASE model_name
                          WHEN 'qwen2.5:7b'       THEN 1
                          WHEN 'mistral'           THEN 2
                          WHEN 'claude-haiku-4-5'  THEN 3
                          ELSE 4 END
                      LIMIT 1
                  )
                GROUP BY project_id
            """).fetchall()
        else:
            rows = conn.execute(f"""
                SELECT project_id, section, section_name, {div_sel}, {div_name_sel}
                FROM {tbl}
                WHERE section IS NOT NULL {tier_filter}
            """).fetchall()

        for pid, sec, sec_name, div, div_name in rows:
            votes.setdefault(pid, {})[source] = {
                "section":       sec,
                "section_name":  sec_name  or "",
                "division":      div       or "",
                "division_name": div_name  or "",
            }
        log.info(f"  {source:12s}: {len(rows):>6,} votes")

    return votes


# ── Vote resolution ────────────────────────────────────────────────────────────

def resolve_vote(project_votes: dict[str, dict]) -> dict:
    """
    Majority vote with priority-order tiebreaking.
    Returns a result dict ready for DB insertion.
    """
    section_counts: Counter = Counter(v["section"] for v in project_votes.values())
    max_votes = section_counts.most_common(1)[0][1]

    # Sections tied at the top
    top_sections = [s for s, n in section_counts.items() if n == max_votes]

    # Break ties using PRIORITY order
    winning_section = top_sections[0]   # fallback
    if len(top_sections) > 1:
        for src in PRIORITY:
            if src in project_votes and project_votes[src]["section"] in top_sections:
                winning_section = project_votes[src]["section"]
                break

    # Confidence from agreement count
    total = len(project_votes)
    confidence = (
        "very_high" if max_votes >= 4 else
        "high"      if max_votes == 3 else
        "medium"    if max_votes == 2 else
        "low"
    )

    # Division + names: take section_name from highest-priority agreeing source,
    # then independently find the best division from any agreeing source
    # (bert/tfidf have no division column, so we fall through to embed/distilbert/st
    # which have 100% division coverage).
    section_name = division = division_name = ""
    for src in PRIORITY:
        if src in project_votes and project_votes[src]["section"] == winning_section:
            if not section_name:
                section_name = project_votes[src].get("section_name", "")
            d = project_votes[src].get("division", "")
            if d and not division:
                division      = d
                division_name = project_votes[src].get("division_name", "")
            if section_name and division:
                break

    voters = [s for s in PRIORITY
              if s in project_votes and project_votes[s]["section"] == winning_section]

    return {
        "section":       winning_section,
        "section_name":  section_name,
        "division":      division,
        "division_name": division_name,
        "confidence":    confidence,
        "vote_count":    max_votes,
        "total_voters":  total,
        "voters":        ",".join(voters),
    }


# ── Build table ────────────────────────────────────────────────────────────────

def build_vote_table(conn: sqlite3.Connection) -> None:
    log.info("Loading votes from all classifier tables…")
    all_votes = load_votes(conn)
    log.info(f"Total projects with at least one vote: {len(all_votes):,}")

    log.info("Resolving votes…")
    t0 = time.time()

    rows = []
    for pid, pvotes in all_votes.items():
        r = resolve_vote(pvotes)
        rows.append((
            pid,
            r["section"],  r["section_name"],
            r["division"], r["division_name"],
            r["confidence"],
            r["vote_count"], r["total_voters"],
            r["voters"],
        ))

    conn.execute(f"DELETE FROM {TABLE}")
    conn.executemany(f"""
        INSERT INTO {TABLE}
            (project_id, section, section_name, division, division_name,
             confidence, vote_count, total_voters, voters)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

    log.info(f"Saved {len(rows):,} rows to {TABLE} in {time.time()-t0:.1f}s")


# ── Dry-run preview ────────────────────────────────────────────────────────────

def dry_run_preview(conn: sqlite3.Connection, n: int = 20) -> None:
    all_votes = load_votes(conn)
    sample    = list(all_votes.items())[:n]
    pids      = [pid for pid, _ in sample]

    titles = {r[0]: r[1] for r in conn.execute(
        f"SELECT id, title FROM projects WHERE id IN ({','.join('?'*len(pids))})", pids
    )}

    print(f"\n{'═'*72}")
    print(f"  DRY-RUN — {n} sample projects  (no writes)")
    print(f"{'═'*72}\n")

    for pid, pvotes in sample:
        r = resolve_vote(pvotes)
        vote_str = "  ".join(
            f"{s}={pvotes[s]['section']}" for s in PRIORITY if s in pvotes
        )
        print(f"[{pid}] {(titles.get(pid) or '')[:65]}")
        print(f"  votes : {vote_str}")
        print(f"  result: {r['section']}/{r['division']}  "
              f"[{r['confidence']}  {r['vote_count']}/{r['total_voters']} agree  "
              f"from: {r['voters']}]")
        print()


# ── Stats ──────────────────────────────────────────────────────────────────────

def show_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if total == 0:
        print("Table is empty — run without --stats first.")
        return

    print(f"\n{'═'*68}")
    print(f"  classifications_vote  —  {total:,} projects")
    print(f"{'═'*68}\n")

    # Confidence
    print("Confidence distribution:")
    order = "CASE confidence WHEN 'very_high' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
    for conf, n, pct in conn.execute(f"""
        SELECT confidence, COUNT(*) as n,
               ROUND(COUNT(*)*100.0/{total}, 1) as pct
        FROM {TABLE} GROUP BY confidence ORDER BY {order}
    """):
        bar = "█" * int(pct / 2)
        print(f"  {conf:<12} {n:>7,}  {pct:>5}%  {bar}")

    # Section
    print("\nSection distribution:")
    print(f"  {'Sec':<5} {'Name':<46} {'N':>7}  {'%':>5}")
    print(f"  {'─'*66}")
    for sec, name, n, pct in conn.execute(f"""
        SELECT section, section_name, COUNT(*) as n,
               ROUND(COUNT(*)*100.0/{total}, 1) as pct
        FROM {TABLE} GROUP BY section ORDER BY n DESC
    """):
        print(f"  {sec:<5} {(name or '')[:46]:<46} {n:>7,}  {pct:>5}%")

    # Vote breakdown
    print("\nVote agreement breakdown:")
    print(f"  {'Agreement':<16} {'Projects':>9}  {'%':>5}")
    print(f"  {'─'*35}")
    for vc, tv, n, pct in conn.execute(f"""
        SELECT vote_count, total_voters, COUNT(*) as n,
               ROUND(COUNT(*)*100.0/{total}, 1) as pct
        FROM {TABLE}
        GROUP BY vote_count, total_voters
        ORDER BY total_voters DESC, vote_count DESC
    """):
        label = f"{vc}/{tv} classifiers"
        print(f"  {label:<16} {n:>9,}  {pct:>5}%")

    # Voter combinations for high-confidence results
    print("\nTop voter combinations (≥medium confidence):")
    for voters, n in conn.execute(f"""
        SELECT voters, COUNT(*) as n
        FROM {TABLE}
        WHERE confidence IN ('very_high','high','medium')
        GROUP BY voters ORDER BY n DESC LIMIT 10
    """):
        print(f"  {n:>7,}  {voters}")
    print()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build voting ensemble from all classifier tables."
    )
    ap.add_argument("--db",      default=DEFAULT_DB)
    ap.add_argument("--stats",   action="store_true",
                    help="Show statistics from existing vote table (no rebuild)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Preview 20 results without writing to DB")
    ap.add_argument("--n",       type=int, default=20,
                    help="Number of projects to preview in --dry-run (default 20)")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}"); sys.exit(1)

    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    if args.dry_run:
        dry_run_preview(conn, n=args.n)
    elif args.stats:
        show_stats(conn)
    else:
        build_vote_table(conn)
        show_stats(conn)

    conn.close()


if __name__ == "__main__":
    main()
