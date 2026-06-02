#!/usr/bin/env python3
"""
ensemble_writeback.py
─────────────────────
Reads the `classifications` table, computes a majority-vote ensemble
across mistral + llama3.1:8b + keyword for every classified project,
and writes the result back into `projects.isic_*` columns.

Also computes an `agreement_score` (0–3) reflecting how many models agreed.

Usage:
    python3 ensemble_writeback.py
    python3 ensemble_writeback.py --db 23692652-sq26.db --dry-run
"""

import argparse
import sqlite3
from collections import Counter
from pathlib import Path

DEFAULT_DB = "23692652-sq26.db"
VOTE_MODELS = ["mistral", "llama3.1:8b", "keyword"]


def ensure_columns(conn: sqlite3.Connection) -> None:
    """Add any missing columns to projects table."""
    existing = {r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()}
    additions = {
        "isic_section":       "TEXT",
        "isic_section_name":  "TEXT",
        "isic_division":      "TEXT",
        "isic_division_name": "TEXT",
        "isic_confidence":    "TEXT",
        "isic_method":        "TEXT",
        "isic_agreement":     "INTEGER",
    }
    for col, dtype in additions.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col} {dtype}")
            print(f"  Added column: {col}")
    conn.commit()


def compute_ensemble(rows: list[dict]) -> dict | None:
    """
    Given rows from classifications for one project (multiple models),
    return the ensemble result dict or None if no data.

    Voting logic:
      - Count t1_section votes from VOTE_MODELS only
      - Winning section = plurality
      - Winning division = most common division among rows that voted for winning section
      - Agreement = number of VOTE_MODELS that voted for the winning section
      - Confidence = high (≥2 agree) | low (1 agrees)
    """
    if not rows:
        return None

    # Only use VOTE_MODELS for voting
    vote_rows = [r for r in rows if r["model_name"] in VOTE_MODELS]
    if not vote_rows:
        # Fall back to whatever models are available
        vote_rows = rows

    # Count section votes
    section_votes = [r["t1_section"] for r in vote_rows if r["t1_section"]]
    if not section_votes:
        return None

    section_counter = Counter(section_votes)
    winning_section, winning_count = section_counter.most_common(1)[0]

    # Among rows that voted for the winning section, pick most common division
    winning_rows = [r for r in vote_rows if r["t1_section"] == winning_section]
    div_counter = Counter(r["t1_division"] for r in winning_rows if r["t1_division"])
    if not div_counter:
        return None
    winning_division = div_counter.most_common(1)[0][0]

    # Get names from the first row that has both
    section_name = next(
        (r["t1_section_name"] for r in winning_rows if r.get("t1_section_name")), ""
    )
    division_name = next(
        (r["t1_division_name"] for r in winning_rows
         if r.get("t1_division_name") and r["t1_division"] == winning_division), ""
    )

    agreement = winning_count
    n_models = len(VOTE_MODELS)
    confidence = "high" if agreement >= 2 else "low"

    return {
        "isic_section":       winning_section,
        "isic_section_name":  section_name,
        "isic_division":      winning_division,
        "isic_division_name": division_name,
        "isic_confidence":    confidence,
        "isic_method":        f"ensemble({','.join(VOTE_MODELS)})",
        "isic_agreement":     agreement,
    }


def run(db_path: str, dry_run: bool = False) -> None:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row

    if not dry_run:
        ensure_columns(conn)

    # Fetch all classifications
    rows = conn.execute("""
        SELECT project_id, model_name,
               t1_section, t1_section_name, t1_division, t1_division_name,
               t1_confidence
        FROM classifications
        ORDER BY project_id
    """).fetchall()

    # Group by project
    by_project: dict[int, list[dict]] = {}
    for r in rows:
        pid = r["project_id"]
        by_project.setdefault(pid, []).append(dict(r))

    print(f"Projects with classifications: {len(by_project)}")
    print(f"Vote models: {VOTE_MODELS}\n")

    updated = 0
    skipped = 0
    agreement_dist: Counter = Counter()

    for pid, proj_rows in by_project.items():
        result = compute_ensemble(proj_rows)
        if not result:
            skipped += 1
            continue

        agreement_dist[result["isic_agreement"]] += 1

        if dry_run:
            print(f"[{pid}] {result['isic_section']}/{result['isic_division']} "
                  f"— {result['isic_division_name'][:50]} "
                  f"[agree={result['isic_agreement']}/{len(VOTE_MODELS)}, "
                  f"conf={result['isic_confidence']}]")
        else:
            conn.execute("""
                UPDATE projects SET
                    isic_section       = :isic_section,
                    isic_section_name  = :isic_section_name,
                    isic_division      = :isic_division,
                    isic_division_name = :isic_division_name,
                    isic_confidence    = :isic_confidence,
                    isic_method        = :isic_method,
                    isic_agreement     = :isic_agreement
                WHERE id = :pid
            """, {**result, "pid": pid})
            updated += 1

    if not dry_run:
        conn.commit()
        print(f"Updated {updated} projects  (skipped {skipped} with no data)")
    else:
        print(f"\nWould update {len(by_project) - skipped} projects")

    print("\nAgreement distribution:")
    for ag in sorted(agreement_dist):
        print(f"  {ag}/{len(VOTE_MODELS)} models agree: {agreement_dist[ag]} projects")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.db, dry_run=args.dry_run)
