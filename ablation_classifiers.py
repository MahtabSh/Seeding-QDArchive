#!/usr/bin/env python3
"""
Ablation study: measure the contribution of each classifier.

Simulates the voting ensemble for progressive subsets of classifiers:
  Round 1 : [ensemble]
  Round 2 : [ensemble, bert]
  Round 3 : [ensemble, bert, bert_div]
  Round 4 : [ensemble, bert, bert_div, tfidf]
  Round 5 : [ensemble, bert, bert_div, tfidf, embed]
  Round 6 : [ensemble, bert, bert_div, tfidf, embed, nli]
  Round 7 : [ensemble, bert, bert_div, tfidf, embed, nli, distilbert]
  Round 8 : [ensemble, bert, bert_div, tfidf, embed, nli, distilbert, st]  ← all

Metrics per round:
  • Confidence distribution (very_high / high / medium / low)
  • # projects that CHANGED section vs previous round  (stability delta)
  • # projects that CHANGED division vs previous round
  • Coverage of division field

Output: printed table + ablation_results.csv

Usage:
    python3 ablation_classifiers.py --db 23692652-sq26.db
    python3 ablation_classifiers.py --db 23692652-sq26.db --csv ablation_results.csv
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import Counter
from pathlib import Path

DEFAULT_DB = "23692652-sq26.db"

# Full priority order (same as classify_vote.py)
ALL_PRIORITY = ["ensemble", "bert_base", "bert_div", "tfidf"]

SOURCES = {
    "ensemble":  "classifications_combined",
    "bert_base": "classifications_bert_base",
    "bert_div":  "classifications_bert_division",
    "tfidf":     "classifications_tfidf",
}

# Tables that filter on tier='metadata'
TIER_FILTER = {"tfidf", "bert_base", "bert_div"}


# ── Load all votes once ────────────────────────────────────────────────────────

def load_all_votes(conn: sqlite3.Connection) -> dict[int, dict[str, dict]]:
    existing = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    votes: dict[int, dict[str, dict]] = {}

    for source, tbl in SOURCES.items():
        if tbl not in existing:
            continue
        tbl_cols = {r[1] for r in conn.execute(f"PRAGMA table_info({tbl})")}
        div_sel      = "division"      if "division"      in tbl_cols else "'' AS division"
        div_name_sel = "division_name" if "division_name" in tbl_cols else "'' AS division_name"
        tier_clause  = "AND tier = 'metadata'" if source in TIER_FILTER else ""

        rows = conn.execute(f"""
            SELECT project_id, section, section_name, {div_sel}, {div_name_sel}
            FROM {tbl}
            WHERE section IS NOT NULL {tier_clause}
        """).fetchall()

        for pid, sec, sec_name, div, div_name in rows:
            votes.setdefault(pid, {})[source] = {
                "section":       sec or "",
                "section_name":  sec_name  or "",
                "division":      div       or "",
                "division_name": div_name  or "",
            }

    return votes


# ── Resolve vote for a given subset ───────────────────────────────────────────

def resolve_vote(project_votes: dict[str, dict], priority: list[str]) -> dict:
    active = {s: v for s, v in project_votes.items() if s in priority}
    if not active:
        return {"section": "", "section_name": "", "division": "",
                "division_name": "", "confidence": "low", "vote_count": 0,
                "total_voters": 0}

    section_counts: Counter = Counter(v["section"] for v in active.values())
    max_votes = section_counts.most_common(1)[0][1]
    top_sections = [s for s, n in section_counts.items() if n == max_votes]

    winning_section = top_sections[0]
    if len(top_sections) > 1:
        for src in priority:
            if src in active and active[src]["section"] in top_sections:
                winning_section = active[src]["section"]
                break

    total = len(active)
    confidence = (
        "very_high" if max_votes >= 4 else
        "high"      if max_votes == 3 else
        "medium"    if max_votes == 2 else
        "low"
    )

    section_name = division = division_name = ""
    for src in priority:
        if src in active and active[src]["section"] == winning_section:
            if not section_name:
                section_name = active[src].get("section_name", "")
            d = active[src].get("division", "")
            if d and not division:
                division      = d
                division_name = active[src].get("division_name", "")
            if section_name and division:
                break

    return {
        "section":       winning_section,
        "section_name":  section_name,
        "division":      division,
        "division_name": division_name,
        "confidence":    confidence,
        "vote_count":    max_votes,
        "total_voters":  total,
    }


# ── Run one round ─────────────────────────────────────────────────────────────

def run_round(
    all_votes: dict[int, dict[str, dict]],
    priority: list[str],
) -> dict[int, dict]:
    results = {}
    for pid, pvotes in all_votes.items():
        results[pid] = resolve_vote(pvotes, priority)
    return results


# ── Compare two rounds ────────────────────────────────────────────────────────

def compare_rounds(prev: dict[int, dict], curr: dict[int, dict]) -> dict:
    pids = set(prev) | set(curr)
    total = len(pids)

    sec_changed = 0
    div_changed = 0
    conf_upgrade = 0
    conf_downgrade = 0
    conf_order = {"very_high": 3, "high": 2, "medium": 1, "low": 0}

    for pid in pids:
        p = prev.get(pid, {})
        c = curr.get(pid, {})
        if p.get("section") != c.get("section"):
            sec_changed += 1
        if p.get("division") != c.get("division"):
            div_changed += 1
        po = conf_order.get(p.get("confidence", "low"), 0)
        co = conf_order.get(c.get("confidence", "low"), 0)
        if co > po:
            conf_upgrade += 1
        elif co < po:
            conf_downgrade += 1

    return {
        "total": total,
        "sec_changed": sec_changed,
        "sec_changed_pct": sec_changed / total * 100,
        "div_changed": div_changed,
        "div_changed_pct": div_changed / total * 100,
        "conf_upgrade": conf_upgrade,
        "conf_downgrade": conf_downgrade,
    }


# ── Summary for one round ──────────────────────────────────────────────────────

def summarize(results: dict[int, dict]) -> dict:
    total = len(results)
    conf = Counter(r["confidence"] for r in results.values())
    div_coverage = sum(1 for r in results.values() if r.get("division"))
    return {
        "total":        total,
        "very_high":    conf["very_high"],
        "high":         conf["high"],
        "medium":       conf["medium"],
        "low":          conf["low"],
        "vh_pct":       conf["very_high"] / total * 100,
        "h_pct":        conf["high"]      / total * 100,
        "m_pct":        conf["medium"]    / total * 100,
        "l_pct":        conf["low"]       / total * 100,
        "div_coverage": div_coverage,
        "div_cov_pct":  div_coverage / total * 100,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db",  default=DEFAULT_DB)
    ap.add_argument("--csv", default="ablation_results.csv",
                    help="Output CSV path (default: ablation_results.csv)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    print("Loading all classifier votes…")
    all_votes = load_all_votes(conn)
    conn.close()
    print(f"Loaded {len(all_votes):,} projects\n")

    # Check which sources actually have data
    available = set()
    for pvotes in all_votes.values():
        available |= set(pvotes.keys())
    active_priority = [s for s in ALL_PRIORITY if s in available]
    print(f"Available classifiers: {', '.join(active_priority)}\n")

    # Build progressive rounds
    rounds = []
    for i in range(1, len(active_priority) + 1):
        rounds.append(active_priority[:i])

    # Run and collect stats
    round_results = []
    prev_result = None

    W = 115
    print("═" * W)
    print(f"  {'Round':<6} {'Classifiers':<42} {'VeryHigh':>9} {'High':>8} {'Medium':>8} {'Low':>6} "
          f"{'DivCov':>7} {'ΔSection':>9} {'ΔDivision':>10} {'ConfUp':>7}")
    print("─" * W)

    csv_rows = []

    for i, priority in enumerate(rounds, 1):
        label = "+".join(priority)
        result = run_round(all_votes, priority)
        stats  = summarize(result)

        if prev_result is not None:
            delta = compare_rounds(prev_result, result)
        else:
            delta = {"sec_changed": 0, "sec_changed_pct": 0.0,
                     "div_changed": 0, "div_changed_pct": 0.0,
                     "conf_upgrade": 0}

        added = priority[-1]
        classifiers_str = label[:42]

        print(
            f"  R{i:<5} {classifiers_str:<42} "
            f"{stats['vh_pct']:>8.1f}% {stats['h_pct']:>7.1f}% "
            f"{stats['m_pct']:>7.1f}% {stats['l_pct']:>5.1f}% "
            f"{stats['div_cov_pct']:>6.1f}% "
            f"{delta['sec_changed']:>6,} ({delta['sec_changed_pct']:>4.1f}%) "
            f"{delta['div_changed']:>6,} ({delta['div_changed_pct']:>4.1f}%) "
            f"{delta['conf_upgrade']:>7,}"
        )

        csv_rows.append({
            "round":        i,
            "classifiers":  label,
            "added":        added,
            "very_high_pct": round(stats["vh_pct"], 2),
            "high_pct":     round(stats["h_pct"], 2),
            "medium_pct":   round(stats["m_pct"], 2),
            "low_pct":      round(stats["l_pct"], 2),
            "div_coverage_pct": round(stats["div_cov_pct"], 2),
            "section_changed":  delta["sec_changed"],
            "section_changed_pct": round(delta["sec_changed_pct"], 2),
            "division_changed":    delta["div_changed"],
            "division_changed_pct": round(delta["div_changed_pct"], 2),
            "conf_upgrades": delta["conf_upgrade"],
        })

        prev_result = result

    print("═" * W)

    # Verdict per classifier
    print("\nVerdict — did adding each classifier help?")
    print(f"  {'Classifier':<14} {'ΔSection%':>10} {'ΔDivision%':>11} {'ConfUpgrades':>13}  Verdict")
    print("  " + "─" * 60)
    for row in csv_rows[1:]:  # skip round 1 (baseline)
        sec_d  = row["section_changed_pct"]
        div_d  = row["division_changed_pct"]
        upg    = row["conf_upgrades"]
        if sec_d < 0.5 and upg < 200:
            verdict = "MARGINAL — consider dropping"
        elif sec_d < 1.5 and upg < 500:
            verdict = "Minor contribution"
        elif sec_d < 3.0 or upg >= 500:
            verdict = "Moderate contribution"
        else:
            verdict = "Strong contribution"
        print(f"  {row['added']:<14} {sec_d:>9.1f}%  {div_d:>9.1f}%  {upg:>12,}  {verdict}")

    # Save CSV
    csv_path = Path(args.csv)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
        writer.writeheader()
        writer.writerows(csv_rows)
    print(f"\nSaved → {csv_path}")


if __name__ == "__main__":
    main()
