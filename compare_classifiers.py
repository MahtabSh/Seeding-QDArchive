#!/usr/bin/env python3
"""
Classifier Comparison Report
=============================
Reads the `classifier_results` table (populated by classify_isic.py --classifier all)
and computes side-by-side statistics for every classifier.

Metrics:
  1. Agreement rate   — % of items where ALL classifiers agree (section / division)
  2. Section distribution  — count per ISIC section per classifier
  3. Division top-10  — most assigned divisions per classifier
  4. Confidence distribution — high / medium / low breakdown per classifier
  5. Disagreement cases  — items where classifiers differ (sample + cross-tab)
  6. PDF export       — charts for all metrics

Usage:
  python compare_classifiers.py [--db PATH] [--target project|file]
                                [--project-type QDA_PROJECT|QD_PROJECT|all]
                                [--repository-id N] [--no-pdf]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

DEFAULT_DB = "metadata.sqlite"


# ── Data loading ──────────────────────────────────────────────────────────────

def load_results(
    db_path: Path,
    target_type: str = "project",
    project_type: str | None = None,
    repository_id: int | None = None,
) -> dict[str, list[dict]]:
    """Return {classifier_name: [result_row, ...]} for all classifiers."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    conds = [f"target_type = '{target_type}'"]
    if project_type and project_type != "all":
        conds.append(f"project_type = '{project_type}'")
    if repository_id is not None:
        conds.append(f"repository_id = {repository_id}")
    where = "WHERE " + " AND ".join(conds)

    rows = conn.execute(
        f"SELECT * FROM classifier_results {where} ORDER BY classifier, target_id"
    ).fetchall()
    conn.close()

    results: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        results[row["classifier"]].append(dict(row))
    return dict(results)


def pivot_by_id(results: dict[str, list[dict]]) -> list[dict]:
    """
    Join all classifiers by target_id.
    Returns list of {target_id, {clf}_section, {clf}_division, {clf}_confidence, ...}
    for items present in ALL classifiers.
    """
    if not results:
        return []
    clf_names = list(results.keys())

    # Index each classifier by target_id
    indexed = {
        name: {r["target_id"]: r for r in rows}
        for name, rows in results.items()
    }

    # Only keep IDs present in all classifiers
    common_ids = set(indexed[clf_names[0]].keys())
    for name in clf_names[1:]:
        common_ids &= set(indexed[name].keys())

    rows = []
    for tid in sorted(common_ids):
        row: dict = {"target_id": tid}
        for name in clf_names:
            r = indexed[name][tid]
            row[f"{name}_section"]    = r["section"]
            row[f"{name}_division"]   = r["division"]
            row[f"{name}_confidence"] = r["confidence"]
        rows.append(row)
    return rows


# ── Statistics ────────────────────────────────────────────────────────────────

def agreement_rate(pivoted: list[dict], clf_names: list[str], level: str = "section") -> float:
    if not pivoted:
        return 0.0
    agree = 0
    for row in pivoted:
        vals = [row.get(f"{n}_{level}") for n in clf_names]
        if len(set(vals)) == 1:
            agree += 1
    return agree / len(pivoted) * 100


def section_distribution(results: dict[str, list[dict]]) -> dict[str, dict[str, int]]:
    """Return {clf: {section: count}} for all classifiers."""
    dist: dict[str, dict[str, int]] = {}
    for clf, rows in results.items():
        counts: dict[str, int] = defaultdict(int)
        for r in rows:
            counts[r["section"] or "?"] += 1
        dist[clf] = dict(counts)
    return dist


def division_topN(results: dict[str, list[dict]], n: int = 10) -> dict[str, list[tuple]]:
    """Return {clf: [(division, name, count), ...]} top-N."""
    top: dict[str, list[tuple]] = {}
    for clf, rows in results.items():
        counts: dict[str, list] = defaultdict(lambda: [0, ""])
        for r in rows:
            d = r["division"] or "?"
            counts[d][0] += 1
            counts[d][1] = r["division_name"] or ""
        ranked = sorted(counts.items(), key=lambda x: x[1][0], reverse=True)[:n]
        top[clf] = [(div, info[1], info[0]) for div, info in ranked]
    return top


def confidence_distribution(results: dict[str, list[dict]]) -> dict[str, dict[str, int]]:
    dist: dict[str, dict[str, int]] = {}
    for clf, rows in results.items():
        counts: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
        for r in rows:
            conf = r.get("confidence") or "low"
            counts[conf] = counts.get(conf, 0) + 1
        dist[clf] = counts
    return dist


def disagreement_cases(
    pivoted: list[dict],
    clf_names: list[str],
    db_path: Path,
    n: int = 15,
) -> list[dict]:
    """Return up to n items where classifiers disagree, enriched with project title."""
    differ = [
        row for row in pivoted
        if len({row.get(f"{c}_section") for c in clf_names}) > 1
    ][:n]

    if not differ:
        return []

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    ids = [r["target_id"] for r in differ]
    placeholders = ",".join("?" * len(ids))
    titles = {
        row["id"]: row["title"]
        for row in conn.execute(
            f"SELECT id, title FROM projects WHERE id IN ({placeholders})", ids
        ).fetchall()
    }
    conn.close()

    for row in differ:
        row["title"] = (titles.get(row["target_id"]) or "")[:70]
    return differ


def cross_tab(
    pivoted: list[dict],
    clf_a: str,
    clf_b: str,
) -> dict[tuple[str, str], int]:
    """Count (clf_a_section, clf_b_section) pairs."""
    ct: dict[tuple[str, str], int] = defaultdict(int)
    for row in pivoted:
        sa = row.get(f"{clf_a}_section") or "?"
        sb = row.get(f"{clf_b}_section") or "?"
        ct[(sa, sb)] += 1
    return dict(ct)


# ── Text report ───────────────────────────────────────────────────────────────

def print_report(
    results: dict[str, list[dict]],
    db_path: Path,
    target_type: str,
    project_type: str | None,
    repository_id: int | None,
) -> None:
    clf_names = list(results.keys())
    if not clf_names:
        print("No classifier_results found. Run classify_isic.py --classifier all first.")
        return

    label = f"{project_type or 'all'}"
    if repository_id:
        label += f" / repo {repository_id}"
    print(f"\n{'='*70}")
    print(f"CLASSIFIER COMPARISON — {target_type.upper()}S  [{label}]")
    print(f"{'='*70}")
    print(f"Classifiers : {', '.join(clf_names)}")
    for name in clf_names:
        print(f"  {name}: {len(results[name]):,} items classified")

    pivoted = pivot_by_id(results)
    print(f"\nCommon items (in ALL classifiers): {len(pivoted):,}")

    if len(clf_names) >= 2:
        sec_agree  = agreement_rate(pivoted, clf_names, "section")
        div_agree  = agreement_rate(pivoted, clf_names, "division")
        print(f"\n── Agreement rate ──")
        print(f"  Section level : {sec_agree:.1f}%")
        print(f"  Division level: {div_agree:.1f}%")

    # Section distribution
    sec_dist = section_distribution(results)
    all_sections = sorted({s for d in sec_dist.values() for s in d}, key=lambda s: (
        -max(d.get(s, 0) for d in sec_dist.values())
    ))
    print(f"\n── Section distribution ──")
    header = f"  {'Sec':<4}" + "".join(f"{n:>10}" for n in clf_names)
    print(header)
    print("  " + "-" * (4 + 10 * len(clf_names)))
    for sec in all_sections[:20]:
        row = f"  {sec:<4}" + "".join(f"{sec_dist[n].get(sec, 0):>10}" for n in clf_names)
        # Agreement marker
        vals = [sec_dist[n].get(sec, 0) for n in clf_names]
        if len(clf_names) > 1 and max(vals) > 0:
            # Check if rankings agree
            ranks = [sorted(sec_dist[n], key=sec_dist[n].get, reverse=True).index(sec)
                     if sec in sec_dist[n] else 99
                     for n in clf_names]
            if max(ranks) == min(ranks):
                row += "  ✓"
        print(row)

    # Division top-10
    div_top = division_topN(results, 10)
    print(f"\n── Top 10 divisions per classifier ──")
    for clf, tops in div_top.items():
        print(f"\n  [{clf}]")
        for div, name, cnt in tops:
            bar = "█" * min(20, cnt // max(1, tops[0][2] // 20))
            print(f"    {div}  {name[:40]:<40}  {cnt:>6}  {bar}")

    # Confidence distribution
    conf_dist = confidence_distribution(results)
    print(f"\n── Confidence distribution ──")
    print(f"  {'Level':<8}" + "".join(f"{n:>10}" for n in clf_names))
    for level in ("high", "medium", "low"):
        row = f"  {level:<8}" + "".join(f"{conf_dist[n].get(level, 0):>10}" for n in clf_names)
        print(row)

    # Cross-tabulation (if exactly 2 classifiers)
    if len(clf_names) == 2:
        a, b = clf_names
        ct = cross_tab(pivoted, a, b)
        secs = sorted({s for (sa, sb) in ct for s in [sa, sb]})
        print(f"\n── Cross-tabulation: {a} (rows) vs {b} (cols) ──")
        print(f"  {'':4}", end="")
        for s in secs:
            print(f"  {s:>4}", end="")
        print()
        for sa in secs:
            print(f"  {sa:<4}", end="")
            for sb in secs:
                cnt = ct.get((sa, sb), 0)
                print(f"  {cnt:>4}" if cnt > 0 else "     .", end="")
            print()

    # Sample disagreements
    if len(clf_names) >= 2:
        disagree = disagreement_cases(pivoted, clf_names, db_path, n=15)
        if disagree:
            print(f"\n── Sample disagreements ({len(disagree)} shown) ──")
            for row in disagree:
                print(f"  [{row['target_id']}] {row.get('title','')}")
                for clf in clf_names:
                    s = row.get(f"{clf}_section", "?")
                    d = row.get(f"{clf}_division", "?")
                    c = row.get(f"{clf}_confidence", "?")
                    print(f"      {clf:>8}: {s}/{d} ({c})")


# ── PDF report ────────────────────────────────────────────────────────────────

def export_pdf(
    results: dict[str, list[dict]],
    db_path: Path,
    target_type: str,
    project_type: str | None,
    repository_id: int | None,
    output_path: Path,
) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages
    except ImportError:
        print("matplotlib not installed — skipping PDF export.")
        return

    clf_names = list(results.keys())
    if not clf_names:
        return

    pivoted  = pivot_by_id(results)
    sec_dist = section_distribution(results)
    div_top  = division_topN(results, 15)
    conf_dist = confidence_distribution(results)

    label = f"{project_type or 'all'}"
    if repository_id:
        label += f" / repo {repository_id}"

    COLORS = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3"]

    with PdfPages(output_path) as pdf:

        # ── Page 1: section distribution (grouped bar) ─────────────────────
        all_secs = sorted(
            {s for d in sec_dist.values() for s in d},
            key=lambda s: -max(d.get(s, 0) for d in sec_dist.values())
        )[:20]
        x = range(len(all_secs))
        width = 0.8 / len(clf_names)

        fig, ax = plt.subplots(figsize=(14, 6))
        for i, clf in enumerate(clf_names):
            vals = [sec_dist[clf].get(s, 0) for s in all_secs]
            offsets = [xi + (i - len(clf_names) / 2 + 0.5) * width for xi in x]
            ax.bar(offsets, vals, width=width * 0.9,
                   label=clf, color=COLORS[i % len(COLORS)])
        ax.set_xticks(list(x))
        ax.set_xticklabels(all_secs, fontsize=9)
        ax.set_xlabel("ISIC Section")
        ax.set_ylabel("Number of items")
        ax.set_title(f"Section distribution by classifier — {target_type}s [{label}]")
        ax.legend()
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

        # ── Page 2: agreement rate (if ≥2 classifiers) ────────────────────
        if len(clf_names) >= 2:
            sec_agree = agreement_rate(pivoted, clf_names, "section")
            div_agree = agreement_rate(pivoted, clf_names, "division")

            fig, axes = plt.subplots(1, 2, figsize=(10, 4))
            for ax, (title, pct) in zip(axes, [
                ("Section agreement", sec_agree),
                ("Division agreement", div_agree),
            ]):
                ax.pie(
                    [pct, 100 - pct],
                    labels=[f"Agree\n{pct:.1f}%", f"Disagree\n{100-pct:.1f}%"],
                    colors=["#55A868", "#C44E52"],
                    startangle=90,
                    autopct="%1.1f%%",
                )
                ax.set_title(title)
            fig.suptitle(f"Classifier agreement — {target_type}s [{label}]")
            plt.tight_layout()
            pdf.savefig(fig)
            plt.close(fig)

        # ── Page 3: confidence distribution ───────────────────────────────
        fig, ax = plt.subplots(figsize=(8, 4))
        levels = ["high", "medium", "low"]
        x = range(len(clf_names))
        bottoms = [0] * len(clf_names)
        level_colors = {"high": "#55A868", "medium": "#CCBB44", "low": "#C44E52"}
        for lvl in levels:
            vals = [conf_dist[n].get(lvl, 0) for n in clf_names]
            ax.bar(x, vals, bottom=bottoms, label=lvl, color=level_colors[lvl])
            bottoms = [b + v for b, v in zip(bottoms, vals)]
        ax.set_xticks(list(x))
        ax.set_xticklabels(clf_names)
        ax.set_ylabel("Items")
        ax.set_title(f"Confidence distribution — {target_type}s [{label}]")
        ax.legend(loc="upper right")
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

        # ── Page 4: top divisions per classifier (horizontal bars) ─────────
        for clf in clf_names:
            tops = div_top[clf]
            if not tops:
                continue
            labels = [f"{div}: {name[:35]}" for div, name, _ in tops]
            vals   = [cnt for _, _, cnt in tops]

            fig, ax = plt.subplots(figsize=(10, max(4, len(tops) * 0.45)))
            colors = [COLORS[clf_names.index(clf) % len(COLORS)]] * len(tops)
            ax.barh(labels[::-1], vals[::-1], color=colors)
            ax.set_xlabel("Items")
            ax.set_title(f"Top divisions — {clf} — {target_type}s [{label}]")
            plt.tight_layout()
            pdf.savefig(fig)
            plt.close(fig)

        # ── Page 5: cross-tab heatmap (if exactly 2 classifiers) ──────────
        if len(clf_names) == 2:
            try:
                import numpy as np
                a, b = clf_names
                ct = cross_tab(pivoted, a, b)
                secs = sorted({s for (sa, sb) in ct for s in [sa, sb]})
                matrix = [[ct.get((sa, sb), 0) for sb in secs] for sa in secs]
                arr = [[float(v) for v in row] for row in matrix]

                fig, ax = plt.subplots(figsize=(max(7, len(secs) * 0.7),
                                                max(6, len(secs) * 0.6)))
                im = ax.imshow(arr, cmap="Blues")
                ax.set_xticks(range(len(secs)))
                ax.set_yticks(range(len(secs)))
                ax.set_xticklabels(secs, fontsize=8)
                ax.set_yticklabels(secs, fontsize=8)
                ax.set_xlabel(b)
                ax.set_ylabel(a)
                ax.set_title(f"Section cross-tabulation: {a} vs {b} [{label}]")
                plt.colorbar(im, ax=ax)
                for i in range(len(secs)):
                    for j in range(len(secs)):
                        v = matrix[i][j]
                        if v > 0:
                            ax.text(j, i, str(v), ha="center", va="center", fontsize=7)
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)
            except Exception as e:
                print(f"Heatmap skipped: {e}")

    print(f"PDF saved → {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="Compare ISIC classification results across all classifiers."
    )
    p.add_argument("--db",            default=DEFAULT_DB)
    p.add_argument("--target",        choices=["project", "file"], default="project")
    p.add_argument("--project-type",  default=None,
                   choices=["QDA_PROJECT", "QD_PROJECT", "all", None])
    p.add_argument("--repository-id", type=int, default=None)
    p.add_argument("--no-pdf",        action="store_true")
    p.add_argument("--output",        default="comparison_report.pdf")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: database not found: {db_path}")
        raise SystemExit(1)

    results = load_results(
        db_path,
        target_type=args.target,
        project_type=args.project_type,
        repository_id=args.repository_id,
    )

    print_report(results, db_path, args.target, args.project_type, args.repository_id)

    if not args.no_pdf and results:
        export_pdf(results, db_path, args.target,
                   args.project_type, args.repository_id, Path(args.output))


if __name__ == "__main__":
    main()
