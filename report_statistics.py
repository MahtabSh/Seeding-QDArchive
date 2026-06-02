#!/usr/bin/env python3
"""
Part 2 Step 4 — Statistics Report
===================================
Reports on ISIC classification results across project types and repositories.

Produces:
  1. Console summary tables
  2. CSV exports ready for Google Sheets (one sheet per distribution)
  3. PDF with histograms (one page per distribution)

The 4 distributions (Repository × Project Type):
  Distribution 1: Zenodo       (repo 1)  × QDA_PROJECT
  Distribution 2: Zenodo       (repo 1)  × QD_PROJECT
  Distribution 3: Harvard      (repo 10) × QDA_PROJECT
  Distribution 4: Harvard      (repo 10) × QD_PROJECT

Usage
-----
  python report_statistics.py                  # generate all outputs
  python report_statistics.py --db my.sqlite   # custom DB path
  python report_statistics.py --no-pdf         # skip PDF

Outputs
-------
  report_distributions.csv   — all 4 distributions in one CSV (Google Sheets ready)
  report_isic_histograms.pdf — one histogram page per distribution
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

# ── Optional matplotlib (only needed for PDF) ─────────────────────────────────
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

DEFAULT_DB = "metadata.sqlite"

# Repositories to report on
REPOSITORIES = [
    (1,  "Zenodo"),
    (10, "Harvard Dataverse"),
]

PROJECT_TYPES = ["QDA_PROJECT", "QD_PROJECT"]


# ── Data queries ──────────────────────────────────────────────────────────────

def count_by_type_and_repo(conn: sqlite3.Connection) -> dict:
    """Return project counts keyed by (repo_id, project_type)."""
    rows = conn.execute("""
        SELECT repository_id, type, COUNT(*) as n
        FROM projects
        WHERE type IN ('QDA_PROJECT','QD_PROJECT','OTHER_PROJECT','NOT_A_PROJECT')
        GROUP BY repository_id, type
    """).fetchall()
    result = {}
    for repo_id, ptype, n in rows:
        result[(repo_id, ptype)] = n
    return result


def isic_section_distribution(
    conn: sqlite3.Connection,
    repo_id: int,
    project_type: str,
    level: str = "project",   # "project" or "file"
) -> list[tuple[str, str, int]]:
    """
    Return [(section_code, section_name, count)] sorted by count descending.
    level="project" queries projects table; level="file" queries project_files.
    """
    if level == "project":
        rows = conn.execute("""
            SELECT isic_section, isic_section_name, COUNT(*) as n
            FROM projects
            WHERE repository_id = ?
              AND type = ?
              AND isic_section IS NOT NULL
            GROUP BY isic_section
            ORDER BY n DESC
        """, (repo_id, project_type)).fetchall()
    else:
        rows = conn.execute("""
            SELECT pf.isic_section, pf.isic_section_name, COUNT(*) as n
            FROM project_files pf
            JOIN projects p ON p.id = pf.project_id
            WHERE p.repository_id = ?
              AND p.type = ?
              AND pf.isic_section IS NOT NULL
            GROUP BY pf.isic_section
            ORDER BY n DESC
        """, (repo_id, project_type)).fetchall()
    return [(r[0], r[1] or "", r[2]) for r in rows]


def isic_division_distribution(
    conn: sqlite3.Connection,
    repo_id: int,
    project_type: str,
    level: str = "project",
) -> list[tuple[str, str, str, int]]:
    """
    Return [(section_code, division_code, division_name, count)] sorted by count desc.
    """
    if level == "project":
        rows = conn.execute("""
            SELECT isic_section, isic_division, isic_division_name, COUNT(*) as n
            FROM projects
            WHERE repository_id = ?
              AND type = ?
              AND isic_division IS NOT NULL
            GROUP BY isic_division
            ORDER BY n DESC
        """, (repo_id, project_type)).fetchall()
    else:
        rows = conn.execute("""
            SELECT pf.isic_section, pf.isic_division, pf.isic_division_name, COUNT(*) as n
            FROM project_files pf
            JOIN projects p ON p.id = pf.project_id
            WHERE p.repository_id = ?
              AND p.type = ?
              AND pf.isic_division IS NOT NULL
            GROUP BY pf.isic_division
            ORDER BY n DESC
        """, (repo_id, project_type)).fetchall()
    return [(r[0], r[1], r[2] or "", r[3]) for r in rows]


def top_tags(
    conn: sqlite3.Connection,
    repo_id: int,
    project_type: str,
    n: int = 20,
) -> list[tuple[str, int]]:
    rows = conn.execute("""
        SELECT t.tag, COUNT(*) as cnt
        FROM tags t
        JOIN projects p ON p.id = t.project_id
        WHERE p.repository_id = ?
          AND p.type = ?
        GROUP BY t.tag
        ORDER BY cnt DESC
        LIMIT ?
    """, (repo_id, project_type, n)).fetchall()
    return [(r[0], r[1]) for r in rows]


# ── Console output ────────────────────────────────────────────────────────────

def print_overview(conn: sqlite3.Connection) -> None:
    counts = count_by_type_and_repo(conn)
    print("\n" + "="*70)
    print("QUALITATIVE DATA OVERVIEW — Projects by Type and Repository")
    print("="*70)
    header = f"{'Repository':<22} {'QDA_PROJECT':>12} {'QD_PROJECT':>12} {'OTHER':>8} {'NOT_A':>8}"
    print(header)
    print("-" * 70)
    totals = {}
    for repo_id, repo_name in REPOSITORIES:
        row = {}
        for pt in ("QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT"):
            row[pt] = counts.get((repo_id, pt), 0)
        print(f"{repo_name:<22} {row['QDA_PROJECT']:>12} {row['QD_PROJECT']:>12} "
              f"{row['OTHER_PROJECT']:>8} {row['NOT_A_PROJECT']:>8}")
        for pt, v in row.items():
            totals[pt] = totals.get(pt, 0) + v
    print("-" * 70)
    print(f"{'TOTAL':<22} {totals.get('QDA_PROJECT',0):>12} {totals.get('QD_PROJECT',0):>12} "
          f"{totals.get('OTHER_PROJECT',0):>8} {totals.get('NOT_A_PROJECT',0):>8}")


def print_distribution(
    conn: sqlite3.Connection,
    repo_id: int,
    repo_name: str,
    project_type: str,
) -> None:
    label = f"{repo_name} × {project_type}"
    print(f"\n{'─'*64}")
    print(f"Distribution: {label}")
    print(f"{'─'*64}")

    sec_proj  = isic_section_distribution(conn, repo_id, project_type, "project")
    sec_files = isic_section_distribution(conn, repo_id, project_type, "file")
    div_proj  = isic_division_distribution(conn, repo_id, project_type, "project")

    if not sec_proj:
        print("  (no classified projects yet)")
        return

    print(f"\n  Section distribution — projects (n={sum(x[2] for x in sec_proj)})")
    for sec, name, n in sec_proj:
        bar = "█" * min(25, max(1, n * 25 // sec_proj[0][2]))
        print(f"    {sec}  {name[:40]:<40}  {n:>5}  {bar}")

    if sec_files:
        print(f"\n  Section distribution — primary files (n={sum(x[2] for x in sec_files)})")
        for sec, name, n in sec_files:
            bar = "█" * min(25, max(1, n * 25 // sec_files[0][2]))
            print(f"    {sec}  {name[:40]:<40}  {n:>5}  {bar}")

    print(f"\n  Top 10 divisions — projects")
    for sec, div, name, n in div_proj[:10]:
        print(f"    {sec}/{div}  {name[:44]:<44}  {n:>5}")

    tags = top_tags(conn, repo_id, project_type, 10)
    if tags:
        print(f"\n  Top 10 search tags")
        for tag, n in tags:
            print(f"    {n:>4}  {tag}")


# ── CSV export ────────────────────────────────────────────────────────────────

def export_csv(conn: sqlite3.Connection, out_path: str) -> None:
    """
    Write a CSV with all 4 distributions.
    Columns: repository, project_type, level, isic_section, section_name,
             isic_division, division_name, count
    """
    rows = []
    for repo_id, repo_name in REPOSITORIES:
        for pt in PROJECT_TYPES:
            for sec, name, n in isic_section_distribution(conn, repo_id, pt, "project"):
                rows.append({
                    "repository": repo_name,
                    "project_type": pt,
                    "level": "project",
                    "isic_section": sec,
                    "section_name": name,
                    "isic_division": "",
                    "division_name": "",
                    "count": n,
                })
            for sec, div, dname, n in isic_division_distribution(conn, repo_id, pt, "project"):
                rows.append({
                    "repository": repo_name,
                    "project_type": pt,
                    "level": "project_division",
                    "isic_section": sec,
                    "section_name": "",
                    "isic_division": div,
                    "division_name": dname,
                    "count": n,
                })
            for sec, name, n in isic_section_distribution(conn, repo_id, pt, "file"):
                rows.append({
                    "repository": repo_name,
                    "project_type": pt,
                    "level": "file",
                    "isic_section": sec,
                    "section_name": name,
                    "isic_division": "",
                    "division_name": "",
                    "count": n,
                })

    if not rows:
        print("No classified data yet — CSV will be empty.")
        rows = [{"repository": "none", "project_type": "none", "level": "none",
                 "isic_section": "", "section_name": "", "isic_division": "",
                 "division_name": "", "count": 0}]

    fieldnames = ["repository", "project_type", "level",
                  "isic_section", "section_name",
                  "isic_division", "division_name", "count"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # Also write overview counts
    overview_path = out_path.replace(".csv", "_overview.csv")
    counts = count_by_type_and_repo(conn)
    with open(overview_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["repository", "repository_id",
                                           "QDA_PROJECT", "QD_PROJECT",
                                           "OTHER_PROJECT", "NOT_A_PROJECT", "TOTAL"])
        w.writeheader()
        for repo_id, repo_name in REPOSITORIES:
            qda   = counts.get((repo_id, "QDA_PROJECT"),   0)
            qd    = counts.get((repo_id, "QD_PROJECT"),    0)
            other = counts.get((repo_id, "OTHER_PROJECT"), 0)
            nota  = counts.get((repo_id, "NOT_A_PROJECT"), 0)
            w.writerow({"repository": repo_name, "repository_id": repo_id,
                        "QDA_PROJECT": qda, "QD_PROJECT": qd,
                        "OTHER_PROJECT": other, "NOT_A_PROJECT": nota,
                        "TOTAL": qda + qd + other + nota})

    print(f"CSV written: {out_path}")
    print(f"CSV written: {overview_path}")


# ── PDF histograms ────────────────────────────────────────────────────────────

def export_pdf(conn: sqlite3.Connection, out_path: str) -> None:
    if not HAS_MATPLOTLIB:
        print("matplotlib not installed — skipping PDF. Run: pip install matplotlib")
        return

    from matplotlib.backends.backend_pdf import PdfPages

    with PdfPages(out_path) as pdf:
        # Page 0: overview bar chart (project counts by type × repo)
        counts = count_by_type_and_repo(conn)
        fig, ax = plt.subplots(figsize=(10, 5))
        repo_names = [r[1] for r in REPOSITORIES]
        x = range(len(repo_names))
        width = 0.35
        qda_vals = [counts.get((r[0], "QDA_PROJECT"), 0) for r in REPOSITORIES]
        qd_vals  = [counts.get((r[0], "QD_PROJECT"),  0) for r in REPOSITORIES]
        bars1 = ax.bar([i - width/2 for i in x], qda_vals, width,
                       label="QDA_PROJECT", color="#2c7bb6")
        bars2 = ax.bar([i + width/2 for i in x], qd_vals,  width,
                       label="QD_PROJECT",  color="#d7191c")
        ax.set_xticks(list(x))
        ax.set_xticklabels(repo_names, fontsize=12)
        ax.set_ylabel("Number of projects")
        ax.set_title("Projects by Type and Repository", fontsize=14, fontweight="bold")
        ax.legend()
        for bar in bars1 + bars2:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width()/2, h + 0.5,
                        str(h), ha="center", va="bottom", fontsize=9)
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

        # Pages 1-4: one per distribution (section histogram)
        dist_num = 0
        for repo_id, repo_name in REPOSITORIES:
            for pt in PROJECT_TYPES:
                dist_num += 1
                sec_proj  = isic_section_distribution(conn, repo_id, pt, "project")
                sec_files = isic_section_distribution(conn, repo_id, pt, "file")

                if not sec_proj and not sec_files:
                    # Still draw an empty page with note
                    fig, ax = plt.subplots(figsize=(12, 6))
                    ax.text(0.5, 0.5, f"Distribution {dist_num}\n{repo_name} × {pt}\n\n"
                            "(not yet classified — run classify_isic.py first)",
                            ha="center", va="center", fontsize=13, transform=ax.transAxes)
                    ax.axis("off")
                    pdf.savefig(fig)
                    plt.close(fig)
                    continue

                fig = plt.figure(figsize=(14, 10))
                fig.suptitle(
                    f"Distribution {dist_num}: {repo_name}  ×  {pt}\n"
                    f"ISIC Rev. 5 Section Distribution",
                    fontsize=13, fontweight="bold", y=0.98,
                )
                gs = gridspec.GridSpec(2, 1, figure=fig, hspace=0.45)

                # Top: project-level sections
                if sec_proj:
                    ax1 = fig.add_subplot(gs[0])
                    labels = [f"{s[0]}: {s[1][:35]}" for s in sec_proj]
                    vals   = [s[2] for s in sec_proj]
                    colors = plt.cm.tab20.colors[:len(labels)]
                    bars = ax1.barh(labels[::-1], vals[::-1], color=colors[::-1])
                    ax1.set_xlabel("Number of projects")
                    ax1.set_title(f"Projects (n={sum(vals)})", fontsize=11)
                    ax1.xaxis.set_major_locator(
                        matplotlib.ticker.MaxNLocator(integer=True))
                    for bar, val in zip(bars, vals[::-1]):
                        ax1.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                                 str(val), va="center", fontsize=8)

                # Bottom: file-level sections
                if sec_files:
                    ax2 = fig.add_subplot(gs[1])
                    labels2 = [f"{s[0]}: {s[1][:35]}" for s in sec_files]
                    vals2   = [s[2] for s in sec_files]
                    colors2 = plt.cm.tab20.colors[:len(labels2)]
                    bars2 = ax2.barh(labels2[::-1], vals2[::-1], color=colors2[::-1])
                    ax2.set_xlabel("Number of primary data files")
                    ax2.set_title(f"Primary data files (n={sum(vals2)})", fontsize=11)
                    ax2.xaxis.set_major_locator(
                        matplotlib.ticker.MaxNLocator(integer=True))
                    for bar, val in zip(bars2, vals2[::-1]):
                        ax2.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                                 str(val), va="center", fontsize=8)

                plt.tight_layout(rect=[0, 0, 1, 0.96])
                pdf.savefig(fig)
                plt.close(fig)

        # Final page: top tags per distribution
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle("Top 15 Search Tags by Distribution", fontsize=13, fontweight="bold")
        for idx, (repo_id, repo_name) in enumerate(REPOSITORIES):
            for jdx, pt in enumerate(PROJECT_TYPES):
                ax = axes[idx][jdx]
                dist_tags = top_tags(conn, repo_id, pt, 15)
                if dist_tags:
                    tag_labels = [t[0] for t in dist_tags]
                    tag_vals   = [t[1] for t in dist_tags]
                    ax.barh(tag_labels[::-1], tag_vals[::-1], color="#4393c3")
                    ax.set_title(f"Dist {idx*2+jdx+1}: {repo_name[:15]} × {pt[:10]}",
                                 fontsize=9)
                    ax.xaxis.set_major_locator(
                        matplotlib.ticker.MaxNLocator(integer=True))
                else:
                    ax.text(0.5, 0.5, "no data yet",
                            ha="center", va="center", transform=ax.transAxes)
                    ax.set_title(f"Dist {idx*2+jdx+1}: {repo_name[:15]} × {pt[:10]}",
                                 fontsize=9)
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

    print(f"PDF written: {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate statistics report for ISIC classification results."
    )
    parser.add_argument("--db",     default=DEFAULT_DB)
    parser.add_argument("--csv",    default="report_distributions.csv")
    parser.add_argument("--pdf",    default="report_isic_histograms.pdf")
    parser.add_argument("--no-pdf", action="store_true", help="Skip PDF generation")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    print_overview(conn)
    for repo_id, repo_name in REPOSITORIES:
        for pt in PROJECT_TYPES:
            print_distribution(conn, repo_id, repo_name, pt)

    export_csv(conn, args.csv)

    if not args.no_pdf:
        export_pdf(conn, args.pdf)

    conn.close()


if __name__ == "__main__":
    main()
