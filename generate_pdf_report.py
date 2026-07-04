#!/usr/bin/env python3
"""
Generate PDF classification report per repository for Part 2 Step 4d of SQ26.

Each PDF contains:
  - Page 1: Histogram of primary ISIC sections (vector graphics, counts on bars)
  - Page 2: Top-20 rank-ordered class table + comments on findings

Outputs: 23692652-sq26-report-repo{id}.pdf  (one per repository)

Usage:
    python3 generate_pdf_report.py
    python3 generate_pdf_report.py --repo 1 10 19
"""
from __future__ import annotations

import argparse
import sqlite3
import textwrap
from pathlib import Path

import matplotlib
matplotlib.use("PDF")   # vector PDF — no rasterisation
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

DB = "23692652-sq26.db"

ISIC_SECTIONS: dict[str, str] = {
    "A": "Agriculture, forestry and fishing",
    "B": "Mining and quarrying",
    "C": "Manufacturing",
    "D": "Electricity, gas, steam and air conditioning supply",
    "E": "Water supply; sewerage, waste management",
    "F": "Construction",
    "G": "Wholesale and retail trade",
    "H": "Transportation and storage",
    "I": "Accommodation and food service activities",
    "J": "Publishing, broadcasting, and content production",
    "K": "ICT (telecom, programming, computing infrastructure)",
    "L": "Financial and insurance activities",
    "M": "Real estate activities",
    "N": "Professional, scientific and technical activities",
    "O": "Administrative and support service activities",
    "P": "Public administration and defence",
    "Q": "Education",
    "R": "Human health and social work activities",
    "S": "Arts, sports and recreation",
    "T": "Other service activities",
    "U": "Households – goods/services for own use",
    "V": "Extraterritorial organizations and bodies",
}

SHORT_LABELS: dict[str, str] = {
    "A": "Agri.", "B": "Mining", "C": "Mfg.", "D": "Energy",
    "E": "Water", "F": "Constr.", "G": "Trade", "H": "Transport",
    "I": "Food/Hotel", "J": "Publish.", "K": "ICT", "L": "Finance",
    "M": "Real Est.", "N": "Prof./Sci.", "O": "Admin.", "P": "Gov.",
    "Q": "Educ.", "R": "Health", "S": "Arts", "T": "Other Svc.",
    "U": "Household", "V": "Intl. Org.",
}

REPO_NAMES: dict[int, str] = {
    1:  "Zenodo",
    10: "Harvard Dataverse",
    19: "Columbia University Libraries",
}

BLUE_DARK  = "#1565C0"
BLUE_MID   = "#2196F3"
BLUE_LIGHT = "#90CAF9"
GRID_COLOR = "#E0E0E0"


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_section_counts(conn: sqlite3.Connection, repo_id: int) -> list[tuple]:
    return conn.execute("""
        SELECT cv.section, cv.section_name, COUNT(*) AS cnt
        FROM projects p
        JOIN classifications_vote cv ON p.id = cv.project_id
        WHERE p.repository_id = ?
        GROUP BY cv.section
        ORDER BY cnt DESC
    """, (repo_id,)).fetchall()


def get_type_stats(conn: sqlite3.Connection, repo_id: int) -> list[tuple]:
    return conn.execute("""
        SELECT COALESCE(type, 'UNKNOWN'), COUNT(*) AS cnt
        FROM projects
        WHERE repository_id = ?
        GROUP BY type ORDER BY cnt DESC
    """, (repo_id,)).fetchall()


def get_top20(conn: sqlite3.Connection, repo_id: int) -> list[tuple]:
    return conn.execute("""
        SELECT
            cv.section,
            cv.section_name,
            COALESCE(cv.division, '') AS division,
            COALESCE(cv.division_name, '') AS division_name,
            COUNT(*) AS cnt,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM projects p2
                JOIN classifications_vote cv2 ON p2.id = cv2.project_id
                WHERE p2.repository_id = ?
            ), 1) AS pct
        FROM projects p
        JOIN classifications_vote cv ON p.id = cv.project_id
        WHERE p.repository_id = ?
        GROUP BY cv.section, cv.division
        ORDER BY cnt DESC
        LIMIT 20
    """, (repo_id, repo_id)).fetchall()


def get_confidence_stats(conn: sqlite3.Connection, repo_id: int) -> list[tuple]:
    return conn.execute("""
        SELECT cv.confidence, COUNT(*) AS cnt
        FROM projects p
        JOIN classifications_vote cv ON p.id = cv.project_id
        WHERE p.repository_id = ?
        GROUP BY cv.confidence
        ORDER BY CASE cv.confidence
            WHEN 'very_high' THEN 1 WHEN 'high' THEN 2
            WHEN 'medium'    THEN 3 ELSE 4 END
    """, (repo_id,)).fetchall()


def get_top_divisions(conn: sqlite3.Connection, repo_id: int, n: int = 15) -> list[tuple]:
    return conn.execute("""
        SELECT
            cv.section,
            cv.division,
            COALESCE(cv.division_name, cv.section_name, '') AS label,
            COUNT(*) AS cnt
        FROM projects p
        JOIN classifications_vote cv ON p.id = cv.project_id
        WHERE p.repository_id = ?
          AND cv.division IS NOT NULL AND cv.division != ''
        GROUP BY cv.section, cv.division
        ORDER BY cnt DESC
        LIMIT ?
    """, (repo_id, n)).fetchall()


# ── Page 1: histogram ──────────────────────────────────────────────────────────

def _page_histogram(pdf: PdfPages, repo_id: int, repo_name: str,
                    section_counts: list[tuple], type_stats: list[tuple],
                    conf_stats: list[tuple]) -> None:
    total = sum(c for _, _, c in section_counts)
    sec_dict: dict[str, int] = {s: c for s, _, c in section_counts}
    all_secs = sorted(ISIC_SECTIONS.keys())
    counts   = [sec_dict.get(s, 0) for s in all_secs]
    top3     = {s for s, _ in sorted(sec_dict.items(), key=lambda x: -x[1])[:3]}
    colors   = [BLUE_DARK if s in top3 else BLUE_LIGHT for s in all_secs]

    fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
    fig.patch.set_facecolor("white")
    gs  = gridspec.GridSpec(3, 1, height_ratios=[0.13, 0.78, 0.09], figure=fig, hspace=0.04)

    # ── title strip ──
    ax_t = fig.add_subplot(gs[0])
    ax_t.axis("off")
    ax_t.text(0.5, 0.85, "SQ26 — ISIC Rev.5 Classification Report",
              transform=ax_t.transAxes, fontsize=17, fontweight="bold",
              ha="center", va="top")
    ax_t.text(0.5, 0.45, f"Repository {repo_id}: {repo_name}  ·  {total:,} projects classified",
              transform=ax_t.transAxes, fontsize=11, ha="center", va="top", color="#555")
    type_summary = "   |   ".join(f"{t}: {c:,}" for t, c in type_stats)
    ax_t.text(0.5, 0.05, f"Project types — {type_summary}",
              transform=ax_t.transAxes, fontsize=8.5, ha="center", color="#777")

    # ── histogram ──
    ax = fig.add_subplot(gs[1])
    bar_container = ax.bar(range(len(all_secs)), counts, color=colors,
                           edgecolor="white", linewidth=0.4, width=0.75)

    max_count = max(counts) if counts else 1
    for idx, (bar, count, sec) in enumerate(zip(bar_container, counts, all_secs)):
        if count > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max_count * 0.012,
                f"{count:,}",
                ha="center", va="bottom",
                fontsize=6.5,
                fontweight="bold" if sec in top3 else "normal",
                color="#111" if sec in top3 else "#444",
            )

    ax.set_xlim(-0.6, len(all_secs) - 0.4)
    ax.set_ylim(0, max_count * 1.18)
    ax.set_xticks(range(len(all_secs)))
    ax.set_xticklabels(
        [f"{s}\n{SHORT_LABELS.get(s, '')}" for s in all_secs],
        fontsize=7,
    )
    ax.set_xlabel("ISIC Rev.5 Section", fontsize=11, labelpad=8)
    ax.set_ylabel("Number of Projects", fontsize=11)
    ax.set_title(
        "Primary ISIC Sections — All Classified Projects",
        fontsize=13, pad=10, fontweight="bold",
    )
    ax.yaxis.grid(True, linestyle="--", alpha=0.6, color=GRID_COLOR)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#AAAAAA")
    ax.spines["bottom"].set_color("#AAAAAA")

    # Confidence annotation
    conf_text = "   ".join(
        f"{c}: {n:,}" for c, n in conf_stats
    )
    ax.annotate(f"Confidence  —  {conf_text}",
                xy=(0.01, 0.97), xycoords="axes fraction",
                fontsize=8, va="top", color="#555")

    # Legend patch
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=BLUE_DARK,  label="Top-3 sections"),
        Patch(facecolor=BLUE_LIGHT, label="Other sections"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=8, framealpha=0.7)

    # ── footer ──
    ax_f = fig.add_subplot(gs[2])
    ax_f.axis("off")
    ax_f.text(0.5, 0.5,
              "Student ID: 23692652  ·  FAU Erlangen-Nürnberg  ·  SQ26 Project",
              transform=ax_f.transAxes, fontsize=8, ha="center", color="#AAA")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── Page 2: top-20 table + comments ───────────────────────────────────────────

def _page_table_comments(pdf: PdfPages, repo_id: int, repo_name: str,
                         top20: list[tuple], section_counts: list[tuple],
                         type_stats: list[tuple], conf_stats: list[tuple]) -> None:
    total = sum(c for _, _, c in section_counts)
    sec_dict: dict[str, int] = {s: c for s, _, c in section_counts}
    sorted_secs = sorted(sec_dict.items(), key=lambda x: -x[1])
    dom_sec, dom_cnt = sorted_secs[0] if sorted_secs else ("?", 0)
    dom_name = ISIC_SECTIONS.get(dom_sec, "")
    dom_pct  = dom_cnt * 100.0 / total if total else 0.0
    n_secs   = len(sec_dict)

    very_high = sum(n for c, n in conf_stats if c == "very_high")
    high      = sum(n for c, n in conf_stats if c == "high")
    confident = very_high + high
    conf_pct  = confident * 100.0 / total if total else 0.0

    fig, ax = plt.subplots(figsize=(11.69, 8.27))
    fig.patch.set_facecolor("white")
    ax.axis("off")

    ax.text(0.5, 0.975,
            f"Top-20 ISIC Classes — Repository {repo_id}: {repo_name}",
            transform=ax.transAxes, fontsize=14, fontweight="bold",
            ha="center", va="top")

    # ── Table ──
    col_labels = ["Rank", "Sec.", "Division", "Class Name", "Count", "%"]
    col_widths = [0.055, 0.055, 0.075, 0.545, 0.085, 0.07]

    table_rows = []
    for rank, (sec, sec_name, div, div_name, cnt, pct) in enumerate(top20, 1):
        if div:
            class_label = f"{div} — {div_name}" if div_name else div
        else:
            class_label = sec_name or ""
        table_rows.append([
            str(rank),
            sec,
            div or "—",
            class_label[:62],
            f"{cnt:,}",
            f"{pct:.1f}%",
        ])

    tbl = ax.table(
        cellText=table_rows,
        colLabels=col_labels,
        colWidths=col_widths,
        loc="upper center",
        bbox=[0.01, 0.345, 0.98, 0.605],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)

    header_fc = "#1565C0"
    alt_fc    = "#E3F2FD"
    for (row_idx, col_idx), cell in tbl.get_celld().items():
        cell.set_linewidth(0.3)
        if row_idx == 0:
            cell.set_facecolor(header_fc)
            cell.get_text().set_color("white")
            cell.get_text().set_fontweight("bold")
        elif row_idx % 2 == 0:
            cell.set_facecolor(alt_fc)
        if col_idx == 3:   # class name: left-align
            cell.get_text().set_ha("left")
            cell.PAD = 0.01

    # ── Comments ──
    ax.text(0.01, 0.335, "Comments on Findings",
            transform=ax.transAxes, fontsize=11, fontweight="bold", va="top")

    # Build comment paragraphs
    type_str = ";  ".join(f"{t} ({c:,})" for t, c in type_stats)
    secs_list = ", ".join(sorted(sec_dict.keys()))

    comments: list[str] = [
        f"Repository {repo_id} ({repo_name}) contains {total:,} classified projects "
        f"across {n_secs} ISIC Rev.5 sections ({secs_list}).",

        f"Project types present: {type_str}.",

        f"The dominant ISIC section is {dom_sec} — {dom_name}, accounting for "
        f"{dom_cnt:,} projects ({dom_pct:.1f}% of the repository).",

        f"Classification confidence: {confident:,} projects ({conf_pct:.1f}%) "
        f"classified at high or very-high confidence by the voting ensemble.",
    ]

    if dom_sec == "N":
        comments.append(
            "Section N (Professional, scientific and technical activities) is the dominant "
            "class. This is expected for general-purpose academic data repositories where many "
            "datasets describe scientific research processes. Sub-section 72 "
            "(Scientific research and development) is typically the most frequent division."
        )
    elif dom_sec == "A":
        comments.append(
            "Section A (Agriculture, forestry and fishing) dominates, indicating a strong "
            "representation of environmental, ecological, and agricultural datasets — consistent "
            "with open-data repositories that serve the natural-sciences community."
        )
    elif dom_sec == "R":
        comments.append(
            "Section R (Human health and social work activities) is the dominant class, "
            "reflecting the repository's focus on medical, clinical, and social datasets."
        )
    elif dom_sec == "P":
        comments.append(
            "Section P (Public administration and defence) dominates, reflecting a large "
            "share of political-science, governance, and survey datasets typical of social "
            "science repositories."
        )
    elif dom_sec == "Q":
        comments.append(
            "Section Q (Education) is dominant, consistent with repositories that focus on "
            "educational research datasets."
        )

    if n_secs >= 18:
        comments.append(
            f"The distribution spans {n_secs} of 22 ISIC sections, indicating high thematic "
            "diversity. No single field monopolises the repository."
        )
    elif n_secs <= 8:
        comments.append(
            f"With only {n_secs} sections represented, the repository has a narrow thematic "
            "focus. Future curation may benefit from broadening the scope."
        )

    y = 0.295
    for para in comments:
        wrapped = textwrap.fill(para, width=125)
        lines = wrapped.split("\n")
        ax.text(0.01, y, "•  " + lines[0],
                transform=ax.transAxes, fontsize=8.5, va="top")
        for line in lines[1:]:
            y -= 0.030
            ax.text(0.025, y, line, transform=ax.transAxes, fontsize=8.5, va="top")
        y -= 0.042

    ax.text(0.5, 0.01,
            "Student ID: 23692652  ·  FAU Erlangen-Nürnberg  ·  SQ26 Project",
            transform=ax.transAxes, fontsize=8, ha="center", color="#AAA")

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── Page 3: division-level horizontal bar chart ────────────────────────────────

# Distinct colour per ISIC section (A–V) so bars are identifiable by section
_SEC_COLORS: dict[str, str] = {
    "A": "#2E7D32", "B": "#4E342E", "C": "#E65100", "D": "#F9A825",
    "E": "#00838F", "F": "#546E7A", "G": "#AD1457", "H": "#6A1B9A",
    "I": "#880E4F", "J": "#1565C0", "K": "#0277BD", "L": "#00695C",
    "M": "#558B2F", "N": "#1565C0", "O": "#4527A0", "P": "#283593",
    "Q": "#01579B", "R": "#BF360C", "S": "#827717", "T": "#37474F",
    "U": "#795548", "V": "#616161",
}


def _page_division_histogram(pdf: PdfPages, repo_id: int, repo_name: str,
                              top_divs: list[tuple],
                              section_counts: list[tuple]) -> None:
    if not top_divs:
        return

    total = sum(c for _, _, c in section_counts)

    # Build labels and values (reverse order for bottom-to-top reading)
    labels = []
    values = []
    colors = []
    for sec, div, label, cnt in reversed(top_divs):
        short_label = label[:50] if len(label) > 50 else label
        labels.append(f"{sec}/{div}  {short_label}")
        values.append(cnt)
        colors.append(_SEC_COLORS.get(sec, BLUE_MID))

    fig, ax = plt.subplots(figsize=(11.69, 8.27))
    fig.patch.set_facecolor("white")

    y_pos = range(len(labels))
    bars = ax.barh(list(y_pos), values, color=colors, edgecolor="white",
                   linewidth=0.4, height=0.7)

    max_val = max(values) if values else 1
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_width() + max_val * 0.008,
            bar.get_y() + bar.get_height() / 2,
            f"{val:,}",
            va="center", ha="left", fontsize=7.5, color="#222",
        )

    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlim(0, max_val * 1.18)
    ax.set_xlabel("Number of Projects", fontsize=11)
    ax.set_title(
        f"Top-{len(top_divs)} ISIC Divisions (Secondary Classes)\n"
        f"Repository {repo_id}: {repo_name}  ·  {total:,} projects total",
        fontsize=13, pad=12, fontweight="bold",
    )
    ax.xaxis.grid(True, linestyle="--", alpha=0.5, color=GRID_COLOR)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#AAAAAA")
    ax.spines["bottom"].set_color("#AAAAAA")

    # Section colour legend (only sections that appear in the chart)
    from matplotlib.patches import Patch
    seen_secs = list(dict.fromkeys(sec for sec, *_ in top_divs))
    legend_handles = [
        Patch(facecolor=_SEC_COLORS.get(s, BLUE_MID),
              label=f"{s} — {ISIC_SECTIONS.get(s, s)[:35]}")
        for s in seen_secs
    ]
    ax.legend(handles=legend_handles, loc="lower right", fontsize=7,
              framealpha=0.85, ncol=2)

    ax.text(0.5, -0.07,
            "Student ID: 23692652  ·  FAU Erlangen-Nürnberg  ·  SQ26 Project",
            transform=ax.transAxes, fontsize=8, ha="center", color="#AAA")

    fig.tight_layout(rect=[0, 0.03, 1, 1])
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── Main ───────────────────────────────────────────────────────────────────────

def generate_repo_pdf(conn: sqlite3.Connection, repo_id: int) -> str:
    repo_name      = REPO_NAMES.get(repo_id, f"Repository {repo_id}")
    section_counts = get_section_counts(conn, repo_id)
    type_stats     = get_type_stats(conn, repo_id)
    top20          = get_top20(conn, repo_id)
    conf_stats     = get_confidence_stats(conn, repo_id)
    top_divs       = get_top_divisions(conn, repo_id, n=15)

    out_path = f"23692652-sq26-report-repo{repo_id}.pdf"
    with PdfPages(out_path) as pdf:
        _page_histogram(pdf, repo_id, repo_name,
                        section_counts, type_stats, conf_stats)
        _page_table_comments(pdf, repo_id, repo_name,
                             top20, section_counts, type_stats, conf_stats)
        _page_division_histogram(pdf, repo_id, repo_name,
                                 top_divs, section_counts)

    print(f"[repo {repo_id}] Saved: {out_path}  "
          f"(total {sum(c for _,_,c in section_counts):,} projects)")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate PDF classification report per repository."
    )
    ap.add_argument("--db",   default=DB)
    ap.add_argument("--repo", nargs="+", type=int, default=None,
                    help="Specific repository IDs (default: all)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    repo_ids = args.repo or [
        r[0] for r in conn.execute(
            "SELECT DISTINCT repository_id FROM projects ORDER BY repository_id"
        ).fetchall()
    ]

    print(f"Generating PDF reports for {len(repo_ids)} repositories…\n")
    for rid in repo_ids:
        generate_repo_pdf(conn, rid)

    conn.close()
    print("\nAll done.")


if __name__ == "__main__":
    main()
