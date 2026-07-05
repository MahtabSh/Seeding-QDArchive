#!/usr/bin/env python3
"""
Generate a self-contained LaTeX report for all 3 repositories.
Output: 23692652-sq26-classification-report.tex
Compile on Overleaf / ShareLaTeX with pdflatex (no extra packages needed beyond standard).

Usage:
    python3 generate_latex_report.py --db 23692652-sq26.db
"""
from __future__ import annotations

import argparse
import io
import sqlite3
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch
import numpy as np
import base64

DB = "23692652-sq26.db"
OUT = "23692652-sq26-classification-report.tex"

REPO_NAMES = {1: "Zenodo", 10: "Harvard Dataverse", 19: "Columbia University Libraries"}

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
    "U": "Activities of households as employers",
    "V": "Activities of extraterritorial organizations",
}

SHORT_LABELS: dict[str, str] = {
    "A": "Agri.", "B": "Mining", "C": "Manuf.", "D": "Energy",
    "E": "Water", "F": "Constr.", "G": "Trade", "H": "Transport",
    "I": "Accom.", "J": "Publish.", "K": "ICT", "L": "Finance",
    "M": "Real Est.", "N": "Prof./Sci.", "O": "Admin.", "P": "Public Adm.",
    "Q": "Educ.", "R": "Health", "S": "Arts", "T": "Other Svc.",
    "U": "Households", "V": "Extraterr.",
}

BLUE_DARK  = "#1565C0"
BLUE_LIGHT = "#90CAF9"


# ── helpers ────────────────────────────────────────────────────────────────────

def esc(s: str) -> str:
    """Escape special LaTeX characters."""
    replacements = [
        ("\\", "\\textbackslash{}"),
        ("&",  "\\&"),
        ("%",  "\\%"),
        ("$",  "\\$"),
        ("#",  "\\#"),
        ("_",  "\\_"),
        ("{",  "\\{"),
        ("}",  "\\}"),
        ("~",  "\\textasciitilde{}"),
        ("^",  "\\textasciicircum{}"),
    ]
    for old, new in replacements:
        s = s.replace(old, new)
    return s


def fig_to_png_file(fig, path: str) -> None:
    fig.savefig(path, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)


# ── DB queries ─────────────────────────────────────────────────────────────────

def get_section_counts(conn, repo_id):
    return conn.execute("""
        SELECT cv.section, cv.section_name, COUNT(*) AS cnt
        FROM projects p JOIN classifications_vote cv ON p.id = cv.project_id
        WHERE p.repository_id = ?
        GROUP BY cv.section ORDER BY cnt DESC
    """, (repo_id,)).fetchall()


def get_type_stats(conn, repo_id):
    return conn.execute("""
        SELECT COALESCE(type,'UNKNOWN'), COUNT(*)
        FROM projects WHERE repository_id = ?
        GROUP BY type ORDER BY COUNT(*) DESC
    """, (repo_id,)).fetchall()


def get_top20(conn, repo_id):
    return conn.execute("""
        SELECT cv.section,
               COALESCE(cv.division,'') AS div,
               COALESCE(cv.division_name, cv.section_name,'') AS div_name,
               COUNT(*) AS cnt,
               ROUND(COUNT(*)*100.0/(
                   SELECT COUNT(*) FROM projects p2
                   JOIN classifications_vote cv2 ON p2.id=cv2.project_id
                   WHERE p2.repository_id=?),1) AS pct
        FROM projects p JOIN classifications_vote cv ON p.id=cv.project_id
        WHERE p.repository_id=?
        GROUP BY cv.section, cv.division
        ORDER BY cnt DESC LIMIT 20
    """, (repo_id, repo_id)).fetchall()


def get_confidence_stats(conn, repo_id):
    return conn.execute("""
        SELECT cv.confidence, COUNT(*)
        FROM projects p JOIN classifications_vote cv ON p.id=cv.project_id
        WHERE p.repository_id=?
        GROUP BY cv.confidence
        ORDER BY CASE cv.confidence
            WHEN 'very_high' THEN 1 WHEN 'high' THEN 2
            WHEN 'medium' THEN 3 ELSE 4 END
    """, (repo_id,)).fetchall()


def get_top_divisions(conn, repo_id, n=15):
    return conn.execute("""
        SELECT cv.section, cv.division,
               COALESCE(cv.division_name, cv.section_name,'') AS label,
               COUNT(*) AS cnt
        FROM projects p JOIN classifications_vote cv ON p.id=cv.project_id
        WHERE p.repository_id=?
          AND cv.division IS NOT NULL AND cv.division!=''
        GROUP BY cv.section, cv.division
        ORDER BY cnt DESC LIMIT ?
    """, (repo_id, n)).fetchall()


def get_file_stats(conn, repo_id):
    total = conn.execute("""
        SELECT COUNT(*) FROM classifications_files cf
        JOIN projects p ON p.id=cf.project_id WHERE p.repository_id=?
    """, (repo_id,)).fetchone()[0]
    if total == 0:
        return 0, [], []
    conf_rows = conn.execute("""
        SELECT cf.confidence, COUNT(*), ROUND(COUNT(*)*100.0/?,1)
        FROM classifications_files cf JOIN projects p ON p.id=cf.project_id
        WHERE p.repository_id=?
        GROUP BY cf.confidence
        ORDER BY CASE cf.confidence WHEN 'very_high' THEN 1 WHEN 'high' THEN 2
                 WHEN 'medium' THEN 3 ELSE 4 END
    """, (total, repo_id)).fetchall()
    top_rows = conn.execute("""
        SELECT cf.section, cf.section_name, COUNT(*), ROUND(COUNT(*)*100.0/?,1)
        FROM classifications_files cf JOIN projects p ON p.id=cf.project_id
        WHERE p.repository_id=?
        GROUP BY cf.section ORDER BY COUNT(*) DESC LIMIT 8
    """, (total, repo_id)).fetchall()
    return total, conf_rows, top_rows


# ── Chart generators ───────────────────────────────────────────────────────────

def make_histogram(section_counts, type_stats, conf_stats, repo_id, repo_name, path):
    total    = sum(c for _, _, c in section_counts)
    sec_dict = {s: c for s, _, c in section_counts}
    all_secs = sorted(ISIC_SECTIONS.keys())
    counts   = [sec_dict.get(s, 0) for s in all_secs]
    top3     = {s for s, _ in sorted(sec_dict.items(), key=lambda x: -x[1])[:3]}
    colors   = [BLUE_DARK if s in top3 else BLUE_LIGHT for s in all_secs]

    fig = plt.figure(figsize=(12, 5))
    fig.patch.set_facecolor("white")
    ax = fig.add_subplot(111)

    bars = ax.bar(range(len(all_secs)), counts, color=colors,
                  edgecolor="white", linewidth=0.4, width=0.75)
    max_c = max(counts) if counts else 1
    for bar, count, sec in zip(bars, counts, all_secs):
        if count > 0:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max_c * 0.012,
                    f"{count:,}", ha="center", va="bottom", fontsize=6,
                    fontweight="bold" if sec in top3 else "normal")

    ax.set_xlim(-0.6, len(all_secs) - 0.4)
    ax.set_ylim(0, max_c * 1.18)
    ax.set_xticks(range(len(all_secs)))
    ax.set_xticklabels([f"{s}\n{SHORT_LABELS.get(s,'')}" for s in all_secs], fontsize=7)
    ax.set_xlabel("ISIC Rev.5 Section", fontsize=10)
    ax.set_ylabel("Number of Projects", fontsize=10)
    ax.set_title(f"Primary ISIC Sections — Repository {repo_id}: {repo_name}", fontsize=11)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(handles=[
        Patch(facecolor=BLUE_DARK, label="Top-3"),
        Patch(facecolor=BLUE_LIGHT, label="Other"),
    ], fontsize=8)

    conf_text = "   ".join(f"{c}: {n:,}" for c, n in conf_stats)
    ax.annotate(f"Confidence — {conf_text}", xy=(0.01, 0.97),
                xycoords="axes fraction", fontsize=7.5, va="top", color="#555")

    fig_to_png_file(fig, path)


def make_division_chart(top_divs, section_counts, repo_id, repo_name, path):
    if not top_divs:
        return

    sec_colors = {
        "A":"#2E7D32","B":"#4E342E","C":"#1565C0","D":"#E65100","E":"#00695C",
        "F":"#6A1B9A","G":"#F57F17","H":"#0277BD","I":"#AD1457","J":"#558B2F",
        "K":"#1565C0","L":"#4527A0","M":"#37474F","N":"#00838F","O":"#827717",
        "P":"#283593","Q":"#BF360C","R":"#880E4F","S":"#2E7D32","T":"#4E342E",
        "U":"#37474F","V":"#263238",
    }
    labels  = [f"{d} {lbl[:35]}" for _, d, lbl, _ in top_divs]
    values  = [cnt for _, _, _, cnt in top_divs]
    colors  = [sec_colors.get(sec, BLUE_DARK) for sec, _, _, _ in top_divs]

    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor("white")
    y_pos = range(len(labels) - 1, -1, -1)
    bars = ax.barh(list(y_pos), values, color=colors, edgecolor="white", height=0.65)
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("Number of Projects", fontsize=10)
    ax.set_title(f"Top-{len(top_divs)} ISIC Divisions — Repository {repo_id}: {repo_name}", fontsize=11)
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=7.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.grid(True, linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    fig_to_png_file(fig, path)


# ── LaTeX builders ─────────────────────────────────────────────────────────────

PREAMBLE = r"""\documentclass[a4paper,11pt]{article}
\usepackage[T1]{fontenc}
\usepackage[utf8]{inputenc}
\usepackage{lmodern}
\usepackage[a4paper, margin=2.5cm]{geometry}
\usepackage{graphicx}
\usepackage{booktabs}
\usepackage{longtable}
\usepackage{array}
\usepackage{xcolor}
\usepackage{colortbl}
\usepackage{hyperref}
\usepackage{fancyhdr}
\usepackage{titlesec}
\usepackage{parskip}

\definecolor{bluedark}{RGB}{21,101,192}
\definecolor{bluerow}{RGB}{222,234,241}
\definecolor{gray}{RGB}{120,120,120}

\hypersetup{colorlinks=true, linkcolor=bluedark, urlcolor=bluedark}

\pagestyle{fancy}
\fancyhf{}
\renewcommand{\headrulewidth}{0.4pt}
\fancyhead[L]{\small\color{gray}SQ26 --- ISIC Rev.5 Classification Report}
\fancyhead[R]{\small\color{gray}Student ID: 23692652}
\fancyfoot[C]{\small\thepage}

\titleformat{\section}{\large\bfseries\color{bluedark}}{}{0em}{}[\titlerule]
\titleformat{\subsection}{\normalsize\bfseries\color{bluedark}}{}{0em}{}

\begin{document}

%% ── Cover page ──────────────────────────────────────────────────────────────
\begin{titlepage}
\centering
\vspace*{3cm}
{\Huge\bfseries\color{bluedark} SQ26 --- ISIC Rev.5\\[0.4em]Classification Report}\\[1.5em]
{\large Part 2: Project Classification by Economic Sector}\\[2em]
\rule{0.5\textwidth}{0.4pt}\\[1.5em]
{\large Student ID:} \textbf{23692652}\\[0.5em]
{\large University:} FAU Erlangen-N\"urnberg\\[0.5em]
{\large Supervisor:} Prof.\ Dirk Riehle\\[0.5em]
{\large Date:} July 2026\\[2em]
\rule{0.5\textwidth}{0.4pt}\\[1.5em]
{\normalsize\color{gray}
53{,}080 research dataset projects classified\\
across 3 repositories (Zenodo, Harvard Dataverse, Columbia)\\
using ISIC Rev.5 (UN 2025) via a 4-classifier voting ensemble
}
\end{titlepage}

\tableofcontents
\newpage
"""

CLOSING = r"\end{document}" + "\n"


def repo_section(conn, repo_id: int, img_dir: str) -> str:
    repo_name      = REPO_NAMES.get(repo_id, f"Repository {repo_id}")
    section_counts = get_section_counts(conn, repo_id)
    type_stats     = get_type_stats(conn, repo_id)
    top20          = get_top20(conn, repo_id)
    conf_stats     = get_confidence_stats(conn, repo_id)
    top_divs       = get_top_divisions(conn, repo_id, n=15)
    total          = sum(c for _, _, c in section_counts)
    file_total, file_conf, file_top_secs = get_file_stats(conn, repo_id)

    hist_path = f"{img_dir}/hist_repo{repo_id}.png"
    div_path  = f"{img_dir}/div_repo{repo_id}.png"
    make_histogram(section_counts, type_stats, conf_stats, repo_id, repo_name, hist_path)
    make_division_chart(top_divs, section_counts, repo_id, repo_name, div_path)

    lines: list[str] = []
    a = lines.append

    a(f"\\section{{Repository {repo_id}: {esc(repo_name)}}}")
    a(f"\\textbf{{Total projects:}} {total:,}\\quad")

    type_parts = ",  ".join(f"\\texttt{{{esc(t)}}}: {c:,}" for t, c in type_stats)
    a(f"\\textbf{{Project types:}} {type_parts}\\par")
    a("")

    # Confidence
    conf_parts = ",  ".join(f"{esc(c)}: {n:,}" for c, n in conf_stats)
    a(f"\\textbf{{Confidence:}} {conf_parts}\\par")
    a("")

    # Histogram
    a("\\subsection{Primary ISIC Section Distribution}")
    a(f"\\includegraphics[width=\\textwidth]{{{hist_path}}}")
    a("")

    # Section table
    a("\\subsection{Section Summary}")
    a("\\begin{center}")
    a("\\begin{tabular}{llrr}")
    a("\\toprule")
    a("\\textbf{Sec} & \\textbf{Name} & \\textbf{Projects} & \\textbf{\\%} \\\\")
    a("\\midrule")
    for sec, sec_name, cnt in section_counts:
        pct = round(cnt * 100.0 / total, 1) if total else 0
        a(f"{esc(sec)} & {esc((sec_name or '')[:52])} & {cnt:,} & {pct}\\% \\\\")
    a("\\bottomrule")
    a("\\end{tabular}")
    a("\\end{center}")
    a("")

    # Top-20 table
    a("\\subsection{Top-20 ISIC Classes (Section + Division)}")
    a("\\begin{center}")
    a("\\begin{tabular}{llp{7cm}rr}")
    a("\\toprule")
    a("\\textbf{Sec} & \\textbf{Div} & \\textbf{Division Name} & \\textbf{N} & \\textbf{\\%} \\\\")
    a("\\midrule")
    for i, (sec, div, div_name, cnt, pct) in enumerate(top20):
        row_color = "\\rowcolor{bluerow}" if i % 2 == 0 else ""
        a(f"{row_color}{esc(sec)} & {esc(div)} & {esc((div_name or '')[:60])} & {cnt:,} & {pct}\\% \\\\")
    a("\\bottomrule")
    a("\\end{tabular}")
    a("\\end{center}")
    a("")

    # Division bar chart
    if top_divs:
        a("\\subsection{Top-15 ISIC Divisions}")
        a(f"\\includegraphics[width=\\textwidth]{{{div_path}}}")
        a("")

    # File-level classification
    if file_total > 0:
        a("\\subsection{File-Level ISIC Classification}")
        a(f"Individual files within \\texttt{{QDA\\_PROJECT}} and \\texttt{{QD\\_PROJECT}} "
          f"were classified using the fine-tuned \\texttt{{bert-base-uncased}} model. "
          f"Repository {repo_id} contributed {file_total:,} classified files.\\par")
        a("")
        a("\\begin{center}")
        a("\\begin{tabular}{lrr}")
        a("\\toprule")
        a("\\textbf{Confidence} & \\textbf{Files} & \\textbf{\\%} \\\\")
        a("\\midrule")
        for conf, n, pct in file_conf:
            a(f"{esc(conf)} & {n:,} & {pct}\\% \\\\")
        a("\\bottomrule")
        a("\\end{tabular}")
        a("\\end{center}")
        a("")
        a("\\begin{center}")
        a("\\begin{tabular}{llrr}")
        a("\\toprule")
        a("\\textbf{Sec} & \\textbf{Name} & \\textbf{Files} & \\textbf{\\%} \\\\")
        a("\\midrule")
        for sec, sec_name, n, pct in file_top_secs:
            a(f"{esc(sec)} & {esc((sec_name or '')[:52])} & {n:,} & {pct}\\% \\\\")
        a("\\bottomrule")
        a("\\end{tabular}")
        a("\\end{center}")

    a("\\newpage")
    a("")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db",  default=DB)
    ap.add_argument("--out", default=OUT)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    repo_ids = [r[0] for r in conn.execute(
        "SELECT DISTINCT repository_id FROM projects ORDER BY repository_id"
    ).fetchall()]

    # Images go next to the .tex file so Overleaf can find them
    img_dir = "report_images"
    Path(img_dir).mkdir(exist_ok=True)

    print(f"Building LaTeX report for repos {repo_ids}…")

    body_parts = [PREAMBLE]
    for rid in repo_ids:
        print(f"  repo {rid}…")
        body_parts.append(repo_section(conn, rid, img_dir))
    body_parts.append(CLOSING)

    tex = "\n".join(body_parts)
    Path(args.out).write_text(tex, encoding="utf-8")
    print(f"Saved: {args.out}")
    print(f"Images: {img_dir}/")
    print("\nTo compile on Overleaf:")
    print("  1. Create a new project → upload .tex + all files in report_images/")
    print("  2. Compile with pdflatex")

    conn.close()


if __name__ == "__main__":
    main()
