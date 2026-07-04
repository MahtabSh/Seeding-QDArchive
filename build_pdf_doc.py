#!/usr/bin/env python3
"""Convert 23692652-sq26-classification-methodology.md to a styled PDF document."""

import re
import textwrap
from pathlib import Path

import matplotlib
matplotlib.use("PDF")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import FancyBboxPatch
import matplotlib.gridspec as gridspec

SRC = "23692652-sq26-classification-methodology.md"
OUT = "23692652-sq26-classification-methodology.pdf"

# A4 portrait in inches
W, H = 8.27, 11.69

MARGIN_L = 0.95  # inches from left
MARGIN_R = 0.95
MARGIN_T = 0.85
MARGIN_B = 0.70

TEXT_W   = W - MARGIN_L - MARGIN_R   # usable text width in inches
FONT     = "DejaVu Sans"
MONO     = "DejaVu Sans Mono"

C_DARK   = "#1F4E79"
C_MID    = "#2763A0"
C_LIGHT  = "#DEEAF1"
C_ALT    = "#EBF3FA"
C_BODY   = "#111111"
C_GRAY   = "#666666"
C_CODE   = "#B71C1C"
C_LINE   = "#CCCCCC"


# ── parser ─────────────────────────────────────────────────────────────────────

def parse_md(text: str):
    """Yield (kind, content) tokens."""
    lines   = text.splitlines()
    i, n    = 0, len(lines)
    in_code = False
    code_buf = []

    while i < n:
        line = lines[i]

        if line.strip().startswith("```"):
            if not in_code:
                in_code, code_buf = True, []
            else:
                yield ("code", "\n".join(code_buf))
                in_code = False
            i += 1
            continue
        if in_code:
            code_buf.append(line)
            i += 1
            continue

        if re.match(r"^---+$", line.strip()):
            yield ("hr", "")
            i += 1
            continue

        m = re.match(r"^(#{1,3})\s+(.*)", line)
        if m:
            yield (f"h{len(m.group(1))}", m.group(2).strip())
            i += 1
            continue

        if line.strip().startswith("|"):
            tbl = []
            while i < n and lines[i].strip().startswith("|"):
                tbl.append(lines[i])
                i += 1
            def parse_row(r):
                return [c.strip() for c in r.strip().strip("|").split("|")]
            header = parse_row(tbl[0])
            data   = [parse_row(r) for r in tbl[2:]]
            yield ("table", (header, data))
            continue

        bm = re.match(r"^(\s*)[-*]\s+(.*)", line)
        if bm:
            level = len(bm.group(1)) // 2
            yield ("bullet", (level, bm.group(2)))
            i += 1
            continue

        if not line.strip():
            i += 1
            continue

        yield ("body", line)
        i += 1


# ── page renderer ──────────────────────────────────────────────────────────────

class PageWriter:
    def __init__(self, pdf: PdfPages):
        self.pdf  = pdf
        self._new_page()

    def _new_page(self):
        self.fig, self.ax = plt.subplots(figsize=(W, H))
        self.fig.patch.set_facecolor("white")
        self.ax.set_xlim(0, W)
        self.ax.set_ylim(0, H)
        self.ax.axis("off")
        self.y = H - MARGIN_T   # current y position (descending)
        self._draw_footer()

    def _draw_footer(self):
        self.ax.axhline(y=MARGIN_B - 0.05, xmin=MARGIN_L/W, xmax=(W-MARGIN_R)/W,
                        color=C_LINE, linewidth=0.6)
        self.ax.text(W/2, MARGIN_B - 0.22,
                     "Student ID: 23692652  ·  FAU Erlangen-Nürnberg  ·  SQ26 — ISIC Rev.5 Classification",
                     ha="center", va="top", fontsize=7.5, color="#AAAAAA",
                     fontfamily=FONT)

    def _flush(self):
        self.pdf.savefig(self.fig, bbox_inches="tight")
        plt.close(self.fig)

    def _need(self, height: float) -> bool:
        """Return True if we need a new page for `height` inches."""
        return self.y - height < MARGIN_B + 0.1

    def _new_if_needed(self, height: float):
        if self._need(height):
            self._flush()
            self._new_page()

    # ── rendering primitives ──────────────────────────────────────────────────

    def draw_h1(self, text: str):
        self._new_if_needed(0.55)
        self.y -= 0.12
        self.ax.text(MARGIN_L, self.y, text,
                     fontsize=17, fontweight="bold", color=C_MID,
                     va="top", fontfamily=FONT)
        self.y -= 0.32
        self.ax.axhline(y=self.y, xmin=MARGIN_L/W, xmax=(W-MARGIN_R)/W,
                        color=C_MID, linewidth=1.2)
        self.y -= 0.06

    def draw_h2(self, text: str):
        self._new_if_needed(0.40)
        self.y -= 0.10
        self.ax.text(MARGIN_L, self.y, text,
                     fontsize=13.5, fontweight="bold", color=C_DARK,
                     va="top", fontfamily=FONT)
        self.y -= 0.30

    def draw_h3(self, text: str):
        self._new_if_needed(0.35)
        self.y -= 0.06
        self.ax.text(MARGIN_L, self.y, text,
                     fontsize=11, fontweight="bold", color="#37474F",
                     va="top", fontfamily=FONT)
        self.y -= 0.24

    def _strip_inline(self, text: str) -> str:
        """Remove markdown bold/code markers for plain-text rendering."""
        text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
        text = re.sub(r"`([^`]+)`", r"\1", text)
        return text

    def draw_body(self, text: str):
        plain = self._strip_inline(text)
        wrapped = textwrap.wrap(plain, width=90)
        for line in wrapped:
            self._new_if_needed(0.20)
            self.ax.text(MARGIN_L, self.y, line,
                         fontsize=9.5, color=C_BODY, va="top", fontfamily=FONT)
            self.y -= 0.185
        self.y -= 0.04

    def draw_bullet(self, level: int, text: str):
        plain  = self._strip_inline(text)
        indent = MARGIN_L + 0.18 + level * 0.25
        marker_x = indent - 0.14
        wrapped = textwrap.wrap(plain, width=85 - level * 3)
        for k, line in enumerate(wrapped):
            self._new_if_needed(0.20)
            if k == 0:
                self.ax.text(marker_x, self.y, "•",
                             fontsize=9.5, color=C_MID, va="top", fontfamily=FONT)
            self.ax.text(indent, self.y, line,
                         fontsize=9.5, color=C_BODY, va="top", fontfamily=FONT)
            self.y -= 0.185
        self.y -= 0.03

    def draw_code(self, code: str):
        lines = code.split("\n")
        h = len(lines) * 0.165 + 0.14
        self._new_if_needed(h)
        self.y -= 0.04
        # grey background box
        rect = FancyBboxPatch((MARGIN_L - 0.05, self.y - h + 0.05),
                               TEXT_W + 0.1, h,
                               boxstyle="square,pad=0", linewidth=0.5,
                               edgecolor="#CCCCCC", facecolor="#F5F5F5")
        self.ax.add_patch(rect)
        cy = self.y - 0.04
        for line in lines:
            self.ax.text(MARGIN_L + 0.05, cy, line,
                         fontsize=8, color="#37474F", va="top",
                         fontfamily=MONO)
            cy -= 0.165
        self.y -= h + 0.08

    def draw_hr(self):
        self.y -= 0.08
        self.ax.axhline(y=self.y, xmin=MARGIN_L/W, xmax=(W-MARGIN_R)/W,
                        color=C_LINE, linewidth=0.6)
        self.y -= 0.10

    def draw_table(self, header: list[str], rows: list[list[str]]):
        ncols = len(header)
        # Estimate column widths (equal split, adjusted for content)
        col_w = TEXT_W / ncols

        row_h   = 0.20
        hdr_h   = 0.22
        total_h = hdr_h + len(rows) * row_h + 0.08

        # If table is very tall, split across pages
        chunk_size = max(1, int((self.y - MARGIN_B - 0.3 - hdr_h) / row_h))
        chunks     = [rows[j:j+chunk_size] for j in range(0, len(rows), chunk_size)]

        for c_idx, chunk in enumerate(chunks):
            if c_idx > 0:
                self._flush()
                self._new_page()

            self._new_if_needed(hdr_h + len(chunk) * row_h + 0.08)
            self.y -= 0.06
            x0 = MARGIN_L

            # Header row
            for ci, htext in enumerate(header):
                cx = x0 + ci * col_w
                rect = FancyBboxPatch((cx, self.y - hdr_h), col_w, hdr_h,
                                      boxstyle="square,pad=0", linewidth=0,
                                      facecolor=C_DARK)
                self.ax.add_patch(rect)
                self.ax.text(cx + 0.06, self.y - 0.04, htext,
                             fontsize=8, fontweight="bold", color="white",
                             va="top", fontfamily=FONT,
                             clip_on=True)
            self.y -= hdr_h

            # Data rows
            for ri, row in enumerate(chunk):
                bg = C_ALT if ri % 2 == 0 else "white"
                for ci, cell_text in enumerate(row):
                    cx = x0 + ci * col_w
                    rect = FancyBboxPatch((cx, self.y - row_h), col_w, row_h,
                                          boxstyle="square,pad=0",
                                          edgecolor=C_LINE, facecolor=bg,
                                          linewidth=0.3)
                    self.ax.add_patch(rect)
                    display = self._strip_inline(cell_text)
                    # Truncate long text to fit cell
                    max_chars = max(6, int(col_w / 0.065))
                    if len(display) > max_chars:
                        display = display[:max_chars-1] + "…"
                    self.ax.text(cx + 0.06, self.y - 0.04, display,
                                 fontsize=7.8, color=C_BODY, va="top",
                                 fontfamily=FONT, clip_on=True)
                self.y -= row_h

            self.y -= 0.12

    def finish(self):
        self._flush()


# ── title page ─────────────────────────────────────────────────────────────────

def title_page(pdf: PdfPages, meta_lines: list[str]):
    fig, ax = plt.subplots(figsize=(W, H))
    fig.patch.set_facecolor("white")
    ax.set_xlim(0, W); ax.set_ylim(0, H); ax.axis("off")

    # Top colour band
    rect = FancyBboxPatch((0, H - 2.2), W, 2.2,
                           boxstyle="square,pad=0", linewidth=0,
                           facecolor=C_DARK)
    ax.add_patch(rect)

    ax.text(W/2, H - 0.55,
            "SQ26 — ISIC Rev.5 Classification",
            ha="center", va="top", fontsize=22, fontweight="bold",
            color="white", fontfamily=FONT)
    ax.text(W/2, H - 1.10,
            "of QDArchive Research Dataset Projects",
            ha="center", va="top", fontsize=16, color="#BBDEFB", fontfamily=FONT)
    ax.text(W/2, H - 1.62,
            "Methodology Report · Part 2",
            ha="center", va="top", fontsize=13, color="#90CAF9", fontfamily=FONT)

    # Meta block
    by = H - 2.8
    for line in meta_lines:
        plain = re.sub(r"\*\*([^*]+)\*\*", r"\1", line)
        ax.text(W/2, by, plain,
                ha="center", va="top", fontsize=11, color=C_BODY, fontfamily=FONT)
        by -= 0.30

    ax.axhline(y=by - 0.1, xmin=0.2, xmax=0.8, color=C_LINE, linewidth=0.8)

    ax.text(W/2, 0.45,
            "FAU Erlangen-Nürnberg  ·  Student ID: 23692652  ·  July 2026",
            ha="center", va="top", fontsize=9, color="#AAAAAA", fontfamily=FONT)

    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    text   = Path(SRC).read_text(encoding="utf-8")
    tokens = list(parse_md(text))

    with PdfPages(OUT) as pdf:
        # Collect title-page metadata (bold **Key:** Value lines near the top)
        meta = []
        body_start = 0
        for idx, (kind, content) in enumerate(tokens):
            if kind == "h1":
                body_start = idx
                break
            if kind == "body" and content.strip().startswith("**"):
                meta.append(content)

        title_page(pdf, meta)

        pw = PageWriter(pdf)
        for kind, content in tokens[body_start:]:
            if kind == "h1":
                pw.draw_h1(content)
            elif kind == "h2":
                pw.draw_h2(content)
            elif kind == "h3":
                pw.draw_h3(content)
            elif kind == "body":
                pw.draw_body(content)
            elif kind == "bullet":
                level, text = content
                pw.draw_bullet(level, text)
            elif kind == "code":
                pw.draw_code(content)
            elif kind == "hr":
                pw.draw_hr()
            elif kind == "table":
                header, rows = content
                pw.draw_table(header, rows)

        pw.finish()

    print(f"Saved → {OUT}")


if __name__ == "__main__":
    main()
