#!/usr/bin/env python3
"""
Part 2: Project Classification
================================
Adds a `type` column (PROJECT_TYPE) to the `projects` table and classifies
each project based on the file extensions found in `files`.

Priority order (first match wins):
  QDA_PROJECT   — has at least one QDA software project file
  QD_PROJECT    — has at least one qualitative primary-data file (audio/video/transcript)
  OTHER_PROJECT — has at least one recognized data-file extension
  NOT_A_PROJECT — no files, or only files with unrecognized/empty extensions

Usage:
  python classify_projects.py [--db PATH] [--dry-run]
"""

import argparse
import sqlite3
from pathlib import Path

# ── Extension sets ────────────────────────────────────────────────────────────

# QDA software project/workspace files (proprietary formats)
QDA_EXTENSIONS = {
    # ATLAS.ti
    ".atlasti", ".atlpac", ".atlproj", ".atlproj23", ".atlproj9",
    # MAXQDA
    ".mex", ".mx3", ".mx18", ".mx20", ".mx22", ".mx24", ".mqda",
    # NVivo
    ".nvp", ".nvpx",
    # REFI-QDA open exchange format
    ".qdpx",
    # RQDA (R package, deprecated)
    ".rqda",
    # QDA Miner
    ".qdc", ".qdp",
    # f4transkript / f4analyse
    ".f4", ".f4a", ".f4v",
}

# Raw qualitative primary data: recordings, transcripts, images as data
PRIMARY_DATA_EXTENSIONS = {
    # Audio recordings
    ".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".wma",
    ".aiff", ".aif", ".mp2", ".mpa", ".ra", ".au",
    # Video recordings
    ".mp4", ".avi", ".mov", ".wmv", ".mkv", ".m4v",
    ".mpg", ".mpeg", ".m2ts", ".mts", ".flv", ".3gp", ".webm",
    # Text transcripts / qualitative documents
    ".txt", ".doc", ".docx", ".rtf", ".odt",
    # PDF (primary documents, interview reports, field notes)
    ".pdf",
    # ELAN qualitative annotation format
    ".eaf",
    # Images (visual primary data)
    ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".gif",
}

# Any recognized data-file format (broad; used for OTHER_PROJECT fallback)
OTHER_DATA_EXTENSIONS = {
    # Tabular / statistical
    ".csv", ".tsv", ".tab", ".xls", ".xlsx", ".xlsm", ".xlsb", ".xltx",
    ".ods", ".ots",
    ".sav", ".dta", ".por", ".sas7bdat", ".jnb",
    ".do", ".sps", ".spssx", ".ado", ".prg", ".smcl", ".spv",
    # R data
    ".rds", ".rda", ".rdata", ".rproj", ".r", ".rmd",
    # Scientific / structured data
    ".json", ".jsonl", ".jsonld", ".xml", ".yaml", ".yml",
    ".mat", ".nc", ".nc4", ".h5", ".hdf5", ".hdf",
    ".parquet", ".feather", ".zarr",
    # Databases
    ".db", ".sqlite", ".sqlite3", ".duckdb", ".mdb",
    # Geospatial
    ".shp", ".dbf", ".shx", ".gpkg", ".geojson", ".kmz",
    ".tif",  # also GeoTIFF
    # Code / scripts
    ".py", ".m", ".f90", ".f", ".cpp", ".c", ".h", ".cs",
    ".js", ".ts", ".java", ".rb", ".pl", ".sh", ".ps1",
    ".ipynb", ".nb", ".mlx", ".sage",
    # Archives / compressed
    ".zip", ".gz", ".tar", ".7z", ".rar", ".tgz", ".bz2", ".xz", ".zst",
    # Documents
    ".pptx", ".ppt", ".bib", ".ris", ".epub", ".mobi",
    ".tex", ".rst", ".md", ".markdown", ".html", ".htm",
    # Other common data formats
    ".npy", ".npz", ".pkl", ".pickle", ".bin", ".dat", ".data",
    ".vcf", ".fasta", ".fa", ".faa", ".fastq", ".gtf", ".bed",
    ".bam", ".sam", ".vcf", ".pdb", ".mol2",
    ".svg", ".eps", ".ai", ".ps",
    ".las", ".laz",  # LiDAR
    ".jasp",  # JASP stats
    ".pzfx",  # Prism
    ".opj", ".opju",  # Origin
    ".sas",
    ".edat2",  # E-Prime
    ".fcs",  # Flow cytometry
    ".ndpi", ".lif", ".czi",  # Microscopy
    ".owl", ".ttl", ".nq", ".sparql", ".trig",  # Semantic web
    ".dta", ".log",
    ".whl", ".conda",  # Python packages (still data artifacts)
    ".dxf", ".gpkg",
    ".stan",  # Stan statistical model
    ".r",
}

# Merge all known extensions (for NOT_A_PROJECT detection)
ALL_KNOWN_EXTENSIONS = QDA_EXTENSIONS | PRIMARY_DATA_EXTENSIONS | OTHER_DATA_EXTENSIONS


def _placeholders(items):
    return ", ".join("?" * len(items))


def classify(db_path: Path, dry_run: bool = False) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # ── 1. Add column if missing ──────────────────────────────────────────────
    existing = {row[1] for row in conn.execute("PRAGMA table_info(projects)")}
    if "type" not in existing:
        print("Adding 'type' column to projects table …")
        conn.execute("ALTER TABLE projects ADD COLUMN type TEXT")
        conn.commit()
    else:
        print("Column 'type' already exists — will overwrite existing values.")
        conn.execute("UPDATE projects SET type = NULL")
        conn.commit()

    # ── 2. Build SQL IN-lists ─────────────────────────────────────────────────
    qda_list  = sorted(QDA_EXTENSIONS)
    prim_list = sorted(PRIMARY_DATA_EXTENSIONS)
    # OTHER uses the full known set minus QDA/primary (already handled above)
    other_list = sorted(OTHER_DATA_EXTENSIONS | PRIMARY_DATA_EXTENSIONS | QDA_EXTENSIONS)

    # ── 3. QDA_PROJECT ────────────────────────────────────────────────────────
    print("Classifying QDA_PROJECT …")
    conn.execute(f"""
        UPDATE projects SET type = 'QDA_PROJECT'
        WHERE id IN (
            SELECT DISTINCT project_id FROM files
            WHERE LOWER(file_type) IN ({_placeholders(qda_list)})
        )
    """, qda_list)
    conn.commit()

    # ── 4. QD_PROJECT ─────────────────────────────────────────────────────────
    print("Classifying QD_PROJECT …")
    conn.execute(f"""
        UPDATE projects SET type = 'QD_PROJECT'
        WHERE type IS NULL
          AND id IN (
              SELECT DISTINCT project_id FROM files
              WHERE LOWER(file_type) IN ({_placeholders(prim_list)})
          )
    """, prim_list)
    conn.commit()

    # ── 5. OTHER_PROJECT ──────────────────────────────────────────────────────
    print("Classifying OTHER_PROJECT …")
    conn.execute(f"""
        UPDATE projects SET type = 'OTHER_PROJECT'
        WHERE type IS NULL
          AND id IN (
              SELECT DISTINCT project_id FROM files
              WHERE file_type IS NOT NULL
                AND file_type != ''
                AND LOWER(file_type) IN ({_placeholders(other_list)})
          )
    """, other_list)
    conn.commit()

    # ── 6. NOT_A_PROJECT ──────────────────────────────────────────────────────
    print("Classifying NOT_A_PROJECT …")
    conn.execute("UPDATE projects SET type = 'NOT_A_PROJECT' WHERE type IS NULL")
    conn.commit()

    # ── 7. Report ─────────────────────────────────────────────────────────────
    print("\n=== Classification results ===")
    for row in conn.execute("""
        SELECT type, COUNT(*) AS cnt
        FROM projects
        GROUP BY type
        ORDER BY cnt DESC
    """):
        print(f"  {row['type']:<20} {row['cnt']:>6} projects")

    total = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    print(f"\n  {'TOTAL':<20} {total:>6} projects")

    if dry_run:
        print("\n[dry-run] Rolling back …")
        conn.rollback()

    conn.close()
    print("\nDone.")


def main():
    parser = argparse.ArgumentParser(description="Classify projects by file type.")
    parser.add_argument(
        "--db", default="metadata.sqlite",
        help="Path to the SQLite database (default: metadata.sqlite)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would happen without writing to the database",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: database not found: {db_path}")
        raise SystemExit(1)

    classify(db_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
