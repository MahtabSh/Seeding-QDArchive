"""
CLI entry point for the QDArchive Seeding Pipeline.

Usage:
  python qdarchive_pipeline.py                          # all sources, metadata only
  python qdarchive_pipeline.py --sources harvard        # Harvard only
  python qdarchive_pipeline.py --sources columbia       # Columbia only
  python qdarchive_pipeline.py --download               # also fetch files to disk
  python qdarchive_pipeline.py --max-records 1000 \\
      --output-dir /data/qda --db /data/qda/metadata.sqlite
"""
import argparse
import logging
from pathlib import Path

from qdarchive.crawlers import CRAWLERS, ALL_SOURCES
from qdarchive.db import MetadataDB
from qdarchive.progress import ProgressState


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("pipeline.log"),
        ],
    )
    log = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="QDArchive Seeding Pipeline v7 — Harvard Dataverse, Zenodo, Columbia Oral History Archive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python qdarchive_pipeline.py
  python qdarchive_pipeline.py --sources harvard columbia
  python qdarchive_pipeline.py --download
  python qdarchive_pipeline.py --max-records 1000 --download \\
      --output-dir /data/qdarchive --db /data/qdarchive/metadata.sqlite
        """,
    )
    parser.add_argument(
        "--output-dir", default="./archive",
        help="Root directory for downloaded files (default: ./archive)",
    )
    parser.add_argument(
        "--db", default="./metadata.sqlite",
        help="SQLite database path (default: ./metadata.sqlite)",
    )
    parser.add_argument(
        "--max-records", type=int, default=0,
        help="Max records to collect per source (0 = unlimited, default: unlimited)",
    )
    parser.add_argument(
        "--download", action="store_true", default=False,
        help="Download files to disk. Without this flag only metadata is saved.",
    )
    parser.add_argument(
        "--extensions", nargs="+", default=None, metavar="EXT",
        help=(
            "Whitelist of file extensions to download (e.g. .qdpx .nvp .pdf). "
            "Files with other extensions are recorded in the DB with status "
            "SUCCEEDED but not saved to disk. Only applies when --download is set."
        ),
    )
    parser.add_argument(
        "--max-file-size", default=None, metavar="SIZE",
        help=(
            "Skip downloading files larger than this size. "
            "Accepts bytes (10485760), KB (10240KB), or MB (50MB). "
            "Skipped files are recorded with status FAILED_TOO_LARGE. "
            "Only applies when --download is set."
        ),
    )
    parser.add_argument(
        "--gdrive", action="store_true", default=False,
        help="Upload downloaded files to Google Drive after crawling. Keeps local copies.",
    )
    parser.add_argument(
        "--gdrive-only", action="store_true", default=False,
        help="Upload to Google Drive and DELETE local files after upload. No files stay on disk.",
    )
    parser.add_argument(
        "--sources", nargs="+", choices=ALL_SOURCES, default=ALL_SOURCES,
        help=f"Which repositories to crawl (default: all). Choices: {', '.join(ALL_SOURCES)}",
    )
    args = parser.parse_args()

    # Parse --max-file-size into bytes
    max_file_size_bytes: int | None = None
    if args.max_file_size:
        raw = args.max_file_size.strip().upper()
        if raw.endswith("MB"):
            max_file_size_bytes = int(float(raw[:-2]) * 1024 * 1024)
        elif raw.endswith("KB"):
            max_file_size_bytes = int(float(raw[:-2]) * 1024)
        else:
            max_file_size_bytes = int(raw)
        log.info(f"Max file size: {max_file_size_bytes:,} bytes ({args.max_file_size})")

    # Normalise extensions to lowercase with leading dot
    allowed_extensions: set[str] | None = None
    if args.extensions:
        allowed_extensions = {
            e.lower() if e.startswith(".") else f".{e.lower()}"
            for e in args.extensions
        }
        log.info(f"Extension whitelist: {sorted(allowed_extensions)}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    db = MetadataDB(Path(args.db))

    # Use a per-source progress file so two processes running different sources
    # concurrently (e.g. --sources harvard / --sources zenodo) never overwrite
    # each other's pagination state.
    sources_key  = "_".join(sorted(args.sources))
    progress_path = Path(args.db).with_suffix(f".{sources_key}.progress.json")
    progress = ProgressState(progress_path)

    use_gdrive   = args.gdrive or args.gdrive_only

    # ── Initialise Google Drive service once (shared across all crawlers) ─────
    gdrive_service      = None
    gdrive_root_id      = None
    gdrive_folder_cache = {}

    if use_gdrive and args.download:
        from qdarchive.gdrive import (
            get_drive_service, _get_or_create_folder,
            make_public, folder_url, _migrate_db,
        )
        log.info("GDrive: initialising Drive service...")
        gdrive_service = get_drive_service()
        _migrate_db(db)
        gdrive_root_id = _get_or_create_folder(gdrive_service, "QDArchive")
        make_public(gdrive_service, gdrive_root_id)
        log.info(f"GDrive: public root → {folder_url(gdrive_root_id)}")
    elif use_gdrive:
        log.warning("GDrive: --gdrive / --gdrive-only have no effect without --download")

    log.info(f"Pipeline start | sources={args.sources} | max_records={args.max_records} | download={args.download} | gdrive={use_gdrive}")

    for source in args.sources:
        crawler = CRAWLERS[source]
        try:
            crawler(
                output_dir          = output_dir,
                db                  = db,
                max_records         = args.max_records,
                progress            = progress,
                download            = args.download,
                allowed_extensions  = allowed_extensions,
                max_file_size_bytes = max_file_size_bytes,
                gdrive_service      = gdrive_service,
                gdrive_folder_cache = gdrive_folder_cache,
                gdrive_root_id      = gdrive_root_id,
            )
        except Exception as e:
            log.error(f"{source} crawler crashed: {e}", exc_info=True)

    # --gdrive (without --gdrive-only): upload any remaining local files after crawl
    if args.gdrive and not args.gdrive_only and args.download:
        log.info("GDrive: uploading any remaining local files...")
        from qdarchive.gdrive import upload_archive
        upload_archive(db, output_dir, delete_after=False)

    # --gdrive-only: also clean up any leftover local files from this run
    if args.gdrive_only and args.download:
        from qdarchive.gdrive import upload_archive
        upload_archive(db, output_dir, delete_after=True)

    db.close()
    log.info(f"Pipeline complete. Database -> {args.db}")
