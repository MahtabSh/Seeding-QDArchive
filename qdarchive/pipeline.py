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
        "--sources", nargs="+", choices=ALL_SOURCES, default=ALL_SOURCES,
        help=f"Which repositories to crawl (default: all). Choices: {', '.join(ALL_SOURCES)}",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    db = MetadataDB(Path(args.db))

    # Use a per-source progress file so two processes running different sources
    # concurrently (e.g. --sources harvard / --sources zenodo) never overwrite
    # each other's pagination state.
    sources_key  = "_".join(sorted(args.sources))
    progress_path = Path(args.db).with_suffix(f".{sources_key}.progress.json")
    progress = ProgressState(progress_path)

    log.info(f"Pipeline start | sources={args.sources} | max_records={args.max_records} | download={args.download}")

    for source in args.sources:
        crawler = CRAWLERS[source]
        try:
            crawler(
                output_dir  = output_dir,
                db          = db,
                max_records = args.max_records,
                progress    = progress,
                download    = args.download,
            )
        except Exception as e:
            log.error(f"{source} crawler crashed: {e}", exc_info=True)

    db.close()
    log.info(f"Pipeline complete. Database -> {args.db}")
