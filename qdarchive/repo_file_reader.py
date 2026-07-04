"""
Repository file reader for QDArchive classification.

Downloads text snippets directly from Zenodo and Dataverse APIs for
projects that don't have (or can't use) a Google Drive folder.

Supported sources
-----------------
  Zenodo              zenodo.org
  Harvard Dataverse   dataverse.harvard.edu  (and other Dataverse instances:
                      data.qdr.syr.edu, data.worldagroforestry.org, …)

The reader consults the local `files` table to know which files exist for
each project (so we avoid a listing API call for Zenodo).  For Dataverse
we need one metadata call to retrieve internal file IDs, then download.

Cache: .repo_file_cache/  (separate from Drive's .file_cache/)

Usage
-----
  from qdarchive.repo_file_reader import RepoFileReader

  reader = RepoFileReader(db_path="23692652-sq26.db")
  snippets = reader.get_snippets(
      project_id=3,
      doi="https://doi.org/10.5281/zenodo.7814638",
      repo_url="https://zenodo.org",
  )
  # {"README.txt": "This dataset contains…", …}
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from pathlib import Path

import requests

from qdarchive.file_reader import (
    MAX_FILE_BYTES,
    MAX_FILE_BYTES_QDA,
    MAX_FILES_PER_PROJECT,
    SNIPPET_CHARS,
    _EXTRACTORS,
    _QDA_ZIP_EXTS,
    _file_priority,
)

log = logging.getLogger(__name__)

TIMEOUT          = 30    # seconds per HTTP request
MAX_RETRIES      = 3
RETRY_WAIT       = 5     # seconds between retries on transient errors
RATE_LIMIT_WAIT  = 60    # seconds to wait after HTTP 429

_READABLE_EXTS = (
    ".txt", ".pdf", ".docx", ".doc", ".csv", ".tab", ".tsv",
    ".atlproj", ".atlproj23", ".atlproj9",
    ".nvpx", ".qdpx",
    ".mex", ".mx18", ".mx20", ".mx22", ".mx24", ".mqda",
    ".f4", ".f4a",
)


class RepoFileReader:
    """
    Downloads and caches file snippets from Zenodo / Dataverse APIs.

    Interface mirrors FileContentReader so classify_combined.py can
    use either interchangeably.
    """

    def __init__(
        self,
        db_path: str | Path,
        cache_dir: Path = Path(".repo_file_cache"),
    ):
        self.db_path   = Path(db_path)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

        self._session = requests.Session()
        self._session.headers["User-Agent"] = "QDArchive-Classifier/1.0 (research)"
        log.info("RepoFileReader ready.")

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_snippets(
        self,
        project_id: int,
        doi: str | None,
        repo_url: str | None,
        project_url: str | None = None,
    ) -> dict[str, str]:
        """
        Return {filename: snippet} for up to MAX_FILES_PER_PROJECT readable files.
        Returns {} when nothing is downloadable (no DOI, restricted, offline…).
        """
        cache_key = f"repo_project_{project_id}"
        cached = self._load_cache(cache_key)
        if cached is not None:
            return cached

        if not doi:
            self._save_cache(cache_key, {})
            return {}

        db_files = self._get_db_files(project_id)
        if not db_files:
            self._save_cache(cache_key, {})
            return {}

        repo = (repo_url or "").lower()
        base = self._base_url(project_url or repo_url or "")

        try:
            if "zenodo.org" in repo:
                snippets = self._fetch_zenodo(doi, db_files)
            elif base:
                snippets = self._fetch_dataverse(doi, base, db_files)
            else:
                snippets = {}
        except Exception as e:
            log.warning(f"[p{project_id}] Repo fetch error: {e}")
            snippets = {}

        self._save_cache(cache_key, snippets)
        return snippets

    # ── DB file manifest ───────────────────────────────────────────────────────

    def _get_db_files(self, project_id: int) -> list[dict]:
        """Return readable files from the files table, sorted by priority."""
        placeholders = ",".join("?" * len(_READABLE_EXTS))
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            f"""
            SELECT file_name, file_type, file_size_bytes
            FROM files
            WHERE project_id = ?
              AND status     = 'SUCCEEDED'
              AND file_type  IN ({placeholders})
            """,
            (project_id, *_READABLE_EXTS),
        ).fetchall()
        conn.close()

        files = [
            {"file_name": r[0], "file_type": r[1], "file_size_bytes": r[2] or 0}
            for r in rows
        ]
        files.sort(
            key=lambda f: _file_priority(f["file_name"], f["file_type"]),
            reverse=True,
        )
        return files

    # ── Zenodo ─────────────────────────────────────────────────────────────────

    def _fetch_zenodo(self, doi: str, db_files: list[dict]) -> dict[str, str]:
        """Download priority files from a Zenodo record."""
        record_id = self._zenodo_id(doi)
        if not record_id:
            log.debug(f"Cannot extract Zenodo record ID from: {doi}")
            return {}

        snippets: dict[str, str] = {}
        for f in db_files:
            if len(snippets) >= MAX_FILES_PER_PROJECT:
                break

            name = f["file_name"]
            ext  = f["file_type"].lower()
            size = f["file_size_bytes"]

            size_limit = MAX_FILE_BYTES_QDA if ext in _QDA_ZIP_EXTS else MAX_FILE_BYTES
            if size > size_limit:
                continue

            # Zenodo direct download — no API key needed for public records
            url  = f"https://zenodo.org/records/{record_id}/files/{name}/content"
            data = self._download(url)
            if data is None:
                continue

            text = self._extract_text(data, ext)
            if text:
                snippets[name] = text[:SNIPPET_CHARS]
                log.debug(f"  Zenodo [{record_id}] fetched {name} ({len(text)} chars)")

        return snippets

    @staticmethod
    def _zenodo_id(doi: str) -> str | None:
        m = re.search(r"zenodo\.(\d+)", doi or "", re.I)
        return m.group(1) if m else None

    # ── Dataverse ──────────────────────────────────────────────────────────────

    def _fetch_dataverse(
        self,
        doi: str,
        base_url: str,
        db_files: list[dict],
    ) -> dict[str, str]:
        """Download priority files from any Dataverse instance."""
        pid = self._normalise_doi(doi)
        if not pid:
            return {}

        # One metadata call to map filename → internal file ID
        meta = self._get_json(
            f"{base_url}/api/datasets/:persistentId/?persistentId={pid}"
        )
        if not meta:
            return {}

        file_id_map: dict[str, int] = {}
        try:
            for entry in meta["data"]["latestVersion"]["files"]:
                df = entry["dataFile"]
                file_id_map[df["filename"]] = df["id"]
        except (KeyError, TypeError):
            return {}

        if not file_id_map:
            return {}

        snippets: dict[str, str] = {}
        for f in db_files:
            if len(snippets) >= MAX_FILES_PER_PROJECT:
                break

            name = f["file_name"]
            ext  = f["file_type"].lower()
            size = f["file_size_bytes"]

            fid = file_id_map.get(name)
            if fid is None:
                continue

            size_limit = MAX_FILE_BYTES_QDA if ext in _QDA_ZIP_EXTS else MAX_FILE_BYTES
            if size > size_limit:
                continue

            data = self._download(f"{base_url}/api/access/datafile/{fid}")
            if data is None:
                continue

            text = self._extract_text(data, ext)
            if text:
                snippets[name] = text[:SNIPPET_CHARS]
                log.debug(f"  Dataverse [{base_url}] fetched {name} ({len(text)} chars)")

        return snippets

    @staticmethod
    def _normalise_doi(doi: str) -> str | None:
        """Return doi:10.XXXX/YYYY form, or None."""
        m = re.search(r"(10\.\d{4,}/\S+)", doi or "")
        return "doi:" + m.group(1).rstrip(".") if m else None

    @staticmethod
    def _base_url(url: str) -> str:
        """Extract scheme + host from any URL."""
        m = re.match(r"(https?://[^/]+)", url or "")
        return m.group(1) if m else ""

    # ── HTTP ───────────────────────────────────────────────────────────────────

    def _download(self, url: str) -> bytes | None:
        for attempt in range(MAX_RETRIES):
            try:
                r = self._session.get(url, timeout=TIMEOUT)
                if r.status_code == 200:
                    return r.content
                if r.status_code == 429:
                    log.debug(f"Rate limited — sleeping {RATE_LIMIT_WAIT}s")
                    time.sleep(RATE_LIMIT_WAIT)
                    continue
                if r.status_code in (401, 403):
                    log.debug(f"Access denied: {url}")
                    return None
                if r.status_code == 404:
                    log.debug(f"Not found: {url}")
                    return None
                log.debug(f"HTTP {r.status_code}: {url}")
                return None
            except requests.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_WAIT)
                else:
                    log.debug(f"Download failed: {e}")
        return None

    def _get_json(self, url: str) -> dict | None:
        data = self._download(url)
        if data is None:
            return None
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None

    # ── Text extraction ────────────────────────────────────────────────────────

    @staticmethod
    def _extract_text(data: bytes, ext: str) -> str:
        extractor = _EXTRACTORS.get(ext)
        if extractor is None:
            return ""
        try:
            text = extractor(data)
            return re.sub(r"\s+", " ", text).strip()
        except Exception:
            return ""

    # ── Cache ──────────────────────────────────────────────────────────────────

    def _cache_path(self, key: str) -> Path:
        safe = re.sub(r"[^a-zA-Z0-9_\-]", "_", key)
        return self.cache_dir / f"{safe}.json"

    def _load_cache(self, key: str):
        p = self._cache_path(key)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
        return None

    def _save_cache(self, key: str, data) -> None:
        try:
            self._cache_path(key).write_text(
                json.dumps(data, ensure_ascii=False), encoding="utf-8"
            )
        except Exception as e:
            log.debug(f"Cache write failed: {e}")
