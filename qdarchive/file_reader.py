"""
Google Drive file content reader for QDArchive classification.

For each project that has a gdrive_folder_id, this module:
  1. Lists files in the Drive folder
  2. Selects up to MAX_FILES_PER_PROJECT text-readable files (prioritising
     READMEs, codebooks, interview guides, and description documents)
  3. Downloads and extracts plain text (up to SNIPPET_CHARS chars per file)
  4. Caches results on disk so reruns skip Drive API calls

Supported formats:  .txt  .pdf  .docx  .doc  .csv  .tab  .tsv

Usage
-----
  from qdarchive.file_reader import FileContentReader

  reader = FileContentReader()          # builds Drive service from token.pickle
  snippets = reader.get_snippets(project_id=1, folder_id="1A4Ju3Ei…")
  # snippets = {"Codebook.docx": "This codebook describes coding…", …}
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────────
MAX_FILES_PER_PROJECT = 3      # max files to read per project
SNIPPET_CHARS         = 900    # max characters extracted per file
MAX_FILE_BYTES        = 5 * 1024 * 1024    # skip regular files > 5 MB
MAX_FILE_BYTES_QDA    = 50 * 1024 * 1024   # QDA ZIPs can be larger
MAX_INNER_FILES_QDA   = 3      # max primary data files to extract from one QDA ZIP
CACHE_DIR             = Path(".file_cache")

# Priority score for filename patterns (higher = download first)
_PRIORITY_PATTERNS: list[tuple[re.Pattern, int]] = [
    (re.compile(r"\breadme\b",        re.I), 100),
    (re.compile(r"\bdescri",          re.I),  90),
    (re.compile(r"\babout\b",         re.I),  85),
    (re.compile(r"\boverview\b",      re.I),  85),
    (re.compile(r"\bcoding.*(guide|book|schema|system)\b", re.I), 80),
    (re.compile(r"\bcodebook\b",      re.I),  80),
    (re.compile(r"\bcode.?system\b",  re.I),  80),
    (re.compile(r"\binterview.*(guide|question|topic)\b", re.I), 75),
    (re.compile(r"\btopic.?list\b",   re.I),  70),
    (re.compile(r"\bquestionnaire\b", re.I),  70),
    (re.compile(r"\bsurvey\b",        re.I),  65),
    (re.compile(r"\bprotocol\b",      re.I),  65),
    (re.compile(r"\btranscript\b",    re.I),  60),
    (re.compile(r"\bpolicy\b",        re.I),  55),
    (re.compile(r"\bdocument\b",      re.I),  40),
    (re.compile(r"\breport\b",        re.I),  35),
]

# QDA project file extensions that are actually ZIP archives containing primary data
_QDA_ZIP_EXTS = {
    ".atlproj", ".atlproj23", ".atlproj9",   # ATLAS.ti
    ".nvpx",                                  # NVivo
    ".qdpx",                                  # REFI-QDA (open standard)
    ".mex", ".mx18", ".mx20", ".mx22", ".mx24",  # MAXQDA
    ".mqda",                                  # other QDA tools
}

# f4transkript XML format
_F4_EXTS = {".f4", ".f4a"}

_READABLE_EXTS = {".txt", ".pdf", ".docx", ".doc", ".csv", ".tab", ".tsv"} | _QDA_ZIP_EXTS | _F4_EXTS

# MIME types for export (Google Docs native formats)
_EXPORT_MIME = {
    "application/vnd.google-apps.document":     "text/plain",
    "application/vnd.google-apps.spreadsheet":  "text/csv",
}


def _file_priority(name: str, ext: str) -> int:
    base = {"txt": 5, "csv": 2, "tab": 2, "tsv": 2, "pdf": 3, "docx": 4, "doc": 3,
            "f4": 4, "f4a": 4}.get(ext.lstrip("."), 0)
    # QDA ZIPs have lower base priority than direct primary data (processed last)
    if ext in _QDA_ZIP_EXTS:
        base = 1
    score = base
    for pattern, bonus in _PRIORITY_PATTERNS:
        if pattern.search(name):
            score += bonus
            break
    return score


# ── Text extractors ───────────────────────────────────────────────────────────

def _extract_txt(data: bytes) -> str:
    for enc in ("utf-8", "latin-1", "utf-16"):
        try:
            return data.decode(enc)
        except (UnicodeDecodeError, ValueError):
            continue
    return data.decode("utf-8", errors="replace")


def _extract_pdf(data: bytes) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        parts = []
        for page in reader.pages[:4]:     # first 4 pages is enough
            text = page.extract_text() or ""
            parts.append(text)
            if sum(len(p) for p in parts) > SNIPPET_CHARS * 2:
                break
        return "\n".join(parts)
    except Exception as e:
        log.debug(f"PDF extraction failed: {e}")
        return ""


def _extract_docx(data: bytes) -> str:
    try:
        from docx import Document
        doc = Document(io.BytesIO(data))
        return "\n".join(p.text for p in doc.paragraphs if p.text.strip())
    except Exception as e:
        log.debug(f"DOCX extraction failed: {e}")
        return ""


def _extract_csv(data: bytes) -> str:
    try:
        text = _extract_txt(data)
        lines = [l for l in text.splitlines() if l.strip()]
        return "\n".join(lines[:20])       # first 20 rows = schema + sample
    except Exception:
        return ""


def _extract_qda_zip(data: bytes) -> str:
    """Open a QDA project file as a ZIP and extract text from embedded primary data files."""
    _inner_readable = {".txt", ".pdf", ".docx", ".doc", ".rtf"}
    _inner_extractors = {
        ".txt":  _extract_txt,
        ".pdf":  _extract_pdf,
        ".docx": _extract_docx,
        ".doc":  _extract_docx,
        ".rtf":  _extract_txt,   # RTF: rough plain-text fallback
    }
    MAX_INNER_BYTES = 2 * 1024 * 1024  # skip inner files > 2 MB

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            entries = []
            for info in zf.infolist():
                ext = Path(info.filename).suffix.lower()
                if ext not in _inner_readable:
                    continue
                if info.file_size > MAX_INNER_BYTES:
                    continue
                name = Path(info.filename).name
                priority = _file_priority(name, ext)
                entries.append((priority, info))

            entries.sort(key=lambda x: x[0], reverse=True)
            parts = []
            for _, info in entries[:MAX_INNER_FILES_QDA]:
                ext = Path(info.filename).suffix.lower()
                try:
                    raw = zf.read(info.filename)
                    text = _inner_extractors.get(ext, _extract_txt)(raw).strip()
                    if text:
                        parts.append(f"[{Path(info.filename).name}]\n{text}")
                except Exception as e:
                    log.debug(f"QDA inner file {info.filename} failed: {e}")

            return "\n\n".join(parts)
    except zipfile.BadZipFile:
        log.debug("QDA file is not a valid ZIP archive")
        return ""
    except Exception as e:
        log.debug(f"QDA ZIP extraction failed: {e}")
        return ""


def _extract_f4_xml(data: bytes) -> str:
    """Extract speaker transcript text from f4transkript XML format."""
    try:
        root = ET.fromstring(data)
        # f4transkript uses <Event> elements with speaker and text attributes
        parts = []
        for event in root.iter("Event"):
            speaker = event.get("Sprecher") or event.get("Speaker") or ""
            text = (event.text or "").strip()
            if text:
                parts.append(f"{speaker}: {text}" if speaker else text)
        if parts:
            return " ".join(parts)
        # fallback: just concatenate all text nodes
        return " ".join(t.strip() for t in root.itertext() if t.strip())
    except ET.ParseError:
        # Might be plain text with XML-like extension
        return _extract_txt(data)
    except Exception as e:
        log.debug(f"F4 XML extraction failed: {e}")
        return ""


_EXTRACTORS = {
    ".txt":  _extract_txt,
    ".csv":  _extract_csv,
    ".tab":  _extract_csv,
    ".tsv":  _extract_csv,
    ".pdf":  _extract_pdf,
    ".docx": _extract_docx,
    ".doc":  _extract_docx,
    ".f4":   _extract_f4_xml,
    ".f4a":  _extract_f4_xml,
    # QDA ZIP formats
    **{ext: _extract_qda_zip for ext in _QDA_ZIP_EXTS},
}


# ── Drive helpers ─────────────────────────────────────────────────────────────

def _build_drive_service():
    """Build a Drive v3 service from the existing token.pickle, or return None."""
    try:
        import pickle
        import socket
        from pathlib import Path
        from googleapiclient.discovery import build
        from google.auth.transport.requests import Request

        token_path = Path("token.pickle")
        if not token_path.exists():
            log.warning("token.pickle not found — Drive access disabled")
            return None

        socket.setdefaulttimeout(60)
        with open(token_path, "rb") as f:
            creds = pickle.load(f)

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_path, "wb") as f:
                pickle.dump(creds, f)

        if not creds or not creds.valid:
            log.warning("Drive credentials invalid — Drive access disabled")
            return None

        return build("drive", "v3", credentials=creds)
    except Exception as e:
        log.warning(f"Drive service unavailable: {e}")
        return None


# ── Main class ────────────────────────────────────────────────────────────────

class FileContentReader:
    """
    Downloads and caches text snippets from files in a project's GDrive folder.

    If Google Drive is unavailable (token expired, offline, etc.) every call
    returns {} so the classifier silently falls back to metadata-only mode.
    """

    def __init__(self, cache_dir: Path = CACHE_DIR):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self._service = _build_drive_service()
        if self._service:
            log.info("FileContentReader: Drive service ready")
        else:
            log.info("FileContentReader: Drive unavailable — metadata-only mode")

    # ── Public API ────────────────────────────────────────────────────────────

    def get_snippets(
        self,
        project_id: int,
        folder_id: str,
        known_files: list[str] | None = None,
    ) -> dict[str, str]:
        """
        Return {filename: snippet} for up to MAX_FILES_PER_PROJECT readable files.

        known_files: list of filenames already in project_files (used for priority
                     scoring when listing the folder).
        """
        cache_key = f"project_{project_id}"
        cached = self._load_cache(cache_key)
        if cached is not None:
            return cached

        if not self._service or not folder_id:
            return {}

        try:
            candidates = self._list_folder(folder_id, known_files or [])
        except Exception as e:
            log.warning(f"[p{project_id}] Folder listing failed: {e}")
            return {}

        snippets: dict[str, str] = {}
        for file_id, file_name, mime, size in candidates[:MAX_FILES_PER_PROJECT]:
            text = self._download_and_extract(file_id, file_name, mime, size)
            if text and text.strip():
                snippets[file_name] = text.strip()[:SNIPPET_CHARS]

        self._save_cache(cache_key, snippets)
        return snippets

    # ── Drive operations ──────────────────────────────────────────────────────

    def _list_folder(
        self,
        folder_id: str,
        known_files: list[str],
    ) -> list[tuple[str, str, str, int]]:
        """
        Return list of (file_id, name, mime_type, size) sorted by priority,
        filtered to text-readable types only.
        """
        results = []
        page_token = None

        while True:
            params = dict(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, mimeType, size)",
                pageSize=100,
            )
            if page_token:
                params["pageToken"] = page_token

            try:
                resp = self._service.files().list(**params).execute()
            except Exception as e:
                log.warning(f"Drive API list error: {e}")
                break

            for item in resp.get("files", []):
                name  = item.get("name", "")
                ext   = Path(name).suffix.lower()
                mime  = item.get("mimeType", "")
                size  = int(item.get("size", 0) or 0)

                is_readable = (
                    ext in _READABLE_EXTS
                    or mime in _EXPORT_MIME
                )
                if not is_readable:
                    continue
                size_limit = MAX_FILE_BYTES_QDA if ext in _QDA_ZIP_EXTS else MAX_FILE_BYTES
                if size > size_limit:
                    continue

                results.append((item["id"], name, mime, size))

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        # Sort by priority (README > codebook > interview guide > other)
        known_set = set(known_files)
        results.sort(
            key=lambda x: (
                _file_priority(x[1], Path(x[1]).suffix.lower()),
                x[1] in known_set,     # known files slightly preferred
            ),
            reverse=True,
        )
        return results

    def _download_and_extract(
        self,
        file_id: str,
        file_name: str,
        mime: str,
        size: int,
    ) -> str:
        """Download one file and return extracted plain text."""
        # Check per-file cache
        file_cache_key = f"file_{file_id}"
        cached = self._load_cache(file_cache_key)
        if cached is not None:
            return cached.get("text", "")

        try:
            from googleapiclient.http import MediaIoBaseDownload

            ext = Path(file_name).suffix.lower()

            # Google Docs native formats → export as text/csv
            if mime in _EXPORT_MIME:
                export_mime = _EXPORT_MIME[mime]
                request = self._service.files().export_media(
                    fileId=file_id, mimeType=export_mime
                )
                ext = ".csv" if "csv" in export_mime else ".txt"
            else:
                request = self._service.files().get_media(fileId=file_id)

            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            data = buf.getvalue()
        except Exception as e:
            log.debug(f"Download failed for {file_name}: {e}")
            self._save_cache(file_cache_key, {"text": ""})
            return ""

        # Extract text
        extractor = _EXTRACTORS.get(ext, _extract_txt)
        try:
            text = extractor(data)
            # Normalise whitespace
            text = re.sub(r"\s+", " ", text).strip()
        except Exception as e:
            log.debug(f"Extraction failed for {file_name}: {e}")
            text = ""

        self._save_cache(file_cache_key, {"text": text[:SNIPPET_CHARS * 2]})
        return text

    # ── Cache helpers ─────────────────────────────────────────────────────────

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
            log.debug(f"Cache write failed for {key}: {e}")
