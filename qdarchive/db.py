"""
SQLite database layer.

Two schemas coexist:
  • Legacy `files` table  — flat, one row per file (v1, kept for compat)
  • Normalised v2 tables  — projects / project_files / keywords / person_role / licenses
"""
import json
import logging
import sqlite3
import threading
from pathlib import Path

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Legacy schema (v1)
# ─────────────────────────────────────────────────────────────────────────────
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS files (
    id                 INTEGER  PRIMARY KEY AUTOINCREMENT,

    -- Identity
    url                TEXT NOT NULL,
    doi                TEXT,

    -- File
    local_dir          TEXT,
    local_filename     TEXT,
    file_type          TEXT,
    file_extension     TEXT,
    file_size_bytes    INTEGER,
    checksum_md5       TEXT,

    -- Download
    download_timestamp TEXT,
    source_name        TEXT,
    source_url         TEXT,

    -- Dataset
    title              TEXT,
    description        TEXT,
    year               TEXT,
    keywords           TEXT,
    language           TEXT,

    -- People
    author             TEXT,
    uploader_name      TEXT,
    uploader_email     TEXT,

    -- Legal
    license            TEXT,
    license_url        TEXT,

    -- Discovery
    matched_query      TEXT,

    -- Access
    manual_download    INTEGER DEFAULT 0,
    access_note        TEXT
);
"""

_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_url             ON files(url);
CREATE INDEX IF NOT EXISTS idx_doi             ON files(doi);
CREATE INDEX IF NOT EXISTS idx_file_type       ON files(file_type);
CREATE INDEX IF NOT EXISTS idx_matched_query   ON files(matched_query);
CREATE INDEX IF NOT EXISTS idx_manual_download ON files(manual_download);
"""

# ─────────────────────────────────────────────────────────────────────────────
# Normalised schema (v2) — from schema spreadsheet 2026-03-13
# ─────────────────────────────────────────────────────────────────────────────
_CREATE_PROJECTS_TABLE = """
CREATE TABLE IF NOT EXISTS projects (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    query_string                TEXT,
    repository_id               INTEGER,
    repository_url              TEXT    NOT NULL,
    project_url                 TEXT    NOT NULL,
    version                     TEXT,
    title                       TEXT    NOT NULL,
    description                 TEXT,
    language                    TEXT,
    doi                         TEXT,
    upload_date                 TEXT,
    download_date               TEXT    NOT NULL,
    download_repository_folder  TEXT    NOT NULL,
    download_project_folder     TEXT    NOT NULL,
    download_version_folder     TEXT,
    download_method             TEXT    NOT NULL
);
"""

_CREATE_PROJECT_FILES_TABLE = """
CREATE TABLE IF NOT EXISTS project_files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    file_name   TEXT    NOT NULL,
    file_type   TEXT    NOT NULL,
    status      TEXT    NOT NULL
);
"""

_CREATE_KEYWORDS_TABLE = """
CREATE TABLE IF NOT EXISTS keywords (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    keyword     TEXT    NOT NULL
);
"""

_CREATE_PERSON_ROLE_TABLE = """
CREATE TABLE IF NOT EXISTS person_role (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    name        TEXT    NOT NULL,
    role        TEXT    NOT NULL
);
"""

_CREATE_LICENSES_TABLE = """
CREATE TABLE IF NOT EXISTS licenses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    license     TEXT    NOT NULL
);
"""

_CREATE_V2_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_projects_doi        ON projects(doi);
CREATE INDEX IF NOT EXISTS idx_projects_url        ON projects(project_url);
CREATE INDEX IF NOT EXISTS idx_projects_repo       ON projects(repository_id);
CREATE INDEX IF NOT EXISTS idx_pfiles_project      ON project_files(project_id);
CREATE INDEX IF NOT EXISTS idx_keywords_project    ON keywords(project_id);
CREATE INDEX IF NOT EXISTS idx_person_role_project ON person_role(project_id);
CREATE INDEX IF NOT EXISTS idx_licenses_project    ON licenses(project_id);
"""

_COLUMNS = [
    "url", "doi",
    "local_dir", "local_filename", "file_type", "file_extension",
    "file_size_bytes", "checksum_md5",
    "download_timestamp", "source_name", "source_url",
    "title", "description", "year", "keywords", "language",
    "author", "uploader_name", "uploader_email",
    "license", "license_url",
    "matched_query",
    "manual_download", "access_note",
]


class MetadataDB:
    """SQLite wrapper. Manages both the legacy flat schema and normalised v2 tables."""

    # Commit to disk every BATCH_SIZE inserts instead of after every row.
    # Larger = faster writes; smaller = less data lost on crash.
    BATCH_SIZE = 50

    def __init__(self, path: Path):
        # check_same_thread=False lets worker threads share this connection;
        # _lock serialises every write so SQLite never sees concurrent mutations.
        # timeout=60: wait up to 60 s for a write lock when two pipeline
        # processes run concurrently (e.g. --sources harvard in one terminal,
        # --sources zenodo in another).  WAL mode allows concurrent reads;
        # writes will serialise automatically with this timeout.
        self.conn = sqlite3.connect(str(path), check_same_thread=False, timeout=60)
        self.conn.row_factory = sqlite3.Row
        self._lock    = threading.Lock()
        self._pending = 0          # inserts since last commit
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")  # safe with WAL, ~2× faster
        # Legacy schema
        self.conn.executescript(_CREATE_TABLE + _CREATE_INDEXES)
        # Normalised v2 schema
        self.conn.executescript(
            _CREATE_PROJECTS_TABLE
            + _CREATE_PROJECT_FILES_TABLE
            + _CREATE_KEYWORDS_TABLE
            + _CREATE_PERSON_ROLE_TABLE
            + _CREATE_LICENSES_TABLE
            + _CREATE_V2_INDEXES
        )
        # Migrate existing databases
        for col, definition in [
            ("manual_download", "INTEGER DEFAULT 0"),
            ("access_note",     "TEXT"),
        ]:
            try:
                self.conn.execute(f"ALTER TABLE files ADD COLUMN {col} {definition}")
                self.conn.commit()
                log.info(f"DB migration: added column '{col}'")
            except Exception:
                pass
        self.conn.commit()
        log.info(f"Database ready: {path}")

    # ── Legacy v1 helpers ─────────────────────────────────────────────────────

    def seen_urls(self) -> set:
        return {r["url"] for r in self.conn.execute("SELECT url FROM files").fetchall()}

    def seen_dois(self) -> set:
        return {r["doi"] for r in self.conn.execute(
            "SELECT DISTINCT doi FROM files WHERE doi IS NOT NULL AND doi != ''"
        ).fetchall()}

    def _tick(self):
        """Commit every BATCH_SIZE writes; avoids per-row disk flushes.
        Must be called while self._lock is already held."""
        self._pending += 1
        if self._pending >= self.BATCH_SIZE:
            self.conn.commit()
            self._pending = 0

    def flush(self):
        """Force-commit any pending writes (call at end of each dataset)."""
        with self._lock:
            if self._pending:
                self.conn.commit()
                self._pending = 0

    def append_query(self, doi: str, new_query: str):
        """Append new_query to the matched_query JSON array for all rows with this DOI."""
        with self._lock:
            rows = self.conn.execute(
                "SELECT id, matched_query FROM files WHERE doi = ?", (doi,)
            ).fetchall()
            for row in rows:
                raw = row["matched_query"] or "[]"
                try:
                    parts = json.loads(raw)
                    if not isinstance(parts, list):
                        parts = [raw]
                except Exception:
                    parts = [p.strip() for p in raw.split(";") if p.strip()]
                if new_query not in parts:
                    parts.append(new_query)
                    self.conn.execute(
                        "UPDATE files SET matched_query = ? WHERE id = ?",
                        (json.dumps(parts, ensure_ascii=False), row["id"]),
                    )
            if self._pending:
                self.conn.commit()
                self._pending = 0

    def insert(self, record: dict):
        with self._lock:
            values = [record.get(c) for c in _COLUMNS]
            ph = ", ".join("?" for _ in _COLUMNS)
            self.conn.execute(
                f"INSERT INTO files ({', '.join(_COLUMNS)}) VALUES ({ph})",
                values,
            )
            self._tick()

    # ── Normalised v2 helpers ─────────────────────────────────────────────────

    def seen_project_urls(self) -> set:
        with self._lock:
            return {r["project_url"] for r in self.conn.execute("SELECT project_url FROM projects").fetchall()}

    def append_project_query(self, doi: str, new_query: str):
        """Append new_query to query_string (JSON array) for all projects with this DOI."""
        with self._lock:
            rows = self.conn.execute(
                "SELECT id, query_string FROM projects WHERE doi = ?", (doi,)
            ).fetchall()
            for row in rows:
                raw = row["query_string"] or "[]"
                try:
                    parts = json.loads(raw)
                    if not isinstance(parts, list):
                        parts = [raw]
                except Exception:
                    parts = [p.strip() for p in raw.split(";") if p.strip()]
                if new_query not in parts:
                    parts.append(new_query)
                    self.conn.execute(
                        "UPDATE projects SET query_string = ? WHERE id = ?",
                        (json.dumps(parts, ensure_ascii=False), row["id"]),
                    )
            if self._pending:
                self.conn.commit()
                self._pending = 0

    def insert_project(self, record: dict) -> int:
        """Insert a row into projects and return its new id."""
        with self._lock:
            cols = list(record.keys())
            ph   = ", ".join("?" for _ in cols)
            cur  = self.conn.execute(
                f"INSERT INTO projects ({', '.join(cols)}) VALUES ({ph})",
                [record[c] for c in cols],
            )
            self._tick()
            return cur.lastrowid

    def insert_project_file(self, project_id: int, file_name: str, file_type: str, status: str):
        with self._lock:
            self.conn.execute(
                "INSERT INTO project_files (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)",
                (project_id, file_name, file_type, status),
            )
            self._tick()

    def insert_keyword(self, project_id: int, keyword: str):
        with self._lock:
            self.conn.execute(
                "INSERT INTO keywords (project_id, keyword) VALUES (?, ?)",
                (project_id, keyword),
            )
            self._tick()

    def insert_person_role(self, project_id: int, name: str, role: str):
        with self._lock:
            self.conn.execute(
                "INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)",
                (project_id, name, role),
            )
            self._tick()

    def insert_license(self, project_id: int, license_str: str):
        with self._lock:
            self.conn.execute(
                "INSERT INTO licenses (project_id, license) VALUES (?, ?)",
                (project_id, license_str),
            )
            self._tick()

    def close(self):
        self.flush()   # commit any remaining pending writes
        self.conn.close()
