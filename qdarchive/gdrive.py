"""
Google Drive uploader for QDArchive.

Uploads downloaded files to Google Drive, makes the root folder public,
and records Drive IDs/URLs in all three relevant tables:

  files         → gdrive_file_id, gdrive_url        (one row per file)
  project_files → gdrive_file_id, gdrive_url        (v2 normalised)
  projects      → gdrive_folder_id, gdrive_folder_url (one row per project)

Auth:
  First run opens a browser for OAuth2 consent.
  Token is saved to token.pickle and reused/refreshed automatically.
"""
import logging
import pickle
import socket
import time
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

log = logging.getLogger(__name__)

SCOPES           = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.pickle"
GDRIVE_ROOT_NAME = "QDArchive"

# Seconds before any Drive API call gives up instead of hanging forever.
_SOCKET_TIMEOUT = 120

# Number of consecutive upload failures before the service is rebuilt.
_REBUILD_AFTER_FAILURES = 3


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_drive_service():
    """Return an authenticated Drive v3 service, refreshing token if needed."""
    # Prevent any socket operation from blocking forever.
    socket.setdefaulttimeout(_SOCKET_TIMEOUT)

    creds = None
    if Path(TOKEN_FILE).exists():
        with open(TOKEN_FILE, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)

    return build("drive", "v3", credentials=creds)


# ── Folder helpers ────────────────────────────────────────────────────────────

def _get_or_create_folder(service, name: str, parent_id: str | None = None,
                          retries: int = 3) -> str:
    """Return the Drive folder ID for `name` under `parent_id`, creating if needed.
    Retries on transient network errors and rebuilds the service if needed."""
    escaped_name = name.replace("'", "\\'")
    query = (
        f"name='{escaped_name}' and mimeType='application/vnd.google-apps.folder' "
        f"and trashed=false"
    )
    if parent_id:
        query += f" and '{parent_id}' in parents"

    _TRANSIENT = (socket.timeout, socket.error, ConnectionResetError, BrokenPipeError, OSError)

    for attempt in range(1, retries + 1):
        try:
            results = service.files().list(
                q=query, spaces="drive", fields="files(id, name)"
            ).execute()
            existing = results.get("files", [])
            if existing:
                return existing[0]["id"]

            meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
            if parent_id:
                meta["parents"] = [parent_id]

            folder = service.files().create(body=meta, fields="id").execute()
            log.info(f"GDrive: created folder '{name}' (id={folder['id']})")
            return folder["id"]

        except _TRANSIENT as e:
            wait = 15 * attempt
            log.warning(f"GDrive: connection error getting folder '{name}' attempt {attempt}: {e}"
                        f" — rebuilding service and retrying in {wait}s")
            if attempt < retries:
                time.sleep(wait)
                service = get_drive_service()  # fresh connection
        except HttpError as e:
            if e.resp.status >= 500:
                wait = 15 * attempt
                log.warning(f"GDrive: Drive API error getting folder '{name}': {e}"
                            f" — retry in {wait}s")
                if attempt < retries:
                    time.sleep(wait)
            else:
                raise  # 4xx — caller must handle

    raise RuntimeError(f"GDrive: could not get/create folder '{name}' after {retries} attempts")


def make_public(service, file_id: str):
    """Make a file or folder publicly readable (anyone with the link)."""
    try:
        service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "reader"},
        ).execute()
    except HttpError as e:
        log.warning(f"GDrive: could not make {file_id} public: {e}")


def folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def file_url(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/view"


# ── File upload ───────────────────────────────────────────────────────────────

def stream_url_to_drive(
    service,
    url: str,
    filename: str,
    parent_id: str,
    extra_headers: dict | None = None,
    retries: int = 3,
) -> str | None:
    """
    Download a file from a URL and upload it to Google Drive.
    Uses a temporary file on disk instead of a RAM buffer to avoid OOM kills
    on large files. The temp file is always deleted when done.
    Returns Drive file ID on success, None on failure.
    """
    import tempfile
    import os
    from ssl import SSLError
    from requests.exceptions import Timeout as _Timeout

    # Check if already uploaded
    escaped = filename.replace("'", "\\'")
    query   = f"name='{escaped}' and '{parent_id}' in parents and trashed=false"
    existing = service.files().list(q=query, fields="files(id)").execute().get("files", [])
    if existing:
        log.info(f"GDrive: '{filename}' already on Drive — skipping upload")
        return existing[0]["id"]

    from qdarchive.http_client import SESSION

    for attempt in range(1, retries + 1):
        tmp_path = None
        try:
            # Download to a temp file so we never hold the whole file in RAM
            r = SESSION.get(url, stream=True, timeout=(10, 120), headers=extra_headers or {})

            # 4xx = permanent error, never retry
            if 400 <= r.status_code < 500:
                log.warning(f"GDrive: permanent HTTP {r.status_code} for '{filename}' — skipping")
                return None

            r.raise_for_status()

            fd, tmp_path = tempfile.mkstemp(suffix="_qdarchive_tmp")
            size = 0
            with os.fdopen(fd, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
                    size += len(chunk)

            # Upload from disk — Drive API requires a seekable stream
            media = MediaFileUpload(tmp_path, mimetype="application/octet-stream", resumable=True)
            meta  = {"name": filename, "parents": [parent_id]}
            result = service.files().create(body=meta, media_body=media, fields="id").execute()
            log.info(f"GDrive: uploaded '{filename}' ({size:,} bytes) → id={result['id']}")
            return result["id"]

        except _Timeout as e:
            log.warning(f"GDrive: timeout for '{filename}' — skipping (no retry): {e}")
            return None
        except (SSLError, socket.timeout, socket.error, ConnectionResetError,
                BrokenPipeError, OSError) as e:
            wait = 10 * attempt
            log.warning(f"GDrive: network error '{filename}' attempt {attempt}: {e}"
                        f" — rebuilding service and retry in {wait}s")
            if attempt < retries:
                time.sleep(wait)
                service = get_drive_service()
        except HttpError as e:
            if e.resp.status >= 500:
                wait = 10 * attempt
                log.warning(f"GDrive: Drive API error '{filename}' attempt {attempt}: {e}"
                            f" — retry in {wait}s")
                if attempt < retries:
                    time.sleep(wait)
            else:
                log.warning(f"GDrive: Drive permanent error {e.resp.status} for '{filename}' — skipping")
                return None
        finally:
            # Always clean up the temp file
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
    return None


def upload_file(service, local_path: Path, parent_id: str, retries: int = 3) -> str | None:
    """
    Upload a local file to Drive under parent_id.
    Returns the Drive file ID, or None on failure.
    Skips silently if already present in that folder.
    Retries on network errors (SSL drops, timeouts) with exponential backoff,
    rebuilding the Drive service connection on each network failure.
    """
    from ssl import SSLError

    name = local_path.name
    # Escape single quotes in filename for Drive API query syntax
    escaped_name = name.replace("'", "\\'")
    query = f"name='{escaped_name}' and '{parent_id}' in parents and trashed=false"
    existing = service.files().list(q=query, fields="files(id)").execute().get("files", [])
    if existing:
        log.debug(f"GDrive: '{name}' already exists — skipping")
        return existing[0]["id"]

    media = MediaFileUpload(str(local_path), mimetype="application/octet-stream", resumable=True)
    meta  = {"name": name, "parents": [parent_id]}

    for attempt in range(1, retries + 1):
        try:
            f = service.files().create(body=meta, media_body=media, fields="id").execute()
            log.info(f"GDrive: uploaded '{name}' (id={f['id']})")
            return f["id"]
        except HttpError as e:
            log.warning(f"GDrive: HTTP error '{name}' attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(10 * attempt)
        except (SSLError, socket.timeout, socket.error, ConnectionResetError,
                BrokenPipeError, OSError) as e:
            wait = 30 * attempt
            log.warning(f"GDrive: network error '{name}' attempt {attempt}: {e}"
                        f" — rebuilding service and retrying in {wait}s")
            if attempt < retries:
                time.sleep(wait)
                # Rebuild service to get a fresh HTTP connection
                service = get_drive_service()
                media = MediaFileUpload(str(local_path), mimetype="application/octet-stream",
                                        resumable=True)
    return None


# ── DB migration ──────────────────────────────────────────────────────────────

def _migrate_db(db):
    """Add Google Drive columns to all three tables if they don't exist yet."""
    migrations = [
        ("files",         "gdrive_file_id",    "TEXT"),
        ("files",         "gdrive_url",         "TEXT"),
        ("project_files", "gdrive_file_id",    "TEXT"),
        ("project_files", "gdrive_url",         "TEXT"),
        ("projects",      "gdrive_folder_id",  "TEXT"),
        ("projects",      "gdrive_folder_url", "TEXT"),
    ]
    for table, col, defn in migrations:
        try:
            db.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
            db.conn.commit()
            log.info(f"DB migration: added '{col}' to {table}")
        except Exception:
            pass  # column already exists


# ── Main uploader ─────────────────────────────────────────────────────────────

def upload_archive(db, output_dir: Path, delete_after: bool = False):
    """
    Upload all locally downloaded files to Google Drive and update the DB.

    For each file:
      - Mirrors archive/ folder structure under QDArchive/ on Drive
      - Sets the project folder as public
      - Updates files.gdrive_file_id / gdrive_url
      - Updates project_files.gdrive_file_id / gdrive_url
      - Updates projects.gdrive_folder_id / gdrive_folder_url

    delete_after=True: delete the local file after confirmed upload to Drive,
      and clear local_dir / local_filename in the DB so it reflects
      that the file only lives on Google Drive.
    """
    service = get_drive_service()
    _migrate_db(db)

    # ── Root public folder ────────────────────────────────────────────────────
    root_id  = _get_or_create_folder(service, GDRIVE_ROOT_NAME)
    make_public(service, root_id)
    root_url = folder_url(root_id)
    log.info(f"GDrive: public root folder → {root_url}")

    # ── Fetch all downloaded files not yet on Drive ───────────────────────────
    # Order by file size ascending so small files upload first.
    # Skip files over 2 GB — they are scientific datasets, not qualitative data.
    MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB
    rows = db.conn.execute("""
        SELECT id, local_dir, local_filename, doi, file_size_bytes
        FROM files
        WHERE local_filename != ''
          AND local_filename IS NOT NULL
          AND download_timestamp != ''
          AND download_timestamp IS NOT NULL
          AND (gdrive_file_id IS NULL OR gdrive_file_id = '')
        ORDER BY file_size_bytes ASC
    """).fetchall()

    # Log files that will be skipped due to size
    oversized = [r for r in rows if r[4] and r[4] > MAX_UPLOAD_BYTES]
    if oversized:
        log.info(f"GDrive: skipping {len(oversized)} file(s) over 2 GB")
        for r in oversized:
            log.info(f"  skipped (too large): {r[2]} ({round(r[4]/1073741824,1)} GB)")
    rows = [r for r in rows if not r[4] or r[4] <= MAX_UPLOAD_BYTES]

    log.info(f"GDrive: {len(rows)} file(s) to upload")

    # Cache folder IDs to avoid redundant API calls
    folder_cache: dict[str, str] = {}
    uploaded            = 0
    failed              = 0
    skipped             = 0
    consecutive_failures = 0

    for i, row in enumerate(rows, 1):
        row_id      = row[0]
        local_dir   = row[1]   # e.g. "harvard_dataverse/doi_10.5064_F6V5VGX3"
        local_fname = row[2]
        doi         = row[3]
        fsize_mb    = round(row[4] / 1048576, 1) if row[4] else 0
        local_path  = output_dir / local_dir / local_fname
        log.info(f"GDrive: [{i}/{len(rows)}] {local_fname} ({fsize_mb} MB)")

        if not local_path.exists():
            log.warning(f"GDrive: missing locally — skipping: {local_path}")
            skipped += 1
            continue

        # Proactively rebuild service after several consecutive failures so a
        # stale/dropped connection doesn't block the whole run.
        if consecutive_failures >= _REBUILD_AFTER_FAILURES:
            log.warning(f"GDrive: {consecutive_failures} consecutive failures — "
                        "rebuilding Drive service connection...")
            service = get_drive_service()
            consecutive_failures = 0

        # ── Build folder structure on Drive ───────────────────────────────────
        parts = Path(local_dir).parts  # ("harvard_dataverse", "doi_10.5064_F6V5VGX3")
        parent = root_id
        cache_key = ""
        project_folder_id = None

        try:
            for j, part in enumerate(parts):
                cache_key = f"{cache_key}/{part}"
                if cache_key not in folder_cache:
                    fid = _get_or_create_folder(service, part, parent)
                    folder_cache[cache_key] = fid
                    # Make project-level folder (depth 1) public
                    if j == 1:
                        make_public(service, fid)
                parent = folder_cache[cache_key]
                if j == len(parts) - 1:
                    project_folder_id = parent
        except Exception as e:
            log.warning(f"GDrive: could not build folder structure for '{local_fname}': {e}")
            consecutive_failures += 1
            failed += 1
            continue

        # ── Upload file ───────────────────────────────────────────────────────
        gdrive_id = upload_file(service, local_path, parent)
        if not gdrive_id:
            consecutive_failures += 1
            failed += 1
            continue

        consecutive_failures = 0

        gdrive_file_url = file_url(gdrive_id)

        # ── Update legacy files table ─────────────────────────────────────────
        db.conn.execute(
            "UPDATE files SET gdrive_file_id=?, gdrive_url=? WHERE id=?",
            (gdrive_id, gdrive_file_url, row_id),
        )

        # ── Update project_files table ────────────────────────────────────────
        # Match by project whose local path matches local_dir
        if len(parts) >= 2:
            repo_folder    = parts[0]   # e.g. "harvard_dataverse"
            project_folder = parts[1]   # e.g. "doi_10.5064_F6V5VGX3"
            db.conn.execute("""
                UPDATE project_files
                SET gdrive_file_id = ?, gdrive_url = ?
                WHERE file_name = ?
                  AND project_id IN (
                      SELECT id FROM projects
                      WHERE download_repository_folder = ?
                        AND download_project_folder = ?
                  )
            """, (gdrive_id, gdrive_file_url, local_fname, repo_folder, project_folder))

            # ── Update projects table (folder link) ───────────────────────────
            if project_folder_id:
                proj_folder_url = folder_url(project_folder_id)
                db.conn.execute("""
                    UPDATE projects
                    SET gdrive_folder_id = ?, gdrive_folder_url = ?
                    WHERE download_repository_folder = ?
                      AND download_project_folder = ?
                      AND (gdrive_folder_id IS NULL OR gdrive_folder_id = '')
                """, (project_folder_id, proj_folder_url, repo_folder, project_folder))

        # ── Optionally delete local file after confirmed upload ───────────────
        if delete_after:
            try:
                local_path.unlink()
                # Remove empty parent directories
                try:
                    local_path.parent.rmdir()
                except OSError:
                    pass  # not empty yet — other files still there
                # Clear local path in DB so it reflects Drive-only storage
                db.conn.execute(
                    "UPDATE files SET local_dir='', local_filename='' WHERE id=?",
                    (row_id,)
                )
                log.info(f"GDrive: deleted local copy of '{local_fname}'")
            except Exception as e:
                log.warning(f"GDrive: could not delete local file '{local_path}': {e}")

        db.conn.commit()
        uploaded += 1

    log.info(f"GDrive: done — {uploaded} uploaded, {failed} failed, {skipped} skipped (missing locally)")
    log.info(f"GDrive: public folder → {root_url}")
    print(f"\nPublic Google Drive folder: {root_url}\n")
    return root_url
