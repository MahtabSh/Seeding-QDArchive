"""
Shared HTTP session, JSON GET helper, and streaming file downloader.

All requests use a direct connection with exponential-backoff retry.
"""
import logging
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# ── Shared session ────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "QDArchivePipeline/7.0 (research; oss.cs.fau.de)",
})


def get_json(url: str, params=None, headers=None, retries: int = 5, delay: int = 3,
             bypass_proxy: bool = False):
    """GET a URL and return parsed JSON, with exponential-backoff retry.

    bypass_proxy is accepted for backwards compatibility but has no effect
    (proxy support has been removed).
    """
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, headers=headers, timeout=(10, 60))

            if r.status_code == 403:
                wait = 30 * (attempt + 1)
                log.warning(f"GET {url} -> 403, waiting {wait}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After", "")
                try:
                    wait = max(int(retry_after), 60)
                except (ValueError, TypeError):
                    wait = 60
                log.warning(f"GET {url} -> 429 rate-limited, waiting {wait}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue

            # Permanent errors — retrying will never help
            if r.status_code in (401, 404, 410):
                log.warning(f"GET {url} -> {r.status_code} (permanent, no retry)")
                return None

            if r.status_code == 400:
                log.warning(f"GET {url} -> 400 Bad Request (permanent, no retry)")
                return None

            r.raise_for_status()

            try:
                return r.json()
            except ValueError:
                log.warning(f"GET {url} -> empty/non-JSON response (attempt {attempt + 1}/{retries})")
                time.sleep(delay * (2 ** attempt))
                continue

        except requests.exceptions.HTTPError as e:
            wait = delay * (2 ** attempt)
            log.warning(f"GET {url} attempt {attempt + 1} failed: {e} — retry in {wait}s")
            time.sleep(wait)
        except Exception as e:
            wait = delay * (2 ** attempt)
            log.warning(f"GET {url} attempt {attempt + 1} failed: {e} — retry in {wait}s")
            time.sleep(wait)

    log.error(f"get_json: gave up after {retries} attempts: {url}")
    return None


def download_file(url: str, dest: Path, retries: int = 5, extra_headers: dict = None) -> bool:
    """Stream a remote file to *dest*. Returns True on success."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        log.debug(f"Already downloaded: {dest}")
        return True

    for attempt in range(retries):
        try:
            r = SESSION.get(
                url, stream=True, timeout=(10, 120),
                headers=extra_headers or {}, allow_redirects=True,
            )

            if r.status_code == 403:
                wait = 30 * (attempt + 1)
                log.warning(f"Download {url} -> 403, waiting {wait}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue

            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
            log.info(f"  -> {dest.name}  ({dest.stat().st_size:,} bytes)")
            return True

        except requests.exceptions.HTTPError as e:
            wait = 2 * (2 ** attempt)
            log.warning(f"Download {url} attempt {attempt + 1} failed: {e} — retry in {wait}s")
            time.sleep(wait)
        except Exception as e:
            wait = 2 * (2 ** attempt)
            log.warning(f"Download {url} attempt {attempt + 1} failed: {e} — retry in {wait}s")
            time.sleep(wait)

    log.error(f"download_file: gave up after {retries} attempts: {url}")
    return False
