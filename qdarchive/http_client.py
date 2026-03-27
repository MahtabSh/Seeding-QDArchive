"""
Shared HTTP session, JSON GET helper, and streaming file downloader.

Proxy / IP rotation
-------------------
Set the PROXY_LIST environment variable to a comma-separated list of proxy URLs.
The session automatically rotates to the next proxy whenever a block (403 HTML /
connection refused) is detected.

Examples:
    export PROXY_LIST="socks5h://127.0.0.1:9050"              # Tor
    export PROXY_LIST="http://proxy1:8080,http://proxy2:8080"  # pool

Tor setup (macOS):
    brew install tor && brew services start tor
    # Tor listens on socks5h://127.0.0.1:9050 by default.
    # Use socks5h:// (not socks5://) so DNS is resolved through Tor too.

    pip install requests[socks]   # needs PySocks
"""
import itertools
import logging
import os
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# ── Tor circuit renewal ───────────────────────────────────────────────────────
# Requires Tor running with ControlPort enabled (default: 9051).
# Add to /usr/local/etc/tor/torrc (or /etc/tor/torrc):
#   ControlPort 9051
#   CookieAuthentication 1
# Then: brew services restart tor
#
# Set TOR_CONTROL_PASSWORD env var if you use HashedControlPassword instead.

def renew_tor_circuit():
    """
    Signal Tor to build a new circuit (= new exit node = new IP).
    Called automatically on every block detection when using Tor.
    No-op if stem is not installed or Tor control port is unavailable.
    """
    try:
        from stem import Signal
        from stem.control import Controller
        password = os.environ.get("TOR_CONTROL_PASSWORD", "")
        with Controller.from_port(port=9051) as ctrl:
            if password:
                ctrl.authenticate(password=password)
            else:
                ctrl.authenticate()
            ctrl.signal(Signal.NEWNYM)
        log.info("Tor circuit renewed — new exit node (new IP)")
        time.sleep(5)   # Tor needs ~5s to build the new circuit
    except Exception as e:
        log.debug(f"Tor circuit renewal skipped: {e}")


# ── Proxy pool ────────────────────────────────────────────────────────────────
def _load_proxy_pool() -> list[dict]:
    """
    Read PROXY_LIST env var and return a list of proxy dicts for requests.
    Returns [{}] (no proxy) if the env var is not set.
    """
    raw = os.environ.get("PROXY_LIST", "").strip()
    if not raw:
        return [{}]
    pool = []
    for p in raw.split(","):
        p = p.strip()
        if p:
            pool.append({"http": p, "https": p})
    return pool or [{}]


_PROXY_POOL  = _load_proxy_pool()
_proxy_cycle = itertools.cycle(_PROXY_POOL)
_current_proxy: dict = next(_proxy_cycle)


def _rotate_proxy() -> dict:
    """Advance to the next proxy and return it."""
    global _current_proxy
    _current_proxy = next(_proxy_cycle)
    if _current_proxy:
        log.info(f"Proxy rotated → {list(_current_proxy.values())[0]}")
    else:
        log.info("Proxy rotated → direct connection (no proxy)")
    return _current_proxy


# ── Shared session ────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "QDArchivePipeline/7.0 (research; oss.cs.fau.de)",
})


def _is_bot_block(response) -> bool:
    """
    Return True when a 403 is a network-level block rather than a real API rate-limit.

    Real Dataverse / REST API 403s return JSON like {"status":"ERROR",...}.
    Bot/IP blocks (Cloudflare JS challenge, nginx 403, WAF) return HTML.
    Retrying these is pointless — the block will last minutes to hours.
    """
    ct = response.headers.get("Content-Type", "")
    return "text/html" in ct


def get_json(url: str, params=None, headers=None, retries: int = 5, delay: int = 3):
    """GET a URL and return parsed JSON.

    On every block (403 HTML / empty body / connection refused) the Tor circuit
    is renewed and the request is retried — up to `retries` times total.
    """
    for attempt in range(retries):
        try:
            r = SESSION.get(
                url, params=params, headers=headers,
                timeout=(10, 60), proxies=_current_proxy,
            )

            if r.status_code == 403:
                if _is_bot_block(r):
                    log.warning(
                        f"GET {url} -> 403 HTML block "
                        f"(attempt {attempt + 1}/{retries}) — renewing Tor circuit"
                    )
                    renew_tor_circuit()
                    if len(_PROXY_POOL) > 1:
                        _rotate_proxy()
                    continue
                wait = 30 * (attempt + 1)
                log.warning(f"GET {url} -> 403, waiting {wait}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue

            # Permanent errors — retrying will never help
            if r.status_code in (401, 404, 410):
                log.debug(f"GET {url} -> {r.status_code} (permanent, no retry)")
                return None

            r.raise_for_status()
            try:
                return r.json()
            except ValueError:
                log.warning(
                    f"GET {url} -> empty response body "
                    f"(attempt {attempt + 1}/{retries}) — renewing Tor circuit"
                )
                renew_tor_circuit()
                continue

        except requests.exceptions.HTTPError as e:
            wait = delay * (2 ** attempt)
            log.warning(f"GET {url} attempt {attempt + 1} failed: {e} — retry in {wait}s")
            time.sleep(wait)
        except requests.exceptions.ConnectionError as e:
            log.warning(
                f"GET {url} -> connection error "
                f"(attempt {attempt + 1}/{retries}) — renewing Tor circuit"
            )
            renew_tor_circuit()
            if len(_PROXY_POOL) > 1:
                _rotate_proxy()
            continue
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
                proxies=_current_proxy,
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
