"""
Credentials, host registries, and DOI-prefix routing tables.

Token resolution order for HARVARD_TOKEN:
  1. HARVARD_TOKEN env var (already set)          → use it directly
  2. HARVARD_USER + HARVARD_PASSWORD env vars      → fetch via Dataverse API
  3. Hardcoded fallback (last resort)

Note: the auto-fetch API only works for accounts created directly on
Harvard Dataverse (built-in users).  Accounts that log in via institutional
SSO (Harvard Key, ORCID, Google) must supply HARVARD_TOKEN manually.
"""
import logging
import os

import requests

log = logging.getLogger(__name__)

HARVARD_BASE = "https://dataverse.harvard.edu"
HARVARD_API  = "https://dataverse.harvard.edu/api"


def _fetch_harvard_token(username: str, password: str) -> str:
    """
    Retrieve a Harvard Dataverse API token using username + password.

    Endpoint: GET /api/builtin-users/{username}/api-token?password={password}
    Returns the token string on success, or "" on failure.

    Only works for built-in (directly registered) accounts.
    Institutional SSO users must set HARVARD_TOKEN manually.
    """
    url = f"{HARVARD_BASE}/api/builtin-users/{username}/api-token"
    try:
        r = requests.get(url, params={"password": password}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("status") == "OK":
                token = data["data"]["apiToken"]
                log.info("Harvard token fetched automatically via API.")
                return token
        log.warning(
            f"Harvard token auto-fetch failed (HTTP {r.status_code}). "
            "If you use institutional login (SSO/ORCID), set HARVARD_TOKEN manually."
        )
    except Exception as e:
        log.warning(f"Harvard token auto-fetch error: {e}")
    return ""


def _resolve_harvard_token() -> str:
    """Return the best available Harvard API token."""
    # 1. Already set in environment
    if token := os.environ.get("HARVARD_TOKEN", ""):
        return token
    # 2. Auto-fetch using credentials
    user = os.environ.get("HARVARD_USER", "")
    pwd  = os.environ.get("HARVARD_PASSWORD", "")
    if user and pwd:
        if token := _fetch_harvard_token(user, pwd):
            return token
    # 3. Hardcoded fallback
    return "583ab5c1-8557-4202-9c45-a9798cbe95c2"


def _validate_harvard_token(token: str) -> bool:
    """
    Ping /api/users/:me to confirm the token is still valid.
    Returns True if valid, False if expired/rejected.
    Logs a prominent WARNING when the token fails.
    """
    if not token:
        log.warning("Harvard token is empty — public access only (rate-limited, restricted datasets hidden).")
        return False
    try:
        r = requests.get(
            f"{HARVARD_BASE}/api/users/:me",
            headers={"X-Dataverse-key": token},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            user = data.get("data", {}).get("identifier", "unknown")
            log.info(f"Harvard token valid — authenticated as {user}")
            return True
        ct = r.headers.get("Content-Type", "")
        if "text/html" in ct:
            log.warning(
                f"Harvard token check returned HTTP {r.status_code} HTML — "
                "your IP may be temporarily blocked by Harvard Dataverse (rate-limit). "
                "Wait a few hours and retry."
            )
        else:
            log.warning(
                f"Harvard token rejected (HTTP {r.status_code}) — "
                "token may have expired. Set a fresh HARVARD_TOKEN env var."
            )
    except Exception as e:
        log.warning(f"Harvard token validation failed (network error): {e}")
    return False


# ── API Credentials ───────────────────────────────────────────────────────────
HARVARD_TOKEN  = _resolve_harvard_token()
_validate_harvard_token(HARVARD_TOKEN)   # warn immediately if expired
BOREALIS_TOKEN = os.environ.get("BOREALIS_TOKEN", "4c0e5038-62ce-4ee4-a320-646cb2d25ef8")

# openICPSR uses Basic Auth (email + password) instead of an API token.
OPENICPSR_USER = os.environ.get("OPENICPSR_USER", "mahtab.shahsavan@fau.de")
OPENICPSR_PASS = os.environ.get("OPENICPSR_PASS", "Mahtabshahsavan@123")

# Dryad and QDR use OAuth / token-based auth
DRYAD_CLIENT_ID     = os.environ.get("DRYAD_CLIENT_ID",     "")
DRYAD_CLIENT_SECRET = os.environ.get("DRYAD_CLIENT_SECRET", "")
QDR_TOKEN           = os.environ.get("QDR_TOKEN",           "")

# ── Dataverse Host Registry ───────────────────────────────────────────────────
# Maps a name fragment (lowercased) to (base_url, api_token).
# Used by resolve_host() to find the right token for each institution.
DATAVERSE_REGISTRY: dict[str, tuple[str, str]] = {
    "harvard":      (HARVARD_BASE,                  HARVARD_TOKEN),
    "borealis":     ("https://borealisdata.ca",      BOREALIS_TOKEN),
    "dans":         ("https://ssh.datastations.nl",  ""),
    "dataverse.no": ("https://dataverse.no",         ""),
    "dataverseno":  ("https://dataverse.no",         ""),
    "dataverse.nl": ("https://dataverse.nl",         ""),
    "dataverseni":  ("https://dataverse.nl",         ""),
    "openicpsr":    ("https://www.openicpsr.org",    ""),  # Basic Auth — see fetchers/icpsr.py
    "icpsr":        ("https://www.openicpsr.org",    ""),
}

# ── DOI Prefix Routing ────────────────────────────────────────────────────────
# Maps DOI prefixes to platform names; used to skip Dataverse logic and route
# to the correct platform fetcher.
NON_DATAVERSE_PREFIXES: dict[str, str] = {
    "10.3886":  "icpsr",    # ICPSR / openICPSR
    "10.5281":  "zenodo",   # Zenodo
    "10.17605": "osf",      # Open Science Framework
    "10.6084":  "figshare", # figshare
    "10.5061":  "dryad",    # Dryad
}

# DOI prefixes to skip silently (no DB row, no download attempt).
SILENT_SKIP_PREFIXES: set[str] = set()
