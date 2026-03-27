"""
Persistent pagination state so interrupted runs can resume where they left off.

State is stored as a JSON file alongside the SQLite database.
Delete the .progress.json file to force a full re-crawl.
"""
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)


class ProgressState:
    def __init__(self, path: Path):
        self.path    = path
        self._state: dict = {}
        if path.exists():
            try:
                with open(path) as f:
                    self._state = json.load(f)
                log.info(f"Resuming from progress file: {path}")
            except Exception:
                pass

    def get(self, query: str, default: int = 0) -> int:
        v = self._state.get(query, default)
        return default if v == "done" else int(v)

    def get_raw(self, query: str):
        """Return the raw stored value (e.g. an OAI-PMH resumption token string)."""
        v = self._state.get(query)
        return None if v == "done" else v

    def save_raw(self, query: str, value):
        """Save any value (e.g. an OAI-PMH resumption token string)."""
        self._state[query] = value
        self._flush()

    def save(self, query: str, start: int):
        self._state[query] = start
        self._flush()

    def mark_done(self, query: str):
        self._state[query] = "done"
        self._flush()

    def is_done(self, query: str) -> bool:
        return self._state.get(query) == "done"

    def _flush(self):
        with open(self.path, "w") as f:
            json.dump(self._state, f, indent=2)
