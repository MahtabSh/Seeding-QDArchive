#!/usr/bin/env python3
"""
Classify uncertain projects via Claude API Message Batches.

Cost strategy (3 savings stacked):
  1. Batch API       — 50% off standard price
  2. Prompt caching  — ISIC section list cached → ~90% cheaper on that portion
  3. Short prompts   — 300-char description instead of 600

Estimated cost for ~19,500 projects:
  Haiku 4.5   → ~$2.50   (default, recommended)
  Sonnet 4.6  → ~$7.50   (use --model claude-sonnet-4-6 for harder cases)

Targets:
  - Projects with no row in classifications_combined (never LLM classified)
  - Projects with low/medium confidence in classifications_vote

Results → classifications_combined with model_name='claude-haiku-4-5' (or whichever
model you use). create_training_labels.py picks highest-confidence row per project,
so Claude's confident labels automatically win over uncertain qwen ones.

Workflow (two steps so you don't have to stay online):
  Step 1 — Submit:
      python3 classify_claude_batch.py --api-key sk-ant-...
      → prints BATCH_ID and saves it to batch_state.txt

  Step 2 — Poll + write results (run after ~1 hour):
      python3 classify_claude_batch.py --api-key sk-ant-... --poll msgbatch_xxx

  Or in one blocking call:
      python3 classify_claude_batch.py --api-key sk-ant-... --wait

  Dry run (no API calls, just shows count + cost estimate):
      python3 classify_claude_batch.py --dry-run

  Set API key via environment instead of flag:
      export ANTHROPIC_API_KEY=sk-ant-...
      python3 classify_claude_batch.py
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_DB    = "23692652-sq26.db"
DEFAULT_MODEL = "claude-haiku-4-5"
STATE_FILE    = "batch_state.txt"

# Pricing per 1M tokens (standard; batch API applies 50% discount automatically)
_PRICING = {
    "claude-haiku-4-5":  {"input": 1.00, "cache_read": 0.10, "output": 5.00},
    "claude-sonnet-4-6": {"input": 3.00, "cache_read": 0.30, "output": 15.00},
    "claude-opus-4-8":   {"input": 5.00, "cache_read": 0.50, "output": 25.00},
}

ISIC_SECTIONS: dict[str, str] = {
    "A": "Agriculture, forestry and fishing",
    "B": "Mining and quarrying",
    "C": "Manufacturing",
    "D": "Electricity, gas, steam and air conditioning supply",
    "E": "Water supply; sewerage, waste management and remediation activities",
    "F": "Construction",
    "G": "Wholesale and retail trade",
    "H": "Transportation and storage",
    "I": "Accommodation and food service activities",
    "J": "Publishing, broadcasting, and content production and distribution activities",
    "K": "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities",
    "L": "Financial and insurance activities",
    "M": "Real estate activities",
    "N": "Professional, scientific and technical activities",
    "O": "Administrative and support service activities",
    "P": "Public administration and defence; compulsory social security",
    "Q": "Education",
    "R": "Human health and social work activities",
    "S": "Arts, sports and recreation",
    "T": "Other service activities",
    "U": "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use",
    "V": "Activities of extraterritorial organizations and bodies",
}

# This is the cacheable system prompt — same for every project.
# Putting it in system[] with cache_control means it's cached after the first
# request and subsequent requests pay only 10% of input price for this portion.
_SYSTEM_PROMPT = """\
You are an expert at ISIC (International Standard Industrial Classification of All Economic Activities, Rev. 5).

Classify this research dataset by its SUBJECT MATTER — what the data is ABOUT, not that it is scientific research.
Do NOT default to section N just because this is academic work. Only use N when the dataset is specifically about the science/technology industry itself.

ISIC Sections:
""" + "\n".join(f"  {k}: {v}" for k, v in ISIC_SECTIONS.items()) + """

Instructions:
- Choose exactly ONE section letter (A–V) that best matches the topic of the data.
- Examples: ecology data → A; hospital records → R; election data → P; transport logs → H; school surveys → Q.
- Output ONLY valid JSON, nothing else.
- Format: {"section": "<letter>", "confidence": "<high|medium|low>"}"""


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_target_projects(
    conn: sqlite3.Connection,
    limit: int | None = None,
    tiers: list[str] | None = None,
) -> list[tuple[int, str, str]]:
    """
    Returns (project_id, title, description) for projects to classify.

    Default (tiers=None): projects with no existing row in classifications_combined.
    With tiers (e.g. ["low","medium"]): projects whose classifications_vote
    confidence is in that list — re-labels uncertain projects with Claude.
    """
    limit_sql = f"LIMIT {limit}" if limit else ""

    if tiers:
        placeholders = ",".join("?" * len(tiers))
        rows = conn.execute(f"""
            SELECT DISTINCT p.id, p.title, p.description
            FROM projects p
            JOIN classifications_vote cv ON p.id = cv.project_id
            WHERE cv.confidence IN ({placeholders})
            ORDER BY p.id
            {limit_sql}
        """, tiers).fetchall()
        return rows

    rows = conn.execute(f"""
        SELECT DISTINCT p.id, p.title, p.description
        FROM projects p
        LEFT JOIN classifications_combined cc ON p.id = cc.project_id
        WHERE cc.project_id IS NULL
        ORDER BY p.id
        {limit_sql}
    """).fetchall()
    return rows


def save_results(conn: sqlite3.Connection, results: list[dict], model: str) -> int:
    """Insert Claude classifications into classifications_combined."""
    saved = 0
    for r in results:
        conn.execute("""
            INSERT OR REPLACE INTO classifications_combined
                (project_id, model_name, section, section_name,
                 division, division_name, confidence, method,
                 files_used, file_source)
            VALUES (?, ?, ?, ?, '', '', ?, 'metadata', NULL, NULL)
        """, (
            r["project_id"],
            model,
            r["section"],
            ISIC_SECTIONS.get(r["section"], ""),
            r["confidence"],
        ))
        saved += 1
    conn.commit()
    return saved


# ── Cost estimation ────────────────────────────────────────────────────────────

def estimate_cost(n: int, model: str) -> None:
    p = _PRICING.get(model, _PRICING[DEFAULT_MODEL])
    sys_tokens  = 450   # system prompt (cached after first request)
    user_tokens = 100   # title + 300-char description
    out_tokens  = 25    # {"section": "R", "confidence": "high"}

    # First request: full system prompt write + cache write premium (25%)
    # Remaining: cache read (10% of input price)
    cache_write_cost = sys_tokens / 1e6 * p["input"] * 1.25 * 0.5   # batch 50% off
    cache_read_cost  = sys_tokens / 1e6 * p["cache_read"] * (n - 1) * 0.5
    input_cost       = user_tokens / 1e6 * p["input"] * n * 0.5
    output_cost      = out_tokens  / 1e6 * p["output"] * n * 0.5
    total = cache_write_cost + cache_read_cost + input_cost + output_cost

    print(f"\n  Model           : {model}")
    print(f"  Projects        : {n:,}")
    print(f"  Cache write     : ${cache_write_cost:.4f}")
    print(f"  Cache reads     : ${cache_read_cost:.2f}")
    print(f"  Variable input  : ${input_cost:.2f}")
    print(f"  Output          : ${output_cost:.2f}")
    print(f"  ─────────────────────────────")
    print(f"  TOTAL ESTIMATE  : ${total:.2f}  (Batch API 50% off applied)\n")


# ── Batch submission ───────────────────────────────────────────────────────────

def build_requests(projects: list[tuple[int, str, str]]) -> list[dict]:
    """Build Batch API request list."""
    reqs = []
    for pid, title, description in projects:
        desc_excerpt = (description or "")[:300].strip()
        user_text = (
            f"Title: {title or '(no title)'}\n"
            f"Description: {desc_excerpt or '(none)'}"
        )
        reqs.append({
            "custom_id": str(pid),
            "params": {
                "model": None,           # filled in submit_batch()
                "max_tokens": 64,
                "temperature": 0.0,
                "system": [
                    {
                        "type": "text",
                        "text": _SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},  # key cost saving
                    }
                ],
                "messages": [
                    {
                        "role": "user",
                        "content": user_text,
                    }
                ],
            },
        })
    return reqs


def submit_batch(client, projects: list[tuple[int, str, str]], model: str) -> str:
    """Submit batch and return batch_id."""
    log.info(f"Building {len(projects):,} batch requests…")
    reqs = build_requests(projects)
    for r in reqs:
        r["params"]["model"] = model

    log.info("Submitting to Claude Batch API…")
    batch = client.messages.batches.create(requests=reqs)
    batch_id = batch.id
    log.info(f"Batch submitted! ID: {batch_id}")
    log.info(f"Status: {batch.processing_status}")

    Path(STATE_FILE).write_text(f"{batch_id}\n{model}\n")
    log.info(f"Batch ID saved to {STATE_FILE} — use --poll {batch_id} to retrieve results later")
    return batch_id


# ── Polling + result retrieval ─────────────────────────────────────────────────

def poll_batch(client, batch_id: str, wait: bool = True) -> str | None:
    """Poll until batch ends. Returns status."""
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        status = batch.processing_status
        counts = batch.request_counts
        log.info(
            f"  [{status}]  "
            f"processing={counts.processing}  "
            f"succeeded={counts.succeeded}  "
            f"errored={counts.errored}"
        )
        if status == "ended":
            return status
        if not wait:
            log.info("Batch still running. Re-run with --poll to check again.")
            return None
        log.info("  Waiting 60s…")
        time.sleep(60)


def fetch_results(client, batch_id: str) -> list[dict]:
    """Download and parse batch results."""
    results = []
    errors  = 0

    for result in client.messages.batches.results(batch_id):
        pid = int(result.custom_id)

        if result.result.type != "succeeded":
            log.warning(f"  project {pid}: {result.result.type} — skipping")
            errors += 1
            continue

        text = result.result.message.content[0].text.strip()
        m = re.search(r'\{[^}]+\}', text)
        if not m:
            log.warning(f"  project {pid}: no JSON in response: {text[:80]}")
            errors += 1
            continue

        try:
            data = json.loads(m.group())
        except json.JSONDecodeError:
            log.warning(f"  project {pid}: invalid JSON: {m.group()}")
            errors += 1
            continue

        sec  = data.get("section", "").strip().upper()
        conf = data.get("confidence", "medium").lower()

        if sec not in ISIC_SECTIONS:
            log.warning(f"  project {pid}: invalid section '{sec}'")
            errors += 1
            continue

        if conf not in {"high", "medium", "low"}:
            conf = "medium"

        results.append({"project_id": pid, "section": sec, "confidence": conf})

    log.info(f"Parsed {len(results):,} results  ({errors} errors/skipped)")
    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Classify uncertain projects via Claude Batch API."
    )
    ap.add_argument("--db",       default=DEFAULT_DB)
    ap.add_argument("--api-key",  default=os.environ.get("ANTHROPIC_API_KEY", ""),
                    help="Anthropic API key (or set ANTHROPIC_API_KEY env var)")
    ap.add_argument("--model",    default=DEFAULT_MODEL,
                    help=f"Claude model (default: {DEFAULT_MODEL})")
    ap.add_argument("--limit",    type=int, default=None,
                    help="Max projects to include (for testing, e.g. --limit 100)")
    ap.add_argument("--poll",     metavar="BATCH_ID", default=None,
                    help="Poll an already-submitted batch and write results to DB")
    ap.add_argument("--wait",     action="store_true",
                    help="Submit AND wait for results (blocks until done)")
    ap.add_argument("--dry-run",  action="store_true",
                    help="Show target count and cost estimate — no API calls")
    ap.add_argument("--tiers",    default=None, metavar="low,medium",
                    help="Re-label projects whose vote confidence is in this list "
                         "(comma-separated: low,medium,high,very_high). "
                         "Default: only label projects with no LLM label at all.")
    args = ap.parse_args()
    tiers = [t.strip() for t in args.tiers.split(",")] if args.tiers else None

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}"); sys.exit(1)

    conn = sqlite3.connect(db_path)

    # ── Dry run ──
    if args.dry_run:
        projects = get_target_projects(conn, limit=args.limit, tiers=tiers)
        print(f"\n  Target projects : {len(projects):,}")
        estimate_cost(len(projects), args.model)
        conn.close()
        return

    # ── Need API key for everything else ──
    if not args.api_key:
        log.error("No API key. Use --api-key or set ANTHROPIC_API_KEY env var.")
        sys.exit(1)

    try:
        import anthropic
    except ImportError:
        log.error("anthropic SDK not installed. Run: pip install anthropic")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=args.api_key)

    # ── Poll existing batch ──
    if args.poll:
        batch_id = args.poll
        model = args.model
        # Try to read model from state file
        if Path(STATE_FILE).exists():
            lines = Path(STATE_FILE).read_text().strip().splitlines()
            if len(lines) >= 2:
                model = lines[1]
        log.info(f"Polling batch {batch_id} (model: {model})…")
        status = poll_batch(client, batch_id, wait=True)
        if status == "ended":
            log.info("Fetching results…")
            results = fetch_results(client, batch_id)
            saved = save_results(conn, results, model)
            log.info(f"Saved {saved:,} classifications to classifications_combined")
            log.info("Next steps:")
            log.info("  python3 create_training_labels.py --db 23692652-sq26.db --balance 20")
            log.info("  python3 classify_bert.py")
            log.info("  python3 classify_vote.py")
        conn.close()
        return

    # ── Submit (+ optionally wait) ──
    projects = get_target_projects(conn, limit=args.limit, tiers=tiers)
    if not projects:
        log.info("No uncertain projects to classify — all already done.")
        conn.close()
        return

    log.info(f"Target: {len(projects):,} projects")
    estimate_cost(len(projects), args.model)

    batch_id = submit_batch(client, projects, args.model)

    if args.wait:
        log.info("Waiting for batch to complete…")
        status = poll_batch(client, batch_id, wait=True)
        if status == "ended":
            log.info("Fetching results…")
            results = fetch_results(client, batch_id)
            saved = save_results(conn, results, args.model)
            log.info(f"Saved {saved:,} classifications to classifications_combined")
            log.info("Next steps:")
            log.info("  python3 create_training_labels.py --db 23692652-sq26.db --balance 20")
            log.info("  python3 classify_bert.py")
            log.info("  python3 classify_vote.py")
    else:
        log.info(f"\nBatch submitted. Come back in ~1 hour and run:")
        log.info(f"  python3 classify_claude_batch.py --api-key YOUR_KEY --poll {batch_id}")

    conn.close()


if __name__ == "__main__":
    main()
