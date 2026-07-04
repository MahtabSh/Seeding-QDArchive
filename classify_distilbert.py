#!/usr/bin/env python3
"""
DistilBERT fine-tuned ISIC classifier for QDArchive.

Two phases:
  1. Extract silver labels  — projects where ≥2 existing classifiers agree
  2. Fine-tune DistilBERT   — weighted cross-entropy to handle class imbalance
  3. Classify all projects  — fast inference on all 53k

Requirements:
    pip install transformers torch scikit-learn

Results → table: classifications_distilbert   (never touches other tables)

Usage:
    # Full pipeline (train + classify):
    python3 classify_distilbert.py

    # Train only (saves model to models/distilbert_isic/):
    python3 classify_distilbert.py --train-only

    # Classify only (model already trained):
    python3 classify_distilbert.py --classify-only

    # Limit projects during classify:
    python3 classify_distilbert.py --classify-only --limit 500

    # Re-classify everything:
    python3 classify_distilbert.py --classify-only --redo

    # Show silver label stats without training:
    python3 classify_distilbert.py --show-labels
"""
from __future__ import annotations

import argparse
import json
import logging
import os
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

DEFAULT_DB     = "23692652-sq26.db"
TABLE          = "classifications_distilbert"
MODEL_DIR      = Path("models/distilbert_isic")
BASE_MODEL     = "distilbert-base-uncased"
MAX_LEN        = 256
TRAIN_BATCH    = 16
EVAL_BATCH     = 32
INFER_BATCH    = 64
EPOCHS         = 5
MAX_PER_CLASS  = 300   # cap dominant class to reduce imbalance

from qdarchive.isic import SECTIONS, DIVISIONS, SECTION_MAP, DIVISION_MAP
from qdarchive.nli_classifier import _DIVISION_DESCRIPTIONS


# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_SQL = f"""CREATE TABLE {TABLE} (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id    INTEGER NOT NULL,
    model_name    TEXT    NOT NULL DEFAULT 'distilbert-isic',
    tier          TEXT    NOT NULL DEFAULT 'metadata',
    section       TEXT, section_name  TEXT,
    division      TEXT, division_name TEXT,
    confidence    TEXT, method        TEXT,
    classified_at TEXT DEFAULT (datetime('now')),
    UNIQUE(project_id, model_name, tier)
)"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)
    ).fetchone():
        conn.execute(_CREATE_SQL)
        conn.commit()


# ── Silver label extraction ────────────────────────────────────────────────────

def extract_silver_labels(conn: sqlite3.Connection) -> list[dict]:
    """
    Return projects where ≥2 classifier tables agree on section.
    Tables checked (in order of preference): classifications_st,
    classifications_nli, classifications_combined.
    """
    available = [
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('classifications_st','classifications_nli','classifications_combined')"
        )
    ]
    log.info(f"Classifier tables available for silver labels: {available}")

    if len(available) < 2:
        log.error("Need at least 2 classifier tables to extract silver labels.")
        sys.exit(1)

    # Build pairwise agreement query
    # Take the first two available tables
    t1, t2 = available[0], available[1]
    rows = conn.execute(f"""
        SELECT c1.project_id, c1.section,
               p.title, p.description,
               GROUP_CONCAT(DISTINCT k.keyword) AS keywords
        FROM {t1} c1
        JOIN {t2} c2 ON c2.project_id = c1.project_id
            AND c2.section = c1.section
        JOIN projects p ON p.id = c1.project_id
        LEFT JOIN keywords k ON k.project_id = p.id
        GROUP BY c1.project_id
    """).fetchall()

    labels = [
        {"project_id": r[0], "section": r[1],
         "title": r[2] or "", "description": r[3] or "",
         "keywords": r[4] or ""}
        for r in rows
    ]
    log.info(f"Silver labels extracted: {len(labels)}")
    return labels


def show_label_stats(labels: list[dict]) -> None:
    from collections import Counter
    counts = Counter(lb["section"] for lb in labels)
    print(f"\nSilver labels: {len(labels)} total\n")
    print(f"{'Section':<6} {'Name':<45} {'Count':>6}")
    print("-" * 60)
    for sec, n in counts.most_common():
        print(f"{sec:<6} {SECTION_MAP.get(sec,''):<45} {n:>6}")
    print()


# ── Text builder ───────────────────────────────────────────────────────────────

def build_text(title: str, description: str, keywords: str) -> str:
    parts: list[str] = []
    if title:
        parts.extend([title.strip()] * 3)
    kw = [k.strip() for k in keywords.split(",") if k.strip()]
    if kw:
        parts.extend([" ".join(kw[:20])] * 2)
    if description:
        parts.append(description.strip()[:400])
    return " ".join(parts)


# ── Training ───────────────────────────────────────────────────────────────────

def train(labels: list[dict], model_dir: Path, epochs: int = EPOCHS, max_per_class: int = MAX_PER_CLASS) -> tuple[list[str], dict]:
    """Fine-tune DistilBERT. Returns (label_list, label2id)."""
    try:
        import torch
        from transformers import (
            DistilBertForSequenceClassification,
            DistilBertTokenizerFast,
            TrainingArguments,
            Trainer,
        )
        from torch.utils.data import Dataset as TorchDataset
        from sklearn.model_selection import train_test_split
    except ImportError:
        raise ImportError("pip install transformers torch scikit-learn")

    from collections import Counter
    import random

    random.seed(42)

    # Build class list (only sections with ≥5 examples)
    counts = Counter(lb["section"] for lb in labels)
    valid_sections = sorted(s for s, n in counts.items() if n >= 5)
    labels = [lb for lb in labels if lb["section"] in valid_sections]

    # Cap dominant classes to reduce imbalance
    capped: list[dict] = []
    per_class: dict[str, list] = {}
    for lb in labels:
        per_class.setdefault(lb["section"], []).append(lb)
    for sec, items in per_class.items():
        random.shuffle(items)
        capped.extend(items[:max_per_class])
    random.shuffle(capped)

    label2id = {s: i for i, s in enumerate(valid_sections)}
    id2label = {i: s for s, i in label2id.items()}

    log.info(f"Training on {len(capped)} samples across {len(valid_sections)} sections")
    show_label_stats(capped)

    texts  = [build_text(lb["title"], lb["description"], lb["keywords"]) for lb in capped]
    ids    = [label2id[lb["section"]] for lb in capped]

    tr_texts, val_texts, tr_ids, val_ids = train_test_split(
        texts, ids, test_size=0.15, random_state=42, stratify=ids
    )

    tokenizer = DistilBertTokenizerFast.from_pretrained(BASE_MODEL)

    tr_enc  = tokenizer(tr_texts,  truncation=True, max_length=MAX_LEN, padding=True, return_tensors="pt")
    val_enc = tokenizer(val_texts, truncation=True, max_length=MAX_LEN, padding=True, return_tensors="pt")

    class ISICDataset(TorchDataset):
        def __init__(self, enc, labs):
            self.enc  = enc
            self.labs = labs
        def __len__(self):  return len(self.labs)
        def __getitem__(self, i):
            return {k: v[i] for k, v in self.enc.items()} | \
                   {"labels": torch.tensor(self.labs[i])}

    tr_ds  = ISICDataset(tr_enc,  tr_ids)
    val_ds = ISICDataset(val_enc, val_ids)

    # Class weights for weighted loss (inverse frequency)
    count_arr = [counts.get(id2label[i], 1) for i in range(len(valid_sections))]
    total = sum(count_arr)
    weights = torch.tensor(
        [total / (len(valid_sections) * c) for c in count_arr], dtype=torch.float
    )

    model = DistilBertForSequenceClassification.from_pretrained(
        BASE_MODEL,
        num_labels=len(valid_sections),
        id2label=id2label,
        label2id=label2id,
    )

    class WeightedTrainer(Trainer):
        def compute_loss(self, model, inputs, return_outputs=False, **kw):
            labs = inputs.pop("labels")
            out  = model(**inputs)
            loss = torch.nn.CrossEntropyLoss(weight=weights.to(out.logits.device))(
                out.logits, labs
            )
            return (loss, out) if return_outputs else loss

    training_args = TrainingArguments(
        output_dir=str(model_dir / "checkpoints"),
        num_train_epochs=epochs,
        per_device_train_batch_size=TRAIN_BATCH,
        per_device_eval_batch_size=EVAL_BATCH,
        eval_strategy="epoch",
        save_strategy="best",
        load_best_model_at_end=True,
        metric_for_best_model="eval_loss",
        logging_steps=20,
        warmup_ratio=0.1,
        weight_decay=0.01,
        report_to="none",
    )

    trainer = WeightedTrainer(
        model=model,
        args=training_args,
        train_dataset=tr_ds,
        eval_dataset=val_ds,
    )

    log.info(f"Fine-tuning {BASE_MODEL} for {EPOCHS} epochs…")
    t0 = time.time()
    trainer.train()
    log.info(f"Training done in {(time.time()-t0)/60:.1f} min")

    model_dir.mkdir(parents=True, exist_ok=True)
    trainer.save_model(str(model_dir))
    tokenizer.save_pretrained(str(model_dir))

    # Save label list for inference
    (model_dir / "label_list.json").write_text(json.dumps(valid_sections))
    log.info(f"Model saved to {model_dir}")

    return valid_sections, label2id


# ── Division prediction (cosine similarity within section) ────────────────────

_div_embs_cache: dict = {}

def predict_division(section: str, text_emb) -> tuple[str, str]:
    """Return (division_code, division_name) using cosine similarity."""
    import numpy as np
    global _div_embs_cache

    if "model" not in _div_embs_cache:
        from sentence_transformers import SentenceTransformer
        _div_embs_cache["model"] = SentenceTransformer("all-MiniLM-L6-v2")

    if section not in _div_embs_cache:
        st_model = _div_embs_cache["model"]
        divs = [(div, name) for sec, div, name in DIVISIONS if sec == section]
        if not divs:
            return "72", "Scientific research and development"
        desc = [_DIVISION_DESCRIPTIONS.get(d, n) for d, n in divs]
        embs = st_model.encode(desc, normalize_embeddings=True)
        _div_embs_cache[section] = (divs, embs)

    divs, embs = _div_embs_cache[section]
    scores = text_emb @ embs.T
    best   = int(scores.argmax())
    return divs[best][0], divs[best][1]


# ── Inference ─────────────────────────────────────────────────────────────────

def classify_all(
    conn: sqlite3.Connection,
    model_dir: Path,
    redo: bool,
    limit: int | None,
) -> None:
    try:
        import torch
        from transformers import (
            DistilBertForSequenceClassification,
            DistilBertTokenizerFast,
        )
        from sentence_transformers import SentenceTransformer
    except ImportError:
        raise ImportError("pip install transformers torch sentence-transformers")

    label_list = json.loads((model_dir / "label_list.json").read_text())
    id2label   = {i: s for i, s in enumerate(label_list)}

    log.info(f"Loading fine-tuned model from {model_dir}")
    tokenizer = DistilBertTokenizerFast.from_pretrained(str(model_dir))
    model     = DistilBertForSequenceClassification.from_pretrained(str(model_dir))
    model.eval()

    st_model = SentenceTransformer("all-MiniLM-L6-v2")

    where = "" if redo else f"""WHERE p.id NOT IN (
        SELECT project_id FROM {TABLE} WHERE model_name='distilbert-isic' AND tier='metadata')"""

    rows = conn.execute(f"""
        SELECT p.id, p.title, p.description,
               GROUP_CONCAT(DISTINCT k.keyword) AS keywords
        FROM projects p
        LEFT JOIN keywords k ON k.project_id = p.id
        {where}
        GROUP BY p.id
    """).fetchall()

    projects = rows[:limit] if limit else rows
    log.info(f"Projects to classify: {len(projects)}")

    total = len(projects)
    saved = 0
    t0    = time.time()

    for start in range(0, total, INFER_BATCH):
        chunk = projects[start: start + INFER_BATCH]
        texts = [
            build_text(r[1] or "", r[2] or "", r[3] or "")
            for r in chunk
        ]

        enc = tokenizer(
            texts,
            truncation=True,
            max_length=MAX_LEN,
            padding=True,
            return_tensors="pt",
        )

        with torch.no_grad():
            logits = model(**enc).logits          # (B, C)
            probs  = torch.softmax(logits, dim=1) # (B, C)

        top_probs, top_ids = probs.max(dim=1)

        # Encode texts for division prediction
        text_embs = st_model.encode(texts, normalize_embeddings=True)

        rows_to_insert = []
        for j, (proj_row, prob, sec_idx) in enumerate(
            zip(chunk, top_probs.tolist(), top_ids.tolist())
        ):
            section    = id2label[sec_idx]
            division, div_name = predict_division(section, text_embs[j])
            confidence = (
                "high"   if prob > 0.70 else
                "medium" if prob > 0.45 else
                "low"
            )
            rows_to_insert.append((
                proj_row[0], "distilbert-isic", "metadata",
                section, SECTION_MAP.get(section, ""),
                division, div_name,
                confidence, "distilbert_finetuned",
            ))

        conn.executemany(f"""
            INSERT INTO {TABLE}
                (project_id, model_name, tier,
                 section, section_name, division, division_name,
                 confidence, method)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(project_id, model_name, tier) DO UPDATE SET
                section=excluded.section, section_name=excluded.section_name,
                division=excluded.division, division_name=excluded.division_name,
                confidence=excluded.confidence, method=excluded.method,
                classified_at=datetime('now')
        """, rows_to_insert)
        conn.commit()
        saved += len(chunk)

        if saved % 2000 == 0 or saved == total:
            elapsed = time.time() - t0
            rate    = saved / elapsed
            eta     = (total - saved) / rate / 60 if rate > 0 else 0
            log.info(f"  {saved}/{total}  ({rate:.1f} proj/s)  ETA {eta:.0f} min")

    log.info(f"Done. {saved} projects saved to {TABLE}.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fine-tune DistilBERT on silver labels, then classify all projects."
    )
    ap.add_argument("--db",            default=DEFAULT_DB)
    ap.add_argument("--model-dir",     default=str(MODEL_DIR),
                    help="Where to save / load the fine-tuned model")
    ap.add_argument("--train-only",    action="store_true",
                    help="Only fine-tune, do not classify")
    ap.add_argument("--classify-only", action="store_true",
                    help="Only classify (model must already be trained)")
    ap.add_argument("--show-labels",   action="store_true",
                    help="Show silver label statistics and exit")
    ap.add_argument("--limit",         type=int, default=None,
                    help="Limit number of projects to classify")
    ap.add_argument("--redo",          action="store_true",
                    help="Re-classify projects already in the table")
    ap.add_argument("--epochs",        type=int, default=EPOCHS)
    ap.add_argument("--max-per-class", type=int, default=MAX_PER_CLASS,
                    help="Cap training samples per section (default 300)")
    args = ap.parse_args()

    model_dir = Path(args.model_dir)
    db_path   = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}"); sys.exit(1)

    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    # ── Show labels only ──────────────────────────────────────────────────────
    if args.show_labels:
        labels = extract_silver_labels(conn)
        show_label_stats(labels)
        conn.close()
        return

    # ── Train phase ───────────────────────────────────────────────────────────
    if not args.classify_only:
        if model_dir.exists() and (model_dir / "config.json").exists():
            log.info(f"Model already exists at {model_dir}. Use --classify-only to skip training.")
            log.info("Delete the model directory to retrain.")
        else:
            labels = extract_silver_labels(conn)
            if len(labels) < 50:
                log.error(
                    f"Only {len(labels)} silver labels found. "
                    "Run classify_st.py --no-file-content first to generate more labels."
                )
                conn.close()
                sys.exit(1)
            train(labels, model_dir, epochs=args.epochs, max_per_class=args.max_per_class)

    if args.train_only:
        conn.close()
        return

    # ── Classify phase ────────────────────────────────────────────────────────
    if not (model_dir / "config.json").exists():
        log.error(
            f"No trained model found at {model_dir}. "
            "Run without --classify-only first to train."
        )
        conn.close()
        sys.exit(1)

    classify_all(conn, model_dir, args.redo, args.limit)
    conn.close()


if __name__ == "__main__":
    main()
