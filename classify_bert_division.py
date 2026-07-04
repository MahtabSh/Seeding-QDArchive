#!/usr/bin/env python3
"""
Two-stage BERT division classifier for ISIC Rev.5.

Stage 1 — Section: handled by existing classify_bert.py (22 classes)
Stage 2 — Division: this script (86 classes)

Training data:
  Primary  — LLM division labels from classifications_combined
             (qwen2.5:7b + mistral: ~20k labeled projects)
  PDF augmentation — division descriptions from ISIC5_Exp_Notes_11Mar2024.pdf
             Each division description is added as a synthetic training example,
             repeated to boost under-represented divisions (< 50 LLM examples).
  Anchor augmentation — enriched anchor texts from classify_embed.py
             Used as extra description text, repeated 3x per division.

The section letter is derived deterministically from the predicted division code,
so this classifier implicitly handles both levels.

Output table : classifications_bert_division
Saved model  : models/bert_division_v1/

Usage:
    python3 classify_bert_division.py --db 23692652-sq26.db
    python3 classify_bert_division.py --db 23692652-sq26.db --epochs 5
    python3 classify_bert_division.py --db 23692652-sq26.db --retrain
    python3 classify_bert_division.py --db 23692652-sq26.db --stats
    python3 classify_bert_division.py --db 23692652-sq26.db --pdf /path/to/ISIC5_Exp_Notes_11Mar2024.pdf
"""
from __future__ import annotations

import argparse
import logging
import math
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

DEFAULT_DB      = "23692652-sq26.db"
DEFAULT_MODEL   = "distilbert-base-uncased"
MODEL_SAVE      = "models/bert_division_v1"
TABLE           = "classifications_bert_division"
DEFAULT_PDF     = "/Users/mahtab/Desktop/ISIC5_Exp_Notes_11Mar2024.pdf"

# ── ISIC Division → Section mapping ───────────────────────────────────────────

DIVISION_TO_SECTION: dict[str, tuple[str, str]] = {
    # (section_letter, section_name)
    "01": ("A", "Agriculture, forestry and fishing"),
    "02": ("A", "Agriculture, forestry and fishing"),
    "03": ("A", "Agriculture, forestry and fishing"),
    "05": ("B", "Mining and quarrying"),
    "06": ("B", "Mining and quarrying"),
    "07": ("B", "Mining and quarrying"),
    "08": ("B", "Mining and quarrying"),
    "09": ("B", "Mining and quarrying"),
    "10": ("C", "Manufacturing"),
    "11": ("C", "Manufacturing"),
    "12": ("C", "Manufacturing"),
    "13": ("C", "Manufacturing"),
    "14": ("C", "Manufacturing"),
    "15": ("C", "Manufacturing"),
    "16": ("C", "Manufacturing"),
    "17": ("C", "Manufacturing"),
    "18": ("C", "Manufacturing"),
    "19": ("C", "Manufacturing"),
    "20": ("C", "Manufacturing"),
    "21": ("C", "Manufacturing"),
    "22": ("C", "Manufacturing"),
    "23": ("C", "Manufacturing"),
    "24": ("C", "Manufacturing"),
    "25": ("C", "Manufacturing"),
    "26": ("C", "Manufacturing"),
    "27": ("C", "Manufacturing"),
    "28": ("C", "Manufacturing"),
    "29": ("C", "Manufacturing"),
    "30": ("C", "Manufacturing"),
    "31": ("C", "Manufacturing"),
    "32": ("C", "Manufacturing"),
    "33": ("C", "Manufacturing"),
    "35": ("D", "Electricity, gas, steam and air conditioning supply"),
    "36": ("E", "Water supply; sewerage, waste management and remediation activities"),
    "37": ("E", "Water supply; sewerage, waste management and remediation activities"),
    "38": ("E", "Water supply; sewerage, waste management and remediation activities"),
    "39": ("E", "Water supply; sewerage, waste management and remediation activities"),
    "41": ("F", "Construction"),
    "42": ("F", "Construction"),
    "43": ("F", "Construction"),
    "45": ("G", "Wholesale and retail trade"),
    "46": ("G", "Wholesale and retail trade"),
    "47": ("G", "Wholesale and retail trade"),
    "49": ("H", "Transportation and storage"),
    "50": ("H", "Transportation and storage"),
    "51": ("H", "Transportation and storage"),
    "52": ("H", "Transportation and storage"),
    "53": ("H", "Transportation and storage"),
    "55": ("I", "Accommodation and food service activities"),
    "56": ("I", "Accommodation and food service activities"),
    "58": ("J", "Publishing, broadcasting, and content production and distribution activities"),
    "59": ("J", "Publishing, broadcasting, and content production and distribution activities"),
    "60": ("J", "Publishing, broadcasting, and content production and distribution activities"),
    "61": ("K", "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities"),
    "62": ("K", "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities"),
    "63": ("K", "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities"),
    "64": ("L", "Financial and insurance activities"),
    "65": ("L", "Financial and insurance activities"),
    "66": ("L", "Financial and insurance activities"),
    "68": ("M", "Real estate activities"),
    "69": ("N", "Professional, scientific and technical activities"),
    "70": ("N", "Professional, scientific and technical activities"),
    "71": ("N", "Professional, scientific and technical activities"),
    "72": ("N", "Professional, scientific and technical activities"),
    "73": ("N", "Professional, scientific and technical activities"),
    "74": ("N", "Professional, scientific and technical activities"),
    "75": ("N", "Professional, scientific and technical activities"),
    "77": ("O", "Administrative and support service activities"),
    "78": ("O", "Administrative and support service activities"),
    "79": ("O", "Administrative and support service activities"),
    "80": ("O", "Administrative and support service activities"),
    "81": ("O", "Administrative and support service activities"),
    "82": ("O", "Administrative and support service activities"),
    "84": ("P", "Public administration and defence; compulsory social security"),
    "85": ("Q", "Education"),
    "86": ("R", "Human health and social work activities"),
    "87": ("R", "Human health and social work activities"),
    "88": ("R", "Human health and social work activities"),
    "90": ("S", "Arts, sports and recreation"),
    "91": ("S", "Arts, sports and recreation"),
    "92": ("S", "Arts, sports and recreation"),
    "93": ("S", "Arts, sports and recreation"),
    "94": ("T", "Other service activities"),
    "96": ("T", "Other service activities"),
    "97": ("U", "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use"),
    "98": ("U", "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use"),
    "99": ("V", "Activities of extraterritorial organizations and bodies"),
}

# All valid division codes (from ISIC Rev.5)
ALL_DIVISIONS = sorted(DIVISION_TO_SECTION.keys())

# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_SQL = f"""CREATE TABLE IF NOT EXISTS {TABLE} (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id    INTEGER NOT NULL UNIQUE,
    section       TEXT,
    section_name  TEXT,
    division      TEXT,
    division_name TEXT,
    confidence    TEXT,
    prob_top1     REAL,
    prob_top2     REAL,
    division_top2 TEXT,
    tier          TEXT DEFAULT 'metadata',
    classified_at TEXT DEFAULT (datetime('now'))
)"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_SQL)
    conn.commit()


# ── Text preparation ──────────────────────────────────────────────────────────

def make_text(
    title: str | None,
    description: str | None,
    keywords: str | None = None,
    file_names: str | None = None,
) -> str:
    parts = []
    if title:
        parts.append(title.strip())
    if description:
        parts.append(description.strip()[:600])
    if keywords:
        parts.append(keywords.strip()[:200])
    if file_names:
        names = " ".join(
            f.rsplit(".", 1)[0].replace("_", " ").replace("-", " ")
            for f in file_names.split("|")[:10]
        )
        parts.append(names[:200])
    return " ".join(parts) or "untitled"


def confidence_from_prob(p: float) -> str:
    if p >= 0.80:
        return "very_high"
    if p >= 0.60:
        return "high"
    if p >= 0.40:
        return "medium"
    return "low"


# ── PDF description extraction ────────────────────────────────────────────────

def extract_pdf_descriptions(pdf_path: str) -> dict[str, str]:
    """
    Parse the ISIC Explanatory Notes PDF and extract a description block for
    each division. Returns {div_code: description_text}.
    """
    try:
        import pdfplumber
    except ImportError:
        log.warning("pdfplumber not installed — skipping PDF augmentation. "
                    "Install with: pip3 install pdfplumber")
        return {}

    if not Path(pdf_path).exists():
        log.warning(f"PDF not found at {pdf_path} — skipping PDF augmentation")
        return {}

    log.info(f"Parsing PDF descriptions from {pdf_path}…")

    page_texts = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                page_texts.append(t)
    except Exception as e:
        log.warning(f"PDF parsing error: {e} — skipping PDF augmentation")
        return {}

    full_text = "\n".join(page_texts)

    # Find division blocks: header line followed by "This division includes..."
    div_pattern = re.compile(
        r"\n(\d{2}) ([A-Z][^\n]{5,100})\n"
        r"((?:This division|This section) (?:includes|also includes)[^\n]*"
        r"(?:\n(?!\d{2,4} [A-Z]|\n[A-V] [A-Z]).+)*)",
        re.IGNORECASE,
    )

    # Collect all division starts for boundary detection
    all_div_starts = re.compile(
        r"\n(\d{2}) ([A-Z][^\n]{5,100})\n(?=This division|This section)",
        re.IGNORECASE,
    )
    starts = list(all_div_starts.finditer(full_text))
    start_positions = {m.group(1): m.start() for m in starts}

    # Extract text between consecutive division starts
    descriptions = {}
    for i, m in enumerate(starts):
        code = m.group(1)
        if code not in DIVISION_TO_SECTION:
            continue
        block_start = m.start() + 1
        block_end = starts[i + 1].start() if i + 1 < len(starts) else len(full_text)
        block = full_text[block_start:block_end]

        # Cut before first group-level code (3-digit: 011, 021, etc.)
        group_match = re.search(r"\n\d{3} [A-Z]", block)
        if group_match:
            block = block[:group_match.start()]

        # Collapse whitespace
        block = re.sub(r"\s+", " ", block).strip()

        if len(block) > 50:
            descriptions[code] = block

    log.info(f"Extracted PDF descriptions for {len(descriptions)} divisions")
    return descriptions


# ── Anchor texts (from classify_embed.py — curated per division) ──────────────

def get_embed_anchors() -> dict[str, tuple[str, str]]:
    """
    Load enriched anchor texts from classify_embed.py.
    Returns {div_code: (division_name, anchor_text)}.
    """
    try:
        import importlib.util, sys as _sys
        spec = importlib.util.spec_from_file_location(
            "classify_embed",
            Path(__file__).parent / "classify_embed.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        anchors = {}
        for entry in mod.ISIC_DIVISIONS:
            code, _sec, _sec_name, div_name, anchor = entry
            anchors[code] = (div_name, anchor)
        return anchors
    except Exception as e:
        log.warning(f"Could not load embed anchors: {e}")
        return {}


# ── Training data ──────────────────────────────────────────────────────────────

def load_training_data(
    conn: sqlite3.Connection,
    pdf_descriptions: dict[str, str],
    embed_anchors: dict[str, tuple[str, str]],
) -> tuple[list[str], list[str]]:
    """
    Build training dataset:
      1. LLM-labeled projects from classifications_combined
      2. PDF description texts (repeated for small classes)
      3. Embed anchor texts (repeated 3x per division)

    Returns (texts, division_codes).
    """
    texts: list[str] = []
    labels: list[str] = []

    # --- Source 1: LLM division labels ---
    rows = conn.execute("""
        SELECT cc.division, p.title, p.description,
               GROUP_CONCAT(DISTINCT k.keyword) AS keywords,
               (SELECT GROUP_CONCAT(file_name, '|')
                FROM (SELECT DISTINCT file_name FROM files
                      WHERE project_id = p.id LIMIT 10)) AS file_names
        FROM classifications_combined cc
        JOIN projects p ON p.id = cc.project_id
        LEFT JOIN keywords k ON k.project_id = p.id
        WHERE cc.model_name IN ('qwen2.5:7b', 'mistral')
          AND cc.division IS NOT NULL
          AND cc.division != ''
          AND cc.division IN (SELECT DISTINCT division FROM classifications_combined)
        GROUP BY cc.project_id
    """).fetchall()

    # Filter to known divisions only
    for div, title, desc, kw, fn in rows:
        if div in DIVISION_TO_SECTION:
            texts.append(make_text(title, desc, kw, fn))
            labels.append(div)

    # Count per division to know what needs boosting
    from collections import Counter
    div_counts = Counter(labels)
    log.info(f"LLM labels: {len(texts):,} examples across {len(div_counts)} divisions")

    # --- Source 2: PDF descriptions (augmentation) ---
    pdf_added = 0
    for code in ALL_DIVISIONS:
        if code not in pdf_descriptions:
            continue
        desc_text = pdf_descriptions[code]
        current_count = div_counts.get(code, 0)
        # Repeat to reach at least 50 examples for small divisions
        reps = max(1, min(50, max(1, 50 - current_count)))
        for _ in range(reps):
            texts.append(desc_text)
            labels.append(code)
        pdf_added += reps
        div_counts[code] += reps

    log.info(f"PDF augmentation: added {pdf_added} synthetic examples")

    # --- Source 3: Embed anchor texts ---
    # Repeat more for divisions still under-represented after PDF augmentation
    anchor_added = 0
    for code, (div_name, anchor) in embed_anchors.items():
        if code not in DIVISION_TO_SECTION:
            continue
        anchor_text = f"{div_name}: {anchor}"
        current_count = div_counts.get(code, 0)
        reps = max(3, min(30, max(3, 30 - current_count // 5)))
        for _ in range(reps):
            texts.append(anchor_text)
            labels.append(code)
        anchor_added += reps
        div_counts[code] = div_counts.get(code, 0) + reps

    log.info(f"Anchor augmentation: added {anchor_added} synthetic examples")
    log.info(f"Total training examples: {len(texts):,} across {len(set(labels))} divisions")

    return texts, labels


# ── PyTorch dataset ───────────────────────────────────────────────────────────

def make_dataset(texts: list[str], labels: list[int], tokenizer, max_length: int):
    import torch
    from torch.utils.data import Dataset

    class TextDataset(Dataset):
        def __init__(self):
            self.encodings = tokenizer(
                texts,
                truncation=True,
                padding=True,
                max_length=max_length,
                return_tensors="pt",
            )
            self.labels = torch.tensor(labels, dtype=torch.long)

        def __len__(self):
            return len(self.labels)

        def __getitem__(self, idx):
            return {
                "input_ids":      self.encodings["input_ids"][idx],
                "attention_mask": self.encodings["attention_mask"][idx],
                "labels":         self.labels[idx],
            }

    return TextDataset()


# ── Training ──────────────────────────────────────────────────────────────────

def train_model(
    texts: list[str],
    labels_str: list[str],
    model_name: str,
    save_dir: str,
    epochs: int,
    batch_size: int,
    max_length: int,
    lr: float,
) -> tuple:
    try:
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, random_split
        from torch.optim import AdamW
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        from sklearn.utils.class_weight import compute_class_weight
        import numpy as np
    except ImportError as e:
        log.error(f"Missing dependency: {e}  — pip install transformers torch scikit-learn")
        sys.exit(1)

    # Device
    if torch.backends.mps.is_available():
        device = torch.device("mps")
        log.info("Device: Apple Silicon MPS")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
        log.info(f"Device: CUDA ({torch.cuda.get_device_name(0)})")
    else:
        device = torch.device("cpu")
        log.info("Device: CPU (training will be slow)")

    # Only keep divisions that appear in training data
    label_set = sorted(set(labels_str))
    label2id  = {l: i for i, l in enumerate(label_set)}
    id2label  = {i: l for l, i in label2id.items()}
    labels_int = [label2id[l] for l in labels_str]

    log.info(f"Classes: {len(label_set)} divisions")

    # Weighted cross-entropy for class imbalance
    cw = compute_class_weight("balanced", classes=np.array(label_set), y=np.array(labels_str))
    class_weights = torch.tensor(cw, dtype=torch.float32).to(device)
    log.info(f"Class weight range: [{cw.min():.2f}, {cw.max():.2f}]")

    log.info(f"Loading model: {model_name}")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(
        model_name,
        num_labels=len(label_set),
        id2label=id2label,
        label2id=label2id,
    )
    model = model.to(device)

    log.info(f"Tokenizing {len(texts):,} examples (max_length={max_length})…")
    dataset = make_dataset(texts, labels_int, tokenizer, max_length)

    val_size   = max(1, int(0.1 * len(dataset)))
    train_size = len(dataset) - val_size
    train_ds, val_ds = random_split(
        dataset, [train_size, val_size],
        generator=torch.Generator().manual_seed(42)
    )

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_ds,   batch_size=batch_size * 2, shuffle=False)

    optimizer    = AdamW(model.parameters(), lr=lr, weight_decay=0.01)
    total_steps  = len(train_loader) * epochs
    warmup_steps = max(1, total_steps // 10)

    def get_lr(step):
        if step < warmup_steps:
            return step / warmup_steps
        progress = (step - warmup_steps) / (total_steps - warmup_steps)
        return max(0.0, 1.0 - progress)

    scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, get_lr)
    loss_fn   = nn.CrossEntropyLoss(weight=class_weights)

    log.info(f"Training: {epochs} epochs, batch={batch_size}, lr={lr}, "
             f"train={train_size:,}, val={val_size:,}")

    best_val_acc = 0.0
    t0 = time.time()

    for epoch in range(1, epochs + 1):
        model.train()
        train_loss = 0.0
        for step, batch in enumerate(train_loader, 1):
            input_ids = batch["input_ids"].to(device)
            attn_mask = batch["attention_mask"].to(device)
            lbls      = batch["labels"].to(device)

            optimizer.zero_grad()
            outputs = model(input_ids=input_ids, attention_mask=attn_mask)
            loss    = loss_fn(outputs.logits, lbls)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            scheduler.step()
            train_loss += loss.item()

            if step % 20 == 0:
                elapsed = time.time() - t0
                steps_done = (epoch - 1) * len(train_loader) + step
                eta = (total_steps - steps_done) / (steps_done / elapsed) if steps_done > 0 else 0
                log.info(
                    f"  Epoch {epoch}/{epochs}  step {step}/{len(train_loader)}"
                    f"  loss={train_loss/step:.4f}  ETA {eta:.0f}s"
                )

        # Validation
        model.eval()
        correct = total = 0
        with torch.no_grad():
            for batch in val_loader:
                input_ids = batch["input_ids"].to(device)
                attn_mask = batch["attention_mask"].to(device)
                lbls      = batch["labels"]
                outputs   = model(input_ids=input_ids, attention_mask=attn_mask)
                preds     = outputs.logits.argmax(dim=-1).cpu()
                correct  += (preds == lbls).sum().item()
                total    += len(lbls)

        val_acc = correct / total if total > 0 else 0
        log.info(
            f"Epoch {epoch}/{epochs}  "
            f"train_loss={train_loss/len(train_loader):.4f}  "
            f"val_acc={val_acc:.3f}  "
            f"elapsed={time.time()-t0:.0f}s"
        )
        best_val_acc = max(best_val_acc, val_acc)

    log.info(f"Training complete. Best val accuracy: {best_val_acc:.3f}")

    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(str(save_path))
    tokenizer.save_pretrained(str(save_path))
    log.info(f"Model saved to {save_path}")

    return model, tokenizer, id2label, device


# ── Load saved model ──────────────────────────────────────────────────────────

def load_saved_model(save_dir: str):
    try:
        import torch
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
    except ImportError as e:
        log.error(f"Missing dependency: {e}")
        sys.exit(1)

    save_path = Path(save_dir)
    log.info(f"Loading saved model from {save_path}…")

    if torch.backends.mps.is_available():
        device = torch.device("mps")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
    else:
        device = torch.device("cpu")

    tokenizer = AutoTokenizer.from_pretrained(str(save_path))
    model     = AutoModelForSequenceClassification.from_pretrained(str(save_path))
    model     = model.to(device)
    id2label  = {int(k): v for k, v in model.config.id2label.items()}
    log.info(f"Model loaded: {len(id2label)} labels, device={device}")
    return model, tokenizer, id2label, device


# ── Classify all projects ─────────────────────────────────────────────────────

def load_all_projects(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    rows = conn.execute("""
        SELECT p.id, p.title, p.description,
               GROUP_CONCAT(DISTINCT k.keyword) AS keywords,
               (SELECT GROUP_CONCAT(file_name, '|')
                FROM (SELECT DISTINCT file_name FROM files
                      WHERE project_id = p.id LIMIT 10)) AS file_names
        FROM projects p
        LEFT JOIN keywords k ON k.project_id = p.id
        GROUP BY p.id
    """).fetchall()
    return [(r[0], make_text(r[1], r[2], r[3], r[4])) for r in rows]


def classify_all(
    model,
    tokenizer,
    id2label: dict[int, str],
    device,
    all_projects: list[tuple[int, str]],
    conn: sqlite3.Connection,
    batch_size: int,
    max_length: int,
) -> None:
    import torch
    import torch.nn.functional as F

    pids  = [r[0] for r in all_projects]
    texts = [r[1] for r in all_projects]
    total = len(texts)

    log.info(f"Classifying {total:,} projects in batches of {batch_size}…")
    t0 = time.time()

    rows = []
    model.eval()
    num_batches = math.ceil(total / batch_size)

    with torch.no_grad():
        for b in range(num_batches):
            start = b * batch_size
            end   = min(start + batch_size, total)
            batch_texts = texts[start:end]
            batch_pids  = pids[start:end]

            enc = tokenizer(
                batch_texts,
                truncation=True,
                padding=True,
                max_length=max_length,
                return_tensors="pt",
            )
            input_ids = enc["input_ids"].to(device)
            attn_mask = enc["attention_mask"].to(device)

            outputs = model(input_ids=input_ids, attention_mask=attn_mask)
            probs   = F.softmax(outputs.logits, dim=-1).cpu()

            for i, pid in enumerate(batch_pids):
                top_idx  = int(probs[i].argmax())
                top2_idx = int(probs[i].topk(2).indices[1]) if probs.shape[1] > 1 else top_idx

                div       = id2label[top_idx]
                div_top2  = id2label[top2_idx]
                prob_top1 = float(probs[i][top_idx])
                prob_top2 = float(probs[i][top2_idx])
                conf      = confidence_from_prob(prob_top1)

                sec, sec_name = DIVISION_TO_SECTION.get(div, ("?", "Unknown"))
                # Get division name from embed anchors (if available)
                rows.append((
                    pid, sec, sec_name, div, "",  # division_name filled below
                    conf, prob_top1, prob_top2, div_top2,
                ))

            if (b + 1) % 50 == 0 or (b + 1) == num_batches:
                elapsed = time.time() - t0
                done    = end
                eta     = (total - done) / (done / elapsed) if done > 0 else 0
                log.info(f"  {done:>6,}/{total:,}  elapsed={elapsed:.0f}s  ETA={eta:.0f}s")

    log.info(f"Classified {len(rows):,} projects in {time.time()-t0:.1f}s")

    # Get division names from embed table (has all 86 division names)
    div_names = {}
    try:
        for code, name in conn.execute(
            "SELECT DISTINCT division, division_name FROM classifications_embed "
            "WHERE division IS NOT NULL AND division != ''"
        ):
            div_names[code] = name
    except Exception:
        pass

    # Fill in division names
    final_rows = []
    for pid, sec, sec_name, div, _, conf, p1, p2, div2 in rows:
        div_name = div_names.get(div, "")
        final_rows.append((pid, sec, sec_name, div, div_name, conf, p1, p2, div2))

    conn.execute(f"DELETE FROM {TABLE}")
    conn.executemany(f"""
        INSERT INTO {TABLE}
            (project_id, section, section_name, division, division_name,
             confidence, prob_top1, prob_top2, division_top2)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, final_rows)
    conn.commit()
    log.info(f"Saved {len(final_rows):,} rows to {TABLE}")


# ── Stats ─────────────────────────────────────────────────────────────────────

def show_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if total == 0:
        print(f"{TABLE} is empty — run without --stats first.")
        return

    print(f"\n{'═'*72}")
    print(f"  {TABLE}  —  {total:,} projects")
    print(f"{'═'*72}\n")

    order = "CASE confidence WHEN 'very_high' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
    print("Confidence distribution:")
    for conf, n, pct in conn.execute(f"""
        SELECT confidence, COUNT(*), ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY confidence ORDER BY {order}
    """):
        bar = "█" * int(pct / 2)
        print(f"  {conf:<12} {n:>7,}  {pct:>5}%  {bar}")

    print("\nTop 15 divisions:")
    print(f"  {'Div':<5} {'Section':<4} {'Name':<46} {'N':>7}  {'%':>5}")
    print(f"  {'─'*68}")
    for div, sec, name, n, pct in conn.execute(f"""
        SELECT division, section, division_name, COUNT(*) as n,
               ROUND(COUNT(*)*100.0/{total},1) as pct
        FROM {TABLE} GROUP BY division ORDER BY n DESC LIMIT 15
    """):
        print(f"  {div:<5} {(sec or '?'):<4} {(name or '')[:46]:<46} {n:>7,}  {pct:>5}%")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fine-tune DistilBERT on division labels + PDF descriptions."
    )
    ap.add_argument("--db",               default=DEFAULT_DB)
    ap.add_argument("--model",            default=DEFAULT_MODEL)
    ap.add_argument("--save-dir",         default=MODEL_SAVE)
    ap.add_argument("--pdf",              default=DEFAULT_PDF,
                    help="Path to ISIC5 Explanatory Notes PDF for augmentation")
    ap.add_argument("--epochs",           type=int,   default=5)
    ap.add_argument("--batch-size",       type=int,   default=16)
    ap.add_argument("--infer-batch-size", type=int,   default=32)
    ap.add_argument("--max-length",       type=int,   default=128)
    ap.add_argument("--lr",               type=float, default=2e-5)
    ap.add_argument("--retrain",          action="store_true")
    ap.add_argument("--stats",            action="store_true")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        log.error(f"DB not found: {db_path}"); sys.exit(1)

    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    save_path = Path(args.save_dir)
    model_ready = (save_path / "config.json").exists()

    if model_ready and not args.retrain:
        log.info(f"Saved model found — loading (use --retrain to retrain)")
        model, tokenizer, id2label, device = load_saved_model(args.save_dir)
    else:
        if args.retrain and model_ready:
            log.info("--retrain: ignoring saved model")

        # Build training data
        pdf_descriptions = extract_pdf_descriptions(args.pdf)
        embed_anchors    = get_embed_anchors()
        train_texts, train_labels = load_training_data(
            conn, pdf_descriptions, embed_anchors
        )

        model, tokenizer, id2label, device = train_model(
            texts      = train_texts,
            labels_str = train_labels,
            model_name = args.model,
            save_dir   = args.save_dir,
            epochs     = args.epochs,
            batch_size = args.batch_size,
            max_length = args.max_length,
            lr         = args.lr,
        )

    all_projects = load_all_projects(conn)
    classify_all(
        model, tokenizer, id2label, device,
        all_projects, conn,
        batch_size = args.infer_batch_size,
        max_length = args.max_length,
    )
    show_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
