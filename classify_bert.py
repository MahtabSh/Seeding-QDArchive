#!/usr/bin/env python3
"""
Fine-tune DistilBERT on labels_training, then classify all 53k projects.

Training data : labels_training table (built by create_training_labels.py)
Text features : project title + description (truncated to max_length tokens)
Model         : distilbert-base-uncased  (configurable via --model)
Device        : MPS (Apple Silicon) > CUDA > CPU
Output table  : classifications_bert
Saved model   : models/bert_isic_v2/  (reloaded on next run — no retrain needed)

Usage:
    python3 classify_bert.py --db 23692652-sq26.db
    python3 classify_bert.py --db 23692652-sq26.db --model distilbert-base-multilingual-cased
    python3 classify_bert.py --db 23692652-sq26.db --epochs 5 --batch-size 16
    python3 classify_bert.py --db 23692652-sq26.db --retrain   # force retrain even if saved
    python3 classify_bert.py --db 23692652-sq26.db --stats
"""
from __future__ import annotations

import argparse
import logging
import math
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
DEFAULT_MODEL = "distilbert-base-uncased"
MODEL_SAVE    = "models/bert_isic_v2"
TABLE         = "classifications_bert"

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

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id    INTEGER NOT NULL UNIQUE,
        section       TEXT,
        section_name  TEXT,
        confidence    TEXT,
        prob_top1     REAL,
        prob_top2     REAL,
        section_top2  TEXT,
        tier          TEXT DEFAULT 'metadata',
        classified_at TEXT DEFAULT (datetime('now'))
    )""")
    conn.commit()


# ── Text preparation ───────────────────────────────────────────────────────────

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
        # Strip extensions and underscores for readability
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


# ── Data loading ───────────────────────────────────────────────────────────────

def load_training_data(conn: sqlite3.Connection) -> tuple[list[str], list[str]]:
    rows = conn.execute("""
        SELECT lt.section, p.title, p.description,
               GROUP_CONCAT(DISTINCT k.keyword) AS keywords,
               (SELECT GROUP_CONCAT(file_name, '|')
                FROM (SELECT DISTINCT file_name FROM files
                      WHERE project_id = p.id LIMIT 10)) AS file_names
        FROM labels_training lt
        JOIN projects p ON p.id = lt.project_id
        LEFT JOIN keywords k ON k.project_id = p.id
        WHERE lt.section IS NOT NULL
        GROUP BY lt.project_id
    """).fetchall()

    if not rows:
        log.error("labels_training is empty — run create_training_labels.py first")
        sys.exit(1)

    texts  = [make_text(r[1], r[2], r[3], r[4]) for r in rows]
    labels = [r[0] for r in rows]
    log.info(f"Training data: {len(texts):,} examples, {len(set(labels))} sections")
    return texts, labels


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


# ── PyTorch dataset ────────────────────────────────────────────────────────────

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


# ── Training ───────────────────────────────────────────────────────────────────

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

    # Device selection
    if torch.backends.mps.is_available():
        device = torch.device("mps")
        log.info("Device: Apple Silicon MPS")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
        log.info(f"Device: CUDA ({torch.cuda.get_device_name(0)})")
    else:
        device = torch.device("cpu")
        log.info("Device: CPU (training will be slow)")

    # Label encoding
    label_set = sorted(set(labels_str))
    label2id  = {l: i for i, l in enumerate(label_set)}
    id2label  = {i: l for l, i in label2id.items()}
    labels_int = [label2id[l] for l in labels_str]

    log.info(f"Labels ({len(label_set)}): {' '.join(label_set)}")

    # Class weights for imbalanced data
    cw = compute_class_weight("balanced", classes=np.array(label_set), y=np.array(labels_str))
    class_weights = torch.tensor(cw, dtype=torch.float32).to(device)
    log.info(f"Class weights range: [{cw.min():.2f}, {cw.max():.2f}]")

    # Tokenizer + model
    log.info(f"Loading tokenizer and model: {model_name}")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(
        model_name,
        num_labels=len(label_set),
        id2label=id2label,
        label2id=label2id,
    )
    model = model.to(device)

    # Dataset
    log.info(f"Tokenizing {len(texts):,} examples (max_length={max_length})…")
    dataset = make_dataset(texts, labels_int, tokenizer, max_length)

    # 90/10 train/val split
    val_size   = max(1, int(0.1 * len(dataset)))
    train_size = len(dataset) - val_size
    train_ds, val_ds = random_split(
        dataset, [train_size, val_size],
        generator=torch.Generator().manual_seed(42)
    )

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_ds,   batch_size=batch_size * 2, shuffle=False)

    # Optimizer with linear warmup
    optimizer   = AdamW(model.parameters(), lr=lr, weight_decay=0.01)
    total_steps = len(train_loader) * epochs
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
        # Train
        model.train()
        train_loss = 0.0
        for step, batch in enumerate(train_loader, 1):
            input_ids  = batch["input_ids"].to(device)
            attn_mask  = batch["attention_mask"].to(device)
            lbls       = batch["labels"].to(device)

            optimizer.zero_grad()
            outputs  = model(input_ids=input_ids, attention_mask=attn_mask)
            loss     = loss_fn(outputs.logits, lbls)
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

        # Validate
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

    # Save model
    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(str(save_path))
    tokenizer.save_pretrained(str(save_path))
    log.info(f"Model saved to {save_path}")

    return model, tokenizer, id2label, device


# ── Classify all projects ──────────────────────────────────────────────────────

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
            input_ids  = enc["input_ids"].to(device)
            attn_mask  = enc["attention_mask"].to(device)

            outputs = model(input_ids=input_ids, attention_mask=attn_mask)
            probs   = F.softmax(outputs.logits, dim=-1).cpu()

            for i, pid in enumerate(batch_pids):
                top_idx  = int(probs[i].argmax())
                top2_idx = int(probs[i].topk(2).indices[1])

                sec       = id2label[top_idx]
                sec_top2  = id2label[top2_idx]
                prob_top1 = float(probs[i][top_idx])
                prob_top2 = float(probs[i][top2_idx])
                conf      = confidence_from_prob(prob_top1)

                rows.append((
                    pid, sec, ISIC_SECTIONS.get(sec, ""),
                    conf, prob_top1, prob_top2, sec_top2,
                ))

            if (b + 1) % 50 == 0 or (b + 1) == num_batches:
                elapsed = time.time() - t0
                done    = end
                eta     = (total - done) / (done / elapsed) if done > 0 else 0
                log.info(
                    f"  {done:>6,}/{total:,}  "
                    f"elapsed={elapsed:.0f}s  ETA={eta:.0f}s"
                )

    log.info(f"Classified {len(rows):,} projects in {time.time()-t0:.1f}s")

    conn.execute(f"DELETE FROM {TABLE}")
    conn.executemany(f"""
        INSERT INTO {TABLE}
            (project_id, section, section_name, confidence,
             prob_top1, prob_top2, section_top2)
        VALUES (?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    log.info(f"Saved {len(rows):,} rows to {TABLE}")


# ── Load saved model ───────────────────────────────────────────────────────────

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
    model = AutoModelForSequenceClassification.from_pretrained(str(save_path))
    model = model.to(device)

    id2label = {int(k): v for k, v in model.config.id2label.items()}
    log.info(f"Model loaded: {len(id2label)} labels, device={device}")
    return model, tokenizer, id2label, device


# ── Stats ──────────────────────────────────────────────────────────────────────

def show_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if total == 0:
        print(f"{TABLE} is empty — run without --stats first.")
        return

    print(f"\n{'═'*72}")
    print(f"  {TABLE}  —  {total:,} projects")
    print(f"{'═'*72}\n")

    print("Confidence distribution:")
    order = "CASE confidence WHEN 'very_high' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
    for conf, n, pct in conn.execute(f"""
        SELECT confidence, COUNT(*), ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY confidence ORDER BY {order}
    """):
        bar = "█" * int(pct / 2)
        print(f"  {conf:<12} {n:>7,}  {pct:>5}%  {bar}")

    print("\nSection distribution:")
    print(f"  {'Sec':<4} {'Name':<52} {'N':>7}  {'%':>5}")
    print(f"  {'─'*70}")
    for sec, name, n, pct in conn.execute(f"""
        SELECT section, section_name, COUNT(*), ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY section ORDER BY COUNT(*) DESC
    """):
        bar = "█" * max(1, int(pct / 2))
        print(f"  {sec:<4} {(name or '')[:52]:<52} {n:>7,}  {pct:>5}%  {bar}")
    print()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    global TABLE
    ap = argparse.ArgumentParser(
        description="Fine-tune a BERT model on labels_training, classify all projects."
    )
    ap.add_argument("--db",         default=DEFAULT_DB)
    ap.add_argument("--model",      default=DEFAULT_MODEL,
                    help=f"HuggingFace model name (default: {DEFAULT_MODEL})")
    ap.add_argument("--save-dir",   default=MODEL_SAVE,
                    help=f"Directory to save/load fine-tuned model (default: {MODEL_SAVE})")
    ap.add_argument("--table",      default=TABLE,
                    help=f"Output DB table name (default: {TABLE})")
    ap.add_argument("--epochs",           type=int,   default=3)
    ap.add_argument("--batch-size",       type=int,   default=16)
    ap.add_argument("--infer-batch-size", type=int,   default=32,
                    help="Batch size for inference (default 32; reduce if MPS runs out of memory)")
    ap.add_argument("--max-length",       type=int,   default=128)
    ap.add_argument("--lr",               type=float, default=2e-5)
    ap.add_argument("--retrain",    action="store_true",
                    help="Force retrain even if a saved model exists")
    ap.add_argument("--stats",      action="store_true",
                    help="Show stats from existing table (no retrain/reclassify)")
    args = ap.parse_args()
    TABLE = args.table

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
        log.info(f"Saved model found at {save_path} — loading (use --retrain to retrain)")
        model, tokenizer, id2label, device = load_saved_model(args.save_dir)
    else:
        if args.retrain and model_ready:
            log.info("--retrain: ignoring saved model, retraining from scratch")
        train_texts, train_labels = load_training_data(conn)
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
