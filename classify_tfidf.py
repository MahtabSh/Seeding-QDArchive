#!/usr/bin/env python3
"""
Train TF-IDF + Logistic Regression on labels_training, then classify all projects.

Training data: labels_training table (built by create_training_labels.py)
Features: TF-IDF over project title + description (sublinear_tf, bigrams)
Model: LogisticRegression with class_weight='balanced' (handles section imbalance)
Output: classifications_tfidf table

Usage:
    python3 classify_tfidf.py --db 23692652-sq26.db
    python3 classify_tfidf.py --db 23692652-sq26.db --stats
    python3 classify_tfidf.py --db 23692652-sq26.db --cross-validate
"""
from __future__ import annotations

import argparse
import logging
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

DEFAULT_DB = "23692652-sq26.db"
TABLE      = "classifications_tfidf"

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

_CREATE_SQL = f"""CREATE TABLE IF NOT EXISTS {TABLE} (
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
)"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_SQL)
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
        names = " ".join(
            f.rsplit(".", 1)[0].replace("_", " ").replace("-", " ")
            for f in file_names.split("|")[:10]
        )
        parts.append(names[:200])
    return " ".join(parts)


def confidence_from_prob(p: float) -> str:
    if p >= 0.80:
        return "very_high"
    if p >= 0.60:
        return "high"
    if p >= 0.40:
        return "medium"
    return "low"


# ── Load data ──────────────────────────────────────────────────────────────────

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

    log.info(f"Training data: {len(texts):,} labeled examples, "
             f"{len(set(labels))} distinct sections")
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


# ── Train ──────────────────────────────────────────────────────────────────────

def train(texts: list[str], labels: list[str]):
    try:
        from sklearn.pipeline import Pipeline
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
    except ImportError:
        log.error("scikit-learn not installed — pip install scikit-learn")
        sys.exit(1)

    log.info("Training TF-IDF + Logistic Regression…")
    t0 = time.time()

    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(
            max_features=20_000,
            ngram_range=(1, 2),
            sublinear_tf=True,
            stop_words="english",
            min_df=2,
        )),
        ("clf", LogisticRegression(
            C=1.0,
            class_weight="balanced",
            max_iter=2000,
            solver="lbfgs",
        )),
    ])
    pipeline.fit(texts, labels)
    log.info(f"Training done in {time.time()-t0:.1f}s")
    return pipeline


# ── Cross-validate ─────────────────────────────────────────────────────────────

def cross_validate(texts: list[str], labels: list[str]) -> None:
    try:
        from sklearn.pipeline import Pipeline
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        import numpy as np
    except ImportError:
        log.error("scikit-learn not installed — pip install scikit-learn")
        return

    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(
            max_features=20_000, ngram_range=(1, 2),
            sublinear_tf=True, stop_words="english", min_df=2,
        )),
        ("clf", LogisticRegression(
            C=1.0, class_weight="balanced", max_iter=2000,
            solver="lbfgs", n_jobs=-1,
        )),
    ])

    log.info("Running 5-fold stratified cross-validation…")
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(pipeline, texts, labels, cv=cv,
                             scoring="f1_macro", n_jobs=-1)
    log.info(f"F1-macro: {scores.mean():.3f} ± {scores.std():.3f}  "
             f"(folds: {' '.join(f'{s:.3f}' for s in scores)})")


# ── Classify all projects ──────────────────────────────────────────────────────

def classify_all(
    pipeline,
    all_projects: list[tuple[int, str]],
    conn: sqlite3.Connection,
) -> None:
    try:
        import numpy as np
    except ImportError:
        log.error("numpy not installed — pip install numpy")
        sys.exit(1)

    pids   = [r[0] for r in all_projects]
    texts  = [r[1] for r in all_projects]
    classes = pipeline.classes_

    log.info(f"Classifying {len(texts):,} projects…")
    t0 = time.time()

    probs = pipeline.predict_proba(texts)  # shape (N, n_classes)

    rows = []
    for i, pid in enumerate(pids):
        top_idx  = int(probs[i].argmax())
        top2_idx = int(probs[i].argsort()[-2])

        sec        = classes[top_idx]
        prob_top1  = float(probs[i][top_idx])
        prob_top2  = float(probs[i][top2_idx])
        sec_top2   = classes[top2_idx]
        confidence = confidence_from_prob(prob_top1)

        rows.append((
            pid, sec, ISIC_SECTIONS.get(sec, ""),
            confidence, prob_top1, prob_top2, sec_top2,
        ))

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


# ── Stats ──────────────────────────────────────────────────────────────────────

def show_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if total == 0:
        print("classifications_tfidf is empty — run without --stats first.")
        return

    print(f"\n{'═'*72}")
    print(f"  classifications_tfidf  —  {total:,} projects")
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
    ap = argparse.ArgumentParser(
        description="Train TF-IDF+LR on labels_training and classify all projects."
    )
    ap.add_argument("--db",             default=DEFAULT_DB)
    ap.add_argument("--stats",          action="store_true",
                    help="Show stats from existing table (no retrain)")
    ap.add_argument("--cross-validate", action="store_true",
                    help="Run 5-fold CV and report F1-macro, then exit")
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

    train_texts, train_labels = load_training_data(conn)

    if args.cross_validate:
        cross_validate(train_texts, train_labels)
        conn.close()
        return

    pipeline     = train(train_texts, train_labels)
    all_projects = load_all_projects(conn)
    classify_all(pipeline, all_projects, conn)
    show_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
