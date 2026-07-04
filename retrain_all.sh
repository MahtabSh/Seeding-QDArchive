#!/usr/bin/env bash
# Retrain all classifiers that depend on labels_training, then rebuild the vote.
# Run this whenever new LLM labels are added to the database.
#
# Usage:
#   bash retrain_all.sh
#   bash retrain_all.sh --db path/to/other.db

DB="${2:-23692652-sq26.db}"

set -e  # stop on any error

echo "=== Retraining bert-base-uncased (section classifier) ==="
python3 classify_bert.py \
    --db "$DB" \
    --model bert-base-uncased \
    --save-dir models/bert_base_isic_v1 \
    --table classifications_bert_base \
    --retrain

echo ""
echo "=== Retraining TF-IDF + Logistic Regression ==="
python3 classify_tfidf.py --db "$DB"

echo ""
echo "=== Retraining BERT division classifier (section + division) ==="
python3 classify_bert_division.py --db "$DB" --retrain

echo ""
echo "=== Rebuilding voting ensemble ==="
python3 classify_vote.py --db "$DB"

echo ""
echo "=== Regenerating deliverables ==="
python3 generate_xlsx.py --db "$DB"
python3 generate_pdf_report.py --db "$DB"

echo ""
echo "Done. All classifiers retrained and deliverables updated."
