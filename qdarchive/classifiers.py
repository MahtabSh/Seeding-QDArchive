"""
Alternative classifiers for ISIC Rev. 5 — no LLM required.

Three classifiers, all with the same interface:
    result = clf.classify(title, description, keywords, file_types, snippets)
    # → {section, section_name, division, division_name, confidence, method}

KeywordClassifier    Rule-based weighted keyword matching.  Fastest, fully
                     deterministic, no models needed.

BM25Classifier       Lexical BM25 retrieval combined with sentence-transformer
                     embeddings (0.4 BM25 + 0.6 embedding). Fixes abbreviation
                     failures that confuse pure embedding.

NLIClassifier        Zero-shot Natural Language Inference using a cross-encoder
                     (nli-deberta-v3-small).  Asks "does this text entail this
                     division?" and picks highest entailment score.  Closest to
                     LLM quality without needing Ollama.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

from qdarchive.isic import DIVISIONS, DIVISION_MAP, SECTION_MAP

log = logging.getLogger(__name__)

# ── Shared division descriptions (same as ensemble_classifier) ────────────────
from qdarchive.ensemble_classifier import _DIVISION_DESCRIPTIONS, _build_query


# ══════════════════════════════════════════════════════════════════════════════
# 1.  KEYWORD CLASSIFIER
# ══════════════════════════════════════════════════════════════════════════════

# (division_code, [(regex_pattern, weight), ...])
# Patterns are matched case-insensitively against title + keywords + description.
_KEYWORD_RULES: dict[str, list[tuple[str, int]]] = {
    # ── Health (R) ────────────────────────────────────────────────────────────
    "86": [
        (r"\b(HIV|AIDS|antiretroviral|ART\s+regimen|NNRTI|PMTCT|nevirapine|lopinavir|CD4)\b", 12),
        (r"\b(cancer|tumor|tumour|oncology|carcinoma|melanoma|chemotherapy)\b", 10),
        (r"\b(clinical\s+trial|randomized\s+trial|randomised|placebo|intervention\s+arm)\b", 9),
        (r"\b(patient|hospital|physician|nurse|doctor|surgeon|clinician)\b", 7),
        (r"\b(disease|diagnosis|symptom|mortality|morbidity|prognosis|treatment)\b", 6),
        (r"\b(public\s+health|epidemiol|prevalence|incidence|outbreak|pandemic|vaccine)\b", 7),
        (r"\b(mental\s+health|depression|anxiety|psychiatric|psycholog|therapy)\b", 7),
        (r"\b(reproductive\s+health|abortion|contraception|prenatal|maternal|neonatal)\b", 9),
        (r"\b(drug|medication|pharmaceutical|dosage|prescription|pharmacol)\b", 5),
        (r"\b(health\b|healthcare|medical\b|medicine\b|nursing\b)\b", 4),
    ],
    "87": [
        (r"\b(nursing\s+home|residential\s+care|elder\s+care|aged\s+care|care\s+home)\b", 10),
        (r"\b(long.term\s+care|disability\s+support|institutional\s+care)\b", 9),
    ],
    "88": [
        (r"\b(social\s+work|social\s+worker|child\s+protection|welfare)\b", 10),
        (r"\b(homeless|homelessness|poverty|food\s+bank|shelter)\b", 8),
        (r"\b(family\s+support|domestic\s+violence|abuse|foster\s+care)\b", 8),
    ],
    # ── Education (Q) ─────────────────────────────────────────────────────────
    "85": [
        (r"\b(school|classroom|teacher|student|pupil|pedagog)\b", 9),
        (r"\b(education|curriculum|learning|teaching|instruction|tutor)\b", 8),
        (r"\b(university|college|higher\s+education|academic\s+program)\b", 6),
        (r"\b(literacy|numeracy|reading\s+skill|writing\s+skill)\b", 8),
        (r"\b(STEM|e-learning|distance\s+learning|dropout|enrollment)\b", 7),
    ],
    # ── Public administration / Politics (P) ─────────────────────────────────
    "84": [
        (r"\b(election|voting|voter|ballot|electoral\s+trust|electoral\s+fraud)\b", 12),
        (r"\b(democracy|democratic|political\s+party|politician|partisan)\b", 9),
        (r"\b(government|governance|legislature|parliament|congress|senate)\b", 8),
        (r"\b(public\s+policy|legislation|regulation|bureaucrac|civil\s+service)\b", 7),
        (r"\b(military|defense|defence|national\s+security|armed\s+forces)\b", 5),
    ],
    # ── Oil & gas (B) ─────────────────────────────────────────────────────────
    "06": [
        (r"\b(oil\s+and\s+gas|oil\s+&\s+gas|petroleum|crude\s+oil|natural\s+gas)\b", 12),
        (r"\b(methane\s+emission|greenhouse\s+gas.*oil|fugitive\s+emission)\b", 11),
        (r"\b(drilling|fracking|hydraulic\s+fracturing|offshore\s+platform)\b", 9),
        (r"\b(refinery|pipeline|hydrocarbon|fossil\s+fuel)\b", 7),
    ],
    "05": [
        (r"\b(coal\s+min|coal\s+industry|coal\s+worker|coal\s+community)\b", 10),
    ],
    # ── Energy (D) ────────────────────────────────────────────────────────────
    "35": [
        (r"\b(renewable\s+energy|solar\s+panel|wind\s+turbine|photovoltaic)\b", 10),
        (r"\b(electricity\s+grid|power\s+plant|energy\s+policy|energy\s+transition)\b", 8),
        (r"\b(carbon\s+neutral|net\s+zero|decarboniz)\b", 6),
    ],
    # ── Agriculture (A) ───────────────────────────────────────────────────────
    "01": [
        (r"\b(farm|farmer|farming|agriculture|crop|harvest|soil\s+health)\b", 9),
        (r"\b(livestock|cattle|poultry|dairy|animal\s+husbandry)\b", 9),
        (r"\b(rural\s+livelihood|smallholder|agronomy|horticulture|irrigation)\b", 8),
        (r"\b(food\s+security|food\s+production|agroecol)\b", 7),
    ],
    "03": [
        (r"\b(fish|fishing|fishery|aquaculture|seafood|marine\s+harvest)\b", 10),
    ],
    # ── IT / Software (K) ─────────────────────────────────────────────────────
    "62": [
        (r"\b(software\s+develop|programmer|coding|agile|DevOps|open.source)\b", 10),
        (r"\b(tech\s+worker|IT\s+professional|computer\s+science|algorithm)\b", 8),
    ],
    "61": [
        (r"\b(telecom|mobile\s+network|broadband|internet\s+access|connectivity)\b", 9),
    ],
    # ── Arts / Culture (S) ────────────────────────────────────────────────────
    "91": [
        (r"\b(museum|archive|library|oral\s+history|cultural\s+heritage)\b", 10),
        (r"\b(digital\s+humanities|manuscript|artifact|collection)\b", 8),
    ],
    "90": [
        (r"\b(theater|theatre|music|dance|performance\s+art|artist|creative)\b", 8),
    ],
    "93": [
        (r"\b(sport|athlete|physical\s+activity|recreation|leisure|fitness)\b", 9),
    ],
    # ── Social science / R&D (N) ──────────────────────────────────────────────
    "72": [
        (r"\b(qualitative\s+research|quantitative\s+research|mixed\s+method)\b", 5),
        (r"\b(research\s+methodology|grounded\s+theory|discourse\s+analysis)\b", 5),
        (r"\b(scientific\s+R&D|basic\s+research|applied\s+research)\b", 6),
    ],
    # ── Environment / Water ───────────────────────────────────────────────────
    "36": [
        (r"\b(water\s+supply|drinking\s+water|water\s+treatment|water\s+utility)\b", 10),
    ],
    "38": [
        (r"\b(waste\s+management|recycling|landfill|garbage|solid\s+waste)\b", 9),
    ],
    "39": [
        (r"\b(remediation|contaminated\s+(land|soil|site)|hazardous\s+waste)\b", 10),
    ],
    # ── Housing / Real estate (M) ─────────────────────────────────────────────
    "68": [
        (r"\b(housing\s+market|gentrification|tenant|landlord|rent|eviction)\b", 10),
        (r"\b(affordable\s+housing|urban\s+housing|property\s+market)\b", 8),
    ],
    # ── Labour / employment (O) ───────────────────────────────────────────────
    "78": [
        (r"\b(labour\s+market|labor\s+market|job\s+seeker|unemployment|gig\s+work)\b", 9),
        (r"\b(precarious\s+work|informal\s+employ|trade\s+union|worker\s+right)\b", 8),
    ],
    # ── Immigration / international ───────────────────────────────────────────
    "99": [
        (r"\b(UN\s+agency|World\s+Bank|IMF|NATO|diplomatic|intergovernmental)\b", 9),
    ],
    "94": [
        (r"\b(NGO|non.governmental|civil\s+society|volunteer|charity|nonprofit)\b", 8),
    ],
}


class KeywordClassifier:
    """
    Weighted keyword/regex rule classifier.
    Scores each project text against rules for all 87 ISIC divisions
    and returns the highest-scoring division.
    """

    model_name = "keyword"

    def __init__(self):
        # Pre-compile patterns
        self._rules: list[tuple[str, re.Pattern, int]] = []
        for div, patterns in _KEYWORD_RULES.items():
            for pat, weight in patterns:
                self._rules.append((div, re.compile(pat, re.I), weight))
        log.info("KeywordClassifier ready.")

    def classify(
        self,
        title: str,
        description: str = "",
        keywords: list[str] | None = None,
        file_types: list[str] | None = None,
        snippets: dict[str, str] | None = None,
    ) -> dict:
        text = " ".join([
            (title or "") * 3,
            " ".join((keywords or [])[:20]) * 2,
            (description or "")[:800],
            " ".join((snippets or {}).values()),
        ])

        scores: dict[str, float] = {}
        for div, pattern, weight in self._rules:
            matches = len(pattern.findall(text))
            if matches:
                scores[div] = scores.get(div, 0) + matches * weight

        if not scores:
            div, conf = "72", "low"   # default: Scientific R&D
        else:
            div = max(scores, key=lambda d: scores[d])
            top = scores[div]
            conf = "high" if top >= 15 else ("medium" if top >= 7 else "low")

        sec = DIVISION_MAP.get(div, ("N", ""))[0]
        return {
            "section":       sec,
            "section_name":  SECTION_MAP.get(sec, ""),
            "division":      div,
            "division_name": DIVISION_MAP.get(div, ("", ""))[1],
            "confidence":    conf,
            "method":        "keyword_rules",
        }


# ══════════════════════════════════════════════════════════════════════════════
# 2.  BM25 + EMBEDDING ENSEMBLE
# ══════════════════════════════════════════════════════════════════════════════

class BM25Classifier:
    """
    Combines BM25 lexical retrieval (0.4) with sentence-transformer
    cosine similarity (0.6) to rank ISIC divisions.

    BM25 catches exact terms (abbreviations, proper nouns) that embeddings miss.
    Embeddings catch meaning when exact words differ.
    """

    model_name = "bm25"

    def __init__(
        self,
        embed_model: str = "paraphrase-multilingual-MiniLM-L12-v2",
        bm25_weight: float = 0.4,
        embed_weight: float = 0.6,
    ):
        self.bm25_w  = bm25_weight
        self.embed_w = embed_weight

        # ── Division texts for both indexes ───────────────────────────────
        self._divisions = list(DIVISIONS)
        self._div_texts = [
            f"Division {div}: {name}. {_DIVISION_DESCRIPTIONS.get(div, '')}"
            for _, div, name in self._divisions
        ]

        # ── BM25 index ────────────────────────────────────────────────────
        from rank_bm25 import BM25Okapi
        tokenized = [self._tokenize(t) for t in self._div_texts]
        self._bm25 = BM25Okapi(tokenized)

        # ── Embedding index ───────────────────────────────────────────────
        log.info(f"Loading sentence-transformer: {embed_model}")
        from sentence_transformers import SentenceTransformer
        self._embedder = SentenceTransformer(embed_model)
        self._embed_index = self._embedder.encode(
            self._div_texts, normalize_embeddings=True, show_progress_bar=False
        )
        log.info("BM25Classifier ready.")

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        return re.findall(r"\b\w+\b", text.lower())

    def classify(
        self,
        title: str,
        description: str = "",
        keywords: list[str] | None = None,
        file_types: list[str] | None = None,
        snippets: dict[str, str] | None = None,
    ) -> dict:
        query_text = _build_query(title, description, keywords, file_types, snippets)

        # BM25 scores (normalised to 0-1)
        bm25_raw = self._bm25.get_scores(self._tokenize(query_text))
        bm25_max = max(bm25_raw) if max(bm25_raw) > 0 else 1.0
        bm25_scores = [s / bm25_max for s in bm25_raw]

        # Embedding scores (already 0-1 after normalisation)
        vec = self._embedder.encode(query_text, normalize_embeddings=True)
        embed_scores = (self._embed_index @ vec).tolist()

        # Combined score
        combined = [
            self.bm25_w * b + self.embed_w * e
            for b, e in zip(bm25_scores, embed_scores)
        ]

        best_idx = max(range(len(combined)), key=lambda i: combined[i])
        best_score = combined[best_idx]
        sec, div, name = self._divisions[best_idx]

        conf = "high" if best_score > 0.55 else ("medium" if best_score > 0.38 else "low")
        return {
            "section":       sec,
            "section_name":  SECTION_MAP.get(sec, ""),
            "division":      div,
            "division_name": name,
            "confidence":    conf,
            "method":        "bm25_ensemble",
        }


# ══════════════════════════════════════════════════════════════════════════════
# 3.  ZERO-SHOT NLI CLASSIFIER
# ══════════════════════════════════════════════════════════════════════════════

class NLIClassifier:
    """
    Zero-shot classification using a cross-encoder NLI model.

    For each top-K candidate (from embedding pre-filter), scores the
    hypothesis "This research dataset studies [division description]"
    against the project text.  Picks the highest entailment score.

    Model: cross-encoder/nli-deberta-v3-small  (~180 MB)
    """

    model_name = "nli"

    def __init__(
        self,
        embed_model: str = "paraphrase-multilingual-MiniLM-L12-v2",
        nli_model: str  = "cross-encoder/nli-deberta-v3-small",
        top_k: int      = 10,
    ):
        self.top_k = top_k

        # ── Embedding pre-filter (narrows 87 → top_k) ─────────────────────
        self._divisions = list(DIVISIONS)
        div_texts = [
            f"Division {div}: {name}. {_DIVISION_DESCRIPTIONS.get(div, '')}"
            for _, div, name in self._divisions
        ]
        log.info(f"Loading embedding model: {embed_model}")
        from sentence_transformers import SentenceTransformer
        self._embedder = SentenceTransformer(embed_model)
        self._embed_index = self._embedder.encode(
            div_texts, normalize_embeddings=True, show_progress_bar=False
        )

        # ── NLI cross-encoder ─────────────────────────────────────────────
        log.info(f"Loading NLI model: {nli_model}")
        from transformers import pipeline
        self._nli = pipeline(
            "text-classification",
            model=nli_model,
            device=-1,           # CPU; set to 0 for GPU
            truncation=True,
            max_length=512,
        )
        log.info("NLIClassifier ready.")

    def classify(
        self,
        title: str,
        description: str = "",
        keywords: list[str] | None = None,
        file_types: list[str] | None = None,
        snippets: dict[str, str] | None = None,
    ) -> dict:
        query_text = _build_query(title, description, keywords, file_types, snippets)

        # Step 1: embedding pre-filter → top-K candidates
        vec = self._embedder.encode(query_text, normalize_embeddings=True)
        scores = (self._embed_index @ vec).tolist()
        top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[: self.top_k]

        # Step 2: NLI entailment scoring for each candidate
        # Truncate premise to keep total input within 512 tokens
        premise = f"{title}. {' '.join((keywords or [])[:10])}. {(description or '')[:300]}"
        if snippets:
            premise += " " + list(snippets.values())[0][:150]

        pairs = []
        for idx in top_indices:
            _, div, name = self._divisions[idx]
            hypothesis = f"This research dataset studies {name.lower()}. {_DIVISION_DESCRIPTIONS.get(div, '')[:100]}"
            pairs.append((premise, hypothesis))

        results = self._nli([{"text": p, "text_pair": h} for p, h in pairs])

        # Pick candidate with highest ENTAILMENT score
        best_score = -1.0
        best_idx_local = 0
        for i, r in enumerate(results):
            label = r["label"].upper()
            score = r["score"] if label == "ENTAILMENT" else (1 - r["score"])
            if label == "ENTAILMENT" and r["score"] > best_score:
                best_score = r["score"]
                best_idx_local = i

        best_global_idx = top_indices[best_idx_local]
        sec, div, name = self._divisions[best_global_idx]

        conf = "high" if best_score > 0.80 else ("medium" if best_score > 0.55 else "low")
        return {
            "section":       sec,
            "section_name":  SECTION_MAP.get(sec, ""),
            "division":      div,
            "division_name": name,
            "confidence":    conf,
            "method":        f"nli_entailment",
        }


# ── Factory ────────────────────────────────────────────────────────────────────

def build_classifier(name: str, embed_model: str = "paraphrase-multilingual-MiniLM-L12-v2"):
    """Return the right classifier instance for a given name."""
    if name == "keyword":
        return KeywordClassifier()
    if name == "bm25":
        return BM25Classifier(embed_model=embed_model)
    if name == "nli":
        return NLIClassifier(embed_model=embed_model)
    raise ValueError(f"Unknown classifier: {name}. Choose from: keyword, bm25, nli")
