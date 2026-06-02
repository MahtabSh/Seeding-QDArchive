"""
Hybrid ISIC Rev. 5 classifier — no external API required.

Three-stage pipeline (Option C):
  Stage 1  Keyword scoring on title + description + keywords
           (title weighted 3×, keywords 2×, description 1×)
  Stage 2  File-type extension boosts for domain-specific formats
  Stage 3  Google Drive file-content re-scoring (0.5× weight, if available)

Division with the highest combined score wins.
Confidence is derived from the score margin between 1st and 2nd best division.
"""

from __future__ import annotations

import re
from pathlib import Path

from qdarchive.isic import (
    SECTION_MAP, DEFAULT_SECTION, DEFAULT_DIVISION,
    section_for_division, division_name as _div_name,
)

# ── Keyword vocabulary ─────────────────────────────────────────────────────────
# Format: division_code → [(keyword_or_phrase, weight), ...]
# Weight 2.0 = highly discriminative, 1.0 = moderate signal

_DIVISION_KEYWORDS: dict[str, list[tuple[str, float]]] = {

    # A — Agriculture
    "01": [
        ("farming", 2.0), ("agriculture", 2.0), ("agricultural", 2.0),
        ("crop", 2.0), ("livestock", 2.0), ("harvest", 2.0),
        ("irrigation", 2.0), ("pesticide", 2.0), ("fertilizer", 2.0),
        ("farmer", 2.0), ("smallholder", 2.0), ("food security", 1.5),
        ("agroforestry", 2.0), ("soil", 1.5), ("land use", 1.5),
        ("cattle", 2.0), ("poultry", 2.0), ("dairy", 1.5), ("goat", 1.5),
        ("seed", 1.5), ("yield", 1.5), ("horticulture", 2.0),
        ("plantation", 1.5), ("organic farming", 2.0), ("rural household", 2.0),
        ("maize", 2.0), ("rice", 1.5), ("wheat", 1.5), ("agri", 1.0),
    ],
    "02": [
        ("forestry", 2.0), ("logging", 2.0), ("timber", 2.0),
        ("deforestation", 2.0), ("woodland", 2.0), ("forest management", 2.0),
        ("REDD", 2.0),
    ],
    "03": [
        ("fishing", 2.0), ("fishery", 2.0), ("aquaculture", 2.0),
        ("fishermen", 2.0), ("seafood", 2.0), ("artisanal fishing", 2.0),
    ],

    # B — Mining
    "05": [("coal", 2.0), ("lignite", 2.0), ("coal mining", 2.0)],
    "06": [("petroleum", 2.0), ("crude oil", 2.0), ("natural gas", 2.0), ("drilling", 2.0)],
    "07": [("mining", 2.0), ("metal ore", 2.0), ("gold mining", 2.0), ("artisanal mining", 2.0)],
    "08": [("quarrying", 2.0), ("mineral extraction", 2.0)],
    "09": [("mining support", 2.0), ("oilfield service", 2.0)],

    # C — Manufacturing
    "10": [("food processing", 2.0), ("food production", 2.0), ("food industry", 2.0)],
    "11": [("beverage", 2.0), ("beer", 2.0), ("wine", 2.0), ("brewery", 2.0), ("distillery", 2.0)],
    "12": [("tobacco", 2.0), ("cigarette", 2.0)],
    "13": [("textile", 2.0), ("fabric", 2.0), ("weaving", 2.0), ("garment factory", 1.5)],
    "14": [("apparel", 2.0), ("clothing industry", 2.0), ("fashion industry", 2.0), ("garment worker", 2.0)],
    "15": [("leather", 2.0), ("footwear", 2.0), ("tannery", 2.0)],
    "16": [("sawmill", 2.0), ("timber processing", 2.0), ("wood product", 2.0)],
    "17": [("paper mill", 2.0), ("pulp", 2.0), ("paper industry", 2.0)],
    "18": [("printing industry", 2.0)],
    "19": [("refinery", 2.0), ("petroleum product", 2.0), ("fuel production", 2.0)],
    "20": [("chemical industry", 2.0), ("chemical plant", 2.0), ("petrochemical", 2.0)],
    "21": [("pharmaceutical", 2.0), ("drug manufacturing", 2.0), ("pharma", 2.0)],
    "22": [("rubber", 2.0), ("plastic industry", 2.0), ("polymer", 2.0)],
    "23": [("cement", 2.0), ("ceramic", 2.0), ("brick", 2.0)],
    "24": [("steel", 2.0), ("iron smelting", 2.0), ("aluminum", 2.0), ("metallurgy", 2.0)],
    "25": [("fabricated metal", 2.0), ("forging", 2.0), ("stamping", 2.0)],
    "26": [("semiconductor", 2.0), ("electronics manufacturing", 2.0), ("computer hardware", 2.0)],
    "27": [("electrical equipment", 2.0), ("battery manufacturing", 2.0)],
    "28": [("machinery", 2.0), ("industrial machine", 2.0)],
    "29": [("automobile", 2.0), ("automotive", 2.0), ("motor vehicle", 2.0)],
    "30": [("aircraft", 2.0), ("shipbuilding", 2.0), ("aerospace", 2.0)],
    "31": [("furniture manufacturing", 2.0)],
    "32": [("toy", 2.0), ("musical instrument", 2.0), ("jewelry", 2.0)],
    "33": [("machinery repair", 2.0), ("industrial repair", 2.0)],

    # D — Electricity / Energy
    "35": [
        ("electricity", 2.0), ("power generation", 2.0), ("energy grid", 2.0),
        ("renewable energy", 2.0), ("solar energy", 2.0), ("wind energy", 2.0),
        ("nuclear power", 2.0), ("hydropower", 2.0), ("electrification", 2.0),
        ("energy transition", 2.0), ("power plant", 2.0),
    ],

    # E — Water / Waste
    "36": [
        ("drinking water", 2.0), ("water supply", 2.0), ("WASH", 2.0),
        ("water access", 2.0), ("water treatment", 2.0), ("water quality", 2.0),
    ],
    "37": [("sewage", 2.0), ("wastewater", 2.0), ("sanitation", 1.5), ("sewerage", 2.0)],
    "38": [("waste management", 2.0), ("solid waste", 2.0), ("recycling", 2.0), ("landfill", 2.0)],
    "39": [("remediation", 2.0), ("contamination cleanup", 2.0), ("environmental remediation", 2.0)],

    # F — Construction
    "41": [("housing construction", 2.0), ("residential building", 2.0), ("informal settlement", 2.0), ("slum", 1.5)],
    "42": [("civil engineering", 2.0), ("road construction", 2.0), ("dam", 2.0), ("infrastructure project", 2.0)],
    "43": [("renovation", 1.5), ("specialized construction", 2.0), ("building contractor", 2.0)],

    # G — Trade
    "46": [("wholesale", 2.0), ("distribution channel", 2.0), ("supply chain", 1.5), ("trading company", 2.0)],
    "47": [("retail", 2.0), ("consumer behavior", 1.5), ("shopping", 1.5), ("e-commerce", 2.0), ("market vendor", 2.0)],

    # H — Transportation
    "49": [("road transport", 2.0), ("trucking", 2.0), ("railway", 2.0), ("public transit", 2.0), ("commuting", 2.0)],
    "50": [("maritime", 2.0), ("shipping", 2.0), ("port", 1.5), ("sea transport", 2.0)],
    "51": [("aviation", 2.0), ("airline", 2.0), ("airport", 2.0), ("air travel", 2.0)],
    "52": [("logistics", 2.0), ("warehousing", 2.0), ("freight", 1.5)],
    "53": [("postal", 2.0), ("courier", 2.0), ("delivery service", 2.0)],

    # I — Accommodation / Food service
    "55": [("hotel", 2.0), ("accommodation", 2.0), ("hospitality", 2.0), ("guest house", 2.0), ("lodging", 2.0)],
    "56": [("restaurant", 2.0), ("food service", 2.0), ("catering", 2.0), ("street food", 2.0)],

    # J — Publishing / Broadcasting
    "58": [("publishing", 2.0), ("newspaper", 2.0), ("print media", 2.0), ("book publishing", 2.0)],
    "59": [("film", 2.0), ("movie", 2.0), ("television", 2.0), ("video production", 2.0), ("music industry", 2.0)],
    "60": [("broadcasting", 2.0), ("radio", 2.0), ("news agency", 2.0), ("media outlet", 2.0)],

    # K — IT / Telecom
    "61": [("telecommunications", 2.0), ("telecom", 2.0), ("mobile network", 2.0), ("broadband", 2.0), ("5G", 2.0)],
    "62": [
        ("software", 2.0), ("programming", 1.5), ("IT service", 2.0),
        ("app development", 2.0), ("digital platform", 2.0),
        ("computer science", 2.0), ("information system", 2.0),
    ],
    "63": [
        ("data center", 2.0), ("cloud computing", 2.0), ("big data", 2.0),
        ("artificial intelligence", 2.0), ("machine learning", 2.0), ("data analytics", 2.0),
    ],

    # L — Finance
    "64": [
        ("banking", 2.0), ("bank", 1.5), ("credit", 2.0), ("loan", 2.0),
        ("microfinance", 2.0), ("financial inclusion", 2.0), ("mobile money", 2.0),
        ("fintech", 2.0), ("savings group", 2.0),
    ],
    "65": [("insurance", 2.0), ("pension", 2.0), ("health insurance", 2.0), ("life insurance", 2.0)],
    "66": [
        ("investment", 2.0), ("stock market", 2.0), ("financial market", 2.0),
        ("asset management", 2.0), ("remittance", 2.0), ("capital market", 2.0),
    ],

    # M — Real estate
    "68": [
        ("real estate", 2.0), ("property market", 2.0), ("housing market", 2.0),
        ("landlord", 2.0), ("rental market", 2.0), ("gentrification", 2.0),
        ("land tenure", 2.0), ("property rights", 2.0), ("eviction", 2.0),
    ],

    # N — Professional / Scientific
    "69": [("legal", 2.0), ("law", 1.5), ("court", 1.5), ("justice system", 2.0), ("accounting", 2.0), ("audit", 2.0)],
    "70": [("management consultancy", 2.0), ("corporate strategy", 2.0)],
    "71": [("engineering firm", 2.0), ("technical testing", 2.0), ("quality inspection", 2.0)],
    "72": [
        ("research", 2.0), ("scientific", 2.0), ("experiment", 2.0),
        ("laboratory", 2.0), ("qualitative research", 2.0), ("quantitative research", 2.0),
        ("methodology", 2.0), ("data collection", 2.0), ("interview", 1.5),
        ("ethnography", 2.0), ("grounded theory", 2.0), ("phenomenology", 2.0),
        ("mixed method", 2.0), ("longitudinal study", 2.0), ("cross-sectional", 2.0),
        ("survey", 1.5), ("questionnaire", 1.5), ("focus group", 2.0),
        ("participant observation", 2.0), ("case study", 1.5),
        ("content analysis", 2.0), ("discourse analysis", 2.0),
        ("thematic analysis", 2.0), ("NVivo", 2.0), ("MAXQDA", 2.0),
        ("ATLAS.ti", 2.0), ("social science", 1.5), ("field research", 2.0),
    ],
    "73": [("advertising", 2.0), ("marketing", 2.0), ("public relations", 2.0), ("market research", 2.0)],
    "74": [("translation", 2.0), ("linguistic", 2.0), ("language use", 2.0), ("photography", 1.5)],
    "75": [("veterinary", 2.0), ("animal health", 2.0), ("livestock disease", 2.0), ("zoonotic", 2.0)],

    # O — Administrative
    "77": [("leasing", 2.0), ("equipment rental", 2.0)],
    "78": [
        ("labor market", 2.0), ("job search", 2.0), ("recruitment", 2.0),
        ("working condition", 2.0), ("gig economy", 2.0), ("informal employment", 2.0),
        ("precarious work", 2.0), ("occupational", 2.0),
    ],
    "79": [("travel agency", 2.0), ("tour operator", 2.0), ("ecotourism", 2.0)],
    "80": [("security service", 2.0), ("crime", 1.5), ("policing", 2.0), ("private security", 2.0)],
    "81": [("facility management", 2.0), ("cleaning service", 2.0)],
    "82": [("call center", 2.0), ("business support service", 2.0)],

    # P — Public administration
    "84": [
        ("government", 2.0), ("policy", 1.5), ("public administration", 2.0),
        ("governance", 2.0), ("regulation", 1.5), ("public sector", 2.0),
        ("military", 2.0), ("defence", 2.0), ("social security", 1.5),
        ("municipality", 2.0), ("parliament", 2.0), ("democracy", 1.5),
        ("bureaucracy", 2.0), ("welfare state", 2.0), ("public policy", 2.0),
        ("election", 2.0), ("voting", 2.0), ("corruption", 2.0),
        ("taxation", 2.0), ("law enforcement", 2.0), ("decentralization", 2.0),
    ],

    # Q — Education
    "85": [
        ("education", 2.0), ("school", 2.0), ("teaching", 2.0),
        ("learning", 1.5), ("student", 1.5), ("teacher", 2.0),
        ("curriculum", 2.0), ("classroom", 2.0), ("higher education", 2.0),
        ("primary school", 2.0), ("secondary school", 2.0),
        ("vocational training", 2.0), ("literacy", 2.0), ("dropout", 2.0),
        ("academic performance", 2.0), ("pedagogy", 2.0),
        ("educational", 2.0), ("adult education", 2.0), ("university student", 2.0),
    ],

    # R — Human health / Social work
    "86": [
        ("health", 1.5), ("medical", 2.0), ("clinical", 2.0),
        ("patient", 2.0), ("disease", 2.0), ("illness", 2.0),
        ("hospital", 2.0), ("healthcare", 2.0), ("doctor", 2.0),
        ("nurse", 2.0), ("treatment", 1.5), ("therapy", 2.0),
        ("diagnosis", 2.0), ("epidemic", 2.0), ("pandemic", 2.0),
        ("COVID", 2.0), ("mortality", 2.0), ("morbidity", 2.0),
        ("chronic disease", 2.0), ("mental health", 2.0),
        ("depression", 2.0), ("anxiety", 2.0), ("HIV", 2.0),
        ("malaria", 2.0), ("tuberculosis", 2.0), ("vaccination", 2.0),
        ("nutrition", 1.5), ("cancer", 2.0), ("surgery", 2.0),
        ("public health", 2.0), ("epidemiology", 2.0),
        ("health system", 2.0), ("primary care", 2.0), ("clinic", 2.0),
        ("maternal health", 2.0), ("child health", 2.0),
        ("drug use", 1.5), ("addiction", 2.0), ("obesity", 2.0),
        ("diabetes", 2.0), ("hypertension", 2.0), ("psychiatric", 2.0),
        ("health worker", 2.0), ("community health", 2.0),
    ],
    "87": [("nursing home", 2.0), ("care home", 2.0), ("residential care", 2.0), ("elderly care", 2.0)],
    "88": [
        ("social work", 2.0), ("social service", 2.0), ("welfare", 1.5),
        ("poverty", 1.5), ("disability", 2.0), ("refugee", 2.0),
        ("homeless", 2.0), ("child protection", 2.0), ("social support", 2.0),
        ("marginalized", 2.0), ("asylum seeker", 2.0), ("domestic violence", 2.0),
        ("gender violence", 2.0), ("social protection", 2.0),
    ],

    # S — Arts / Sports / Recreation
    "90": [
        ("art", 1.5), ("music", 1.5), ("theater", 2.0), ("dance", 2.0),
        ("performing arts", 2.0), ("artist", 2.0), ("cultural production", 2.0),
        ("creative industry", 2.0), ("visual art", 2.0),
    ],
    "91": [
        ("library", 2.0), ("archive", 2.0), ("museum", 2.0),
        ("cultural heritage", 2.0), ("digitization", 1.5),
        ("heritage", 1.5), ("monument", 2.0), ("cultural institution", 2.0),
    ],
    "92": [("gambling", 2.0), ("betting", 2.0), ("lottery", 2.0), ("casino", 2.0)],
    "93": [
        ("sport", 2.0), ("recreation", 1.5), ("leisure", 1.5),
        ("fitness", 2.0), ("athlete", 2.0), ("physical activity", 2.0),
    ],

    # T — Other services
    "94": [
        ("NGO", 2.0), ("non-profit", 2.0), ("civil society", 2.0),
        ("association", 1.5), ("volunteer", 2.0), ("charity", 2.0),
        ("community organization", 2.0), ("trade union", 2.0), ("advocacy", 2.0),
    ],
    "95": [("computer repair", 2.0), ("appliance repair", 2.0)],
    "96": [("personal service", 2.0), ("beauty salon", 2.0), ("hairdresser", 2.0)],

    # U — Households
    "97": [("domestic worker", 2.0), ("housekeeper", 2.0), ("paid domestic work", 2.0)],
    "98": [("household production", 2.0)],

    # V — Extraterritorial
    "99": [
        ("international organization", 2.0), ("United Nations", 2.0),
        ("diplomatic", 2.0), ("embassy", 2.0), ("INGO", 2.0),
    ],
}

# ── File-type boosts ───────────────────────────────────────────────────────────
# Extensions that are strong signals for specific ISIC divisions.
_FILETYPE_BOOSTS: dict[str, dict[str, float]] = {
    # QDA software → scientific research (N/72)
    ".atlasti":  {"72": 2.0}, ".atlproj":  {"72": 2.0}, ".atlproj23": {"72": 2.0},
    ".nvp":      {"72": 2.0}, ".nvpx":     {"72": 2.0},
    ".mex":      {"72": 2.0}, ".mx18":     {"72": 2.0}, ".mx20":      {"72": 2.0},
    ".qdpx":     {"72": 2.0}, ".rqda":     {"72": 2.0},
    ".f4":       {"72": 1.5}, ".f4a":      {"72": 1.5},
    # Biomedical / clinical data → R/86
    ".fcs":      {"86": 2.5},
    ".ndpi":     {"86": 2.0},
    ".lif":      {"86": 1.5},
    ".czi":      {"86": 1.5},
    ".fastq":    {"86": 1.5, "72": 0.5},
    ".bam":      {"86": 1.5, "72": 0.5},
    ".vcf":      {"86": 1.5},
    ".pdb":      {"86": 1.5, "72": 0.5},
    # ELAN qualitative annotation → linguistics/education
    ".eaf":      {"74": 1.5, "85": 1.0},
    # Statistical software → social science research
    ".sav":      {"72": 1.0, "86": 0.5, "85": 0.5},
    ".dta":      {"72": 1.0, "86": 0.5, "64": 0.5},
    ".jasp":     {"72": 1.5},
    # Geospatial → agriculture / civil engineering / real estate
    ".shp":      {"01": 0.5, "42": 0.5, "68": 0.5},
    ".gpkg":     {"01": 0.5, "42": 0.5},
    ".geojson":  {"01": 0.5, "42": 0.5},
    # Climate/environmental rasters → agriculture / energy
    ".nc":       {"01": 0.5, "35": 0.5},
    ".nc4":      {"01": 0.5, "35": 0.5},
}

# ── Extra tag vocabulary ───────────────────────────────────────────────────────
_TAG_EXTRAS: list[str] = [
    "qualitative", "quantitative", "mixed methods", "longitudinal", "cross-sectional",
    "survey", "interview", "focus group", "ethnography", "observation",
    "randomized", "cohort", "case study", "secondary data",
    "Africa", "Asia", "Europe", "Latin America", "developing country",
    "low-income", "urban", "rural", "community",
    "women", "children", "adolescent", "elderly", "youth",
    "indigenous", "minority", "immigrant", "migrant",
    "gender", "race", "ethnicity", "inequality", "poverty",
    "audio", "video", "transcript", "open data",
]


class HybridClassifier:
    """
    Option C hybrid classifier: keyword scoring → file-type boosts → content re-scoring.
    Instantiate once; reuse across all projects and files.
    """

    def __init__(self) -> None:
        self._pats: dict[str, list[tuple[re.Pattern, float, str]]] = {}
        for div, kws in _DIVISION_KEYWORDS.items():
            self._pats[div] = [
                (re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE), w, kw)
                for kw, w in kws
            ]
        self._tag_pats: list[tuple[re.Pattern, str]] = [
            (re.compile(r"\b" + re.escape(t) + r"\b", re.IGNORECASE), t)
            for t in _TAG_EXTRAS
        ]

    # ── Scoring helpers ───────────────────────────────────────────────────────

    def _score(self, text: str, mult: float = 1.0) -> dict[str, float]:
        out: dict[str, float] = {}
        if not text:
            return out
        for div, pats in self._pats.items():
            s = sum(w for pat, w, _ in pats if pat.search(text)) * mult
            if s:
                out[div] = s
        return out

    def _add(self, base: dict[str, float], extra: dict[str, float]) -> None:
        for div, s in extra.items():
            base[div] = base.get(div, 0.0) + s

    def _apply_filetype_boosts(self, scores: dict[str, float], file_types: str) -> None:
        for ft in (file_types or "").split():
            ext = ft if ft.startswith(".") else "." + ft
            boosts = _FILETYPE_BOOSTS.get(ext.lower(), {})
            for div, b in boosts.items():
                scores[div] = scores.get(div, 0.0) + b

    def _pick(self, scores: dict[str, float]) -> tuple[str, str]:
        if not scores:
            return DEFAULT_DIVISION, "low"
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        best_div, best_score = ranked[0]
        second_score = ranked[1][1] if len(ranked) > 1 else 0.0
        margin = best_score - second_score
        confidence = "high" if margin >= 3.0 else "medium" if margin >= 1.0 else "low"
        return best_div, confidence

    def _resolve(self, division: str) -> tuple[str, str, str]:
        sec, sec_name = section_for_division(division)
        if not sec:
            sec, sec_name = DEFAULT_SECTION, SECTION_MAP.get(DEFAULT_SECTION, "")
        return sec, sec_name or "", _div_name(division) or ""

    def _extract_tags(self, text: str, best_div: str) -> list[str]:
        matched: list[tuple[float, str]] = []
        for div, pats in self._pats.items():
            for pat, w, kw in pats:
                if pat.search(text):
                    boost = 1.5 if div == best_div else 1.0
                    matched.append((w * boost, kw.lower()))
        for pat, tag in self._tag_pats:
            if pat.search(text):
                matched.append((1.0, tag.lower()))
        seen: set[str] = set()
        tags: list[str] = []
        for _, kw in sorted(matched, reverse=True):
            if kw not in seen and len(kw) > 2:
                seen.add(kw)
                tags.append(kw)
            if len(tags) >= 8:
                break
        return tags or [(_div_name(best_div) or "").lower()]

    # ── Public API ────────────────────────────────────────────────────────────

    def classify_project(
        self,
        title: str,
        description: str,
        keywords: str,
        file_types: str,
        snippets: dict[str, str] | None = None,
    ) -> dict:
        """Classify a whole project. Returns dict with section/division/confidence/tags."""
        scores: dict[str, float] = {}
        self._add(scores, self._score(title or "",       mult=3.0))
        self._add(scores, self._score(keywords or "",    mult=2.0))
        self._add(scores, self._score(description or "", mult=1.0))
        self._apply_filetype_boosts(scores, file_types or "")
        if snippets:
            for text in snippets.values():
                self._add(scores, self._score(text, mult=0.5))

        best_div, confidence = self._pick(scores)
        section, sec_name, div_name = self._resolve(best_div)

        full_text = " ".join(filter(None, [title, keywords, description] + list((snippets or {}).values())))
        tags = self._extract_tags(full_text, best_div)

        return {
            "section": section, "section_name": sec_name,
            "division": best_div, "division_name": div_name,
            "confidence": confidence, "tags": tags,
        }

    def classify_file(
        self,
        file_name: str,
        file_type: str,
        proj_section: str,
        proj_division: str,
        proj_section_name: str,
        proj_division_name: str,
        proj_snippets: dict[str, str] | None = None,
    ) -> dict:
        """
        Classify one primary data file.
        Inherits parent project classification when file-level signal is too weak.
        """
        scores: dict[str, float] = {}
        self._add(scores, self._score(file_name or "", mult=2.0))

        ext = Path(file_name).suffix.lower() if file_name else ""
        if not ext and file_type:
            ext = file_type if file_type.startswith(".") else "." + file_type
        boosts = _FILETYPE_BOOSTS.get(ext, {})
        for div, b in boosts.items():
            scores[div] = scores.get(div, 0.0) + b

        if proj_snippets and file_name in proj_snippets:
            self._add(scores, self._score(proj_snippets[file_name], mult=0.5))

        if not scores or max(scores.values()) < 1.5:
            return {
                "section": proj_section or DEFAULT_SECTION,
                "section_name": proj_section_name,
                "division": proj_division or DEFAULT_DIVISION,
                "division_name": proj_division_name,
                "confidence": "low",
            }

        best_div, confidence = self._pick(scores)
        section, sec_name, div_name = self._resolve(best_div)
        return {
            "section": section, "section_name": sec_name,
            "division": best_div, "division_name": div_name,
            "confidence": confidence,
        }
