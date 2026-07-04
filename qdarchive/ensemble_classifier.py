"""
Two-step ensemble classifier for ISIC Rev. 5  —  two-stage + few-shot.

Stage A  Sentence Transformer embeds project text → top-3 ISIC Sections
         (22 choices).  LLM picks one Section.

Stage B  Sentence Transformer narrows to divisions inside the chosen Section
         → top-K divisions.  LLM picks one Division, guided by few-shot
         examples so it classifies by subject, not methodology.

Fallback  if Ollama is unavailable, returns top-1 embedding at each stage.
"""

from __future__ import annotations

import json
import logging
import re

from qdarchive.isic import (
    DIVISIONS, DIVISION_MAP, SECTION_MAP, SECTIONS, VALID_DIVISIONS,
)

from qdarchive.isic_enriched import get_disambiguation_block, build_division_context

log = logging.getLogger(__name__)

TOP_K_SECTIONS  = 3   # section candidates forwarded to LLM in Stage A
TOP_K_DIVISIONS = 5   # division candidates forwarded to LLM in Stage B
LOW_SCORE_THRESHOLD = 0.28  # free-pick mode when embedding is uncertain

_DISAMBIGUATION = get_disambiguation_block()   # loaded once at import

# ── Keyword-based section pre-filter ─────────────────────────────────────────
# These rules fire BEFORE the embedding when the signal is unambiguous.
# Each entry: (forced_section, compiled_regex).
# Only add patterns where false positives are near-impossible.
import re as _re
_FORCED_SECTION_RULES: list[tuple[str, "_re.Pattern"]] = [
    # P — elections / voting datasets
    ("P", _re.compile(
        r"\b("
        r"(general|local|regional|national|municipal|parliamentary|presidential"
        r"|spanish|french|german|italian|british|american|canadian|australian"
        r"|swedish|norwegian|danish|dutch|belgian|polish|czech|hungarian"
        r"|brazilian|mexican|indian|japanese|korean|nigerian|kenyan|ghanaian"
        r"|south\s+african|ugandan|tanzanian|zambian|zimbabwean|senegalese)"
        r"\s+elections?"
        r"|election\s+(result|data|outcome|return|dataset|replication|study|survey)"
        r"|elections?\s+(data|result|outcome|study|survey|dataset|replication)"
        r"|voting\s+(record|data|pattern|behavior|behaviour|turnout|result)"
        r"|electoral\s+(roll|register|fraud|trust|integrity|data|system|reform)"
        r"|ballot\s+(data|result|count|box)"
        r")\b",
        _re.I,
    )),
    # R — clinical trials / HIV / disease / public-health studies
    ("R", _re.compile(
        r"\b("
        r"clinical\s+trial|randomized\s+(controlled\s+)?trial|randomised\s+trial"
        r"|antiretroviral|NNRTI|PMTCT|nevirapine|lopinavir|efavirenz"
        r"|cancer\s+(screening|registry|cohort|incidence|survival)"
        r"|epidemiolog(y|ical)\s+(survey|study|data)"
        r"|vaccine\s+(trial|efficacy|coverage|uptake)"
        # tropical / neglected diseases that embed near E (water/waste)
        r"|onchocerciasis|lymphatic\s+filariasis|loiasis|schistosomiasis"
        r"|leishmaniasis|trypanosomiasis|neglected\s+tropical\s+disease"
        r"|disease\s+(mapping|burden|surveillance|incidence\s+map)"
        r"|public\s+health\s+impacts?"
        r"|district\s+mapping\s+.*?(disease|infection|prevalence)"
        r")\b",
        _re.I,
    )),
    # B — oil / gas / petroleum datasets
    ("B", _re.compile(
        r"\b("
        r"oil\s+(and|&)\s+gas\s+(industr|emission|sector|compan)"
        r"|methane\s+emissions?\s+from\s+(oil|gas|petroleum|fossil)"
        r"|petroleum\s+(sector|industr|extract)"
        r"|hydraulic\s+fracturing|fracking\s+data"
        r")\b",
        _re.I,
    )),
    # O — labour / employment market studies
    ("O", _re.compile(
        r"\b("
        r"labour\s+market|labor\s+market"
        r"|unemployment\s+(insurance|benefit|duration|spell|transition|to\s+self)"
        r"|self.employment\s+(decision|transition|assistance|program|rate|entry)"
        r"|from\s+unemployment\s+to"
        r"|job\s+(search|seeker|finding|loss)\s+(model|data|behav)"
        r"|gig\s+(economy|work|worker)\s+(data|survey|study)"
        r"|wage\s+(inequality|gap|growth|premium)\s+(data|study|survey)"
        r"|informal\s+(employ|labour|labor|work)\s+(data|survey)"
        r")\b",
        _re.I,
    )),
    # N — environmental / earth / physical science (prevents false-positive E)
    ("N", _re.compile(
        r"\b("
        # hydrological / coastal / oceanographic measurements
        r"bathymetry|bathymetric"
        r"|sea[\s\-]level\s+(rise|data|change|record|measur|monitor|trends?|pressure|height|anomal|variab|contribution)"
        r"|barrier\s+islands?"
        r"|mixed\s+layer\s+(depth|data|variab)"
        r"|ocean(ic|ographic|ography)?\s+(current|circulation|heat|acoust|sound|temperatur|wave|vortex|eddy|front|mixing|salinity|sector)"
        r"|oceanic\s+(internal|mesoscale|submesoscale)"
        r"|submesoscale\s+(coherent|vortex|front|eddy)"
        r"|internal\s+solitary\s+wave"
        r"|air[\s\-]sea\s+(interaction|flux|exchange|interface)"
        r"|sound\s+speed\s+profile"
        r"|acoustic\s+sound\s+speed"
        r"|sea\s+(surface\s+temperature|foam|ice|floor|level\s+pressure|floor\s+mapping)"
        r"|sea\s+ice\s+(concentration|thickness|extent|cover|melt|data|record|area|drift)"
        r"|tidal\s+(gauge|record|data|station)"
        r"|ocean\s+salinity|salinity\s+profile"
        r"|shoreline\s+(data|change|extract|dataset|mapping)"
        r"|(sea|ocean|coastal|marine|lake|estuar\w*)\s+shorelines?"
        r"|coastal\s+(bathymetry|morpholog|erosion\s+data|sediment\s+data)"
        # atmospheric / climate science
        r"|atmospheric\s+(model|composition|pressure|dynamic|chemistry|transport)"
        r"|radiosonde|lidar\s+(profile|measurement|data)"
        r"|ice\s+(core|sheet|cap)\s+(data|records?|drill|sample)"
        r"|global\s+sea\s+level\s+pressure"
        # geological / geophysical
        r"|seismic\s+(wave|survey|data|record|profile)"
        r"|geophysical\s+(survey|data|model|profile)"
        r"|stratigraph(y|ic)\s+(data|record|section|core)"
        r"|mineral(ogical|ogy)\s+(analysis|data|sample)"
        # ecology / biology (field science, not sector)
        r"|metabarcoding|metagenom(ics|e)"
        r"|phylogeograph(y|ic)"
        r"|biogeograph(y|ic)\s+(classification|analysis|survey)"
        r"|wrackbed|microbiome\s+(analysis|survey|data)"
        r")\b",
        _re.I,
    )),
]

# ── Section descriptions (Stage A index) ──────────────────────────────────────
_SECTION_DESCRIPTIONS: dict[str, str] = {
    "A": "Research on agriculture, farming, crops, livestock, animal husbandry, fisheries, aquaculture, forestry, rural livelihoods, food production, agronomy",
    "B": "Research on extractive industries: mining companies, coal mines, oil and gas firms, petroleum workers, drilling operations, quarrying operations, mineral extraction businesses, fracking industry",
    "C": "Research on manufacturing, industrial production, factories, goods production, textiles, chemicals, pharmaceuticals production, food processing",
    "D": "Research on electricity, power generation, renewable energy, solar, wind, gas supply, energy grids, energy policy",
    "E": "Research on water supply, sanitation, sewerage, waste management, recycling, environmental remediation, pollution cleanup",
    "F": "Research on construction, building, housing, civil engineering, infrastructure, urban development, architecture",
    "G": "Research on trade, retail, wholesale, shops, markets, consumer behavior, e-commerce, supply chains, market vendors",
    "H": "Research on transport, logistics, railways, roads, shipping, aviation, trucking, mobility, commuting, freight",
    "I": "Research on hospitality, hotels, restaurants, tourism accommodation, food service, catering",
    "J": "Research on media, publishing, broadcasting, journalism, news, film, television, music, content production, social media platforms",
    "K": "Research on telecommunications, software development, IT, programming, internet, digital technology, cloud computing, data centers",
    "L": "Research on banking, finance, insurance, investment, credit, microfinance, financial markets, pension funds",
    "M": "Research on real estate, housing markets, property, rent, tenants, gentrification, urban housing",
    "N": "Research on scientific R&D, basic science, geochemistry, volcanology, geology, oceanography, atmospheric science, climatology, marine biology, ecology, condensed matter physics, materials science, bioacoustics, interdisciplinary science, professional services, management consulting, legal services, architecture, advertising, veterinary activities",
    "O": "Research on administrative services, employment agencies, travel agencies, security services, cleaning, outsourcing",
    "P": "Research on government, public policy, politics, elections, voting, governance, civil service, military, defense, public administration, democracy",
    "Q": "Research on education, schools, universities, teachers, students, learning, pedagogy, literacy, curriculum, educational policy",
    "R": "Research on healthcare, medicine, hospitals, patients, clinical trials, mental health, public health, disease, epidemiology, social work, child protection, welfare, nursing homes",
    "S": "Research on arts, culture, sports, recreation, museums, libraries, archives, cultural heritage, performing arts, leisure",
    "T": "Research on personal services, repair, household activities, membership organizations, NGOs, civil society, trade unions",
    "U": "Research on domestic workers, household employment, subsistence activities, informal household economy",
    "V": "Research on international organizations, UN agencies, diplomatic missions, global governance, intergovernmental bodies",
}

# ── Division descriptions (Stage B index) ─────────────────────────────────────
_DIVISION_DESCRIPTIONS: dict[str, str] = {
    "01": "Agriculture, farming, crop science, animal husbandry, livestock, food production, rural livelihoods, agronomy, horticulture, farmers",
    "02": "Forestry, forest management, timber, deforestation, reforestation, woodland ecology, logging, silviculture",
    "03": "Fisheries, aquaculture, marine fishing, fish stocks, coastal fishing communities, seafood",
    "05": "Coal mining, miners, mining communities, coal industry, occupational health in coal sector",
    "06": "Oil and gas extraction, petroleum industry, drilling, offshore platforms, energy workers, fracking, methane emissions, fossil fuels",
    "07": "Metal mining, mineral extraction, mining communities, ore processing, artisanal mining",
    "08": "Quarrying, sand extraction, stone mining, non-metallic minerals, construction materials extraction",
    "09": "Mining support services, drilling contractors, mine safety",
    "10": "Food processing, food manufacturing, food industry workers, food safety, food production chains",
    "11": "Beverage industry, alcohol production, brewing, winemaking, soft drinks manufacturing",
    "12": "Tobacco industry, smoking, tobacco farming, cigarette manufacturing, tobacco policy",
    "13": "Textile industry, garment workers, fabric production, cotton, fashion supply chains",
    "14": "Clothing, apparel manufacturing, fashion industry, garment factories, textile workers",
    "15": "Leather goods, shoe manufacturing, tanneries, leather workers",
    "16": "Wood products, timber industry, furniture manufacturing, sawmills",
    "17": "Paper and pulp industry, papermaking, paper recycling",
    "18": "Printing industry, publishing production, media printing",
    "19": "Petroleum refining, fuel production, petrochemical industry, energy processing",
    "20": "Chemical industry, chemical production, industrial chemicals, hazardous substances, pesticides",
    "21": "Pharmaceutical manufacturing, drug production, medicine manufacturing, biopharmaceuticals",
    "22": "Rubber, plastics industry, plastic pollution, packaging manufacturing",
    "23": "Glass, ceramics, cement, non-metallic mineral products, construction materials manufacture",
    "24": "Steel, iron, metals production, foundries, metallurgy, smelting",
    "25": "Metal fabrication, machine parts, industrial components, metalworking",
    "26": "Electronics, semiconductors, computers, optical instruments, ICT hardware manufacturing",
    "27": "Electrical equipment, power systems, batteries, electrical engineering manufacturing",
    "28": "Machinery, industrial equipment, engineering manufacturing, factory machines",
    "29": "Automotive industry, car manufacturing, vehicle production, automobile workers",
    "30": "Aerospace, shipbuilding, rail vehicles, military vehicles, transport equipment manufacturing",
    "31": "Furniture manufacturing, interior design industry, home goods production",
    "32": "Other manufacturing, sports equipment, toys, medical devices, jewelry",
    "33": "Machinery repair, maintenance services, industrial servicing",
    "35": "Energy sector, electricity production, power grids, renewable energy, solar, wind, gas supply, energy policy",
    "36": "Water supply, drinking water, water treatment, water utilities, access to clean water",
    "37": "Sewerage, sanitation systems, wastewater management, sewage treatment",
    "38": "Waste management, recycling, landfills, garbage collection, environmental pollution",
    "39": "Environmental remediation, contaminated land cleanup, hazardous waste, soil pollution",
    "41": "Construction industry, building workers, residential housing, real estate development",
    "42": "Civil engineering, infrastructure construction, roads, bridges, dams, public works",
    "43": "Specialized construction, electrical installation, plumbing, building trades",
    "46": "Wholesale trade, supply chains, distribution networks, business-to-business commerce",
    "47": "Retail, shops, consumer behavior, shopping, supermarkets, e-commerce, market vendors",
    "49": "Road transport, railways, trucking, logistics, commuting, mobility, transport workers",
    "50": "Shipping, maritime transport, ferries, ports, seafarers, ocean freight",
    "51": "Aviation, airlines, airports, air travel, flight workers, air transport policy",
    "52": "Logistics, warehousing, freight handling, supply chain management",
    "53": "Postal services, courier delivery, last-mile logistics, postal workers",
    "55": "Hotels, tourism accommodation, hospitality workers, lodging, guesthouses",
    "56": "Restaurants, food service, hospitality industry, catering, street food vendors",
    "58": "Book publishing, journals, newspapers, magazines, media publishing, academic publishing",
    "59": "Film industry, television production, music recording, documentary, media production",
    "60": "Broadcasting, radio, television, news agencies, journalism, content distribution",
    "61": "Telecommunications, mobile networks, internet access, telecom companies, digital connectivity",
    "62": "Software development, IT consulting, digital technology, app development, tech workers, programming",
    "63": "Cloud computing, data centers, internet platforms, big data, digital infrastructure",
    "64": "Banking, financial services, credit, loans, microfinance, financial inclusion, fintech",
    "65": "Insurance, risk management, pension funds, social insurance systems",
    "66": "Financial markets, investment, securities trading, financial regulation",
    "68": "Real estate, housing markets, property development, urban housing, gentrification, rent, tenants",
    "69": "Legal services, law firms, accounting, auditing, justice systems, courts",
    "70": "Corporate management, organizational behavior, business strategy, leadership, governance",
    "71": "Engineering consultancy, architecture, urban planning, technical testing, design professions",
    "72": "Scientific research and development, academic studies, R&D, interdisciplinary research, scholarly inquiry, research methodology",
    "73": "Marketing, advertising, public relations, market research, consumer behavior surveys, branding",
    "74": "Creative professionals, design, photography, translation, scientific consulting",
    "75": "Veterinary medicine, animal health, livestock disease, pet care, wildlife health",
    "77": "Rental services, car rental, equipment leasing, sharing economy, platform economy",
    "78": "Employment, labor markets, job seeking, recruitment agencies, workforce, gig economy",
    "79": "Tourism, travel agencies, tour operators, tourist experiences, travel behavior",
    "80": "Security services, private policing, surveillance, crime prevention",
    "81": "Cleaning services, landscaping, facility management, building services",
    "82": "Administrative services, call centers, business process outsourcing, office work",
    "84": "Government, public policy, governance, civil service, military, defense, public administration, elections, voting, democracy, political participation",
    "85": "Education, schools, universities, teachers, students, learning, pedagogy, educational policy, literacy, curriculum",
    "86": "Medicine, healthcare, hospitals, patients, clinical research, clinical trials, mental health, public health, disease, epidemiology, nursing, HIV, cancer, reproductive health, abortion",
    "87": "Nursing homes, residential care, elder care, disability support, care institutions",
    "88": "Social work, community services, welfare, child protection, homelessness, family support, poverty",
    "90": "Arts, culture, music, theater, dance, creative expression, performing arts, artists",
    "91": "Museums, libraries, archives, cultural heritage, memory institutions, oral history, digital humanities",
    "92": "Gambling, casinos, betting, lottery, gaming behavior",
    "93": "Sports, athletics, recreation, leisure, physical activity, sports sociology, fitness",
    "94": "Civil society, NGOs, associations, trade unions, religious organizations, volunteering",
    "95": "Repair economy, electronics repair, consumer goods maintenance, circular economy",
    "96": "Personal services, hair salons, personal care, beauty industry, wellness",
    "97": "Domestic workers, housekeeping, household employment, care workers in private homes",
    "98": "Subsistence activities, household production, informal economy",
    "99": "International organizations, UN agencies, diplomatic missions, global governance",
}

# ── Few-shot examples for the LLM ─────────────────────────────────────────────
_FEW_SHOT = """EXAMPLES of correct ISIC classifications for research datasets:
  • HIV antiretroviral treatment in patients, clinical trial, CD4 counts, Africa → R/86 Human health activities
  • Cancer screening, TikTok posts about skin cancer, health communication → R/86 Human health activities
  • Abortion reporting in surveys, cognitive interviews, reproductive health → R/86 Human health activities
  • Mental health interviews, depression, therapy outcomes, patients → R/86 Human health activities
  • Social work with homeless families, child protection cases → R/88 Social work activities
  • Electoral trust, voting behavior, 2020 US election, political participation → P/84 Public administration
  • Teacher professional development, classroom observation, student outcomes → Q/85 Education
  • Methane emissions from oil and gas industry, petroleum sector → B/06 Petroleum extraction
  • Organic farming practices, crop yield, smallholder farmers → A/01 Agriculture
  • Museum collections, oral history archives, cultural heritage → S/91 Libraries and archives
  • Software engineers' work practices, agile development, tech companies → K/62 Software development
  • Lake bathymetry, hydrology measurements, environmental field study → N/72 Scientific R&D
  • Zeeman-type band splitting, ferroelectrics, DFT calculations, condensed matter → N/72 Scientific R&D
  • RNA sequencing, protein structure, genomics, molecular biology → N/72 Scientific R&D
  • Acoustic sound speed profiling in ocean (physics method) → N/72 Scientific R&D
  • Sea-level rise measurements, coastal barrier island transects, oceanographic data → N/72 Scientific R&D
  • District mapping of onchocerciasis / lymphatic filariasis / neglected tropical diseases → R/86 Health
  • Public health impacts of oil spill, pollution effects on population → R/86 Health
  • Drinking water utility operations, sanitation company management → E/36 Water supply

RULE: Classify by the SUBJECT being studied, NOT the method.
  A qualitative study ABOUT health → R/86 (not N/72 just because it uses qualitative methods)
  A survey ABOUT politics → P/84 (not N/73 just because it involves surveys)
  Physics/materials/chemistry with no specific sector → N/72 (NOT D/35 just because it mentions electricity)
  Environmental SCIENCE (oceanography, hydrology, ecology, climatology) → N/72 (NOT E — E is for utility/waste companies)
  Only use N/72 if the research is ABOUT research itself, or spans multiple sectors."""


class EnsembleClassifier:
    """
    Two-stage ISIC Rev. 5 classifier with few-shot LLM prompting.

    Stage A: embed → top-3 Sections → LLM picks one Section
    Stage B: embed within Section → top-5 Divisions → LLM picks one Division

    Usage::
        clf = EnsembleClassifier()
        result = clf.classify(title=..., description=..., keywords=[...],
                              file_types=[...], snippets={...})
    """

    def __init__(
        self,
        embed_model: str = "paraphrase-multilingual-MiniLM-L12-v2",
        llm_model: str = "mistral",
        top_k_sections: int = TOP_K_SECTIONS,
        top_k_divisions: int = TOP_K_DIVISIONS,
        ollama_url: str = "http://localhost:11434",
    ):
        self.llm_model       = llm_model
        self.top_k_sections  = top_k_sections
        self.top_k_divisions = top_k_divisions
        self.ollama_url      = ollama_url.rstrip("/")

        log.info(f"Loading sentence-transformer: {embed_model}")
        from sentence_transformers import SentenceTransformer
        self._embedder = SentenceTransformer(embed_model)

        # ── Section index (Stage A) ────────────────────────────────────────
        self._sections = list(SECTIONS)   # [(letter, name), ...]
        sec_texts = [
            f"Section {letter}: {name}. {_SECTION_DESCRIPTIONS.get(letter, '')}"
            for letter, name in self._sections
        ]
        log.info("Building section index (22 sections)...")
        self._sec_index = self._embedder.encode(
            sec_texts, normalize_embeddings=True, show_progress_bar=False
        )

        # ── Division index (Stage B) ───────────────────────────────────────
        self._divisions = list(DIVISIONS)  # [(section, division, name), ...]
        div_texts = [
            f"Division {div}: {name}. {_DIVISION_DESCRIPTIONS.get(div, '')}"
            for _, div, name in self._divisions
        ]
        log.info("Building division index (87 divisions)...")
        self._div_index = self._embedder.encode(
            div_texts, normalize_embeddings=True, show_progress_bar=False
        )
        log.info("Classifier ready.")

        self._ollama_ok = self._ping_ollama()
        if self._ollama_ok:
            log.info(f"Ollama available — two-stage LLM enabled ({llm_model})")
        else:
            log.warning("Ollama unavailable — using top-1 embedding fallback")

    # ── Public API ────────────────────────────────────────────────────────────

    def classify(
        self,
        title: str,
        description: str = "",
        keywords: list[str] | None = None,
        file_types: list[str] | None = None,
        snippets: dict[str, str] | None = None,
        file_names: list[str] | None = None,
    ) -> dict:
        """Return {section, section_name, division, division_name, confidence, method}."""
        import numpy as np
        text = _build_query(title, description, keywords, file_types, snippets)
        vec  = self._embedder.encode(text, normalize_embeddings=True)

        # Guard: zero/NaN vector (binary file content) → re-encode title only
        if not np.isfinite(vec).all() or np.linalg.norm(vec) < 1e-9:
            fallback = _build_query(title, "", keywords, [], None)
            vec = self._embedder.encode(fallback, normalize_embeddings=True)
            if not np.isfinite(vec).all():
                vec = np.zeros_like(vec)

        # Stage A — pick Section
        section, sec_method = self._stage_a(
            vec, title, description, keywords, file_types, snippets, file_names
        )

        # Stage B — pick Division within that Section
        division, div_name, confidence, div_method = self._stage_b(
            vec, section, title, description, keywords, file_types, snippets, file_names
        )

        method = f"{sec_method}+{div_method}"

        # N/72 challenge — if the result is N/72, ask the LLM to reconsider
        # whether there's a more specific section (fired once, cheap)
        if division == "72" and self._ollama_ok:
            challenged = self._challenge_n72(
                vec, title, description, keywords, file_types, snippets, file_names
            )
            if challenged:
                return challenged

        # E challenge — verify E is really water/waste sector, not env science or health
        if section == "E" and self._ollama_ok:
            challenged = self._challenge_e(
                vec, title, description, keywords, file_types, snippets, file_names
            )
            if challenged:
                return challenged

        return {
            "section":       section,
            "section_name":  SECTION_MAP.get(section, ""),
            "division":      division,
            "division_name": div_name,
            "confidence":    confidence,
            "method":        method,
        }

    # ── Stage A: Section ──────────────────────────────────────────────────────

    def _stage_a(self, vec, title, description, keywords, file_types, snippets, file_names=None):
        # ── Keyword pre-filter: bypass embedding for unambiguous cases ────────
        signal_text = " ".join(filter(None, [
            title or "",
            " ".join(keywords or []),
            (description or "")[:300],
        ]))
        for forced_section, pattern in _FORCED_SECTION_RULES:
            if pattern.search(signal_text):
                log.debug(f"Section forced to {forced_section} by keyword rule")
                return forced_section, "forced_section"

        # ── Normal path: embedding → LLM ─────────────────────────────────────
        scores = (self._sec_index @ vec).tolist()
        top = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)[: self.top_k_sections]
        candidates = [
            {"letter": self._sections[i][0], "name": self._sections[i][1], "score": round(s, 4)}
            for i, s in top
        ]

        if not self._ollama_ok:
            return candidates[0]["letter"], "embedding"

        section = self._llm_pick_section(
            candidates, title, description, keywords, file_types, snippets, file_names
        )
        method = "llm"

        if section not in SECTION_MAP:
            section = candidates[0]["letter"]
            method  = "embedding_fallback"

        return section, method

    def _llm_pick_section(self, candidates, title, description, keywords, file_types, snippets, file_names=None):
        options = "\n".join(f"  {c['letter']}. {c['name']}" for c in candidates)
        kw_str = ", ".join((keywords or [])[:12]) or "—"
        ft_str = ", ".join(sorted(set(file_types or []))) or "—"
        fn_str = ", ".join((file_names or [])[:8]) or "—"
        snip_block = ""
        if snippets:
            for fname, text in list(snippets.items())[:2]:
                snip_block += f"\n[{fname}]: {text[:200]}"

        prompt = (
            f"{_DISAMBIGUATION}\n\n"
            "Classify this research dataset into the best ISIC Section.\n"
            f"Title      : {title}\n"
            f"Keywords   : {kw_str}\n"
            f"File names : {fn_str}\n"
            f"Description: {(description or '')[:300]}\n"
            + (f"File content:{snip_block}\n" if snip_block else "")
            + f"\nCandidates:\n{options}\n\n"
            "Reply with ONLY valid JSON:\n"
            '{"section": "<letter>", "confidence": "high|medium|low", "reason": "<one sentence>"}'
        )
        raw = self._call_llm(prompt, max_tokens=100)
        parsed = _extract_json(raw)
        return str(parsed.get("section", "")).strip().upper()

    # ── Stage B: Division ─────────────────────────────────────────────────────

    def _stage_b(self, vec, section, title, description, keywords, file_types, snippets, file_names=None):
        section_divs = [(i, sec, div, name)
                        for i, (sec, div, name) in enumerate(self._divisions)
                        if sec == section]

        if not section_divs:
            scores = (self._div_index @ vec).tolist()
            best_i = max(range(len(scores)), key=lambda x: scores[x])
            _, div, name = self._divisions[best_i]
            return div, name, "low", "embedding_fallback"

        div_scores = [(orig_i, sec, div, name, (self._div_index[orig_i] @ vec))
                      for orig_i, sec, div, name in section_divs]
        div_scores.sort(key=lambda x: x[4], reverse=True)
        top = div_scores[: self.top_k_divisions]

        candidates = [
            {"division": div, "name": name, "score": round(float(score), 4)}
            for _, _, div, name, score in top
        ]

        best_score = candidates[0]["score"]
        conf = "high" if best_score > 0.50 else ("medium" if best_score > 0.35 else "low")

        if not self._ollama_ok:
            c = candidates[0]
            return c["division"], c["name"], conf, "embedding"

        division, confidence, method = self._llm_pick_division(
            candidates, section, title, description, keywords, file_types, snippets, best_score, file_names
        )
        return division, DIVISION_MAP.get(division, ("", ""))[1], confidence, method

    def _llm_pick_division(
        self, candidates, section, title, description, keywords, file_types, snippets, best_score, file_names=None
    ):
        kw_str = ", ".join((keywords or [])[:12]) or "—"
        ft_str = ", ".join(sorted(set(file_types or []))) or "—"
        fn_str = ", ".join((file_names or [])[:8]) or "—"
        snip_block = ""
        if snippets:
            for fname, text in list(snippets.items())[:2]:
                snip_block += f"\n[{fname}]: {text[:250]}"

        # Build option lines enriched with disambiguation hints from DIVISION_GUIDE
        def _option_line(div: str, name: str) -> str:
            ctx = build_division_context(div)
            return f"  {div}. {name}" + (f"  [{ctx}]" if ctx else "")

        if best_score < LOW_SCORE_THRESHOLD:
            options = "\n".join(
                _option_line(div, name)
                for sec, div, name in self._divisions if sec == section
            )
            note = "(embedding was uncertain — all divisions in this section are shown)"
        else:
            options = "\n".join(_option_line(c["division"], c["name"]) for c in candidates)
            note = "(top candidates by semantic similarity)"

        prompt = (
            f"Section {section} — {SECTION_MAP.get(section, '')}\n\n"
            "Pick the single best ISIC Division for this research dataset.\n"
            f"Title      : {title}\n"
            f"Keywords   : {kw_str}\n"
            f"File types : {ft_str}\n"
            f"File names : {fn_str}\n"
            f"Description: {(description or '')[:400]}\n"
            + (f"Primary data:{snip_block}\n" if snip_block else "")
            + f"\nCandidates {note}:\n{options}\n"
            "(Each candidate shows: code. name  [description | NOT: common confusions])\n\n"
            "Reply with ONLY valid JSON:\n"
            '{"division": "<2-digit code>", "confidence": "high|medium|low", "reason": "<one sentence>"}'
        )
        raw = self._call_llm(prompt, max_tokens=150)
        parsed = _extract_json(raw)
        div  = str(parsed.get("division", "")).strip().zfill(2)
        conf = parsed.get("confidence", "medium")

        if div in DIVISION_MAP and DIVISION_MAP[div][0] == section:
            return div, conf, "llm"

        valid = {d for s, d, _ in self._divisions if s == section}
        if div in valid:
            return div, conf, "llm"

        log.debug(f"LLM division {div} not valid for section {section}; using top-1")
        return candidates[0]["division"], "low", "embedding_fallback"

    # ── N/72 challenge ────────────────────────────────────────────────────────

    def _challenge_n72(self, vec, title, description, keywords, file_types, snippets, file_names=None):
        """
        When N/72 is chosen, ask the LLM to reconsider whether a more specific
        section applies.  If it picks a different section, re-run Stage B there.
        Returns a full result dict, or None to keep the original N/72.
        """
        kw_str = ", ".join((keywords or [])[:12]) or "—"
        fn_str = ", ".join((file_names or [])[:8]) or "—"
        snip_block = ""
        if snippets:
            for fname, text in list(snippets.items())[:2]:
                snip_block += f"\n[{fname}]: {text[:200]}"

        # Offer all non-N sections plus N as last resort
        non_n = [(letter, name) for letter, name in self._sections if letter != "N"]
        options = "\n".join(f"  {letter}. {name}" for letter, name in non_n)
        options += "\n  N. Scientific/technical activities (ONLY if about research methodology itself)"

        prompt = (
            "The initial classification was N/72 (Scientific R&D).\n"
            "N/72 is ONLY correct for basic science (physics, chemistry, materials) or meta-science.\n"
            "If the research studies a specific HUMAN or SOCIAL domain, pick that domain:\n"
            "  health/disease/epidemiology → R  |  politics/elections → P  |  education → Q\n"
            "  agriculture/farming → A           |  transport/mobility → H   |  energy sector → D\n\n"
            "CRITICAL: Do NOT pick E (water/waste) for environmental SCIENCE.\n"
            "  E = water utilities, sanitation companies, waste management OPERATIONS.\n"
            "  Oceanography, hydrology, lake science, sea-level, ecology, atmospheric science,\n"
            "  marine biology, climatology → stay N/72.\n\n"
            f"Title      : {title}\n"
            f"Keywords   : {kw_str}\n"
            f"File names : {fn_str}\n"
            f"Description: {(description or '')[:400]}\n"
            + (f"Primary data:{snip_block}\n" if snip_block else "")
            + f"\nWhat is this research ABOUT? Choose the best section:\n{options}\n\n"
            "Reply with ONLY valid JSON:\n"
            '{"section": "<letter>", "confidence": "high|medium|low", "reason": "<one sentence>"}'
        )
        raw = self._call_llm(prompt, max_tokens=100)
        parsed = _extract_json(raw)
        new_section = str(parsed.get("section", "N")).strip().upper()
        new_conf    = parsed.get("confidence", "low")

        # If still N, or invalid, keep original N/72
        if new_section == "N" or new_section not in SECTION_MAP:
            return None

        # Run Stage B in the newly chosen section
        division, div_name, confidence, div_method = self._stage_b(
            vec, new_section, title, description, keywords, file_types, snippets, file_names
        )
        log.debug(f"N/72 challenged → {new_section}/{division} ({new_conf})")
        return {
            "section":       new_section,
            "section_name":  SECTION_MAP.get(new_section, ""),
            "division":      division,
            "division_name": div_name,
            "confidence":    confidence,
            "method":        f"llm+n72_challenge+{div_method}",
        }

    # ── E challenge ──────────────────────────────────────────────────────────

    def _challenge_e(self, vec, title, description, keywords, file_types, snippets, file_names=None):
        """
        When section E is chosen, verify it's really about water/waste management
        OPERATIONS — not environmental science (→ N/72) or health (→ R).

        Returns a corrected result dict, or None to keep the original E.
        """
        kw_str = ", ".join((keywords or [])[:12]) or "—"
        fn_str = ", ".join((file_names or [])[:8]) or "—"
        snip_block = ""
        if snippets:
            for fname, text in list(snippets.items())[:2]:
                snip_block += f"\n[{fname}]: {text[:200]}"

        prompt = (
            "The classifier chose section E (Water supply; sewerage, waste management).\n"
            "Section E is ONLY for research about water utility companies, sanitation systems,\n"
            "waste collection/treatment operations, or environmental remediation BUSINESSES.\n\n"
            "Common false positives for E — these should NOT be E:\n"
            "  • Oceanography, hydrology, lake bathymetry, sea-level, coastal science → N/72\n"
            "  • Atmospheric science, climatology, ecology, marine biology → N/72\n"
            "  • Disease mapping, epidemiology, public health, neglected diseases → R/86\n"
            "  • Oil spill health impacts, pollution effects on population health → R/86\n"
            "  • Energy policy, electricity grids, renewable energy → D/35\n\n"
            f"Title      : {title}\n"
            f"Keywords   : {kw_str}\n"
            f"File names : {fn_str}\n"
            f"Description: {(description or '')[:400]}\n"
            + (f"Primary data:{snip_block}\n" if snip_block else "")
            + "\nIs E really correct, or should this be reclassified?\n"
            "Reply with ONLY valid JSON:\n"
            '{"section": "<letter or E to keep>", "confidence": "high|medium|low", "reason": "<one sentence>"}'
        )
        raw = self._call_llm(prompt, max_tokens=120)
        parsed = _extract_json(raw)
        new_section = str(parsed.get("section", "E")).strip().upper()

        if new_section == "E" or new_section not in SECTION_MAP:
            return None

        division, div_name, confidence, div_method = self._stage_b(
            vec, new_section, title, description, keywords, file_types, snippets, file_names
        )
        log.debug(f"E challenged → {new_section}/{division}")
        return {
            "section":       new_section,
            "section_name":  SECTION_MAP.get(new_section, ""),
            "division":      division,
            "division_name": div_name,
            "confidence":    confidence,
            "method":        f"llm+e_challenge+{div_method}",
        }

    # ── LLM call ─────────────────────────────────────────────────────────────

    def _call_llm(self, prompt: str, max_tokens: int = 150) -> str:
        try:
            import urllib.request
            body = json.dumps({
                "model": self.llm_model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.0,
                    "num_predict": max_tokens,
                    "num_ctx": 8192,   # 2× default — fits enriched prompts
                },
            }).encode()
            req = urllib.request.Request(
                f"{self.ollama_url}/api/generate",
                data=body,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=90) as resp:
                return json.loads(resp.read()).get("response", "")
        except Exception as e:
            log.debug(f"LLM call failed: {e}")
            return ""

    def _ping_ollama(self) -> bool:
        try:
            import urllib.request
            with urllib.request.urlopen(f"{self.ollama_url}/api/tags", timeout=3):
                return True
        except Exception:
            return False


# ── Module helpers ─────────────────────────────────────────────────────────────

def _build_query(
    title: str,
    description: str,
    keywords: list[str] | None,
    file_types: list[str] | None,
    snippets: dict[str, str] | None,
) -> str:
    parts: list[str] = []
    if title:
        parts.extend([title] * 3)
    if keywords:
        kw = " ".join(keywords[:20])
        parts.extend([kw] * 2)
    if description:
        parts.append(description[:600])
    if file_types:
        parts.append(" ".join(set(file_types)))
    if snippets:
        for text in list(snippets.values())[:3]:
            parts.append(text[:300])
    return " ".join(parts)


def _extract_json(text: str) -> dict:
    m = re.search(r"\{[^{}]+\}", text, re.S)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    return {}
