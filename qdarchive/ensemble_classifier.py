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

log = logging.getLogger(__name__)

TOP_K_SECTIONS  = 3   # section candidates forwarded to LLM in Stage A
TOP_K_DIVISIONS = 5   # division candidates forwarded to LLM in Stage B
LOW_SCORE_THRESHOLD = 0.28  # free-pick mode when embedding is uncertain

# ── Section descriptions (Stage A index) ──────────────────────────────────────
_SECTION_DESCRIPTIONS: dict[str, str] = {
    "A": "Research on agriculture, farming, crops, livestock, animal husbandry, fisheries, aquaculture, forestry, rural livelihoods, food production, agronomy",
    "B": "Research on mining, quarrying, oil and gas extraction, petroleum industry, coal, drilling, mineral resources, methane emissions from fossil fuels",
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
    "N": "Research on professional services, management consulting, legal services, architecture, scientific R&D, advertising, veterinary activities",
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

RULE: Classify by the SUBJECT being studied, NOT the method.
  A qualitative study ABOUT health → R/86 (not N/72 just because it uses qualitative methods)
  A survey ABOUT politics → P/84 (not N/73 just because it involves surveys)
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
    ) -> dict:
        """Return {section, section_name, division, division_name, confidence, method}."""
        text = _build_query(title, description, keywords, file_types, snippets)
        vec  = self._embedder.encode(text, normalize_embeddings=True)

        # Stage A — pick Section
        section, sec_method = self._stage_a(
            vec, title, description, keywords, file_types, snippets
        )

        # Stage B — pick Division within that Section
        division, div_name, confidence, div_method = self._stage_b(
            vec, section, title, description, keywords, file_types, snippets
        )

        method = f"{sec_method}+{div_method}"
        return {
            "section":       section,
            "section_name":  SECTION_MAP.get(section, ""),
            "division":      division,
            "division_name": div_name,
            "confidence":    confidence,
            "method":        method,
        }

    # ── Stage A: Section ──────────────────────────────────────────────────────

    def _stage_a(self, vec, title, description, keywords, file_types, snippets):
        scores = (self._sec_index @ vec).tolist()
        top = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)[: self.top_k_sections]
        candidates = [
            {"letter": self._sections[i][0], "name": self._sections[i][1], "score": round(s, 4)}
            for i, s in top
        ]

        if not self._ollama_ok:
            return candidates[0]["letter"], "embedding"

        section = self._llm_pick_section(
            candidates, title, description, keywords, file_types, snippets
        )
        method = "llm"

        # Validate
        if section not in SECTION_MAP:
            section = candidates[0]["letter"]
            method  = "embedding_fallback"

        return section, method

    def _llm_pick_section(self, candidates, title, description, keywords, file_types, snippets):
        options = "\n".join(
            f"  {c['letter']}. {c['name']}"
            for c in candidates
        )
        kw_str = ", ".join((keywords or [])[:12]) or "—"
        ft_str = ", ".join(sorted(set(file_types or []))) or "—"
        snip_block = ""
        if snippets:
            for fname, text in list(snippets.items())[:2]:
                snip_block += f"\n[{fname}]: {text[:200]}"

        prompt = (
            f"{_FEW_SHOT}\n\n"
            "Now classify this research dataset.\n"
            f"Title      : {title}\n"
            f"Keywords   : {kw_str}\n"
            f"File types : {ft_str}\n"
            f"Description: {(description or '')[:400]}\n"
            + (f"Primary data:{snip_block}\n" if snip_block else "")
            + f"\nChoose the single best ISIC Section from these candidates:\n{options}\n\n"
            "Reply with ONLY valid JSON:\n"
            '{"section": "<letter>", "confidence": "high|medium|low", "reason": "<one sentence>"}'
        )
        raw = self._call_llm(prompt, max_tokens=100)
        parsed = _extract_json(raw)
        return str(parsed.get("section", "")).strip().upper()

    # ── Stage B: Division ─────────────────────────────────────────────────────

    def _stage_b(self, vec, section, title, description, keywords, file_types, snippets):
        # Filter divisions to the chosen section
        section_divs = [(i, sec, div, name)
                        for i, (sec, div, name) in enumerate(self._divisions)
                        if sec == section]

        if not section_divs:
            # Section has no divisions (shouldn't happen) — fall back to global top-1
            scores = (self._div_index @ vec).tolist()
            best_i = max(range(len(scores)), key=lambda x: scores[x])
            _, div, name = self._divisions[best_i]
            return div, name, "low", "embedding_fallback"

        # Score only divisions in this section
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
            candidates, section, title, description, keywords, file_types, snippets, best_score
        )
        return division, DIVISION_MAP.get(division, ("", ""))[1], confidence, method

    def _llm_pick_division(
        self, candidates, section, title, description, keywords, file_types, snippets, best_score
    ):
        options = "\n".join(
            f"  {c['division']}. {c['name']}"
            for c in candidates
        )
        kw_str = ", ".join((keywords or [])[:12]) or "—"
        ft_str = ", ".join(sorted(set(file_types or []))) or "—"
        snip_block = ""
        if snippets:
            for fname, text in list(snippets.items())[:2]:
                snip_block += f"\n[{fname}]: {text[:250]}"

        # If embedding score is very low, allow LLM to pick any division in this section
        if best_score < LOW_SCORE_THRESHOLD:
            all_in_section = [
                f"  {div}. {name}"
                for sec, div, name in self._divisions if sec == section
            ]
            options = "\n".join(all_in_section)
            note = "(embedding was uncertain — all divisions in this section are shown)"
        else:
            note = "(top candidates by semantic similarity)"

        prompt = (
            f"{_FEW_SHOT}\n\n"
            f"The section has been identified as: Section {section} — {SECTION_MAP.get(section, '')}\n\n"
            "Now pick the single best ISIC Division for this research dataset.\n"
            f"Title      : {title}\n"
            f"Keywords   : {kw_str}\n"
            f"File types : {ft_str}\n"
            f"Description: {(description or '')[:500]}\n"
            + (f"Primary data:{snip_block}\n" if snip_block else "")
            + f"\nChoose the best division {note}:\n{options}\n\n"
            "Reply with ONLY valid JSON:\n"
            '{"division": "<2-digit code>", "confidence": "high|medium|low", "reason": "<one sentence>"}'
        )
        raw = self._call_llm(prompt, max_tokens=150)
        parsed = _extract_json(raw)
        div  = str(parsed.get("division", "")).strip().zfill(2)
        conf = parsed.get("confidence", "medium")

        # Validate division belongs to the chosen section
        if div in DIVISION_MAP and DIVISION_MAP[div][0] == section:
            return div, conf, "llm"

        # Accept any valid division in the section
        valid = {d for s, d, _ in self._divisions if s == section}
        if div in valid:
            return div, conf, "llm"

        log.debug(f"LLM division {div} not valid for section {section}; using top-1")
        return candidates[0]["division"], "low", "embedding_fallback"

    # ── LLM call ─────────────────────────────────────────────────────────────

    def _call_llm(self, prompt: str, max_tokens: int = 150) -> str:
        try:
            import urllib.request
            body = json.dumps({
                "model": self.llm_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": max_tokens},
            }).encode()
            req = urllib.request.Request(
                f"{self.ollama_url}/api/generate",
                data=body,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
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
