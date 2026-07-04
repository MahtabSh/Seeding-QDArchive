"""
Zero-shot NLI classifier for ISIC Rev. 5.

Uses MoritzLaurer/deberta-v3-large-zeroshot-v2 (HuggingFace) to classify
research datasets into ISIC sections and divisions.

No Ollama, no API keys — just:
    pip install transformers torch

Stage A: score all 22 ISIC sections simultaneously → pick highest
Stage B: score all divisions within the chosen section → pick highest

The NLI model checks each label against the hypothesis:
    "This research dataset studies {label_description}."
and returns an entailment probability.  The label with the highest
probability wins — no generative output, no hallucination.
"""

from __future__ import annotations

import logging
from typing import Optional

from qdarchive.isic import DIVISIONS, DIVISION_MAP, SECTION_MAP, SECTIONS

log = logging.getLogger(__name__)

DEFAULT_MODEL = "facebook/bart-large-mnli"
HYPOTHESIS    = "This research dataset studies {}."

# ── Rich section descriptions (same vocabulary as ensemble_classifier) ─────────
_SECTION_DESCRIPTIONS: dict[str, str] = {
    "A": "agriculture, farming, crops, livestock, animal husbandry, fisheries, aquaculture, forestry, rural livelihoods, food production, agronomy",
    "B": "extractive industries: mining companies, coal mines, oil and gas firms, petroleum workers, drilling operations, quarrying, mineral extraction, fracking",
    "C": "manufacturing, industrial production, factories, goods production, textiles, chemicals, pharmaceuticals, food processing",
    "D": "electricity, power generation, renewable energy, solar, wind, gas supply, energy grids, energy policy",
    "E": "water utility companies, sanitation systems, sewerage infrastructure, waste management operations, recycling companies, environmental remediation businesses",
    "F": "construction, building, housing development, civil engineering, infrastructure projects, architecture, urban development",
    "G": "wholesale trade, retail, shops, consumer behavior, e-commerce, supply chains, market vendors",
    "H": "transport, logistics, railways, roads, shipping, aviation, trucking, mobility, commuting, freight",
    "I": "hospitality, hotels, restaurants, tourism accommodation, food service, catering",
    "J": "media, publishing, broadcasting, journalism, news, film, television, music, content production, social media platforms",
    "K": "telecommunications, software development, IT, programming, internet, digital technology, cloud computing, data centers",
    "L": "banking, finance, insurance, investment, credit, microfinance, financial markets, pension funds",
    "M": "real estate, housing markets, property, rent, tenants, gentrification, urban housing",
    "N": "scientific research, basic science, geochemistry, volcanology, geology, oceanography, atmospheric science, climatology, marine biology, ecology, condensed matter physics, materials science, interdisciplinary science",
    "O": "administrative services, employment agencies, travel agencies, security services, cleaning, outsourcing, business support",
    "P": "government, public policy, politics, elections, voting, governance, civil service, military, defense, public administration, democracy",
    "Q": "education, schools, universities, teachers, students, learning, pedagogy, literacy, curriculum, educational policy",
    "R": "healthcare, medicine, hospitals, patients, clinical trials, mental health, public health, disease, epidemiology, social work, child protection, welfare",
    "S": "arts, culture, sports, recreation, museums, libraries, archives, cultural heritage, performing arts, leisure",
    "T": "personal services, repair, household activities, NGOs, civil society, trade unions, membership organizations",
    "U": "domestic workers, household employment, subsistence activities, informal household economy",
    "V": "international organizations, UN agencies, diplomatic missions, global governance, intergovernmental bodies",
}

# ── Rich division descriptions ─────────────────────────────────────────────────
_DIVISION_DESCRIPTIONS: dict[str, str] = {
    "01": "crop and animal production, farming, livestock, food production, rural livelihoods, agronomy, horticulture, farmers",
    "02": "forestry, forest management, timber, deforestation, reforestation, woodland ecology, logging",
    "03": "fisheries, aquaculture, marine fishing, fish stocks, coastal fishing communities, seafood",
    "05": "coal mining, miners, mining communities, coal industry",
    "06": "oil and gas extraction, petroleum industry, drilling, offshore platforms, fracking, methane emissions, fossil fuels",
    "07": "metal mining, mineral extraction, mining communities, ore processing, artisanal mining",
    "08": "quarrying, sand extraction, stone mining, non-metallic minerals, construction materials extraction",
    "09": "mining support services, drilling contractors, mine safety",
    "10": "food processing, food manufacturing, food industry workers, food safety",
    "11": "beverage industry, alcohol production, brewing, winemaking, soft drinks manufacturing",
    "12": "tobacco industry, smoking, tobacco farming, cigarette manufacturing, tobacco policy",
    "13": "textile industry, garment workers, fabric production, cotton, fashion supply chains",
    "14": "clothing, apparel manufacturing, fashion industry, garment factories",
    "15": "leather goods, shoe manufacturing, tanneries, leather workers",
    "16": "wood products, timber industry, furniture manufacturing, sawmills",
    "17": "paper and pulp industry, papermaking, paper recycling",
    "18": "printing industry, publishing production, media printing",
    "19": "petroleum refining, fuel production, petrochemical industry",
    "20": "chemical industry, industrial chemicals, hazardous substances, pesticides",
    "21": "pharmaceutical manufacturing, drug production, medicine manufacturing, biopharmaceuticals",
    "22": "rubber, plastics industry, plastic pollution, packaging manufacturing",
    "23": "glass, ceramics, cement, non-metallic mineral products",
    "24": "steel, iron, metals production, foundries, metallurgy, smelting",
    "25": "metal fabrication, machine parts, industrial components, metalworking",
    "26": "electronics, semiconductors, computers, optical instruments, ICT hardware manufacturing",
    "27": "electrical equipment, power systems, batteries, electrical engineering manufacturing",
    "28": "machinery, industrial equipment, engineering manufacturing, factory machines",
    "29": "automotive industry, car manufacturing, vehicle production, automobile workers",
    "30": "aerospace, shipbuilding, rail vehicles, military vehicles, transport equipment manufacturing",
    "31": "furniture manufacturing, interior design industry, home goods production",
    "32": "other manufacturing, sports equipment, toys, medical devices, jewelry",
    "33": "machinery repair, maintenance services, industrial servicing",
    "35": "electricity production, power grids, renewable energy, solar, wind, gas supply, energy policy",
    "36": "water utility companies, drinking water supply, water treatment plants, water utility management",
    "37": "sewerage systems, sanitation infrastructure, wastewater treatment, sewage management companies",
    "38": "waste collection companies, recycling operations, landfills, garbage collection services, waste treatment",
    "39": "environmental remediation companies, contaminated land cleanup, hazardous waste management services",
    "41": "construction industry, building workers, residential housing, real estate development",
    "42": "civil engineering, infrastructure construction, roads, bridges, dams, public works",
    "43": "specialized construction, electrical installation, plumbing, building trades",
    "46": "wholesale trade, supply chains, distribution networks, business-to-business commerce",
    "47": "retail, shops, consumer behavior, shopping, supermarkets, e-commerce, market vendors",
    "49": "road transport, railways, trucking, logistics, commuting, mobility, transport workers",
    "50": "shipping, maritime transport, ferries, ports, seafarers, ocean freight",
    "51": "aviation, airlines, airports, air travel, flight workers, air transport policy",
    "52": "logistics, warehousing, freight handling, supply chain management",
    "53": "postal services, courier delivery, last-mile logistics, postal workers",
    "55": "hotels, tourism accommodation, hospitality workers, lodging, guesthouses",
    "56": "restaurants, food service, hospitality industry, catering, street food vendors",
    "58": "book publishing, journals, newspapers, magazines, academic publishing",
    "59": "film industry, television production, music recording, documentary, media production",
    "60": "broadcasting, radio, television, news agencies, journalism, content distribution",
    "61": "telecommunications, mobile networks, internet access, telecom companies, digital connectivity",
    "62": "software development, IT consulting, digital technology, app development, tech workers, programming",
    "63": "cloud computing, data centers, internet platforms, big data, digital infrastructure",
    "64": "banking, financial services, credit, loans, microfinance, financial inclusion, fintech",
    "65": "insurance, risk management, pension funds, social insurance systems",
    "66": "financial markets, investment, securities trading, financial regulation",
    "68": "real estate, housing markets, property development, urban housing, gentrification, rent, tenants",
    "69": "legal services, law firms, accounting, auditing, justice systems, courts",
    "70": "corporate management, organizational behavior, business strategy, leadership, governance",
    "71": "engineering consultancy, architecture, urban planning, technical testing, design professions",
    "72": "scientific research and development, basic science, R&D, research methodology, interdisciplinary research, scholarly inquiry",
    "73": "marketing, advertising, public relations, market research, consumer behavior surveys, branding",
    "74": "creative professionals, design, photography, translation, scientific consulting",
    "75": "veterinary medicine, animal health, livestock disease, pet care, wildlife health",
    "77": "rental services, car rental, equipment leasing, sharing economy, platform economy",
    "78": "employment, labor markets, job seeking, recruitment agencies, workforce, gig economy",
    "79": "tourism, travel agencies, tour operators, tourist experiences, travel behavior",
    "80": "security services, private policing, surveillance, crime prevention",
    "81": "cleaning services, landscaping, facility management, building services",
    "82": "administrative services, call centers, business process outsourcing, office work",
    "84": "government, public policy, governance, civil service, military, defense, public administration, elections, voting, democracy",
    "85": "education, schools, universities, teachers, students, learning, pedagogy, educational policy, literacy",
    "86": "healthcare, medicine, hospitals, patients, clinical research, mental health, public health, disease, epidemiology, HIV, cancer, reproductive health",
    "87": "nursing homes, residential care, elder care, disability support, care institutions",
    "88": "social work, community services, welfare, child protection, homelessness, family support, poverty",
    "90": "arts, culture, music, theater, dance, creative expression, performing arts, artists",
    "91": "museums, libraries, archives, cultural heritage, memory institutions, oral history, digital humanities",
    "92": "gambling, casinos, betting, lottery, gaming behavior",
    "93": "sports, athletics, recreation, leisure, physical activity, sports sociology, fitness",
    "94": "civil society, NGOs, associations, trade unions, religious organizations, volunteering",
    "95": "repair economy, electronics repair, consumer goods maintenance, circular economy",
    "96": "personal services, hair salons, personal care, beauty industry, wellness",
    "97": "domestic workers, housekeeping, household employment, care workers in private homes",
    "98": "subsistence activities, household production, informal economy",
    "99": "international organizations, UN agencies, diplomatic missions, global governance",
}


class NLIClassifier:
    """
    Zero-shot NLI classifier for ISIC Rev. 5.

    Usage::
        clf = NLIClassifier()
        result = clf.classify(title=..., description=..., keywords=[...])
    """

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        device: Optional[int] = None,
        batch_size: int = 32,
    ):
        try:
            import torch
            from transformers import pipeline as hf_pipeline
        except ImportError:
            raise ImportError(
                "transformers and torch are required:\n"
                "  pip install transformers torch"
            )

        if device is None:
            device = 0 if torch.cuda.is_available() else -1

        log.info(f"Loading NLI model: {model}  (device={'GPU' if device >= 0 else 'CPU'})")
        self._pipe = hf_pipeline(
            "zero-shot-classification",
            model=model,
            device=device,
            batch_size=batch_size,
        )
        self.model_name = model
        log.info("NLI classifier ready.")

        # Pre-build label lists
        self._sec_labels  = [_SECTION_DESCRIPTIONS[s] for s, _ in SECTIONS]
        self._sec_letters = [s for s, _ in SECTIONS]

        self._div_list = list(DIVISIONS)   # [(sec, div, name), ...]
        self._div_labels = [
            _DIVISION_DESCRIPTIONS.get(div, name)
            for _, div, name in self._div_list
        ]

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
        text = _build_query(title, description, keywords, file_types, snippets)

        # Stage A — pick Section
        section, sec_score = self._classify_section(text)

        # Stage B — pick Division within that Section
        division, div_name, div_score = self._classify_division(text, section)

        confidence = (
            "high"   if div_score > 0.50 else
            "medium" if div_score > 0.35 else
            "low"
        )

        return {
            "section":       section,
            "section_name":  SECTION_MAP.get(section, ""),
            "division":      division,
            "division_name": div_name,
            "confidence":    confidence,
            "method":        "nli_zero_shot",
        }

    # ── Stage A ───────────────────────────────────────────────────────────────

    def _classify_section(self, text: str) -> tuple[str, float]:
        out = self._pipe(
            text,
            candidate_labels=self._sec_labels,
            hypothesis_template=HYPOTHESIS,
            multi_label=False,
        )
        best_label = out["labels"][0]
        best_score = out["scores"][0]
        idx = self._sec_labels.index(best_label)
        return self._sec_letters[idx], best_score

    # ── Stage B ───────────────────────────────────────────────────────────────

    def _classify_division(self, text: str, section: str) -> tuple[str, str, float]:
        section_divs = [
            (i, div, name)
            for i, (sec, div, name) in enumerate(self._div_list)
            if sec == section
        ]

        if not section_divs:
            # Fallback — score all divisions
            section_divs = [(i, div, name) for i, (_, div, name) in enumerate(self._div_list)]

        candidate_labels = [self._div_labels[i] for i, _, _ in section_divs]

        out = self._pipe(
            text,
            candidate_labels=candidate_labels,
            hypothesis_template=HYPOTHESIS,
            multi_label=False,
        )
        best_label = out["labels"][0]
        best_score = out["scores"][0]
        idx_in_candidates = candidate_labels.index(best_label)
        _, best_div, best_name = section_divs[idx_in_candidates]
        return best_div, best_name, best_score


# ── Helpers ───────────────────────────────────────────────────────────────────

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
        parts.extend([" ".join(keywords[:20])] * 2)
    if description:
        parts.append(description[:600])
    if file_types:
        parts.append(" ".join(set(file_types)))
    if snippets:
        for text in list(snippets.values())[:3]:
            parts.append(text[:300])
    return " ".join(parts)
