"""
RAG-based ISIC Rev. 5 classifier — fully local, no external API.

Pipeline per project:
  1. Embed     project text (title × 3, keywords × 2, description × 1)
               using sentence-transformers (multilingual, runs on CPU/GPU)
  2. Retrieve  top-K most similar ISIC divisions from ChromaDB
  3. Generate  local LLM (Ollama) selects the best division from the top-K candidates

After Phase A the project index is updated so the archive supports
semantic search (find projects similar to a query).

Requirements
------------
  pip install sentence-transformers chromadb ollama

Ollama must be running with a model pulled, e.g.:
  ollama pull llama3.2
  ollama pull mistral
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── Rich descriptions for each ISIC division ─────────────────────────────────
# Written from a research-data perspective: what kind of research/data
# would be classified in each division?
_DIVISION_DESCRIPTIONS: dict[str, str] = {
    "01": "Research on crops, livestock, farming practices, agricultural productivity, "
          "smallholder farmers, food security, irrigation, soil health, pesticides, "
          "fertilizers, rural livelihoods, agri-food systems, land use.",
    "02": "Studies on forestry management, deforestation, timber harvesting, forest "
          "biodiversity, REDD+ programs, carbon sequestration in forests, woodland ecosystems.",
    "03": "Research on fishing communities, fisheries management, aquaculture, "
          "marine and freshwater fish populations, artisanal and commercial fishing.",
    "05": "Studies related to coal mining operations, mining communities, "
          "occupational health in coal mines, coal workers.",
    "06": "Research on oil and gas extraction, petroleum industry workers and communities, "
          "energy resource management, oil spills.",
    "07": "Studies on mineral extraction, metal ore mining, artisanal and small-scale "
          "mining, mining impacts on communities.",
    "08": "Research on quarrying, sand, gravel and stone extraction, mineral resources.",
    "09": "Studies on mining support services, drilling services, oilfield contractors.",
    "10": "Research on food processing industry, food manufacturing, food safety "
          "standards, food supply chains, food workers.",
    "11": "Studies on beverage industry, alcohol production, brewing, drinking patterns.",
    "12": "Research on tobacco industry, smoking products, cigarette manufacturing.",
    "13": "Studies on textile industry, garment factories, weaving, spinning, "
          "textile workers.",
    "14": "Research on clothing and apparel manufacturing, fashion industry, "
          "garment workers, fast fashion.",
    "15": "Studies on leather goods and footwear production, tanneries.",
    "16": "Research on wood product manufacturing, sawmills, timber processing.",
    "17": "Studies on paper industry, pulp mills, paper manufacturing.",
    "18": "Research on printing industry, reproduction of recorded media.",
    "19": "Studies on petroleum refining, fuel production, coke production.",
    "20": "Research on chemical industry, petrochemicals, chemical plants, "
          "chemical workers.",
    "21": "Studies on pharmaceutical manufacturing, drug production, medicine supply, "
          "pharma industry.",
    "22": "Research on rubber and plastics industry, polymer production.",
    "23": "Studies on cement, glass, ceramic, brick manufacturing, "
          "non-metallic mineral products.",
    "24": "Research on steel, iron, aluminum, metals industry, "
          "metallurgy, smelting.",
    "25": "Studies on fabricated metal products, forging, stamping, "
          "metalworking.",
    "26": "Research on electronics, semiconductor, computer hardware "
          "manufacturing, optical instruments.",
    "27": "Studies on electrical equipment manufacturing, batteries, "
          "motors, wires.",
    "28": "Research on industrial machinery manufacturing, machine tools.",
    "29": "Studies on automobile industry, automotive manufacturing, "
          "motor vehicle workers.",
    "30": "Research on aircraft, shipbuilding, aerospace manufacturing, "
          "defence equipment.",
    "31": "Studies on furniture manufacturing.",
    "32": "Research on other manufacturing: toys, musical instruments, "
          "jewelry, sports equipment.",
    "33": "Studies on machinery repair and maintenance, installation services.",
    "35": "Research on electricity generation, power grids, renewable energy "
          "(solar, wind, hydro, nuclear), energy transition, electrification, "
          "energy access, power sector.",
    "36": "Studies on drinking water access, water supply infrastructure, "
          "WASH programmes, water treatment, water quality.",
    "37": "Research on sewage systems, wastewater treatment, sanitation "
          "infrastructure.",
    "38": "Studies on solid waste management, recycling programmes, landfills, "
          "waste collection.",
    "39": "Research on environmental contamination cleanup, site remediation, "
          "industrial pollution.",
    "41": "Studies on housing construction, informal settlements, slums, "
          "residential building, construction workers.",
    "42": "Research on roads, bridges, dams, civil engineering infrastructure, "
          "urban infrastructure, public works.",
    "43": "Studies on specialized construction trades, renovation, "
          "electrical and plumbing installation.",
    "46": "Research on wholesale trade, distribution networks, supply chain "
          "management, trading companies.",
    "47": "Studies on retail sector, consumer purchasing behaviour, "
          "e-commerce, market vendors, informal retail.",
    "49": "Research on road transport, railways, public transit, commuting "
          "behaviour, trucking, transport networks, mobility.",
    "50": "Studies on maritime shipping, ports, sea transport, sailors.",
    "51": "Research on aviation, airlines, airports, air travel, air cargo.",
    "52": "Studies on logistics, warehousing, freight, supply chain operations.",
    "53": "Research on postal services, courier delivery, last-mile delivery.",
    "55": "Studies on hotel industry, accommodation, hospitality workers, "
          "tourism accommodation, Airbnb.",
    "56": "Research on restaurants, food service industry, catering, "
          "street food vendors, hospitality.",
    "58": "Studies on publishing industry, newspapers, book publishing, "
          "print media, journalism.",
    "59": "Research on film, television, music industry, video production, "
          "sound recording, documentary.",
    "60": "Studies on broadcasting, radio, news agencies, media organisations, "
          "streaming platforms.",
    "61": "Research on telecommunications companies, mobile networks, "
          "broadband access, internet connectivity, telecom workers.",
    "62": "Studies on software development, IT services, digital platforms, "
          "information systems, app development, computer science, "
          "digital transformation.",
    "63": "Research on data centers, cloud computing, big data, "
          "artificial intelligence, machine learning, data analytics services.",
    "64": "Studies on banking, credit, loans, microfinance, mobile money, "
          "financial inclusion, fintech, savings groups, debt.",
    "65": "Research on insurance industry, pension funds, health insurance, "
          "life insurance, risk.",
    "66": "Studies on investment, capital markets, stock markets, "
          "asset management, remittances, financial services.",
    "68": "Research on housing market, real estate, land tenure, property rights, "
          "landlords, rental housing, gentrification, eviction.",
    "69": "Studies on legal profession, law firms, courts, justice system, "
          "accounting, auditing, legal aid.",
    "70": "Research on management consultancy, corporate strategy, "
          "head offices.",
    "71": "Studies on engineering firms, architectural practices, "
          "technical testing, quality inspection.",
    "72": "Research methodology studies, scientific research where no specific "
          "subject domain is identifiable. Qualitative and quantitative research "
          "methods, mixed methods, interviews, ethnography, focus groups, "
          "grounded theory, thematic analysis, QDA software projects "
          "(NVivo, MAXQDA, ATLAS.ti), social science research without a "
          "clear sectoral focus.",
    "73": "Studies on advertising, marketing campaigns, market research, "
          "public relations, consumer attitudes, branding.",
    "74": "Research on language, linguistics, translation, communication studies, "
          "language documentation, photography, design.",
    "75": "Studies on veterinary medicine, animal health, livestock diseases, "
          "zoonotic diseases, wildlife health.",
    "77": "Research on leasing and rental services.",
    "78": "Studies on labour market, employment, job search, working conditions, "
          "gig economy, informal employment, precarious work, "
          "occupational health and safety.",
    "79": "Research on tourism industry, travel agencies, tour operators, "
          "ecotourism, cultural tourism.",
    "80": "Studies on security services, crime prevention, policing, "
          "private security, surveillance.",
    "81": "Research on facility management, cleaning services, "
          "landscape management.",
    "82": "Studies on business support services, call centers, office administration.",
    "84": "Research on government, public administration, governance, "
          "public policy, elections, voting behaviour, democracy, corruption, "
          "taxation, law enforcement, military, social security systems, "
          "decentralisation, state capacity.",
    "85": "Studies on education systems, schools, universities, teaching, "
          "learning outcomes, students, teachers, curriculum, literacy, "
          "educational achievement, dropout rates, vocational training, "
          "higher education, pedagogical methods, school climate.",
    "86": "Research on health and medicine: diseases, clinical trials, "
          "patient outcomes, healthcare systems, public health interventions, "
          "epidemiology, mental health (depression, anxiety, PTSD, schizophrenia), "
          "COVID-19, HIV/AIDS, malaria, tuberculosis, maternal and child health, "
          "nutrition, cancer, hospitals, nurses, doctors, primary care, "
          "community health workers, health behaviour.",
    "87": "Studies on nursing homes, care homes, residential care facilities, "
          "elderly care, long-term care.",
    "88": "Research on social work, poverty, social inequality, disability, "
          "refugees, homelessness, child protection, domestic violence, "
          "gender-based violence, social protection programmes, "
          "marginalised communities, asylum seekers, welfare recipients.",
    "90": "Studies on arts, music, dance, theatre, performing arts, "
          "creative industries, cultural production, artists, cultural policy.",
    "91": "Research on libraries, archives, museums, cultural heritage, "
          "digitisation of collections, cultural institutions, "
          "heritage preservation, memory institutions.",
    "92": "Studies on gambling, betting, lotteries, casinos.",
    "93": "Research on sports, recreation, leisure activities, "
          "fitness, physical activity, athletes, sports participation.",
    "94": "Studies on NGOs, non-profit organisations, civil society, "
          "membership associations, trade unions, volunteer organisations, "
          "advocacy groups, charity sector.",
    "95": "Research on repair services, computer repair, appliance maintenance.",
    "96": "Studies on personal services, beauty industry, hairdressers, "
          "funeral services.",
    "97": "Research on domestic workers, household employees, paid domestic work.",
    "98": "Studies on household production, subsistence activities.",
    "99": "Research on international organisations, UN agencies, "
          "diplomatic missions, intergovernmental bodies.",
}

# ── LLM prompts ───────────────────────────────────────────────────────────────

_LLM_SYSTEM = """You are an ISIC Rev. 5 classification expert for research datasets.

RULES:
1. Classify the SUBJECT being studied — not the research method.
   Example: a health survey → R/86 (Human health), not N/72 (Scientific R&D).
2. Use N/72 "Scientific research and development" ONLY when no specific
   subject domain can be identified from the project text.
3. Output ONLY a valid JSON object — no prose, no markdown fences.

Required JSON schema:
{
  "section": "<single letter>",
  "section_name": "<full section name>",
  "division": "<2-digit string, e.g. 86>",
  "division_name": "<full division name>",
  "confidence": "high" | "medium" | "low",
  "tags": ["tag1", "tag2", "tag3", "tag4"],
  "rationale": "<one sentence>"
}"""

_LLM_USER_TMPL = """\
Project title: {title}
Description: {description}
Keywords: {keywords}
File types present: {file_types}

Top {k} ISIC candidates retrieved by semantic similarity (ranked):
{candidates}

Classify this research project. Choose the division whose description best
matches the subject being studied. Return only the JSON object."""


# ── Classifier ────────────────────────────────────────────────────────────────

class RAGClassifier:
    """
    Local RAG classifier:
      embed → retrieve top-K ISIC candidates → local LLM picks the best.

    Falls back to cosine-similarity top-1 if the LLM is unavailable.
    """

    CHROMA_ISIC_COLLECTION   = "isic_divisions"
    CHROMA_PROJECT_COLLECTION = "projects"

    def __init__(
        self,
        embed_model: str = "paraphrase-multilingual-MiniLM-L12-v2",
        llm_model:   str = "llama3.2",
        chroma_dir:  str = ".chroma_db",
        top_k:       int = 5,
    ) -> None:
        self.llm_model = llm_model
        self.top_k     = top_k

        # ── sentence-transformers ─────────────────────────────────────────────
        try:
            from sentence_transformers import SentenceTransformer
            log.info(f"Loading embedding model: {embed_model}")
            self._encoder = SentenceTransformer(embed_model)
            log.info("Embedding model ready")
        except ImportError:
            raise ImportError(
                "sentence-transformers is required.\n"
                "  pip install sentence-transformers"
            )

        # ── ChromaDB ──────────────────────────────────────────────────────────
        try:
            import chromadb
            self._chroma = chromadb.PersistentClient(path=chroma_dir)
        except ImportError:
            raise ImportError(
                "chromadb is required.\n"
                "  pip install chromadb"
            )

        # ── Ollama ────────────────────────────────────────────────────────────
        self._ollama_ok = False
        try:
            import ollama as _ollama
            self._ollama = _ollama
            # Check the server is reachable and the model exists
            models = [m.model for m in _ollama.list().models]
            if any(llm_model in m for m in models):
                self._ollama_ok = True
                log.info(f"Ollama model '{llm_model}' ready")
            else:
                log.warning(
                    f"Ollama model '{llm_model}' not found. "
                    f"Available: {models}. Falling back to cosine-similarity only."
                )
        except ImportError:
            log.warning("ollama package not installed — falling back to cosine-similarity only.")
        except Exception as e:
            log.warning(f"Ollama unavailable ({e}) — falling back to cosine-similarity only.")

        # ── Build ISIC index ──────────────────────────────────────────────────
        self._isic_col = self._build_isic_index()

        # ── Project index (created lazily) ────────────────────────────────────
        self._proj_col = self._chroma.get_or_create_collection(
            name=self.CHROMA_PROJECT_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )

    # ── Index builders ────────────────────────────────────────────────────────

    def _build_isic_index(self):
        """
        Create (or reuse) the ISIC divisions collection.
        Always rebuilds to pick up any vocabulary changes.
        """
        from qdarchive.isic import DIVISION_MAP, SECTION_MAP

        col = self._chroma.get_or_create_collection(
            name=self.CHROMA_ISIC_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )

        if col.count() == len(_DIVISION_DESCRIPTIONS):
            log.info(f"ISIC index already has {col.count()} divisions — reusing")
            return col

        log.info("Building ISIC division index …")
        col.delete(where={"type": "isic"})  # clear stale entries

        ids, docs, metas = [], [], []
        for div_code, desc in _DIVISION_DESCRIPTIONS.items():
            entry = DIVISION_MAP.get(div_code)
            if entry is None:
                continue
            sec_letter, div_name = entry
            ids.append(div_code)
            docs.append(f"{div_name}. {desc}")
            metas.append({
                "type":         "isic",
                "division":     div_code,
                "division_name": div_name,
                "section":      sec_letter,
                "section_name": SECTION_MAP.get(sec_letter, ""),
            })

        embeddings = self._encoder.encode(docs, show_progress_bar=False).tolist()
        col.add(ids=ids, documents=docs, embeddings=embeddings, metadatas=metas)
        log.info(f"ISIC index built: {len(ids)} divisions")
        return col

    # ── Embedding ─────────────────────────────────────────────────────────────

    def _embed(self, text: str) -> list[float]:
        return self._encoder.encode(text, show_progress_bar=False).tolist()

    def _project_text(
        self,
        title: str,
        keywords: str,
        description: str,
        snippets: dict[str, str] | None,
    ) -> str:
        parts = []
        if title:
            parts.append((title + " ") * 3)         # title weight × 3
        if keywords:
            parts.append((keywords + " ") * 2)      # keywords weight × 2
        if description:
            parts.append(description[:600])          # description as-is
        if snippets:
            for text in list(snippets.values())[:2]:
                parts.append(text[:300])             # up to 2 file snippets
        return " ".join(filter(None, parts))

    # ── Retrieval ─────────────────────────────────────────────────────────────

    def _retrieve_candidates(self, query_text: str) -> list[dict]:
        """Return top-K ISIC divisions ranked by cosine similarity."""
        emb = self._embed(query_text)
        results = self._isic_col.query(
            query_embeddings=[emb],
            n_results=self.top_k,
        )
        candidates = []
        for i, meta in enumerate(results["metadatas"][0]):
            candidates.append({
                "rank":          i + 1,
                "division":      meta["division"],
                "division_name": meta["division_name"],
                "section":       meta["section"],
                "section_name":  meta["section_name"],
                "description":   results["documents"][0][i],
                "distance":      results["distances"][0][i],
            })
        return candidates

    # ── LLM generation ────────────────────────────────────────────────────────

    def _ask_llm(
        self,
        title:       str,
        description: str,
        keywords:    str,
        file_types:  str,
        candidates:  list[dict],
    ) -> dict | None:
        if not self._ollama_ok:
            return None

        cand_lines = []
        for c in candidates:
            cand_lines.append(
                f"  {c['rank']}. Section {c['section']}, Division {c['division']}: "
                f"{c['division_name']}\n"
                f"     {c['description'][:200]}"
            )

        user_msg = _LLM_USER_TMPL.format(
            title=title or "(untitled)",
            description=(description or "")[:400],
            keywords=keywords or "",
            file_types=file_types or "",
            k=len(candidates),
            candidates="\n".join(cand_lines),
        )

        try:
            response = self._ollama.chat(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": _LLM_SYSTEM},
                    {"role": "user",   "content": user_msg},
                ],
                options={"temperature": 0.1},
            )
            raw = response["message"]["content"].strip()
            # Strip markdown fences if present
            raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
            raw = re.sub(r"```\s*$",          "", raw, flags=re.MULTILINE)
            parsed = json.loads(raw)
            return parsed
        except Exception as e:
            log.debug(f"LLM call failed: {e}")
            return None

    # ── Result builder ────────────────────────────────────────────────────────

    def _build_result(self, llm_out: dict | None, top_candidate: dict) -> dict:
        """Merge LLM output with top cosine-similarity candidate as fallback."""
        from qdarchive.isic import SECTION_MAP, VALID_SECTIONS, VALID_DIVISIONS, DEFAULT_SECTION, DEFAULT_DIVISION
        from qdarchive.isic import section_for_division, division_name as _div_name

        if llm_out:
            section  = str(llm_out.get("section", "")).strip().upper()
            division = str(llm_out.get("division", "")).strip().zfill(2)
            if section not in VALID_SECTIONS:
                section = top_candidate["section"]
            if division not in VALID_DIVISIONS:
                division = top_candidate["division"]
            # Ensure section matches division
            correct_sec, _ = section_for_division(division)
            if correct_sec:
                section = correct_sec
            confidence = llm_out.get("confidence", "medium")
            if confidence not in ("high", "medium", "low"):
                confidence = "medium"
            tags = llm_out.get("tags", [])
            if not isinstance(tags, list):
                tags = []
            tags = [str(t).lower().strip() for t in tags if t][:8]
        else:
            # Cosine-similarity fallback
            division = top_candidate["division"]
            section  = top_candidate["section"]
            correct_sec, _ = section_for_division(division)
            if correct_sec:
                section = correct_sec
            confidence = "medium" if top_candidate["distance"] < 0.4 else "low"
            tags = []

        return {
            "section":       section,
            "section_name":  SECTION_MAP.get(section, ""),
            "division":      division,
            "division_name": _div_name(division) or "",
            "confidence":    confidence,
            "tags":          tags,
        }

    # ── Public API ────────────────────────────────────────────────────────────

    def classify_project(
        self,
        title:       str,
        description: str,
        keywords:    str,
        file_types:  str,
        snippets:    dict[str, str] | None = None,
    ) -> dict:
        query_text = self._project_text(title, keywords, description, snippets)
        candidates = self._retrieve_candidates(query_text)
        top        = candidates[0]

        llm_out = self._ask_llm(title, description, keywords, file_types, candidates)
        result  = self._build_result(llm_out, top)
        return result

    def classify_file(
        self,
        file_name:         str,
        file_type:         str,
        proj_section:      str,
        proj_division:     str,
        proj_section_name: str,
        proj_division_name: str,
        proj_snippets:     dict[str, str] | None = None,
    ) -> dict:
        """
        Classify a primary data file.
        Uses embedding similarity on the filename; only calls LLM when the
        top cosine match diverges significantly from the parent classification.
        Inherits from the parent project otherwise.
        """
        from qdarchive.isic import DEFAULT_SECTION, DEFAULT_DIVISION

        # Check if the file's own content was fetched
        file_text = file_name or ""
        if proj_snippets and file_name in proj_snippets:
            file_text += " " + proj_snippets[file_name][:300]

        if not file_text.strip():
            return {
                "section": proj_section or DEFAULT_SECTION,
                "section_name": proj_section_name,
                "division": proj_division or DEFAULT_DIVISION,
                "division_name": proj_division_name,
                "confidence": "low",
            }

        candidates = self._retrieve_candidates(file_text)
        top = candidates[0]

        # If the file's top match is close AND different from the parent, ask LLM
        if top["distance"] < 0.35 and top["division"] != proj_division:
            llm_out = self._ask_llm(
                title=file_name, description="", keywords="",
                file_types=file_type, candidates=candidates[:3],
            )
            result = self._build_result(llm_out, top)
            return result

        # Weak signal — inherit from parent project
        if top["distance"] >= 0.45:
            return {
                "section": proj_section or DEFAULT_SECTION,
                "section_name": proj_section_name,
                "division": proj_division or DEFAULT_DIVISION,
                "division_name": proj_division_name,
                "confidence": "low",
            }

        # Moderate signal — return cosine match without LLM
        result = self._build_result(None, top)
        return result

    # ── Project index (for semantic search) ──────────────────────────────────

    def index_project(
        self,
        project_id:    int,
        title:         str,
        keywords:      str,
        description:   str,
        isic_section:  str,
        isic_division: str,
    ) -> None:
        """Add or update a classified project in the project vector store."""
        doc  = f"{title} {keywords} {description[:400]}"
        emb  = self._embed(doc)
        pid  = str(project_id)
        meta = {
            "project_id":    project_id,
            "isic_section":  isic_section or "",
            "isic_division": isic_division or "",
        }
        existing = self._proj_col.get(ids=[pid])
        if existing["ids"]:
            self._proj_col.update(ids=[pid], documents=[doc], embeddings=[emb], metadatas=[meta])
        else:
            self._proj_col.add(ids=[pid], documents=[doc], embeddings=[emb], metadatas=[meta])

    def search_projects(
        self,
        query: str,
        n:     int = 10,
        isic_section: str | None = None,
    ) -> list[dict]:
        """
        Semantic search over indexed projects.
        Optionally filter by ISIC section.
        Returns list of {project_id, isic_section, isic_division, distance}.
        """
        emb   = self._embed(query)
        where = {"isic_section": isic_section} if isic_section else None
        kw    = {"query_embeddings": [emb], "n_results": min(n, self._proj_col.count())}
        if where:
            kw["where"] = where
        results = self._proj_col.query(**kw)
        output  = []
        for meta, dist in zip(results["metadatas"][0], results["distances"][0]):
            output.append({**meta, "distance": dist})
        return output

    @property
    def project_count(self) -> int:
        return self._proj_col.count()
