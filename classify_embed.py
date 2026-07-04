#!/usr/bin/env python3
"""
Semantic Embedding Matching classifier for ISIC Rev. 5.

Multi-tier text enrichment
--------------------------
  Tier 1  (53k projects): project title + description (always present)
  Tier 2a (51k projects): file names from `files` table
                          (e.g. "patient_records.csv", "election_2019.tab")
  Tier 2b (33k projects): domain keywords from `keywords` table (local, zero cost)
                          (e.g. "methane emissions, oil and gas, scoping review")
  Tier 2c ( 1k projects): file content snippets from `project_knowledge` table
                          (Zenodo/Dataverse/Drive downloads) + .file_cache/

The combined text gives the embedding model richer subject-matter signal,
especially for projects whose titles are generic ("Dataset for Study XYZ")
but whose keywords or file content reveal the actual research domain.

Steps:
  1. Embed all ~86 ISIC Rev. 5 division descriptions (enriched anchor text).
  2. Build combined Tier1+Tier2 text for every project.
  3. Embed all project texts with all-MiniLM-L6-v2  (~5 min on CPU, faster MPS).
  4. Cosine similarity 53k × 86 → instant.
  5. Top-1 = primary_class,  Top-2 = secondary_class.

Output: classifications_embed table

Usage:
    python3 classify_embed.py --db 23692652-sq26.db
    python3 classify_embed.py --db 23692652-sq26.db --batch-size 256 --stats
    python3 classify_embed.py --db 23692652-sq26.db --file-cache .file_cache
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
DEFAULT_MODEL = "all-MiniLM-L6-v2"
TABLE         = "classifications_embed"

# ── ISIC Rev. 5 divisions with enriched anchor text ────────────────────────────
#
# Each entry: (section_letter, section_name, division_name, enriched_anchor_text)
#
# Enriched anchor text bridges ISIC business language with research/academic
# vocabulary. Section N/72 is deliberately narrow (meta-research only) so
# general scientific datasets are NOT pulled toward N.

ISIC_DIVISIONS: list[tuple[str, str, str, str, str]] = [
    # (div_code, section, section_name, division_name, anchor_text)

    # ── Section A: Agriculture, forestry and fishing ──────────────────────────
    ("01", "A", "Agriculture, forestry and fishing",
     "Crop and animal production, hunting and related service activities",
     "crop production, animal husbandry, livestock, farming, soil, seeds, "
     "plant growth, agriculture, food crops, cattle, poultry, aquaculture, "
     "irrigation, fertilizer, pesticide, agronomics, food security, harvest"),

    ("02", "A", "Agriculture, forestry and fishing",
     "Forestry and logging",
     "forestry, deforestation, forest management, timber, logging, tree species, "
     "woodland, biodiversity, reforestation, carbon sequestration, forest cover, "
     "trees, wood biomass, silviculture, forest inventory, forest ecology"),

    ("03", "A", "Agriculture, forestry and fishing",
     "Fishing and aquaculture",
     "fishing, fisheries, aquaculture, marine species, fish population, "
     "fish stock, seafood, ocean catch, freshwater fish, fish farming, "
     "marine biology, fishing fleet, shrimp, salmon, tuna, cod, overfishing"),

    # ── Section B: Mining and quarrying ──────────────────────────────────────
    ("05", "B", "Mining and quarrying",
     "Mining of coal and lignite",
     "coal mining, lignite, coal deposits, mine safety, coal production, "
     "coal seam, mining industry, underground mining, coal reserves, "
     "coal combustion, fossil fuel extraction"),

    ("06", "B", "Mining and quarrying",
     "Extraction of crude petroleum and natural gas",
     "oil, petroleum, natural gas, crude oil, gas extraction, oil well, "
     "drilling, hydrocarbon, offshore oil, pipeline, oil field, gas reservoir, "
     "energy extraction, fossil fuel, oil production, gas production"),

    ("07", "B", "Mining and quarrying",
     "Mining of metal ores",
     "metal mining, iron ore, copper, gold mine, silver, bauxite, "
     "mineral extraction, ore deposit, lithium, rare earth metals, "
     "mining operations, mineral processing, metal reserves"),

    ("08", "B", "Mining and quarrying",
     "Other mining and quarrying",
     "quarrying, sand extraction, gravel, stone quarry, mineral mining, "
     "phosphate, potash, salt extraction, dimension stone, industrial minerals, "
     "peat extraction, mining"),

    ("09", "B", "Mining and quarrying",
     "Mining support service activities",
     "drilling services, mining support, mine exploration, geophysical survey, "
     "test drilling, mine operation support"),

    # ── Section C: Manufacturing ──────────────────────────────────────────────
    ("10", "C", "Manufacturing",
     "Manufacture of food products",
     "food processing, food manufacturing, food production, dairy, meat processing, "
     "bakery, sugar, oil processing, food factory, nutrition, food safety, "
     "food supply chain, processed foods, cereal, grain milling"),

    ("11", "C", "Manufacturing",
     "Manufacture of beverages",
     "beverage production, beer, wine, spirits, soft drinks, juice, "
     "alcohol manufacturing, brewery, distillery, carbonated drinks, "
     "drinking water production, energy drinks"),

    ("12", "C", "Manufacturing",
     "Manufacture of tobacco products",
     "tobacco manufacturing, cigarette production, tobacco processing, "
     "nicotine products, tobacco industry, smoking products"),

    ("13", "C", "Manufacturing",
     "Manufacture of textiles",
     "textile manufacturing, fabric production, cotton, wool, synthetic fibers, "
     "yarn spinning, weaving, dyeing, knitting, textile industry"),

    ("14", "C", "Manufacturing",
     "Manufacture of wearing apparel",
     "clothing manufacturing, apparel production, garment industry, fashion, "
     "textile clothing, outerwear, underwear, tailoring, sewing, dress production"),

    ("15", "C", "Manufacturing",
     "Manufacture of leather and related products",
     "leather goods, footwear production, leather industry, shoe manufacturing, "
     "tannery, leather products, bags, handbags, leather processing"),

    ("16", "C", "Manufacturing",
     "Manufacture of wood products",
     "wood processing, lumber, plywood, sawmill, furniture wood, "
     "wood manufacturing, timber products, chipboard, wooden panels, "
     "wooden flooring, cork products"),

    ("17", "C", "Manufacturing",
     "Manufacture of paper and paper products",
     "paper manufacturing, pulp and paper, cardboard, packaging paper, "
     "paper mill, newsprint, tissue paper, paper industry"),

    ("18", "C", "Manufacturing",
     "Printing and reproduction of recorded media",
     "printing industry, book printing, newspaper printing, media reproduction, "
     "CD production, DVD duplication, digital printing, publishing production"),

    ("19", "C", "Manufacturing",
     "Manufacture of coke and refined petroleum products",
     "petroleum refining, oil refinery, coke production, fuel manufacturing, "
     "petrochemicals, gasoline, diesel, kerosene, refinery operations"),

    ("20", "C", "Manufacturing",
     "Manufacture of chemicals and chemical products",
     "chemical manufacturing, industrial chemicals, chemical production, "
     "fertilizer production, pesticide manufacturing, paints, adhesives, "
     "cleaning products, chemical industry, polymer synthesis, dyes"),

    ("21", "C", "Manufacturing",
     "Manufacture of basic pharmaceutical products and pharmaceutical preparations",
     "pharmaceutical manufacturing, drug production, medicine manufacturing, "
     "pharmaceuticals, drug synthesis, vaccine production, generic drugs, "
     "active pharmaceutical ingredients, medicinal chemistry, biotech"),

    ("22", "C", "Manufacturing",
     "Manufacture of rubber and plastics products",
     "rubber manufacturing, plastics production, polymer products, "
     "plastic packaging, tires, rubber goods, plastic industry, "
     "polyethylene, PVC, synthetic rubber"),

    ("23", "C", "Manufacturing",
     "Manufacture of other non-metallic mineral products",
     "glass manufacturing, ceramics, cement production, concrete products, "
     "brick manufacturing, building materials, clay products, mineral products"),

    ("24", "C", "Manufacturing",
     "Manufacture of basic metals",
     "metal production, steel manufacturing, aluminum smelting, "
     "iron and steel, metal alloys, copper production, metallurgy, "
     "foundry, cast iron, metal refining"),

    ("25", "C", "Manufacturing",
     "Manufacture of fabricated metal products",
     "metal fabrication, structural metal, tools manufacturing, "
     "metal containers, metal forging, metal stamping, hardware production, "
     "cutlery, weapons manufacturing"),

    ("26", "C", "Manufacturing",
     "Manufacture of computer, electronic and optical products",
     "electronics manufacturing, semiconductor production, computer hardware, "
     "electronic components, optical instruments, medical devices, "
     "integrated circuits, smartphones, cameras, electronic equipment"),

    ("27", "C", "Manufacturing",
     "Manufacture of electrical equipment",
     "electrical equipment manufacturing, motors, generators, batteries, "
     "wiring devices, electrical installation equipment, power equipment, "
     "electric appliances"),

    ("28", "C", "Manufacturing",
     "Manufacture of machinery and equipment",
     "industrial machinery, machine tools, agricultural machinery, "
     "manufacturing equipment, engines, turbines, pumps, mechanical equipment, "
     "industrial robots"),

    ("29", "C", "Manufacturing",
     "Manufacture of motor vehicles, trailers and semi-trailers",
     "automotive manufacturing, car production, truck manufacturing, "
     "vehicle assembly, auto industry, electric vehicles, car parts, "
     "automobile production, trailers"),

    ("30", "C", "Manufacturing",
     "Manufacture of other transport equipment",
     "ship building, aircraft manufacturing, railway equipment, "
     "aerospace manufacturing, boat construction, spacecraft, "
     "transport vehicle production"),

    ("31", "C", "Manufacturing",
     "Manufacture of furniture",
     "furniture manufacturing, office furniture, home furniture, "
     "wooden furniture, mattresses, furniture industry"),

    ("32", "C", "Manufacturing",
     "Other manufacturing",
     "jewelry manufacturing, musical instruments, toys, games, "
     "sports goods manufacturing, medical supplies, pens, pencils, "
     "brooms, brushes, other goods manufacturing"),

    ("33", "C", "Manufacturing",
     "Repair and installation of machinery and equipment",
     "machinery repair, equipment maintenance, industrial repair, "
     "installation services, machine maintenance, technical repair"),

    # ── Section D: Electricity, gas, steam and air conditioning supply ────────
    ("35", "D", "Electricity, gas, steam and air conditioning supply",
     "Electricity, gas, steam and air conditioning supply",
     "electricity generation, power grid, energy supply, renewable energy, "
     "solar power, wind power, electric utility, power plants, natural gas "
     "distribution, steam energy, district heating, nuclear power, "
     "energy consumption, electricity demand, grid management"),

    # ── Section E: Water supply; sewerage; waste management ───────────────────
    ("36", "E", "Water supply; sewerage, waste management and remediation activities",
     "Water collection, treatment and supply",
     "drinking water, water treatment, water supply, water quality, "
     "wastewater treatment, water infrastructure, groundwater, surface water, "
     "water distribution, water sanitation, water resources, hydrology"),

    ("37", "E", "Water supply; sewerage, waste management and remediation activities",
     "Sewerage",
     "sewage treatment, wastewater management, sewerage systems, "
     "sanitation, sewage disposal, effluent treatment, sewage networks"),

    ("38", "E", "Water supply; sewerage, waste management and remediation activities",
     "Waste collection, treatment and disposal, and recovery activities",
     "waste management, recycling, solid waste, landfill, garbage collection, "
     "waste disposal, composting, waste sorting, household waste, "
     "municipal solid waste, electronic waste, hazardous waste"),

    ("39", "E", "Water supply; sewerage, waste management and remediation activities",
     "Remediation and other waste management service activities",
     "soil remediation, contamination cleanup, environmental remediation, "
     "polluted land cleanup, hazardous site remediation, "
     "environmental restoration, toxic waste cleanup"),

    # ── Section F: Construction ───────────────────────────────────────────────
    ("41", "F", "Construction",
     "Construction of residential and non-residential buildings",
     "building construction, residential construction, housing development, "
     "commercial buildings, building projects, construction sites, "
     "urban development, housing, real estate construction"),

    ("42", "F", "Construction",
     "Civil engineering",
     "infrastructure construction, road building, bridges, tunnels, "
     "highway construction, railway construction, water infrastructure, "
     "civil works, dam construction, port construction"),

    ("43", "F", "Construction",
     "Specialized construction activities",
     "electrical installation, plumbing, roofing, painting, construction "
     "finishing, concrete work, foundation construction, demolition, "
     "site preparation, specialized trades"),

    # ── Section G: Wholesale and retail trade ─────────────────────────────────
    ("45", "G", "Wholesale and retail trade",
     "Sale of motor vehicles and motorcycles",
     "car dealerships, vehicle sales, automotive retail, motorcycle sales, "
     "car market, auto sales, vehicle trade"),

    ("46", "G", "Wholesale and retail trade",
     "Wholesale trade",
     "wholesale markets, bulk trade, wholesale distribution, supply chain, "
     "trading companies, commodity trading, wholesale pricing, "
     "B2B trade, wholesale food, wholesale goods"),

    ("47", "G", "Wholesale and retail trade",
     "Retail trade",
     "retail stores, consumer goods, shopping, e-commerce, supermarkets, "
     "retail sales, retail market, consumer retail, online shopping, "
     "store performance, retail industry, consumer behavior, "
     "purchasing patterns, retail analytics"),

    # ── Section H: Transportation and storage ─────────────────────────────────
    ("49", "H", "Transportation and storage",
     "Land transport and transport via pipelines",
     "road transport, rail transport, trucking, pipelines, freight transport, "
     "public transit, bus systems, urban mobility, traffic, "
     "commuting, road congestion, train travel, logistics"),

    ("50", "H", "Transportation and storage",
     "Water transport",
     "maritime transport, shipping lanes, ocean freight, cargo ships, "
     "port operations, naval routes, maritime logistics, container ships"),

    ("51", "H", "Transportation and storage",
     "Air transport",
     "aviation, airline industry, air freight, flight routes, airports, "
     "passenger flights, cargo aviation, air traffic, aircraft operations"),

    ("52", "H", "Transportation and storage",
     "Warehousing and support activities for transportation",
     "warehousing, storage facilities, logistics hubs, cargo handling, "
     "freight terminals, distribution centers, supply chain management"),

    ("53", "H", "Transportation and storage",
     "Postal and courier activities",
     "postal services, mail delivery, courier services, parcel delivery, "
     "postal network, package tracking, last-mile delivery"),

    # ── Section I: Accommodation and food service activities ──────────────────
    ("55", "I", "Accommodation and food service activities",
     "Accommodation",
     "hotels, tourism accommodation, hospitality, lodging, guesthouses, "
     "hotel industry, bed and breakfast, holiday rentals, "
     "accommodation services, tourism, travel, hospitality"),

    ("56", "I", "Accommodation and food service activities",
     "Food and beverage service activities",
     "restaurants, food service, catering, hospitality, dining, "
     "fast food, cafeteria, bar, pub, eating habits, food consumption, "
     "food industry, culinary, meal delivery, food culture"),

    # ── Section J: Publishing, broadcasting, and content production ────────────
    ("58", "J", "Publishing, broadcasting, and content production and distribution activities",
     "Publishing activities",
     "book publishing, newspaper publishing, magazine publishing, "
     "digital publishing, academic journals, media publishing, "
     "content creation, editorial, publishing industry, periodicals"),

    ("59", "J", "Publishing, broadcasting, and content production and distribution activities",
     "Motion picture, video and television programme production",
     "film production, television programs, video content, movies, "
     "documentary films, streaming content, media production, "
     "entertainment industry, screenwriting, film industry"),

    ("60", "J", "Publishing, broadcasting, and content production and distribution activities",
     "Programming, broadcasting, news agency and other content distribution activities",
     "radio broadcasting, television broadcasting, news media, "
     "news agencies, media broadcasting, media channels, content distribution, "
     "journalism, news reporting, mass media"),

    # ── Section K: Telecommunications, computer programming, IT ──────────────
    ("61", "K", "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities",
     "Telecommunications",
     "mobile telecommunications, internet service, broadband, "
     "wireless networks, telephone services, satellite communication, "
     "network infrastructure, 5G, fiber optic, telecom industry"),

    ("62", "K", "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities",
     "Computer programming, consultancy and related activities",
     "software development, computer programming, IT consulting, "
     "web development, app development, software engineering, "
     "artificial intelligence, machine learning, algorithms, "
     "data science, programming languages, code, software systems"),

    ("63", "K", "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities",
     "Computing infrastructure, data processing, hosting, and other information service activities",
     "cloud computing, data centers, web hosting, big data, "
     "data processing, information services, search engines, "
     "data management, platform services, computing infrastructure"),

    # ── Section L: Financial and insurance activities ─────────────────────────
    ("64", "L", "Financial and insurance activities",
     "Financial service activities, except insurance and pension funding",
     "banking, financial markets, loans, credit, investment, stock market, "
     "financial services, monetary policy, interest rates, "
     "microfinance, bank accounts, capital markets, financial inclusion"),

    ("65", "L", "Financial and insurance activities",
     "Insurance, reinsurance and pension funding",
     "insurance, life insurance, health insurance, pension funds, "
     "risk assessment, actuarial science, insurance premiums, "
     "financial risk, social security, retirement savings"),

    ("66", "L", "Financial and insurance activities",
     "Activities auxiliary to financial service and insurance activities",
     "financial intermediation, investment advisory, fund management, "
     "financial brokerage, stock exchanges, financial regulation"),

    # ── Section M: Real estate activities ────────────────────────────────────
    ("68", "M", "Real estate activities",
     "Real estate activities",
     "real estate market, housing prices, property values, land use, "
     "housing market, rental market, property sales, urban land, "
     "commercial real estate, residential property, housing affordability, "
     "gentrification, property development, real estate investment"),

    # ── Section N: Professional, scientific and technical activities ──────────
    # NOTE: Division 72 anchor is deliberately narrow — only meta-research
    # (study OF science, R&D management) — NOT general scientific datasets.
    ("69", "N", "Professional, scientific and technical activities",
     "Legal and accounting activities",
     "legal services, law, courts, justice system, accounting, auditing, "
     "taxation, legal profession, law firms, legal aid, judicial system, "
     "financial accounting, bookkeeping, tax compliance"),

    ("70", "N", "Professional, scientific and technical activities",
     "Activities of head offices; management consultancy activities",
     "management consulting, corporate strategy, business consulting, "
     "organizational management, head office activities, strategic planning, "
     "business management, corporate governance"),

    ("71", "N", "Professional, scientific and technical activities",
     "Architectural and engineering activities; technical testing and analysis",
     "engineering design, architectural design, technical consulting, "
     "structural engineering, civil engineering consultancy, "
     "geotechnical survey, environmental assessment, building inspection, "
     "technical testing, quality control, calibration"),

    ("72", "N", "Professional, scientific and technical activities",
     "Scientific research and development",
     "meta-research, science of science, research evaluation, bibliometrics, "
     "scientometrics, research funding policy, R&D management, "
     "research productivity measurement, science policy, innovation systems, "
     "research collaboration networks, knowledge production, "
     "academic publishing metrics, citation analysis"),

    ("73", "N", "Professional, scientific and technical activities",
     "Activities of advertising, market research and public opinion polling",
     "advertising, marketing, market research, consumer surveys, "
     "public opinion polls, brand management, digital marketing, "
     "advertising campaigns, media planning, public relations"),

    ("74", "N", "Professional, scientific and technical activities",
     "Other professional, scientific and technical activities",
     "photography, translation services, design activities, "
     "scientific and technical activities, professional services"),

    ("75", "N", "Professional, scientific and technical activities",
     "Veterinary activities",
     "veterinary medicine, animal health, livestock disease, "
     "pet health, animal treatment, veterinary care, "
     "animal disease control, zoonotic diseases, wildlife health"),

    # ── Section O: Administrative and support service activities ─────────────
    ("77", "O", "Administrative and support service activities",
     "Rental and leasing activities",
     "equipment rental, vehicle leasing, property leasing, "
     "rental services, leasing industry, car rental"),

    ("78", "O", "Administrative and support service activities",
     "Employment activities",
     "employment agency, job market, labor market, recruitment, "
     "staffing, workforce management, employment services, "
     "human resources, labor, job placement"),

    ("79", "O", "Administrative and support service activities",
     "Travel agency, tour operator and other reservation services",
     "tourism, travel agencies, tour operators, travel booking, "
     "travel industry, holiday packages, travel reservations"),

    ("80", "O", "Administrative and support service activities",
     "Investigation and security activities",
     "private security, investigation services, security guards, "
     "surveillance, security industry, private investigation"),

    ("81", "O", "Administrative and support service activities",
     "Services to buildings and landscape activities",
     "building cleaning, facility management, landscape gardening, "
     "pest control, janitorial services, building maintenance"),

    ("82", "O", "Administrative and support service activities",
     "Office administrative, office support and other business support activities",
     "office administration, business process outsourcing, call centers, "
     "administrative services, back-office operations"),

    # ── Section P: Public administration and defence ──────────────────────────
    ("84", "P", "Public administration and defence; compulsory social security",
     "Public administration and defence; compulsory social security",
     "government policy, public sector, governance, elections, democracy, "
     "public expenditure, taxation, military, national defense, "
     "social security, welfare policy, public administration, "
     "political science, government performance, regulatory policy, "
     "civil service, public health policy, urban planning policy"),

    # ── Section Q: Education ──────────────────────────────────────────────────
    ("85", "Q", "Education",
     "Education",
     "schools, universities, higher education, student performance, "
     "educational outcomes, learning, teaching, curriculum, "
     "academic achievement, literacy, education policy, "
     "STEM education, educational attainment, school enrollment, "
     "teacher training, online learning, educational data"),

    # ── Section R: Human health and social work activities ────────────────────
    ("86", "R", "Human health and social work activities",
     "Human health activities",
     "public health, clinical medicine, patient outcomes, hospital, "
     "disease, epidemiology, treatment, health outcomes, "
     "mortality, morbidity, health care, medical research, "
     "drugs, therapeutics, surgery, clinical trials, health surveys, "
     "infectious disease, chronic disease, mental health, nutrition health"),

    ("87", "R", "Human health and social work activities",
     "Residential care activities",
     "nursing homes, elderly care, residential care, long-term care, "
     "disability care, care facilities, social care homes"),

    ("88", "R", "Human health and social work activities",
     "Social work activities without accommodation",
     "social work, social services, community support, welfare, "
     "poverty, social vulnerability, marginalized populations, "
     "child welfare, family support, social assistance, community development"),

    # ── Section S: Arts, sports and recreation ────────────────────────────────
    ("90", "S", "Arts, sports and recreation",
     "Arts creation and performing arts activities",
     "arts, culture, music, literature, visual arts, theater, dance, "
     "creative industries, cultural heritage, art history, "
     "artistic expression, poetry, museums, galleries"),

    ("91", "S", "Arts, sports and recreation",
     "Library, archives, museum and other cultural activities",
     "libraries, archives, museums, cultural institutions, "
     "digital libraries, heritage preservation, cultural collections, "
     "archival data, museum collections, cultural heritage sites"),

    ("93", "S", "Arts, sports and recreation",
     "Sports activities and amusement and recreation activities",
     "sports, athletics, physical activity, exercise, recreational activities, "
     "sports performance, athlete data, team sports, "
     "leisure activities, sport events, physical fitness"),

    # ── Section T: Other service activities ───────────────────────────────────
    ("94", "T", "Other service activities",
     "Activities of membership organizations",
     "trade unions, professional associations, religious organizations, "
     "NGOs, civil society, membership groups, community organizations, "
     "political parties, advocacy groups"),

    ("96", "T", "Other service activities",
     "Personal service activities",
     "hairdressing, beauty services, personal care, laundry services, "
     "wellness services, personal services"),

    # ── Section U: Activities of households ───────────────────────────────────
    ("97", "U", "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use",
     "Activities of households as employers of domestic personnel",
     "domestic workers, household employment, home workers, domestic services, "
     "household help, domestic labor, family labor"),

    ("98", "U", "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use",
     "Undifferentiated goods- and services-producing activities of private households for own use",
     "household production, subsistence activities, home production, "
     "self-sufficient households, informal economy household activities"),

    # ── Section V: Activities of extraterritorial organizations ──────────────
    ("99", "V", "Activities of extraterritorial organizations and bodies",
     "Activities of extraterritorial organizations and bodies",
     "international organizations, United Nations, World Bank, WHO, UNESCO, "
     "IMF, diplomatic missions, embassies, consulates, "
     "international bodies, supranational organizations, "
     "foreign aid, development assistance, international treaties"),
]

_CREATE_SQL = f"""CREATE TABLE IF NOT EXISTS {TABLE} (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id           INTEGER NOT NULL UNIQUE,
    section              TEXT,
    section_name         TEXT,
    division             TEXT,
    division_name        TEXT,
    similarity_score     REAL,
    confidence           TEXT,
    section_top2         TEXT,
    section_name_top2    TEXT,
    division_top2        TEXT,
    division_name_top2   TEXT,
    similarity_score_top2 REAL,
    tier                 TEXT DEFAULT 'metadata',
    classified_at        TEXT DEFAULT (datetime('now'))
)"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_SQL)
    conn.commit()


# ── Text preparation ───────────────────────────────────────────────────────────

def make_text(
    title: str | None,
    description: str | None,
    file_names: list[str] | None = None,
    keywords: str | None = None,
    file_text: str | None = None,
) -> str:
    """Build combined Tier 1 + Tier 2 text for a project."""
    parts = []
    # Tier 1: title + description
    if title:
        parts.append(title.strip())
    if description:
        parts.append(description.strip()[:600])
    # Tier 2a: file names (strip extensions, join with spaces — acts as keywords)
    if file_names:
        names_clean = " ".join(
            Path(n).stem.replace("_", " ").replace("-", " ")
            for n in file_names[:8]
        )
        if names_clean.strip():
            parts.append(names_clean)
    # Tier 2b: domain keywords from keywords table
    if keywords and keywords.strip():
        parts.append(keywords.strip()[:300])
    # Tier 2c: cached file content / project_knowledge snippets
    if file_text:
        parts.append(file_text.strip()[:500])
    return " ".join(parts) or "untitled"


def confidence_from_similarity(score: float) -> str:
    if score >= 0.55:
        return "very_high"
    if score >= 0.45:
        return "high"
    if score >= 0.35:
        return "medium"
    return "low"


# ── Tier 2 data loading ────────────────────────────────────────────────────────

def load_tier2(
    conn: sqlite3.Connection,
    cache_dir: Path,
) -> tuple[dict[int, list[str]], dict[int, str], dict[int, str]]:
    """
    Returns:
        file_names   : {project_id: [file_name, ...]}  — Tier 2a, files table
        kw_texts     : {project_id: keyword_string}    — Tier 2b, keywords table
        file_texts   : {project_id: combined_text}     — Tier 2c, project_knowledge + file cache
    """
    import json

    # Tier 2a: file names from files table (top 8 per project, SUCCEEDED only)
    rows = conn.execute("""
        SELECT project_id, file_name
        FROM files
        WHERE status = 'SUCCEEDED'
        ORDER BY project_id, file_size_bytes DESC
    """).fetchall()

    file_names: dict[int, list[str]] = {}
    for pid, fname in rows:
        lst = file_names.setdefault(pid, [])
        if len(lst) < 8:
            lst.append(fname)

    log.info(f"Tier 2a: file names loaded for {len(file_names):,} projects")

    # Tier 2b: domain keywords from keywords table (local, zero network cost)
    kw_texts: dict[int, str] = {}
    kw_rows = conn.execute("SELECT project_id, keyword FROM keywords").fetchall()
    for pid, kw in kw_rows:
        if kw and kw.strip():
            existing = kw_texts.get(pid, "")
            kw_texts[pid] = (existing + " " + kw.strip()).strip() if existing else kw.strip()
    log.info(f"Tier 2b: keywords loaded for {len(kw_texts):,} projects")

    # Tier 2c: file snippets + subjects from project_knowledge table
    file_texts: dict[int, str] = {}

    pk_rows = conn.execute("""
        SELECT project_id, keywords, file_snippets, source_subjects, source_description
        FROM project_knowledge
        WHERE project_id IS NOT NULL
    """).fetchall()

    for pid, kw, fs_json, subj_json, src_desc in pk_rows:
        parts = []
        if kw and kw.strip():
            parts.append(kw.strip())
        if subj_json:
            try:
                subjects = json.loads(subj_json)
                if subjects:
                    parts.append(" ".join(str(s) for s in subjects if s))
            except Exception:
                pass
        if fs_json:
            try:
                snippets = json.loads(fs_json)
                snippet_text = " ".join(v for v in snippets.values() if v and v.strip())
                if snippet_text.strip():
                    parts.append(snippet_text)
            except Exception:
                pass
        if src_desc and src_desc.strip():
            parts.append(src_desc.strip()[:300])
        combined = " ".join(parts)[:1000]
        if combined.strip():
            file_texts[pid] = combined

    pk_count = len(file_texts)

    # Also read .file_cache/project_*.json for any projects not already covered
    if cache_dir.exists():
        for p in cache_dir.glob("project_*.json"):
            try:
                pid = int(p.stem.replace("project_", ""))
                if pid in file_texts:
                    continue
                data = json.loads(p.read_text(encoding="utf-8"))
                if data:
                    combined = " ".join(data.values())
                    if combined.strip():
                        file_texts[pid] = combined[:1000]
            except Exception:
                pass

    cache_extra = len(file_texts) - pk_count
    log.info(
        f"Tier 2c: file content loaded for {len(file_texts):,} projects "
        f"({pk_count:,} from project_knowledge, {cache_extra:,} from file cache)"
    )
    return file_names, kw_texts, file_texts


# ── Main classification logic ──────────────────────────────────────────────────

def run(
    conn: sqlite3.Connection,
    model_name: str,
    batch_size: int,
    cache_dir: Path,
) -> None:
    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except ImportError:
        log.error("sentence-transformers not installed — pip install sentence-transformers")
        sys.exit(1)

    log.info(f"Loading embedding model: {model_name}")
    t0 = time.time()
    model = SentenceTransformer(model_name)
    log.info(f"Model loaded in {time.time()-t0:.1f}s")

    # Step 1: embed division anchors
    div_codes    = [d[0] for d in ISIC_DIVISIONS]
    div_secs     = [d[1] for d in ISIC_DIVISIONS]
    div_secnames = [d[2] for d in ISIC_DIVISIONS]
    div_names    = [d[3] for d in ISIC_DIVISIONS]
    div_anchors  = [f"{d[3]}. {d[4]}" for d in ISIC_DIVISIONS]

    log.info(f"Embedding {len(div_anchors)} ISIC division anchors…")
    div_embeddings = model.encode(
        div_anchors,
        batch_size=64,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    log.info(f"  Done in {time.time()-t0:.1f}s")

    # Step 2: load Tier 2 data
    tier2_names, tier2_kws, tier2_texts = load_tier2(conn, cache_dir)

    # Step 3: load all projects and build combined texts
    rows = conn.execute("SELECT id, title, description FROM projects").fetchall()
    pids  = [r[0] for r in rows]
    texts = [
        make_text(
            r[1], r[2],
            file_names=tier2_names.get(r[0]),
            keywords=tier2_kws.get(r[0]),
            file_text=tier2_texts.get(r[0]),
        )
        for r in rows
    ]
    total = len(texts)
    has_2a  = sum(1 for pid in pids if pid in tier2_names)
    has_2b  = sum(1 for pid in pids if pid in tier2_kws)
    has_2c  = sum(1 for pid in pids if pid in tier2_texts)
    tier1_only = sum(1 for pid in pids if pid not in tier2_names and pid not in tier2_kws and pid not in tier2_texts)
    log.info(
        f"Text coverage: Tier1-only={tier1_only:,}  "
        f"+file_names={has_2a:,}  +keywords={has_2b:,}  +file_content={has_2c:,}"
    )
    log.info(f"Embedding {total:,} projects (batch={batch_size})…")

    # Step 4: embed projects in batches and compute cosine similarity
    import numpy as np

    results: list[dict] = []
    t_embed = time.time()
    num_batches = math.ceil(total / batch_size)

    for b in range(num_batches):
        start = b * batch_size
        end   = min(start + batch_size, total)
        batch_texts = texts[start:end]
        batch_pids  = pids[start:end]

        proj_embs = model.encode(
            batch_texts,
            batch_size=batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )

        # Cosine similarity: dot product (embeddings are already normalized)
        sims = proj_embs @ div_embeddings.T  # shape (batch, n_divisions)

        for i, pid in enumerate(batch_pids):
            top2_idx  = sims[i].argsort()[-2:][::-1]
            idx1, idx2 = int(top2_idx[0]), int(top2_idx[1])

            results.append({
                "project_id":         pid,
                "section":            div_secs[idx1],
                "section_name":       div_secnames[idx1],
                "division":           div_codes[idx1],
                "division_name":      div_names[idx1],
                "similarity_score":   float(sims[i][idx1]),
                "confidence":         confidence_from_similarity(float(sims[i][idx1])),
                "section_top2":       div_secs[idx2],
                "section_name_top2":  div_secnames[idx2],
                "division_top2":      div_codes[idx2],
                "division_name_top2": div_names[idx2],
                "similarity_score_top2": float(sims[i][idx2]),
            })

        if (b + 1) % 20 == 0 or (b + 1) == num_batches:
            elapsed = time.time() - t_embed
            done    = end
            rate    = done / elapsed if elapsed > 0 else 1
            eta     = (total - done) / rate if rate > 0 else 0
            log.info(
                f"  {done:>6,}/{total:,}  "
                f"elapsed={elapsed:.0f}s  ETA={eta:.0f}s  "
                f"({rate:.0f} proj/s)"
            )

    log.info(f"Embedding complete in {time.time()-t_embed:.1f}s")

    # Step 4: save
    conn.execute(f"DELETE FROM {TABLE}")
    conn.executemany(f"""
        INSERT INTO {TABLE}
            (project_id, section, section_name, division, division_name,
             similarity_score, confidence,
             section_top2, section_name_top2, division_top2, division_name_top2,
             similarity_score_top2)
        VALUES (:project_id, :section, :section_name, :division, :division_name,
                :similarity_score, :confidence,
                :section_top2, :section_name_top2, :division_top2, :division_name_top2,
                :similarity_score_top2)
    """, results)
    conn.commit()
    log.info(f"Saved {len(results):,} rows to {TABLE}")


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

    print("\nTop-10 divisions:")
    print(f"  {'Div':<5} {'Name':<50} {'N':>7}  {'%':>5}")
    print(f"  {'─'*68}")
    for div, name, n, pct in conn.execute(f"""
        SELECT division, division_name, COUNT(*), ROUND(COUNT(*)*100.0/{total},1)
        FROM {TABLE} GROUP BY division ORDER BY COUNT(*) DESC LIMIT 10
    """):
        print(f"  {div:<5} {(name or '')[:50]:<50} {n:>7,}  {pct:>5}%")

    avg_sim = conn.execute(f"SELECT AVG(similarity_score) FROM {TABLE}").fetchone()[0]
    print(f"\nAverage similarity score: {avg_sim:.4f}")
    print()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Semantic embedding matching against ISIC Rev. 5 divisions (Tier 1+2)."
    )
    ap.add_argument("--db",         default=DEFAULT_DB)
    ap.add_argument("--model",      default=DEFAULT_MODEL,
                    help=f"Sentence-transformer model (default: {DEFAULT_MODEL})")
    ap.add_argument("--batch-size", type=int, default=256,
                    help="Embedding batch size (default: 256)")
    ap.add_argument("--file-cache", default=".file_cache",
                    help="Directory with cached file snippets project_*.json (default: .file_cache)")
    ap.add_argument("--stats",      action="store_true",
                    help="Show stats from existing table (no re-embed)")
    args = ap.parse_args()

    db_path    = Path(args.db)
    cache_dir  = Path(args.file_cache)

    if not db_path.exists():
        log.error(f"DB not found: {db_path}"); sys.exit(1)

    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    if args.stats:
        show_stats(conn)
    else:
        run(conn, args.model, args.batch_size, cache_dir)
        show_stats(conn)

    conn.close()


if __name__ == "__main__":
    main()
