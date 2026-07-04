"""
Richer ISIC Rev. 5 division descriptions for LLM prompting.

Each entry has:
  description  — what this division covers
  include      — signals that strongly indicate this division
  exclude      — common confusions and where they really belong
  examples     — representative research dataset titles

Used by EnsembleClassifier to build more discriminative LLM prompts.
"""

DIVISION_GUIDE: dict[str, dict] = {
    "01": {
        "description": "Farming, crop science, livestock, animal husbandry, rural livelihoods, food production",
        "include": ["farm", "farmer", "crop", "harvest", "livestock", "cattle", "soil", "irrigation",
                    "smallholder", "agronomy", "horticulture", "food security", "rural"],
        "exclude": "fisheries → 03 | forestry → 02 | food manufacturing → 10 | water/environment → 36-39",
        "examples": ["Crop yield under drought conditions in sub-Saharan Africa",
                     "Smallholder farmer adoption of improved seed varieties"],
    },
    "02": {
        "description": "Forestry, forest management, deforestation, timber, woodland ecology",
        "include": ["forest", "deforestation", "timber", "logging", "woodland", "reforestation"],
        "exclude": "agriculture → 01 | environmental remediation → 39",
        "examples": ["Deforestation rates in the Amazon basin", "Community forest management in Nepal"],
    },
    "03": {
        "description": "Fishing, aquaculture, marine fishing, fish stocks, coastal fishing communities",
        "include": ["fish", "fishing", "fishery", "aquaculture", "seafood", "coastal fishing"],
        "exclude": "marine environment research → 36 or 39 | oceanography → 72",
        "examples": ["Small-scale fisheries livelihoods in West Africa",
                     "Aquaculture adoption among coastal communities"],
    },
    "05": {
        "description": "Coal mining, miners, mining communities, coal industry",
        "include": ["coal", "coal miner", "coal community", "coal industry"],
        "exclude": "oil/gas → 06 | metal mining → 07 | general energy → 35",
        "examples": ["Occupational health in coal mining communities"],
    },
    "06": {
        "description": "Oil and gas extraction, petroleum industry, drilling, methane emissions, fossil fuels",
        "include": ["oil", "gas", "petroleum", "drilling", "fracking", "methane", "fossil fuel",
                    "crude oil", "natural gas", "offshore platform", "refinery"],
        "exclude": "energy policy/renewables → 35 | coal → 05",
        "examples": ["Methane emissions from the global oil and gas industry",
                     "Community impacts of offshore drilling in the Niger Delta"],
    },
    "08": {
        "description": "Quarrying, sand extraction, stone mining, non-metallic minerals",
        "include": ["quarry", "sand extraction", "stone mining", "aggregate"],
        "exclude": "metal mining → 07 | coal → 05",
        "examples": [],
    },
    "10": {
        "description": "Food processing, food manufacturing, food safety, food industry",
        "include": ["food processing", "food manufacturing", "food safety", "food industry"],
        "exclude": "agriculture/farming → 01 | food security → 01",
        "examples": ["Food safety practices in small food enterprises"],
    },
    "19": {
        "description": "Petroleum refining, fuel production, petrochemical industry",
        "include": ["petroleum refining", "fuel production", "petrochemical", "refinery"],
        "exclude": "oil extraction → 06",
        "examples": [],
    },
    "20": {
        "description": "Chemical industry, industrial chemicals, pesticides, hazardous substances",
        "include": ["chemical industry", "industrial chemical", "pesticide", "hazardous substance"],
        "exclude": "pharmaceuticals → 21 | environmental contamination → 39",
        "examples": ["Pesticide use and health outcomes among farmworkers"],
    },
    "21": {
        "description": "Pharmaceutical manufacturing, drug production, medicine manufacturing",
        "include": ["pharmaceutical", "drug production", "medicine manufacturing", "biopharmaceutical"],
        "exclude": "drug/medication in clinical context → 86 | chemical industry → 20",
        "examples": [],
    },
    "22": {
        "description": "Rubber, plastics industry, plastic pollution, packaging",
        "include": ["plastic pollution", "microplastics", "rubber industry", "packaging waste"],
        "exclude": "waste management → 38 | environmental contamination → 39",
        "examples": ["Microplastic contamination in freshwater systems"],
    },
    "23": {
        "description": "Glass, ceramics, cement, non-metallic mineral products manufacturing",
        "include": ["glass", "ceramics", "cement", "non-metallic mineral products"],
        "exclude": "mining/quarrying → 08 | construction → 41",
        "examples": [],
    },
    "26": {
        "description": "Electronics, semiconductors, computers, optical instruments manufacturing",
        "include": ["semiconductor", "electronics manufacturing", "optical instrument"],
        "exclude": "software/IT → 62 | telecom → 61",
        "examples": [],
    },
    "29": {
        "description": "Automotive industry, car manufacturing, vehicle production",
        "include": ["automotive", "car manufacturing", "vehicle production", "automobile"],
        "exclude": "transport/roads → 49",
        "examples": ["Electric vehicle adoption rates among urban consumers"],
    },
    "35": {
        "description": "Electricity production, power grids, renewable energy, solar, wind, energy policy",
        "include": ["renewable energy", "solar", "wind power", "electricity grid", "energy policy",
                    "energy transition", "carbon neutral", "net zero", "decarbonization"],
        "exclude": "oil/gas → 06 | coal → 05 | environmental remediation → 39",
        "examples": ["Community solar adoption in rural areas",
                     "Energy transition policy in developing countries"],
    },
    "36": {
        "description": "Water supply, drinking water, water treatment, water utilities, sea-level, hydrology",
        "include": ["water supply", "drinking water", "water treatment", "water utility",
                    "sea level", "hydrology", "water access", "ocean", "coastal flooding",
                    "sea level rise", "water quality", "watershed"],
        "exclude": "sewage/sanitation → 37 | waste management → 38 | ocean ecology → 72",
        "examples": ["Sea level rise data for coastal communities",
                     "Drinking water access in rural sub-Saharan Africa",
                     "Lake bathymetry and water quality measurements"],
    },
    "37": {
        "description": "Sewerage, sanitation systems, wastewater, sewage treatment",
        "include": ["sewage", "sanitation", "wastewater", "open defecation", "WASH"],
        "exclude": "water supply → 36 | waste management → 38",
        "examples": ["Open defecation and child health in rural India"],
    },
    "38": {
        "description": "Waste management, recycling, landfills, garbage collection",
        "include": ["waste management", "recycling", "landfill", "garbage", "solid waste"],
        "exclude": "environmental contamination → 39 | plastic pollution → 22",
        "examples": [],
    },
    "39": {
        "description": "Environmental remediation, contaminated land, hazardous waste, soil/water pollution",
        "include": ["contamination", "remediation", "hazardous waste", "soil pollution",
                    "water pollution", "environmental cleanup", "toxicity"],
        "exclude": "regular waste management → 38 | chemical industry → 20",
        "examples": ["Anthropogenic contamination of tap water supplies"],
    },
    "41": {
        "description": "Construction industry, building workers, housing construction, real estate development",
        "include": ["construction", "building workers", "housing construction"],
        "exclude": "civil engineering/infrastructure → 42 | housing markets → 68 | urban planning → 71",
        "examples": [],
    },
    "42": {
        "description": "Civil engineering, infrastructure, roads, bridges, dams, public works",
        "include": ["civil engineering", "infrastructure", "road construction", "bridge", "dam"],
        "exclude": "construction → 41 | engineering consultancy → 71",
        "examples": [],
    },
    "47": {
        "description": "Retail, consumer behavior, shopping, supermarkets, e-commerce",
        "include": ["retail", "consumer behavior", "shopping", "e-commerce", "supermarket"],
        "exclude": "wholesale/supply chain → 46 | tourism → 79",
        "examples": [],
    },
    "49": {
        "description": "Road transport, railways, trucking, logistics, commuting, mobility, transport workers",
        "include": ["transport", "mobility", "commuting", "railway", "trucking", "road safety",
                    "transit", "urban mobility", "transport worker", "naval", "naval power"],
        "exclude": "aviation → 51 | shipping → 50 | logistics → 52",
        "examples": ["Naval power dataset 1865-2011",
                     "Urban mobility patterns in megacities"],
    },
    "50": {
        "description": "Shipping, maritime transport, ports, seafarers, ocean freight",
        "include": ["shipping", "maritime", "seafarer", "port", "ocean freight", "vessel"],
        "exclude": "transport → 49 | ocean science → 72",
        "examples": [],
    },
    "51": {
        "description": "Aviation, airlines, airports, air travel, flight",
        "include": ["aviation", "airline", "airport", "air travel", "flight safety"],
        "exclude": "transport → 49",
        "examples": [],
    },
    "58": {
        "description": "Book publishing, academic journals, newspapers, magazines, media publishing",
        "include": ["publishing", "academic journal", "newspaper", "book publishing", "media publishing"],
        "exclude": "broadcasting/news → 60 | social media platform research → 63",
        "examples": [],
    },
    "60": {
        "description": "Broadcasting, radio, television, news agencies, journalism, political media",
        "include": ["broadcasting", "television", "radio", "news agency", "journalism",
                    "political speech", "media content", "political communication", "propaganda"],
        "exclude": "social media → 63 | publishing → 58 | politics itself → 84",
        "examples": ["Gendered speech patterns in presidential addresses",
                     "News media framing of immigration"],
    },
    "61": {
        "description": "Telecommunications, mobile networks, broadband, internet access, connectivity",
        "include": ["telecom", "mobile network", "broadband", "internet access", "connectivity",
                    "digital divide", "ICT access"],
        "exclude": "software → 62 | digital platforms → 63",
        "examples": [],
    },
    "62": {
        "description": "Software development, IT consulting, tech workers, programming, agile, open source",
        "include": ["software development", "programmer", "tech worker", "agile", "open source",
                    "IT professional", "coding", "DevOps"],
        "exclude": "hardware/electronics → 26 | telecom → 61 | data infrastructure → 63",
        "examples": [],
    },
    "63": {
        "description": "Cloud computing, data centers, internet platforms, big data, digital infrastructure",
        "include": ["cloud computing", "data center", "internet platform", "big data",
                    "digital infrastructure", "online platform"],
        "exclude": "software dev → 62 | telecom → 61",
        "examples": [],
    },
    "64": {
        "description": "Banking, financial services, credit, microfinance, financial inclusion, fintech",
        "include": ["banking", "microfinance", "financial inclusion", "credit", "fintech",
                    "financial service", "mobile money"],
        "exclude": "insurance → 65 | financial markets → 66",
        "examples": [],
    },
    "68": {
        "description": "Real estate, housing markets, gentrification, rent, tenants, eviction",
        "include": ["housing market", "gentrification", "tenant", "landlord", "rent", "eviction",
                    "affordable housing", "property market"],
        "exclude": "construction → 41 | homelessness (social work) → 88",
        "examples": [],
    },
    "69": {
        "description": "Legal services, law firms, accounting, auditing, justice systems, courts",
        "include": ["legal", "law firm", "accounting", "audit", "court", "justice", "law"],
        "exclude": "public administration/policy → 84 | compliance → 84",
        "examples": ["Legal aid access among low-income populations"],
    },
    "70": {
        "description": "Corporate management, organizational behavior, business strategy, leadership",
        "include": ["corporate management", "organizational behavior", "business strategy", "leadership"],
        "exclude": "public administration → 84 | labor markets → 78",
        "examples": [],
    },
    "71": {
        "description": "Engineering consultancy, architecture, urban planning, technical testing, surveys, measurements",
        "include": ["engineering", "architecture", "urban planning", "technical survey",
                    "measurement", "geospatial", "GIS", "mapping", "bathymetry",
                    "coastal survey", "topographic"],
        "exclude": "construction → 41 | scientific R&D → 72 | water/environment → 36",
        "examples": ["Barrier island shoreline mapping",
                     "Lake bathymetry measurements"],
    },
    "72": {
        "description": "Scientific R&D, academic research, interdisciplinary studies, research methodology",
        "include": ["research methodology", "meta-analysis", "systematic review", "bibliometrics",
                    "physics", "chemistry", "materials science", "geophysics", "astronomy",
                    "basic science", "fundamental research", "interdisciplinary"],
        "exclude": (
            "IMPORTANT: N/72 is for research ABOUT science/methodology itself, or basic science "
            "with no specific sector. Do NOT use for: "
            "health research → 86 | politics → 84 | education → 85 | "
            "agriculture → 01 | water/environment → 36 | oil/gas → 06 | "
            "transport → 49 | social work → 88. "
            "If the research has a clear subject domain, use that domain."
        ),
        "examples": ["Zeeman-type band splitting in sliding ferroelectrics (physics → N/72)",
                     "Systematic review of systematic review methods (meta-science → N/72)",
                     "WRONG: HIV clinical trial → R/86, not N/72",
                     "WRONG: Electoral trust survey → P/84, not N/72"],
    },
    "73": {
        "description": "Marketing, advertising, public relations, market research, consumer surveys, branding",
        "include": ["marketing", "advertising", "PR", "market research", "consumer survey", "branding"],
        "exclude": "political surveys → 84 | health surveys → 86 | social surveys → 72",
        "examples": [],
    },
    "74": {
        "description": "Creative professionals, design, photography, translation, scientific consulting",
        "include": ["creative professional", "design", "photography", "translation", "consulting"],
        "exclude": "arts/culture → 90 | software → 62",
        "examples": [],
    },
    "75": {
        "description": "Veterinary medicine, animal health, livestock disease, pet care, wildlife health",
        "include": ["veterinary", "animal health", "livestock disease", "wildlife health",
                    "zoonotic", "animal welfare"],
        "exclude": "human health → 86 | agriculture/livestock → 01 | ecology → 72",
        "examples": ["Zoonotic disease transmission between livestock and humans"],
    },
    "78": {
        "description": "Employment, labor markets, job seeking, recruitment, workforce, gig economy",
        "include": ["labor market", "unemployment", "job seeker", "gig work", "precarious work",
                    "trade union", "worker rights", "informal employment"],
        "exclude": "public admin/labor policy → 84 | social work → 88",
        "examples": ["Gig economy workers' income volatility",
                     "Labor market integration of refugees"],
    },
    "79": {
        "description": "Tourism, travel agencies, tour operators, tourist behavior, travel",
        "include": ["tourism", "tourist", "travel agency", "tour operator", "ecotourism"],
        "exclude": "hotels → 55 | transport → 49",
        "examples": [],
    },
    "82": {
        "description": "Administrative services, call centers, business process outsourcing, office work",
        "include": ["administrative service", "call center", "BPO", "office work", "clerical"],
        "exclude": "public administration → 84",
        "examples": [],
    },
    "84": {
        "description": "Government, public policy, politics, elections, voting, civil service, military, democracy",
        "include": ["government", "election", "voting", "voter", "political party", "parliament",
                    "democracy", "governance", "public policy", "legislature", "civil service",
                    "military", "defense", "bureaucracy", "state capacity", "political trust",
                    "electoral fraud", "political participation"],
        "exclude": "political media/communication → 60 | legal services → 69 | international orgs → 99",
        "examples": ["Electoral trust and operational transparency",
                     "Spanish general elections replication data",
                     "Military expenditure and state building in Africa"],
    },
    "85": {
        "description": "Education, schools, universities, teachers, students, learning, pedagogy, literacy",
        "include": ["school", "classroom", "teacher", "student", "education", "curriculum",
                    "pedagogy", "literacy", "numeracy", "university", "higher education",
                    "dropout", "learning outcome", "STEM education"],
        "exclude": "training/workforce → 78 | educational policy (if govt focus) → 84",
        "examples": ["Teacher professional development in low-income schools",
                     "Language diversity and multilingualism in urban schools"],
    },
    "86": {
        "description": "Healthcare, medicine, hospitals, patients, clinical trials, mental health, public health, epidemiology",
        "include": ["patient", "hospital", "clinical trial", "HIV", "AIDS", "cancer", "disease",
                    "epidemiology", "mortality", "morbidity", "mental health", "depression",
                    "vaccination", "public health", "reproductive health", "abortion",
                    "maternal health", "child health", "malaria", "tuberculosis",
                    "antiretroviral", "ART", "NNRTI", "neglected tropical disease",
                    "onchocerciasis", "filariasis", "parasitology", "infectious disease"],
        "exclude": "social work → 88 | nursing homes → 87 | vet medicine → 75 | health policy (govt focus) → 84",
        "examples": ["HIV antiretroviral treatment in infants exposed to nevirapine",
                     "District mapping of onchocerciasis and lymphatic filariasis",
                     "Abortion underreporting in cognitive interviews",
                     "Cancer screening communication on TikTok"],
    },
    "87": {
        "description": "Nursing homes, residential care, elder care, disability support, long-term care",
        "include": ["nursing home", "residential care", "elder care", "long-term care",
                    "disability support", "care home"],
        "exclude": "healthcare → 86 | social work → 88",
        "examples": [],
    },
    "88": {
        "description": "Social work, welfare, child protection, homelessness, poverty, family support",
        "include": ["social work", "child protection", "welfare", "homeless", "poverty",
                    "food bank", "domestic violence", "foster care", "family support"],
        "exclude": "healthcare → 86 | public admin/policy → 84 | education → 85",
        "examples": ["Child protection outcomes in foster care",
                     "Homelessness trajectories in urban shelters"],
    },
    "90": {
        "description": "Arts, culture, music, theater, dance, creative expression, performing arts",
        "include": ["arts", "music", "theater", "dance", "performing arts", "artist", "creative"],
        "exclude": "museums/archives → 91 | media/broadcasting → 60",
        "examples": [],
    },
    "91": {
        "description": "Museums, libraries, archives, cultural heritage, oral history, digital humanities",
        "include": ["museum", "archive", "library", "cultural heritage", "oral history",
                    "digital humanities", "manuscript", "artifact", "collection"],
        "exclude": "performing arts → 90 | academic publishing → 58",
        "examples": ["Oral history archive of civil rights movement",
                     "Digital preservation of indigenous language materials"],
    },
    "93": {
        "description": "Sports, athletics, recreation, leisure, physical activity, fitness",
        "include": ["sport", "athlete", "physical activity", "recreation", "fitness",
                    "critical power", "exercise", "performance"],
        "exclude": "health outcomes of exercise → 86",
        "examples": ["Critical power and W-prime in endurance athletes"],
    },
    "94": {
        "description": "Civil society, NGOs, associations, trade unions, religious organizations, volunteering",
        "include": ["NGO", "civil society", "trade union", "volunteer", "charity",
                    "nonprofit", "religious organization", "association"],
        "exclude": "social work → 88 | international orgs → 99",
        "examples": [],
    },
    "99": {
        "description": "International organizations, UN agencies, diplomatic missions, global governance",
        "include": ["UN", "World Bank", "IMF", "NATO", "diplomatic", "intergovernmental",
                    "international organization", "global governance", "multilateral",
                    "executive agreement", "treaty", "foreign policy"],
        "exclude": "national government → 84 | NGOs → 94",
        "examples": ["Executive agreements database between states",
                     "UN peacekeeping mission effectiveness"],
    },
}


def build_division_context(div: str) -> str:
    """Return a compact LLM-friendly description for a division."""
    g = DIVISION_GUIDE.get(div)
    if not g:
        return ""
    parts = [g["description"]]
    if g.get("exclude"):
        parts.append(f"NOT: {g['exclude']}")
    return " | ".join(parts)


def get_disambiguation_block() -> str:
    """
    Return a compact disambiguation cheat-sheet for the most confused
    divisions, to be included in every LLM prompt.
    """
    return """KEY DISAMBIGUATION RULES:
  N/72 (Scientific R&D): ONLY for datasets about research methodology/meta-science, or
        basic science (physics, chemistry, materials) with no specific sector.
        NOT for: domain-specific research that USES methods → use the domain instead.
  R/86 (Human Health): ANY research involving patients, diseases, clinical data, public health.
        Includes: HIV, cancer, mental health, epidemiology, reproductive health.
  P/84 (Public Admin): Government, elections, voting, public policy, military, democracy.
  Q/85 (Education): Schools, teachers, students, learning outcomes, pedagogy.
  A/01 (Agriculture): Farming, crops, livestock, rural livelihoods.
  E/36 (Water/Environment): Water supply, sea level, hydrology, ocean data, coastal.
  H/49 (Transport): Roads, railways, naval/military vessels, mobility data.
  V/99 (International Orgs): UN, World Bank, treaties, diplomatic agreements."""
