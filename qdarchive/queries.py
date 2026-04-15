"""
Search query lists used across all crawlers.

QDA_EXTENSION_QUERIES — file-extension searches (.qdpx, .nvpx …)
                         Dataverse matches filenames, so these are the most
                         precise queries and should always run first.
QDA_SOFTWARE_QUERIES  — QDA software names (NVivo, MAXQDA, ATLAS.ti …)
QDA_KEYWORD_QUERIES   — methodology / discipline / language keywords
"""

# ── Tier 1: QDA file extensions ───────────────────────────────────────────────
# Dataverse matches against filenames, so ".qdpx" finds only datasets that
# actually contain a .qdpx file — zero noise, maximum precision.
QDA_EXTENSION_QUERIES: list[str] = [
    ".qdpx",  ".qdc",                              # REFI-QDA / generic
    ".nvp",   ".nvpx",                             # NVivo
    ".mqda",  ".mqbac", ".mqtc", ".mqex",          # MAXQDA
    ".mx24",  ".mx22",  ".mx20", ".mx18", ".mx12", # MAXQDA versioned
    ".mx11",  ".mx5",   ".mx4",  ".mx3",  ".mx2",  # MAXQDA legacy
    ".m2k",   ".loa",   ".sea",  ".mtr",  ".mod",  # MAXQDA misc
    ".atlasproj", ".hpr7",                         # ATLAS.ti
    ".ppj",   ".pprj",  ".qlt",                    # QDA Miner / other
    ".f4p",   ".qpd",                              # f4analyse / other
    ".rqda",                                       # RQDA (R package)
    ".tns",                                        # Transana (video QDA)
]

# ── Tier 2: QDA software names ────────────────────────────────────────────────
QDA_SOFTWARE_QUERIES: list[str] = [
    "NVivo",
    "MAXQDA",
    "ATLAS.ti",
    "Dedoose",
    "QDA Miner",
    "f4analyse",
    "Quirkos",
    "CAQDAS",
    "REFI-QDA",
    "Transana",          # video/audio qualitative analysis
    "Taguette",          # open-source QDAS
    "RQDA",              # R-based QDA package
    "MaxQDA",            # alternate spelling used in some deposits
]

# ── Tier 3–9: methodology / discipline / archiving / language keywords ─────────
QDA_KEYWORD_QUERIES: list[str] = [
    "oral history",
    "ethnographic data",
    "field notes qualitative",
    "coding qualitative",
    "qualitative data archive",
    "qdpx",
    "interview study",
    "qualitative research data",
    "qualitative data analysis",
    "thematic analysis",
    "grounded theory",
    "interview transcripts",
    "focus group transcripts",
    "qualitative interviews",
    "semi-structured interviews",

    # ── Tier 4: data collection methods ───────────────────────────────────────
    "in-depth interviews",
    "narrative inquiry",
    "life history interviews",
    "oral history interview",
    "oral history collection",
    "oral history transcript",
    "biographical interviews",
    "expert interviews",
    "key informant interview",
    "participant observation",
    "ethnographic fieldwork",
    "discourse analysis data",
    "conversation analysis data",
    "content analysis qualitative",
    "photovoice",
    "photo elicitation",
    "diary studies qualitative",
    "think-aloud protocol",
    "think-aloud data",
    "participatory research data",

    # ── Tier 5: transcripts and raw data ──────────────────────────────────────
    "interview transcript",
    "verbatim transcript",
    "audio recording interview",
    "video recording interview",
    "coded transcript",
    "research transcript",
    "focus group data",
    "group interview data",
    "narrative data",
    "qualitative codebook",
    "coding scheme qualitative",
    "open coding",
    "axial coding",
    "constant comparative",
    "data saturation",
    "member checking",

    # ── Tier 6: discipline-specific qualitative data ───────────────────────────
    "qualitative health research",
    "qualitative nursing research",
    "qualitative psychology data",
    "qualitative sociology data",
    "qualitative education research",
    "qualitative social work",
    "qualitative political science",
    "phenomenological study data",
    "interpretive phenomenological analysis",
    "IPA qualitative",
    "case study qualitative",
    "action research qualitative",

    # ── Tier 7: archiving and sharing terms ───────────────────────────────────
    "qualitative data sharing",
    "qualitative data reuse",
    "secondary analysis qualitative",
    "archived interviews",
    "deposited interviews",
    "UK Data Archive qualitative",
    "UKDA qualitative",
    "Qualidata",
    "CESSDA qualitative",
    "Qualiservice",              # German qualitative data archive
    "DANS qualitative",          # Dutch data archive
    "qualitative data deposit",
    "data replication qualitative",

    # ── Tier 8: mixed-methods (qualitative component) ─────────────────────────
    "mixed methods qualitative",
    "qual strand",
    "qualitative component",
    "interview data mixed methods",

    # ── Tier 9: language-specific ─────────────────────────────────────────────
    "qualitative Forschung",        # German: qualitative research
    "qualitative Forschungsdaten",  # German: qualitative research data
    "Interviewdaten",               # German: interview data
    "Interview-Transkript",         # German: interview transcript
    "Leitfadeninterview",           # German: semi-structured interview
    "Feldnotizen",                  # German: field notes
    "Fokusgruppe",                  # German: focus group
    "données qualitatives",         # French: qualitative data
    "données d'entretien",          # French: interview data
    "entretiens qualitatifs",       # French: qualitative interviews
    "investigación cualitativa",    # Spanish: qualitative research
    "entrevistas cualitativas",     # Spanish: qualitative interviews
    "pesquisa qualitativa",         # Portuguese: qualitative research
    "dados qualitativos",           # Portuguese: qualitative data
    "ricerca qualitativa",          # Italian: qualitative research
    "dati qualitativi",             # Italian: qualitative data
    "kwalitatief onderzoek",        # Dutch: qualitative research
]
