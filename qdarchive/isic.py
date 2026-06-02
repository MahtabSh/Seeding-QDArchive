"""
ISIC Rev. 5 — International Standard Industrial Classification of All Economic Activities
Revision 5 (United Nations Statistics Division, 2025)

Source: ISIC_Rev_5_english_structure.csv (official UN data)

NOTE: Section letters in Rev. 5 differ from Rev. 4 — all sections from J onwards
are shifted by at least one position.  Key changes vs Rev. 4:
  • J = Publishing, broadcasting & content  (was "Information & communication" J in Rev.4)
  • K = Telecom, IT, computing              (new; merged from old J)
  • L = Financial & insurance               (was K)
  • M = Real estate                         (was L)
  • N = Professional, scientific & technical (was M — includes div 72 Scientific R&D)
  • O = Administrative & support services   (was N)
  • P = Public administration               (was O)
  • Q = Education                           (was P)
  • R = Human health & social work         (was Q)
  • S = Arts, sports & recreation           (was R)
  • T = Other service activities            (was S)
  • U = Households as employers             (was T)
  • V = Extraterritorial organizations      (was U)
  • G no longer contains division 45 (motor vehicles now inside 46/47 sub-groups)
"""

from __future__ import annotations

# ── Section definitions ────────────────────────────────────────────────────────
# (letter, full name) — exact titles from ISIC Rev. 5 CSV
SECTIONS: list[tuple[str, str]] = [
    ("A", "Agriculture, forestry and fishing"),
    ("B", "Mining and quarrying"),
    ("C", "Manufacturing"),
    ("D", "Electricity, gas, steam and air conditioning supply"),
    ("E", "Water supply; sewerage, waste management and remediation activities"),
    ("F", "Construction"),
    ("G", "Wholesale and retail trade"),
    ("H", "Transportation and storage"),
    ("I", "Accommodation and food service activities"),
    ("J", "Publishing, broadcasting, and content production and distribution activities"),
    ("K", "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities"),
    ("L", "Financial and insurance activities"),
    ("M", "Real estate activities"),
    ("N", "Professional, scientific and technical activities"),
    ("O", "Administrative and support service activities"),
    ("P", "Public administration and defence; compulsory social security"),
    ("Q", "Education"),
    ("R", "Human health and social work activities"),
    ("S", "Arts, sports and recreation"),
    ("T", "Other service activities"),
    ("U", "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use"),
    ("V", "Activities of extraterritorial organizations and bodies"),
]

SECTION_MAP: dict[str, str] = {code: name for code, name in SECTIONS}

# ── Division definitions ───────────────────────────────────────────────────────
# (section_letter, division_code, division_name) — 2-digit divisions only
DIVISIONS: list[tuple[str, str, str]] = [

    # A — Agriculture, forestry and fishing
    ("A", "01", "Crop and animal production, hunting and related service activities"),
    ("A", "02", "Forestry and logging"),
    ("A", "03", "Fishing and aquaculture"),

    # B — Mining and quarrying
    ("B", "05", "Mining of coal and lignite"),
    ("B", "06", "Extraction of crude petroleum and natural gas"),
    ("B", "07", "Mining of metal ores"),
    ("B", "08", "Other mining and quarrying"),
    ("B", "09", "Mining support service activities"),

    # C — Manufacturing
    ("C", "10", "Manufacture of food products"),
    ("C", "11", "Manufacture of beverages"),
    ("C", "12", "Manufacture of tobacco products"),
    ("C", "13", "Manufacture of textiles"),
    ("C", "14", "Manufacture of wearing apparel"),
    ("C", "15", "Manufacture of leather and related products"),
    ("C", "16", "Manufacture of wood and of products of wood and cork, except furniture; manufacture of articles of straw and plaiting materials"),
    ("C", "17", "Manufacture of paper and paper products"),
    ("C", "18", "Printing and reproduction of recorded media"),
    ("C", "19", "Manufacture of coke and refined petroleum products"),
    ("C", "20", "Manufacture of chemicals and chemical products"),
    ("C", "21", "Manufacture of basic pharmaceutical products and pharmaceutical preparations"),
    ("C", "22", "Manufacture of rubber and plastic products"),
    ("C", "23", "Manufacture of other non-metallic mineral products"),
    ("C", "24", "Manufacture of basic metals"),
    ("C", "25", "Manufacture of fabricated metal products, except machinery and equipment"),
    ("C", "26", "Manufacture of computer, electronic and optical products"),
    ("C", "27", "Manufacture of electrical equipment"),
    ("C", "28", "Manufacture of machinery and equipment n.e.c."),
    ("C", "29", "Manufacture of motor vehicles, trailers and semi-trailers"),
    ("C", "30", "Manufacture of other transport equipment"),
    ("C", "31", "Manufacture of furniture"),
    ("C", "32", "Other manufacturing"),
    ("C", "33", "Repair, maintenance and installation of machinery and equipment"),

    # D — Electricity, gas, steam and air conditioning supply
    ("D", "35", "Electricity, gas, steam and air conditioning supply"),

    # E — Water supply; sewerage, waste management
    ("E", "36", "Water collection, treatment and supply"),
    ("E", "37", "Sewerage"),
    ("E", "38", "Waste collection, treatment and disposal, and recovery activities"),
    ("E", "39", "Remediation and other waste management service activities"),

    # F — Construction
    ("F", "41", "Construction of residential and non-residential buildings"),
    ("F", "42", "Civil engineering"),
    ("F", "43", "Specialized construction activities"),

    # G — Wholesale and retail trade
    ("G", "46", "Wholesale trade"),
    ("G", "47", "Retail trade"),

    # H — Transportation and storage
    ("H", "49", "Land transport and transport via pipelines"),
    ("H", "50", "Water transport"),
    ("H", "51", "Air transport"),
    ("H", "52", "Warehousing and support activities for transportation"),
    ("H", "53", "Postal and courier activities"),

    # I — Accommodation and food service activities
    ("I", "55", "Accommodation"),
    ("I", "56", "Food and beverage service activities"),

    # J — Publishing, broadcasting, and content production and distribution
    ("J", "58", "Publishing activities"),
    ("J", "59", "Motion picture, video and television programme production, sound recording and music publishing activities"),
    ("J", "60", "Programming, broadcasting, news agency and other content distribution activities"),

    # K — Telecommunications, computer programming, computing infrastructure
    ("K", "61", "Telecommunications"),
    ("K", "62", "Computer programming, consultancy and related activities"),
    ("K", "63", "Computing infrastructure, data processing, hosting, and other information service activities"),

    # L — Financial and insurance activities
    ("L", "64", "Financial service activities, except insurance and pension funding"),
    ("L", "65", "Insurance, reinsurance and pension funding, except compulsory social security"),
    ("L", "66", "Activities auxiliary to financial service and insurance activities"),

    # M — Real estate activities
    ("M", "68", "Real estate activities"),

    # N — Professional, scientific and technical activities
    ("N", "69", "Legal and accounting activities"),
    ("N", "70", "Activities of head offices; management consultancy activities"),
    ("N", "71", "Architectural and engineering activities; technical testing and analysis"),
    ("N", "72", "Scientific research and development"),
    ("N", "73", "Activities of advertising, market research and public relations"),
    ("N", "74", "Other professional, scientific and technical activities"),
    ("N", "75", "Veterinary activities"),

    # O — Administrative and support service activities
    ("O", "77", "Rental and leasing activities"),
    ("O", "78", "Employment activities"),
    ("O", "79", "Travel agency, tour operator, and other travel related activities"),
    ("O", "80", "Investigation and security activities"),
    ("O", "81", "Services to buildings and landscape activities"),
    ("O", "82", "Office administrative, office support and other business support activities"),

    # P — Public administration and defence; compulsory social security
    ("P", "84", "Public administration and defence; compulsory social security"),

    # Q — Education
    ("Q", "85", "Education"),

    # R — Human health and social work activities
    ("R", "86", "Human health activities"),
    ("R", "87", "Residential care activities"),
    ("R", "88", "Social work activities without accommodation"),

    # S — Arts, sports and recreation
    ("S", "90", "Arts creation and performing arts activities"),
    ("S", "91", "Library, archives, museum and other cultural activities"),
    ("S", "92", "Gambling and betting activities"),
    ("S", "93", "Sports activities and amusement and recreation activities"),

    # T — Other service activities
    ("T", "94", "Activities of membership organizations"),
    ("T", "95", "Repair and maintenance of computers, personal and household goods, and motor vehicles and motorcycles"),
    ("T", "96", "Personal service activities"),

    # U — Activities of households as employers
    ("U", "97", "Activities of households as employers of domestic personnel"),
    ("U", "98", "Undifferentiated goods- and services-producing activities of private households for own use"),

    # V — Activities of extraterritorial organizations and bodies
    ("V", "99", "Activities of extraterritorial organizations and bodies"),
]

# Lookup tables
DIVISION_MAP: dict[str, tuple[str, str]] = {
    div: (sec, name) for sec, div, name in DIVISIONS
}

VALID_SECTIONS: frozenset[str] = frozenset(SECTION_MAP)
VALID_DIVISIONS: frozenset[str] = frozenset(DIVISION_MAP)

# Default fallback when nothing can be inferred:
# N/72 = Scientific research and development (under Professional/scientific in Rev.5)
DEFAULT_SECTION  = "N"
DEFAULT_DIVISION = "72"


def section_for_division(division_code: str) -> tuple[str, str] | tuple[None, None]:
    """Return (section_letter, section_name) for a division code, or (None, None)."""
    entry = DIVISION_MAP.get(division_code)
    if entry is None:
        return None, None
    sec_letter = entry[0]
    return sec_letter, SECTION_MAP.get(sec_letter, "")


def division_name(division_code: str) -> str:
    """Return the division name for a 2-digit code, or empty string."""
    entry = DIVISION_MAP.get(division_code)
    return entry[1] if entry else ""


def taxonomy_prompt_block() -> str:
    """
    Compact text block of the ISIC Rev. 5 taxonomy (sections + divisions),
    suitable for use in a cached system prompt.
    """
    lines = [
        "ISIC Rev. 5 — Two-Level Taxonomy (Sections → Divisions)",
        "Source: UN Statistics Division, 2025",
        "=" * 62,
    ]
    current_sec = None
    for sec, div, name in DIVISIONS:
        if sec != current_sec:
            sec_name = SECTION_MAP[sec]
            lines.append(f"\nSection {sec}: {sec_name}")
            current_sec = sec
        lines.append(f"  Division {div}: {name}")
    return "\n".join(lines)
