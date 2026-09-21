#-----------------------------------------------------------------------------
# process_registered_voters_with_property_data.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to load and process the registered voter list from Travis County
# and analyze it against a list of property addresses and descriptions.
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements,too-many-return-statements
# pylint: disable=too-many-lines
#-----------------------------------------------------------------------------
"""process_registered_voters_with_property_data.py"""

from __future__ import annotations

import csv
import os
import re
import sys
from collections import Counter
from urllib.parse import quote_plus


# Residential TCAD state-code prefixes (A=single family, B=multifamily, E=farm/ranch resid,
# O=residential inventory). These are excluded from the non-residential match report.
RESIDENTIAL_STATE_PREFIXES = ('A', 'B', 'E', 'O')

# Improvement-type markers that are always treated as residential / excluded from the
# non-residential match report, regardless of imprv_state_cd. Matching is case-insensitive
# substring search (so CONDO also matches CONDOS / CONDOMINIUM, etc.).
ALWAYS_RESIDENTIAL_IMPROV_TYPE_MARKERS = (
    'DWELLING',
    'CONDO',
    'CONDOS',
)

# F1 (commercial) improvement-type markers that are residential-use and should be treated as
# residential / excluded from the non-residential match report. Matching is case-insensitive
# substring search so prefixes or additional ';'-separated types still match.
F1_RESIDENTIAL_IMPROV_TYPE_MARKERS = (
    'TREATMENT/REHAB',
    'SFR COMM',
    'DUPLEX COMM',
    'GARAGE APT COMM',
    'DORMITORY',
    'FRAT/SORORITY',
    'INDEPENDENT LIVING',
    'ASSISTED LIVING/MEMORY',
    'SKILLED NURSING',
    'ALT LIVING CTR',
    'MOHO',
    'CONTINUING CARE',
    'OFF HI-RISE',
    'DWELLING',
    'CONDOS',
    'MOTEL',
    'HOTEL',
    'RELIGIOUS',
    'RETIREMENT',
    'CLUBHOUSE',
)

# M1 improvement-type markers that are residential-use and should be treated as residential /
# excluded from the non-residential match report. Matching is case-insensitive substring search.
M1_RESIDENTIAL_IMPROV_TYPE_MARKERS = (
    'MOHO',
)

# Common directional / street-type expansions used during normalization.
_DIRECTION_MAP = {
    'N': 'N', 'NO': 'N', 'NORTH': 'N',
    'S': 'S', 'SO': 'S', 'SOUTH': 'S',
    'E': 'E', 'EAST': 'E',
    'W': 'W', 'WEST': 'W',
    'NE': 'NE', 'NORTHEAST': 'NE',
    'NW': 'NW', 'NORTHWEST': 'NW',
    'SE': 'SE', 'SOUTHEAST': 'SE',
    'SW': 'SW', 'SOUTHWEST': 'SW',
}

_STREET_TYPE_MAP = {
    'AVENUE': 'AVE', 'AVE': 'AVE', 'AV': 'AVE',
    'BOULEVARD': 'BLVD', 'BLVD': 'BLVD', 'BL': 'BLVD',
    'CIRCLE': 'CIR', 'CIR': 'CIR', 'CRCL': 'CIR',
    'COURT': 'CT', 'CT': 'CT', 'CRT': 'CT',
    'COVE': 'CV', 'CV': 'CV',
    'DRIVE': 'DR', 'DR': 'DR', 'DRV': 'DR',
    'EXPRESSWAY': 'EXPY', 'EXPY': 'EXPY', 'EXPWY': 'EXPY',
    'FREEWAY': 'FWY', 'FWY': 'FWY', 'FRWY': 'FWY',
    # TCAD situs often uses HY for highway.
    'HIGHWAY': 'HWY', 'HWY': 'HWY', 'HIWAY': 'HWY', 'HY': 'HWY',
    'LANE': 'LN', 'LN': 'LN',
    'PARKWAY': 'PKWY', 'PKWY': 'PKWY', 'PKY': 'PKWY', 'PKWAY': 'PKWY',
    'PLACE': 'PL', 'PL': 'PL',
    'PLAZA': 'PLAZA', 'PLZ': 'PLAZA',
    'ROAD': 'RD', 'RD': 'RD',
    'STREET': 'ST', 'ST': 'ST', 'STR': 'ST',
    'TERRACE': 'TER', 'TER': 'TER', 'TERR': 'TER',
    'TRAIL': 'TRL', 'TRL': 'TRL', 'TR': 'TRL',
    'WAY': 'WAY', 'WY': 'WAY',
    'LOOP': 'LOOP', 'LP': 'LOOP',
    'PASS': 'PASS',
    'PATH': 'PATH',
    'RUN': 'RUN',
    'ROW': 'ROW',
    'SQUARE': 'SQ', 'SQ': 'SQ',
    'CROSSING': 'XING', 'XING': 'XING',
    'BEND': 'BND', 'BND': 'BND',
    'POINT': 'PT', 'PT': 'PT',
    'RIDGE': 'RDG', 'RDG': 'RDG',
    'HILL': 'HL', 'HL': 'HL',
    'HILLS': 'HLS', 'HLS': 'HLS',
    'CREEK': 'CRK', 'CRK': 'CRK',
    'VIEW': 'VW', 'VW': 'VW',
}

# Leading direction tokens that may be dropped as a match fallback when the
# property situs omits a pre-direction (e.g. voter "W WELLS BRANCH PKWY" vs
# property "WELLS BRANCH PKWY"). Not applied to numbered/highway streets.
_LEADING_DIRECTIONS = frozenset({'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW'})
_HIGHWAY_NAME_TOKENS = frozenset({'IH', 'US', 'SH', 'FM', 'HWY', 'RR', 'HY', 'SVRD'})

# Trailing street-type tokens that may be dropped as a match fallback when the
# voter roll omits a type that TCAD stores in situs_street_suffix (e.g. voter
# "BOB HARRISON" vs property "BOB HARRISON ST"). PLAZA is excluded because it is
# often the primary street name (e.g. "NORTH PLAZA"), not a suffix.
_STRIP_TRAILING_STREET_TYPES = frozenset({
    'AVE', 'BLVD', 'CIR', 'CT', 'CV', 'DR', 'EXPY', 'FWY', 'HWY', 'LN',
    'PKWY', 'PL', 'RD', 'ST', 'TER', 'TRL', 'WAY', 'LOOP', 'PASS', 'PATH',
    'RUN', 'ROW', 'SQ', 'XING', 'BND', 'PT', 'RDG', 'HL', 'HLS', 'CRK', 'VW',
})

_UNIT_TYPE_TOKENS = {
    'APT', 'APARTMENT', 'UNIT', 'STE', 'SUITE', 'BLDG', 'BUILDING',
    'FL', 'FLOOR', 'RM', 'ROOM', 'DEPT', '#', 'NO', 'NUM', 'NUMBER',
}

_STATE_TOKENS = {'TX', 'TEXAS'}

# City names commonly found in Travis County voter files (used when stripping
# free-form residential addresses). Longer multi-word names are checked first.
_KNOWN_CITIES = (
    'WEST LAKE HILLS', 'LAKEWAY', 'LAGO VISTA', 'JONESTOWN', 'BEE CAVE',
    'BEE CAVES', 'CREEDMOOR', 'MUSTANG RIDGE', 'POINT VENTURE', 'ROLLINGWOOD',
    'SAN LEANNA', 'THE HILLS', 'WEBBERVILLE', 'PFLUGERVILLE', 'ROUND ROCK',
    'CEDAR PARK', 'LEANDER', 'MANOR', 'AUSTIN', 'DEL VALLE', 'DELVALLE',
    'SPICEWOOD', 'SUNSET VALLEY', 'VOLENTE', 'BRIARCLIFF', 'COUPLAND',
    'ELGIN', 'HUTTO', 'KYLE', 'BUDA', 'DRIPPING SPRINGS',
)

_STREET_TYPE_PATTERN = '|'.join(
    sorted(_STREET_TYPE_MAP.keys(), key=len, reverse=True)
)


#-----------------------------------------------------------------------------
# build_key_map()
#-----------------------------------------------------------------------------
def build_key_map(fieldnames):
    """Build a case-insensitive header lookup: UPPER(name) -> original name.

    When multiple headers collide after uppercasing (e.g. residential ``City``
    and later jurisdiction code ``CITY``), keep the **first** occurrence so the
    address-field value is preferred.
    """
    key_map = {}
    for name in fieldnames or []:
        if name is None:
            continue
        key = (name or '').strip().upper()
        if not key or key in key_map:
            continue
        key_map[key] = name
    return key_map


#-----------------------------------------------------------------------------
# resolve_field()
#-----------------------------------------------------------------------------
def resolve_field(key_map, *candidates):
    """Return the original header for the first matching candidate name."""
    for candidate in candidates:
        key = (candidate or '').strip().upper()
        if key in key_map:
            return key_map[key]
    return None


#-----------------------------------------------------------------------------
# get_field_value()
#-----------------------------------------------------------------------------
def get_field_value(record, field_name):
    """Return a stripped string value for a field, or '' if missing."""
    if not field_name:
        return ''
    value = record.get(field_name, '')
    if value is None:
        return ''
    return str(value).strip()


#-----------------------------------------------------------------------------
# read_csv_records()
#-----------------------------------------------------------------------------
def read_csv_records(pathname):
    """Read a CSV file with robust encoding handling.

    Returns:
        (records, encoding_name) on success, or (None, None) on failure.
    """
    candidate_encodings = ['utf-8', 'cp1252', 'utf-8-sig', 'latin-1']
    used_encoding = None
    records = []

    for enc in candidate_encodings:
        records = []
        try:
            with open(pathname, 'r', encoding=enc, errors='strict', newline='') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
                for record in reader:
                    records.append(record)
            used_encoding = enc
            break
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"File not found: {pathname}")
            return None, None
        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Error reading file {pathname} with encoding {enc}: {exc}")
            return None, None

    if used_encoding is None:
        records = []
        try:
            with open(pathname, 'r', encoding='utf-8', errors='replace', newline='') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
                for record in reader:
                    records.append(record)
            used_encoding = 'utf-8 (replace)'
        except FileNotFoundError:
            print(f"File not found: {pathname}")
            return None, None
        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Failed to read file {pathname} even with replacement errors: {exc}")
            return None, None

    return records, used_encoding


#-----------------------------------------------------------------------------
# normalize_text()
#-----------------------------------------------------------------------------
def normalize_text(text):
    """Uppercase and collapse whitespace."""
    if not text:
        return ''
    return ' '.join(str(text).upper().split())


#-----------------------------------------------------------------------------
# normalize_zip()
#-----------------------------------------------------------------------------
def normalize_zip(zip_code):
    """Return the 5-digit ZIP portion when possible."""
    if not zip_code:
        return ''
    digits = re.sub(r'[^0-9]', '', str(zip_code))
    if len(digits) >= 5:
        return digits[:5]
    return digits


#-----------------------------------------------------------------------------
# normalize_unit()
#-----------------------------------------------------------------------------
def normalize_unit(unit):
    """Normalize unit designators (strip APT/UNIT/# prefixes)."""
    if not unit:
        return ''
    text = normalize_text(unit)
    text = text.replace('#', ' ').replace('.', ' ').replace(',', ' ')
    text = ' '.join(text.split())
    if not text:
        return ''

    parts = text.split()
    while parts and parts[0] in _UNIT_TYPE_TOKENS:
        parts = parts[1:]
    if parts and parts[0] in {'NO', 'NUM', 'NUMBER'}:
        parts = parts[1:]
    return ' '.join(parts)


#-----------------------------------------------------------------------------
# normalize_street_tokens()
#-----------------------------------------------------------------------------
def normalize_street_tokens(street_text):
    """Normalize street direction, type, highway, and ordinal tokens.

    Aligns common Travis County voter-roll spellings with TCAD situs forms, e.g.:

    * ``N IH 35`` / ``N INTERSTATE HY 35`` → ``N IH 35``
    * ``N FM 620 RD`` / ``N RANCH RD 620`` → ``N FM 620``
    * ``W US 290 HWY`` / ``W U S HY 290`` → ``W US 290``
    * ``E 21ST ST`` / ``E 21 ST`` → ``E 21 ST``
    * ``MC NEIL DR`` / ``MCNEIL DR`` → ``MCNEIL DR``
    * ``NORTH PLZ`` / ``NORTH PLAZA`` → ``N PLAZA``
    """
    text = normalize_text(street_text)
    if not text:
        return ''

    text = re.sub(r'[^A-Z0-9\s]', ' ', text)
    text = ' '.join(text.split())
    if not text:
        return ''

    parts = text.split()
    normalized = []
    for part in parts:
        if part in _DIRECTION_MAP:
            normalized.append(_DIRECTION_MAP[part])
        elif part in _STREET_TYPE_MAP:
            normalized.append(_STREET_TYPE_MAP[part])
        else:
            normalized.append(part)

    # Drop bound indicators not present in TCAD situs streets.
    cleaned = [part for part in normalized if part not in {'NB', 'SB', 'EB', 'WB'}]
    text = ' '.join(cleaned)
    if not text:
        return ''

    # Mc / Mac spacing: "MC NEIL" / "MC KINNEY" → "MCNEIL" / "MCKINNEY".
    text = re.sub(r'\bMC\s+', 'MC', text)
    text = re.sub(r'\bMAC\s+', 'MAC', text)

    # Spaced single-letter highway prefixes used in some TCAD rows.
    text = re.sub(r'\bU\s+S\b', 'US', text)
    text = re.sub(r'\bF\s+M\b', 'FM', text)
    text = re.sub(r'\bI\s+H\b', 'IH', text)
    text = re.sub(r'\bS\s+H\b', 'SH', text)
    text = re.sub(r'\bR\s+R\b', 'RR', text)

    # Interstate variants → IH <n>
    text = re.sub(r'\bINTERSTATE\s+(?:HWY\s+)?(\d+)\b', r'IH \1', text)
    text = re.sub(r'\bIH(\d+)\b', r'IH \1', text)
    text = re.sub(r'\bI\s+(\d+)\b', r'IH \1', text)

    # State highway variants → SH <n>
    text = re.sub(r'\bSTATE\s+HWY\s+(\d+)\b', r'SH \1', text)
    text = re.sub(r'\bSH(\d+)\b', r'SH \1', text)

    # US highway variants → US <n> (drop redundant HWY token)
    text = re.sub(r'\bUS\s+HWY\s+(\d+)\b', r'US \1', text)
    text = re.sub(r'\bUS\s+(\d+)\s+HWY\b', r'US \1', text)
    text = re.sub(r'\bUS(\d+)\b', r'US \1', text)

    # Farm-to-Market / Ranch Road variants → FM <n>
    text = re.sub(r'\bFM\s+RANCH\s+RD\s+(\d+)\b', r'FM \1', text)
    text = re.sub(r'\bRANCH\s+ROAD\s+(\d+)\b', r'FM \1', text)
    text = re.sub(r'\bRANCH\s+RD\s+(\d+)\b', r'FM \1', text)
    text = re.sub(r'\bFM\s+RD\s+(\d+)\b', r'FM \1', text)
    text = re.sub(r'\bFM\s+(\d+)\s+RD\b', r'FM \1', text)
    text = re.sub(r'\b(\d+)\s+RANCH\s+RD\b', r'FM \1', text)
    text = re.sub(r'\bRANCH\s+(\d+)\s+RD\b', r'FM \1', text)
    text = re.sub(r'\b([NSEW])\s+(\d+)\s+RANCH\s+RD\b', r'\1 FM \2', text)

    # Local aliases
    text = re.sub(r'\bCAPITAL\s+OF\s+TX\s+HWY\b', 'CAPITAL OF TEXAS HWY', text)
    text = re.sub(r'\bMOPAC\s+EXPRESSWAY\b', 'MOPAC EXPY', text)
    text = re.sub(r'\bMOPAC\s+EXPWY\b', 'MOPAC EXPY', text)
    text = re.sub(r'\bMARTIN\s+LUTHER\s+KING\b', 'M L KING', text)
    text = re.sub(r'\bMLK\b', 'M L KING', text)

    # Ordinal street names: TCAD often stores street="21", suffix="ST" while the
    # voter roll uses "21ST ST". Collapse 21ST/1ST/2ND/3RD/4TH → digits only.
    text = re.sub(r'\b(\d+)(?:ST|ND|RD|TH)\b', r'\1', text)

    return ' '.join(text.split())


#-----------------------------------------------------------------------------
# street_without_leading_direction()
#-----------------------------------------------------------------------------
def street_without_leading_direction(street):
    """Return street with a leading N/S/E/W stripped, when safe to do so.

    Used as a match fallback when property situs omits a pre-direction that the
    voter roll includes (e.g. ``W WELLS BRANCH PKWY`` vs ``WELLS BRANCH PKWY``).

    Does **not** strip direction from numbered streets (``E 6 ST``) or highway
    designations (``N IH 35``), where direction is part of the identity.
    """
    parts = (street or '').split()
    if len(parts) < 2:
        return ''
    if parts[0] not in _LEADING_DIRECTIONS:
        return ''
    rest = parts[1:]
    if not rest:
        return ''
    if rest[0].isdigit() or rest[0] in _HIGHWAY_NAME_TOKENS:
        return ''
    return ' '.join(rest)


#-----------------------------------------------------------------------------
# street_without_trailing_type()
#-----------------------------------------------------------------------------
def street_without_trailing_type(street):
    """Return street with a trailing street-type token stripped, when present.

    Used as a match fallback when the voter roll omits a type that TCAD stores
    as ``situs_street_suffix`` (e.g. ``BOB HARRISON`` vs ``BOB HARRISON ST``).
    """
    parts = (street or '').split()
    if len(parts) < 2:
        return ''
    if parts[-1] not in _STRIP_TRAILING_STREET_TYPES:
        return ''
    return ' '.join(parts[:-1])


#-----------------------------------------------------------------------------
# street_match_variants()
#-----------------------------------------------------------------------------
def street_match_variants(street):
    """Return ordered (street, level_suffix) variants for indexing and lookup.

    More specific forms are listed first. Fallbacks cover omitted pre-direction
    and omitted street type (common voter-roll vs TCAD differences).
    """
    base = normalize_street_tokens(street)
    if not base:
        return []

    variants = []
    seen = set()

    def add(candidate, level_suffix):
        if not candidate or candidate in seen:
            return
        seen.add(candidate)
        variants.append((candidate, level_suffix))

    add(base, '')
    no_type = street_without_trailing_type(base)
    add(no_type, '_no_type')
    no_dir = street_without_leading_direction(base)
    add(no_dir, '_no_dir')
    if no_dir:
        add(street_without_trailing_type(no_dir), '_no_dir_no_type')
    return variants


#-----------------------------------------------------------------------------
# compose_street()
#-----------------------------------------------------------------------------
def compose_street(prefix, name, suffix, name2=''):
    """Compose and normalize a street string from component parts."""
    parts = [prefix or '', name or '', name2 or '', suffix or '']
    return normalize_street_tokens(' '.join(p for p in parts if p))


#-----------------------------------------------------------------------------
# normalize_house_number()
#-----------------------------------------------------------------------------
def normalize_house_number(number):
    """Return the leading house-number token."""
    text = normalize_text(number)
    if not text:
        return ''
    num_parts = text.replace('#', ' ').split()
    num = num_parts[0] if num_parts else ''
    return re.sub(r'[^0-9A-Z]', '', num)


#-----------------------------------------------------------------------------
# make_address_key()
#-----------------------------------------------------------------------------
def make_address_key(number, street, unit='', zip_code=''):
    """Build a normalized address key from components.

    Empty optional parts are omitted. Returns '' if number or street is missing.
    """
    num = normalize_house_number(number)
    st = normalize_street_tokens(street)
    un = normalize_unit(unit)
    zc = normalize_zip(zip_code)

    if not num or not st:
        return ''

    parts = [num, st]
    if un:
        parts.append(un)
    if zc:
        parts.append(zc)
    return ' '.join(parts)


#-----------------------------------------------------------------------------
# parse_freeform_address()
#-----------------------------------------------------------------------------
def parse_freeform_address(address, fallback_city='', fallback_zip=''):
    """Parse a free-form residential address into components.

    Returns dict with keys: number, street, unit, city, zip, display
    """
    display = ' '.join(str(address or '').split())
    text = normalize_text(address)
    if not text:
        return {
            'number': '',
            'street': '',
            'unit': '',
            'city': normalize_text(fallback_city),
            'zip': normalize_zip(fallback_zip),
            'display': display,
        }

    zip_code = normalize_zip(fallback_zip)
    zip_match = re.search(r'\b(\d{5})(?:\s*-\s*\d{4})?\s*$', text)
    if zip_match:
        zip_code = zip_match.group(1)
        text = text[:zip_match.start()].strip()

    state_match = re.search(r'\b(TX|TEXAS)\s*$', text)
    if state_match:
        text = text[:state_match.start()].strip()

    city = normalize_text(fallback_city)
    for city_name in _KNOWN_CITIES:
        if text.endswith(city_name):
            city = city_name
            text = text[:-len(city_name)].strip()
            break

    text = text.strip(' -,').strip()

    number = ''
    number_match = re.match(r'^(\d+[A-Z]?)\b\s*(.*)$', text)
    if number_match:
        number = number_match.group(1)
        text = number_match.group(2).strip()

    unit = ''
    unit_match = re.search(
        r'\b(?:APT|APARTMENT|UNIT|STE|SUITE|BLDG|BUILDING|FL|FLOOR|RM|ROOM)\s*#?\s*([A-Z0-9-]+)\b',
        text,
    )
    if unit_match:
        unit = unit_match.group(1)
        text = (text[:unit_match.start()] + ' ' + text[unit_match.end():]).strip()
    else:
        hash_match = re.search(r'#\s*([A-Z0-9-]+)\b', text)
        if hash_match:
            unit = hash_match.group(1)
            text = (text[:hash_match.start()] + ' ' + text[hash_match.end():]).strip()

    if not unit:
        trailing = re.search(
            rf'^(.*\b(?:{_STREET_TYPE_PATTERN}))\s+([A-Z0-9-]{{1,8}})$',
            text,
        )
        if trailing:
            maybe_unit = trailing.group(2)
            if (
                maybe_unit not in _DIRECTION_MAP
                and maybe_unit not in _STREET_TYPE_MAP
                and maybe_unit not in _STATE_TOKENS
                and maybe_unit not in {'NB', 'SB', 'EB', 'WB'}
            ):
                unit = maybe_unit
                text = trailing.group(1).strip()

    street = normalize_street_tokens(text)

    return {
        'number': number,
        'street': street,
        'unit': normalize_unit(unit),
        'city': city,
        'zip': zip_code,
        'display': display,
    }


#-----------------------------------------------------------------------------
# format_address_display()
#-----------------------------------------------------------------------------
def format_address_display(number, street, unit='', city='', zip_code=''):
    """Build a human-readable address string from components."""
    parts = []
    head = ' '.join(p for p in [number, street] if p)
    if head:
        parts.append(head)
    if unit:
        parts.append(f"UNIT {unit}")
    tail = ' '.join(p for p in [city, zip_code] if p)
    if tail:
        parts.append(tail)
    return ' '.join(parts).strip()


#-----------------------------------------------------------------------------
# google_maps_url()
#-----------------------------------------------------------------------------
def google_maps_url(address_text):
    """Build a Google Maps search URL for a free-form address string.

    Returns '' when the address is empty so confidential/blank rows stay blank.
    Non-empty URLs end with a trailing space so the CSV field sits cleanly
    before the next comma separator when opened in spreadsheet tools.
    """
    query = ' '.join(str(address_text or '').split())
    if not query:
        return ''
    return f"https://www.google.com/maps/search/?api=1&query={quote_plus(query)} "


#-----------------------------------------------------------------------------
# is_residential_state_cd()
#-----------------------------------------------------------------------------
def is_residential_state_cd(imprv_state_cd, improv_type_desc=''):
    """Return True if the property should be treated as residential.

    A property is residential when:
      - improv_type_desc contains an always-residential marker
        (see ALWAYS_RESIDENTIAL_IMPROV_TYPE_MARKERS: DWELLING, CONDO, CONDOS), or
      - any imprv_state_cd token starts with A, B, E, or O, or
      - any imprv_state_cd token is F1 and improv_type_desc contains a known
        residential-use commercial marker (see F1_RESIDENTIAL_IMPROV_TYPE_MARKERS), or
      - any imprv_state_cd token is M1 and improv_type_desc contains a known
        residential-use marker (see M1_RESIDENTIAL_IMPROV_TYPE_MARKERS).

    Empty/missing codes with no always-residential description are non-residential.
    """
    desc = (improv_type_desc or '').upper()
    if desc:
        for marker in ALWAYS_RESIDENTIAL_IMPROV_TYPE_MARKERS:
            if marker in desc:
                return True

    text = (imprv_state_cd or '').strip()
    if not text:
        return False

    codes = []
    for token in text.split(';'):
        code = token.strip().upper()
        if code:
            codes.append(code)

    if not codes:
        return False

    for code in codes:
        if code[0] in RESIDENTIAL_STATE_PREFIXES:
            return True

    if desc:
        if any(code == 'F1' for code in codes):
            for marker in F1_RESIDENTIAL_IMPROV_TYPE_MARKERS:
                if marker in desc:
                    return True
        if any(code == 'M1' for code in codes):
            for marker in M1_RESIDENTIAL_IMPROV_TYPE_MARKERS:
                if marker in desc:
                    return True
    return False


#-----------------------------------------------------------------------------
# property_is_preferred()
#-----------------------------------------------------------------------------
def property_is_preferred(candidate, existing):
    """Prefer property rows that have a residential state code."""
    if existing is None:
        return True
    cand_res = is_residential_state_cd(
        candidate.get('imprv_state_cd', ''),
        candidate.get('improv_type_desc', ''),
    )
    exist_res = is_residential_state_cd(
        existing.get('imprv_state_cd', ''),
        existing.get('improv_type_desc', ''),
    )
    if cand_res and not exist_res:
        return True
    return False


#-----------------------------------------------------------------------------
# load_property_indexes()
#-----------------------------------------------------------------------------
def load_property_indexes(property_data_path):
    """Load property CSV and build multi-level normalized address indexes.

    Returns:
        (indexes, property_count, encoding) where indexes is a dict of
        key-level -> {normalized_key: property_info}.
        On failure returns (None, 0, None).
    """
    print(f"Reading property data from '{property_data_path}'...")
    records, used_encoding = read_csv_records(property_data_path)
    if records is None:
        return None, 0, None

    print(f"Read in {len(records)} properties from '{property_data_path}' (encoding={used_encoding})")

    if not records:
        print(f"No property records found in '{property_data_path}'.")
        return None, 0, used_encoding

    key_map = build_key_map(records[0].keys())
    required = {
        'prop_id': resolve_field(key_map, 'PROP_ID'),
        'situs_num': resolve_field(key_map, 'SITUS_NUM'),
        'situs_street': resolve_field(key_map, 'SITUS_STREET'),
    }
    missing = [name for name, field in required.items() if field is None]
    if missing:
        print(f"Property file missing required columns: {', '.join(missing)}")
        return None, 0, used_encoding

    fields = {
        'prop_id': required['prop_id'],
        'situs_num': required['situs_num'],
        'situs_street_prefx': resolve_field(key_map, 'SITUS_STREET_PREFX', 'SITUS_STREET_PREFIX'),
        'situs_street': required['situs_street'],
        'situs_street_suffix': resolve_field(key_map, 'SITUS_STREET_SUFFIX'),
        'situs_unit': resolve_field(key_map, 'SITUS_UNIT'),
        'situs_city': resolve_field(key_map, 'SITUS_CITY'),
        'situs_zip': resolve_field(key_map, 'SITUS_ZIP'),
        'improv_type_cd': resolve_field(key_map, 'IMPROV_TYPE_CD'),
        'improv_type_desc': resolve_field(key_map, 'IMPROV_TYPE_DESC'),
        'imprv_state_cd': resolve_field(key_map, 'IMPRV_STATE_CD', 'IMPROV_STATE_CD'),
    }

    level_names = (
        'num_street_unit_zip',
        'num_street_zip',
        'num_street_unit',
        'num_street',
    )
    indexes = {name: {} for name in level_names}
    indexed_count = 0
    skipped_no_address = 0

    for record in records:
        prop_id = get_field_value(record, fields['prop_id'])
        number = get_field_value(record, fields['situs_num'])
        prefix = get_field_value(record, fields['situs_street_prefx'])
        street_name = get_field_value(record, fields['situs_street'])
        suffix = get_field_value(record, fields['situs_street_suffix'])
        unit = get_field_value(record, fields['situs_unit'])
        city = get_field_value(record, fields['situs_city'])
        zip_code = get_field_value(record, fields['situs_zip'])
        improv_type_cd = get_field_value(record, fields['improv_type_cd'])
        improv_type_desc = get_field_value(record, fields['improv_type_desc'])
        imprv_state_cd = get_field_value(record, fields['imprv_state_cd'])

        street = compose_street(prefix, street_name, suffix)
        if not normalize_house_number(number) or not street:
            skipped_no_address += 1
            continue

        unit_norm = normalize_unit(unit)
        zip_norm = normalize_zip(zip_code)
        display = format_address_display(number, street, unit_norm, city, zip_norm)
        prop_info = {
            'prop_id': prop_id,
            'number': normalize_house_number(number),
            'street': street,
            'unit': unit_norm,
            'city': city,
            'zip': zip_norm,
            'display': display,
            'improv_type_cd': improv_type_cd,
            'improv_type_desc': improv_type_desc,
            'imprv_state_cd': imprv_state_cd,
        }

        # Index full street plus fallbacks: omit situs suffix / trailing type so
        # voter "BOB HARRISON" can match TCAD "BOB HARRISON" + suffix "ST".
        street_forms = []
        seen_streets = set()
        for street_form, _label in street_match_variants(street):
            if street_form not in seen_streets:
                seen_streets.add(street_form)
                street_forms.append(street_form)
        if suffix:
            street_no_suffix = compose_street(prefix, street_name, '')
            if street_no_suffix and street_no_suffix not in seen_streets:
                street_forms.append(street_no_suffix)

        stored = False
        for street_form in street_forms:
            key_levels = {
                'num_street_unit_zip': (
                    make_address_key(number, street_form, unit, zip_code)
                    if unit_norm and zip_norm else ''
                ),
                'num_street_zip': (
                    make_address_key(number, street_form, '', zip_code)
                    if zip_norm else ''
                ),
                'num_street_unit': (
                    make_address_key(number, street_form, unit, '')
                    if unit_norm else ''
                ),
                'num_street': make_address_key(number, street_form, '', ''),
            }
            for level in level_names:
                key = key_levels[level]
                if not key:
                    continue
                existing = indexes[level].get(key)
                if existing is None or property_is_preferred(prop_info, existing):
                    indexes[level][key] = prop_info
                stored = True

        if stored:
            indexed_count += 1
        else:
            skipped_no_address += 1

    print(
        f"Indexed {indexed_count} properties "
        f"(skipped {skipped_no_address} without usable situs address)"
    )
    for level in level_names:
        print(f"  Lookup keys [{level}]: {len(indexes[level])}")

    return indexes, len(records), used_encoding


#-----------------------------------------------------------------------------
# match_property()
#-----------------------------------------------------------------------------
def match_property(indexes, number, street, unit='', zip_code=''):
    """Match address components against property indexes using fallback keys.

    Lookup order (most specific first):

    1. number + street + unit + ZIP
    2. number + street + ZIP
    3. number + street + unit
    4. number + street

    The same sequence is retried across street variants:

    - full normalized street
    - trailing street type stripped (``BOB HARRISON ST`` → ``BOB HARRISON``)
    - leading N/S/E/W stripped from named streets (not numbered/highway)
    - both direction and type stripped

    That covers common voter-roll vs TCAD differences (omitted pre-direction or
    street type / situs suffix).

    Returns (property_info_or_None, match_level_or_None).
    """
    if not indexes:
        return None, None

    level_names = (
        'num_street_unit_zip',
        'num_street_zip',
        'num_street_unit',
        'num_street',
    )

    unit_norm = normalize_unit(unit)
    zip_norm = normalize_zip(zip_code)

    for street_try, level_suffix in street_match_variants(street):
        key_candidates = {
            'num_street_unit_zip': (
                make_address_key(number, street_try, unit, zip_code)
                if unit_norm and zip_norm else ''
            ),
            'num_street_zip': (
                make_address_key(number, street_try, '', zip_code)
                if zip_norm else ''
            ),
            'num_street_unit': (
                make_address_key(number, street_try, unit, '')
                if unit_norm else ''
            ),
            'num_street': make_address_key(number, street_try, '', ''),
        }

        for level in level_names:
            key = key_candidates[level]
            if not key:
                continue
            prop = indexes[level].get(key)
            if prop is not None:
                return prop, f"{level}{level_suffix}"

    return None, None


#-----------------------------------------------------------------------------
# detect_voter_fields()
#-----------------------------------------------------------------------------
def detect_voter_fields(key_map):
    """Detect voter CSV field names across known Travis County layouts."""
    fields = {
        'vuid': resolve_field(key_map, 'VUID', 'VUIDNO'),
        'fullname': resolve_field(key_map, 'NAME', 'FULL_NAME'),
        'last_name': resolve_field(key_map, 'LAST_NAME', 'LSTNAM'),
        'first_name': resolve_field(key_map, 'FIRST_NAME', 'FSTNAM'),
        'middle_name': resolve_field(key_map, 'MIDDLE_NAME', 'MIDNAM'),
        'dob': resolve_field(key_map, 'DATE_OF_BIRTH', 'DOB'),
        'residential_address': resolve_field(
            key_map,
            'RESIDENTIAL ADDRESS',
            'RESIDENTIAL_ADDRESS',
            'RES_ADDR',
            'RESIDENCE_ADDRESS',
            'ADDRESS',
        ),
        'street_number': resolve_field(
            key_map,
            'STREET NUMBER 1',
            'STREET_NUMBER_1',
            'STREET NUMBER',
            'STREET_NUMBER',
            'SITUS_NUM',
            'HOUSE_NUMBER',
            'ADDR_NUM',
        ),
        'pre_direction': resolve_field(
            key_map,
            'PRE-DIRECTION',
            'PRE_DIRECTION',
            'PREDIRECTION',
            'STREET_PREFIX',
            'STRDIR',
        ),
        'street_name': resolve_field(
            key_map,
            'STREET NAME 1',
            'STREET_NAME_1',
            'STREET NAME',
            'STREET_NAME',
            'STRNAM',
        ),
        'street_name_2': resolve_field(
            key_map,
            'STREET NAME 2',
            'STREET_NAME_2',
        ),
        'street_type': resolve_field(
            key_map,
            'STREET TYPE',
            'STREET_TYPE',
            'STREET SUFFIX',
            'STREET_SUFFIX',
            'STRTYP',
        ),
        'post_direction': resolve_field(
            key_map,
            'POST-DIRECTION',
            'POST_DIRECTION',
            'POSTDIRECTION',
        ),
        'unit': resolve_field(
            key_map,
            'UNIT',
            'UNIT NUMBER',
            'UNIT_NUMBER',
            'APT',
            'APARTMENT',
        ),
        'unit_type': resolve_field(
            key_map,
            'UNIT TYPE',
            'UNIT_TYPE',
        ),
        # Prefer explicit residential city headers. Bare CITY is last because some
        # Travis extracts also include a later jurisdiction-code column named CITY
        # (after US CONGRESS); build_key_map keeps the earlier residential City.
        'city': resolve_field(
            key_map,
            'RESIDENT_CITY',
            'RESIDENCE_CITY',
            'RES_CITY',
            'CITY',
        ),
        'zip': resolve_field(
            key_map,
            'ZIP CODE 5',
            'ZIP_CODE_5',
            'RESIDENT_ZIP_CODE',
            'ZIP',
            'ZIPCODE',
            'ZIP_CODE',
            'RES_ZIP',
        ),
        'zip4': resolve_field(
            key_map,
            'ZIP CODE 4',
            'ZIP_CODE_4',
        ),
    }
    return fields


#-----------------------------------------------------------------------------
# extract_voter_name()
#-----------------------------------------------------------------------------
def extract_voter_name(record, fields):
    """Return the best available voter display name."""
    full_name = get_field_value(record, fields['fullname'])
    if full_name:
        return full_name

    last_name = get_field_value(record, fields['last_name'])
    first_name = get_field_value(record, fields['first_name'])
    middle_name = get_field_value(record, fields['middle_name'])

    if last_name or first_name or middle_name:
        given = ' '.join(p for p in [first_name, middle_name] if p).strip()
        if last_name and given:
            return f"{last_name}, {given}"
        return last_name or given

    return ''


#-----------------------------------------------------------------------------
# is_confidential_address_text()
#-----------------------------------------------------------------------------
def is_confidential_address_text(*values):
    """Return True if any value looks like a confidential-address redaction.

    Travis County confidential voters list residential address fields as one or
    more groupings of three asterisks, e.g. ``***`` or
    ``*** *** *** *** *** -***``.
    """
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        if '***' in text:
            return True
    return False


#-----------------------------------------------------------------------------
# is_confidential_address()
#-----------------------------------------------------------------------------
def is_confidential_address(address):
    """Return True if a parsed voter address dict is confidential / redacted."""
    if not address:
        return False
    if address.get('confidential'):
        return True
    return is_confidential_address_text(
        address.get('display', ''),
        address.get('number', ''),
        address.get('street', ''),
        address.get('unit', ''),
        address.get('city', ''),
        address.get('zip', ''),
    )


#-----------------------------------------------------------------------------
# extract_voter_address()
#-----------------------------------------------------------------------------
def extract_voter_address(record, fields):
    """Extract normalized address components from a voter record.

    Prefers component columns when a street number is present; otherwise
    parses the free-form residential address field.

    Confidential / redacted addresses (``***`` groupings) are flagged with
    ``confidential=True`` and left largely unparsed.
    """
    city = get_field_value(record, fields['city'])
    zip_code = get_field_value(record, fields['zip'])
    freeform = get_field_value(record, fields['residential_address'])

    number = get_field_value(record, fields['street_number'])
    pre = get_field_value(record, fields['pre_direction'])
    name1 = get_field_value(record, fields['street_name'])
    name2 = get_field_value(record, fields['street_name_2'])
    street_type = get_field_value(record, fields['street_type'])
    post = get_field_value(record, fields['post_direction'])
    unit = get_field_value(record, fields['unit'])
    unit_type = get_field_value(record, fields['unit_type'])

    if is_confidential_address_text(
        freeform, number, pre, name1, name2, street_type, post, unit, unit_type, city, zip_code
    ):
        display = freeform or number or '***'
        return {
            'number': '',
            'street': '',
            'unit': '',
            'city': '',
            'zip': '',
            'display': ' '.join(str(display).split()),
            'confidential': True,
        }

    if number and (name1 or name2 or street_type or pre):
        street = compose_street(pre, name1, street_type, name2=name2)
        if post:
            street = normalize_street_tokens(f"{street} {post}")

        unit_combined = unit
        if unit_type and unit and normalize_text(unit_type) not in normalize_text(unit):
            unit_combined = f"{unit_type} {unit}"
        elif unit_type and not unit:
            unit_combined = unit_type

        parsed_unit = normalize_unit(unit_combined)
        if not parsed_unit and freeform:
            parsed = parse_freeform_address(freeform, city, zip_code)
            parsed_unit = parsed.get('unit', '')
            if not zip_code:
                zip_code = parsed.get('zip', '')
            if not city:
                city = parsed.get('city', '')

        display = freeform or format_address_display(
            number, street, parsed_unit, city, normalize_zip(zip_code)
        )
        return {
            'number': normalize_house_number(number),
            'street': street,
            'unit': parsed_unit,
            'city': normalize_text(city),
            'zip': normalize_zip(zip_code),
            'display': ' '.join(str(display).split()),
            'confidential': False,
        }

    if freeform:
        parsed = parse_freeform_address(freeform, city, zip_code)
        if city:
            parsed['city'] = normalize_text(city)
        if zip_code:
            parsed['zip'] = normalize_zip(zip_code)
        parsed['confidential'] = False
        return parsed

    if name1 or name2 or street_type:
        street = compose_street(pre, name1, street_type, name2=name2)
        parsed_unit = normalize_unit(unit)
        display = format_address_display(number, street, parsed_unit, city, normalize_zip(zip_code))
        return {
            'number': normalize_house_number(number),
            'street': street,
            'unit': parsed_unit,
            'city': normalize_text(city),
            'zip': normalize_zip(zip_code),
            'display': display,
            'confidential': False,
        }

    return {
        'number': '',
        'street': '',
        'unit': '',
        'city': normalize_text(city),
        'zip': normalize_zip(zip_code),
        'display': '',
        'confidential': False,
    }


#-----------------------------------------------------------------------------
# write_csv()
#-----------------------------------------------------------------------------
def write_csv(pathname, fieldnames, rows):
    """Write rows to CSV with UTF-8 encoding."""
    with open(pathname, 'w', encoding='utf-8', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


#-----------------------------------------------------------------------------
# process_registered_voter_list()
#-----------------------------------------------------------------------------
def process_registered_voter_list(pathname, property_data_path):
    """Process the specified voter registration list against property data."""

    print(f"Reading voter registration data from '{pathname}'...")
    registered_voters, used_encoding = read_csv_records(pathname)
    if registered_voters is None:
        return False

    print(f"Read in {len(registered_voters)} voters from '{pathname}' (encoding={used_encoding})")

    if not registered_voters:
        print(f"No voter records found in '{pathname}'.")
        return False

    property_indexes, _property_count, _prop_encoding = load_property_indexes(property_data_path)
    if property_indexes is None:
        return False

    first_record = registered_voters[0]
    key_map = build_key_map(first_record.keys())
    fields = detect_voter_fields(key_map)

    if fields['vuid'] is None:
        print(f"No 'VUID' or 'VUIDNO' header found in '{pathname}'.")
        return False

    vuids = {}
    num_duplicates = 0
    num_skipped = 0

    for record in registered_voters:
        vuid_number = get_field_value(record, fields['vuid'])
        if not vuid_number:
            num_skipped += 1
            continue

        if vuid_number in vuids:
            num_duplicates += 1
            continue

        name = extract_voter_name(record, fields)
        address = extract_voter_address(record, fields)

        vuids[vuid_number] = {
            'vuid': vuid_number,
            'name': name,
            'address': address,
            'record': record,
        }

    print(f"Found {num_duplicates} duplicate entries in '{pathname}'")
    if num_skipped:
        print(
            f"Skipped {num_skipped} records with missing/empty "
            f"'{fields['vuid']}' values in '{pathname}'"
        )
    print(f"There are {len(vuids)} unique voters in '{pathname}'")

    unmatched_rows = []
    nonresidential_rows = []
    matched_group_counts = Counter()
    nonresidential_group_counts = Counter()
    match_level_counts = Counter()

    matched_count = 0
    residential_count = 0
    nonresidential_count = 0
    unmatched_count = 0
    confidential_count = 0

    for vuid_number, voter in vuids.items():
        addr = voter['address']

        # Confidential voters have redacted residential addresses (*** groupings).
        # Do not attempt property matching and do not write them to the unmatched
        # or non-residential detail reports.
        if is_confidential_address(addr):
            confidential_count += 1
            continue

        prop, match_level = match_property(
            property_indexes,
            addr.get('number', ''),
            addr.get('street', ''),
            addr.get('unit', ''),
            addr.get('zip', ''),
        )

        display_address = addr.get('display') or format_address_display(
            addr.get('number', ''),
            addr.get('street', ''),
            addr.get('unit', ''),
            addr.get('city', ''),
            addr.get('zip', ''),
        )

        if prop is None:
            unmatched_count += 1
            unmatched_rows.append({
                'VUID': vuid_number,
                'Name': voter['name'],
                'Address': display_address,
                'StreetNumber': addr.get('number', ''),
                'Street': addr.get('street', ''),
                'Unit': addr.get('unit', ''),
                'City': addr.get('city', ''),
                'Zip': addr.get('zip', ''),
            })
            continue

        matched_count += 1
        match_level_counts[match_level] += 1

        improv_type_cd = prop.get('improv_type_cd', '')
        improv_type_desc = prop.get('improv_type_desc', '')
        imprv_state_cd = prop.get('imprv_state_cd', '')

        group_key = (
            improv_type_desc if improv_type_desc else '(blank improv_type_desc)',
            imprv_state_cd if imprv_state_cd else '(blank imprv_state_cd)',
        )
        matched_group_counts[group_key] += 1

        if is_residential_state_cd(imprv_state_cd, improv_type_desc):
            residential_count += 1
        else:
            nonresidential_count += 1
            nonresidential_group_counts[group_key] += 1
            nonresidential_rows.append({
                'VUID': vuid_number,
                'Name': voter['name'],
                'Address': display_address,
                'StreetNumber': addr.get('number', ''),
                'Street': addr.get('street', ''),
                'Unit': addr.get('unit', ''),
                'City': addr.get('city', ''),
                'Zip': addr.get('zip', ''),
                'GoogleMapsURL': google_maps_url(display_address),
                'MatchLevel': match_level or '',
                'PropId': prop.get('prop_id', ''),
                'PropertyAddress': prop.get('display', ''),
                'ImprovTypeCd': improv_type_cd,
                'ImprovTypeDesc': improv_type_desc,
                'ImprvStateCd': imprv_state_cd,
            })

    # Sort non-residential detail: improv type description, then address.
    # Rows with a blank improvement description sort last.
    nonresidential_rows.sort(
        key=lambda row: (
            0 if (row.get('ImprovTypeDesc') or '').strip() else 1,
            (row.get('ImprovTypeDesc') or '').upper(),
            (row.get('Address') or '').upper(),
            row.get('VUID') or '',
        )
    )

    voter_dir = os.path.dirname(os.path.abspath(pathname))
    voter_base = os.path.splitext(os.path.basename(pathname))[0]

    unmatched_path = os.path.join(voter_dir, f"{voter_base}_unmatched_properties.csv")
    nonres_path = os.path.join(voter_dir, f"{voter_base}_nonresidential_matches.csv")
    matched_summary_path = os.path.join(voter_dir, f"{voter_base}_matched_by_improv_type.csv")
    nonres_summary_path = os.path.join(
        voter_dir, f"{voter_base}_nonresidential_matches_improv_types.csv"
    )

    unmatched_fields = [
        'VUID', 'Name', 'Address', 'StreetNumber', 'Street', 'Unit', 'City', 'Zip',
    ]
    nonres_fields = [
        'VUID', 'Name', 'Address', 'StreetNumber', 'Street', 'Unit', 'City', 'Zip',
        'GoogleMapsURL',
        'MatchLevel', 'PropId', 'PropertyAddress',
        'ImprovTypeCd', 'ImprovTypeDesc', 'ImprvStateCd',
    ]
    summary_fields = ['ImprovTypeDesc', 'ImprvStateCd', 'Count']

    write_csv(unmatched_path, unmatched_fields, unmatched_rows)
    write_csv(nonres_path, nonres_fields, nonresidential_rows)

    def build_improv_summary_rows(group_counts):
        """Build sorted improv_type_desc / imprv_state_cd count rows."""
        return [
            {
                'ImprovTypeDesc': desc,
                'ImprvStateCd': state_cd,
                'Count': count,
            }
            for (desc, state_cd), count in sorted(
                group_counts.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1]),
            )
        ]

    summary_rows = build_improv_summary_rows(matched_group_counts)
    nonres_summary_rows = build_improv_summary_rows(nonresidential_group_counts)
    write_csv(matched_summary_path, summary_fields, summary_rows)
    write_csv(nonres_summary_path, summary_fields, nonres_summary_rows)

    print("")
    print("=" * 72)
    print("SUMMARY")
    print("=" * 72)
    print(f"Unique voters processed:              {len(vuids)}")
    print(f"Confidential address (excluded):      {confidential_count}")
    print(f"Matched to property data:             {matched_count}")
    print(f"  Residential (A/B/E state code):     {residential_count}")
    print(f"  Non-residential / blank state code: {nonresidential_count}")
    print(f"Unmatched to property data:           {unmatched_count}")
    print("")

    if match_level_counts:
        print("Matches by lookup level:")
        for level, count in match_level_counts.most_common():
            print(f"  {level}: {count}")
        print("")

    print(f"Total properties that could not be matched: {unmatched_count}")
    print(f"Wrote unmatched detail: {unmatched_path}")
    print(f"Wrote non-residential detail: {nonres_path}")
    print("")

    def print_improv_summary_table(title, rows):
        """Print a count table grouped by improv_type_desc and imprv_state_cd."""
        print(title)
        if not rows:
            print("  (none)")
            return
        desc_width = min(
            40,
            max(len('improv_type_desc'), max(len(r['ImprovTypeDesc']) for r in rows)),
        )
        state_width = min(
            24,
            max(len('imprv_state_cd'), max(len(r['ImprvStateCd']) for r in rows)),
        )
        header = f"  {'Count':>8}  {'imprv_state_cd':<{state_width}}  {'improv_type_desc'}"
        print(header)
        print(f"  {'-' * 8}  {'-' * state_width}  {'-' * desc_width}")
        for row in rows:
            desc = row['ImprovTypeDesc']
            if len(desc) > 60:
                desc = desc[:57] + '...'
            print(f"  {row['Count']:>8}  {row['ImprvStateCd']:<{state_width}}  {desc}")

    print_improv_summary_table(
        "Matched properties grouped by improv_type_desc and imprv_state_cd:",
        summary_rows,
    )
    print("")
    print(f"Wrote matched grouping summary: {matched_summary_path}")
    print("")

    print_improv_summary_table(
        "Non-residential matches grouped by improv_type_desc and imprv_state_cd:",
        nonres_summary_rows,
    )
    print("")
    print(f"Wrote non-residential improv-type summary: {nonres_summary_path}")
    print(f"Total non-residential flagged voters: {nonresidential_count}")

    return True


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    if len(sys.argv) < 3:
        print(
            "Usage: python process_registered_voters_with_property_data.py "
            "<registered_voter_list_file_path> <property_data_file_path>"
        )
        return False

    voter_list_pathname = sys.argv[1]
    property_data_path = sys.argv[2]

    return process_registered_voter_list(voter_list_pathname, property_data_path)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)
