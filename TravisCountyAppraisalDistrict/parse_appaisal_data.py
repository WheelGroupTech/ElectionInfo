#-----------------------------------------------------------------------------
# parse_appaisal_data.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to extract and combine property and improvement data from
# Travis County Appraisal District fixed-width export files.
#-----------------------------------------------------------------------------
#!/usr/bin/env python3
#-----------------------------------------------------------------------------
"""Simple parser for Travis County Appraisal District fixed-width exports.

Reads PROP.TXT and IMP_INFO.TXT from an input directory and writes a CSV with
one row per property containing: prop_id, situs address fields, and the
improvement type code(s) and description(s) (joined with ';' if multiple).

Usage:
    python parse_appaisal_data.py /path/to/export_dir [output.csv]
"""
from __future__ import annotations

import csv
import os
import sys
from typing import Dict, List

TOTAL_EMPTY_SITUS_CITY = 0
TOTAL_EMPTY_SITUS_CITY_FILLED = 0


#-----------------------------------------------------------------------------
# parse_fixed_width()
#-----------------------------------------------------------------------------
def parse_fixed_width(line: str, start: int, end: int) -> str:
    """Return the substring from a fixed-width record.

    The layout specification uses 1-based inclusive positions for start and end.
    This helper converts to Python's 0-based indexing and strips trailing
    whitespace.
    """
    # start and end are 1-based inclusive positions per the layout file
    return line[start - 1:end].rstrip()


#-----------------------------------------------------------------------------
# _normalize_addr()
#-----------------------------------------------------------------------------
def _normalize_addr(text: str) -> str:
    """Normalize address text for loose comparison."""
    return " ".join(text.upper().split())


#-----------------------------------------------------------------------------
# _build_situs_street_key()
#-----------------------------------------------------------------------------
def _build_situs_street_key(
    situs_num: str,
    situs_street_prefx: str,
    situs_street: str,
    situs_street_suffix: str,
) -> str:
    """Build a normalized situs street string used for owner matching."""
    parts = [situs_num, situs_street_prefx, situs_street, situs_street_suffix]
    return _normalize_addr(" ".join(part for part in parts if part))


#-----------------------------------------------------------------------------
# _owner_street_matches()
#-----------------------------------------------------------------------------
def _owner_street_matches(situs_street_key: str, owner_lines: List[str]) -> bool:
    """Return True if the situs street appears in any owner address line."""
    if not situs_street_key:
        return False

    # Prefer matching the street name portion when house number is absent
    # from owner lines, so also try without a leading house number.
    situs_street_only = " ".join(situs_street_key.split()[1:]) if (
        situs_street_key.split() and situs_street_key.split()[0].isdigit()
    ) else situs_street_key

    for line in owner_lines:
        owner_norm = _normalize_addr(line)
        if not owner_norm:
            continue
        if situs_street_key and situs_street_key in owner_norm:
            return True
        if situs_street_only and situs_street_only in owner_norm:
            return True
    return False


#-----------------------------------------------------------------------------
# _fill_situs_city_from_owner()
#-----------------------------------------------------------------------------
def _fill_situs_city_from_owner(
    situs_city: str,
    situs_street_key: str,
    owner_lines: List[str],
    owner_city: str,
) -> str:
    """Fill empty situs_city from an owner city when streets match."""
    if situs_city:
        return situs_city
    owner_city = owner_city.strip()
    if not owner_city:
        return situs_city
    if _owner_street_matches(situs_street_key, owner_lines):
        return owner_city
    return situs_city


# Owner address layouts: three line ranges followed by city range.
# Each range is (start, end) using 1-based inclusive positions.
_OWNER_ADDR_LAYOUTS = (
    ((694, 753), (754, 813), (814, 873), (874, 923)),       # property year
    ((2273, 2332), (2333, 2392), (2393, 2452), (2453, 2502)),  # january 1
)


#-----------------------------------------------------------------------------
# _parse_owner_address()
#-----------------------------------------------------------------------------
def _parse_owner_address(
    line: str,
    layout: tuple[tuple[int, int], tuple[int, int], tuple[int, int], tuple[int, int]],
) -> tuple[List[str], str]:
    """Parse owner address lines and city using a layout of field ranges."""
    line1, line2, line3, city = layout
    owner_lines = [
        parse_fixed_width(line, line1[0], line1[1]).strip(),
        parse_fixed_width(line, line2[0], line2[1]).strip(),
        parse_fixed_width(line, line3[0], line3[1]).strip(),
    ]
    owner_city = parse_fixed_width(line, city[0], city[1]).strip()
    return owner_lines, owner_city


#-----------------------------------------------------------------------------
# _resolve_situs_city()
#-----------------------------------------------------------------------------
def _resolve_situs_city(line: str, situs_city: str, situs_street_key: str) -> str:
    """Fill blank situs_city from matching property-year or Jan 1 owner city."""
    if situs_city:
        return situs_city
    global TOTAL_EMPTY_SITUS_CITY
    TOTAL_EMPTY_SITUS_CITY += 1
    for layout in _OWNER_ADDR_LAYOUTS:
        owner_lines, owner_city = _parse_owner_address(line, layout)
        filled = _fill_situs_city_from_owner(
            situs_city, situs_street_key, owner_lines, owner_city
        )
        if filled:
            global TOTAL_EMPTY_SITUS_CITY_FILLED
            TOTAL_EMPTY_SITUS_CITY_FILLED += 1
            return filled
    return situs_city


#-----------------------------------------------------------------------------
# load_properties()
#-----------------------------------------------------------------------------
def load_properties(prop_path: str) -> Dict[str, Dict[str, str]]:
    """Load PROP.TXT returning a map prop_id -> address fields.

    When situs_city is blank, attempt to fill it from the property-year owner
    or January 1 owner city if the owner street matches the situs street.
    """
    props: Dict[str, Dict[str, str]] = {}
    with open(prop_path, "r", encoding="latin1", errors="replace") as fh:
        for ln in fh:
            if not ln.strip():
                continue
            # Fields per layout (1-based positions)
            prop_id = parse_fixed_width(ln, 1, 12).strip()
            situs_num = parse_fixed_width(ln, 4460, 4474).strip()
            situs_street_prefx = parse_fixed_width(ln, 1040, 1049).strip()
            situs_street = parse_fixed_width(ln, 1050, 1099).strip()
            situs_street_suffix = parse_fixed_width(ln, 1100, 1109).strip()
            situs_unit = parse_fixed_width(ln, 4475, 4479).strip()
            situs_city = parse_fixed_width(ln, 1110, 1139).strip()
            situs_zip = parse_fixed_width(ln, 1140, 1149).strip()

            situs_street_key = _build_situs_street_key(
                situs_num,
                situs_street_prefx,
                situs_street,
                situs_street_suffix,
            )
            situs_city = _resolve_situs_city(ln, situs_city, situs_street_key)

            props[prop_id] = {
                "prop_id": prop_id,
                "situs_num": situs_num,
                "situs_street_prefx": situs_street_prefx,
                "situs_street": situs_street,
                "situs_street_suffix": situs_street_suffix,
                "situs_unit": situs_unit,
                "situs_city": situs_city,
                "situs_zip": situs_zip,
            }
    return props


#-----------------------------------------------------------------------------
# load_improvements()
#-----------------------------------------------------------------------------
def load_improvements(imp_path: str) -> Dict[str, List[Dict[str, str]]]:
    """Load IMP_INFO.TXT returning a map prop_id -> list of improvements."""
    imps: Dict[str, List[Dict[str, str]]] = {}
    with open(imp_path, "r", encoding="latin1", errors="replace") as fh:
        for ln in fh:
            if not ln.strip():
                continue
            prop_id = parse_fixed_width(ln, 1, 12).strip()
            improv_type_cd = parse_fixed_width(ln, 29, 38).strip()
            improv_type_desc = parse_fixed_width(ln, 39, 63).strip()
            # store minimal fields; could extend later
            entry = {"type_cd": improv_type_cd, "type_desc": improv_type_desc}
            imps.setdefault(prop_id, []).append(entry)
    return imps


#-----------------------------------------------------------------------------
# write_csv()
#-----------------------------------------------------------------------------
def write_csv(
    output_path: str,
    props: Dict[str, Dict[str, str]],
    imps: Dict[str, List[Dict[str, str]]],
) -> None:
    """Write the combined property and improvement data to a CSV file.

    Each row contains the property ID, situs address components, and a
    semicolon-separated list of improvement type codes and descriptions.
    """
    fieldnames = [
        "prop_id",
        "situs_num",
        "situs_street_prefx",
        "situs_street",
        "situs_street_suffix",
        "situs_unit",
        "situs_city",
        "situs_zip",
        "improv_type_cd",
        "improv_type_desc",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as csvf:
        writer = csv.DictWriter(csvf, fieldnames=fieldnames)
        writer.writeheader()

        # iterate properties; include those without improvements as well
        for prop_id, pdata in props.items():
            improvements = imps.get(prop_id, [])
            if improvements:
                # join unique codes/descriptions preserving order
                seen_cd = []
                seen_desc = []
                for e in improvements:
                    cd = e.get("type_cd", "")
                    desc = e.get("type_desc", "")
                    if cd and cd not in seen_cd:
                        seen_cd.append(cd)
                    if desc and desc not in seen_desc:
                        seen_desc.append(desc)
                row = {
                    **pdata,
                    "improv_type_cd": ";".join(seen_cd),
                    "improv_type_desc": ";".join(seen_desc),
                }
                writer.writerow(row)
            else:
                row = {**pdata, "improv_type_cd": "", "improv_type_desc": ""}
                writer.writerow(row)


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main(argv: List[str]) -> int:
    """Entry point for the script.

    argv is expected to be sys.argv. Returns an exit code suitable for
    SystemExit.
    """
    if len(argv) < 2:
        print("Usage: parse_appaisal_data.py /path/to/export_dir [output.csv]")
        return 2
    in_dir = argv[1]
    output = argv[2] if len(argv) > 2 else os.path.join(in_dir, "travis_properties.csv")

    prop_file = os.path.join(in_dir, "PROP.TXT")
    imp_file = os.path.join(in_dir, "IMP_INFO.TXT")

    if not os.path.isfile(prop_file):
        print(f"PROP.TXT not found in {in_dir}")
        return 3
    if not os.path.isfile(imp_file):
        print(f"IMP_INFO.TXT not found in {in_dir}")
        return 4

    props = load_properties(prop_file)
    imps = load_improvements(imp_file)

    write_csv(output, props, imps)
    print(f"Wrote {output} with {len(props)} properties")
    print(f"Empty situs cities: {TOTAL_EMPTY_SITUS_CITY}")
    print(f"Filled situs cities: {TOTAL_EMPTY_SITUS_CITY_FILLED}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
