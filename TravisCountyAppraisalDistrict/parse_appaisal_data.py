#!/usr/bin/env python3
"""Simple parser for Travis County Appraisal District fixed-width exports.

Reads PROP.TXT and IMP_INFO.TXT from an input directory and writes a CSV with
one row per property containing: prop_id, situs address fields, and the
improvement type code(s) and description(s) (joined with ';' if multiple).

Usage:
    python parse_appaisal_data.py /path/to/export_dir [output.csv]

This file was created to implement the layout described by the user and is
minimal and defensive. It does not require external libraries.
"""
from __future__ import annotations

import csv
import os
import sys
from typing import Dict, List


def parse_fixed_width(line: str, start: int, end: int) -> str:
    """Return the substring from a fixed-width record.

    The layout specification uses 1-based inclusive positions for start and end.
    This helper converts to Python's 0-based indexing and strips trailing
    whitespace.
    """
    # start and end are 1-based inclusive positions per the layout file
    return line[start - 1:end].rstrip()


def load_properties(prop_path: str) -> Dict[str, Dict[str, str]]:
    """Load PROP.TXT returning a map prop_id -> address fields."""
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
