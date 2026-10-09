#!/usr/bin/env python3
"""Regenerate georgia_county_election_info.csv and Georgia_County_Election_Info.md from the JSON.

georgia_county_election_info.json is the source of truth. Edit it, then run this script to
rebuild the CSV and Markdown so all three files stay in sync:

    python generate.py

Paths are resolved relative to this file, so it can be run from any directory.
"""

import csv
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
JSON_PATH = HERE / "georgia_county_election_info.json"
CSV_PATH = HERE / "georgia_county_election_info.csv"
MD_PATH = HERE / "Georgia_County_Election_Info.md"

# Record types in display order, with the short codes used in the Markdown table.
RECORD_CODES = [
    ("results", "RES"),
    ("canvass", "CAN"),
    ("reconciliation", "REC"),
    ("hand_count_audit", "HCA"),
    ("voter_roster", "ROS"),
    ("results_tape", "TAPE"),
    ("hash_la_test", "HASH"),
    ("audit_log", "LOG"),
    ("ballot_transfer_log", "XFER"),
    ("cvr", "CVR"),
    ("ballot_images", "IMG"),
    ("voter_file", "VF"),
]

CSV_FIELDS = [
    "county",
    "fips",
    "county_website",
    "election_official_title",
    "election_official_name",
    "election_office_address",
    "election_office_mailing",
    "election_office_phone",
    "election_office_email",
    "voter_registrar_title",
    "voter_registrar_name",
    "voter_registrar_phone",
    "voter_registrar_email",
    "voter_registrar_url",
    "voter_list_download_available",
    "voter_list_url",
    "voter_list_notes",
    "elections_url",
    "historical_results_url",
    "results_platforms",
] + [f"records_{key}" for key, _ in RECORD_CODES] + [
    "records_evidence",
    "records_example_links",
    "historical_notes",
    "open_records_url",
    "open_records_method",
    "tabulation_method",
    "voting_system_vendor",
    "ems_software",
    "voting_equipment",
    "tabulation_notes",
]


def load():
    with open(JSON_PATH, encoding="utf-8") as f:
        return json.load(f)


def years_text(entry, short=False):
    """Render a {'years': [...], 'undated_items': bool} entry as text."""
    if entry is None:
        return ""
    ys = entry["years"]
    if short:
        ys = [y[2:] for y in ys]
    parts = []
    if ys:
        parts.append(span(ys, short))
    if entry["undated_items"]:
        parts.append("undated")
    return "; ".join(parts) if parts else "yes"


def span(ys, short):
    """Collapse consecutive years: 2022,2023,2024,2026 -> 2022-2024, 2026."""
    nums = sorted(int(y) for y in ys)
    runs, start, prev = [], nums[0], nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        runs.append((start, prev))
        start = prev = n
    runs.append((start, prev))
    fmt = (lambda n: f"{n:02d}") if short else str
    return ", ".join(fmt(a) if a == b else f"{fmt(a)}-{fmt(b)}" for a, b in runs)


def flatten(c):
    eo, vr, vl = c["election_official"], c["voter_registrar"], c["voter_list"]
    hr, orr, tab = c["historical_results"], c["open_records"], c["tabulation"]
    row = {
        "county": c["county"],
        "fips": c["fips"],
        "county_website": c["county_website"],
        "election_official_title": eo["title"],
        "election_official_name": eo["name"],
        "election_office_address": eo["physical_address"],
        "election_office_mailing": eo["mailing_address"],
        "election_office_phone": eo["phone"],
        "election_office_email": eo.get("email"),
        "voter_registrar_title": vr["title"],
        "voter_registrar_name": vr["name"],
        "voter_registrar_phone": vr["phone"],
        "voter_registrar_email": vr.get("email"),
        "voter_registrar_url": vr["url"],
        "voter_list_download_available": "yes" if vl["download_available"] else "no",
        "voter_list_url": vl["url"],
        "voter_list_notes": vl["notes"],
        "elections_url": c["elections_url"],
        "historical_results_url": hr["url"],
        "results_platforms": "; ".join(hr["platforms"]),
        "records_evidence": hr["evidence"],
        "records_example_links": " ".join(u for links in hr["example_links"].values() for u in links),
        "historical_notes": hr["notes"],
        "open_records_url": orr["url"],
        "open_records_method": orr["method"],
        "tabulation_method": tab["method"],
        "voting_system_vendor": tab["vendor"],
        "ems_software": tab["ems_software"],
        "voting_equipment": tab["equipment_summary"],
        "tabulation_notes": tab["notes"],
    }
    found = hr["records_found_2020_present"]
    for key, _ in RECORD_CODES:
        row[f"records_{key}"] = years_text(found.get(key))
    return row


def write_csv(counties):
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for c in counties:
            writer.writerow({k: (v if v is not None else "") for k, v in flatten(c).items()})


def link(url, text):
    return f"[{text}]({url})" if url else "—"


def records_cell(found):
    parts = []
    for key, code in RECORD_CODES:
        if key in found:
            parts.append(f"{code} {years_text(found[key], short=True)}".strip())
    return "<br>".join(parts) if parts else "_none found_"


def write_markdown(data):
    counties = data["counties"]
    labels = data["record_type_labels"]
    sr = data["state_resources"]
    vendors = Counter(c["tabulation"]["vendor"] for c in counties)
    found = lambda key: sorted(c["county"] for c in counties
                               if key in c["historical_results"]["records_found_2020_present"])
    downloads = [c["county"] for c in counties if c["voter_list"]["download_available"]]
    portals = sum(1 for c in counties if c["open_records"]["url"])

    lines = [
        "# Georgia County Election Information",
        "",
        data.get("description", "For each of the 159 Georgia counties: the voter registrar and whether a registered-voter list can be downloaded; the elections website; the historical election-results location and the record types posted for 2020-present; the open-records route; and the ballot tabulation method/voting system (uniform statewide)."),
        "",
        f"Researched {data['generated']}. Machine-readable copies: `georgia_county_election_info.json` "
        "(canonical) and `georgia_county_election_info.csv`.",
        "",
        "## Summary",
        "",
        f"- **Counties:** {len(counties)}",
        f"- **Voting system:** {'; '.join(f'{v}: {n} counties' for v, n in vendors.most_common())}. "
        "One state-supplied system in every county: ImageCast X ballot-marking devices, "
        "ImageCast Precinct and ImageCast Central scanners.",
        f"- **Voter list:** {'free download in ' + ', '.join(downloads) if downloads else 'No free public download in any county'}. "
        "The SOS sells voter lists; counties furnish them on request under the Georgia Open Records Act.",
        f"- **County results posted on county sites:** {len(found('results'))} counties.",
        f"- **Cast vote records posted on county sites:** {', '.join(found('cvr')) or 'none found'} "
        "(the SOS posts redacted statewide CVRs).",
        f"- **Ballot images posted on county sites:** {', '.join(found('ballot_images')) or 'none found'} "
        "(every county uploads to the SOS Ballot Image Library).",
        f"- **EMS audit logs posted online:** {', '.join(found('audit_log')) or 'none found'}.",
        f"- **Ballot transfer / chain-of-custody logs posted online:** {', '.join(found('ballot_transfer_log')) or 'none found'}.",
        f"- **Results tapes / zero reports posted online:** {len(found('results_tape'))} counties.",
        f"- **Voter rosters / numbered lists posted online:** {', '.join(found('voter_roster')) or 'none found'}.",
        f"- **Post-election audit reports posted online:** {len(found('hand_count_audit'))} counties.",
        f"- **Reconciliation reports posted online:** {len(found('reconciliation'))} counties.",
        f"- **Open records:** {portals} counties have an online request portal or request page; the rest take "
        "written Open Records Act requests by mail, email or in person.",
        "",
        "## Statewide resources",
        "",
    ]
    lines += [f"- {key.replace('_', ' ').capitalize()}: {url}" for key, url in sr.items()]
    lines += [
        "",
        "## Notes",
        "",
    ]
    lines += [f"- {n}" for n in data["notes"]]
    lines += [
        "",
        "## Record-type codes",
        "",
        "Years are two-digit (`22-24` = 2022–2024); `undated` = items whose year could not be inferred.",
        "",
        "| Code | Record type |",
        "|------|-------------|",
    ]
    lines += [f"| {code} | {labels[key]} |" for key, code in RECORD_CODES]
    lines += [
        "",
        "## Counties",
        "",
        "| # | County | Election official | Voter registrar | Voter list | Elections site | "
        "Historical results | Records posted (2020–present) | Open records | Tabulation |",
        "|---|--------|-------------------|-----------------|------------|----------------|"
        "--------------------|-------------------------------|--------------|------------|",
    ]
    for i, c in enumerate(counties, 1):
        eo, vr, vl = c["election_official"], c["voter_registrar"], c["voter_list"]
        hr, orr, tab = c["historical_results"], c["open_records"], c["tabulation"]
        official = f"{eo['title']}<br>{eo['name']}<br>{eo['phone']}"
        registrar = f"{vr['title']}<br>{vr['name']}"
        if vr["url"]:
            registrar += f"<br>{link(vr['url'], 'site')}"
        if vl["download_available"]:
            voter_list = f"**Download:** {link(vl['url'], 'yes')}"
        elif vl["url"]:
            voter_list = f"Request ({link(vl['url'], 'form/info')})"
        else:
            voter_list = "Request"
        results = link(hr["url"], "results")
        if hr["platforms"]:
            results += "<br>" + ", ".join(hr["platforms"])
        pir = link(orr["url"], "portal/page") if orr["url"] else "Written Open Records request"
        tabulation = f"{tab['vendor']} {tab['ems_software'] or ''}".strip()
        if "hand count" in tab["method"]:
            tabulation += "<br>**+ primary hand count**"
        if tab["notes"] and "unconfirmed" in tab["notes"]:
            tabulation += "<br>_see notes_"
        county = link(c["county_website"], c["county"])
        lines.append(
            f"| {i} | {county} | {official} | {registrar} | {voter_list} | "
            f"{link(c['elections_url'], 'elections')} | {results} | {records_cell(hr['records_found_2020_present'])} | "
            f"{pir} | {tabulation} |"
        )

    notes = [(c["county"], c["tabulation"]["notes"], c["historical_results"]["notes"]) for c in counties]
    notes = [n for n in notes if n[1] or n[2]]
    if notes:
        lines += ["", "## County notes", ""]
        for county, tab_note, hist_note in notes:
            text = " ".join(t for t in (tab_note, hist_note) if t)
            lines.append(f"- **{county}:** {text}")
    lines.append("")
    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    data = load()
    write_csv(data["counties"])
    write_markdown(data)
    print(f"Regenerated {CSV_PATH.name} and {MD_PATH.name} from {JSON_PATH.name} "
          f"({len(data['counties'])} counties).")


if __name__ == "__main__":
    main()
