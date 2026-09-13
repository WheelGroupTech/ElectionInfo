#!/usr/bin/env python3
"""Regenerate state_election_codes.csv and State_Election_Codes.md from the JSON.

state_election_codes.json is the source of truth. Edit it, then run this script to
rebuild the CSV and Markdown so all three files stay in sync:

    python generate.py

Run with no arguments from within the State_Election_Info directory (or any directory;
paths are resolved relative to this file).
"""

import csv
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
JSON_PATH = HERE / "state_election_codes.json"
CSV_PATH = HERE / "state_election_codes.csv"
MD_PATH = HERE / "State_Election_Codes.md"

# Field order shared by the CSV columns and (a subset of) the Markdown table.
FIELDS = [
    "state",
    "abbr",
    "code_citation",
    "code_url",
    "code_url_secondary",
    "admin_agency",
    "admin_url",
    "voter_portal_url",
]


def load():
    with open(JSON_PATH, encoding="utf-8") as f:
        return json.load(f)


def write_csv(states):
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for s in states:
            writer.writerow({field: (s.get(field) or "") for field in FIELDS})


def write_markdown(states, generated=""):
    verified = f"Verified via web research on {generated}. " if generated else ""
    lines = [
        "# State Election Codes & Election-Administration Sites",
        "",
        "For each of the 50 states: the official **statutory election code** (citation + "
        "official source URL), the state's **chief election-administration agency** and its "
        "official website, and the public **voter portal**.",
        "",
        "**Notes**",
        "- Links point to official state sources: legislature / legislative-service sites, "
        "or the state's designated official code publisher.",
        "- A few states (Arkansas, Georgia, Mississippi, Tennessee) publish their *official* "
        "code only through LexisNexis under contract; the free public-access portal is given "
        "as the secondary link, with the legislature/SOS landing page as primary.",
        "- The chief election authority is **not always the Secretary of State** — several "
        "states use a Board/Commission, a Department of Elections, or the Lieutenant Governor. "
        "The agency name reflects this.",
        "- **Voter portal** = the state's public site to register / check registration / find "
        "polling place / view ballot. **Wyoming has none statewide** (no online registration; "
        "records held by county clerks).",
        f"- {verified}Deep links can shift when codes/portals are updated; the citation "
        "lets you re-navigate. Machine-readable copies: `state_election_codes.json`, "
        "`state_election_codes.csv`.",
        "",
        "| # | State | Election code (citation) | Official code URL | "
        "Election-administration agency | Agency URL | Voter portal |",
        "|---|-------|--------------------------|-------------------|"
        "--------------------------------|------------|--------------|",
    ]
    for i, s in enumerate(states, 1):
        code = s["code_url"]
        if s.get("code_url_secondary"):
            code = f"{code}<br>alt: {s['code_url_secondary']}"
        voter_portal = s.get("voter_portal_url") or "_(none statewide)_"
        lines.append(
            f"| {i} | {s['state']} | {s['code_citation']} | {code} | "
            f"{s['admin_agency']} | {s['admin_url']} | {voter_portal} |"
        )
    lines.append("")
    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    data = load()
    states = data["states"]
    write_csv(states)
    write_markdown(states, data.get("generated", ""))
    print(f"Regenerated {CSV_PATH.name} and {MD_PATH.name} from {JSON_PATH.name} "
          f"({len(states)} states).")


if __name__ == "__main__":
    main()
