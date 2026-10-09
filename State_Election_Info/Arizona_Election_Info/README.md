# Arizona_Election_Info

Reference dataset of election-administration resources for all **15 Arizona counties**. For each
county it records:

- the county **elections director** (who runs elections and tabulation for the Board of
  Supervisors) and the **County Recorder** (who handles voter registration and early voting),
  with contact details, and whether a current registered-voter list can be downloaded (or how to
  request one);
- the county **elections website**;
- the **historical election information** page, and which record types are posted online for
  the 2020 primary to today: results, canvass, reconciliation reports, post-election hand count
  audits, voter rosters, zero reports, hash/L&A test reports, EMS audit/event logs, ballot
  transfer logs, cast vote records (CVRs), ballot images, voter files;
- the route for a **public records** request (A.R.S. 39-121);
- the **voting equipment** in use (vendor, software and firmware versions, models), per the
  Secretary of State.

As with the parent `State_Election_Info` folder, the records come in three formats that are
kept in sync.

## Files

| File | Overview |
|------|----------|
| `arizona_county_election_info.json` | Canonical source. Top-level `title`, `description`, `generated` date, `state_resources` (SOS links), `record_type_labels`, `notes`, and a `counties` array of one record per county (15). |
| `arizona_county_election_info.csv` | Flat, one row per county (nested JSON fields flattened) for spreadsheets and imports. |
| `Arizona_County_Election_Info.md` | Human-readable summary, statewide resources, notes, a table of all counties, and per-county notes. |
| `generate.py` | Regenerates the CSV and Markdown from the JSON. |

## Record schema

Each entry in the JSON `counties` array has these fields:

| Field | Meaning |
|-------|---------|
| `county`, `fips` | County name and 5-digit FIPS code |
| `county_website` | County government home page |
| `election_official` | `title`, `name`, `physical_address`, `mailing_address`, `phone`, `fax`, `email` of the county elections director, per the SOS county election contact page |
| `voter_registrar` | `title`, `name`, `phone`, `email` of the County Recorder, per the same page, and `url` (the Recorder's page) |
| `voter_list` | `download_available` (bool), `url` (the Recorder's page, or its voter-data request page), `notes` |
| `elections_url` | County elections page or site |
| `historical_results` | `url` (results archive, or the elections page when results are posted there), `platforms` (e.g. `Clarity ENR`, `Enhanced Voting`), `records_found_2020_present` (record type → `years` found plus an `undated_items` flag), `example_links` (sample log / audit links), `evidence` (how the record list was compiled), `notes` |
| `open_records` | `url` (online portal or county request page, or `null`) and `method` (how to file when there is no portal) |
| `tabulation` | `method`, `vendor`, `ems_software` (software version(s) from the SOS list), `equipment_summary`, `equipment` (one entry per row of the SOS equipment list: `vendor`, `type`, `model`, `firmware`, `software`, `quantity` = `null` because counts are not published), and `notes` |

The record-type keys are `results`, `canvass`, `reconciliation`, `hand_count_audit`,
`voter_roster`, `results_tape`, `hash_la_test`, `audit_log`, `ballot_transfer_log`, `cvr`,
`ballot_images`, `voter_file`. `record_type_labels` in the JSON gives the display name for each.

## Sources and method

- **Elections directors and County Recorders:** the Arizona SOS
  [County Election Contact Info](https://azsos.gov/elections/about-elections/county-election-contact-info)
  page, parsed for all 15 counties on 2026-10-09. The page protects emails with Cloudflare email
  obfuscation; they were decoded. Names of former officials left in HTML comments were ignored.
- **County names and FIPS codes:** the Census Bureau 2020 county list for Arizona (`st04_az_cou2020.txt`).
- **Voting equipment:** the SOS
  [2026 Election Cycle / Voting Equipment](https://azsos.gov/sites/default/files/docs/2026-Election-Cycle-Voting-Equipment--Primary-Election.pdf)
  list (Primary Election, revised July 2026), transcribed row by row into `tabulation.equipment`.
  No general-election version was posted when checked.
- **County websites:** the county links on the SOS contact page, corrected to each county's
  current home page.
- **Elections, results and open-records pages:** an automated crawl of each county's elections,
  Recorder and county sites, then a second pass from the elections page through results,
  archive and audit links. Documents were counted only when found on the county's own domains.
  Nine counties block scripted requests (Cloudflare or Akamai): Apache, Coconino, Graham,
  Maricopa, Navajo, Pinal, Santa Cruz, Yavapai and Yuma. These were reviewed in a browser, as
  were Cochise, Gila, Greenlee and Pima, whose results pages load documents by script or by year
  tab. Every chosen URL was fetched again; all load, apart from the bot-blocked sites, which were
  checked in the browser.
- **Record types posted:** link text and file names of documents linked from county results
  pages, classified by keyword. Public notices are excluded. The EMS audit-event and system logs
  that several counties post with their results were checked link by link.
- **Statewide context:** the SOS
  [post-election procedures](https://azsos.gov/elections/about-elections/elections-procedures/post-election-procedures)
  and [2026 election information](https://azsos.gov/elections/election-information/2026-election-info)
  pages (county hand count audit reports), and the
  [Elections Procedures Manual](https://azsos.gov/elections/about-elections/elections-procedures/epm).

## Caveats

- `records_found_2020_present` shows what was **found posted online** on county sites. It is not
  an exhaustive inventory: absence means "not found", not "does not exist". Hand count audit
  reports for every county are posted by the SOS and are not counted per county unless the
  county also posts them. Records that are not online can be requested under A.R.S. 39-121.
  Years are inferred from link text, file names and upload folders, so a few may be off by the
  upload year.
- Several county results pages are built with JavaScript (year tabs or drop-downs), and some
  counties (Cochise, Greenlee) post only the current election. Coverage of earlier years there
  comes from browser checks and may be incomplete.
- The SOS equipment list is a snapshot for the 2026 primary and may change before the general
  election.
- No county posts a free download of its voter file. Under A.R.S. 16-168 voter lists are sold for
  political or election purposes on a signed request.
- Deep links change when counties redesign their sites. Re-navigate from `county_website` or the
  SOS county election contact page if one breaks.
- Researched on **2026-10-09** (also recorded in the JSON `generated` field).

## Regenerating the CSV and Markdown

`arizona_county_election_info.json` is the source of truth. After editing it, rebuild the other
two files rather than hand-editing them:

```
python generate.py
```

The script uses only the Python standard library and resolves paths relative to itself.
