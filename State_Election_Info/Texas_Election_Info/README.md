# Texas_Election_Info

Reference dataset of election-administration resources for all **254 Texas counties**. For each
county it records:

- the **voter registrar**, and whether a current registered-voter list can be downloaded
  (or how to request one);
- the county **elections website**;
- the **historical election information** page, and which record types are posted online for
  the 2020 primary to today: results, canvass, reconciliation forms, hand-count audits, voter
  rosters, results tapes, hash/L&A certifications, EMS audit logs, ballot transfer logs, cast
  vote records (CVRs), ballot images, voter files;
- the route for an **open-records (Public Information Act) request**;
- the **ballot tabulation method** and voting system (vendor, EMS version, equipment
  inventory), plus known party-primary hand counts.

As with the parent `State_Election_Info` folder, the records come in three formats that are
kept in sync.

## Files

| File | Overview |
|------|----------|
| `texas_county_election_info.json` | Canonical source. Top-level `title`, `description`, `generated` date, `state_resources` (SOS links), `record_type_labels`, `notes`, and a `counties` array of 254 records. |
| `texas_county_election_info.csv` | Flat, one row per county (nested JSON fields flattened) for spreadsheets and imports. |
| `Texas_County_Election_Info.md` | Human-readable summary, statewide resources, notes, a table of all 254 counties, and per-county notes. |
| `generate.py` | Regenerates the CSV and Markdown from the JSON. |

## Record schema

Each entry in the JSON `counties` array has these fields:

| Field | Meaning |
|-------|---------|
| `county`, `fips` | County name and 5-digit FIPS code |
| `county_website` | County government home page |
| `election_official` | `title`, `name`, `physical_address`, `mailing_address`, `phone` of the officer who conducts elections (Elections Administrator, County Clerk, or Tax Assessor-Collector), per the SOS Election Duties list |
| `voter_registrar` | `title`, `name`, `phone`, `url` of the voter registrar (Elections Administrator or Tax Assessor-Collector, sometimes the County Clerk), per the SOS County Voter Registration Officials list |
| `voter_list` | `download_available` (bool), `url` (download page, or request form/price page), `notes` |
| `elections_url` | Primary elections page or site |
| `historical_results` | `url` (results archive, or the elections page when results are posted there), `platforms` (e.g., Clarity ENR), `records_found_2020_present` (record type → `years` found plus an `undated_items` flag), `example_links` (sample CVR / ballot-image / voter-file / log links), `evidence` (how the record list was compiled), `notes` |
| `open_records` | `url` (online portal or county request page, or `null`) and `method` (how to file when there is no portal) |
| `tabulation` | `method`, `vendor` (ES&S or Hart), `ems_software`, `equipment_summary`, full `equipment` inventory from the SOS, and `notes` (party-primary hand counts, inventory anomalies) |

The record-type keys are `results`, `canvass`, `reconciliation`, `hand_count_audit`,
`voter_roster`, `results_tape`, `hash_la_test`, `audit_log`, `ballot_transfer_log`, `cvr`,
`ballot_images`, `voter_file`. `record_type_labels` in the JSON gives the display name for each.

## Sources and method

- **Officials and registrars:** Texas SOS
  [Election Duties](https://www.sos.state.tx.us/elections/voter/county.shtml) (including its
  Excel export) and
  [County Voter Registration Officials](https://www.sos.state.tx.us/elections/voter/votregduties.shtml).
- **Voting systems:** SOS
  [Voting Systems by County](https://www.sos.state.tx.us/elections/forms/sysexam/voting-sys-bycounty.pdf),
  revised 09/01/2026.
- **County websites:** the SOS county links list, plus the official-website property in Wikidata,
  with redirects resolved to the current domain.
- **Elections, results, registrar, and open-records pages:** an automated crawl of each county site
  (up to 60 election-related pages per county), followed by manual review of every county's
  picks. About 50 counties were corrected by hand with web searches and the browser, mostly
  bot-blocked sites (Bell, Brazoria, Delta, Galveston, Johnson, Nueces, and others) and sites
  where the crawl chose the wrong page. Every chosen URL was checked to load.
- **Record types posted:** link text and file names on the crawled pages, classified by keyword,
  plus manual browser checks of results pages for large or bot-blocked counties (Dallas, Denton,
  Brazoria, Galveston, Bastrop, Potter, Reeves, and others).
- **Hand counts:** Votebeat and Texas Tribune reporting on the 2024 and 2026 primaries, and SOS
  [Advisory 2025-18](https://www.sos.state.tx.us/elections/laws/advisory2025-18.shtml).

## Caveats

- `records_found_2020_present` shows what was **found posted online**. It is not an exhaustive
  inventory: absence means "not found", not "does not exist". Records that are not online
  (for example CVRs, results tapes, and EMS logs in most counties) can be requested under the
  Public Information Act. Years are inferred from file names, link text, and upload folders, so
  a few may be off by the upload year.
- Of the 254 counties, only **Dallas**, **El Paso**, and **Travis** post a free full voter-list
  download (Travis's is on the Tax Assessor-Collector's ArcGIS hub and also includes voting history
  from 2016 on). The rest furnish the list on request (Tex. Elec. Code 18.008). The crawler can miss
  downloads on script-rendered pages such as ArcGIS hubs, so a few more may exist. Daily early-voting rosters, which
  many counties post, are recorded under `voter_roster`.
- The SOS inventory lists **Kent** with EMS software only, and **Glasscock** and **Loving** with
  ballot markers but no scanner. Their tabulation method is marked unconfirmed.
- A dedicated open-records portal or page was found for 77 counties. For the others, `method`
  gives the statutory route: a written request to the records custodian, with the elections
  office contact.
- Deep links change when counties redesign their sites. Re-navigate from `county_website` or the
  SOS county links list if one breaks.
- Researched on **2026-10-03** (also recorded in the JSON `generated` field).

## Regenerating the CSV and Markdown

`texas_county_election_info.json` is the source of truth. After editing it, rebuild the other
two files rather than hand-editing them:

```
python generate.py
```

The script uses only the Python standard library and resolves paths relative to itself.
