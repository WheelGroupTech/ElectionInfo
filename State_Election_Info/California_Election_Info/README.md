# California_Election_Info

Reference dataset of election-administration resources for all **58 California counties**. For each
county it records:

- the **county elections official** (Registrar of Voters, or the County Clerk / Clerk-Recorder
  acting as registrar) with contact details. In California this official also maintains voter
  registration. It also records whether a current registered-voter list can be downloaded (or how
  to request one);
- the county **elections website**;
- the **historical election information** page, and which record types are posted online for
  the 2020 primary to today: results, official canvass / statement of vote, reconciliation
  reports, 1% manual tally / risk-limiting audit reports, voter rosters, zero reports,
  hash/L&A test reports, EMS or tabulator logs, ballot transfer logs, cast vote records (CVRs),
  ballot images, voter files;
- the route for a **California Public Records Act** request;
- the **voting technology** in use: tabulation system and version, vote center tabulators,
  accessible ballot-marking devices, ballot on demand, remote accessible vote by mail, electronic
  pollbooks and election management system, per the Secretary of State.

As with the parent `State_Election_Info` folder, the records come in three formats that are
kept in sync.

## Files

| File | Overview |
|------|----------|
| `california_county_election_info.json` | Canonical source. Top-level `title`, `description`, `generated` date, `state_resources` (SOS links), `record_type_labels`, `notes`, and a `counties` array of one record per county (58). |
| `california_county_election_info.csv` | Flat, one row per county (nested JSON fields flattened) for spreadsheets and imports. |
| `California_County_Election_Info.md` | Human-readable summary, statewide resources, notes, a table of all counties, and per-county notes. |
| `generate.py` | Regenerates the CSV and Markdown from the JSON. |

## Record schema

Each entry in the JSON `counties` array has these fields:

| Field | Meaning |
|-------|---------|
| `county`, `fips` | County name and 5-digit FIPS code |
| `county_website` | County government home page |
| `election_official` | `title`, `name`, `physical_address`, `mailing_address`, `phone`, `fax`, `email` of the county elections official, per the SOS County Elections Offices list (`physical_address` is `null` when the listing gives only a P.O. box) |
| `voter_registrar` | `title`, `name`, `phone`, `email` (the same official) and `url` (the county elections page) |
| `voter_list` | `download_available` (bool), `url` (the SOS Voter Registration Information File Request page), `notes` |
| `elections_url` | County elections page or site, as linked from the SOS County Elections Offices list (corrected where that link was stale) |
| `historical_results` | `url` (results archive, the elections page when results are posted there, or the SOS statewide results page when no county archive was found), `platforms` (e.g. `Clarity ENR`), `records_found_2020_present` (record type → `years` found plus an `undated_items` flag), `example_links` (sample CVR / ballot-image / audit / log links), `evidence` (how the record list was compiled), `notes` |
| `open_records` | `url` (online portal or county request page, or `null`) and `method` (how to file when there is no portal) |
| `tabulation` | `method`, `vendor` (tabulation vendor), `ems_software` (tabulation system and version), `equipment_summary`, `equipment` (one entry per column of the SOS voting-technology table: `vendor`, `type`, `model` as the SOS lists it, `quantity` = `null` because counts are not published), and `notes` |

The record-type keys are `results`, `canvass`, `reconciliation`, `hand_count_audit`,
`voter_roster`, `results_tape`, `hash_la_test`, `audit_log`, `ballot_transfer_log`, `cvr`,
`ballot_images`, `voter_file`. `record_type_labels` in the JSON gives the display name for each.

## Sources and method

- **Elections officials:** the California SOS
  [County Elections Offices](https://www.sos.ca.gov/elections/voting-resources/county-elections-offices)
  list, parsed for all 58 counties on 2026-10-09 (name and title, street and mailing address,
  phone, fax, email, elections website link).
- **County names and FIPS codes:** the Census Bureau 2020 county list for California (`st06_ca_cou2020.txt`).
- **Voting technology:** the SOS "Voting Technologies in Use by County" table, as of April 30, 2026
  ([PDF](https://votingsystems.cdn.sos.ca.gov/oversight/county-vsys/vot-tech-by-counties-2026-1.pdf),
  the same file as `Voting Technology by Counties 2026-1.pdf`). Each county's row was parsed into
  `tabulation.equipment`. Vendors were mapped from the SOS
  [voting technology vendor pages](https://www.sos.ca.gov/elections/ovsta/voting-technology-vendors):
  Democracy Suite → Liberty Vote (formerly Dominion), EVS/ExpressVote → ES&S, Verity → Hart
  InterCivic, VSAP → Los Angeles County, Secure Select → Democracy Live, Sentio → Runbeck,
  PollPad → KNOWiNK, Precinct Central → Tenex.
- **County websites:** the official-website property in Wikidata (San Francisco added by hand as
  sf.gov), with each site fetched to confirm it loads.
- **Elections, results and open-records pages:** an automated crawl of each county's
  SOS-listed elections site and county site (up to 60 election-related pages per county, plus
  common elections paths and the site's XML sitemap), then a second pass that followed results,
  archive and audit links two levels down from the elections page. Documents were counted only
  when found on the county's own domains, because the crawler sometimes followed links into
  other counties' or states' sites. Every county's results-page pick was reviewed by hand.
  The twelve counties whose sites block scripted requests (Akamai or Cloudflare) were reviewed in
  a browser: Amador, El Dorado, Fresno, Kern, Kings, Mendocino, Monterey, Riverside, San Benito,
  Santa Clara, Sutter and Tulare. San Diego, Orange, Calaveras, Inyo, Del Norte and Santa Barbara
  were also checked in the browser. San Francisco's results page
  (https://www.sf.gov/past-election-results) was supplied by the user. The per-election detail
  pages on sfelections.org were checked for CVRs, ballot images and logs. Glenn County's site
  never cleared its Cloudflare browser challenge, so it was not reviewed. Ten more records portals
  were found at the vendors' standard county addresses (`<county>countyca.nextrequest.com` and
  similar) and are marked as such in `open_records.method`. Every chosen URL was fetched again;
  all load, apart from the bot-blocked sites, which were checked in the browser.
- **Record types posted:** link text and file names of documents linked from county election
  pages, classified by keyword. Documents on a page titled "Election Results" default to
  results. Public notices, press releases, and the public drawings for the 1%
  manual tally are excluded. The rarer types (CVR, ballot images, logs, rosters, manual-tally and
  RLA reports, zero reports) were checked link by link.
- **Statewide context:** SOS pages on
  [post-election audits](https://www.sos.ca.gov/elections/post-election-audits) (1% manual tally,
  Elections Code 15360, and risk-limiting audits) and the
  [voter registration information file request](https://www.sos.ca.gov/elections/voter-registration/voter-registration-information-file-request);
  2026 reporting (CalMatters, Jefferson Public Radio) on Shasta County's Measure B.

## Caveats

- `records_found_2020_present` shows what was **found posted online** on county sites. It is not
  an exhaustive inventory: absence means "not found", not "does not exist". Records that are not
  online can be requested under the California Public Records Act. Years are inferred from link
  text, file names and upload folders, so a few may be off by the upload year. Many CivicPlus
  Document Center links carry no date and show as `undated`.
- Some county sites load their documents with JavaScript or from Google Drive and DotNetNuke
  file-share widgets (Del Norte, Santa Barbara and Calaveras, for example). The crawl
  undercounts those sites; most were checked in a browser, but per-type coverage is thinner
  there.
- The SOS voting-technology table is a snapshot provided by the counties (as of April 30, 2026)
  and is subject to change; the SOS asks voters and media to confirm with the county.
- No county posts a free download of its voter file. Access is limited by Elections Code 2188 and
  2194 to qualified requesters who apply to the county or the SOS.
- Deep links change when counties redesign their sites. Re-navigate from `county_website` or the
  SOS County Elections Offices list if one breaks.
- Researched on **2026-10-09** (also recorded in the JSON `generated` field).

## Regenerating the CSV and Markdown

`california_county_election_info.json` is the source of truth. After editing it, rebuild the
other two files rather than hand-editing them:

```
python generate.py
```

The script uses only the Python standard library and resolves paths relative to itself.
