# Georgia_Election_Info

Reference dataset of election-administration resources for all **159 Georgia counties**. For each
county it records:

- the county **election supervisor** and **chief registrar** with their contact details, and
  whether a current registered-voter list can be downloaded (or how to request one);
- the county **elections website**;
- the **historical election information** page, and which record types are posted online for
  the 2020 primary to today: results, canvass/certification, reconciliation reports,
  post-election audits, voter rosters, results tapes, hash/L&A test reports, EMS audit logs,
  ballot transfer logs, cast vote records (CVRs), ballot images, voter files;
- the route for an **open-records (Open Records Act) request**;
- the **ballot tabulation method** and voting system (vendor, EMS version, equipment). Georgia
  uses one state-supplied system in every county.

As with the parent `State_Election_Info` folder, the records come in three formats that are
kept in sync.

## Files

| File | Overview |
|------|----------|
| `georgia_county_election_info.json` | Canonical source. Top-level `title`, `description`, `generated` date, `state_resources` (SOS links), `record_type_labels`, `notes`, and a `counties` array of one record per county (159). |
| `georgia_county_election_info.csv` | Flat, one row per county (nested JSON fields flattened) for spreadsheets and imports. |
| `Georgia_County_Election_Info.md` | Human-readable summary, statewide resources, notes, a table of all counties, and per-county notes. |
| `generate.py` | Regenerates the CSV and Markdown from the JSON. |

## Record schema

Each entry in the JSON `counties` array has these fields:

| Field | Meaning |
|-------|---------|
| `county`, `fips` | County name and 5-digit FIPS code |
| `county_website` | County government home page |
| `election_official` | `title`, `name`, `physical_address` (`null` when the SOS listing has only a P.O. box), `mailing_address`, `phone`, `email` of the county election supervisor, per the SOS County Election Offices lookup |
| `voter_registrar` | `title`, `name`, `phone`, `email` of the county chief registrar, per the SOS County Election Offices lookup (the same person as the supervisor in counties with a combined Board of Elections and Registration), and `url` (the county elections page) |
| `voter_list` | `download_available` (bool), `url` (download page, or request form/price page), `notes` |
| `elections_url` | Primary county elections page or site; the county's SOS County Election Offices listing when no county page was found |
| `historical_results` | `url` (results archive, the elections page when results are posted there, or the SOS results portal when the county posts none), `platforms` (`SOS results portal`, `Clarity ENR (archived)`), `records_found_2020_present` (record type → `years` found plus an `undated_items` flag), `example_links` (sample CVR / ballot-image / roster / audit links), `evidence` (how the record list was compiled), `notes` (including the county's uploads to the SOS Ballot Image Library) |
| `open_records` | `url` (online portal or county request page, or `null`) and `method` (how to file when there is no portal) |
| `tabulation` | `method`, `vendor` (Dominion Voting Systems, now Liberty Vote, statewide), `ems_software`, `equipment_summary`, `equipment` inventory (empty: the SOS publishes no per-county inventory), and `notes` |

The record-type keys are `results`, `canvass`, `reconciliation`, `hand_count_audit`,
`voter_roster`, `results_tape`, `hash_la_test`, `audit_log`, `ballot_transfer_log`, `cvr`,
`ballot_images`, `voter_file`. `record_type_labels` in the JSON gives the display name for each.

## Sources and method

- **Officials and registrars:** the Georgia SOS
  [County Election Offices](https://mvp.sos.ga.gov/s/county-election-offices) lookup. The page
  loads each county from a Salesforce Apex action (`vrWebIntegrationController.getCountyInformation`),
  which was queried for each of the 159 counties on 2026-10-09. Names and addresses were
  converted from upper case, and the P.O. box and street parts of the SOS address fields were
  split into `mailing_address` and `physical_address`. `phone` is the SOS public phone field.
  Emails are included because the SOS lists them.
- **County names and FIPS codes:** the Census Bureau 2020 county list for Georgia (`st13_ga_cou2020.txt`).
- **County websites:** the county profile pages of the Association County Commissioners of
  Georgia (ACCG), the official-website property in Wikidata, and the domains of the SOS-listed
  election emails. Each candidate was fetched and checked against the county name. Several
  Wikidata domains had lapsed and now host spam or chamber-of-commerce sites, so those were
  dropped. Jeff Davis was found by web search.
- **Elections, results and open-records pages:** an automated crawl of each county site (up to
  60 election-related pages per county, plus common elections paths and the site's XML
  sitemap), then a second pass that followed results, archive and audit links one level down
  from each county's elections page. Every county's picks were reviewed, and about 40 were
  corrected by hand (tag and event pages, results posted as news items, session IDs in GovQA
  links, sheriff or city portals, and similar). Ten more records portals were found at the
  vendors' standard county addresses (`<county>countyga.nextrequest.com` and similar) and are
  marked as such in `open_records.method`. Twenty-one counties whose sites block scripted
  requests (Cloudflare or Akamai) or need a browser (Atkinson, Bryan, Catoosa, Clayton,
  Dougherty, Newton, Stephens, Worth and others) were reviewed in a browser. Wheeler and Jasper
  could not be opened at all; their records are left empty and say so in the notes. Every chosen
  URL was then fetched again. All load, apart from the bot-blocked sites, which were checked in
  the browser.
- **Record types posted:** link text and file names of the documents linked from county
  election pages, classified by keyword. Documents on a page titled "Election Results" or
  "Audit" default to that type. Public notices (L&A testing, canvassing, audit and recount
  notices), financial and PREA audits, and agenda items are excluded. The rarer types (CVR,
  ballot images, rosters, L&A reports, audits, reconciliation) were checked link by link.
  Browser checks covered Cobb (Election History page), Gwinnett, Hall, Newton, Rabun,
  Stephens, Clayton and Worth.
- **Statewide resources and SOS-level records:** the SOS
  [Ballot Image Library](https://sos.ga.gov/ballot-image-library), whose public listing at
  ballotimages.sos.ga.gov was queried for 2024-2026 to get the per-county upload counts in
  `historical_results.notes`; the
  [Cast Vote Records](https://sos.ga.gov/page/cast-vote-records) page; the
  [audit page](https://sos.ga.gov/page/elections-audit-information); the
  [voter-list order page](https://sos.ga.gov/page/order-voter-registration-lists-and-files); and
  the [Open Records page](https://sos.ga.gov/page/georgia-open-records). The previously listed
  `https://sos.ga.gov/page/open-records-act` returns 404 and was replaced. The
  `sos.ga.gov:8443` CVR URL serves the same page as the canonical URL, which has no port.
- **Voting system:** uniform statewide Dominion Democracy Suite 5.5-A, per SOS and EAC
  certification records and 2026 news coverage (AJC, Votebeat, WABE) of the QR-code deadline.
  Dominion was acquired and renamed Liberty Vote in October 2025. SB 3EX (June 2026) postponed
  the QR-code tabulation ban to January 1, 2028. No SOS per-county equipment inventory was found.

## Caveats

- `records_found_2020_present` shows what was **found posted online** on county sites. It is not
  an exhaustive inventory: absence means "not found", not "does not exist". Statewide SOS
  postings are not counted per county: ballot images for every county, redacted CVRs and the
  results portal. Records that are not online can be requested under the Open Records Act.
  Years are inferred from link text, file names and upload folders, so a few may be off by the
  upload year. Many CivicPlus Document Center links carry no date and show as `undated`.
- Some county sites load their documents with JavaScript or hide them in collapsed folders
  (Granicus/Vision "docfold" pages, for example). The crawl undercounts those sites; the
  largest of them were checked in a browser.
- No county posts a free download of its voter file. The SOS sells county and statewide voter
  lists, and counties furnish voter data on request under the Georgia Open Records Act
  (O.C.G.A. 50-18-70 et seq.).
- Deep links change when counties redesign their sites. Re-navigate from `county_website` or the
  SOS County Election Offices lookup if one breaks.
- Researched on **2026-10-09** (also recorded in the JSON `generated` field).

## Regenerating the CSV and Markdown

`georgia_county_election_info.json` is the source of truth. After editing it, rebuild the other
two files rather than hand-editing them:

```
python generate.py
```

The script uses only the Python standard library and resolves paths relative to itself.
