# Texas election results survey — 50 largest counties (2026-10-25)

Survey of where the 50 largest Texas counties (the same list as the voter roster survey,
`voter-roster-county-survey.md`) publish election results, for the election-results loading spec.
Each county's own election site was the starting point (the user's note: the county sites carry the
direct links into Clarity). Everything found for elections from 2020-01-01 through 2026-10-25 was
downloaded into `Election_Results/TX_<County>_County/` (untracked, about 6.7 GB besides Travis).
Travis was left as the user built it.

## Folder layout and naming

Files keep the server's file name behind an election prefix, as in the Travis folder:

| Prefix | Election |
|---|---|
| `P26_` | March primary |
| `PR26_` | primary runoff (May; July 14 in 2020) |
| `L26_` | May uniform (local) election |
| `LR26_` | June runoff of the May election |
| `G25_` | November general / constitutional amendment election |
| `GR25_` | December runoff of the November election |
| `S26_0131_` | any other date (special elections and their runoffs): year, then month and day |

- When a Clarity election covers one party only, the party follows the code:
  `P26_Democratic_summary.zip`, `P26_Republican_summary.zip`.
- When two elections in one county get the same code, the Clarity election name is added:
  `G23_Municipal Utility District Elections_summary.zip`.
- A report dated after the election (a canvass printed a week later) gets the code of the
  election it reports.
- Wix-hosted files whose server names are hashes (Potter) are named by their link text or title instead.

## Sources

- **Clarity Elections (Scytl)** — `results.enr.clarityelections.com/TX/<County>/<EID>/`.
  - **Finding the election IDs:**
    - From the county sites' links.
    - From Clarity's per-county list, `/TX/<County>/elections.json`. This list is complete for only some counties: it is empty for Bexar, Hunt and Victoria, and holds 1–2 entries for Dallas, Tarrant and Williamson.
    - From the Internet Archive's URL index (`web.archive.org/cdx/search/cdx?url=results.enr.clarityelections.com/TX/<County>/&matchType=prefix`). This recovered the IDs that counties no longer link, for example Tarrant 2020–2025 and Dallas 2020–2023. The reports themselves were downloaded from Clarity, not from the archive.
  - **Downloading:** `current_ver.txt` gives the version, and `json/en/electionsettings.json` gives the election name and date. Clarity's date there is sometimes the setup day, so a date or month written in the election name wins.
  - **Report files** are at `/<EID>/<ver>/reports/<file>`: `summary.zip`, `detailxls.zip` (to about 2024) or `detailxlsx.zip` (later), `detailxml.zip` and `detailtxt.zip`.
    - Some elections were never posted with all four files.
    - Nueces 2020 has only `summary.zip`, and a few 2024 runoffs have no summary.
  - **Result:** 467 elections in 22 counties, 1,839 ZIP files (Travis not counted).
  - **Refused:** Clarity answers 403 for the Potter and Liberty county paths.
- **Enhanced Voting** — `app.enhancedvoting.com/results/public/<county>-county-tx/elections/<id>`.
  - Comal uses it from November 2025 on; none of the other 49 counties have elections there.
  - **Data feed:** `results/public/api/jurisdictions/<slug>` lists the elections, and `.../api/elections/<slug>/<id>/data` lists each election's reports (`publicReportCategories[].reports[].blobName`).
  - **Files:** each report is at `cdn/results/<jurisdictionId>/<blobName>`. These are summary and precinct PDFs, two Excel workbooks, and a JSON media export (`cdn/results/<slug>/export-<id>.json`).
- **Laserfiche public portal** — Smith keeps 2005–2026 results in folder "Elections/Election Archive/Election Results".
  - The folder listing is a JSON POST, the same method the roster downloader uses.
- **County websites** (PDF, plus CSV/XLSX/TXT/HTML exports in some counties). Content-management systems seen:
  - CivicPlus DocumentCenter / Archive.aspx: Bexar, Bowie, Henderson, Hays, Randall, Williamson, McLennan, Taylor and Ellis.
  - CIRA (`newtools.cira.state.tx.us`, `/page/...`): Ector, Grayson, Guadalupe, Liberty and Tom Green.
  - WordPress: Dallas, Cameron, Brazos, Galveston, Jefferson and Coryell.
  - Sitefinity: Collin.
  - Wix: Potter.
  - Custom sites: Harris and Montgomery.
  - Election-dated links were chosen by name. Links with no date in their name or link text were placed by the heading above them on the page, or by the date printed on the report's first page.

## Not downloaded

- **Brazoria, Johnson and Wichita** — their sites refuse non-browser requests: 403 for Brazoria and Johnson, a Cloudflare challenge for Wichita. These were not worked around.
- **Refused files** — 336 result files that sites refused or failed to serve are listed with their links in `election-results-manual-downloads.csv`, for download by hand:
  - McLennan: 225 files. The site refuses after bursts of requests.
  - Wichita: 42.
  - Guadalupe: 19.
  - Nueces: 19.
  - Williamson: 12.
  - Brazos: 11.
  - Hays: 4.
  - Angelina and Bell: 2 each.
  - Brazoria's results pages are `brazoriacountyclerktx.gov/departments/elections/election-results` and `/local-election-results`.
- **Hays cast vote record exports** (`CVRExport ... ZIP`) — skipped as not results reports. They exist for 2026 elections.
- **Excluded on purpose:**
  - Early-voting and absentee daily totals, election-night interim uploads, and rosters.
  - Notices, sample ballots, reconciliation forms, and hand-count audit certifications.
  - Unofficial reports when an official one for the same election was posted.

## Quality notes

- **Duplicates:** byte-identical copies within an election were removed, keeping the more descriptive name. Ector links most reports twice.
- **County linking errors:**
  - Bexar's 2022 "Precinct by Precinct Totals" link serves the 2023 report.
  - One Ector 2024 link serves the 2023 cumulative report.
  - Both wrong copies were removed.
  - Dallas named its March 2026 Democratic election-night PDF `...030325`, a 2025 date. Clarity covers P26, so that PDF was not kept.
- **Collin's PDFs** are stored inside a multipart upload envelope; the `%PDF` header comes after a few hundred bytes. They are saved exactly as served, and Acrobat and pdftotext read them.
- **Dallas:** the user's `P26_summary.zip` / `P26_detail*.zip` are byte-identical to `P26_Democratic_*`.
- **Scanned PDFs without a text layer:** Cameron (several), Walker G20 and Webb P24. They were kept on the strength of their link text.
- **Size:**
  - Cameron's 2024 primary precinct reports are 300–430 MB scans, and Collin's 2024 statement of votes cast is 566 MB.
  - Brazos posts a separate 60–90 MB precinct report per vote center (2025–26).
- **Year check:** a check of every county PDF's first-page date against its prefix found and fixed 16 mismatches. Canvass PDFs from other years, 2021–23 constitutional elections filed under primaries, and a Webb salary notice were removed.

## Refreshing

`Election_Results/_download_tools/results_download.py` (gitignored with the data) repeats this survey's
downloads and fetches only what is new:

```
python -I results_download.py                  # all counties, 2020-01-01 to today
python -I results_download.py --only Dallas,Tarrant --dry-run
python -I results_download.py --resolve        # also date undated links by reading the files
python -I results_download.py --archive        # re-query the Internet Archive for Clarity IDs
python -I results_download.py --retry-refused  # ask again for files a site refused on an earlier day
python -I results_download.py --qa             # PDFs whose first-page date disagrees with the code
```

It keeps `results_state.json` next to it. That file holds the Clarity election IDs, their prefixes and versions, and every file URL already handled, including removed non-results files, so those don't come back.

Clarity elections from the last 60 days are re-checked each run and re-downloaded when Clarity posts a new version, such as the canvass. County sources and their results pages are in the script's `COUNTIES` table. Travis is skipped unless `--include-travis` is given, because the user maintains that folder by hand.

Each run writes a log and `manual_downloads.csv` (refused files) to `Election_Results/_download_logs/`.

## Per county

"Uniform elections with nothing" lists the statewide and uniform elections from P20 through PR26
that have no file. A missing May (L) or June (LR) election often means the county held none or does
not post them.

| County | Source | Clarity elections | Other elections (PDF/CSV/XLSX/JSON) | Files | MB | Uniform elections with nothing | Notes |
|---|---|---|---|---:|---:|---|---|
| Angelina | angelinacounty.net/elections PDFs | - | P24 PR24 G24 L25 G25 P26 L26 | 20 | 15.5 | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 L23 G23 L24 PR26 | only 2024 onward posted (plus a few 2025-26 entity totals) |
| Bastrop | Clarity | P20 PR20 G20 L21 G21 P22 S22_0129 L22 PR22 G22 L23 LR23 G23 P24 L24 PR24 G24 L25 LR25 G25 P26 L26 PR26 | - | 97 | 1.2 | none | complete |
| Bell | Clarity | P20 PR20 G20 G21 P22 L22 PR22 G22 G23 P24 PR24 G24 G25 P26 PR26 | - | 59 | 2.6 | L21 L23 L24 L25 L26 | no May local elections found (Bell runs county/state elections on Clarity) |
| Bexar | elections.bexar.gov DocumentCenter (PDF/CSV/TXT/HTML) 2020-24; Clarity from May 2025 | L25 LR25 G25 P26 L26 PR26 | P20 PR20 G20 GR20 S21_0928 L21 LR21 G21 P22 L22 PR22 G22 L23 LR23 G23 P24 L24 PR24 LR24 G24 | 81 | 358.8 | none | 2022 precinct-totals link serves the 2023 report (county error; copy removed) |
| Bowie | bowiecounty.org DocumentCenter PDFs | - | G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 G25 P26 L26 PR26 | 61 | 16.1 | P20 PR20 G20 L21 L25 | 2020 not posted |
| Brazos | elections.brazoscountytx.gov/results PDFs (Hart canvass/cumulative; 2025-26 per-vote-center precinct reports, 30-90 MB each) | - | S20_0128 G20 GR20 S21_0306 S21_1201 L21 G21 P22 L22 PR22 G22 GR22 G23 P24 L24 PR24 G24 G25 P26 PR26 | 73 | 1964.2 | P20 PR20 L23 L25 L26 | 3 files refused (403) |
| Cameron | cameroncountytx.gov/elections/results PDFs/XLSX per entity | - | P20 PR20 G20 GR20 L21 LR21 G21 P22 L22 PR22 LR22 G22 L23 LR23 G23 P24 L24 PR24 LR24 G24 GR24 S25_0415 L25 LR25 G25 GR25 P26 L26 PR26 LR26 | 325 | 1661.7 | none | large scanned PDFs (2024 primary precinct reports 300-430 MB) |
| Collin | collincountytx.gov results archive PDFs + CSV exports | - | P20 S20_0929 PR20 G20 GR20 L21 LR21 G21 P22 S22_0115 S22_0129 S22_0326 PR22 G22 L23 LR23 G23 P24 L24 PR24 LR24 G24 GR24 L25 LR25 G25 P26 S26_0131 L26 PR26 LR26 | 86 | 903.2 | L22 | PDFs are stored inside a multipart upload envelope (PDF starts a few hundred bytes in; readers accept it) |
| Comal | Clarity 2020 - May 2025; Enhanced Voting (app.enhancedvoting.com) Nov 2025 - Jun 2026 | P20 PR20 G20 G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 | G25 P26 L26 PR26 LR26 | 92 | 14.9 | L21 | Nov 2025 has only the Enhanced Voting JSON media export |
| Coryell | coryellcountytax.com/results-previous-elections PDFs | - | PR20 G20 L21 G21 P22 L22 PR22 G23 P24 L24 PR24 G24 G25 P26 PR26 | 42 | 38.8 | P20 G22 L23 L25 L26 |  |
| Dallas | Clarity (county pages 2024+; 2020-23 IDs from the Internet Archive index) + PDFs for small runoffs | P20 S20_0128 PR20 G20 GR20 L21 LR21 G21 P22 L22 PR22 LR22 G22 L23 LR23 G23 P24 L24 PR24 LR24 G24 L25 LR25 G25 P26 L26 PR26 LR26 | GR24 GR25 S26_0131 S26_0314 | 138 | 70.0 | none | your P26_* files are byte-identical to P26_Democratic_* (that Clarity election is the Democratic primary) |
| Denton | Clarity | P20 S20_0714 S20_0929 PR20 G20 GR20 L21 LR21 G21 GR21 P22 S22_0129 S22_0326 L22 PR22 LR22 G22 GR22 L23 G23 P24 L24 PR24 LR24 G24 L25 LR25 G25 P26 S26_0131 L26 PR26 LR26 | - | 172 | 10.8 | none | complete |
| Ector | ectorcountytx.gov / CIRA pages PDFs | - | P20 PR20 G20 GR20 L21 G21 P22 L22 PR22 G22 L23 G23 P24 PR24 G24 L25 G25 P26 PR26 | 36 | 35.8 | L24 L26 | one 2024 link serves the 2023 report (copy removed) |
| El Paso | Clarity | P20 S20_0125 PR20 G20 GR20 L21 LR21 G21 GR21 P22 L22 PR22 G22 GR22 L23 LR23 G23 GR23 P24 S24_0120 L24 PR24 G24 GR24 L25 G25 GR25 P26 L26 PR26 | S21_1204 | 122 | 10.0 | none | complete |
| Ellis | co.ellis.tx.us / elliscountytx.gov per-election pages (two levels below Prior Election Results) | - | P20 PR20 GR20 S21_0202 S21_0727 S21_0831 L21 LR21 G21 P22 L22 PR22 LR22 G22 GR22 L23 LR23 G23 P24 L24 PR24 LR24 G24 GR24 L25 LR25 G25 P26 S26_0131 L26 PR26 | 97 | 383.1 | G20 | three 2020-22 links are dead (404) |
| Fort Bend | Clarity | P20 S20_0128 PR20 G20 GR20 S21_0220 L21 G21 GR21 P22 L22 PR22 LR22 G22 L23 LR23 G23 GR23 P24 L24 PR24 LR24 G24 GR24 L25 LR25 G25 GR25 P26 L26 PR26 LR26 | - | 135 | 10.4 | none | complete |
| Galveston | Clarity 2020 and 2024+; galvestonvotes.org historical PDFs/CSV 2021-23 | P20 PR20 G20 GR20 P24 S24_0113 S24_0217 L24 PR24 LR24 G24 L25 LR25 G25 P26 L26 PR26 LR26 | S21_0828 L21 LR21 G21 GR21 P22 LR22 G22 GR22 S23_0826 S23_0930 L23 G23 | 105 | 13.6 | L22 PR22 |  |
| Grayson | co.grayson.tx.us election history PDFs | - | P20 PR20 G20 L21 LR21 G21 P22 PR22 G22 L23 LR23 G23 L24 L25 G25 P26 L26 PR26 | 42 | 64.0 | L22 P24 PR24 G24 |  |
| Gregg | Clarity (county/state) + greggcountyvotes.gov PDFs (May locals) | P20 PR20 G20 G21 P22 L22 PR22 G22 G23 P24 G24 G25 P26 PR26 | L24 L25 L26 LR26 | 74 | 7.0 | L21 L23 PR24 |  |
| Guadalupe | guadalupetx.gov election history PDFs (per entity) | - | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 S23_0103 L23 G23 P24 L24 G24 GR25 P26 S26_0131 S26_0404 L26 PR26 LR26 | 113 | 39.1 | PR24 L25 G25 |  |
| Harris | harrisvotes.com cumulative + canvass PDFs | - | P20 S20_0128 PR20 G20 GR20 L21 LR21 G21 GR21 P22 S22_0125 L22 PR22 LR22 G22 L23 LR23 G23 GR23 P24 L24 PR24 LR24 G24 GR24 L25 LR25 G25 GR25 P26 L26 PR26 | 86 | 619.5 | none | complete |
| Hays | hayscountytx.gov Election Results Archive (2025+ only) | - | G25 GR25 P26 L26 PR26 | 23 | 13.3 | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 | 4 files refused (403 after rate limiting); CVR export ZIPs listed but not downloaded |
| Henderson | henderson-county.com archive PDFs | - | P20 PR20 G20 S21_0831 S21_0928 G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 | 40 | 25.3 | L21 |  |
| Hidalgo | Clarity | P20 PR20 G20 GR20 L21 LR21 G21 GR21 P22 S22_0810 L22 PR22 LR22 G22 L23 G23 P24 S24_0730 L24 PR24 LR24 G24 GR24 L25 G25 GR25 P26 L26 PR26 | - | 148 | 10.6 | none | complete |
| Hunt | Clarity (IDs from the county site + Internet Archive; Clarity's own list is empty) | P20 PR20 G20 L21 LR21 G21 P22 L22 PR22 G22 L23 G23 P24 S24_0130 L24 PR24 G24 L25 G25 P26 L26 PR26 | - | 92 | 1.9 | none | complete |
| Jefferson | jeffcotxvotes.gov | LR26 | L23 | 7 | 0.4 | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 | archived-results page links only the June 2026 Clarity runoff; a few 2023 PDFs |
| Kaufman | Clarity from Nov 2021 | G21 P22 L22 PR22 LR22 G22 L23 LR23 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 LR26 | - | 74 | 1.5 | P20 PR20 G20 L21 | 2020 - May 2021 not found |
| Liberty | co.liberty.tx.us (CIRA) PDFs | - | P20 GR20 L21 G21 P22 L22 PR22 G22 L23 LR23 G23 P24 L24 G24 S25_0724 L25 G25 P26 L26 PR26 | 80 | 59.4 | PR20 G20 PR24 | Clarity refuses the Liberty slug (403) |
| Lubbock | Clarity (IDs from votelubbock.gov + Internet Archive) | P20 PR20 G20 L21 LR21 G21 P22 L22 PR22 G22 L23 LR23 G23 P24 L24 LR24 G24 L25 G25 P26 S26_0627 S26_0801 L26 PR26 | LR26 | 120 | 4.5 | PR24 | PR24 not found |
| McLennan | mclennan.gov Past Election Results archive -> DocumentCenter PDFs per entity | - | P20 PR20 G20 L21 G21 P22 L22 PR22 LR22 G22 L23 G23 P24 P26 PR26 | 268 | 69.8 | L24 PR24 G24 L25 G25 L26 | site refuses after bursts of requests (403): 268 files saved; the rest (late 2023 - 2026) are in the manual list |
| Midland | Clarity from 2022; county PDFs 2020-22 | G22 G23 P24 L24 G24 L25 G25 P26 PR26 | P20 S20_0128 PR20 G20 G21 P22 L22 PR22 | 52 | 23.5 | L21 L23 PR24 L26 |  |
| Montgomery | elections.mctx.org/results PDFs | - | P20 L20 PR20 G20 GR20 L21 G21 P22 L22 PR22 LR22 G22 L23 LR23 G23 GR23 P24 L24 PR24 LR24 G24 L25 G25 GR25 P26 L26 PR26 | 129 | 107.0 | none | complete |
| Nueces | Clarity 2020+ (IDs from the Internet Archive) + nuecesco.com PDFs | P20 PR20 G20 GR20 G21 P22 L22 PR22 LR22 G22 GR22 GR23 PR24 G24 G25 GR25 P26 L26 PR26 | - | 61 | 4.1 | L21 L23 G23 P24 L24 L25 | 19 county files refused (403) |
| Orange | co.orange.tx.us elections page PDFs | - | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 | 22 | 2.4 | none |  |
| Parker | Clarity (11 elections, IDs from the Internet Archive) | G20 GR20 L21 LR21 G21 G24 L25 G25 P26 S26_0331 L26 | - | 44 | 1.1 | P20 PR20 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 PR26 | other years not found on the county site |
| Potter | pottercountytexasvotes.gov (Wix) PDFs | - | P20 PR20 G20 L21 P22 L22 PR22 G22 L23 LR23 G23 P24 PR24 G24 L25 LR25 G25 P26 PR26 | 71 | 15.7 | G21 L24 L26 | Clarity refuses the Potter slug (403) for its 6 archived IDs |
| Randall | randallcounty.gov DocumentCenter PDFs | - | P20 G20 L21 G21 P22 L22 PR22 G22 L23 LR23 G23 P24 G24 L25 LR25 G25 P26 PR26 | 75 | 18.1 | PR20 L24 PR24 L26 |  |
| Rockwall | Clarity | P20 PR20 G20 GR20 L21 G21 P22 L22 PR22 G22 L23 G23 GR23 P24 L24 PR24 LR24 G24 L25 G25 P26 L26 PR26 | - | 98 | 1.2 | none | complete |
| Smith | Laserfiche public portal (Elections/Election Archive/Election Results) | - | P20 PR20 G20 L21 G21 P22 L22 PR22 LR22 G22 L23 G23 P24 L24 PR24 LR24 G24 L25 LR25 G25 P26 L26 PR26 LR26 | 78 | 154.7 | none | complete |
| Tarrant | Clarity (county site links only the current election; 38 IDs from the Internet Archive) | P20 PR20 G20 GR20 S21_0227 S21_0327 S21_0727 S21_0807 L21 LR21 G21 P22 S22_0820 L22 PR22 LR22 G22 L23 LR23 G23 P24 S24_0831 L24 PR24 LR24 G24 GR24 L25 LR25 G25 GR25 P26 S26_0131 L26 PR26 LR26 | - | 151 | 47.2 | none | complete |
| Taylor | taylorcounty.texas.gov Archived Election Results | - | L22 | 1 | 0.1 | P20 PR20 G20 L21 G21 P22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 | links carry no dates and most files do not either; only L22 placed |
| Tom Green | tomgreencountytx.gov election results PDFs | - | P20 PR20 G20 L21 G21 P22 S22_0129 L22 PR22 G22 L23 G23 P24 L24 PR24 LR24 G24 L25 LR25 G25 P26 PR26 | 46 | 40.9 | L26 | year in the link text, month and day in the file name |
| Travis | your existing files (unchanged) | G22 GR22 L23 G23 P24 L24 PR24 LR24 G24 GR24 L25 G25 GR25 P26 L26 PR26 | P20 PR20 G20 GR20 L21 G21 P22 L22 PR22 LR22 | 92 | 65.7 | none |  |
| Victoria | Clarity (IDs from victoriacountytx.gov; Clarity's own list is empty) | P20 PR20 G20 S21_0703 S21_0821 L21 G21 P22 L22 PR22 G22 L23 G23 P24 S24_0203 L24 PR24 G24 L25 G25 P26 L26 PR26 | - | 91 | 1.4 | none | complete |
| Walker | co.walker.tx.us | - | G20 | 1 | 0.4 | P20 PR20 L21 G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 | only G20 found |
| Webb | webbcountytx.gov | - | P24 | 1 | 2.8 | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 L23 G23 L24 PR24 G24 L25 G25 P26 L26 PR26 | only one 2024 scan found; results not posted online |
| Williamson | wilcotx.gov DocumentCenter PDFs/TXT; Clarity PR26 | PR26 | P20 G20 GR20 S21_0306 LR21 G21 P22 L22 PR22 G22 L23 LR23 G23 L24 PR24 G24 | 26 | 233.0 | PR20 L21 P24 L25 G25 P26 L26 | 6 files refused (403) |
| Brazoria | brazoriacountyclerktx.gov/departments/elections/election-results and /local-election-results | - | - | 0 | 0.0 | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 | **not downloaded**: the site answers 403 to every non-browser request (201 result links seen) |
| Johnson | johnsoncountytx.org | - | - | 0 | 0.0 | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 | **not downloaded**: site answers 403 to non-browser requests |
| Wichita | wichitacountytx.com/election_info/election-results | - | - | 0 | 0.0 | P20 PR20 G20 L21 G21 P22 L22 PR22 G22 L23 G23 P24 L24 PR24 G24 L25 G25 P26 L26 PR26 | **not downloaded**: Cloudflare challenge (42 result links seen) |
