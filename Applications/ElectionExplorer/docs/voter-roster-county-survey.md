# Texas voter roster survey — top 20 counties (2026-10-10)

Survey for extending Election Explorer's Voter Roster loader beyond Travis County
(see `voter-roster-design.md`). Sources: each county's election website (visited
2026-10-10) and, for Tarrant, the files the user copied to
`Voter_Rosters/TX_Tarrant_County/` plus the county's layout PDFs under
`M:\Elections\TX - Tarrant County Election Results and Data\<date>\Voter Rosters\`.
No files were downloaded from county sites; formats marked *(HEAD)* were confirmed with
an HTTP HEAD request, others are from link names or county notes.

Legal background: Texas Election Code §87.121 requires each county to keep an
in-person early-voting roster and a mail roster (name, address, VUID, precinct, date of
voting / date received), updated daily and posted by 11 a.m. the next day. For
**primaries and general elections for state and county officers**, §87.121(g) also has
counties submit the roster to the **Secretary of State**, who posts it — the SOS
"Election Information & Turnout Data" portal
(`goelect.txelections.civixapps.com/ivis-evr-ui/evr`), which Lubbock and Hays link to
for statewide elections.

## Summary

| County | Pop. | Where | Files per election | Format | Granularity | Methods | Party | History | Nov 3 2026 now |
|---|---|---|---|---|---|---|---|---|---|
| Harris | 4.84M | harrisvotes.com → Election Rosters (app.harrisvotes.com/ElectionResults/ElectionRosters) | `Cumulative_BBM_<code>.zip`, `Cumulative_EV_<code>.zip`, `VoterRoster - ELECTION DAY ONLY_<code>.zip`, **`<code>_OfficialRoster.zip`** (post-canvass, all methods) | ZIP *(HEAD)*, contents TBD | cumulative | mail, EV, ED; official roster | TBD | 10 pages (2024–) | BBM 9/24–10/9 posted |
| Dallas | 2.62M | dallascountyvotes.org → Election Results → Historical → election page | "In-Person Early Voter List", "Mail Ballots Returned", "Election Day Roster" (+ hidden precinct-range ZIPs `early-voting/<code>/Precincts_1000-1731.zip` …) | XLSX (names) | cumulative | EV, mail, ED | TBD | 2024– | links not live yet |
| Tarrant | 2.17M | (user copies) | see Tarrant section | TSV in ZIP | cumulative | mail, EV, ED (+ official all-methods roster) | column / file name | 2020– | mail posted |
| Bexar | 2.07M | elections.bexar.gov → "Unofficial Branch Daily Register / EV Roster" (DocumentCenter/Index/245; Archive AMID=37) | per day: Early Voting, Mail Ballots Received, Election Day, per party in primaries | mostly **PDF**, some CSV | daily | EV, mail, ED | file name | 2025– | — |
| Collin | 1.16M | collincountytx.gov/Elections/rosters | "Early Voting by Personal", "Early Voting by Mail" | TBD (posted from EV start) | daily | EV (incl. surrendered-mail voters), mail returned | TBD | none kept | from 10/20 |
| Denton | 0.98M | votedenton.gov → Early Voting and Election Day Rosters | `1126_10-08_Returned_Mail_Roster.xlsx` (+ .pdf); daily in-person EV and ED rosters "coming soon" | XLSX + PDF | dated snapshot (likely cumulative) | mail, EV, ED | TBD | none kept | mail posted |
| Fort Bend | 0.89M | fortbendcountytx.gov → Elections → Early Voting Statistics | `BBM` (`0609.xlsx`), `EV` (`0609_1.xlsx`) | XLSX *(HEAD)*; older years PDF | cumulative | mail returned, EV | TBD | current only | — |
| Hidalgo | 0.89M | hidalgocounty.us/3491 (and earlier pages 3371, 3281, …) | EV / ED / ABBM roster per party, "Cumulative" | XLSX *(HEAD)* | cumulative | EV, ED, mail | file name | 2014– (CSV/XLSX) | — |
| El Paso | 0.87M | epcountyvotestx.gov → Current Election | `Received_Ballots_Voter_List_<yyyymmddhhmmss>.csv` per day (+ EV rosters during EV) | CSV *(HEAD)* | daily | mail (EV later) | TBD | current only | mail daily |
| Montgomery | 0.68M | elections.mctx.org → Early Voting Roster | one download per date (JavaScript button); `rosterfileinstructions.pdf` | CSV | daily | **Vote Type** column: A mail, E early, L limited, E1/E3 late voting | TBD | current only | daily since 9/23 |
| Williamson | 0.67M | wilcotx.gov/elections → "Daily Voting Roster" (DocumentCenter) | "Voter Turnout – November 3 2026 JGSE" | TBD (HEAD refused) | daily | TBD | TBD | TBD | linked |
| Cameron | 0.43M | cameroncountytx.gov/elections/results | per entity / party, e.g. `01_2026_DEMOCRATIC_PRIMARY_RUNOFF5-22-2026.xlsx`, `General-2026-10-07-1.xlsx` (mail returned) | XLSX *(HEAD)* | dated snapshots | EV, mail | file name | 2026– | mail 10/7 |
| Brazoria | 0.39M | brazoriacountyclerktx.gov → Current Election Information → Voter Lists | per day: `2026-03-03-IN PERSON-REP`, `…-MAIL-DEM` | TBD (document links; HEAD refused) | daily | in person, mail | file name | 2023– | — |
| Bell | 0.39M | bellcountytx.com → Elections → Daily Early Voting Reports | per day: "Ballot by Mail", "In Person" (10/19–10/30) | TBD | daily | mail, EV | TBD | none kept | from 10/19 |
| Galveston | 0.36M | galvestonvotes.org → Current / Upcoming Elections | `NOV-3-2026-BALLOTS-RECEIVED-10.7.csv` per day | CSV | daily | mail (EV later) | TBD | none kept (results only) | mail daily |
| Nueces | 0.35M | nuecesco.com → Elections Department | none found (turnout PDFs only) | — | — | — | — | — | use SOS portal? |
| Lubbock | 0.32M | votelubbock.gov → Historical → "Early Voting and Election Day Rosters" per election | `Early-Voting-Roster-July-20-2026.csv` + `.pdf` per day; statewide elections link to the SOS portal instead | CSV + PDF | daily | EV (+ ED) | TBD | 2023– | (SOS portal) |
| Webb | 0.27M | webbcountytx.gov/420/Daily-Voted-List | per day "In Person Only", "Mail Ballot Received", "Election Day" | **PDF** | daily | EV, mail, ED | TBD | current only | mail 10/2–10/8 |
| Hays | 0.27M | hayscountytx.gov → Elections | none found; links the SOS portal | — | — | — | — | — | use SOS portal? |

## Tarrant County (local files, fully profiled)

All files are TAB-delimited text, one file per ZIP (two — `_Dem` / `_Rep` — in
primaries since 2026), **cumulative for the whole election** (not per day). Every row
has the VUID (`SOS Voter ID`), the county ID (`ID Number`), name, address, precinct,
`Party of Ballot Issued`, and district codes. Two layout generations:

**Layout 1 (2020 – P24)** — `a_reqexp_tc.txt` (mail; layout "ABSENTEE REQUEST FILE
EXPORT"), `ev_vtrex_tc.txt` (EV), `ed_vtrex_tc.txt` (ED):
- Name `"ROSS, MONICA M"` (mail) or `"ROBERT FRANCIS AARON"` (EV/ED) plus separate
  `Firstname` / `Middlename` / `Lastname`; `Precinct` 4 digits + `Precinct Subcode`.
- Dates: ED `Date Voted` (one value, the election day); EV **no date** (the layout's
  `Date Voted` column is not in the files); mail `Request Date`, `Print Date` — the
  layout PDF lists `Return Date` but the files' header has none.
- Mail has a `Return_Status Code` (OK 70,164 / NT / NC / BB / AI / NB / IF / IS / V6–V8 in
  G20 …) — rows are ballots with any status, not only accepted ones. Codes undocumented.
- **P24 `ev_vtrex_tc.txt` header is damaged**: the first four titles are blank
  (`\t\t\t\tCity…`).

**Layout 2 (G24 –)** — `Early_voting_in_person_report[_Redacted][_Dem|_Rep].txt`,
`Election_Day_in_person_report…`, `absentee_returned_voter_report…` (layout PDFs dated
07/20/2024):
- Name `"LAST, FIRST M"`; `Precinct` 6 digits (precinct + subcode, e.g. `100101`);
  from P26 extra trailing columns (`Address Line3/4`, `Zip_Code`, `Zip4`, `Firstname`,
  `Middlename`, `Lastname`, `Certificate`, `Voter_Name`), mostly empty.
- Dates: EV and ED **none**; mail `Date_Abs_Requested` and `Return Date`
  (MM/DD/YYYY in 2024, ISO `2026-09-23` in 2026).
- **Redacted voters**: rows with `--Redacted--` in the name / address / VUID fields
  (precinct and party kept): G24 EV 267, P26 77–281 per file, PR26 up to 641.
- **Party**: in the 2026 primary files `Party of Ballot Issued` is mostly `NPA` — the
  party comes only from the `_Dem` / `_Rep` file name; values also vary in case (`Npa`).

**Full Election History Roster** (post-canvass, all methods; e.g.
`DEM_Election Roster_Redacted.txt`, `REP_…` for P26): one file per party with a
`Vote_Type` column — **A** absentee/mail, **E** early, **F** early-voting provisional,
**Y** election day, **Z** election-day provisional, and **L** (limited; not in the
layout PDF, which says limited ballots are in E). P26: DEM E 122,028 / Y 62,066 /
A 4,806 / F 164 / Z 145 / L 134; REP E 91,486 / Y 51,059 / A 4,459 / F 128 / Z 70 /
L 48. This is the only Tarrant source with provisional and limited ballots.

Tarrant row counts (cumulative files): G20 mail 70,676 / EV 659,787 / ED 100,530;
G24 EV 634,997 / ED 162,741 / mail 25,518; P26 EV 122,154 DEM + 91,542 REP, ED 62,184 +
51,116, mail 4,904 + 4,593.

## What this means for the loader

Format families seen:

1. **Daily per-method spreadsheets in a ZIP** (Travis) — supported.
2. **Cumulative per-method delimited text** with rich headers (Tarrant; probably
   Harris).
3. **Cumulative spreadsheet per method** (Dallas, Hidalgo, Fort Bend, Denton, Cameron).
4. **Daily CSV / XLSX per method** (El Paso, Galveston, Lubbock, Montgomery, Brazoria,
   Bell, Bexar's CSVs).
5. **PDF only** (Webb; most of Bexar; Lubbock and Denton also post PDFs beside CSV /
   XLSX). `pdf_reader.c` (Hart CVR reports) could be the starting point.
6. **All-methods roster with a vote-type code** (Tarrant official roster A/E/F/Y/Z/L,
   Montgomery A/E/L/E1/E3, likely Harris's Official Roster).
7. **SOS statewide portal** for primaries / generals (fallback for Hays, Nueces, Lubbock
   statewide elections).

Generalizations needed in `voter_roster.c`:

- **Header-driven mapping** with more synonyms: VUID (`SOS Voter ID`, `SOS_VoterID`,
  `VUID`, `Voter ID`), county ID kept as an extra, name as one column (either order) or
  parts, address columns (new: rosters with addresses → the Address column and
  roster↔list address compare become possible), `Party of Ballot Issued`, precinct with
  separate subcode or 6-digit combined.
- **Voting method** from the file name (Travis words plus `ev_vtrex`, `ed_vtrex`,
  `a_reqexp`, `Early_voting_in_person`, `Election_Day_in_person`,
  `absentee_returned`, `Received_Ballots`, `BALLOTS-RECEIVED`, `IN PERSON`, `MAIL`,
  `BBM`, `ABBM`, `EV`, `ED`) **or** from a vote-type column (A/E/F/L/Y/Z, E1/E3 → mail,
  early, provisional, limited, election day).
- **Date** from a column (`Date Voted`, `Return Date`), the file name (many forms:
  `MM.DD.YYYY`, `YYYY-MM-DD`, `MM-DD-YYYY`, `M.D.YY`, `10.7`, `July-20-2026`,
  timestamp `20261008095902`), or none (cumulative EV / ED files: blank, or the election
  day for ED). Voting Totals then shows "(no date)" rows for those.
- **Party** from a column, the file name (`_Dem`/`_Rep`, `-DEM`/`-REP`, `Republican`), or
  the sheet name; the file name wins when the column says `NPA` in a primary.
- **Cumulative snapshots**: several dated cumulative files of one method (Hidalgo,
  Denton, Cameron) must not be added together — keep the latest snapshot per method
  (or merge identical records).
- **Redacted rows**: keep as records with no VUID (they count in precinct and Voting
  Totals but cannot be matched).
- **Damaged headers** (Tarrant P24): recover by matching the column count / data shape
  to the known layout, as Travis's missing-header recovery does.
- **Clerical errors seen**: file-name years off (`2027`, `2029`, `9.25.25`,
  `nov-3rd-2025-…-10.8.26`), misspellings (`recevied`), the same file posted under two
  dates (El Paso 10/09 → 10/08 file), broken links (`.csv%22` in Lubbock).

## Tarrant support (done 2026-10-10)

User decisions: Tarrant first; keep every mail row, with its return status as a column.
Changes (`voter_roster.c`, `csv_sheet.c` `EeCsv_ReadSheetMem`):

- Delimited text inside ZIPs is read (from memory).
- Header recognition: any "voter id" / "voterid" title; `_` counts as a space in
  titles (`Voter_Name`, `Vote_Type`); `Return Date` is a mail roster's Date Voted;
  `Vote Type` / `Vote_Type` codes (A, E, F, Y, Z, L, E1/E3, P) set the method per row
  (F / Z → Provisional, L → Limited) and are kept as a `Vote Type` column.
- Method words in file names: `absentee`, `reqexp`, `ev_vtrex`, `ed_vtrex`.
- Party from file-name words (`_Dem`, `-REP`, `Republican`) wins over the party column;
  `NP` / `NPA` in the column mean no party. Voting Totals splits by party only when at
  least half the voters have one (G24 mail's 15 stray DEM/REP rows no longer split a
  general election).
- Wide exports (15+ titled columns): untitled columns are kept as `Column N` (P24 ED's
  ballot-style column) instead of Notes; a repeated title becomes `City 2`; name-part
  columns override the full name only when they have a value.
- Tarrant 6-digit precincts (`100101`) → Precinct `1001` + `Precinct Subcode` `01`
  (when the file has no subcode column), matching the older files and the voter list.
- Protected voters — `--Redacted--` rows, and the older files' rows with no ID / name
  but a precinct — are kept as `(Redacted)` records with no Voter ID (load summary line).
- P24 `ev_vtrex_tc.txt`'s damaged header (first four titles blank) is repaired from the
  known Layout-1 Early Vote header (`repair_header`, counted as a recovered sheet).
- Load summary: undated records ("the county's file gives no voting date").
- **Bug fix (affects all counties):** selecting several ZIPs reallocated the open-ZIP
  array, but a miniz archive points at itself → use-after-free / crash (found with
  AddressSanitizer on Tarrant PR26). The array is now allocated once.

Validation: every Tarrant election 2020–2026 loads with row counts equal to an
independent Python parse (e.g. G20 70,676 / 659,787 / 100,530; G24 25,518 / 634,997 /
162,741; P26 DEM+REP 9,497 / 213,696 / 113,300 with the party from the file names), the
P26 full rosters give E 213,514 / Y 113,125 / A 9,265 / provisional 507 (F 292 + Z 215)
/ limited 182, and ZIP → TSV → reload is identical for all of them (and still for all 12
Travis ZIPs). Smoke test `tarrant`. In the app: G24's three ZIPs selected together →
823,256 records; Voting Totals one "(no date)" row + mail by return date.

## Open questions for the user

1. Order of counties to support (suggest Tarrant first — local files and layout PDFs —
   then Harris, Dallas, and the daily-CSV counties).
2. Permission to download one sample election per county (sizes listed when asking),
   or the user copies them as for Tarrant.
3. ~~Tarrant mail status~~ — keep every row (decided).
4. Cumulative files with no voting date: leave Date Voted blank, or use the election
   date for Election Day files?
5. PDF-only counties (Webb, Bexar): defer like Travis's Limited Ballot PDFs?
6. The SOS portal: survey it now (it may cover every county for statewide elections) or
   later?
