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

## Harris support (done 2026-10-12)

Files (`Voter_Rosters/TX_Harris_County/`, 2020-2026):

- **Daily rosters** (CSV in ZIPs, plus a `DISCLAIMER.TXT`): `Cumulative_BBM_<code>.csv` (all
  mail ballots returned), `Cumulative_EV_<code>_<MM-DD-YY>.csv` (despite the name, *one day*
  of early voting per file: no voter repeats across days), `VoterRoster - ELECTION DAY
  ONLY_<code>_<n>.csv` (split at 100,000 rows; G20's is one `.xlsx`). Columns: `ElectionCode,
  IDNumber` (county certificate), `StateVoterID` (VUID), `Party` (D / R / `N~P`), `Name`
  ("LAST, FIRST MIDDLE"), `VoterAddress, VoterCity, VoterZIP`, [`BirthDate` (year), `Sex`,
  `VoteType` "Election Day"], `Precinct` (4 digits), `ActivityDate` (the voting day, or the
  day a mail ballot came back), `Dist02..Dist36` (district names). G26's mail file
  (`Cumulative_BBM_ToPost_1126.csv`) has only 8 columns.
- **Post-canvass voter history** (`<code>_VoterHistory[ (Set n of m)].xlsx`, G21-G25, P22,
  P24, with `Generic File Layout.pdf` and polling-place workbooks): `ELECTION_CODE, IDNUMBER,
  VUID, STATUS, VOTE_TYPE, FIRSTNAME, [MIDDLENAME,] LASTNAME, ADDRESS, ..., DATE_VOTED` (an
  Excel date), `ACTIVITY_DATE` (when the record was updated), `[OLDPRECINCT, NEWPRECINCT,]
  PRECINCT, PRECSUB, Dist01..`. Harris's VOTE_TYPE codes **differ from Tarrant's**:
  E = Election Day, P = early in person, M = mail, A = overseas / military, Z = provisional,
  1 / 2 = limited ballot in person / by mail (confirmed by DATE_VOTED: E rows on Election
  Day, P rows on early-voting days).
- **P26 official roster** (`0326_OfficialRoster_..._001_2026-07-09-14-36-38.xlsx`):
  `VoteType` is only "In Person" or "Mail" and `ActivityDate` is the posting date (late
  March / April), so it gives no voting method for in-person voters and no voting dates.

Loader changes (`voter_roster.c`, `xlsx.c`):

- File-name abbreviations as whole words: `BBM` / `ABBM` mail, `EV` early, `ED` Election Day
  (also Hidalgo's "EV Roster" / "ED Roster" / "ABBM LIST").
- A header with an `IDNumber` title (Tarrant writes "ID Number") reads vote types with
  Harris's codes; word values ("Election Day", "Mail") are read as words. An export's
  Voting Method label now wins over its kept Vote Type code (a mixed Tarrant + Harris
  export holds both schemes).
- `ActivityDate` becomes Date Voted when the sheet has no Date Voted column and no vote type
  other than Election Day; otherwise (the official roster) it stays an extra column.
- `N~P` = no party; `VoteType` title recognized; an exact `Precinct` title wins over
  `OLDPRECINCT`, and `NEWPRECINCT` fills a blank PRECINCT (1,872 truncated P22 rows).
- A `YYYY-MM-DD-hh-mm-ss` export timestamp in a file name is not a voting date.
- Reference files packed with rosters (disclaimer / read-me, file layout, "Poll Code"
  location lists) are not read; the load summary lists them (they do not open it).
- `xlsx.c`: SpreadsheetML parts whose elements carry a namespace prefix (`<x:row>`, the
  OpenXML SDK: G20's Election Day workbook, 440 MB of sheet XML) are normalized in place.

Validation: every daily roster ZIP P20-G26 loads with per-method counts equal to an
independent Python parse (e.g. G20 mail 176,453 / early 1,265,359 + Election Day xlsx
202,535; G24 62,332 / 1,177,333 / 319,501; G25 7,111 / 204,993 / 227,506; P26 20,013 /
333,010 / 213,381; G26 mail 14,008). Voter histories: G21 229,496, P22 358,910, G22
1,107,401, G23 452,555, P24 381,858, G24 1,566,985 (44 s), G25 441,163 (incl. provisional
and limited); P26 official 566,839. Round trips identical (G25 rosters, G25 history, P26
official, G26). Travis (12 ZIPs) and Tarrant (10 elections) load byte-identical to the
previous build. Smoke tests `harris`, `xlsxns`. In the app: G25's three ZIPs -> 439,610
records; Voting Totals per day matches the daily files.

## Dallas support (done 2026-10-12)

Files (`Voter_Rosters/TX_Dallas_County/`, P24, G24, G25, P26; P20 is a PDF, not read):

- **Mail returns report** (`Mail-Ballot-Returns-Report_<date>.xlsx`): a `TOTAL RECORDS` line,
  then `County Voter ID, State Voter ID, Name` ("LAST, FIRST MIDDLE"), `Address, City, ZIP,
  Precinct, PrecinctSub, Ballot Party` (NP / D / R), `Date Printed, Return Date` [P26 adds
  `Disposition Code / Description`; every row kept].
- **Early voting report** (`In-Person-Early-Voting-{Turnout,Details}-Report_<date>.xlsx`):
  `EV_SITE, IDNUMBER, FIRSTNAME, MIDDLENAME, LASTNAME`, street parts, `PRECINCT,
  PRECINCT_SUB, BALLOTSTYLE, BALLOT_PARTY / PARTY, DATE_VOTED, VOTING_UNIT, VOTING_SUBUNIT,
  SOS_VOTERID`.
- **Election Day "Website Roster"** (`Election-Day-Website-Roster*.xlsx`; G24's file name says
  "11.05.23"): a `Total Voters` line, then `SITE ID, County Voter ID, State Voter ID`, name and
  street parts, `PRECINCT, PRECINCT SUB, BALLOTSTYLE, PARTY, DATE VOTED`.
- **2026 layout** (P26 `InPersonEarlyVotingDetails`, `030326-ED-In-Person-Roster`): `Name`
  ("FIRST MIDDLE LAST"), `State ID, Address, Polling Place, Date, Precinct, Split / Check-in
  Party, Party / Ballot Style`.
- **Precinct ZIPs** (G24 / G25 `Precincts_1000-1731.zip`, 4 per election): one CSV each,
  early voting in person and by mail **again**, by precinct, as of a day of early voting
  (G24's stops at 10/31): `Precinct, Precinct Sub, StateIDNumber, Voter Name` ("LAST -
  FIRST MIDDLE"), `P=InPerson M=Mail`, `Date Ballot Request / Mailed / Returned` (DD-MON-YY),
  addresses and district codes / names. Two G24 rows have an unquoted comma in the address
  (later columns shift by one; the voter fields before it are intact).
- Dates in several workbooks (G25 Election Day / early voting, P24 Election Day, P26 mail)
  are Excel serials with no date format.

Loader changes (`voter_roster.c`):

- The Voter ID column is ranked: a state ID (`VUID`, `SOS Voter ID`, `State Voter ID`,
  `StateVoterID`, `State ID`, `StateIDNumber`) over a generic `Voter ID` over `County Voter
  ID` (kept as an extra column); `State ID` titles also mark the header row.
- Date Voted also from `Date` and `Date Ballot Returned`; `DD-MON-YY[YY]` dates; an Excel
  serial with no date format is converted in the Date Voted column and in extra columns
  titled "date" (written YYYY-MM-DD like a formatted one).
- "LAST - FIRST MIDDLE" names (a spaced dash; hyphenated surnames kept).
- The `P=InPerson M=Mail` column is a vote type whose title gives its legend (P = early in
  person, M = mail); vote-type schemes are now an enum (default / Harris / P-M).
- `Election-Day` in a file name; a party value that is only digits is no party (the shifted
  rows); a reloaded Source File keeps its own spacing (trimmed, not collapsed).

Validation: every Dallas workbook's rows, blank and repeated Voter IDs equal an independent
Python count (P24 9,783 / 104,978 / 116,848; G24 34,546 / 633,142 / 187,680; G25 2,757 /
97,837 / 137,395; P26 9,450 / 251,881 / 122,562); precinct ZIPs G24 511,724 (mail 21,839,
in person 489,883 + the 2 shifted rows) and G25 43,951 (1,163 / 42,788). Round trips
identical for all four elections' reports and both precinct sets. Travis, Tarrant and Harris
load byte-identical to the previous build. Smoke test `dallas`. In the app: G24's three
reports -> 855,368 records, 1,241 voters on two reports counted once in Voting Totals.

## PDF rosters (done 2026-10-12)

Bexar switched to PDF for its 2026 lists (G26 mail `ABBM_Recvd_<MMDDYYYY>.pdf`, printed
from Excel by Acrobat PDFMaker): `VUID, Last Name, Suffix, First Name, Middle Name, Ballot
Received Date, Precinct` (one file has a single `Name` column, "LAST,FIRST MIDDLE"). Cells
are centered (one file left-aligned); the header is on page 1 only and the column
positions are the same on every page. `pdftotext -layout` scrambles these rows (names on
the wrong lines); the PDF's own text runs do not.

- `pdf_reader.c`: each run now reports its font size and advance width (simple fonts'
  `/FirstChar` + `/Widths` / `/MissingWidth`, Type0 descendants' `/W` + `/DW`, TJ
  adjustments); positions are unchanged (Hart PDF reading unaffected).
- `voter_roster.c` `read_pdf_table`: loose PDFs are read as tables -- runs grouped into lines
  by baseline, the line with a Voter ID title is the header, each run goes to the header
  cell its span overlaps most (else the nearest center), pages without a header reuse the
  columns, a later page's repeated titles above its header and "Page N of M" lines are
  skipped. PDFs with no such header in their first pages (Dallas P20's turnout tables,
  scanned PDFs) are listed as not readable. (PDFs inside ZIPs and Limited Ballot poll
  lists are read since 2026-10-14: see "Limited Ballot PDFs".)
- Also: `Ballot Received Date` / `ReceivedDate` / `Date Received`, `CHECK-IN` / `Check-in
  Date` are Date Voted; `CERT` / `CERT #` is a (generic-rank) Voter ID; a "Voting Method"
  value that is not one of ours is read as words (Lubbock "MAIL-IN").
- Load Voter Roster's file filter includes `*.pdf` (user request). A PDF that cannot be read is
  listed in the load summary; when no selected file can be read, the error names the files
  and explains that only roster tables printed from a spreadsheet can be read.

Validation: Bexar's 7 G26 PDFs -> 2,129 mail records; per-file counts equal the date-bearing
lines of `pdftotext`, and every row equals `pdftotext -raw`'s row (names in order, date,
precinct); every VUID 10 digits, every suffix (117) in the suffix column. Webb's 3 PDFs
(Print To PDF, left-aligned) -> 24 (8 with the county's 8-digit IDs); Lubbock's P24
first-day PDF -> 1,610 (= raw rows; MAIL-IN 401, in person 1,209, dates from CHECK-IN).
Travis / Tarrant / Harris / Dallas load byte-identical to the previous build (only the
"cannot be read" note reworded). Smoke tests `pdfroster`, `hart`, `hartpdf`, `hartocr`.

## Remaining counties support (done 2026-10-12)

Hidalgo, Bexar (CSV), Brazoria, Montgomery, Lubbock and the G26-only counties (Galveston,
El Paso, Denton, Cameron, Williamson, Webb). Layouts are in "Downloaded rosters" below.

Loader changes (`voter_roster.c`), all generic (no county switch):

- **Method sources**, in order: a vote-type column; a method word in the file name (also
  `MAIL`, `IN PERSON` -- an "IN PERSON" file stays in person even under a mail list's
  `Ballot Received Date` title, Brazoria); a `SITE` value starting `EV-` / `ED-` (Hidalgo);
  a mail list's own columns (`RETURNDATE`, `Ballot Received Date`, `ReceivedDate`, `Status
  Date`, ...) -> Mail; otherwise the **Texas election calendar** (`method_from_calendar`):
  the date in the file name (numeric forms only -- a month-name date in a file name is the
  election's, not a voting day) on the election day is Election Day, within 17 days before
  it early voting (21 until 2026-10-14; early voting starts at most 17 days before) (Bexar's `... - 10-21-2024 (CSV)` files). November = Tuesday after the
  first Monday, March primary = first Tuesday, May = first Saturday, runoff = 4th Tuesday
  of May. Brazoria P22's "Voting Method 1" holds the party, so only a vote-type column
  blocks the calendar.
- **Header roles:** `VoterName` / `Voter Full Name` / `Name of ... Voter`, `CERT` (Hidalgo
  VUID), `RETURNDATE`, `CHECK-IN` / `Checkin Type`, `Status Date`, `Ballot Date`, `Received
  Date`, `CountyElectionPrecinct` (preferred over Montgomery's `Voting_Precinct`, which is a
  jurisdiction code on non-county rows); dates "11-FEB-20", "BBM Recvd Oct  9 2024
  1:00PM", month-name cells.
- **Montgomery** lists a voter once per jurisdiction on the ballot (`Election_Name`): rows
  with the same VUID, date, method and source file are merged into one record with the
  jurisdictions joined by "; " (note: "N rows merged"). Its "0" placeholder VUID on named
  voters becomes a blank Voter ID; named short numeric IDs (Webb's 8 digits) are kept as
  unusual.
- **Brazoria 2026** bilingual roster: title rows, a parenthesized Spanish translation row
  under the header (skipped), and the VUID and name columns swapped under their titles in
  some files (repaired per file; the note names them).
- **Galveston** names "LAST  FIRST MIDDLE" (one two-space gap): decided per sheet
  (`sheet_gap_comma`: most gapped names have 2+ words after the gap), so Dallas P26's
  "SANDRA  GLEDHILL" (an empty middle name) is still FIRST LAST.
- Empty sheets are ignored (not "no recognizable layout").

Validation (record counts = independent Python counts per file; round trips identical for
Montgomery G21 / G24, Brazoria P26, Hidalgo G22, Bexar G24, Bexar G26 PDFs, Galveston,
Lubbock P24 and El Paso + Denton + Cameron + Williamson together):

| County | Records per election |
|---|---|
| Hidalgo | P20 82,258; G20 221,042; G21 22,099; P22 58,858; G22 143,742; G23 39,042; P24 58,042; G24 217,831; G25 37,707; P26 86,763; G26 1,791 (mail) |
| Bexar | G23 108,567; P24 185,096; G24 785,582; G25 248,162; P26 329,488; G26 2,129 (PDF mail) |
| Brazoria | P22 42,593; G22 110,148; G23 30,724; P24 45,419; G24 162,796; G25 37,308; P26 62,227 |
| Montgomery | G21 20,943; P22 44,274; G22 141,108; G23 36,911; P24 50,640; G24 250,973 (679,232 rows merged); G25 34,578; P26 71,399; G26 3,033 |
| Lubbock | P24 35,854; G25 24,339 |
| G26 mail | Galveston 540; El Paso 2,658; Denton 1,279; Cameron 18; Webb 24 (PDF); Williamson 474 (mail, see below) |

Hidalgo G24 / G25 / P26 cumulative lists give no voting date (Date Voted blank). Travis,
Tarrant, Harris and Dallas load byte-identical to the previous build. Smoke test
`counties`.

Known leftovers: Bexar P26's 02-19 mail CSV has no header row (1,880 voters, listed as not
readable); Brazoria mail workbooks' headerless "rejected ballots" sheets are listed as not
readable. (Williamson's method, first left unknown, is read since 2026-10-14: see
"Williamson snapshots".)

## Williamson snapshots (2026-10-14)

The user downloaded a second Williamson "Voter Turnout" workbook
(`G26_VoterTurnout_261013_202610131840074642.xlsx`, 818 rows) beside the 10/10 one (474):
`VUID, Name ("LAST,FIRST MIDDLE"), Precinct, Election Name, Voting Method, Ballot Date`.
Each is a **cumulative snapshot**: the 10/13 file holds 461 of the 10/10 file's voters,
unchanged, plus 357 new (10/12 - 10/13); 13 voters dated 10/10 are no longer listed. Every
filled Voting Method is `AV` (absentee = mail: all dates 9/21 - 10/13, before in-person early
voting starts on 10/19); the 10/10 file leaves 285 method cells blank (the 10/13 file has `AV`
for the same voters).

- A Voting Method value `AV` is Mail Ballot (`method_from_column`).
- A Voting Method column whose filled cells all give one county code is the sheet's method,
  so its blank cells take it (`sheet_column_method`; not for our own labels, whose blanks in
  an export mean unknown).
- The calendar fallback had called the 10/13 rows early voting (21-day window); early voting
  starts at most 17 days before the election, so the window is now 17 days.
- Loading both snapshots keeps both copies of the 461 voters (D1: duplicates kept and
  flagged; Voting Totals counts each once). Load only the latest snapshot for the current
  list; load each one separately and Compare them to see the voters a later snapshot drops.

## Limited Ballot PDFs (done 2026-10-14)

Travis posts a Limited Ballot poll list for each day of early voting inside its ZIPs: G25
(12, `<date> Limited Ballot.pdf` / `<date> Poll List for Limited Ballot Voters.pdf`) and
P26 (11); PR26 has the same form as workbooks. It is the Secretary of State's form 5-38
("Poll List for Voters Voting Limited Ballot by Personal Appearance"): a title block with
the date of the election, numbered lines 1-50 with the Name of Voter (FIRST MIDDLE LAST
[SUFFIX]) and the Name / Address of a person assisting the voter. No VUID or precinct (a
limited ballot voter is not registered in the county); limited ballots are cast only during
early voting. The PDFs are text, not scanned (Microsoft Print To PDF, Word, Acrobat
PDFMaker); `pdftotext -layout` draws each name a line above its number, but the baselines
match.

- `voter_roster.c`: a PDF whose first page says "poll list" and "limited ballot" is read by
  `limited_form_row`: each line whose first mark lies left of the "Name of Voter" title is
  a row (digits, or a glyph with no text mapping -- Word's automatic numbering in P26's
  02.26.2026 file); runs left of the midpoint between that title and the assisting
  person's titles are the voter's name, the rest the assisting name / address (split at
  "Name  Address"'s center when one run holds both titles). The rows go out under the
  workbook form's titles (`No.`, `Name of Voter`, `Name`, `Address`), so `map_header`'s
  form handling applies to both. Titles are found across runs (`pdf_phrase`: Acrobat draws
  each word separately; one file draws "3/3/2026" in four pieces).
- Names: when Print To PDF draws the merge fields as separate runs ("AUDREY KATE" | "KATE"
  | "MCCAFFITY", an empty field as a double gap), they become "LAST, FIRST MIDDLE SUFFIX",
  so two-word surnames stay whole (PEREYRA REYES, THOMPSON ARISPE); a suffix typed into the
  last-name field ("BABIN Jr", "Jones, Jr.") or as its own field is the suffix. Names drawn
  as one run use the usual FIRST MIDDLE LAST split.
- Method Limited for the form whatever the file is called (also workbooks: a "Name of
  Voter" header with no Voter ID column); date from the file name unless it is not within
  17 days before the form's election date -- `02.26.2027 Limited Ballot.pdf` (P26; its
  names are not in the 02.26.2026 file, probably 02/27, the last day of early voting) gets
  a blank Date Voted and a note.
- ZIP-packed PDFs are now read (from memory: `EePdf_OpenMem`), as tables or forms; others
  are still listed as not readable. Load note: "N Limited Ballot poll lists ... read".

Validation: an independent Python reader (poppler `pdftotext -bbox`, words matched to row
numbers by baseline) gives G25 123 and P26 443 names; the loader gives the same rows in the
same order with identical names (only the comma of "Jones, Jr." / "THOMPSON, JR" dropped).
No form has an assisting person. Travis G25 227,758 -> 227,881 records (Limited 123), P26
273,740 -> 274,183 (Limited 443, 167 with no date); every other Travis, Tarrant, Harris,
Dallas and other-county load is byte-identical to the previous build (PR26 adds only the
note line). Smoke test `limitedpdf`.

## Texas Secretary of State rosters (done 2026-10-17)

The SOS "Election Information & Turnout Data" site
(`goelect.txelections.civixapps.com/ivis-evr-ui/evr`, an Angular app by Civix) lists the
statewide elections (primaries, runoffs, generals, constitutional amendment and special
elections, 2025 on) with their early-voting days. Per day it shows a turnout table by county
(registered voters, in person, by mail, cumulative %), with a **CSV download per county** and
a **Generate Statewide Report** button; Election Day has the same after the election. The
page builds the downloads from JSON (`{"upload": <base64>}`) returned by
`/api-ivis-system/api/v1/getFile` and `getFileByFormat`:

| Download | Request | Saved as |
|---|---|---|
| Statewide early voting | `getFile?type=EVR_STATEWIDE&electionId=<id>&electionDate=MM/DD/YYYY` | `STATEWIDE.<election>.EarlyVoting.<MM_DD_YYYY>.csv` (the roster's day) |
| One county, early voting | `getFileByFormat?type=EVR_EARLYVOTING&electionId=&electionDate=&county=&countyId=&format=csv\|zip` | `<COUNTY>.<election>.EarlyVoting.<MM_DD_YYYY>.<csv\|zip>` |
| Statewide Election Day | `getFile?type=EVR_STATEWIDE_ELECTIONDAY&...` | `STATEWIDE.<election>.ElectionDay.<download day>.zip` |
| One county, Election Day | `getFileByFormat?type=EVR_ELECTIONDAYTURNOUT&...` | `<COUNTY>.<election>.ElectionDay.<download day>.<fmt>` |

(G26 = election 53815; P26 REP 53813, DEM 53814; PR26 REP 58315, DEM 58314.) Statewide
requests for the last days of the P26 primary returned 502 and later requests 503: the
server builds the larger files slowly. The county downloads were not tried (503 at the
time); their layout is expected to match the statewide one.

**Early voting roster** (CSV, LF line ends, every field quoted, no BOM):
`COUNTY, VOTER_NAME ("LAST, FIRST MIDDLE [SUFFIX]"), ID_VOTER (VUID), VOTING_METHOD (MAIL-IN /
IN-PERSON), PRECINCT`, sorted by county. **Cumulative** ("Cumulative Totals Thru Close of
Business"): the user's 10/17 file (44,205, all MAIL-IN in 85 counties: no in-person early
voting yet) holds 31,534 of the 10/14 file's 31,750 voters (216 dropped, 7 changed). The SOS
also **revises earlier days**: its 10/14 roster had 38,994 voters on 10/17 (the user's copy,
downloaded 10/15, has 31,750), so a day's file depends on when it was downloaded. P26 REP on
02/17 (first day): 103,085 IN-PERSON + 4,547 MAIL-IN. Precincts: 1-5 digits, letter suffixes
(`2E`, `3S`), Bexar `99999`, a few blank.

**Election Day ZIP**: `STATEWIDE_VOTER_INFO.csv` (`COUNTY_NAME, VOTER_NAME, ID_VOTER,
VOTING_METHOD, PRECINCT`) and `STATEWIDE_POLLING_PLACE_INFO.csv` (`COUNTY, POLL PLACE ID, POLL
PLACE NAME, VOTER COUNT`: turnout per polling place, not a roster), the same entry names for
both parties. P26 DEM 802,979 (IN-PERSON 792,105, MAIL-IN 10,874), REP 812,852 (797,556 /
15,296), 254 counties each. The user's downloads of 09/05, 09/06 and 09/07 are byte-identical
per party (entries dated 2026-06-10: the file is built once after the canvass). Checked against
Travis's own P26 rosters: 101,069 of Travis's 101,090 Election Day voters are on it (21 not),
plus 206 voters in no Travis roster; its 488 Travis MAIL-IN rows are all mail ballots in
Travis's rosters. So it is the Election Day in-person roster plus some mail ballots.
**118 Voter IDs are on both parties' rosters** (69 in person in both, 47 mail in both, 2
mixed), worth checking against the counties' rosters.

Loader changes (`voter_roster.c`):

- `ID_VOTER` is a state Voter ID (`vuid_rank` 1, as in voter lists).
- A header with `ID_VOTER`, `VOTING_METHOD` and `COUNTY` / `COUNTY_NAME` is an SOS roster
  (`HeaderMap.sos_list`): Date Voted blank for early voting (its file-name date is a cumulative
  "through" date); on an Election Day roster IN-PERSON rows get the election's day from the
  election name (`sos_election_day`: primary = first Tuesday in March, primary runoff = fourth
  Tuesday in May, general = Tuesday after the first Monday in November; a special election or
  any other name stays blank; user decision 2026-10-17) and MAIL-IN rows no date (the
  file-name date is the download day); MAIL-IN → Mail (`method_from_column`);
  IN-PERSON takes the file's method (`EarlyVoting` / `ElectionDay`), else the calendar's for
  the file's date; the county column is kept as **County**. Party from the file name (`2026
  REPUBLICAN PRIMARY ELECTION`).
- A ZIP named `….EarlyVoting.…` / `….ElectionDay.…` names its `*VOTER_INFO*` entry after itself
  (`sos_entry_name`: `STATEWIDE.2026 DEMOCRATIC PRIMARY ELECTION.ElectionDay.09_06_2026.csv`),
  so the party comes from it and the DEM and REP entries are not taken for one repeated file;
  `*polling_place_info*` is a reference file.
- New for every county: ZIP entries in different ZIPs with the same size and CRC-32 are the
  same download saved twice (`SKIP_IDENTICAL`): the later input's copy is loaded, the other
  listed in the summary.
- Load summary: "N Texas Secretary of State early voting roster(s) read (cumulative through
  MM/DD/YYYY) …" (with "a later roster repeats an earlier one's voters" when two of one party
  are loaded), "N … Election Day roster(s) read: in-person voters get the day of the election
  named in the file name (03/03/2026) …" and, for a special election, "… no voting date"; their
  blank dates are not repeated in the generic "no Date Voted" line.

Validation: every row of both early voting CSVs and of the DEM + REP Election Day rosters
equals an independent Python parse (Voter ID, precinct, name parts, method, party, county),
except the loader's usual cleanup (47 of 1,615,831 Election Day rows: doubled spaces, `13.0`
precincts, a lone suffix word such as `GARRETT, IV`); P26 Election Day: all 1,589,661 in-person
rows dated 03/03/2026, all 26,170 mail rows blank; load → TSV export → reload identical; the four
ZIPs together load 1,615,831 records in 8 s (the 09/05 copies skipped as identical, the
polling-place files as reference). Every county roster file and election loads byte-identical
to the previous build (per-file and per-election). Smoke test `sosroster`.

Possible follow-ups: precinct numbers repeat across counties, so the Precinct report merges them in a
statewide roster (filter by County first); several daily early voting rosters could be read
as one roster where each voter's first appearance approximates the day voted.

## Downloaded rosters (2026-10-12)

Saved under `Voter_Rosters/TX_<County>_County/` as `<Code>_<county file name>`
(P20, G20, G21, P22, G22, G23, P24, G24, G25, P26, G26: November and March primary
elections 2020 to date; runoffs and local elections skipped). Untrusted data, never run.

| County | Files | Size | Elections | Format / layout notes |
|---|---|---|---|---|
| Harris | 39 | 1.4 GB | P20 G20 G21 P22 G22 G23 P24 G24 G25 P26 G26 (mail) | cumulative BBM / EV / ED ZIPs + post-canvass official rosters / voter history |
| Dallas | 21 | 205 MB | P20 (PDF) P24 G24 G25 P26 | XLSX per method; G24 / G25 `Precincts_*.zip` (the G26 page links the G25 ZIPs) |
| Bexar | 217 | 77 MB | G23 P24 G24 G25 P26 G26 (mail, PDF) | daily CSV: `Name` (full), `State ID`, `Time`, `Precinct`; per party in primaries; some XLSX |
| Brazoria | 317 | 32 MB | P22 G22 G23 P24 G24 G25 P26 | daily XLSX (G25 in-person CSV), `IN PERSON` / `MAIL`, party in file name; site blocks plain curl (needs browser headers) |
| Hidalgo | 128 | 98 MB | P20 G20 G21 P22 G22 G23 P24 G24 G25 P26 G26 (mail) | cumulative EV / ED CSV or XLSX (`CERT` = VUID, `CHECK-IN` date, `PRECINCT`, `SITE`, address); daily ABBM XLSX through 2022, cumulative after; per party in primaries |
| Montgomery | 260 | 20 MB | G21 P22 G22 G23 P24 G24 G25 P26 G26 | daily CSV in ZIP (`EVHistoryFiles/<ELEC>_<mm.dd.yyyy>.zip`, older ELIDs still listed): `VUID`, `Voted_Party`, `DateVoted` (`BBM Recvd Oct  9 2026  1:00PM`), `VoteType` A/E/…; P20 / G20 not listed; P22 02-22 is 404 |
| Lubbock | 25 | 4 MB | P24 G25 | daily CSV, a multi-line title in the first cell, header row 2: `CERT #`, names, `CHECK-IN`, `PRECINCT`, `VOTING METHOD`; other statewide elections only on the SOS portal |
| Galveston | 14 | 80 KB | G26 (mail) | daily CSV: `Selected - Election`, `Precinct` / `Precinct Name`, `VUID`, `Name`, `Ballot Received Date`, `Status` |
| El Paso | 3 | 160 KB | G26 (mail) | daily CSV: `VUID`, name parts, `Precinct`, `MailDate`, `ReceivedDate` (10/09 post repeats the 10/08 file) |
| Denton | 2 | 116 KB | G26 (mail) | cumulative XLSX snapshots 10-08 and 10-12 |
| Webb | 3 | 1 MB | G26 (mail) | PDF |
| Cameron | 1 | 8 KB | G26 (mail) | XLSX; archived files are turnout totals, not rosters |
| Williamson | 1 | 40 KB | G26 | XLSX; one document replaced daily (only the latest snapshot exists) |

Nothing available: Collin, Bell (posting starts with early voting on 10/19), Fort Bend
(still shows the June runoff; no archive), Nueces, Hays. Dallas 2020–2023: the old Clarity
site is gone and the Internet Archive was "Temporarily Offline" (retry later).

## Early voting update (2026-10-20)

Early voting for the November 3, 2026 general election began Monday 10/19. Downloaded on 10/20
into `Voter_Rosters/TX_<County>_County/` as `G26_<county file name>`; county files posted
under the same name every day (Harris, Tarrant) get `_2026-10-20` before the extension, El
Paso's undated early voting workbook its link text (`G26_Early Voting 10-19-2026_...`).

| County | New files (county site) | Loaded (10/19 in person / mail) | SOS county CSV 10/19 in person |
|---|---|---|---|
| Travis | user's `G26-Voter-Rosters_2026-10-19.zip` | 40,777 / 5,076 + 40 Limited (= county's 40,777) | 40,777 |
| Harris | `Cumulative_EV_ToPost_1126` (10/19), `Cumulative_BBM_ToPost_1126` (10/20) ZIPs | 104,659 / 24,794 | 7,031 (partial) |
| Dallas | In-Person-Early-Voting-Turnout-Report, Mail-Ballot-Returns-Report 10.19.26 (+ a turnout-by-location report: not a roster) | 76,243 / 11,239 | 103,056 |
| Tarrant | `Early_voting_in_person_report`, `absentee_returned_voter_report` ZIPs (site's ev_reports) | 50,409 (no date) / 11,935 | 17 (partial) |
| Bexar | mail PDFs 10/07-10/19, `voter_check_in_details 10-19-2026.pdf` (1,297 pages) | 42,776 / 14,614 | 30,707 |
| Collin (new) | `early-voting-through-october-19-2026.xlsx`, `absentee-returns-through-...xlsx` | 34,425 + 97 provisional / 3,883 | 5,936 |
| Denton | `1126_10-19_Early-Voting-Roster-1.xlsx`, `1126_10-19_Returned_Mail_Roster.xlsx` | 34,739 / 5,003 | 34,536 |
| Fort Bend (new) | `EV-10-19_1.xlsx`, `Mail-Ballot-List-2026-10-19.xlsx` | 31,831 / 2,337 | 1 (partial) |
| Hidalgo | `EV Roster November 3, 2026 (Cumulative).csv`, ABBM list (10/20) | 17,448 / 4,921 | 17,828 |
| El Paso | mail CSVs 10/09-10/17, early voting xlsx 10/19 | 17,516 / 25,319 (mail snapshots overlap) | 1,215 (partial) |
| Montgomery | daily ZIPs 10/12-10/19 | 13,568 / 5,387 + 11 Limited | 13,555 |
| Williamson | `VoterTurnout_261019b` snapshot | 25,694 / 4,524 (with the older snapshots) | 11,737 |
| Cameron | countywide `01_Cameron_County10-19-2026.xlsx`, `MAIL-GN26-...-CC-2026-10-19.xlsx` (per-entity files skipped: subsets) | 9,424 / 1,071 | 9,427 |
| Brazoria | `2026-10-19 IN PERSON`, mail 10/19 and 09-28 thru 10-16 | 14,322 / 2,388 | 14,312 |
| Bell (new) | `20261019_EV.pdf`, `20261019_BBM.pdf` | 7,111 / 1,685 | 7,094 |
| Galveston | mail CSVs 10/12-10/19, `Checkins_10192026.xlsx` | 13,654 / 2,042 | 6,248 |
| Webb | mail PDFs 10/09-10/19, `Check-in Report ... 10-19-2026.pdf` | 4,944 / 345 | 4,921 |
| Lubbock, Nueces, Hays (new) | none: the county links the SOS site | SOS files below | 8,855 / 1,586 / 10,251 |

The **SOS county CSVs** (`G26_<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv`,
`VOTER_NAME, ID_VOTER, VOTING_METHOD, PRECINCT`, no county column) were saved for all 20
counties for 10/14-10/19 where the SOS had data (an empty response = nothing reported that
day; 10/14-10/18 are mail only). Unlike the statewide report, a county CSV holds **only that
day's** voters: Travis's 10/14, 10/16 and 10/19 files share no Voter ID (141, 486, 1,464 mail;
40,777 in person on 10/19); 40,766 of Travis's 40,777 in-person voters are on the county's own
roster. The loader dates its rows from the file name (no county column, so not `sos_list`).
Several counties' SOS numbers are far behind their own rosters (Harris, Tarrant, Fort Bend, El
Paso, Galveston, Williamson, Bexar, Collin): for those counties use the county's files.

Loader fixes found with these files (`voter_roster.c`):

- **PDF tables** (`read_pdf_table`): a run under no column title goes to the nearest column
  not yet filled on its line. Bell's names are left-aligned under a centered `NAME`; a short
  name lay nearer the `SOS_VOTERID` title, was appended to the Voter ID cell and the row
  dropped as a non-voter (208 of 7,111). Bell now = `pdftotext -raw` (7,111 / 1,685 lines).
- **`CertificateNumber`** (Collin's and Webb's check-in reports: the VUID) is a Voter ID title
  (generic rank; Tarrant's plain `Certificate` is still not one); **`VoteDate`** is Date Voted.
- **Webb's mail list without VUIDs** (from `10-28-2026 - Mail Ballot Received (PDF).pdf`, added
  2026-10-30): `Certificate`, `LastName`, `FirstName`, `MiddleName`, `NameSuffix`, `ReceivedDate`. The
  `Certificate` value is the county's mail ballot certificate number (4-8 digits), not the VUID.
  A `Certificate` title beside a received-date title, with no Voter ID title, now marks the
  header (`note_cert_title`). The voters load with a blank Voter ID and the number in a
  `Certificate` extra column, and the load summary names the file. In such a PDF, a line whose
  Certificate cell is not a number is a title, not a voter (page 2 repeats no header). Checked
  2026-10-30: 52 records = the 52 numbers in the PDF, middle names 35 and suffixes 6 as printed.
  Re-reading the 34 counties' current files changed only Webb (+52 rows), and smoke test
  `county1028` was added.
- **Mail lists with unreturned ballots**: Collin's "absentee returns" workbook lists every
  ballot mailed, with `-` in `ReceivedDate` for the 10,257 not back yet; a mail list row
  whose received date is `-` is skipped and counted ("... sent but not received yet ... not
  loaded").

Independent counts: Galveston 13,654, Dallas 76,243, Williamson 28,926 rows (25,694 `EV` on
10/19), Collin 34,522 rows, Bexar 42,765 VUID lines + 11 blank, Bell 7,111 / 1,685 lines, all
equal to the loader. Smoke tests `counties` (Collin) and `pdfroster` (Bell layout).

## Counties 21-50 (2026-10-21)

The user extended tracking to the next 30 counties by population. Ranks are from the Census
Bureau's 2025 county estimates (`co-est2025-alldata.csv`, `POPESTIMATE2025`). The 20 counties
already tracked are exactly ranks 1-20 (Hays 19, Webb 20), and ranks 21-50 are the same set
in the 2024 estimates. All 30 are added to the daily downloader's SOS list. SOS county ids come
from `getFile?type=EVR_ELECTION` → `counties`. They follow FIPS order except **McLennan = 161**:
the SOS sorts "Mc" after McCulloch, FIPS after Lynn.

Surveyed 10/21. The 16 counties that post their own rosters each have a function in
`Voter_Rosters/_download_tools/daily_rosters.py`, which holds the page URL. ✓ marks a roster
seen to carry a VUID column (only a few were sampled).

| # | County | Own roster (format, posting) |
|---|---|---|
| 21 | McLennan | one PDF "List of Early Voters: In-Person and By-Mail" (CivicPlus doc 18980, re-posted under a timestamped name) |
| 22 | Jefferson | own rosters found 2026-10-24 (the user): daily `JeffersonCountyInPersonVotingMMDDYYYY.xls` (Voter_ID, Voter_Name) and a cumulative `JeffersonCountyMailBallotsReceived11032026-01012026toMMDDYYYY.xls` (election, VUID, name), Excel 97-2003 files on jeffcotxvotes.gov |
| 23 | Smith | daily PDFs "MM.DD.26 In Person / Absentee Voter List" in a Laserfiche public portal (folder 41; JSON listing needs the portal's cookies) |
| 24 | Brazos | daily PDFs "mail-MM-DD-2026" (from 9/23) and "in-person-MM-DD-2026" (`/roster/`); the 10/02 link points at the 9/30 file |
| 25 | Ellis | SOS only (page quotes §87.121) |
| 26 | Johnson | SOS only |
| 27 | Kaufman | SOS only |
| 28 | Comal | SOS only |
| 29 | Guadalupe | daily returned-mail PDFs; "Early Voting Ballots Cast" is a count table, not a roster |
| 30 | Midland | SOS only for G26 (it posted its own PDF in May) |
| 31 | Parker | SOS only |
| 32 | Ector | SOS only |
| 33 | Grayson | daily XLSX "October DD 2026 EV Check-in Report" / "Absentee Voter List" |
| 34 | Randall | two PDFs "Early Voting Roster" / "Mail Ballot Roster" (CivicPlus docs 4867 / 4827, re-posted daily) |
| 35 | Taylor | SOS only (links the SOS for in person) |
| 36 | Rockwall | SOS only |
| 37 | Wichita | daily PDFs "Voter-Check-ins-MM.DD.2026", "Absentee-MM.DD.2026", **behind a Cloudflare bot challenge**: not downloaded by the script, which lists them for a manual download |
| 38 | Gregg | daily PDFs "YYYY-MM-DD-Voter-Turnout" (check-ins: VUID, polling place, precinct) ✓ and "…-BBM-Voters" (VUID, precinct, mail and received dates) ✓ |
| 39 | Hunt | daily PDFs "Vote(r) History MM.DD.26" (date, VUID, name, precinct, polling place) ✓ |
| 40 | Liberty | SOS only |
| 41 | Tom Green | daily PDFs "MMDDYYRoster" / "MMDDYYBallotsByMail" |
| 42 | Bastrop | daily CSVs ("Monday-October-19-2.csv") + a mail CSV re-posted under a new name each day (`Mail-Ballots-14.csv`) |
| 43 | Potter | two PDFs "Early Voting Roster" (VUID, precinct) ✓ / "Mail Ballot Roster" ✓ (Wix file URLs change with each upload) |
| 44 | Bowie | daily CivicPlus PDFs "BBM Received M.D.2026" (from 9/22) and "M.D.2026 Early Vote History" |
| 45 | Victoria | cumulative "MM-DD-2026 mail and in person" PDF + XLSX (CivicPlus docs 2846 / 2847) |
| 46 | Henderson | daily CivicPlus PDFs "Early Voting October DD, 2026" |
| 47 | Angelina | daily PDF `voter_history_MMDDYY.pdf` |
| 48 | Orange | SOS only (no roster found) |
| 49 | Coryell | SOS only |
| 50 | Walker | SOS only |

The first download on 10/21 got 79 county files (16 counties).

### Loader support (2026-10-21)

All 16 load except Guadalupe's mail lists, which are scanned images (ScandAll PRO, no text
layer: listed as not readable). Checked against an independent count of 10-digit Voter IDs
per file (`pdftotext -raw`, CSV text, XLSX XML) and against the SOS county CSV for 10/19:

| County | Loaded (files of 10/21) | 10/19 in person: loaded / SOS |
|---|---|---|
| McLennan | 11,958 = IDs in the PDF; no method or date in the list | - / 4,912 |
| Smith | 5,863 + 5,982 in person, 1,308 + 1,568 mail (= IDs) | 5,863 / 5,827 |
| Brazos | 4,561 in person, 906 mail (17 daily PDFs, = IDs) | 4,561 / 4,563 |
| Grayson | 5,497 + 4,788 check-ins, 920 mail | 5,497 / 5,457 |
| Randall | 8,520 in person (10/19-10/20, from "Time"), 1,503 mail (rotated page) | 4,394 / 4,399 |
| Wichita | 2,945 + 2,783 check-ins, 698 mail (= IDs) | 2,945 / 2,942 |
| Gregg | 3,677 + 3,536 in person, 839 mail | 3,677 / 2 (SOS lags) |
| Hunt | 10/20 file 3,920 (cumulative, 10/19 + 10/20); 10/19 file 1,808 of 2,362 | 1,840 (10/20 file) / 1,798 |
| Tom Green | 2,634 in person, 1,131 mail (= IDs) | 2,634 / 2,620 |
| Bastrop | 3,143 + 2,888 in person, 824 mail (= IDs) | 3,143 / 3,143 |
| Potter | 4,148 in person (cumulative, no date), 812 mail (= IDs) | - / 2,179 |
| Bowie | 2,023 in person, 696 mail (20 daily PDFs) | 2,023 / 2,027 |
| Victoria | 2,872 (1,114 mail + 1,758 in person dated 10/20 only), PDF and XLSX alike | - / 1,655 |
| Henderson | 3,257 + 2,823 (= IDs) | 3,257 / 3,257 |
| Angelina | 2,566 (= IDs) | 2,566 / 2,566 |

Loader changes (`pdf_reader.c`, `voter_roster.c`):

- **TJ column gaps** (`EePdfPageText.split_gaps`, set by the roster reader only): Acrobat
  Distiller output (Randall, Gregg) draws each printed line as one TJ with the columns set
  apart by kerns; a word-gap kern now ends the run and the text after it starts a new run
  at its own position (off = a space, as before, for the Hart readers).
- **Rotated pages** (`EePdfTextRun.rot`): Randall's mail roster reads bottom to top; the
  runs are turned into the reading frame before lines are formed.
- **Cells**: a run under no title that is a Voter ID goes to the Voter ID column, a short
  number to the precinct column (Brazos); an untitled row number left of the table is
  dropped (Potter); runs that touch inside one clip join without a space, also in the
  header (Gregg "MiddleNam" + "e"; Hunt's touching titles are in separate clips); a line
  with "N of M" and no Voter ID is a page footer (Gregg); on a page without a header row,
  lines above its first Voter ID line are titles (Bowie).
- **Titles**: "Prct" (Smith), "SFX", "Vote Method" (Victoria; "IN PERSON" there = early
  voting or Election Day by the row's date, never the file name's "mail"), "ballot
  returned", "Ballot Received", "Ballot Status Date", "Date Rec'd", a bare "Voter" as the
  name when nothing else names it (Bowie), Hunt's printed-short "voter_state_vot"; a
  check-in "Time" / "checkin_time" holding date + time is Date Voted when the sheet has no
  date column and nearly every row has a date in it (not Bexar's mostly empty
  "Check-in Time").
- **Dates**: cell dates "M/D/YY" (Tom Green); file-name dates "MMDDYY" / "M.D.YY" when the
  name has no other date and no digit run of 7+ (Randall's and Hidalgo's upload stamps),
  and a weekday + month + day (Bastrop "Monday-October-19": the latest year, up to next
  year, in which that day was that weekday).
- **Stacked headers** (Grayson's check-in report): 3+ consecutive rows of one or two
  digit-free titles become one header, each title over the nearest data column at or left
  of it.

Known gaps: Hunt's revised 10/19 file switches to a shifted column order without a header
on pages 41-54 (554 rows left out, counted as non-voter rows; its 10/20 file lists 10/19
too). McLennan's list gives no method or date. Victoria re-posts one file each day; its 10/19
in-person voters were already gone on 10/21. Potter's cumulative early voting roster has no
dates: the downloader now names its snapshots `_YYYY-MM-DD-hh-mm-ss` (a save time, not a
voting day). Harris's `Cumulative_EV_ToPost_1126_10-19-26.csv` entries now get their
file-name date too (same rows, ordered by date).

Smoke test `county50` covers every rule above with synthetic files.

**Wrapped rows (2026-10-22, for 1.4.1.0):** Wichita's absentee list changed layout on 10/21
(precinct instead of received date; the election and the voter's name wrap onto a second line;
the row number on a third). A PDF line with no Voter ID that lies closer below the row above
than 0.85 of the page's row spacing (the median gap between lines that hold a Voter ID) now
continues that row, and a lone row number left of the table is dropped. Wichita 10/21: 898
voters (= its distinct IDs) instead of 993 rows with 95 empty ones; 10/19: full names, 1
skipped line instead of 699. Every other county loads byte-identical (regression incl. all 16
county 21-50 folders).

**Jefferson and .xls (2026-10-24):** Jefferson County posts its rosters as legacy Excel
97-2003 `.xls` files (BIFF8 in an OLE2 compound file), which Election Explorer could not read.
`src/xls.c` now reads them; `EeXlsx_ListSheetsMem` / `EeXlsx_ReadSheetMem` hand any compound file
to it, so rosters, voter lists and the sheet picker accept `.xls` like `.xlsx` (same cell text:
dates by number format, zero-pad formats, integers without ".0"). Jefferson's files carry only
VUID and name: in-person dates come from the file name, the method from the election calendar.
The mail list's name holds the election date and then a range (`11032026-01012026to10232026`):
a `<date>to<date>` range now gives the list date (the end), not the first date. Six in-person rows
mask the VUID as `**********` with the name shown: kept as voters with a blank Voter ID (they
were dropped as non-voter rows). Through 10/23: 29,106 in-person rows and 1,754 mail ballots,
matching an independent count; 99.94% agreement with the SOS files over 10/19-10/23. The
downloader's `jefferson()` takes the October / November uploads (the page also lists June's
Port Arthur runoff files).

**October 23 layouts (2026-10-24):** three files posted for 10/23 were misread. Hunt's "Vote History"
titles its polling-place column "Polling Pl" (cut short) and lists mail ballots there as "BBM": the
polling-site rule now takes any title starting "polling pl" (61 mail ballots that were counted as
in-person). Gregg prints long polling-place names past their cells, the last letter as a separate run in
another clip region, nearer the precinct column ("r 21" for precinct 21, 223 rows): a run of up to three
letters that starts where the previous run ends, in another clip, now continues that run's word
(`pdf_run_overflow`). Wichita's 10/23 absentee list prints each row number 2.4 pt below the record's
wrapped second line, so the number shared that line and, looking like a precinct, went into the
precinct ("P 402 1") and 50 of 971 voters were lost: a number lying wholly left of the table is now
always a row number unless the first column is the precinct. Regression: only those three counties
change. Smoke test `county1023`.

**Rows lost to a stray piece in the Voter ID column (2026-10-25):** checking why 11 files' loaded
VUID counts differed from a raw count of 10-digit numbers found 17 voters the loader dropped as non-voter
rows (the others were checker artifacts: date-time decimals and a phone number in Dallas's workbooks,
older same-named Harris ZIP entries; and Travis's deliberate skip of a row holding only a VUID). Gregg's
check-in PDFs draw some names in two touching pieces, or a name's last letter on its own, and the piece
lay nearer the "State ID" title; Gregg's mail list draws a middle name's last letter at the left margin
under "VUID"; Brazos puts "FED" (a federal-only ballot) between the "Pct" and "VUID" titles, where it
took the Voter ID column before the Voter ID. Now a run outside every title that touches the end of the
run drawn just before it continues that run's cell (replacing the October 23 clipped-letter rule), one to
three letters under "VUID" on a line with its Voter ID join the run drawn before them, and the line's
Voter ID claims its column first in the second pass. Regression: Brazos +1, Gregg +16 (and a middle
name's last letter no longer read as a suffix), one Angelina middle name now whole. Smoke test `pdfstray`.

**October 23-25 PDF layouts (2026-10-27):** the nightly report patched three counties' loads with
scripts; the loader now reads them itself. Henderson's October 23 roster titles its date column
"Timestamp": a "Timestamp" column is now a check-in time column (Date Voted when it holds a date in
nearly all rows, like Randall's "Time"). Its October 25 file, titled "Henderson County Voter Turnout",
loaded no rows: a title containing "county voter" ranked as a "County Voter ID" header, so the title line
became the header. Only a title starting "county voter" or containing "county voter id" ranks so now (441
rows, 10/25/2026). The same rule had taken Guadalupe's "Guadalupe County Voter Roster (per TEC 87.121)"
title as the header of its check-in reports (Distiller, one TJ per line, already split at the kerns by
`split_gaps`): Oct 19-23 now load 4,834 / 4,810 / 4,661 / 4,431 / 4,659 distinct 10-digit VUIDs, the
counts of the side comparison against the SOS (23,132 in both + 263 county-only). Besides these they hold
voters with no VUID (20-34 a day) and 7-digit county numbers (up to 11 a day); each file's "TOTAL" line
(read as the name "TOTAL") is now a non-voter row. Tom Green's October 25 mail list prints no column
titles: a page whose lines nearly all start with a 6- to 11-digit ID now has its columns named from their
contents (`pdf_untitled_columns`: VUID, then Date / Precinct / one to three name columns). The same file
prints its titles once, near the foot of page 15, below 45 rows that were then skipped as repeated
titles: lines above a later page's header that hold a Voter ID are now data (1,510 mail ballots, 3 with
9-digit county IDs). Regression (every Travis / Tarrant / Harris / Dallas election, the other counties'
files, each G26 PDF alone, the SOS CSVs): only Henderson, Tom Green and Guadalupe change, plus Fort
Bend's `EV-10-19_1.xlsx`, whose 38 "Total" subtotal rows had loaded as voters with no VUID (now non-voter
rows). Smoke test `county1026`.

## Open questions for the user

1. Order of counties to support (suggest Tarrant first — local files and layout PDFs —
   then Harris, Dallas, and the daily-CSV counties).
2. ~~Downloads~~ — done for every county that posts files (see "Downloaded rosters").
3. ~~Tarrant mail status~~ — keep every row (decided).
4. ~~Cumulative files with no voting date~~ — Date Voted left blank (Voting Totals shows
   "(no date)").
5. ~~PDF-only counties (Webb, Bexar)~~ — supported (see "PDF rosters").
6. ~~The SOS portal~~ — surveyed and supported (see "Texas Secretary of State rosters").
