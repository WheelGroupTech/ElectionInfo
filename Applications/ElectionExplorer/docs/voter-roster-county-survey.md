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

## Open questions for the user

1. Order of counties to support (suggest Tarrant first — local files and layout PDFs —
   then Harris, Dallas, and the daily-CSV counties).
2. ~~Downloads~~ — done for every county that posts files (see "Downloaded rosters").
3. ~~Tarrant mail status~~ — keep every row (decided).
4. ~~Cumulative files with no voting date~~ — Date Voted left blank (Voting Totals shows
   "(no date)").
5. ~~PDF-only counties (Webb, Bexar)~~ — supported (see "PDF rosters").
6. The SOS portal: survey it now (it may cover every county for statewide elections) or
   later?
