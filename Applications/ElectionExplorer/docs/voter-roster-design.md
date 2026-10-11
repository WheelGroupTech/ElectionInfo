# Voter rosters — design (Election Explorer 1.4)

Status: **plan — decisions made** (2026-10-10). Decisions D1–D4 are at the end.

## Goal

Texas counties publish, during an election, rosters of who has voted: mail ballots
received, early in-person voters, and (after the fact) Election Day voters. The format
is not specified by law, and every county differs. Election Explorer 1.4 adds a
**Voter Roster** window to load, browse, filter, report on, export and compare these
rosters, starting with **Travis County, TX**. Other top-10 Texas counties and the Texas
SOS roster data come after 1.4.

## What Travis County actually publishes (surveyed 2026-10-10)

12 ZIPs on hand (`Voter_Rosters/TX_Travis_County/`): G23, G24, G25, GR24, GR25, L24, L25,
L26, P24, P26, PR26, and the current G26 (through 10/08/2026). The legacy Python scripts
are in `Scripts/Travis_County_Elections/` (one variant per election).

**Files.** One `.xlsx` per day per voting method, named `<date> <method>.xlsx`:

| Filename method text | Voting Method | Notes |
|---|---|---|
| `Ballot By Mail` / `Ballot by Mail` | Mail Ballot | date = day the ballot was received |
| `Early Vote` | Early Vote In-Person | |
| `Election Day` | Election Day In-Person | |
| `Provisionals` | Provisional | L26 only so far |
| `Limited Ballot` (`.xlsx` in PR26) | Limited | names only, no VUID/precinct |
| `Limited Ballot` / `Poll List for Limited Ballot Voters` (`.pdf`, G25, P26) | Limited | SOS form 5-38; names are vector outlines, not text |

- Date formats: `MM.DD.YYYY` and `M.DD.YYYY`. Filename typos exist (`02.26.2027 Limited
  Ballot.pdf` in a 2026 election). Each sheet's title row 2 also carries a date, but it
  can differ from the filename (a correction's title shows the correction date), so the
  **filename date is authoritative**, with the title date as the fallback when the
  filename date is impossible.
- **Corrections:** `<name>_Updated.xlsx` / `_UPDATED.xlsx` sits beside the original
  (L26: 1, P26: 5, PR26: 7). The original is **not loaded** when its correction is
  present.
- **Primaries:** two sheets per workbook, named `Democrat`/`Democratic` and
  `Republican`; the title rows also say "Democratic Primary" / "Republican Primary
  Runoff"; P24 early-vote files also have a `Party Ballot` column (`DEM`/`REP`). Sheet
  names are otherwise unreliable (G25's Election Day sheet is named `EarlyVote`), so the
  voting method comes from the filename, never the sheet name.

**Sheet layout.** 3–4 title rows ("Travis County <method> Roster", date, [party
primary], election name), then a header row (row 4 or 5), then data. 9 header layouts
were seen, differing in column order and naming:

- `VUID, Precinct|PCT, First Name, Last Name` (G23–GR24, L24, L25 mail)
- `VUID, Last Name, First Name, PCT` (G25 onward, including G26)
- `VUID, First Name, Last Name, PCT` (PR26 mail)
- `Precinct, VUID, Voter Name` with `LAST,FIRST MIDDLE` (L25 early vote)
- `VUID, PCT, First Name, Last Name, Party Ballot` (P24)
- an unlabeled 5th column holding notes such as `CHAPTER 102` (G24/G25 mail)
- PR26 Limited Ballot: `No., Name of Voter, Name, Address` (assisting person), names in
  `First Last` order, mixed case, doubled spaces.

**Clerical errors found (all must load without manual cleanup):**

| Problem | Where | Handling (proposed) |
|---|---|---|
| Header row missing on one party sheet | P24 `02.27.2024 Early Vote` (Republican sheet) | Use the same file's other sheet's header when the column count matches; else infer by data shape (10-digit VUID column, 1–4-digit precinct column) |
| Whole file is a misnamed copy | P24 `03.05.2024 Early Vote.xlsx` = the Election Day roster (74,763 VUIDs, all also in `03.05.2024 Election Day.xlsx`, none in the daily early-vote files) | Skip the file, name it in the load summary (D1) |
| Pasted VUIDs with no name/precinct | P26 `02.20.2026 Ballot By Mail` (379 rows, duplicates of real rows) | Skip, count in the load summary |
| Footer totals rows | L25 `05.03.2025 Election Day` ("Total Voters / Total Suspense / …") | Skip (no valid VUID and no name) |
| "No ballots received." placeholder | P24 mail files | Skip |
| Duplicate VUIDs within / across files | G24 10, G25 3, G26 3, P26 380, PR26 1 | Keep and flag; totals count each voter once (D1) |
| Malformed VUID | PR26 9-digit `114229866`; L26 provisionals `00002_0420160812` (6 rows) | Keep as-is; flag in the load summary |

Unique voters per election for scale: G24 583,188; P26 273,739; G25 227,755; P24 152,217;
PR26 97,364; G23 70,035; L24 56,239; GR24 20,644; L25 17,770; L26 12,720; GR25 7,216.

## User-visible design

**File → Load Voter Roster…** in all three window types (voter list, CVR, roster). A
multi-select open dialog accepts `.zip`, `.xlsx`, `.csv`, `.tsv`, `.txt`. PDFs inside a ZIP are skipped
and listed in the load summary (D2). ZIPs are read in memory (miniz, as for Hart); several ZIPs or loose files
can be combined into one roster.

**Load progress dialog**: like the voter list/CVR dialogs (determinate bar, Cancel,
running "N voters" count), plus an **estimated time remaining drawn on the bar**
("About 1 min remaining"), from the observed rate once a few percent is done.

**Load summary** (shown when the window opens, like the CVR `load_note`): files
loaded per method, corrected originals skipped, rows skipped and why, malformed VUIDs,
duplicate VUIDs, and anything not loaded (e.g. PDFs).

**Voter Roster window** — a primary top-level window (own `AppState`, survives other
windows closing), **single pane** (one owner-data ListView, same bold/grey headers):

| Column | Source |
|---|---|
| Voter ID | VUID (blank for Limited) |
| Precinct | leading precinct number |
| Name | `Last, First Middle` (surname-first option, as in the voter list) |
| Voting Method | Mail Ballot / Early Vote In-Person / Election Day In-Person / Provisional / Limited |
| Date Voted | `MM/DD/YYYY` from the filename |
| *extra columns* | when present: Party, Notes, Assisting Person, Assisting Address, Source File |

Menus:

- **File:** Load Voter List…, Load Cast Vote Records…, Load Voter Roster…, Export Voter
  Roster… (UTF-8 CSV/TSV, no normalized option), Close Voter Roster, Exit.
- **Edit:** Copy, Options… (surname-first, zoom).
- **Filter:** Filter…, Reset Filter, Show Duplicate Voter IDs…, Reset View.
- **Reports:** Display Precinct Report…, Display Voting Totals….
- **Compare:** Compare with another open roster, or with an open voter list.
- **Help:** Loading Voter Rosters, Options, Filters, Reports, Compare, Export, About.

**Voting Totals report**: one row per Date Voted; columns Mail, Early Vote, Election
Day, Provisional, Limited, Total; a final Total row (overall count of voters). A
Provisional or Limited column with no source files shows "not available" rather than 0.
Each voter is counted once (D1). When a party is known (primaries), each date is broken
down by party with an overall total (D4).

**Compare**: matches by Voter ID.
- Roster vs roster: Only in A / Only in B / Identical / Changed (Precinct, Name, Voting
  Method, Date Voted).
- Roster vs voter list: Voter ID, Precinct and Name only (rosters have no address).
  Names are equal when first and last names match, ignoring middle names, case and
  punctuation; otherwise graded minor/major as today (D3).

**Round-trip test** (the acceptance test): load a Travis ZIP → export CSV → load the CSV
in another window → compare → everything Identical. Export columns are the window's
columns, so a CSV with exactly those headers reloads into the same roster.

## Implementation status

**Step 1 — engine: done (2026-10-10).** `src/voter_roster.{c,h}` (`EeRoster_LoadFiles`,
`EeRosterLoadInfo`, `EeRoster_MethodLabel`), plus `EeXlsx_ListSheetsMem` /
`EeXlsx_ReadSheetMem` (`xlsx.c`, workbooks from ZIP entries) and the
`EeVoterTable_Builder*` API (`voter_table.c`, builds a table through the normal header
classification so filters / duplicates / export / compare work unchanged).

- Output columns: `VUID, Precinct, Last Name, First Name, [Middle Name], [Name Suffix],
  Voting Method, Date Voted, [Party], [Notes], [Assisting Person], [Assisting Address],
  [other extras], Source File` (bracketed only when used). Notes / Assisting Person /
  Assisting Address have fixed positions so a roster and its export match.
- Rows are emitted in source order (date, then method, then name).
- Blank Voter IDs on named rows are kept and counted (G24 has 4 mail ballots so);
  `vuid_like` needs a 6+ digit run so title-row dates never read as Voter IDs.
- CSV/TSV inside a ZIP is not read yet (listed as unsupported).
- **Validated on all 12 Travis ZIPs:** every row matches an independent Python parse
  (VUID, precinct, names, method, date, party; the only difference is a surname
  "TRUE" that Excel stored as a boolean, which openpyxl reads as `True`), counts match
  the survey (G24 583,202 incl. 4 blank-VUID rows; P24 152,217 with the misnamed copy
  skipped and the missing header recovered; P26 273,740; PR26 97,414 incl. 49 Limited),
  and load → TSV export → reload is cell-for-cell identical for all 12. G24 loads in
  ~2 s. Smoke test `roster`.

**Step 2 — window: done (2026-10-10).** A roster window is the voter viewer in roster
mode (`AppState.is_roster`, created by `App_CreateViewerEx(..., TRUE)`), so filters,
Show Duplicate Voter IDs, Reset View, the Precinct report, copy, export and options
are the voter-list code unchanged.

- Single pane: the left (frozen) pane, the pane titles and the splitter are hidden. The
  right pane shows `roster_cols` (Voter ID, Precinct, Name from the normalized columns,
  then every source column except VUID and the name parts). `App_ScrollToTable` /
  `App_TableToScroll` / `App_ShowSortArrows` map pane columns for display, sorting, the
  context menu and sort arrows. The hidden left pane still mirrors the selection, so
  copy and export-selection work.
- Menus (`App_CreateRosterMenu`): File (Load Voter List / CVRs / Voter Roster, Close
  Voter Roster, Export Voter Roster, Exit), Edit (Copy, Options), Filter (Filter, Reset
  Filter, Show Duplicate Voter IDs, Reset View), Reports (Precinct, Load Summary),
  Compare (disabled placeholder until step 4), Help (Loading Voter Rosters, About).
  `IDM_FILE_OPEN_ROSTER` 40100 and `IDM_REPORT_LOAD_SUMMARY` 40101. "Load Voter
  Roster…" is also in the voter-list and CVR File menus.
- Options shows only surname-first + zoom. Export never asks about normalized columns
  (it writes the roster's own columns, which reload into an identical roster); default
  names `<zip>-Roster` / `-Filtered_Roster` / `-Selected_Roster`. The Filter dialog lists
  the displayed columns. Status bar says "voter records". Voter-list Compare menus skip
  roster windows, and opening a voter list from a roster window always opens a new window.
- Load: `App_BeginOpenRoster` (multi-select .zip/.xlsx/.csv/.tsv/.txt) runs
  `EeRoster_LoadFiles` on a worker thread behind a modal dialog: a running record count,
  a progress bar with the time remaining drawn on it (subclassed, double-buffered:
  "Estimating time remaining…" until 3% and 1.5 s, then "About N seconds/minutes
  remaining", updated at most twice a second), and Cancel. The load summary opens after
  loading when something was skipped or flagged (repeated / copy / unsupported files,
  recovered or skipped sheets, malformed / blank / duplicate Voter IDs), and is always
  available from Reports → Display Load Summary.
- Checked by driving the Debug build: G24 (583,202 records, summary: 4 blank IDs + 10
  duplicate IDs), P24 (summary lists the skipped misnamed copy and the recovered header),
  G26 duplicates view; Options, Filter, Precinct report; P24 → Export TSV → Load Voter
  Roster on the TSV → 152,217 records with the same columns; Cancel mid-load.

**Step 3 — Voting Totals: done (2026-10-10).** Reports → Display Voting Totals… in a
roster window (`IDM_REPORT_VOTING_TOTALS` 40102, `RosterTotalsWindow` in `main.c`).

- Engine `EeRoster_ComputeTotals` / `EeRoster_FreeTotals` (`voter_roster.c`, GUI-free):
  voters per Date Voted × Party × Voting Method over the whole table (like the Precinct
  report, not the filter). Each Voter ID is counted once on its earliest record (date,
  then method order; an unknown date sorts last); blank-VUID rows are each counted
  (D1). Unrecognized Voting Method values go to an "Other" column, shown only when
  present.
- Rows: one per date; in a primary (some row has a Party), one row per party that voted
  that day plus a bold "All parties" row when several did, then a Total row per party
  and a bold overall Total (D4).
- A method column shows counts when a record or a read sheet of that method exists
  (`EeRosterLoadInfo.method_read`, new); "not loaded" when its files were only
  found as PDFs (D2); otherwise "not available". The status bar gives the voter count,
  the repeat records not counted, and explains "not loaded".
- Numbers display with thousands separators; Copy (Ctrl+C / right-click, with a header
  row) and Export Selected / All (CSV/TSV) write plain numbers.
- Smoke test `rtotals` (repeat records earlier/later, blank VUID, no date, unknown
  method, two parties, a table without Party). Driven in the Debug build on G24, P24,
  PR26, L26, G26, P26, G25: G24 583,192 voters (583,188 unique IDs + 4 blank), P24
  DEM 100,277 + REP 51,940 = 152,217 matching the load's method counts, Limited "not
  loaded" for P26 and G25 (PDF poll lists), L26 Provisional 23.

**Step 4 — Compare: done (2026-10-10).** The Compare menu of every window lists the other
open voter lists and rosters (rosters labeled "(voter roster)"); the existing compare
summary / differences windows show the categories that apply to the pair.

- Engine: `EeVoterTable_CompareByVoterIdEx` / `EeVoterTable_CollectDifferencesEx`
  (`voter_table.c`) take `EeCompareOptions` and 16-bit classes; new bits
  `EE_CMP_METHOD_CHANGED` / `EE_CMP_DATE_CHANGED`. The old API is a wrapper (voter-list
  defaults), so list↔list compares are unchanged. `App_CompareOptions` (`main.c`) picks:
  - list↔list: Name (full), Address, Precinct when the address is unchanged (as before);
  - roster↔roster: Precinct, Name parts (last/first/middle/suffix, independent of the
    surname-first option), Voting Method, Date Voted; no Address. A Voter ID on several
    rows pairs with its closest record (identical, else fewest changed fields); blank
    Voter IDs match an otherwise identical blank-ID row;
  - roster↔list (D3): Precinct and Name; no Address. Names are equal when last name and
    the first word of the first name match on letters and digits (case, spaces,
    punctuation and middle names ignored). A list with one full-name column (Travis
    `NAME` = "LAST, FIRST MIDDLE") is parsed; "FIRST … LAST [JR/SR/II…]" too. Otherwise
    graded minor/major on "LAST FIRST" only.
  - Roster compares use a loose precinct match: an optional letter prefix and leading
    zeros are ignored (Travis lists say "P 267", rosters "267").
- **One-to-one pairing (user report, 2026-10-10):** identical counts differed by side
  (G26 roster vs 2026-10-05 list 1550 / 1548; 09-28 vs 10-05 lists 923,803 / 923,802)
  because a Voter ID on two rows of one file paired both rows with the other file's one
  row. Travis lists repeat a voter (CHUPICK 2203587625 in 09-28, MCCREA 1206552053 in
  10-05 with an old and a new address); the G26 roster has 3 such IDs. Now every compare
  (list↔list too) pairs rows one to one — identical pairs first, then the closest — and
  left-over rows get the new `EE_CMP_REPEATED` bit / "Voter ID repeated" category, so
  all matched categories count the same on both sides (G26 1548 / 1548 + 3 repeated;
  lists 923,802 / 923,802 + 1 / 1 repeated). The 8-bit API reports repeated rows as
  "only here".
- Smoke test `rcmp`. **Acceptance test passed in the app:** G24 ZIP → Export TSV → Load
  Voter Roster on the TSV → Compare: 583,202 identical on both sides, "No differing
  voters found" (P24: 152,217 identical). Roster↔list P26 vs the 2026-02-10 Travis list
  (920,251 voters): 272,260 identical, 448 precinct changes (moves), 42 name changes —
  all real: misspellings, married / hyphenated names, and county errors such as First
  "DE" / Last "GIL" for GIL DE LAMADRID and a blank last name — and 994 roster records
  whose Voter ID is not in that list.

**Step 5 — help, listing, version: done (2026-10-10).** Roster Help menu: Loading Voter
Rosters, Options, Filters, Reports, Compare, Export, About (`k_RosterHelp*` in `main.c`;
the IDM_HELP_* handlers pick the roster topic in a roster window; the filter rules text is
shared through `EE_HELP_FILTER_RULES`). The voter-list Compare and Loading topics mention
"Voter ID repeated" and Load Voter Roster. Version 1.4.0.0 (`res/ElectionExplorer.rc`,
`res/app.manifest`, `ElectionExplorer.Package/Package.appxmanifest`); `RELEASES.md` 1.4.0.0
"What's new" (1,055 characters); `STORE-LISTING.txt` gains "TRACK WHO HAS VOTED". Release
x64 and ARM64 build clean. The Store bundle is built after step 6.

**Added before release (2026-10-10, user request):**

- **Compare exports.** The Compare Summary has Export Summary… (Category, count in A,
  count in B) and Export All Records… (one row per record of both files: File, Voter ID,
  Precinct, Name, Address for two lists / Voting Method + Date Voted when a roster is
  involved, Result, Changed Fields) — buttons plus the right-click menu. The Differences
  window's right-click adds Export Selected / Export All (Voter ID, Field, value in A,
  value in B, in the order shown). All through `App_ExportReportModel` (UTF-8 CSV/TSV).
- **Filter → Show Duplicate Voters Voting…** (voter-list windows; enabled when the list
  has a birth-date column and a roster is loaded; a picker — the generalized sheet
  picker, `App_PickItem` — chooses among several rosters). Engine
  `EeVoterTable_MarkDuplicateVotersVoting` (`voter_table.c`): voters whose Voter ID is in
  the roster, grouped by the name + DOB rule of Show Duplicate Voters; a group needs two
  or more distinct Voter IDs; only the voters who voted are shown (user choice), sorted
  by name. P26 roster vs `p26_olvr_primary_voter_data_rep.csv` (920,114 voters, 0.23 s):
  one group — HERNANDEZ, ALEXA, DOB 05/03/2007, precinct 312, VUIDs 2222663146 and
  2223084032 (the county-confirmed case). Smoke test `dupvote`.

## Implementation plan

1. **Engine — `src/voter_roster.{c,h}` (GUI-free, unit-tested).** Reads ZIP / xlsx /
   csv / tsv into a table. Travis adapter: filename parsing (date, method, `_Updated`),
   header detection by keywords within the first ~10 rows, column mapping (VUID,
   precinct, first/last or combined name, party, notes), missing-header recovery,
   row validation (skip VUID-only / footer / placeholder rows), party from sheet name /
   title / column, correction suppression, duplicate and malformed-VUID accounting, and
   the load summary. Reuses `xlsx.c` (`EeXlsx_ReadSheet`), miniz, and `csv_sheet.c`.
   The output is an `EeVoterTable` whose normalized columns are Voter ID / Precinct /
   Name (the Address slot unused), followed by Voting Method, Date Voted and the extras.
   That reuses the voter list's filter, duplicate scan, Copy, export, value reports and
   compare engine instead of duplicating them.
2. **Roster window — `main.c`.** New window class with a single owner-data ListView,
   menus above, the load dialog with time remaining, the load summary, Export, Options.
   Add Load Voter Roster… to the voter-list and CVR File menus.
3. **Reports:** Precinct (existing value-count report) and the new Voting Totals
   window.
4. **Compare:** roster↔roster and roster↔list, via the existing compare engine with a
   column-set parameter (no Address; Voting Method/Date for roster↔roster) and the
   first + last name rule (D3).
5. **Help, store listing, RELEASES.md, version 1.4.0.0.**
6. **Tests:** smoke tests with in-memory xlsx/zip fixtures for each layout and error
   case above; real-data validation against all 12 Travis ZIPs (row counts vs the
   survey, G24 583,188 unique, P24 misnamed file, P26 corrections) and the CSV
   round-trip compare.

## Decisions (2026-10-10)

- **D1 — Duplicates:** load every row and flag duplicates (Show Duplicate Voter IDs
  reviews them); Voting Totals count each voter once, using the earliest record. A file
  whose voters are *all* already in another file of a different voting method (the P24
  misnamed copy) is skipped and named in the load summary.
- **D2 — Limited Ballot PDFs:** deferred past 1.4. PDFs are skipped and listed in the
  load summary; Limited ballots from `.xlsx` files (PR26) load. Revisit (Windows PDF
  rendering + OCR) when surveying other counties.
- **D3 — Roster ↔ voter-list names:** equal when first and last names match, ignoring
  middle names, case and punctuation; otherwise minor/major as today.
- **D4 — Party totals:** the Voting Totals report splits each date by party when party
  is known (primaries), with an overall total.
