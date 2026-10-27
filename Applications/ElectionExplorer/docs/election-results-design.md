# Election results — design (Election Explorer 1.5)

Status: **steps 1–8 done (engine, Clarity loaders, Election Results window, filter, reports, compare, export, all Clarity counties, ES&S and Hart PDFs)** (2026-10-26). Decisions D1–D8 are at the end.

## Goal

Counties report election results as an overall **summary** (cumulative totals per contest and
choice) and as **detail** (the same totals broken down by precinct, and usually by vote type:
mail, early voting, Election Day, provisional). Election Explorer 1.5 adds an **Election
Results** window to load, browse, filter by precinct, export and compare these results, against
another results window or against the tabulation of a Cast Vote Record (CVR) window. Travis
County, TX comes first. It publishes through Clarity Elections, which is also what most of the
other 49 surveyed counties use (`election-results-county-survey.md`).

1.5 also ships the filter export/load feature and the bug fixes already on `main`.

## What Clarity publishes (surveyed 2026-10-26)

Each election has four ZIPs, and each holds one file:

| ZIP | File | Precincts | Vote types | Party | Contest-level fields |
|---|---|---|---|---|---|
| `summary.zip` | `summary.csv` | no (counts only) | no | yes | Vote For, registered voters, ballots cast, precincts total/reporting, over/under votes |
| `detailtxt.zip` | `detail.txt` | yes | yes | no | Vote For (in the title) |
| `detailxls.zip` / `detailxlsx.zip` | `detail.xls` (BIFF8, through 2025) / `detail.xlsx` (2026 on) | yes | yes | no | Vote For (in the title) |
| `detailxml.zip` | `detail.xml` | yes | yes | yes (per choice) | Vote For, isQuestion, precincts reporting; election name, date, region, timestamp |

**XML is the richest file** and the only detail file with the election name, date and parties.
The other details add nothing the XML lacks, so they are loaded only to be cross-checked.

### Formats in detail

- **summary.csv:** header `line number, contest name, choice name, party name, total votes,
  percent of votes, registered voters, ballots cast, num Precinct total, num Precinct rptg, over
  votes, under votes`.
  - Contest names end in ` (Vote For N)`.
  - **Encoding varies:** 358 of the 471 surveyed files are UTF-8, and 113 are Windows-1252. In
    the cp1252 files, a character cp1252 lacks becomes `?`: Travis G24's "Pierre Huy Nguyễn" is
    `Nguy?n` in summary.csv but correct in detail.xml. The CSV reader already falls back to the
    ANSI code page; cross-checks and compares must treat a summary `?` as matching any one
    character.
  - Header variants across the 50 counties: `num Area` / `num County` instead of `num
    Precinct`; some files lack registered voters, ballots cast or over/under; 2 files have
    `Cumulative votes` columns (cumulative voting).
- **detail.txt:** fixed-width text with 30-character columns.
  - First a turnout table: `Precinct | Registered Voters | Ballots Cast | Voter Turnout`, ending
    in a `Total:` row.
  - Then one block per contest:
    - a centered title, `City of Bee Cave, Mayor ( 1)` (the number is Vote For);
    - a line of choice names, each starting at its group's column;
    - a header line repeating `<vote types…> Total` per choice, then an overall `Total`;
    - precinct rows, then `Totals:`.
  - **Only the precincts in the contest's district are listed.**
- **detail.xls(x):**
  - Sheet 1 is `Table of Contents` (page number → contest).
  - Sheet 2 is `Registered Voters`: the turnout table, and unlike the TXT it breaks Ballots Cast
    down by vote type.
  - Sheets 3 on hold one contest each:
    - row 1: the title;
    - row 2: choice names over merged cells;
    - row 3: `Precinct, Registered Voters, <vote types…>, Total Votes` per choice, then `Total`;
    - then the data, ending in a `Total:` row.
  - Sheet names are page numbers, so contest titles are never cut to Excel's 31-character
    limit.
- **detail.xml:**
  - The root is `ElectionResult`, holding `Timestamp`, `ElectionName`, `ElectionDate`, `Region`,
    and `VoterTurnout` (with per-precinct `totalVoters`, `ballotsCast` and `percentReporting`).
  - Then one `Contest key text voteFor isQuestion precinctsReporting` element per contest,
    containing:
    - `VoteType name="regVotersCounty"`: a per-precinct registered-voter placeholder, usually 0;
    - in some counties, contest-level `VoteType name="Undervotes"` / `"Overvotes"`;
    - `Choice key text party totalVotes`, each holding `VoteType name votes`, each holding
      `Precinct name votes`.

### What varies across the 50 counties (471 summary + 479 XML files)

- **Vote type names are free text set by each county.** The survey found about 40, for example
  `Absentee`, `Early`, `Election`, `Ballot by Mail`, `EV Mail`, `EV In-person`, `PROV/EV/ED`,
  `Limited` and `Election Day Pct 29`. They are kept **as loaded, in file order**: no fixed list,
  and no mapping onto the roster's voting methods.
- **Over/under votes:**
  - Travis reports none: they are 0 in summary.csv, with no Undervotes/Overvotes elements.
  - 213 files from other counties report them (Denton, Tarrant, Lubbock, Hidalgo and others), as
    summary columns and as XML contest-level vote types with precinct detail.
- **Primaries:** contest names carry the party prefix (`DEM United States Senator`). Clarity
  lists **every DEM contest, then every REP contest**, and the party name is `DEM`/`REP`.
  Generals use `(R)`/`(D)`/`NON`, and choice names carry `(R) ` prefixes.
- **Repeated contest names are separate races** (L26 has two "City of Bee Cave, City
  Councilmember at Large" contests, matching the CVR finding), so contests are identified by
  **position + name**, never by name alone.

### Cross-file agreement (Travis G24, P26, PR26, L26)

The summary and XML choice totals agree row for row in all four elections (283, 263, 20 and 160
choices). The only text difference is the cp1252 `?` noted above.

### CVR ↔ results naming (Travis)

| | CVR tabulation | Clarity results |
|---|---|---|
| Contest | `City of Bee Cave, Mayor (45918)`, `DEM United States Senator (43450)` | `City of Bee Cave, Mayor`, `DEM United States Senator` |
| Choice | `Kara King (EC12)`, `(R) Donald J. Trump (EC1)` | `Kara King`, `(R) Donald J. Trump` |
| Write-in | `write-in` (merged image + text) | `Write-in` |
| Over/under | `overvote`, `undervote` | Travis: not reported |
| Ballots | rows (minus multi-card continuation cards) | `ballots cast` 587,090 (G24) |

The match rule is: strip a trailing ` (<digits>)` from CVR contests and a trailing ` (<code>)`
(`EC12`, `E1`, …) from CVR choices; ignore case for write-in, overvote and undervote; collapse
whitespace. The G24 CVR tabulation saved earlier (`G24 CVR-All_Contests-1.csv`) already equals
the Clarity totals under this rule. One example is President: Harris 398,968 / Trump 170,781 /
Stein 5,886 / Oliver 4,631 / write-in 3,690.

## User-visible design (proposed)

### Loading

- **File → Load Election Results…** in every primary window: voter list, voter roster, CVR,
  and results.
- A standard multi-select open dialog. The filter is `*.zip;*.csv;*.tsv;*.txt;*.xml;*.xls;*.xlsx`.
- Accepted inputs:
  - any of the four Clarity ZIPs;
  - their unzipped files;
  - Election Explorer's own results export (CSV/TSV).
- The format is detected from the content, not the file name. Files are loaded on a worker
  thread behind the existing progress dialog.
- **Several files for one election:** the richest file is shown (XML > XLS/XLSX > TXT >
  summary), and **every file is cross-checked** against it:
  - per contest: choice totals, Vote For, and choice names (with the `?` rule);
  - overall: registered voters and ballots cast.
- Disagreements are listed in a **Load Summary** (Reports menu, opened automatically when there
  is one), naming the file, contest, choice and both values.
- **Files from different elections (D4):** the selected files are grouped by election and **one
  window opens per election**. The election is the XML election name and date when present;
  otherwise the file name prefix with the `_summary` / `_detail…` suffix removed (`G24_`,
  `P26_`), confirmed by matching contest lists.

### The Election Results window

- A primary, standalone top-level window, like the CVR window: its own `AppState`, it counts
  toward `App_MaybeQuit`, and it is closed with File → Close.
- An owner-data list with a bold/grey header, sortable by header click, and a status bar.
- Title: `<election name or file base name> — Election Results`.
- Default (**Totals**) view, one row per contest × choice, in file order:

  | Contest | Vote For | Choice | Party | *vote type 1…n* | Total Votes | Percent |
  |---|---|---|---|---|---|---|

  - Each contest is followed by its reported Over Votes and Under Votes rows, when the files
    report them (shown like the CVR report's special rows).
  - A Ballots Cast / Registered Voters line per contest when the summary carries it.
  - **Vote type columns are shown by default (D2).** There is one per vote type in the file, in
    file order, and an Options checkbox hides them. A summary-only load has none.
- **View → Totals / By Precinct (D1).** By Precinct shows the same list with a Precinct column:
  one row per contest × choice × precinct, holding only the precincts in the contest's district.
  Filter, sort, Copy and Export work the same in both views. This view is greyed out when no
  precinct detail is loaded.
- The status bar shows the election name and date, the number of contests and precincts, the
  source format(s), whether the results are filtered, and the Timestamp (XML) so unofficial and
  final snapshots can be told apart.

### Menus

- **File:**
  - Load Voter List… (Ctrl+O), Load Cast Vote Records…, Load Voter Roster…, Load Election
    Results…;
  - Close Election Results;
  - Export Election Results…;
  - Exit.
- **Edit:**
  - Copy (Ctrl+C; also by right-click), tab-separated, as in the other windows;
  - Options…: zoom (as the voter list), and **Party to display first**: *As in file* (default),
    or one of the parties found (DEM, REP, …), which moves that party's contests to the top.
    Persisted in HKCU, like the other settings.
- **Filter:**
  - Filter… (Ctrl+L): the CVR-style Include/Exclude rule dialog, with is/is not. The columns are
    **Precinct**, Contest, Choice, Party and Vote Type. Values come from drop-down lists of the
    loaded values.
  - Reset Filter.
  - A precinct filter re-tabulates the totals from only the included precincts. A contest or
    choice filter hides rows. A vote type filter re-totals from the included vote types.
  - Needs precinct detail: with only summary.csv loaded, the Precinct and Vote Type columns are
    greyed out.
  - The rules save and load as CSV / TSV filter files, like the voter and CVR filters (1.5 filter
    export/load).
- **Reports:**
  - Display Turnout Report…: precinct, registered voters, ballots cast (by vote type when the
    file has it), turnout %.
  - Display Load Summary….
- **View:** Totals / By Precinct; Reset View (back to the loaded row order after sorting).
- **Compare:**
  - Compare with `<window>`: one item for each open results window and each open CVR window.
  - The CVR is compared using its tabulation. **If the CVR window has an active filter, ask**
    whether to use all ballots or only the filtered ballots; with no filter, all ballots are
    used (D3).
  - **The CVR window gets a Compare menu too (D6):** Compare with `<results window>` opens the
    same compare window.
- **Help:**
  - Loading Election Results, Options, Filters, Reports, Compare, Export;
  - About.

### Export

- **File → Export Election Results…** and right-click **Export Selected…** write UTF-8 CSV
  (default) or TSV with a BOM, like every other export, in the current view and filter.
- **The export dialog offers two layouts (D5):**
  - **Displayed view:** exactly the visible columns and rows. Totals view → totals; By Precinct
    view → precinct rows. This is easy to read in Excel. A totals export reloads as a
    summary-only result, with no precinct detail.
  - **All detail (reloadable):** a lossless long form, one row per contest × choice × precinct
    × vote type. The columns are
    `Election, Date, Contest Order, Contest, Vote For, Choice, Party, Precinct, Vote Type, Votes`,
    plus Registered Voters / Ballots Cast turnout records and Over/Under Votes records. The
    filter limits which precincts, contests and vote types are written.
- Both layouts reload into a results window; the format is detected from the header row.
- No normalized option.

### Compare

- Opens a **Results Compare** window, with one row per contest × choice (plus Over/Under Votes
  and Ballots Cast):

  | Contest | Choice | This window | Other window | Difference | Status |
  |---|---|---|---|---|---|

  - Status is one of Match, Differs, Only here, Only there, or Not reported (for example, a CVR
    undervote count against Travis results that report none).
  - Show Differences Only toggle; Copy; Export.
  - Summary line: contests and choices compared, and how many match.
- **Results ↔ results:**
  - Contests are paired by order + name, choices by name (with the `?` rule).
  - A zero-vote choice and a missing choice are equal.
  - **When both sides have precinct detail, the compare also goes precinct by precinct (D7).**
    A precinct row that differs is listed under its contest × choice. When both sides have the
    same vote types, it is compared by vote type as well.
- **Results ↔ CVR:** compared with the CVR tabulation, matched by the CVR naming rule above.
  The CVR window's "Merge image and text write-ins" option must be on to match Clarity's single
  Write-in total.
  - Precinct-level compare is possible only when the CVR's Precinct column is not redacted
    (Travis redacts it).
  - Vote-type compare is possible only when the CVR has a voting type column (Travis's does
    not).
- **Acceptance tests (from the request):**
  1. Load Travis G24 → export CSV and TSV → load each in a new window → compare: identical.
  2. Travis G24 results ↔ the G24 CVR (6 files): exact for every candidate and write-in row.
     Over/under show as Not reported.
  3. The same for P26, PR26 and L26.

## Implementation status

### Step 1 — engine + Clarity loaders (done 2026-10-26)

- **`src/ee_results.{c,h}`.**
  - Model: `EeResults`, with contests, choices, precincts, vote types, sources and issues.
    `counts[choice][precinct][vote type]` covers each contest's district precincts.
  - `EeResults_LoadFiles`: ZIPs and unzipped files, with format sniffed from the content; grouping,
    cross-check, and taking missing figures from the other files.
  - `EeResults_Tabulate`: precinct and vote-type masks.
  - `EeResults_FormatDetailUtf8`: the reloadable long-form export, record types `Info`,
    `Vote Type`, `Registered Voters`, `Ballots Cast`, `Contest`, `Contest …` figures, `Choice`,
    `Over Votes`, `Under Votes`.
  - `EeResults_NamesMatch`, `EeResults_FormatLabel`.
- **Readers.**
  - `EeXlsx_ReadAllSheetsMem` / `EeXls_ReadAllSheetsMem` stream every worksheet in one pass,
    parsing shared strings and styles once. Clarity workbooks have one sheet per contest (141 in
    Travis G24), so the 64-sheet `EE_XLSX_MAX_SHEETS` limit does not apply here
    (`EE_XLSX_MAX_ALL_SHEETS` = 4096). The `.xls` sheet list is now a heap array.
  - Clarity's `detail.xls` up to 2024 is an **Excel 2003 XML Spreadsheet** (`<ss:Workbook>`), not
    BIFF; `ee_results.c` reads it.
- **Tests.** Smoke test `results`, plus the read-all check in `xls`; the whole suite passes.
  Release x64 and ARM64 build clean.
- **Validation:**
  - Travis G24, P26, PR26, L26: all four files agree; the CSV and TSV export round trips are
    identical; each file alone gives the same counts.
  - All 16 Travis Clarity elections: no differences between files.
  - **All 471 Clarity elections of the 50 counties:** all load, each as one election, with no
    crashes; every export round trip is identical; 350 have no notes at all.
  - Speed: all four G24 files load in about 0.2 s.

**Format variants found in the 50-county sweep (all handled):**

| Variant | Where | Handling |
|---|---|---|
| First grid column titled `County` / `Area`, or blank; rows `Precinct 1001` | Bastrop, Denton, Hidalgo, Fort Bend, Travis 2022–23, … | header found by `Registered Voters` in column 2, or by the area label |
| No Registered Voters column at all (`County \| Election \| Early \| Absentee \| Choice Total`) | Hunt LR21 | header = area label + titles |
| `Choice Total` instead of `Total` ending a choice's columns | Bastrop, Bell 2025 | a total label |
| Vote For suffix labeled `(Voter Turnout 1)`, or `(Vote For -1)` | Rockwall; Galveston P26 | any words before N; `-1` = not set up |
| `votes="protected"` / `totalVotes="protected"`: withheld counts | Galveston P26 (all 51,345 cells) | counted as 0, marked per cell, kept by the export; a withheld choice total stays unknown and is taken from the summary |
| Precinct name longer than its 30 TXT characters overwrites the next value(s) | Galveston G25 (`419 - BS1, BS2, BS13, and BS1763`) | the leftmost values are left blank (the TXT loses them; the cross-check reports them against the XML) |
| XML registered-voter total ≠ its own precinct sum (half of it in primaries) | Travis P24/P26/PR26/G25/L25, Dallas G20, … | noted once on the XML; the TXT/XLS totals (the precinct sum) are not flagged |

**Still to review with the user (step 7):**
- Most notes per election: Galveston P26 (105) / G25 (49) / PR26 (14), and Dallas L22 (61) /
  L21 (28).
- Dallas L21/L22: CFBISD choice names differ between files ("… - Ballot Oval Selection N").
- XLS ballots-cast-by-vote-type totals that are not their precinct sums.
- Summary over/under totals that differ from the XML's.

### Step 2 — the Election Results window (done 2026-10-26)

- **`main.c`, "Election Results window (1.5)" section.** `ResultsWindow` with
  `App_BeginOpenResults` (multi-select open dialog → worker thread → one window per election),
  `App_CreateResultsWindow` and `ResultsWndProc`.
  - The window is standalone, with its own resource-only `AppState` (`is_cvr_ui`); it is
    registered in the CVR window list, so it keeps the app alive and closes on Exit.
- **Menus:**
  - File: Load Voter List / CVR / Roster / Election Results, Close Election Results, Exit.
    Load Election Re&sults… was added to the voter, roster and CVR windows too.
  - Edit: Copy (Ctrl+C, also on right-click), Options….
  - View: Totals / By Precinct; By Precinct is greyed when no file has precinct detail.
  - View → Reset View (added after step 7) undoes a column sort, putting the rows back in the
    order they had when loaded; greyed while unsorted.
  - Reports: Display Load Summary….
  - Help: Loading Election Results, Views and Options, About.
- **List:**
  - Owner-data. Columns: Contest | Vote For | Choice | Party | [Precinct] | one per vote type |
    Total Votes | Percent.
  - Over/Under Votes rows when reported.
  - Click a column title to sort: counts sort largest first, precincts numerically.
  - Withheld cells show "protected"; a withheld total shows "withheld".
- **Contest order:** a primary's contests are ordered by the shared party-first setting
  (`cvr_tab_party_first`), so a results window and a CVR tabulation list the parties the same
  way.
- **Options** (modal): Zoom (the list font), Show a column for each vote type (new setting
  `results_show_vote_types`, registry `ResultsShowVoteTypes`, default on), and Party to display
  first. All three are saved.
- **Status bar:** election name and date, contests, precincts, rows (By Precinct), the source
  formats, "as of" the XML timestamp, and the number of differences or notes.
- **Load Summary:** the files (which one is shown), then each file's notes and differences. It
  opens by itself when another file disagrees with the shown one.
- **Engine change:** names (contest, choice, party, precinct, vote type) now have whitespace
  collapsed, as the CVR loader does ("John   Cornyn" in Travis P26).
- **Checked:**
  - The real app was driven headlessly. Travis G24 (four ZIPs) gives 283 rows, and 22,899 by
    precinct; a copied row matches the summary (Trump 170,781, 29.25%).
  - Travis P26 lists REP contests first.
  - Dallas L22 opens its Load Summary (20 differences).
  - Two elections in one load open two windows.
  - File → Close and Exit work.
  - Smoke suite passes (`settings` covers the new option); Release x64 and ARM64 build clean;
    the 471-election sweep is unchanged.
- **Not yet checked:** header-click sorting and a zoom change in the GUI.

### Step 3 — the Filter menu (done 2026-10-26)

- **Menu:** Filter → Filter… (Ctrl+L) / Reset Filter, between View and Reports, plus a
  Help → Filters topic.
- **Shared filter window.** The CVR window's "is / is not" rule filter is generalized:
  - `CvrFilterState` now describes a host: column titles, offered columns, a value collector, the
    applied rule set, an apply callback, the caption and the export base name.
  - `Filter_ShowRuleWindow` / `Filter_NewState` create it; `App_ShowCvrFilter` and
    `App_ShowResultsFilter` are the two hosts.
  - The CVR filter behaves as before (checked: Travis L26 CVR, Ballot Status is not "Not
    Reviewed" → 9 of 12,710).
- **Results filter columns: Precinct, Contest, Choice, Party, Vote Type.** Precinct and Vote
  Type are offered only when loaded.
  - Each column is filtered by its own rules (`Filter_ColumnAccepts`): an exclude that matches
    rejects; includes on a column are alternatives.
  - **Precinct rules** build a precinct mask, and every total, percentage and Over/Under figure
    is re-tabulated from the included precincts (`EeResults_Tabulate`). A contest with no
    included precinct is hidden; By Precinct shows only included precincts.
  - **Vote Type rules** build a vote-type mask: only included vote types are counted, and
    excluded ones lose their column.
  - **Contest / Choice / Party rules** hide rows. Totals and percentages stay those of the
    whole contest.
- **Status bar:** "N rows • filtered: X of Y precincts", plus "(some totals cannot be
  filtered)" when a contest has only summary totals or withheld cells.
- **Export Filter… / Load Filter…** use the shared `.csv` / `.tsv` filter files. The column
  titles are fixed, so a filter saved in one results window loads in another.
- **Checked against the XML (Travis G24):**
  - Precinct is 101 → Trump 4 + 150 + 61 + 2 + 0 = 217, 14.02% of the precinct's 1,548
    presidential votes.
  - Adding Vote Type is Early Voting → 150, 12.21%.
  - Adding Party is (R) → 13 rows.
  - Excluding precinct 101 → 170,781 − 217 = 170,564.
  - Reset restores all 283 rows.
  - Release x64 and ARM64 build clean.

### Step 4 — Reports (done 2026-10-26)

- **Reports → Display Turnout Report…** (`ResTurnoutWindow`, class
  `ElectionExplorerResultsTurnout`, `IDM_RES_TURNOUT`); greyed when there are no precincts.
  - Columns: Precinct | Registered Voters | ballots per vote type (when the files give them,
    and the vote-type option is on) | Ballots Cast | Turnout.
  - Header click sorts: numbers largest first, precincts numerically.
  - Ballots by vote type come from the Clarity XLS turnout sheet; the XML has none.
- **Rows** are the precincts the results filter includes. The report refreshes when the filter
  or options change (vote-type mask, zoom) and closes with its results window.
- **Status bar:** precincts, registered, ballots cast, turnout. Unfiltered it shows the file's
  reported totals; filtered, the sum of the precincts shown.
- **Right-click:** Copy; Include / Exclude the selected precincts (adds Precinct rules to the
  results filter); Export Selected / All (CSV / TSV, `<base>-All_Turnout`).
- **Display Load Summary…** stays under the same menu.
- **Help → Reports** topic.
- **Checked against the G24 XML:**
  - 287 precincts, 925,858 registered, 587,090 ballots cast, 63.41% turnout.
  - Precinct 100: 3,605 registered, 58 + 1,608 + 402 + 1 + 4 = 2,073 ballots, 57.50%.
  - Precincts 101 + 102 filtered → 6,604 registered, 3,664 ballots, 55.48%.
  - Closing the results window closes the report.
  - Release x64 and ARM64 build clean.
- **Not yet clicked:** the report's right-click menu (posted mouse messages do not open it), so
  Export Selected / All are untested in the GUI. They use the shared report exporter
  (`App_ExportReportModel`).

### Step 5 — Compare (done 2026-10-26)

- **Engine (`ee_results.c`):**
  - `EeResults_FromTally` turns a tabulation into results. It drops a CVR's contest
    "(digits)" and choice "(EC n)" ids, makes "overvote" / "undervote" the Over/Under Votes, uses
    a contest key so same-named contests stay apart, and gives precinct detail when items carry a
    precinct.
  - `EeResults_Compare` compares two results, each with its own precinct / vote-type masks:
    - contests by name (repeats in order), choices by name;
    - Over/Under Votes, Ballots Cast, Registered Voters;
    - per precinct, and per vote type when both have the same ones; only differing precinct and
      vote-type rows are kept.
    - A 0 and a missing figure are equal, so a contest nobody voted in matches its absence.
    - Statuses: Match / Differs / Only in this window / Only in the other window / Not
      reported.
  - `EeResults_NamesMatch` now also treats all quote marks alike: Clarity writes a nickname
    'Lulu', an ES&S CVR "Lulu". This was the only difference left in Travis P26 and G24.
  - `EeCvrTally` gained `group` (the contest's column group).
  - Smoke test `rescmp`.
- **UI (`main.c`):**
  - The results window has a **Compare** menu between Reports and Help, listing the other open
    results and CVR windows. The **CVR window has a Compare menu too**, listing results windows
    (D6).
  - A filtered CVR asks for its filtered ballots or all ballots (D3); CVR write-ins follow the
    CVR's "merge" option.
  - When the CVR's Precinct column has real data, its ballots are grouped by precinct
    (hashed) and tabulated per precinct, so the compare goes precinct by precinct (D7). Travis
    redacts the Precinct column, so its compares are totals.
  - A results window's filter applies to its side.
  - **Compare window** (`ResCompareWindow`, a standalone window registered like a CVR window):
    - columns Contest | Choice | Precinct | Vote Type | this window | other window | Difference
      | Status;
    - rows colored red (differs), yellow (only one side) or grey (not reported);
    - View → Show Differences Only; Copy; File → Export Compare… (CSV / TSV);
    - status "Identical: N figures match", or the counts, plus notes (results filter, write-ins
      not merged, multi-card CVR).
  - Help → Compare in both windows.
- **Acceptance (driven in the real app, both directions):**

  | Comparison | Result |
  |---|---|
  | Travis L26 results ↔ L26 CVR | identical, 161 figures |
  | PR26 ↔ CVR | identical, 21 figures |
  | P26 ↔ CVR | identical, 264 figures |
  | G24 ↔ 6-file CVR | identical, 284 figures (Ballots Cast 587,090 = 587,090) |
  | G24 XML ↔ G24 TXT (two results windows) | identical, 285 figures |

  - The only other rows are the CVR's over/under votes, "not reported" by Travis results.
  - G24's CVR loads in about 16 s and compares in under 1 s.
  - Smoke suite passes; Release x64 and ARM64 build clean; the 471-election sweep is unchanged.
- **Still open:**
  - A CVR with a real Precinct column (precinct-level CVR compare) has not been run on real
    data yet; the engine path is covered by `rescmp`.
  - Results ↔ its own export waits for step 6.

### Step 6 — Export (done 2026-10-26)

- **File → Export Election Results…** asks for the layout (D5); the last choice is kept for the
  session. Then the shared Save dialog (CSV / TSV, UTF-8 with BOM).
  - **Displayed view:** the shown columns and rows (Totals / By Precinct, filter, vote-type
    columns, sort), through `App_ExportReportModel`. Named `<base>-Results[_By_Precinct]` or
    `-Filtered_Results…`.
  - **All detail (reloadable):** `EeResults_FormatDetailUtf8` with the window's precinct and
    vote-type masks, and a contest mask of the contests showing a row. Named `<base>-All_Detail`
    or `-Filtered_Detail`.
  - **Right-click → Export Selected…:** the selected rows, displayed view.
  - **Help → Export.**
- **Engine:** a displayed-view export loads back as format `EE_RES_FMT_EE_VIEW` (`parse_ee_view`),
  detected by a first line with Contest, Choice and Total Votes.
  - Vote-type columns lie between Party/Precinct and Total Votes; "Over Votes" / "Under Votes"
    rows have no party.
  - Back-to-back same-named contests are split when a precinct comes back, a choice repeats in a
    precinct, or the percentages pass 100 (Totals).
  - A Totals file has no precinct or vote-type detail and no turnout.
  - Smoke test `resview`.
- **Acceptance (the user's export → load → compare test, driven in the real app):**

  | Election | All detail | Displayed Totals | Displayed By Precinct |
  |---|---|---|---|
  | Travis G24 (XML + summary + XLS) | 17 MB; identical, 285 figures | identical, 283 figures | identical, precinct and vote type level |
  | Travis P26 | 35 MB; identical, 265 figures | identical, 263 figures | identical |

  - "Ballots Cast not reported" is the only other row for the displayed-view files, which carry
    no turnout.
  - Smoke suite passes; Release x64 and ARM64 build clean; the 471-election sweep is unchanged.

### Step 7 — all Clarity counties (done 2026-10-26)

- **Sweep:** every Clarity election in `Election_Results/` (471, from the 50-county survey), each
  loaded with all its files (summary + TXT + XLS/XLSX + XML), then exported (All detail) and
  reloaded.
  - 471/471 load as one election each; every round trip is identical.
  - **389 load with no notes** (350 before the step-7 fixes). The other 82 carry only notes about
    the files themselves (below).
- **Format findings handled:**
  - **Withheld counts** (Galveston, Nueces, Fort Bend): `votes="protected"` cells, including
    contest-level Under/Overvotes. These figures are unknown, not 0, so the cross-check skips them
    and takes them from a file that has them.
  - **Registered voters of 0** (a county total of 0, or every precinct 0) means not reported.
  - **TXT names cut to 30 characters:** a precinct name fills its column, and the next value
    follows with no space. Data rows are read by fixed 30-character columns first.
  - **TXT header labels longer than 30 characters** ("Total Cumulative Ileana Garza-Rojas") are
    written whole and push the rest of the header right. Such a cell is extended to where a label
    already seen on the line starts.
  - **TXT line of spaces** between the choice names and the header (Hunt L26).
  - **Cumulative voting** (Dallas CFBISD, L21/L22):
    - The TXT gives one column group per ballot oval ("Kim Brady - Ballot Oval Selection
      1…3") plus a "Total Cumulative Kim Brady" column. The groups are merged into one choice.
    - The 2021 XLS puts the sums in the candidate's own group ("Total Cumulative Votes"), then
      bare "Ballot Oval Selection N" groups, which are dropped.
    - The summary has a "Cumulative votes" column and " 1"/" 2"/" 3" sub-rows.
    - Smoke test `rescum`.
  - **Vote For suffixes** "( 1)", "(Voter Turnout 1)" and "(Vote For -1)"; a first column titled
    Precinct, County, Area or blank.
- **Notes left, all genuine file content** (the same figure in every file, or a file's own
  arithmetic):
  - **Registered voters / ballots cast:** the reported total is not the sum of the precincts
    (70 elections). For example, Bell G22 reports 228,113 against a precinct sum of 270,664.
  - **2020 detail.xls turnout Total rows with the vote-type totals swapped**: Bastrop, Bell,
    Dallas, El Paso, Fort Bend, Rockwall and Victoria G20/GR20/PR20. For example, Dallas G20
    "EV In-person" says 118,670, but its precincts add up to 728,888; 118,670 is the Election
    Day sum.
  - **Withheld counts** (Galveston P26 51,345 cells; Nueces PR26, Fort Bend LR26 and others),
    noted once per file.
  - **Tarrant PR22:** ballots cast of 98,052 against a precinct sum of 62,480, in all three files.
- Smoke suite passes; Release x64 and ARM64 build clean.

### Step 8 — ES&S and Hart results report PDFs (done 2026-10-26)

- **New module `src/ee_results_pdf.{c,h}`** reads a report through `pdf_reader.c`'s positioned
  text into a plain model (`EeRpdfDoc`); `ee_results.c` (`parse_results_pdf`) turns it into
  `EeResults`. New formats: `EE_RES_FMT_ESS_SUMMARY` / `_ESS_PRECINCT` / `_HART_SUMMARY` /
  `_HART_PRECINCT`. A precinct report or canvass is shown ahead of a summary (rank of XLS).
- **ES&S Electionware** (JasperReports; detected by the "Report generated with Electionware"
  footer, not by the report title, which counties change freely):
  - Fonts: 13 pt page heading, 12 pt contest title, 10 pt "Vote For" and column headings, 8 pt
    rows. A heading can take two lines ("Election" over "Day").
  - Numbers are right-aligned and the number font has no glyph widths. Each number goes to the
    column whose right edge (the heading centre plus half the column pitch) is nearest its right
    edge, measured with Helvetica widths.
  - **Precinct report:** a fourth 13 pt line, set further below the heading (about 27 pt) than
    the heading's own lines are apart and alone at the left, names the precinct.
  - Rows: candidates keep their party code in the name ("REP Donald J. Trump", as the ES&S CVR
    writes it) and the code is also the party. "Write-In Totals" is the choice "Write-in"; the
    indented "Write-In: name" and "Not Assigned" rows are left out (a note counts them).
    "Overvotes", "Undervotes" and "Contest Totals" (summary: the contest's ballots) are read.
  - Statistics: "Registered Voters - Total" / "Ballots Cast - Total" (or "- Democratic Party" in
    a primary), ballots cast by vote type.
  - Quirks:
    - two cells shown as one string ("263 1,361": the first number keeps its place, the next
      goes to the next column);
    - a summary heading with a blank middle line;
    - percentages under TOTAL (turnout) are ignored.
  - **Contest order:** a precinct report meets contests precinct by precinct. They are put in
    the order every precinct agrees with (a topological sort), and contests that never share a
    ballot go in natural title order (District 10 before District 35).
  - Also, the cross-check's row alignment now pairs rows left over by the in-order pass by name
    and repeat number, so a file with contests in another order still groups and checks.
  - PDFs printed through PScript5 (L22 Cumulative, LR22, PR22 Republican Cumulative) have no text
    mapping and are refused.
- **Hart Verity** (Microsoft Reporting Services; detected by the Producer):
  - Every page starts with the same heading, ending with "Run Date". It also gives "<ballots> of
    <registered>", the election name (the small left line), the date and the run date.
  - **Cumulative:** a contest is a title (10 pt) and a "Choice Party <vote types> Total" table.
    Each vote type has a count and a percentage; the count goes to the column whose heading ends
    just right of it.
  - **Canvass:** one contest per page with turned (90°) column headings and a row per precinct,
    then "Totals". Columns that do not fit go on to a following page under the same title. It
    has no vote-type split, so it loads with no vote types.
  - Naming as the rest of the program and the Hart CVR:
    - "Ken Paxton, REP" is "Ken Paxton" with party REP;
    - " - Democratic Party" becomes the "DEM " prefix;
    - "- Vote for none, one, two, or three." becomes Vote For 3;
    - a primary's "110 - DEM" precinct is 110.
  - A county ZIP's Hart CSV and XLSX exports are skipped with a note (any ZIP member that is not
    a readable results file is skipped; the ZIP fails only when none is readable).
- **Compare with a CVR:** a Hart CVR files ballots under precinct + ballot style ("315BS14",
  "315-BS14", "110DEM"). New `EeResults_MatchPrecinctName` gives the results precinct a name
  stands for (exact, else the longest precinct name it starts with when no digit follows), and
  `Cvr_BuildResults` files the CVR's rows under it when comparing.
- **Results-wide figures:** `adopt_figures` now also takes registered voters, ballots cast and
  ballots by vote type from another file when the shown one lacks them (an ES&S precinct report
  whose statistics give only turnout).
- **Acceptance:**

  | Test | Result |
  |---|---|
  | Travis, 13 ES&S elections (2020–22), summary + precinct report each | one election each; precinct sums equal the summary except G20 below |
  | Travis G20 PDFs ↔ G20 CVR (612,696 ballots) | 399 figures match, precinct and vote type too; differences below |
  | Hays G25 / GR25 / L26 / P26 D+R / PR26 D+R, Hart canvass + cumulative | one election each, no differences |
  | Hays G25 PDFs ↔ G25 CVR | 345 figures match at every level, nothing differs |
  | Hays PR26 PDFs ↔ PR26 CVR (both parties in one CVR) | each party's contests match at every level |
  | Hays P26 PDFs ↔ pre-canvass CVRs | all contests and precincts pair; counts a little lower in the CVR, as expected |
  | Real app: G20 PDFs + CVR, Hays PR26 PDFs + CVR, Compare | as above |

  - **Travis G20, genuine file differences:**
    - "Directors Precinct 4 Barton Springs/Edwards Aquifer Conservation District" is missing
      from about 30 precincts of the county's precinct report (it sums to 30,800 where the
      summary and the CVR say 33,478);
    - the CVR has a second, smaller pair of "Board of Trustees Place 6/7 Pflugerville ISD"
      contests that the official reports leave out;
    - the PDFs print no over/under votes.
  - **Sweep of every results PDF in `Election_Results/`** (1,941 files, one at a time; no
    crashes):
    - **978 load** (975 of 1,914 Texas files):
      - ES&S summary and precinct reports from Guadalupe, Randall, McLennan, Montgomery, Potter,
        Liberty, Smith, Tom Green, Brazos, Henderson, Bowie, Travis, Bexar, Orange, Coryell, Ellis,
        Gregg, Collin, Grayson, Williamson, Lubbock and others;
      - Hart reports from Hays, Harris, Randall and others.
    - **Refused:**
      - 261 scanned PDFs with no text;
      - 30 encrypted;
      - 5 unreadable;
      - 36 Electionware layouts with no contest tables: Collin's "Statement of Votes Cast" (24),
        and Guadalupe, Ellis, Henderson and Smith extras;
      - 631 other layouts. The largest are:
        - an older Hart "Canvass Report — Total Voters — Official" with no PDF Producer
          (McLennan 200, Montgomery 65, Harris 14);
        - scans with an OCR text layer (Cameron 131, Ector 34, Grayson 25);
        - Smith's rotated Electionware "Custom Table Report" canvass (28);
        - Comal's "Detailed Results by Contest" (19).
    - Candidates for a later step: the older Hart canvass and the Electionware Statement of Votes
      Cast / custom canvass grids.
  - **Bexar G24 summary ↔ precinct report:** one election, 98 contests, candidate totals equal.
    The county's files differ slightly in write-ins (President 2,916 vs 2,921 over precincts) and
    ballots cast (763,198 vs 762,285). A Bexar precinct name can come back in a later section with
    its own statistics; its contests add to the same ones.
- Smoke test `respdf`; smoke suite passes; Release x64 and ARM64 build clean.

### Step 9 — more ES&S and Hart PDF layouts, and Travis's printed PDFs (planned 2026-10-27)

Goal: version 1.5 reads most of the results files in `Election_Results/`. The step 8 sweep loaded
978 of 1,941 PDFs. Step 9 adds the two largest groups of refused text PDFs from ES&S and Hart, plus
the three Travis County reports printed to PDF. Do the parts in order (9a is small), and after each
one rerun the full sweep and record the counts. Expected result: about 1,300 loads.

#### 9a — Electionware reports without the footer (72 files)

- **Files** (refused as "not a results report", sweep 2026-10-26):

  | County | Files | Elections |
  |---|---:|---|
  | Cameron | 49 | G23, L24, L25, LR25, P26, PR26 (one summary, sometimes one precinct report, per entity) |
  | Grayson | 13 | G25, L24, L26, PR26 |
  | Dallas | 7 | GR24, GR25, S26 (Election Summary) |
  | Williamson | 3 | G24, PR24 |

  Cameron has no loadable results PDF today, so this adds a top-20 county.
- **Why they are refused:** the layout is the step 8 Electionware summary / precinct report (13 pt
  title lines, STATISTICS block, "Registered Voters - Total", 12 pt contest titles over "Vote For"
  and the column headings). However, the "Report generated with Electionware" footer is missing.
  The Producer is iText 2.1.7 and the Creator is "JasperReports (<report name>)". Counties name
  the report freely ("Election Summary", "Precinct Summary", "05 Brownsville ISD", "02 CBR Sum").
- **Detection:** accept an iText + JasperReports PDF whose first pages show the Electionware
  structure (a STATISTICS block, or a 12 pt contest title followed by "Vote For"), whatever the
  title and with or without the footer. Keep refusing the Electionware "Custom Table Report" and
  "Statement of Votes Cast" grids (not in step 9).
- **Watch for:**
  - the title "Election Summary Results Report";
  - Grayson's "Election Day Precincts Reporting" and "Absentee/ Early Precincts Reporting" rows,
    and its constitutional-amendment-only report;
  - Williamson's vote-type headings that take two lines ("Early Voting In-Person");
  - Cameron's "pct split" reports (precinct splits as precinct names) and its per-entity files of
    one election, which must merge into one election.
- **Acceptance:**
  - each file loads into its election;
  - summary ↔ precinct cross-check is clean wherever a county posted both (Cameron G23 entities and
    L25 venue, Grayson G25 / L24 / L26 / PR26, Williamson PR24), or every difference is a genuine
    file difference recorded here;
  - sweep diff: the 978 step 8 loads are unchanged.
- **Done 2026-10-27 (uncommitted).** Detection as planned: a JasperReports Creator plus a
  "STATISTICS" block or a contest's "Vote For" line. Reading these files needed more than
  detection. Everything below applies to Electionware only; Hart reports read as in step 8.
  - **Text placement (`pdf_reader.c`, new `EePdfPageText.std_widths`, set only by the
    results parser):**
    - a font without widths (a standard 14 font) is measured as Helvetica;
    - the text position moves on after each string;
    - `split_gaps` also splits such fonts' TJ gaps.

    Cause: Smith's tagged re-saves draw "(TOTAL ) Tj [-831.5 (VOTE %)] TJ", and Cameron's
    summaries were edited in Acrobat, which draws "104" "," "443" one after another. The
    reader used to put every such string at the start point.
  - **Joining and splitting runs (`rp_items`):**
    - runs drawn from the same point are joined, and so are touching runs that are pieces
      of a number or have a space between them. Bowie's touching "Early" "Voting" stay two
      words: the space is inside "Early"'s width;
    - a string in the columns area with cells 2+ spaces apart is split into cells (Smith's
      "TOTAL  VOTE %  Absentee  Early  Election");
    - a row printed as one string may include the percentage ("4,219 59.75% 2,380 1,839").
  - **Widths:** estimates use Helvetica's AFM widths. Williamson's headings sit 1 pt apart.
  - **Column headings:**
    - lines up to 13 pt above TOTAL are part of the heading (Williamson's three lines
      "Election" / "Day In-" / "Person", Bowie's "Late" over "Mail/Correcti");
    - lines are joined at a hyphen ("Election Day In-Person");
    - pieces left of TOTAL are not headings.
  - **Stray words:** Smith 2025 has stray 9 pt words that the page does not show: "TOTAL"
    and "VOTE %" beside contest titles, "Statistics" on a heading line, and a "183" beside
    "Total Votes Cast". They are dropped.
  - **Word joins:** pieces that touch are one word ("City of Linda" + "le" by the font's
    widths, "H" + "ershal" by Helvetica's for a letter or two). A space shown at either end
    is kept ("Place " + "4 Chapel", "R." + " Wade Washmon").
  - **Page heading:** a status ("Final Unofficial") is not the election, and an election
    line that names the county ("May Primary Run-Off Elections of Williamson County, TX")
    is not the region.
  - **Williamson 2020-21 precinct reports:** the report first prints results by polling
    place, headed "Precinct 119 - Results" with the county's registered voters. Its vote
    centres take any precinct's voters. These pages are skipped with a note; the precinct
    pages follow.
  - **Grouping (`ee_results.c`):** two files that share half their contest titles pair
    leftover rows by name, even when the in-order pass matched little. Grayson's precinct
    reports order contests differently from its summaries.
- **Results:**
  - **Sweep:** 1,087 of 1,941 PDFs load (978 at step 8): 109 more (Cameron 53, Collin 24,
    Grayson 19, Dallas 7, Williamson 3, Ellis 2, Smith 1), none lost, no crashes. Collin's
    newly loading files include its Statement of Votes Cast reports.
  - **Changed readings:** 34 files that loaded before now read differently, all Electionware.
    - Williamson's precinct reports: step 8 read every contest as "Election".
    - Smith's reports with headings printed as one string.
    - Bowie G22, which now has 21 contests like its summary.
    - Collin LR26: its summary had only 3 of its 6 candidates.
    - Ellis PR20: duplicate vote types are gone.
  - **Still read poorly, before and after:**
    - Angelina P26 unofficial;
    - Orange G20 and PR20 (scans with an OCR text layer);
    - Henderson G21;
    - Smith G24 and G25 headings;
    - Bowie L26.
  - **Summary ↔ precinct cross-check:** clean for every pair listed above:
    - Cameron: 12 pairs;
    - Grayson: G25, L24, L26, PR26 (Democratic, Republican, county);
    - Bowie G22, which step 8 split into two elections;
    - Smith L22, L23 and PR22;
    - Collin G24 (Statement of Votes Cast ↔ accumulated totals), Collin PR24 and PR26, and
      Richardson L26.
  - **Genuine differences:**
    - Smith L25 Bullard ISD Place 6: the summary lists three candidates, whose votes add
      up to its own "Total Votes Cast" of 547; the precinct report lists five;
    - Collin's other precinct reports (G25, L26 joint, LR25, P26, S26, LR26) come out a few
      votes below their summaries in every contest. They leave out small precincts, and
      LR26's file is marked "(redacted)".
  - **Cameron's G23 summaries were edited after Electionware produced them** (Acrobat
    TouchUp text). In the Brownsville ISD summary, registered voters 109,961 became
    104,443 and ballots cast 9,156 became 7,428. The edited figures agree with the precinct
    report.
  - **Unchanged:** Travis G20 PDFs ↔ G20 CVR (399 figures match) and every Hart report.
  - **Builds:** smoke `respdf` gained a no-footer summary fixture; the smoke suite passes;
    Release x64 and ARM64 build clean.

#### 9b — older Hart reports, before Verity (about 250 files, 21 elections)

- **Files:** PDFs with no Producer and no Creator, from Hart's reporting before Verity (2020–2022):

  | County | Elections | Files |
  |---|---|---:|
  | McLennan | G20, G21, G22, L21, L22, LR22, P22, PR22: a Canvass, Cumulative and Precinct report per entity (county, each city and ISD) | 185 |
  | McLennan | P20, PR20 (Democratic and Republican): titles whose text runs are split up ("C", "umulative"), check the text layer | 12 |
  | McLennan | L21 Crawford ISD (Coryell County part): text that looks OCR'd ("ISO-", "15D-"), check | 3 |
  | Montgomery | G20, G21, GR20, L20, L21, L22, LR22, P20, P22, PR20, PR22: Canvass, Cumulative and Precinct | 45 |
  | Harris | G20, GR20: Canvass and Cumulative | ~4 |

  For all of these elections, no file loads today. Harris's other refused 2020 PDFs are Word
  documents and stay out of scope.
- **Layout** (seen in McLennan G20 / G21 and Montgomery G20):
  - **Heading on every page:**
    - "<entity> <Canvass | Cumulative | Precinct> Report — [Total Voters —] Official | Unofficial".
      Counties edit it ("City of Moody- Canvass", "Republican Party Cumulative Report").
    - "<County> — <election name> — <date>", "Page N of M" and the run date.
    - "Total Number of Voters : 98,898 of 149,737 = 66.05%" and "Precincts Reporting 91 of 91 =
      100.00%". Entity files add "Number of District Voters" and "District Precincts Reporting".
    - The em dash can come out as an unmapped character (McLennan G21), so detection must not
      depend on it.
  - **Canvass:**
    - per contest, a precinct grid with the columns Precinct, Early Ballots Cast, Election Ballots
      Cast, Total Ballots Cast, Registered Voters and Percent Turnout, then one column per choice;
    - choice headings wrap over two or three lines, with the party (LIB, REP, DEM, GRN) on top
      ("Donald J. Trump / Michael R. Pence");
    - "Continued" marks choices carried on to the next page, and a "Totals" row ends the grid.
  - **Cumulative and Precinct:** to survey at the start of 9b. Expected: choice rows with Early /
    Election Day / Total columns, repeated per precinct in the Precinct report. Montgomery G20's
    Precinct report has 731 pages.
- **Detection:** no Producer, plus a first page with the heading pattern and the "Total Number of
  Voters :" line. Do not rely on the file name or the exact title.
- **Reuse from step 8's Hart reader:**
  - naming: "Name, REP" → party; " - Democratic Party" → "DEM "; "Vote for none, one, ..." →
    Vote For; "110 - DEM" → precinct 110;
  - the canvass's carry-over of columns to a following page under the same title.

  Vote types are Early and Election Day where the report splits them; ballots cast per precinct
  come from the canvass columns.
- **McLennan's per-entity files:** one election is spread over 15–20 entity sets that share the
  election line. They merge through the step 1 multi-file merge. A contest printed in two entity
  files must agree, which the cross-check reports.
- **Acceptance:**
  - per election, the canvass totals equal the cumulative report, and the precinct report summed
    over precincts equals the cumulative report;
  - McLennan G20's entity sets load as one election;
  - Montgomery G20's 731-page precinct report loads in reasonable time (record it);
  - the step 8 Hart tests (Hays G25, GR25, L26, P26, PR26) are unchanged.

- **Done 2026-10-27 (uncommitted).**
  - **Reader (`ee_results_pdf.c`, `hl_*`):**
    - **Detection:** a voters line ("Number of Voters" / "Party Voters") plus "Precincts
      Reporting" or a report title, on the first or second page. McLennan's 2020 primaries
      open with a cover page.
    - **Heading:** read from the first report page as joined lines. The em dash may decode
      as U+FFFD, and plain " - " is accepted.
      - A title beginning "Democratic"/"Republican" makes a primary's contests "DEM …" /
        "REP …" and names the party in the election.
      - A party's report takes its totals only from a "Party Voters" line, since McLennan
        2022's "Total Number of Voters" counts both parties.
      - An entity's file with "Number of District Voters" leaves the county totals to the
        county's file.
    - **Cumulative / precinct report:**
      - columns are the headings after "Candidate"; each count goes to the heading nearest a
        point 10 pt right of it;
      - labels and counts are paired by the nearest line, since baselines wander by 2 pt;
      - "Precinct 075  (Ballots Cast: 17)" starts a precinct;
      - "Early" / "Election" become "Early Voting" / "Election Day".
    - **Canvass:**
      - the fixed columns give precinct ballots and registered voters, with a precinct taking
        the largest figure across its contests (a contest's own figures cover its part of a
        split precinct). A registered voters figure of 0 counts as not reported (McLennan
        2020 Republican);
      - turned headings are grouped into columns at gaps over 13 pt, with the party on the
        first line. A line ending in a letter with no space was wrapped mid-word;
      - "Continued" pages and a " Continued" title suffix carry the contest on;
      - the canvass has no vote types.
    - **Names:**
      - Harris's 2020 reports are Word copies that split words into touching pieces; these
        are joined, and " ," "14 th" " -Collector" "( Four" "Tami \"" are tidied;
      - "/" is spaced one way;
      - " (W)" is dropped because only some reports print it.
    - **Refused:** a report whose date does not read, i.e. Ector's scans with an OCR layer
      ("Governor, voto For l").
    - **New format:** `EE_RES_FMT_HART_PCT_REPORT` ("Hart precinct report (PDF)") for the
      precinct report, so it groups with a canvass.
  - **One election from several files (`ee_results.c`, `combine_split`):**
    - **When:** results PDFs of one format with the same election, date and region (letters
      and digits compared) that share less than half their contests. Two snapshots share all
      of theirs and stay apart, and a primary's two parties stay apart.
    - **Same contest:** the same title, half the names, and a total or every shared
      precinct the same. Two ISDs' "Board of Trustees-At Large" are kept apart.
    - **How:** the larger file's copy of a shared contest is kept, and the other's totals
      are checked against it. Each file stays a source with its own notes.
    - **McLennan:** G20, G21 and G22 (a county-wide file plus per-entity files) and L21 and L22
      (per-entity files only) each load as one election.
    - **Cameron:** LR25 (two cities) loads as one election. Cameron's other entity reports
      name their own elections and stay apart.
- **Results:**
  - **Sweep:** 1,337 of 1,941 PDFs load (1,087 after 9a): 250 more (McLennan 198,
    Montgomery 45, Harris 7). None lost, no file that loaded before reads differently, no
    crashes. The rest of the 279 refused files in these counties are Montgomery's
    "Consolidations" polling-place lists (not results), its partial manual count, and
    McLennan's OCR-layer Crawford ISD (Coryell County) canvass and precinct report.
  - **Cross-check, every election's files together:**
    - clean: Harris G20 (canvass ↔ cumulative, 343 contests, 1,012 precincts), McLennan G20,
      G21, G22, LR22, P20, P22, PR20 and PR22, Montgomery GR20, LR22, P20 and P22;
    - left over, all genuine:
      - figures that differ by one ballot between a canvass and a precinct report
        (Montgomery G21, L22, PR20, PR22; McLennan L22);
      - county totals against the precincts of the contests (Montgomery L20, L21; Harris
        GR20);
      - the EMCID write-ins the Montgomery G20 precinct report omits;
      - McLennan L22: the City of Lorena's precinct report covers the Lorena ISD contest in
        the city's part of each split precinct (42 against 147 votes);
      - McLennan L21's OCR-layer Crawford ISD (Coryell County) cumulative report.
  - **Builds:** smoke `reshart0` added; the smoke suite passes; Release x64 and ARM64 build
    clean.

#### 9c — Travis County reports printed to PDF (3 files)

- **Files** (`Election_Results/TX_Travis_County/`):

  | File | Pages | Type 3 fonts | Other file of the election | Role |
  |---|---:|---:|---|---|
  | `L22_Cumulative Report.pdf` | 23 | 7 | `L22_Precinct.pdf` (loads) | cross-check |
  | `LR22_Official Canvass.pdf` | 10 | 3 | none | **only results file of LR22** |
  | `PR22_Republican Cumulative.pdf` | 3 | 3 | `PR22_Republican Precinct.pdf` (loads) | cross-check |

- **What they are:** Electionware reports printed through the Windows PScript5 driver. There is no
  Producer or Creator. All text is in Type 3 fonts whose glyphs are vector outlines (the original
  Roboto, drawn with m/l/c/h and filled). The glyph names are /0 … /255, given in order of first
  use. There is no ToUnicode and no meaningful Encoding, so the reader gets character codes, not
  letters, and the step 8 sweep refused the files. Font sizes (13 / 12 / 10 / 8 pt) and glyph
  widths survive, so once the text is decoded, the step 8 ES&S parser applies unchanged.
  `LR22_Official Canvass.pdf` is expected to have the layout of Travis's P22 canvass PDFs, which
  load as ES&S precinct reports.
- **Approach — learn each font's code → character map, then read the report as usual:**
  1. **`pdf_reader.c`, Type 3 fonts:** read CharProcs, FontMatrix and Widths, and give positioned
     glyphs with their (font, code) when a font has no Unicode mapping.
  2. **Render:** draw each page's glyph outlines into a bitmap with GDI (BeginPath, PolyBezierTo,
     FillPath, at about 300 dpi), encode it as PNG with WIC, and recognize it with `ocr_win.c`
     (Windows OCR, already used by `hart_ocr.c`).
  3. **Vote:** align each OCR word with the glyph run under its box. When the word's character
     count equals the run's glyph count, each glyph votes for its character. The majority over
     the whole document gives each (font, code) its character; digits and common letters get
     hundreds of votes, so a single OCR error is outvoted. A code with split or few votes (l / 1
     / I, O / 0) is decided by context (numeric columns take digits only) and listed in the load
     notes.
  4. **Decode** all text with the learned map and pass it to the ES&S parser. 9a's structural
     detection recognizes it whether or not the footer is present.
- **Fallback if voting is not unanimous:** match the glyph outlines against the embedded Roboto
  fonts of the county's JasperReports PDFs from the same elections. This is exact, but it needs
  outline comparison (TrueType quadratic vs the driver's cubic curves).
- **Marking:** the load notes say the text was recovered by OCR, with any undecided codes.
- **Acceptance:**
  - the L22 Cumulative report equals `L22_Precinct.pdf` summed over precincts, for every contest
    and choice;
  - the PR22 Republican Cumulative report equals `PR22_Republican Precinct.pdf` summed;
  - LR22's canvass precinct rows add up to its totals, and every contest of the runoff is present;
  - every learned code is unanimous or resolved by context;
  - the step 8 Travis tests are unchanged.
- **Not included:** `LR22_Reconcilliation Final.pdf` is a one-page scanned image (no text, no
  fonts), not a PScript5 file. It holds the ballot reconciliation, not contest results.

- **Done 2026-10-27 (uncommitted).**
  - **The files, as found:**
    - they do name their tools: Producer "Acrobat Distiller 23.0 (Windows)", Creator
      "PScript5.dll Version 5.2.2";
    - each Type 3 font has FontMatrix [1 0 0 1 0 0] and declares every glyph 1 em wide. The
      driver places each glyph itself with `TD`, or shows two glyphs in one string spread by a
      large `Tc` (a number's first digit set apart from its label);
    - a space is a glyph named /i255 whose CharProc draws nothing;
    - each row is drawn inside `q … Q`, and the `Q` restores the `Tc` an earlier row set (L22's
      zero rows, "0 000").
  - **PDF reader (`pdf_reader.c`):**
    - **Type 3 fonts:** FontMatrix, the CharProc of each code, and a fingerprint (FNV hash) of
      each CharProc stream. The same drawing is the same character, across fonts and pages.
    - **Blank glyphs:** a CharProc that draws nothing reads as a space.
    - **Widths:** when every glyph is declared the same width (`ink_widths`), a run's width
      ends where its last drawing ends.
    - **Unknown glyphs:** a glyph with no name and no learned character reads as U+FFFD and is
      counted (`unknown_glyphs`).
    - **Glyph API:** with `collect_glyphs`, each glyph is listed with its matrix (`EePdfGlyph`).
      `EePdf_GlyphOutline` gives a CharProc's path, and `EePdf_SetGlyphChars` takes
      fingerprint → character pairs.
    - **`Tc`:** kept in the graphics state (saved by `q`, restored by `Q`) and applied to Type 3
      fonts only, so other fonts read as before.
    - **Word gaps:** a `Tc` over 0.2 em is a word gap. With `split_gaps` it ends the run, as a
      large TJ kern does; otherwise it reads as a space.
  - **Learning (`ee_glyph_ocr.c`, `EeGlyphOcr_Learn`):**
    - **Render:** each page's glyphs are filled with GDI into a grey bitmap at 3 px/pt (at most
      2,600 px) and recognized with Windows OCR (`ocr_win.c`). Pixels go straight to the OCR
      engine, with no PNG step.
    - **Align:** each OCR word is matched to the glyphs whose middles fall in its box and whose
      baselines sit near its bottom.
    - **Vote:** when the counts agree, each glyph votes for its letter, and each drawing takes
      the letter most of its votes give.
  - **Results reader (`ee_results_pdf.c`):**
    - **Trigger:** when the first page has 40 or more unknown glyphs, the document is learned
      before parsing.
    - **Load note:** says the text was read by OCR, how many characters over how many pages,
      and whether some had few or split readings.
    - **Word joins:** Type 3 runs that touch within 0.3 em join into words.
    - **Heading:** a line naming the county together with an election word (Joint, General,
      Special, Primary, Runoff) is the election, not the region (L22's cut-off "Travis County
      Joint General and Special").
    - **Not done:** undecided codes are not settled by context, as the plan proposed. The load
      note flags them instead. In L22 and PR22 every figure still matches the precinct reports.
  - **Results:**
    - **PR22 Republican Cumulative:** 9 contests, equal to `PR22_Republican Precinct.pdf` summed
      (no cross-check difference). 146 characters learned over 3 pages, in about 5 s.
    - **L22 Cumulative:** 86 contests and 199 choices, equal to `L22_Precinct.pdf` summed. 167
      characters learned over 23 pages.
    - **LR22 Official Canvass:** 3 contests over 10 precincts, and each of its 52 rows has a total
      equal to its vote types. LR22's only results file now loads.
    - **Travis G20 against its CVR:** unchanged (399 matches).
    - **Sweep:** 1,340 of 1,941 PDFs load (1,337 after 9b), with no crashes. Against the 9b
      sweep, only the three Travis files changed. The 9b sweep had run before 9b's Ector
      refusal was added, so it still showed four Ector OCR-layer scans loading with garbled
      text; they are now refused, as 9b intended.
  - **Builds:**
    - smoke `resglyph` added: a summary report printed PScript5-style, with Arial outlines as
      unnamed Type 3 glyphs and a spread pair restored by `Q`. It is read through OCR and
      cross-checked against a text precinct report, and is skipped without an OCR language;
    - the smoke suite passes;
    - Release x64 and ARM64 build clean.

#### After step 9

- **Sweep:** record loads and refusals by reason, as in step 8.
- **Still out of scope:**
  - scanned PDFs (about 450, including Cameron's other files, Ector, Grayson, Guadalupe and
    Potter);
  - Electionware "Custom Table Report" and "Statement of Votes Cast" grids (about 78: Smith,
    Collin, Ellis, Bowie, Guadalupe, Grayson). Check first whether those elections are already
    covered by other files;
  - the encrypted Ellis PDFs (30);
  - Word documents and other one-off layouts.

## Implementation plan (proposed)

1. **Engine `src/ee_results.{c,h}`** (no UI):
   - **Model `EeResults`:**
     - election name, date, region, timestamp, source formats;
     - vote type names;
     - precincts (name, registered voters, ballots cast per vote type);
     - contests: title, Vote For, isQuestion, party, file order, the contest's precinct list,
       choices (name, party), and a dense `int32` count array choice × contest-precinct × vote
       type, plus optional over/under arrays of the same shape;
     - summary-only contests carry totals and no precinct rows.
   - **Loaders:**
     - Clarity summary CSV (via `EeCsv_ReadSheet`);
     - Clarity XML: a small streaming scanner for the five elements, not a general XML parser;
     - Clarity TXT: fixed-width columns, with choice name columns from line 2;
     - Clarity XLS/XLSX (via `EeXlsx_ListSheetsMem` / `EeXlsx_ReadSheetMem`, which already
       handle BIFF8);
     - Election Explorer CSV/TSV export;
     - ZIP members through the vendored miniz.
   - **Multi-file merge + cross-check** → `EeResultsLoadInfo` (mismatch list).
   - **Tabulate under a filter:** an included precinct and vote type set → contest/choice totals.
   - **Compare:** results ↔ results, and results ↔ `EeCvrTally[]` → diff rows.
   - **Delimited export formatter.**
   - **Smoke tests:** small fixtures of each format, including the cp1252 `?` and the
     repeated-name contest; export → reload round trip; compare against a CVR tally.
2. **Window (`main.c`):** the results window, menus, Copy, Options (zoom, party first), status
   bar; File menu items added to the voter, roster and CVR windows.
3. **Filter:** CVR-style dialog reuse, the precinct re-tabulation, filter file save/load (CSV / TSV).
4. **Reports:** Turnout and Load Summary windows.
5. **Compare** window + export.
6. **Export** + round-trip validation on all 18 Travis Clarity elections in `Election_Results/`.
7. **All Clarity counties:** load + cross-check all 471 Clarity elections in `Election_Results/`.
   Handle free-text vote types, over/under votes, `num Area`/`num County` headers, missing
   columns and cumulative voting.
8. **ES&S and Hart PDF results (D8)** through the existing positioned-text reader
   (`pdf_reader.c`, already used for Hart CVR report PDFs and rosters). `pdftotext -layout`
   scrambles these reports' columns, so columns are assigned by x position.

   | Vendor | Report | Producer | Layout |
   |---|---|---|---|
   | ES&S Electionware (EVS 6.x) | Summary Results Report (cumulative) | JasperReports / iText | Statistics block (registered, ballots cast by vote type); per contest `TOTAL, VOTE %, <vote types>`; party prefix (`REP`, `DEM`); `Write-In Totals` with indented write-in names; `Not Assigned`; Total Votes Cast / Overvotes / Undervotes / Contest Totals |
   | ES&S Electionware | Statement of Votes Cast / Precinct Report | JasperReports | per-precinct breakdown (layout to be surveyed) |
   | Hart Verity (2.5/2.7) | Cumulative Results Report | Microsoft Reporting Services | per contest `Choice, Party, <vote type count %>…, Total`; `Cast Votes`, `Undervotes`, `Overvotes`; write-ins marked `(W)`; header with registered voters and precincts reporting |
   | Hart Verity | Precinct Results Report | Microsoft Reporting Services | same blocks, repeated per precinct under a precinct heading with its registered voters / turnout |
   | Both | Canvass reports | | precinct × choice grids (survey; may follow the precinct reports) |

   - Detected from the PDF's Producer/Creator and the first page's report title.
   - Unknown layouts are refused with a clear message.
   - **ES&S test set (user, 2026-10-26): the older Travis elections first, then every other
     Electionware PDF on hand.**
     - **Travis, 10 pre-Clarity elections (2020–22):** G20, G21, GR20, L21, L22, P20 (Dem/Rep),
       P22 (Dem/Rep), PR20 (Dem/Rep), PR22 (Dem/Rep).
       - Each has a Cumulative (`Summary Results Report`) and a Precinct (`Precinct Summary
         Results Report`) JasperReports PDF; P22 has Canvass PDFs instead of precinct ones.
       - **G20 PDFs ↔ the Travis G20 CVR** (7 files, 612,696 ballots; its tabulation was
         already matched to the cumulative PDF) is the main CVR acceptance test.
       - For every Travis election, the **precinct PDF summed over precincts must equal the
         cumulative PDF**. This is the PDF counterpart of the Clarity cross-check.
       - Three Travis files are printed to PDF (PScript5): L22 Cumulative, LR22 Canvass +
         Reconciliation, and PR22 Republican Cumulative. Their fonts have no Unicode mapping,
         so their text extracts as garbage. They are refused for now (OCR could read them
         later). L22 still has its JasperReports precinct PDF.
     - **All other Electionware PDFs:** 553 JasperReports PDFs from 16 ES&S counties —
       Guadalupe 81, Smith 66, Collin 60, Bowie 57, Ellis 55, Cameron 53, Bexar 41, Grayson 37,
       Henderson 33, Travis 24, Orange 18, Williamson 14, Dallas 7, Midland 3, El Paso 3,
       Angelina 1.
       - Report titles seen: Summary Results Report, Precinct (Summary) Results Report,
         Statement of Votes Cast (Collin), Canvass (Travis P22, Smith), Results by Precinct by
         Race, Voter Turnout.
       - All are run through load + cumulative/precinct cross-check before release.
       - Some Electionware PDFs carry no Creator (Smith, Cameron), so detection also uses the
         report title and the `Report generated with Electionware` footer.
       - About 250 scanned PDFs (RICOH, Canon, PaperStream, ScanSnap) are out of scope.
     - **ES&S CVR pairs besides Travis G20:** Collin G20 (accumulated totals + statement of
       votes cast PDFs + CVR) and Smith G24 (Summary, Precinct and Canvass PDFs + CVR).
   - **Hart CVR pairs:** Hays G25 and PR26 (Cumulative + Canvass PDFs + CVRs), and Hays P26. The P26 CVRs are
       **pre-canvass**, which is a real case of the compare showing differences.
   - Scanned PDFs (Cameron, Walker G20, Webb P24) are out of scope; they could use the existing
     OCR later.
9. **More ES&S and Hart PDF layouts (step 9):** Electionware reports without the footer,
   older Hart reports (McLennan, Montgomery, Harris 2020–22), and Travis's three
   PScript5-printed reports read through learned glyph maps.
10. **Help topics, version 1.5.0.0, RELEASES.md, STORE-LISTING.txt.**
11. **Later:** Dominion (Liberty Vote) PDFs; ES&S CSV exports (Collin, Bexar); Enhanced Voting
    (Comal); per-county website formats.

## Decisions (2026-10-26)

- **D1 — By precinct:** View menu toggle, Totals / By Precinct, in one list.
- **D2 — Vote types:** shown as columns by default, one per vote type in the file; an Options
  checkbox hides them.
- **D3 — CVR tabulation for Compare:** ask all-or-filtered when the CVR window has an active
  filter; otherwise use all ballots.
- **D4 — Files from different elections:** grouped, one results window per election.
- **D5 — Export layout:** the export dialog chooses between Displayed view and All detail
  (reloadable).
- **D6 — Compare menu:** in both the results window and the CVR window.
- **D7 — Precinct compare:** yes, whenever both sides have precinct detail.
- **D8 — Scope:** Clarity (all counties) plus ES&S and Hart PDF results in 1.5. Dominion later.
