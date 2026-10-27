# Election results — design (Election Explorer 1.5)

Status: **steps 1–7 done (engine, Clarity loaders, Election Results window, filter, reports, compare, export, all Clarity counties)** (2026-10-26). Decisions D1–D8 are at the end.

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
- **View:** Totals / By Precinct.
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

### Step 1 — engine + Clarity loaders (done 2026-10-26, uncommitted)

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

### Step 2 — the Election Results window (done 2026-10-26, uncommitted)

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

### Step 3 — the Filter menu (done 2026-10-26, uncommitted)

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

### Step 4 — Reports (done 2026-10-26, uncommitted)

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

### Step 5 — Compare (done 2026-10-26, uncommitted)

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

### Step 6 — Export (done 2026-10-26, uncommitted)

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

### Step 7 — all Clarity counties (done 2026-10-26, uncommitted)

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
9. **Help topics, version 1.5.0.0, RELEASES.md, STORE-LISTING.txt.**
10. **Later:** Dominion (Liberty Vote) PDFs; ES&S CSV exports (Collin, Bexar); Enhanced Voting
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
