# Design: Cast Vote Record (CVR) support

**Status:** Phase 1 (data engine + loader + viewer window + File/Edit menus + copy)
and Phase 2 (vote tabulation report) implemented. Verified on real ES&S files,
including tabulation cross-checked against official published results.
**Date:** 2026-09-18 (Phase 2 added 2026-09-19)

## Motivation

Cast Vote Record exports (one row per ballot, one column per contest, the cell
holding the chosen candidate/selection) are a primary election-audit artifact.
ES&S and other systems export them as Excel `.xlsx` or as delimited text
(`.csv` / `.tsv`). This adds a **File → Load Cast Vote Records…** flow that opens
one or more of those files and shows the ballots in a sortable grid like the
voter-list window.

## Data shape (from real samples)

- Header row, then one row per ballot.
- Leading "key" columns: some subset of `Cast Vote Record, Batch, Ballot Status,
  Precinct, Ballot Style` (files vary — some omit Batch/Ballot Status).
- Every remaining column is a **contest/proposition**; the cell is the selection
  (e.g. `Greg Abbott (EC77)`, `undervote`, `Yes (EY15)`) or blank.
- Very wide (hundreds to thousands of contest columns) and very tall (ES&S
  exports ~100,000 ballots per file; several files per election).
- **Blank vs `undervote` (ES&S):** a blank contest cell means the contest was
  **not on that voter's ballot**; `undervote` means it **was** on the ballot but
  the voter made no selection. (`overvote` likewise = too many selections.) This
  distinction matters for Phase 2 tallies. Whether non-ES&S vendors follow the
  same convention is not yet known — keep the report logic vendor-tolerant.
- **Write-ins as images (ES&S):** for a write-in selection, ES&S leaves the
  contest cell's *text* empty and pastes a scanned image of the handwriting over
  the cell (via a worksheet drawing). Such a cell would otherwise read blank —
  indistinguishable from "contest not on ballot" — even though the voter *did*
  vote. The XLSX reader detects the anchored image and surfaces the cell as
  `[write-in]` (presence only; the handwriting is a raster, so the actual name is
  not extracted without OCR, which is out of scope). See
  [xlsx-import-design.md](xlsx-import-design.md) §6.1. Phase 2 should count
  `[write-in]` as a cast selection, distinct from blank/`undervote`/`overvote`.
- The Cast Vote Record number may be stored as an integer or a float (some ES&S
  files store `1.0`); the reader normalizes integer-valued numeric cells so it
  displays/sorts as `1` (handled in `xlsx.c`, benefits all `.xlsx`).
- **Sparse:** a ballot only has values for contests on its ballot style; most
  cells are blank.
- **"Vote for N" contests span multiple columns.** A contest that lets the voter
  pick several candidates uses one column per selection: the first column carries
  the contest title and each additional column has a **blank** header. A
  continuation cell holds another selection or `undervote`. The loader attributes
  these blank columns to the contest — see
  [Multi-column contests](#multi-column-contests-vote-for-n). **A repeated identical
  title is a *different* race, not a continuation** — verified against the official
  results (see that section).

## Decisions (resolved 2026-09-18)

1. **Multi-file = identical schema, concatenated.** ES&S splits one export across
   files (`…-1`, `…-2`); users may rename them. The first file sets the columns;
   every other selected file's header row must match **exactly** (same count, same
   header text, in order). On any mismatch, show an error window and load
   **nothing**. Same-schema files are concatenated into one table.
2. **Sparse storage** — target thousands of columns × millions of rows without a
   dense `rows×cols` matrix.
3. **New CVR window** reusing the frozen/scroll grid + sorting; the voter viewer
   (Store release) is left untouched.

## Storage (`ee_cvr.{c,h}`)

Sparse, CSR-style, with value interning (candidate names repeat massively):

- `col_titles[ncols]` (wide), `frozen_count` (leading key columns), `nrows`.
- Per row, only non-blank cells are stored, in ascending column order:
  - `row_start[nrows+1]` → ranges into `ent_col[]` / `ent_val[]`.
  - `ent_col[k]` = column index; `ent_val[k]` = interned value id.
- **Value intern pool:** distinct UTF-8 selection strings stored once (open-address
  hash → id; `val_off[id]` into `val_pool`). A contest has a few distinct values,
  so the pool stays tiny; entries dominate memory (~8 bytes × non-blank cells).
- `view_index[nrows]` gives display order; sorting permutes it.
- Cell lookup `(row,col)`: binary search the row's entry range on `ent_col`.

Rough memory: ~100 filled cells/ballot × 1M ballots ≈ 100M entries ≈ 800 MB
(x64); typical ~100k-ballot files ≈ 80 MB. Blank cells cost nothing.

## Loader

`EeCvr_LoadFromFiles(paths[], count, out, cancel, progress, err)`:
- For each file, read rows via a **row sink** whose signature is shared by both
  readers. The reader is chosen per file by extension:
  - `.xlsx` → `EeXlsx_ReadSheet` (first worksheet), as before.
  - `.csv` / `.tsv` / `.txt` (or anything else) → `EeCsv_ReadSheet`
    (`csv_sheet.c`): a delimited-text reader. Encoding: a UTF-8 or UTF-16 (LE/BE)
    **BOM** is honored; a BOM-less file is used as UTF-8 when it is well-formed
    UTF-8 (covers plain ASCII), and otherwise decoded as the **system ANSI code
    page** — which is what Excel's "CSV (Comma delimited)" / "Text (Tab delimited)"
    exports produce (e.g. `Peña` as a single `0xF1` byte). RFC-4180 quoting
    (double-quoted fields, `""` escapes, delimiters **and** newlines inside quotes);
    CRLF/LF/CR line endings. The delimiter is the extension's (`.tsv`→tab,
    `.csv`→comma) or, for `.txt`/unknown, sniffed from the first line (tab if it has
    more tabs than commas, else comma). Cells are delivered as UTF-8, exactly as the
    XLSX reader delivers them, so everything downstream (header logic, sparse
    storage, tabulation) is identical regardless of source format.
  - Because both readers feed the same sink, files of **different formats but
    identical headers concatenate** (e.g. a `.csv` plus a `.tsv`).
  - **Write-in note:** in `.xlsx`, an ES&S write-in is an embedded image over an
    otherwise text-empty cell, scanned to the `[write-in]` marker. Delimited exports
    carry no images, so those cells export **blank** and their write-ins are lost
    from a CSV/TSV. Text write-in variants (`Write-in`, a typed name, `No image
    found`) do survive. Validated on Travis: L26 and P26 tabulate **identically**
    across `.xlsx`, UTF-8, UTF-8-BOM and ANSI exports; G24 (which has ~3,680
    image write-ins) matches on every candidate/undervote/overvote count, differing
    only on the write-in rows Excel could not export. So counts match exactly for
    CVRs without image write-ins; for ES&S image write-ins, prefer the `.xlsx`.
- First row of the first file → establish `col_titles` + `frozen_count`
  (leading run of columns whose trimmed, case-insensitive title is one of the
  known keys; else freeze column 0).
- First row of every later file → compare to `col_titles`; mismatch →
  `EeLoadStatus_Error` with a message naming the offending file; caller loads
  nothing.
- Data rows → append a sparse row (skip blank cells; intern non-blank values).
  Selection values are **whitespace-normalized** before interning (`normalize_ws`:
  runs of spaces/tabs collapse to a single space, ends trimmed) so exporter quirks
  like `John   Cornyn` display cleanly and equivalent selections share one interned
  value and one tally. UTF-8 safe (only ASCII space/tab are touched). A value that
  is only whitespace becomes blank (not stored). Header/contest titles are left
  as-is.
- **Empty-ballot rows are dropped.** A row with no non-blank cell in any *contest*
  column (nothing beyond the frozen key columns) is not a countable ballot: a real
  ballot records a value — a candidate, `undervote`, or `overvote` — in every
  contest on its style, and a continuation card in a multi-card CVR carries its own
  page's contests. Skipping such rows keeps the ballot-record count and the
  multi-card heuristic consistent across formats and absorbs two Excel CSV/TSV
  export artifacts: the trailing all-empty (`,,,,`) line Excel appends, and the
  occasional record Excel breaks with a spurious **unquoted** newline after the
  first field (which otherwise leaves a lone Cast-Vote-Record-id line plus a
  headless row — whose contest cells are still column-aligned and tally correctly).
- Honors cancel + progress (by rows).

## Multi-column contests (vote for N)

A "vote for N" contest occupies N adjacent columns: the first is titled and the
rest have a **blank** header. When building the header, `cvr_build_titles`:

- keeps the first column's title verbatim (so Phase 2 can key a race by its title);
- gives each **blank** continuation the derived display title `<contest> (2)`,
  `<contest> (3)`, … so the grid no longer shows blank headers;
- records `col_group[i]` = the contest's title column for every column (a titled or
  key column points at itself; a blank continuation points at the title column), so
  **Phase 2 can tabulate a race across all its columns without parsing the display
  suffix** — the race is listed once with the selections drawn from
  `{ c : col_group[c] == title_col }`.

**Only a blank header marks a continuation.** A title that repeats verbatim on the
next column is a *separate* race, not a continuation. This was confirmed against the
official Travis County results for `L26 CVR.xlsx`
(`results.enr.clarityelections.com/TX/Travis/126203/web.345435`): two adjacent
identically-named "City of Bee Cave, City Councilmember at Large" columns are listed
there as **two separate (Vote For 1) races**, whereas the blank-header contests are
genuinely multi-seat — Village of Briarcliff Alderman = **Vote For 3** (3 columns),
Ensenadas MUD Director Election = **Vote For 5** (5 columns), Ranch at Cypress Creek
MUD No. 1 Directors = **Vote For 2** (2 columns). So repeated titles stay as
distinct columns/groups; only blank headers merge.

The blank-only derivation runs on every file's header, so the multi-file
identical-schema check still accepts matching layouts. The N selections remain N
sortable/copyable columns (the user confirmed one column per selection, not a merged
cell).

## Frozen vs scrolling columns

Freeze the leading key columns; horizontally scroll the contest columns — same
frozen/scroll split as the voter window.

## UI (next phase)

A dedicated CVR window (own class + state) with two virtual list views (frozen +
scroll) driven by `EeCvr_GetViewCellW`, header-click sorting on any column
(numeric-aware for `Cast Vote Record`), reusing the voter grid's rendering
helpers where they are not `AppState`-coupled. **File → Load Cast Vote Records…**
uses a multi-select open dialog (`OFN_ALLOWMULTISELECT`), filtered to
`*.xlsx;*.csv;*.tsv;*.txt` (with per-format and all-files alternatives).

**Load progress dialog.** A determinate progress bar (0..100), a status line, and Cancel.
The bar is driven by the loader's `percent` and is continuous across the whole load: the
Hart loader spans its two passes (`total = entries × 2`), and `EeCvr_LoadFromFiles` remaps
each ES&S file's 0..100 into an overall `(file × 100 + inner) / count` (via an internal
`cvr_multi_prog` wrapper) so the bar does not restart per file. The status line shows
"Scanning ballots…" during the Hart discovery pass (`EeLoadProgress.scanning`, no rows
yet) and "N ballot records" once rows are being filled (cumulative across ES&S files).

**Column-width cap.** The CVR grid is a single report ListView holding every column. Like
the voter list, its per-column width is clamped so the cumulative header width stays under
the Win32 16-bit limit (`32000 / ncols`, floored at ~20 px): past 32,767 px the header
stops hit-testing and clicking a far-right column jumps the view back to the start. A wide
CVR (~188 columns) needs this; a narrow one is unaffected.

## Phase 2 — vote tabulation (implemented)

**Reports → Tabulate All CVR Votes…** on the CVR window opens a report tallying every
contest. **Reports → Tabulate Filtered CVR Votes…** does the same over only the
records the active filter shows; it is greyed when no filter is applied (it would
duplicate Tabulate All). The report window remembers which mode created it, so
changing an Option (write-in merge, party order) or the filter re-tabulates it in the
same mode; its title reads `CVR Tabulation` or `CVR Tabulation (Filtered)`.

- Core (GUI-free): `EeCvr_Tabulate(t, merge_writeins, &items, &count)` in
  `ee_cvr.{c,h}`. It scans the sparse entries once (O(entries), not rows×cols), skips
  the frozen key columns, and aggregates each non-blank selection **by `(col_group,
  value)`** — so a "vote for N" contest is summed across all of its columns under the
  contest's title. Blank cells (contest not on the ballot) are not counted;
  `undervote`/`overvote`/`[write-in]` are counted as the selections they are. Output
  is an `EeCvrTally[]` of `{contest, selection, count}` grouped by contest column;
  **within a contest the candidates come first (count descending, then text),
  followed by the non-candidate outcomes in the fixed order write-in, overvote,
  undervote** — even when a special out-counts a candidate. `EeCvr_FreeTally` frees
  it.
- **Write-in merge (`merge_writeins`):** an election can record write-ins several
  ways in one contest — the scanned `[write-in]` image marker, a literal `Write-in`
  text value (e.g. hand-marked paper vs. BMD ballots; both seen in Travis `G24 CVR`),
  and ES&S's **`No image found`** placeholder for a write-in whose scanned image was
  not retrieved (seen in Dallas `G24 CVR`; official results fold it into the
  Write-In total). `cvr_selection_rank` classifies all of these as the write-in
  category. When `merge_writeins` is TRUE (the default) they collapse into one
  `write-in` row whose count is their sum (matching how official results report a
  single "Write-in" line — e.g. Dallas G24 President 4,578 + 8 = **4,586**); when
  FALSE each variant is its own row. The sort keeps a contest's write-in entries adjacent, so the merge is a
  linear collapse of consecutive same-contest write-in rows. Controlled per user by
  **Edit → Options… → "Merge image and text write-ins"** on the CVR window
  (`g_settings.cvr_merge_writeins`, persisted in the registry; default on). Changing
  it re-tabulates any open report in place.
- **Filtered subset:** `EeCvr_TabulateRows(t, rows, nrows, merge, &items, &count)`
  runs the same core over an explicit physical-row list (`cw->disp` / `cw->disp_count`)
  instead of every row, backing **Tabulate Filtered CVR Votes…**. Both entry points
  share one `cvr_tabulate_core`; `EeCvr_Tabulate` passes `rows == NULL` to mean all.
- **Primary party split & ordering:** the Hart loader (below) prefixes a primary
  contest's title with its party (`REP `/`DEM `), matching the ES&S convention, so the
  two parties' copies of a race tally separately. After tabulating,
  `EeCvr_ReorderTallyByParty(items, count, party_first)` stably regroups whole contest
  blocks so one party's contests print first, then the other, then any non-partisan
  contest — overriding the Federal/State/County category order *between* parties while
  preserving it within each. `party_first` comes from **Edit → Options… → "Party to
  display first for tabulation"** (`g_settings.cvr_tab_party_first`,
  `EE_TAB_PARTY_REP`=0 default / `EE_TAB_PARTY_DEM`=1, persisted in the registry). It is
  a no-op on a general election (no prefixes) and never changes the CVR window's column
  order or any count.
- UI: `CvrReportWindow` (class `k_CvrReportClassName`) — an owner-data three-column
  list (**Contest | Selection | Votes**); the contest name repeats on each of its
  selection rows. Bold/grey header via the shared `App_HeaderCustomDraw` (list
  subclass). Multi-select + **right-click → Copy** (and Ctrl+C) copy the rows as
  tab-separated UTF-8. It is an unowned top-level window (the CVR window can cover
  it), tracked in `CvrWindow.report`, one per CVR window, and closed when the CVR
  window closes. Tabulation runs synchronously behind a wait cursor.
- **Verified against official results (four elections, exact):** `L26` (Bee Cave
  Mayor 871/369; Briarcliff Alderman Vote-For-3 239/233/167/120/76/75/61/26; two
  same-named Councilmember races 830 & 826), `P26` (Mar 2026 primary, 274,443
  ballots, both parties), `PR26` (June 2026 runoff, 97,460 ballots), and **`G24`**
  (Nov 2024 general, **6 files → 587,090 ballots**, ~35 s: President
  Harris 398,968 / Trump 170,781 / …, and the merged **write-in 3,690** = 3,680
  image + 10 text, matching the official combined Write-in total).

## Per-column value reports (Batch / Precinct / Ballot Style / Polling Place / Device Serial / Voting Type)

**Reports → Display Batch Report… / Display Precinct Report… / Display Ballot Style
Report… / Display Polling Place Report… / Display Device Serial Report… / Display
Voting Type Report…** on the CVR window each open a two-column report — the distinct
values of that column on the left and the **number of ballot records** carrying each
on the right — modeled on the voter-list Precinct/Address reports. The last three
report the key columns a Hart PDF CVR Report adds (PDF-only or ZIP+PDF loads), so they
are greyed for ES&S and ZIP-only Hart data.

- **Column lookup (`Cvr_FindReportColumn`):** by header title; the **Batch** report also
  accepts Hart's **`Batch Number`** (ES&S calls it `Batch`), keeping the "Batch" label.
  A Hart PDF-only load of Election Day / Early Voting data has a blank Central Batch Id,
  so its Batch report stays greyed (nothing to report).

- **Menu gating (`CvrWndProc` `WM_INITMENUPOPUP`):** each item is greyed unless the
  CVR actually has that column *and* the column holds real data. `EeCvr_FindColumnByTitle`
  matches a key column by its header (trimmed, case-insensitive: `Batch`, `Precinct`,
  `Ballot Style`); `EeCvr_ColumnHasReportableData` returns FALSE when the column is
  entirely blank or entirely a **redaction placeholder** (`cvr_is_redaction_marker`:
  a value that begins with `<` and contains `redact`, matching `<Redacted>` /
  `<REDACTED>` / `<Redact>`). So a CVR whose Precinct is redacted greys the Precinct
  report.
- **Core (GUI-free):** `EeCvr_CollectColumnCounts(t, col, &items, &count, &blank)` in
  `ee_cvr.{c,h}` scans the sparse entries once, counting the ballot records per
  distinct value in `col`; blank cells are tallied into `*blank` (not in the array).
  Returns `EeCvrValueCount[]` `{value, count}` (unsorted); `EeCvr_FreeColumnCounts`
  frees it.
- **UI:** `CvrValueReportWindow` (class `k_CvrValueReportClassName`) — owner-data
  two-column list (**`<label>` | Number of Ballot Records**), bold/grey header via the
  shared `App_HeaderCustomDraw`, header-click sort on either column (the value column
  sorts numerically when every value is a digit run — typical for these codes). A
  `(blank)` row is appended for records with no value in the column. Export suffixes:
  `-…_Batches`, `-…_Precincts`, `-…_Ballot_Styles`, `-…_Polling_Places`,
  `-…_Device_Serials`, `-…_Voting_Types`. Multi-select +
  **right-click → Copy** (and Ctrl+C) copy as tab-separated UTF-8; **right-click →
  Include/Exclude** adds an `is` rule for that value to the CVR window's own filter
  (`EeFilterSet` + `Cvr_ApplyFilter`, same ProcMon semantics as the CVR filter). The
  window title is `<label> Report - <filename>`. It is an unowned top-level window
  (the CVR window can cover it), one of each kind per CVR window (tracked in
  `CvrWindow.vreports[]`), closed when the CVR window closes.

### Multi-card / multi-page ballots

Some counties (e.g. Dallas) export **one row per ballot card/sheet**, not per
ballot: a ballot that spills onto page 2 produces a second CVR row that is blank in
the page-1 contests. So the row count can exceed the number of ballots (Dallas Mar
2026 Dem: 286,258 rows vs 280,326 ballots — 5,932 continuation sheets). Travis
exports one row per ballot, so its row count equals ballots. **Tallies are
unaffected** either way — each contest is counted from whichever card carries it, and
every validated election matched official results. ES&S supports up to 9-page
front/back mail ballots, and a contest can be page 1 in one style and page 2 in
another, so cards are *not* a fixed column range.

- The CVR window status bar reads "**N ballot records**" (a record is a card/sheet),
  and appends "**(multi-card ballots detected)**" when detected.
- `EeCvr_HasMultiCard(t)` (informational only; never affects tallies) is
  threshold-free: the top-of-ballot statewide races — **President, Governor, U.S.
  Senator** (excluding Lieutenant Governor) — appear on page 1 of every ballot style
  in every U.S. county. It gates on such a *reference contest* being present, then
  flags the CVR when the rows carrying **none** of them (continuation sheets) are a
  minority (>0, <50% of rows). This fires on Dallas-Dem but not on Dallas-Rep, the
  Travis elections, or **combined-party primaries** (each party's ballot carries its
  own top race, so no row lacks a reference contest). Test `cvrmc`.

## Filtering (CVR window)

The CVR window has a **Filter** menu (between Edit and Reports) with **Filter…** and
**Reset Filter**. Filter… opens a modeless window titled **"Election Explorer CVR
Filter"**, modelled on the voter-list filter but adapted to CVR data:

- **Column** — any CVR column (key columns and every contest column).
- **Relation** — limited to **is** / **is not**.
- **Value** — for a normal column, a non-editable drop-down (`CBS_DROPDOWNLIST`) of
  the distinct selections that appear in the chosen column (`EeCvr_CollectColumnValues`),
  sorted case-insensitively; blanks are not offered. **For an all-numeric column
  (e.g. Cast Vote Record) the value box becomes editable** (`CBS_DROPDOWN`) so any
  number can be typed — necessary because the suggestion list is capped
  (`EE_FILTER_MAX_DISTINCT` = 8000) and a CVR with 500k+ ballots would otherwise only
  offer the first ~8000 record numbers. Numeric-ness is decided from the collected
  values (`cvr_wstr_is_number`); the value combo is recreated in place
  (`CvrFilt_EnsureValueCombo`) when the kind must change, and editing an existing
  rule sets the text directly so out-of-list values survive. Populated eagerly on
  column change and lazily on drop-down (wait cursor).

Long contest names would otherwise be clipped by the combo width (and the drop-down
scrollbar), so both the Column and Value drop-down *lists* are widened to their
longest item via `CB_SETDROPPEDWIDTH` (`Combo_AutosizeDropdown`, capped ~900 DIP,
scrollbar allowance included) without widening the combo controls. The dialog also
defaults wider (940 DIP) with a roomy "Column" column in the rules list so applied
rules read cleanly.
- **Action** — Include / Exclude, same ProcMon semantics as the voter filter
  (same-column includes OR, different columns AND; any matching exclude hides the
  row).

Filtering is non-destructive and layered over sorting: `Cvr_ApplyFilter` rebuilds a
display map (`CvrWindow.disp` — physical rows in current sort order that pass
`Cvr_FilterAccepts`), the owner-data list shows `disp_count` rows, and the status bar
reads "X of N ballot records … filtered". Header-click sort re-applies the filter in
the new order; Copy maps the selection through `disp`. Reset Filter clears the rules.
Filter rules reuse `EeFilterSet`/`EeFilterRule` (relation restricted to is/is-not),
and matching uses `EeCvr_GetCellW` (physical-row access). The reusable primitive
`EeCvr_CollectColumnValues` is covered by test `cvrfilt`.

Tabulation has two entry points: **Tabulate All CVR Votes…** counts every ballot
(like the voter-list reports, which ignore filters), while **Tabulate Filtered CVR
Votes…** counts only the current `disp` subset. The Batch / Precinct / Ballot Style
reports always count all ballots.

### Future Phase 2 polish

Blank-vs-`undervote`-vs-`overvote` rate summaries, ballot-style breakdowns, and
per-precinct cross-tabs. Count **blank** (contest not on ballot) separately from
**`undervote`** (on ballot, no selection) and **`overvote`** — do not lump them
together. (The current report already keeps `undervote`/`overvote` as distinct
selections and simply omits blanks.)

## Standalone CVR window

A CVR window is a **first-class top-level window**, not a satellite of the voter list
that launched it. Each CVR window owns its **own resource-only `AppState`** (`cw->app`,
`is_cvr_ui = TRUE`) holding its fonts, header brush, DPI and instance, with its
`hwnd_main` set to the CVR window itself. Because the window no longer borrows the
launching voter window's `AppState`, it survives that window closing, and its own
dialogs (Help, About, Options, and the Load progress modal) own to and center on the
CVR window.

App lifetime counts CVR windows: `App_CreateCvrWindow` registers each in
`g_cvr_windows[]` (`g_cvr_window_count`), and the process quits (`App_MaybeQuit`) only
once **both** `g_viewer_count` and `g_cvr_window_count` reach zero. So closing the last
voter list leaves an open CVR window running as the only window, and **Exit**
(`App_ExitAll`) closes CVR windows too. From a lone CVR window, **File → Load Voter
List…** always opens a fresh voter window (`is_cvr_ui` forces the new-window path), and
**File → Load Cast Vote Records…** opens another standalone CVR window. The CVR window's
`AppState` is released in its `WM_DESTROY` via `App_FreeCvrUi` after its child report/
filter windows have closed.

## Delimited-text export (CSV / TSV)

Every CVR view and report can export to **UTF-8 CSV (default) or UTF-8 TSV**. A
shared exporter in `main.c` handles the Save dialog (`App_PromptExportPath` — CSV is
filter 1, TSV filter 2; an explicitly-typed `.csv`/`.tsv` wins, else the extension is
appended to match the chosen type) and writes the file with a **UTF-8 BOM** so Excel
opens it as UTF-8 (`App_WriteExportUtf8`). The suggested file name is the initial CVR
file's base name (no extension, stored in `CvrWindow.base_name`) plus a
window-specific suffix. All exports emit a **header row** and follow the window's
current sort/filter (display order).

- **CVR window** — File → **Export Cast Vote Records…** (all visible rows, suffix
  `-Filtered_Records` / `-All_Records` by `cw->filt_active`) and right-click →
  **Export Selected…** (selected rows, suffix `-Selected_Records`). Physical rows are
  gathered in display order (`cw->disp` when filtered, else `view_index`) and written
  by the efficient `EeCvr_FormatDelimitedUtf8` (interned UTF-8, RFC-4180 quoting).
- **CVR reports** — right-click → **Export Selected…** / **Export All…** on the
  Tabulation (`-Selected_Contests`/`-All_Contests`), Batch (`-…_Batches`), Precinct
  (`-…_Precincts`), and Ballot Style (`-…_Ballot_Styles`) reports. These small
  reports build the file from their in-memory items via `App_ExportReportModel` (a
  wide-cell callback).

The voter-list window mirrors this: File → **Export Voter List…**
(`-Filtered_Voters`/`-All_Voters`) and right-click → **Export Selected…**
(`-Selected_Voters`), with an **Include normalized data fields** option (default off;
`App_AskExportNormalized`); the voter
Precinct/Address reports gain **Export Selected…**/**Export All…**
(`-…_Precincts`/`-…_Addresses`). Voter rows use the efficient
`EeVoterTable_FormatDelimitedUtf8` (delim + header added to the former copy path).

## Hart voting-system CVRs (`hart_cvr.c`)

Hart exports a CVR as one or more **`.zip`** files, each holding **one XML per ballot
sheet** (`1_<guid>.xml` = first/only sheet; `<guid>.xml` = later sheets). Each XML's
`CvrGuid` is its own filename guid and there is no shared ballot id, so the sheets of a
multi-sheet ballot cannot be linked — each XML is one row. `EeCvr_LoadFromHartFiles`
(dispatched from the loader when the selection is `.zip`/`.pdf` files; `EeCvr_LoadFromHartZips`
remains as a zip-only wrapper; see the PDF section below) produces the same
`EeCvrTable` the ES&S path does, so tabulation, reports, filtering and export all work
unchanged.

- **XML** (custom scanner, no third-party): `<Cvr><Contests><Contest><Name/><Id/>
  <Options><Option><Name/><Id/><Value/>[<WriteInData><OriginalText/>…]</Option>…</Options>
  [<Undervotes>n</Undervotes>][<Overvoted/>]</Contest>…</Contests>` then metadata
  `BatchSequence, SheetNumber, PrecinctSplit{Name}, Party{Name} (primary only),
  BatchNumber, CvrGuid, IsBlank`. Entities are decoded; a UTF-8 BOM is skipped.
- **ZIP**: iterated entry-by-entry with the vendored **miniz** (`mz_zip_reader_init_cfile`
  on a wide-opened `FILE*`), so a multi-GB export is never held in memory at once.
  Non-`.xml` entries (Hart stores scanned write-in images as `.png`) are ignored.
- **Columns**: frozen keys `CvrGuid, Sheet Number, Batch Sequence, Batch Number,
  Precinct, Party (only if any ballot has one), Is Blank`, then contests. These Hart key
  names are also recognized by `cvr_is_key_header`, so a Hart CVR exported to CSV/TSV and
  reloaded through `EeCvr_LoadFromFiles` freezes the same leading columns (primary → 7
  incl. `Party`; general → 6) instead of tabulating them as contests. Each contest
  is one column, or several ("vote for N") with blank continuation headers so
  `col_group` sums the race. A selected candidate is its name; a write-in is the marker
  **"Write-in"** (generic — the handwritten text and `.png` image are not used, matching
  the merge-write-ins behavior); an unfilled seat is `undervote`; an over-marked contest
  fills every seat with `overvote`. Seat count comes only from non-overvoted ballots (an
  overvoted vote-for-1 is one `overvote`, not two).
- **Primary party split**: when a ballot carries a `Party`, its contest titles are
  prefixed with the party abbreviation (`party_abbr` → `REP`/`DEM`/`LIB`/`GRN`) via
  `contest_display_name`, e.g. `DEM United States Senator` vs. `REP United States
  Senator`. This mirrors the ES&S `REP …`/`DEM …` convention so each party's copy of a
  race interns as a distinct contest and tallies separately. General-election exports
  (no `Party`) are unaffected.
- **Contest order** (Hart only): a keyword classifier ranks each contest
  Federal → State → County → City → ISD → Other → MUD (with the office sub-orders inside
  Federal/State/County). Within one rank, contests sort by a case-insensitive **natural**
  compare of the name (`natural_cmp_ci`: digit runs compare by value), so races that
  differ only by a trailing number come out in numeric order — `United States
  Representative, District 6 < 12 < 26 < 33`, `Precinct Chair, Precinct 3486 < 4095` —
  rather than the arbitrary order Hart wrote them (first-seen is only the final tiebreak).
  Cosmetic — it does not affect tallies. In a tabulation report the party-first reorder
  (above) groups the ranked contests by party first.
- **Multi-card**: exact here — flagged when any row's `Sheet Number` >= 2 (no heuristic).
- **Two passes** over the zip(s): pass 1 discovers the contest set, each contest's seat
  count and category, and party presence; pass 2 fills rows via `EeCvr_BuildBegin` +
  `EeCvr_BuildAppendRow` (the shared table builder, which keeps blank sheets). Pass 1
  reports `scanning = TRUE` (no rows yet) so the load dialog shows "Scanning ballots…";
  pass 2 reports a running ballot-record count so it shows "N ballot records". Progress
  percent spans both passes (`total = xml_entries × 2`), driving a determinate bar.
- **Validated** against official Clarity results for two Tarrant County elections:
  - **G24** (general; single 2.2 GB zip, 828,544 ballot sheets, ~1m40s): Railroad
    Commissioner matches on all four candidates exactly (418,535 / 342,948 / 20,791 /
    20,248); President/US Senator match within a handful of votes (certified totals add
    cured/provisional ballots after the election-night CVR snapshot).
  - **P26** (primary; 6 zips, 488,862 ballot sheets, ~1m20s): with the per-party split,
    U.S. Senator matches the certified totals **exactly** for both parties — DEM Crockett
    103,743 / Talarico 83,233 / Hassan 2,060 (189,036 cast); REP Cornyn 65,621 /
    Paxton 55,341 / Hunt 19,729 / … (145,798 cast).

  Test `hart`.

### Hart PDF "CVR Report" (`pdf_reader.c` + `hart_cvr.c`)

Some Texas counties publish only Hart's PDF **CVR Report**, not the ZIP; others publish
both. The PDF is rendered by Microsoft Reporting Services (document title
`Count_CvrReport`). It has one record per ballot **sheet** (same Cvr Ids as the ZIP's
`CvrGuid`), each starting a new page and continuing onto further pages when long. Each
page has a header block — `Precinct`, `Party`, `Polling Place`, `Voting Type`,
`Device Type`, `Device Serial`, `Device Data Id`, `Cvr Id`, `Central Batch Id` — above a
two-column `Contest Title` / `Option` table. An Option cell is a selection, `Write-in`,
`Overvote` (the selections of an overvoted contest are not listed), or
`Undervotes: N`. Unlike the XML, the PDF has **no** Sheet Number, Batch Sequence or
Is Blank, but it does carry the device and polling-place fields the XML lacks.

- **`pdf_reader.{c,h}`** — a minimal read-only PDF text extractor (no third-party
  beyond miniz for FlateDecode). It reads on demand through a 256 KB window (never the
  whole file), loads classic xref tables and xref streams (`/Prev` chains, PNG
  predictors, object streams), walks the page tree, and interprets each page's content
  stream (CTM, `q`/`Q`, `re W n` clip rectangles, text operators) into **runs** = text +
  origin + enclosing clip rectangle + clip id. Fonts: simple fonts via WinAnsi +
  `/Differences`; Type0 Identity-H 2-byte codes; a ToUnicode CMap overrides either
  (SSRS draws non-WinAnsi names such as *Perla Muñoz Hopkins* / *Jenné Molacek* with a
  per-page Type0 font `F2xx`, decoded through its ToUnicode map).
  **Repairs:** Hart's 3.7 GB G24 report has `startxref -604206927` — the writer stored the
  offset as a signed 32-bit int. A negative `startxref`/`/Prev` is taken modulo 2^32, and
  any object or xref offset that does not land on its `N G obj` header is retried at
  +4 GiB multiples (32-bit offsets wrapped past 4 GiB). If the xref is unusable the object
  table is rebuilt by scanning for `N G obj` headers and the last `/Root`.
  Not supported (not needed for these reports): encryption, filters other than Flate.
- **Detection** (`EeCvr_IsHartCvrPdf`): page 1 must have the `CVR Report` title, a
  `Cvr Id:` label **and** the `Contest Title` / `Option` table header — only the fields
  that survive county redaction (see below). Every selected PDF is checked before any work, so another vendor's PDF
  (or an unrelated PDF) aborts the load with "<file>: not a Hart Cast Vote Record report…".
- **Page → sheet**: runs inside one clip rectangle form a **cell** (a wrapped contest
  title is two runs in one taller cell, joined directly — SSRS keeps the trailing space on
  the first line). Header cells (above the table header) are parsed by label; table cells
  are split into the title column and the Option column at the Option header's x. Each
  Option cell attaches to the title cell whose vertical extent contains its centre (else
  the nearest title above; with none on the page, the previous page's last contest).
  Multiple Option cells on one title are multiple selections (vote for N) — no Tarrant
  export contains a valid multi-selection, so this rendering is assumed. Consecutive pages
  with the same Cvr Id are one sheet. The Cvr Id is lower-cased and `"3156 - 008"` precinct
  splits are normalized to the XML's `"3156-008"`, so both sources key identically.
- **Redacted reports** (Burnet County L25/G25): the county's redaction tool deletes the
  header text (no black boxes) — L25 keeps only Precinct and Cvr Id; G25 also keeps
  Party, Device Data Id and Central Batch Id — and strips the document Info. Any
  PDF-sourced key field (Batch Number, Voting Type, Polling Place, Device Type/Serial/
  Data Id) that is blank on **every** record gets no column (so Tarrant ED/EV PDFs have
  no Batch Number column and ABM PDFs no Polling Place). Redacted clip paths are
  polygons; the reader uses their bounding box.
- **Vote-for-N in the PDF**: a multi-seat contest is printed as **one row per seat with
  the title repeated** (Burnet L25 "CITY OF BURNET COUNCIL MEMBERS": `Undervotes: 1`,
  `Ricky Langley`, `Dennis Langley`). Rows with the same title on one sheet are merged
  into one contest (`sheet_find_contest`), so the seat count and tally match the XML
  model (Burnet council: 957 marks = 3 seats × 319 ballots).
- **Load modes** (`EeCvr_LoadFromHartFiles`, any mix of `.zip`/`.pdf`; anything else is
  rejected, and the load thread refuses to mix Hart and ES&S files):
  - **PDF only** — two passes over the PDFs (discover, fill). Frozen keys: `CvrGuid,
    [Batch Number], Precinct, [Party], [Voting Type], [Polling Place], [Device Type],
    [Device Serial], [Device Data Id]` (bracketed = only when non-blank on some record).
  - **ZIP + PDF** — one pass over the PDFs builds a lowercase-Cvr-Id → header-field map
    (`HartMeta`: interned values in a `StrMap`), then the usual two ZIP passes; pass 2
    decorates each row by Cvr Id (rows with no PDF record leave the fields blank). Votes
    come only from the ZIP. Frozen keys: the ZIP keys with `Voting Type, Polling Place,
    Device Type, Device Serial, Device Data Id` inserted before `Is Blank`. If no ZIP Cvr Id
    appears in the PDFs the load fails ("The PDF reports do not match the CVR zip files").
  - The new key names are in `cvr_is_key_header`, so CSV/TSV round-trips stay frozen.
  - Multi-card: a PDF-only load has no Sheet Number, so `EeCvr_HasMultiCard` falls back to
    the reference-contest heuristic.
- **Performance**: ~10–11k pages/s per pass. G24 (3.7 GB, 1,654,992 pages): open 6.5 s
  (page tree of 1.65M kids), one full pass 147 s.
- **Validated** — PDF-only vs. ZIP-only tabulations are **byte-identical** for every
  Tarrant pair: G25 ABM/ED/EV, L26 ABM/ED/EV, PR26 ABM/ED/EV × Dem/Rep, P26 ABM/ED/EV ×
  Dem/Rep, and G24 (see the session handoff for counts); ZIP+PDF tallies equal ZIP-only.
  The PDFs' Voting Type revealed that the local `L26 CVR-ED.zip` / `L26 CVR-EV.zip` are
  swapped (ED.pdf matches EV.zip exactly and vice versa).

  Test `hartpdf` (authors Hart-style PDFs with an xref stream, indirect `/Length`, a Type0
  font + ToUnicode, a record continued across pages, a wrapped title, vote-for-2,
  overvote, undervote, write-in; plus a bogus-`startxref` variant, a non-Hart PDF, ZIP+PDF
  decoration and a no-common-Cvr-Id error).

## Dominion voting-system CVRs (`dominion_cvr.c`)

Dominion Voting Systems (Democracy Suite; the company is now **Liberty Vote**) exports
CVRs as a `.zip` of JSON files. Developed and validated against twelve San Francisco
County exports, 2019–2026 (`Election_CVRs/CA_San_Francisco_County/`).

- **Format:** manifests `{"Version":..,"List":[..]}` — `ContestManifest` (Id,
  Description, VoteFor, NumOfRanks, Disabled), `CandidateManifest` (Id, Description,
  ContestId, Type = Regular / WriteIn / QualifiedWriteIn), `PrecinctPortionManifest`,
  `BallotTypeManifest`, `CountingGroupManifest` (Election Day / Vote by Mail),
  `TabulatorManifest` (Description, VotingLocationName) — plus the records: one
  `CvrExport.json` or one `CvrExport_<n>.json` per batch, each
  `{"Sessions":[..]}`. A **session** is one tabulation of one scanned card (a
  4-card ballot is 4 sessions); a ballot-marking-device (`QRVote`) session can hold every
  card of its ballot. Each session has `Original` and, when adjudicated, `Modified`
  (whichever has `IsCurrent`), each with `Cards[] → Contests[] → Marks[]`
  (`CandidateId`, `Rank`, `IsAmbiguous`, `IsVote`, `WriteinIndex`).
- **Three schema generations:** 5.2 (2019: no `Cards` — `Contests` sit directly under
  `Original`; no `SessionType`; no contest `Undervotes`/`Overvotes`; one 500 MB
  `CvrExport.json`), 5.10 (2020–2024: `Cards`, one file per batch), 5.19 (2025–2026:
  adds `CastVoteRecordId`, `BallotLayoutManifest`). The column layout is decided from
  the manifests plus the first session's shape.
- **Which marks count:** only `IsVote = true` marks — exactly what Dominion tabulates
  (ambiguous marks the adjudicator did not accept stay `IsVote = false`). Exception: 5.2
  sets `IsVote` only on the mark counted in the first round, so in a 5.2 ranked-choice
  contest every lower ranking and every overvoted mark is `IsVote = false`; there a
  ranking is any non-ambiguous mark. Separate write-in lines (`WriteinIndex` 0, 1, …)
  are separate marks, so two write-ins at one rank are an overvote.
- **Rows / key columns:** one row per session. Frozen keys: `Cvr Number` (5.19's
  CastVoteRecordId), `Record Id` (the ballot-image name from `ImageMask`, e.g.
  `00005_01198_000022` — still unique where the county redacted `RecordId` to `"X"`, as
  in Nov 2024), `Tabulator`, `Batch` (`<tabulator>-<batch>`, zero-padded — batch ids
  repeat across tabulators), `Counting Group`, `Polling Place` (the tabulator's
  VotingLocationName), `Precinct Portion`, `Ballot Type`, `Session Type`, `Card`
  (1-based PaperIndex; `1,2,3,4` for a session holding several cards), `Adjudicated`
  (Yes = the Modified version). Fields a version lacks get no column.
  `cvr_is_key_header` knows these names (CSV/TSV re-import keeps them frozen);
  `Cvr_FindReportColumn` maps the Precinct / Ballot Style / Voting Type reports to
  `Precinct Portion` / `Ballot Type` / `Counting Group`; `EeCvr_HasMultiCard` is exact
  via `Card` ≥ 2.
- **Contest columns** in ContestManifest order (disabled contests omitted). Vote-for-N:
  N columns (blank continuation headers → `col_group`), selections then `undervote`s;
  every column `overvote` when the contest's `Overvotes` > 0 (5.2: more non-ambiguous
  marks than seats). Unresolved write-ins read `Write-in` (the manifest name); qualified
  write-ins carry the candidate's name (SF's summary report folds those into its
  "Write-in" row).
- **Ranked choice:** a contest with `NumOfRanks` > 0 becomes one column per rank titled
  `<contest> (Rank N)` holding the candidate ranked there, `undervote` (nothing at that
  rank) or `overvote` (several candidates at that rank). Each rank is its own contest
  in the ordinary tabulation, so `(Rank 1)` gives the first-choice counts that match
  the official summary.
- **Redaction:** SF replaces some sessions' contest data (and potentially ids) with the
  text `"*** REDACTED ***"` (15 sessions in Nov 2025, 317 contests in Jun 2026). Such
  cells read `<Redacted>` (the existing redaction marker), never `undervote`.
- **Several zips** load together only when their key fields and contest columns are
  identical (error naming the file otherwise).
- **Parsing:** a small streaming JSON parser (no third party): each zip entry is
  inflated in 256 KB chunks via miniz's `mz_zip_reader_extract_iter_*`; each session is
  parsed into a reusable index-based DOM, turned into a row, and discarded. One pass.
  Speed ≈ 150 MB of JSON/s: Nov 2024 (5.0 GB JSON, 27,554 files, 1,603,908 sessions,
  42.5 M cells) loads in ~32 s; the 500 MB single-file 2019 export in ~3.5 s.
- **Detection / dispatch:** `EeCvr_IsDominionZip` (ContestManifest.json + a
  CvrExport*.json); `CvrLoadThreadProc` routes Dominion zips to
  `EeCvr_LoadFromDominionZips`, Hart zips/PDFs to the Hart loader, and refuses a mix of
  vendors.

### Ranked-choice (instant-runoff) tabulation (`ee_rcv.c`)

CVR **Reports → Tabulate Ranked-Choice Contests…** / **Tabulate Filtered Ranked-Choice
Contests…** (greyed without RCV contests / without a filter).

- **Engine:** `EeCvr_FindRcvContests` recognizes a run of `<base> (Rank 1)`,
  `(Rank 2)`, … columns (≥ 2 ranks) — from the Dominion loader or a CSV/TSV re-import.
  `EeCvr_TabulateRcv(t, first_col, rows|NULL, n, &result)` runs single-winner IRV with
  the options printed on SF's official Dominion RCV reports: single elimination;
  threshold = continuing ballots; **Exclude unresolved write-ins = True** (the ranking
  is passed over; a ballot with only write-ins is a Blank); **Declare winners by
  threshold = False** (continue until two candidates remain); **Skip overvoted rankings
  = False** (reaching an overvoted ranking stops the ballot, counted under Overvotes);
  **Assign skipped rankings to exhausted = False** (a skipped rank is passed over).
  Rows without the contest are ignored. A tie for last is broken by the most recent
  earlier round that separates the tied candidates, then by name, and flagged
  (`elim_tie`) — officials break real ties by lot. `EeRcvResult` holds per-round votes
  per candidate (finishing order: winner, runner-up, then latest-eliminated first),
  continuing / blanks / exhausted / overvotes, the eliminated candidate per round, the
  winner and the first round with a majority.
- **UI:** `CvrRcvWindow` (class `k_CvrRcvClassName`): owner-data list **Contest |
  Candidate | Result | Round 1…N**; per contest one row per candidate ("Winner (majority
  in round k)", "Runner-up", "Eliminated round k [(tie)]"; cells blank after
  elimination) then Continuing Ballots Total, Blanks, Exhausted, Overvotes and Non
  Transferable Total rows (as in the official report). Right-click Copy / Export
  Selected / Export All (`-…_RCV_Rounds`), Ctrl+C; one per CVR window
  (`CvrWindow.rcv_report`), closed with it; `CvrWindow.rcv_count` gates the menu.
- **Validated:** all 25 official SF RCV short reports available for the test exports
  match **exactly, every round** — every candidate, Continuing, Blanks, Exhausted and
  Overvotes value: Nov 2019 (Mayor, D5, DA — the 5.2 format), Nov 2020 (D1, D3, D5, D7,
  D9, D11), Nov 2022 (D4, D6, D8, D10, DA, Public Defender) and Nov 2024 (Mayor — 14
  rounds, Lurie 182,364 / Breed 149,113, Exhausted 57,859, Overvotes 2,229 — City
  Attorney, DA, Sheriff, D1, D3, D5, D7, D9, D11).

### Dominion validation (summary reports)

Tabulation compared contest-by-contest with SF's official `summary.xml` (candidate
totals, undervotes, overvotes): exact for every contest of Nov 2019 (15), Mar 2020
(23), Nov 2020 (41), Sep 2021 recall (2), Feb 2022 (5), Apr 2022 (1), Jun 2022 (24),
Nov 2022 (61), Mar 2024 (27) and Nov 2024 (48). Nov 2025 and Jun 2026 differ only by
the ballots the county redacted in the CVR (15 and 317 contest records, shown as
`<Redacted>`), which the official count includes. Qualified write-ins are compared
after folding them into the summary's "Write-in" row.

Tests `dominion` (synthetic 5.10 + 5.2 exports: entry ordering, keys, adjudication,
vote-for-2, ambiguous marks, RCV columns incl. duplicate/overvoted rankings and two
write-in lines, redaction, multi-card, a `É` escape, multi-zip mismatch, CSV round
trip) and `rcv` (rounds, transfers, ties, skipped/overvoted rankings, write-in exclusion,
blanks, exhausted ballots, majority round, filtered subset).

## Testing

Author small `.xlsx` files with miniz's writer (as the XLSX tests do): two files
with the same header → concatenated row count; a third with a different header →
load rejected with an error. Sparse round-trip: blank cells read back as "",
non-blank cells preserved; frozen-count detection. Export: `cvrexp` checks
`EeCvr_FormatDelimitedUtf8` header + CSV quoting vs. TSV; `vexport` checks the voter
formatter's header row and delimiter.
