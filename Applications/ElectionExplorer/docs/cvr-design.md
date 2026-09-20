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

## Phase 2 — vote tabulation (implemented)

**Reports → Tabulate CVR Votes…** on the CVR window opens a report tallying every
contest.

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

## Per-column value reports (Batch / Precinct / Ballot Style)

**Reports → Display Batch Report… / Display Precinct Report… / Display Ballot Style
Report…** on the CVR window each open a two-column report — the distinct
**Batch / Precinct / Ballot Style** values on the left and the **number of ballot
records** carrying each on the right — modeled on the voter-list Precinct/Address
reports.

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
  `(blank)` row is appended for records with no value in the column. Multi-select +
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

Tabulation (Reports → Tabulate CVR Votes…) still counts **all** ballots, not the
filtered subset — consistent with the voter-list reports, which ignore filters.

### Future Phase 2 polish

Blank-vs-`undervote`-vs-`overvote` rate summaries, ballot-style breakdowns, and
per-precinct cross-tabs. Count **blank** (contest not on ballot) separately from
**`undervote`** (on ballot, no selection) and **`overvote`** — do not lump them
together. (The current report already keeps `undervote`/`overvote` as distinct
selections and simply omits blanks.)

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

## Testing

Author small `.xlsx` files with miniz's writer (as the XLSX tests do): two files
with the same header → concatenated row count; a third with a different header →
load rejected with an error. Sparse round-trip: blank cells read back as "",
non-blank cells preserved; frozen-count detection. Export: `cvrexp` checks
`EeCvr_FormatDelimitedUtf8` header + CSV quoting vs. TSV; `vexport` checks the voter
formatter's header row and delimiter.
