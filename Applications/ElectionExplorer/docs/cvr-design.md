# Design: Cast Vote Record (CVR) support

**Status:** Phase 1 (data engine + loader + viewer window + File/Edit menus + copy)
and Phase 2 (vote tabulation report) implemented. Verified on real ES&S files,
including tabulation cross-checked against official published results.
**Date:** 2026-09-18 (Phase 2 added 2026-09-19)

## Motivation

Cast Vote Record exports (one row per ballot, one column per contest, the cell
holding the chosen candidate/selection) are a primary election-audit artifact.
ES&S and other systems export them as Excel `.xlsx`. This adds a **File → Load
Cast Vote Records…** flow that opens one or more `.xlsx` files and shows the
ballots in a sortable grid like the voter-list window.

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
- For each file, read the first worksheet via `EeXlsx_ReadSheet` (the existing
  reader) with a row sink.
- First row of the first file → establish `col_titles` + `frozen_count`
  (leading run of columns whose trimmed, case-insensitive title is one of the
  known keys; else freeze column 0).
- First row of every later file → compare to `col_titles`; mismatch →
  `EeLoadStatus_Error` with a message naming the offending file; caller loads
  nothing.
- Data rows → append a sparse row (skip blank cells; intern non-blank values).
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
uses a multi-select open dialog (`OFN_ALLOWMULTISELECT`), filtered to `.xlsx`.

## Phase 2 — vote tabulation (implemented)

**Reports → Tabulate CVR Votes…** on the CVR window opens a report tallying every
contest.

- Core (GUI-free): `EeCvr_Tabulate(t, &items, &count)` in `ee_cvr.{c,h}`. It scans
  the sparse entries once (O(entries), not rows×cols), skips the frozen key columns,
  and aggregates each non-blank selection **by `(col_group, value)`** — so a
  "vote for N" contest is summed across all of its columns under the contest's
  title. Blank cells (contest not on the ballot) are not counted;
  `undervote`/`overvote`/`[write-in]` are counted as the selections they are.
  Output is an `EeCvrTally[]` of `{contest, selection, count}` grouped by contest
  column; **within a contest the candidates come first (count descending, then
  text), followed by the non-candidate outcomes in the fixed order write-in,
  overvote, undervote** — even when a special out-counts a candidate.
  `EeCvr_FreeTally` frees it.
- UI: `CvrReportWindow` (class `k_CvrReportClassName`) — an owner-data three-column
  list (**Contest | Selection | Votes**); the contest name repeats on each of its
  selection rows. Bold/grey header via the shared `App_HeaderCustomDraw` (list
  subclass). Multi-select + **right-click → Copy** (and Ctrl+C) copy the rows as
  tab-separated UTF-8. It is an unowned top-level window (the CVR window can cover
  it), tracked in `CvrWindow.report`, one per CVR window, and closed when the CVR
  window closes. Tabulation runs synchronously behind a wait cursor (fast: L26's
  12,710 ballots × 91 columns tabulate instantly).
- **Verified against official results:** tabulating `L26 CVR.xlsx` reproduces the
  certified Travis County totals exactly (e.g. Bee Cave Mayor 871/369; Briarcliff
  Alderman, a Vote-For-3, 239/233/167/120/76/75/61/26 across its three columns; the
  two same-named Councilmember races counted separately at 830 and 826).

### Future Phase 2 polish

Blank-vs-`undervote`-vs-`overvote` rate summaries, ballot-style breakdowns, and
per-precinct cross-tabs. Count **blank** (contest not on ballot) separately from
**`undervote`** (on ballot, no selection) and **`overvote`** — do not lump them
together. (The current report already keeps `undervote`/`overvote` as distinct
selections and simply omits blanks.)

## Testing

Author small `.xlsx` files with miniz's writer (as the XLSX tests do): two files
with the same header → concatenated row count; a third with a different header →
load rejected with an error. Sparse round-trip: blank cells read back as "",
non-blank cells preserved; frozen-count detection.
