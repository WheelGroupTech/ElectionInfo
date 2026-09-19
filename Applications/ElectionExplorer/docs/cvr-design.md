# Design: Cast Vote Record (CVR) support

**Status:** In progress. Phase 1a (data engine + loader) landing first; then the
CVR viewer window + File menu; then Phase 2 (reports).
**Date:** 2026-09-18

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

## Frozen vs scrolling columns

Freeze the leading key columns; horizontally scroll the contest columns — same
frozen/scroll split as the voter window.

## UI (next phase)

A dedicated CVR window (own class + state) with two virtual list views (frozen +
scroll) driven by `EeCvr_GetViewCellW`, header-click sorting on any column
(numeric-aware for `Cast Vote Record`), reusing the voter grid's rendering
helpers where they are not `AppState`-coupled. **File → Load Cast Vote Records…**
uses a multi-select open dialog (`OFN_ALLOWMULTISELECT`), filtered to `.xlsx`.

## Phase 2 (later)

Report/analysis over the loaded CVR data (e.g. per-contest tallies,
undervote/overvote rates, ballot-style breakdowns). Count **blank** (contest not
on ballot) separately from **`undervote`** (on ballot, no selection) and
**`overvote`** — do not lump them together.

## Testing

Author small `.xlsx` files with miniz's writer (as the XLSX tests do): two files
with the same header → concatenated row count; a third with a different header →
load rejected with an error. Sparse round-trip: blank cells read back as "",
non-blank cells preserved; frozen-count detection.
