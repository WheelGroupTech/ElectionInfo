# Session Handoff — ElectionExplorer

> Live handoff note passed between the desktop and laptop via git.
> Update this at the end of each session; read it at the start of the next.
> Keep it short and current — git history is the permanent record.

**Last updated:** 2026-09-20
**Branch:** main — Store prep, the Travis address fix, the full
**XLSX import** feature, and the complete **CVR support** (incl. the CVR **Filter**
menu) are committed in `main`. CVR =
the sparse Cast Vote Record engine + multi-file loader (`src/ee_cvr.{c,h}`), the CVR
viewer window + **File → Load Cast Vote Records…** (`main.c`), **write-in image
detection** in `xlsx.c` (a contest cell whose only content is an anchored picture —
ES&S write-ins — reads `[write-in]` instead of blank), **"vote for N" multi-column
contest handling**, **Phase 2 vote tabulation** (Reports → Tabulate CVR Votes…) with
a per-user **Edit → Options… "Merge image and text write-ins"** toggle,
**selection whitespace normalization**, **multi-card/multi-page detection**
(`EeCvr_HasMultiCard`; status bar reads "N ballot records" and appends "(multi-card
ballots detected)" when a minority of rows lack a top-of-ballot reference contest —
President/Governor/U.S. Senator, excl. Lt. Governor), and treating ES&S's
**`No image found`** placeholder as a write-in variant. Builds clean (x64
Debug+Release), full smoke suite passes (incl. `cvr`, `cvrmulti`, `cvrtab`,
`cvrmerge`, `cvrmc`, `cvrws`, `writein`); tabulation validated exactly against
official published results for **nine**
elections across two counties — Travis (L26, P26, PR26, G24 = 6 files/587,090, G20 =
7 files/612,696) and Dallas (P26 Rep = 2 files/103,818, P26 Dem = 3 files/280,326,
P24 = Mar 2024 combined primary, 3 files/231,465 = Rep 105,410 + Dem 126,055, G24 =
Nov 2024 general, 9 files/873,455 rows = 858,562 ballots + 14,893 continuation cards).
Dallas P26 Dem / G24 revealed the CVR is **one row per ballot card** (multi-sheet
ballots ⇒ more rows than ballots; contest tallies unaffected — all matched); Dallas
P24 (single-card combined primary) confirms the multi-card detector does not
false-positive on a real combined primary, and Dallas G24 confirms it flags a real
multi-card general.

**CVR CSV/TSV loading (committed).** The CVR loader accepts delimited-text exports as
well as `.xlsx`: `src/csv_sheet.{c,h}` (`EeCsv_ReadSheet`, same row-sink contract as
`EeXlsx_ReadSheet`) handles UTF-8 (BOM or not; BOM-less falls back to the system ANSI
code page when not valid UTF-8), UTF-16LE/BE (BOM), RFC-4180 quoting, and CRLF/LF/CR.
`EeCvr_LoadFromFiles` dispatches per file by extension (`.xlsx`→XLSX, else CSV), so
mixed-format same-header files concatenate; the File→Load filter is
`*.xlsx;*.csv;*.tsv;*.txt`. `append_data_row` drops a row with no contest cell (never a
real ballot), absorbing Excel's trailing `,,,,` line and the odd record it breaks with
a spurious unquoted newline. Caveat: Excel drops write-in **images** when saving to
CSV/TSV (text variants survive). Validated byte-identical across formats for Travis L26
& P26; Travis G24 and Dallas G24 match every candidate/undervote/overvote count (only
image write-in rows differ). Tests `cvrcsv`.

**CVR per-column value reports (committed).** CVR **Reports** → **Display Batch /
Precinct / Ballot Style Report…**, two-column (value | number of ballot records)
reports modeled on the voter Precinct/Address reports. `CvrValueReportWindow`
(`k_CvrValueReportClassName`): bold/grey header, header-click sort, `(blank)` row,
right-click Copy + Include/Exclude (into the CVR window's `EeFilterSet`). Menu-gated by
`CvrWindow.vreport_avail[]` (resolved once at load) via new `ee_cvr` helpers
`EeCvr_FindColumnByTitle` / `EeCvr_ColumnHasReportableData` (FALSE for an all-blank or
all-redacted column) / `EeCvr_CollectColumnCounts`. Test `cvrcnt`.

**Uncommitted (this session): CSV/TSV export.** Every voter/CVR view and report can
export to **UTF-8 CSV (default) or UTF-8 TSV** (BOM-prefixed so Excel reads UTF-8), in
the window's current display order, with a header row. Shared plumbing in `main.c`:
`App_PromptExportPath` (Save dialog; CSV filter 1 / TSV filter 2; typed `.csv`/`.tsv`
wins else extension appended → delimiter), `App_WriteExportUtf8` (BOM + bytes),
`App_ExportReportModel` (wide-cell callback for the small reports),
`App_CollectSelectedIndices`, and a `Utf8Export` field builder. Suggested name =
source file base name (no ext) + a window suffix.
- **Voter window:** File → **Export Voter List…** (visible/filtered rows,
  `-Filtered_Voters`/`-All_Voters`) and right-click → **Export Selected…**
  (`-Selected_Voters`), each with an **Include normalized data fields** option
  (default off, `App_AskExportNormalized` modal). Efficient UTF-8 via new
  `EeVoterTable_FormatDelimitedUtf8` (delim + header; old `FormatCopyUtf8` now delegates).
- **Voter Precinct/Address reports:** right-click → **Export Selected…**/**Export
  All…** → `-…_Precincts` / `-…_Addresses`.
- **CVR window:** File → **Export Cast Vote Records…**
  (`-Filtered_Records`/`-All_Records`) and right-click → **Export Selected…**
  (`-Selected_Records`). Efficient UTF-8 via new `EeCvr_FormatDelimitedUtf8`
  (interned values, RFC-4180 quoting, header).
- **CVR reports:** right-click Export Selected/All on Tabulation
  (`-…_Contests`), Batch (`-…_Batches`), Precinct (`-…_Precincts`), Ballot Style
  (`-…_Ballot_Styles`). CVR base name stored in `CvrWindow.base_name`.
New IDs `IDM_FILE_EXPORT_VOTERS/_CVR`, `IDM_EXPORT_SELECTED/_ALL`,
`IDC_EXPORT_NORMALIZED`. Files: `main.c`, `resource.h`, `voter_table.{c,h}`,
`ee_cvr.{c,h}`, `docs/cvr-design.md`, `test/README.md`, `test/smoke_load.c`. Tests
`vexport`, `cvrexp`. Full suite green (40); app builds clean x64 Debug+Release.
**Not yet GUI click-tested.**

The app is being published via the **Microsoft Store**.

---

## What ElectionExplorer is

Win32 GUI (C11, no MFC, static CRT, VS 2026 `v145`; x64/ARM64 Debug+Release via
`Applications/Applications.sln`). Loads large county voter-list exports
(CSV/TSV) in the background into an in-memory table and shows them in a
dual-pane virtual grid with frozen columns (Voter ID, Precinct, Name, Address),
sorting, filtering, and address normalization.

Source: [ElectionExplorer/src/](ElectionExplorer/src/) —
`main.c` (window/UI), `voter_table.c` (load + storage + normalization),
`filter.c` (filter dialog + predicates).

## Current state

Recent work (newest first) has centered on data-quality checks and normalized
display:

- Duplicate detection: duplicate Voter IDs, and an initial check for duplicate
  voter records sharing the same name + DOB.
- Normalized data pane: added Precinct; improved normalized address processing
  and block-number handling.
- Partial / imperfect birth dates handled during parse and filtering.

## CVR support — Phases 1 & 2 (committed)

Cast Vote Record import — data engine, loader, viewer window, and tabulation
reports. Decisions and design: `docs/cvr-design.md`.
- **CVR window + File menu (`main.c`):** new `File → Load Cast Vote Records…`
  opens a multi-select (`OFN_ALLOWMULTISELECT`) `*.xlsx` dialog; files load on a
  worker thread behind a modal marquee progress dialog with Cancel; on success a
  dedicated top-level **CVR window** (`CvrWndProc`, class `k_CvrClassName`) shows
  the ballots in an owner-data (virtual) report ListView — every column, header-
  click sort on any column (`EeCvr_SortByColumn`), status bar `N ballots · M
  columns`. Modeled on the Report/Diff windows; owns its `EeCvrTable`, frees on
  close. Schema-mismatch aborts with an error message box.
- **CVR window polish (`main.c`):**
  - **Bold/grey column headers** matching the voter list — `CvrListSubclass`
    forwards the header `NM_CUSTOMDRAW` to `App_HeaderCustomDraw(app, cd, FALSE)`
    (same mechanism as the Differences window).
  - **Independent window** — created with owner `NULL` (was `app->hwnd_main`) so the
    voter list can overlap it instead of it always staying on top.
  - **Menu bar** (`App_CreateCvrMenu`): **File** (Load Voter List Ctrl+O, Load Cast
    Vote Records, **Close Cast Vote Records** = new `IDM_FILE_CLOSE_CVR`, Exit) +
    **Edit** (Copy Ctrl+C), handled in `CvrWndProc` `WM_COMMAND`.
  - **Copy selected ballots** — `Cvr_CopySelected` writes the selected rows as
    tab-separated UTF-8 (all columns, CR/LF between rows) to the clipboard;
    reachable via Edit→Copy, Ctrl+C (shared accelerator table), and right-click →
    Copy (`Cvr_OnContextMenu`, `NM_RCLICK`). Click-tested OK.
- **"Vote for N" multi-column contests (`ee_cvr.c`, `xlsx.c`):** a
  contest that lets a voter pick several candidates spans multiple columns; only the
  first is titled and each additional column has a **blank** header. Previously these
  showed as stray blank-titled columns after the contest.
  - `cvr_build_titles` keeps the first column's title, gives each **blank**
    continuation the derived display title `<contest> (2)`/`(3)`/…, and records
    `col_group[i]` (new `EeCvrTable` field) = the contest's title column so **Phase 2
    can tabulate a race across all its columns** (its stated purpose). The derivation
    also runs in `header_matches`, so the multi-file identical-schema check still
    works. N selections stay N sortable/copyable columns (user chose columns, not a
    merged cell).
  - **Only blank headers merge; a repeated identical title is a *separate* race.**
    Confirmed against the official Travis County results
    (`results.enr.clarityelections.com/TX/Travis/126203/web.345435`): two adjacent
    identically-named "City of Bee Cave, City Councilmember at Large" columns are two
    separate (Vote For 1) races, while the blank-header contests are genuinely
    multi-seat (Briarcliff Alderman = Vote For 3, Ensenadas Director = Vote For 5,
    Ranch at Cypress Creek = Vote For 2). (An earlier draft also merged repeated
    titles; reverted after this check.)
  - **Reader bug fixed in `xlsx.c`:** self-closed cells (`<c r="E1"/>`) were skipped
    with a `</c>`-length advance, dropping the *next* cell — so rows of adjacent
    empty cells (blank continuation columns) came up short. Now resumes just past
    the `/>`. Benefits all `.xlsx`.
  - Test `test_cvr_multiselect` (tag `cvrmulti`) covers blank-header grouping,
    repeated-title separation, and self-closed empties. Verified on real
    `L26 CVR.xlsx` (91 cols): Briarcliff (3), Ensenadas Director (5), Ranch at
    Cypress Creek (2), Travis MUD 15 (3) grouped; Bee Cave stays two columns. Full
    suite green; app + tests build clean x64.
- **Phase 2 — CVR vote tabulation (`ee_cvr.c`, `main.c`):** the CVR
  window gains a **Reports** menu (after Edit) with **Tabulate CVR Votes…**
  (`IDM_CVR_TABULATE`).
  - Core: `EeCvr_Tabulate(t, &items, &count)` / `EeCvr_FreeTally` — scans the sparse
    entries once (O(entries)), skips frozen key columns, aggregates each non-blank
    selection by `(col_group, value)` so a "vote for N" contest is summed across its
    columns under the contest title. Returns `EeCvrTally[]` `{contest, selection,
    count}` grouped by contest; within a contest candidates first (count desc), then
    write-in, overvote, undervote (fixed order, even if a special out-counts a
    candidate). Blanks (not-on-ballot) omitted.
  - UI: `CvrReportWindow` (class `k_CvrReportClassName`) — owner-data 3-column list
    **Contest | Selection | Votes**, bold/grey header (shared `App_HeaderCustomDraw`
    via list subclass), multi-select **right-click → Copy** + Ctrl+C (tab-separated
    UTF-8). Unowned top-level, tracked in `CvrWindow.report`, one per CVR window,
    closed when the CVR window closes. Runs synchronously behind a wait cursor.
  - **Write-in merge (`merge_writeins` param, default ON):** some contests record
    write-ins two ways — the `[write-in]` image marker and a literal `Write-in` text
    value (seen in G24). Merge ON collapses a contest's write-in variants into one
    `write-in` row (summed), matching official "Write-in" totals; OFF keeps them
    separate. Toggle via **Edit → Options… → "Merge image and text write-ins"** on
    the CVR window (`App_ShowCvrOptions`/`CvrOptionsDlgProc`), persisted as
    `g_settings.cvr_merge_writeins` (registry `CvrMergeWriteins`); changing it
    re-tabulates the open report (`App_RefreshCvrReport`). Test `cvrmerge`.
  - **Selection whitespace normalization (`normalize_ws` in `ee_cvr.c`):** collapse
    space/tab runs to one space and trim ends before interning, so exporter quirks
    (e.g. `John   Cornyn` → `John Cornyn`) display cleanly and equivalent selections
    share one tally; UTF-8 safe; all-whitespace → blank; headers untouched. Test
    `cvrws`.
  - **Validated against official results (four elections, all exact):**
    `L26` (May 2026 local; Bee Cave Mayor 871/369; Briarcliff Alderman Vote-For-3
    239/233/167/120/76/75/61/26; two same-named Councilmember races 830 & 826);
    `P26` (Mar 2026 primary, 274,443 ballots, both parties; 127 MB, ~18 s);
    `PR26` (June 2026 runoff, 97,460 ballots, all 10 contests); **`G24`** (Nov 2024
    general, **6 files → 587,090 ballots**, ~35 s; President Harris 398,968 /
    Trump 170,781; merged write-in 3,690 = 3,680 image + 10 text). Result pages under
    `results.enr.clarityelections.com/TX/Travis/{126203,125931,126357,122432}`.
  - Tests `test_cvr_tabulate` (`cvrtab`), `test_cvr_merge_writeins` (`cvrmerge`),
    `test_cvr_whitespace` (`cvrws`). Full suite green; app builds clean x64.
- **Click-test fix:** some ES&S files (Dallas P26) store the Cast Vote Record as a
  float (`1.0`), which displayed as `1.0` and sorted lexically. `xlsx.c` now
  normalizes integer-valued numeric cells (drops the trailing `.0`, no scientific
  notation) so it shows/sorts as `1` — verified on P26 Dem. Guarded by `test_cvr`
  (CVR values authored as floats). Benefits all `.xlsx`.
- **Write-in image detection (`xlsx.c`):** ES&S renders a write-in selection as a
  scanned image pasted over the (text-empty) contest cell, so it would otherwise
  read blank. The reader now resolves the worksheet's drawing part
  (`worksheets/_rels/sheetN.xml.rels` → `Relationship Type …/drawing` → target),
  parses each `<xdr:twoCellAnchor>`'s `<xdr:from>` `(row,col)`, and while streaming
  overlays the marker **`[write-in]`** onto empty cells that carry an anchored
  picture. Presence only — the handwriting is a raster; OCR is out of scope.
  Benefits every `.xlsx`. Test `test_xlsx_writein` (tag `writein`). Verified on the
  real **Travis G24** set (6 files, 587,090 ballots): President write-ins that had
  read blank now show `[write-in]` (3,680 in President; also Senator, Railroad
  Commissioner, a JP race). See `docs/xlsx-import-design.md` §6.1.
- **Domain note for Phase 2 (recorded in the design doc):** in ES&S CVRs a
  **blank** contest cell = contest not on that ballot; **`undervote`** = on the
  ballot, no selection made; **`[write-in]`** = a write-in was cast (image cell).
  Count them separately in reports. Unknown yet whether other vendors match this
  convention.
- **`src/ee_cvr.{c,h}` — sparse CVR table.** One row per ballot; leading key
  columns (`Cast Vote Record, Batch, Ballot Status, Precinct, Ballot Style`, as
  present) are frozen, the rest are contests. Cells stored **sparsely** (CSR:
  per-row non-blank entries) with **value interning**, so thousands of mostly-blank
  contest columns cost nothing. API: `EeCvr_Init/Clear`, `EeCvr_LoadFromFiles`,
  `EeCvr_GetViewCellW`, `EeCvr_SortByColumn` (numeric-aware, `qsort_s`).
- **Multi-file = identical schema, concatenated** (per decision): the first file
  sets columns; any later file whose header differs → error window naming the
  file, load nothing. Reuses `EeXlsx_ReadSheet` (one sink per file).
- **Verified on real ES&S files:** Dallas P24 (123 cols, frozen 3, 99,999 rows,
  ~5 s); concat 0+1+2 → 231,465 rows; Travis G24 (157 cols, frozen 5, contest
  `President / Vice President (4268)` = `(D) Kamala D. Harris (EC2)`); cross-county
  pair correctly rejected. Test `test_cvr` (tag `cvr`); full suite passes; builds
  clean (app + tests). `.vcxproj`/`.filters` + `test/README.md` updated.
- **Next (Phase 1b):** the CVR viewer window (dedicated class, frozen/scroll grid,
  header-click sort via `EeCvr_SortByColumn`) + **File → Load Cast Vote Records…**
  with a multi-select (`OFN_ALLOWMULTISELECT`) `.xlsx` dialog. Then Phase 2 reports.

## Recently committed

XLSX import (reader, sheet picker, Phase 3 fidelity), MSIX packaging + Store
association, privacy/trademarks, app icon, and the Travis address fix are all
committed. The dated sections below are committed history.

## This session (2026-09-16) — Store association

**Associated the package with the Store (done in Visual Studio).** Reviewed the
staged changes — correct and consistent with the manifest identity.
- New `ElectionExplorer.Package/Package.StoreAssociation.xml` (staged): Publisher
  `CN=19C9DED9-…980D6`, PublisherDisplayName `WheelGroupTech`,
  MainPackageIdentityName `WheelGroupTech.ElectionExplorer`, ReservedName
  `Election Explorer`. No secrets (all public Store identifiers). Wired into the
  `.wapproj` as `<None Include=…>`.
- `.wapproj` (staged): added `GenerateTemporaryStoreCertificate=True` (benign —
  Store signs the upload; local signing stays off) and the `<None>` above; the
  rest of the diff is VS reformatting (multi-line `Content`, blank-line/trailing-
  newline changes), no functional change.
- Added `*.pfx` to the repo-root `.gitignore` (committed) — a temporary store
  cert could otherwise be dropped next to the project and accidentally committed.
  Verified ignored via `git check-ignore`. No `.pfx` exists yet.
- `Package.appxmanifest` was NOT rewritten by the association (identity intact).

## XLSX import — Phases 1–3 + GUI (committed)

Wrote `ElectionExplorer/docs/xlsx-import-design.md` — a proposal to read Excel
`.xlsx` (voter lists, rosters, CVRs, ePollbook exports) by emitting rows into the
existing load pipeline. `.xlsx` = ZIP of XML parts, so it needs a vendored
DEFLATE decompressor (Windows has no ZIP-DEFLATE API) + a small streaming XML
scanner (no COM). **Decisions resolved 2026-09-17:** (1) ZIP engine = vendor
**miniz** (Option A); (2) ship **Phases 1–2 first** (values as stored; date/number
fidelity is a Phase 3 follow-up); (3) **sheet picker up front** (part of v1);
(4) tests via a **generator** (Python/openpyxl) + runtime-built `.xlsx`, no
binaries; (5) `t="b"`→TRUE/FALSE, `t="e"`→error text. Doc updated to match.

**Implemented + tested (committed) — reader, row-sink refactor, GUI, Phase 3.**
XLSX import works end to end.
- Vendored **miniz 3.0.2** at `src/third_party/miniz/` (`miniz.c`, `miniz.h`,
  `LICENSE`, `README.md`).
- `src/xlsx.{c,h}` — self-contained reader: reads the file into memory, opens it
  with miniz, extracts parts, and scans with a small XML reader (no COM). Handles
  BOTH shared strings and inline strings, numeric/bool(`TRUE`/`FALSE`)/error
  (verbatim) cells, `r=`-addressed gap-filled rows, sheet enumeration, progress +
  cancel, and size caps (zip-bomb guard). API: `EeXlsx_ListSheets`,
  `EeXlsx_ReadSheet`, `EeXlsxRowSink`.
- **Row-sink refactor** in `voter_table.c`: extracted `ingest_header` +
  `ingest_row` (shared by CSV and XLSX) and a `LoadColumnMap`; CSV path now calls
  them (behavior-preserving — all 26 prior tests still pass). New public
  `EeVoterTable_LoadXlsxSheet`; `EeVoterTable_LoadFromFile` dispatches a `.xlsx`
  path to sheet 0.
- `.vcxproj`/`.filters` wired (`xlsx.c`; `miniz.c` with per-file warning
  suppression). App builds clean, x64 Debug + Release.
- Tests: `test_xlsx_roundtrip` (tag `xlsx`) authors a tiny `.xlsx` via miniz to
  cover the shared-string + error-cell paths; full smoke suite passes (`xlsx ok`).
  `test/README.md` compile line updated (adds `xlsx.c` + `miniz.c`).
- Verified on the openpyxl fixture end to end via `EeVoterTable_LoadFromFile`:
  sheet list, inline strings, gap-filled empties, Name/Address composition. Date
  cells surface as **serials** (e.g. DOB `28957`) — the documented v1 limitation;
  Phase 3 (`styles.xml`) converts them and preserves leading zeros.
- **Finding:** openpyxl emits `t="inlineStr"` with no `sharedStrings.xml`; real
  Excel files use shared strings. Both paths are handled + tested.
- **GUI (`main.c`):** File→Open filter now includes `*.xlsx`; opening a workbook
  with >1 sheet shows a modal **sheet picker** (`SheetPickerDlgProc` /
  `App_PickSheet`, listbox + Load/Cancel) before loading. `App_StartLoad` gained a
  `sheet_index`; `LoadThreadProc` calls `EeVoterTable_LoadXlsxSheet` for `.xlsx`,
  else `EeVoterTable_LoadFromFile`. New IDs `IDC_SHEET_LIST`/`IDC_SHEET_LABEL`.
  Builds clean (x64 Debug+Release); **click-tested OK** with the 3-sheet fixture.
- **Phase 3 fidelity (`xlsx.c`):** reads `xl/styles.xml` (`cellXfs` → numFmt) and
  the workbook date system; a date-formatted numeric cell now loads as
  `YYYY-MM-DD` (was a serial), and a zero-padded format keeps leading zeros
  (ZIP `00000`). Custom + builtin date formats handled; `date1904` respected.
  Test `test_xlsx_styles` (tag `xlsxfmt`); verified on the real openpyxl fixture
  (DOB `28957` → `1979-04-12`). Known caveats: extract-to-heap per part (memory
  ~2× on huge sheets); arbitrary custom number formats beyond date/zero-pad fall
  back to the stored value.

## Travis normalized-address fix (committed)

**District codes appended to normalized address (Travis 2026-07-06 export).** The
first record showed `1109 N IH 35  NB AUSTIN TX 78702, C10, 5` instead of
`…78702`. Root cause: the file has a complete `Residential Address` column plus a
jurisdiction-code column literally named `CITY` (`C10`) and a
`STATE BOARD OF EDUCATION` column (`5`); the header heuristics classify those as
residence city/state, and `compose_address` always appended the city/state/ZIP
tail from columns.
- Fix in `compose_address` (`voter_table.c`): in the full-address branch, if the
  address already ends with its own ZIP tail **and** none of the city/state/ZIP
  columns match that tail, skip appending them (`append_tail = FALSE`). New helper
  `last_token_is_zip`. The `123 Main St` + real City/State/Zip case (no ZIP tail)
  still appends — the `resdup` test covers both.
- Regression test `test_district_codes_not_appended` (tag `distcode`). Full smoke
  suite passes; clang-clean. Verified on the **real 929,662-row Travis file**:
  row 0 address is now `1109 N IH 35  NB AUSTIN TX 78702`.
- Files: `src/voter_table.c`, `test/smoke_load.c`. Committed.
- Note: the header heuristics still *classify* `CITY`/`STATE BOARD OF EDUCATION`
  as city/state (loose `header_contains` matches at ~line 2697-2703 in
  `voter_table.c`); the compose gate neutralizes the effect for ZIP-tailed
  addresses. Tightening detection is a possible follow-up but was left alone to
  avoid breaking legitimate `CITY`/`STATE` residence columns.

## App icon refresh (committed)

**App icon refresh.** Rebuilt `res/app.ico` from the transparent
`logo-source.png` — a multi-resolution icon (16/24/32/48/64/128/256) using a
**square crop** of the portrait logo (previously the icon topped out at 64px and
was upscaled). New `ElectionExplorer.Package/tools/generate-icon.ps1` (ImageMagick
7+) regenerates it; documented in the package `README.md`. App rebuilt so the icon
is compiled in, and the **MSIX bundle was rebuilt** to include it (verified: x64 +
arm64, identity intact). Icon not yet eyeballed in a real Explorer/title-bar view;
the 16px frame is a busy composition — swap to a simplified small-size glyph later
if it reads poorly.

**Store listing prep (committed earlier this session).**
- `ElectionExplorer/PRIVACY.md` — no data collection; local-only; discloses that
  "Show in Maps" opens the chosen provider in the browser. Store privacy URL
  (GitHub Pages, needs Pages enabled to resolve):
  `https://wheelgrouptech.github.io/ElectionInfo/Applications/ElectionExplorer/PRIVACY`
- About dialog shows a **Privacy Policy** link under the GitHub link
  (`k_PrivacyUrl`, `IDC_ABOUT_PRIVACY`); dialog is 320px tall (the long URL wraps
  to two lines).
- `ElectionExplorer/TRADEMARKS.md` — code is MIT, names/logos are WheelGroupTech
  trademarks (not licensed by MIT). License stays **MIT** (repo-root `LICENSE`).
  For Partner Center "License terms," accept MS Standard Application License Terms.
- `res/ElectionExplorer.rc` brand strings fixed to `WheelGroupTech`.
- Synthetic sample datasets under `ElectionExplorer/docs/` (+ generators in
  `docs/sample-data/`) for Store screenshots: base list, an edited copy for
  Compare, and a duplicates list (with DOB). Screenshots already taken.
- Store note: the `runFullTrust` capability triggers a Partner Center approval
  prompt (a warning, not a blocker) — expected for a packaged Win32 app.

## MSIX packaging for the Microsoft Store (committed)

**MSIX packaging project.** New
`ElectionExplorer.Package/` — a Windows Application Packaging Project (`.wapproj`)
that wraps `ElectionExplorer.exe` as MSIX and builds a single **x64 + ARM64**
`.msixupload` for the Store. Verified end to end: the bundle builds to
`Build/msix/ElectionExplorer.Package_1.0.1.0_x64_arm64_bundle.msixupload`.
- Files: `ElectionExplorer.Package.wapproj`, `Package.appxmanifest`,
  `Images/*.png` + `tools/logo-source.png` (assets generated by
  `tools/generate-assets.ps1`), `README.md`. Added to `Applications.sln`
  (ActiveCfg only, **no Build.0**, so a normal solution build stays fast; package
  on demand). `.gitignore` updated for the project's `bin/`, `obj/`,
  `AppPackages/`, `BundleArtifacts/`.
- **Store identity filled in** (from Partner Center): Name
  `WheelGroupTech.ElectionExplorer`, Publisher
  `CN=19C9DED9-26A0-4883-97F5-C2A81BF980D6`, PublisherDisplayName
  `WheelGroupTech`. Ready to upload.
- **Logo:** `tools/logo-source.png` (ballot-box + magnifier art, 1152×1712) has a
  **real alpha channel**, so assets are rendered on a **transparent** canvas
  (transparent tiles + unplated taskbar icons), cropped to the logo content.
  Manifest `BackgroundColor=#FFFFFF` (shows behind the logo on plated tiles and
  splash; kept white per the user's choice). User was shown the transparent tiles.
- Build command (also VS → Publish → Create App Packages):
  `msbuild ElectionExplorer.Package\ElectionExplorer.Package.wapproj /p:Configuration=Release /p:Platform=x64 /p:AppxBundle=Always /p:AppxBundlePlatforms="x64|arm64" /p:UapAppxPackageBuildMode=StoreUpload`
- First Store upload was rejected once because a **stale bundle** (built before
  the PublisherDisplayName fix) was uploaded; a clean rebuild fixed it. Always
  rebuild + re-grab the `.msixupload` from `Build/msix/` after any manifest/`.rc`
  change. See the project `README.md` for full submission + sideload-signing steps.
- Gotchas found & fixed (documented in the README): renamed the `AppConfig`
  property in `Directory.Build.props` (it collided with MSBuild's reserved
  `AppConfig` and broke `.wapproj`); assets must be `<Content>`+`<Link>` (not
  `<Image>`) to be harvested; dropped the 256px small-logo targetsizes (they can
  exceed the 200 KB asset cap). Logo source is now the transparent 1152px PNG, so
  tiles are crisp.

**Registry-backed option persistence (committed this session).** User options
now survive between runs,
stored per user in the registry under
`HKEY_CURRENT_USER\Software\WheelGroupTech\ElectionExplorer`, following
Microsoft's guidance for
Win32 desktop settings: per-user options under HKCU (no elevation), REG_DWORD
values, key path carries no version segment so upgrades preserve options.
Persisted: `ZoomPercent`, `MapEngine`, `CopyPrependNormalized`,
`NameSurnameFirst`.
- New `src/settings.{c,h}` module (`EeSettings` struct; `EeSettings_Load/Save`
  on the default key, `EeSettings_LoadFrom/SaveTo(subkey, …)` for testing,
  `EeSettings_Defaults`). Links `advapi32.lib` via `#pragma comment`.
- `main.c` wiring: global `g_settings`; `EeSettings_Load` at the top of
  `wWinMain`; `App_InitViewerState`'s no-prefs branch (the first window of the
  run) now seeds from `g_settings` instead of hardcoded defaults; the Options
  dialog `IDOK` handler saves after applying (single choke point for all four
  options). New windows still inherit live values from their parent window.
- **Store note:** for an MSIX/Store package these HKCU writes are transparently
  virtualized by the packaging runtime, so no code change is needed — the same
  registry APIs work. Documented in `settings.h`.
- Added `settings.{c,h}` to `.vcxproj` + `.filters`; unit test
  `test_settings_roundtrip` (tag `settings`, uses a throwaway
  `…\Election Explorer Test` key so real options are untouched); updated the
  `test/README.md` compile command (adds `src\settings.c` + `advapi32.lib`).
- Verified: x64 Debug **and** Release build clean; full smoke suite passes
  (26 tests incl. `settings roundtrip: ok`). **GUI not yet click-tested** — i.e.
  change options, reopen the app, confirm they stuck.
- Note on tooling: use `C:\Program Files\LLVM\bin\clang-format.exe` (22.1.8).
  The committed C baseline isn't uniformly clang-clean under it (`main.c`,
  `voter_table.c` have pre-existing comment-alignment violations), so a
  whole-file `-i` run churns unrelated lines. The new `settings.{c,h}` are
  formatted; edits to `main.c` / `test/smoke_load.c` were hand-written in style
  and verified clean with `clang-format --dry-run` on the added lines.

## Prior session (2026-09-08) — committed

**Version 1.0.1.0 + About "Open Source" line.** Bumped the app version from
`0.1.0.0` to `1.0.1.0` in `res/ElectionExplorer.rc` (`FILEVERSION` /
`PRODUCTVERSION` and the `FileVersion` / `ProductVersion` strings) and in
`res/app.manifest` (`assemblyIdentity version`). The About dialog reads the
version from the compiled binary at runtime (`App_GetVersionString`), so no code
change was needed for the number to update. In the About dialog (`main.c`, in the
`AboutDlgProc` `WM_INITDIALOG`) added a static label **"Election Explorer is Open
Source:"** directly above the GitHub `SysLink` (`k_RepoUrl`), with the standard UI
font and a 22 px gap. Debug x64 builds clean.

**Ignore `Voter_Lists/`.** Added `Voter_Lists/` to the repo-root `.gitignore`
(the raw county voter-registration exports are too large to push to GitHub).
Nothing under it was ever tracked, so no `git rm --cached` was needed;
`git check-ignore` confirms it is now ignored.

## Recently committed (Compare feature family — now all in `main`)

The Compare work previously tracked here as "uncommitted" is committed (see git
log, newest first: `5ea9bd5` sortable/copyable Differences view, `e613c4b`
side-by-side Differences, `74871da` canonical normalized address + ZIP+4 handling,
`cb21055` address-comparison fixes, `8741f69` minor/major grading). In brief:

- **Compare by Voter ID** — two open lists, matched on normalized Voter ID;
  modeless Compare Summary window driving the per-window mark layer.
- **Minor/major grading** — Name and Address graded minor vs major via
  case-insensitive Levenshtein (`ee_levenshtein_ci`, thresholds
  `EE_CMP_MINOR_MAX_EDITS`=4 / `EE_CMP_MINOR_MAX_PCT`=25%); binary Precinct-changed
  gated on unchanged address (re-precincting). `EE_CMP_*` is a bit set per row.
- **Canonical normalized address** — `compose_address` emits one consistent
  `street[, unit], City, STATE ZIP[-ZIP4]` style, preferring structured street
  columns. Compare canonicalization (`ee_canon_for_compare` /
  `ee_canon_address_for_compare`) ignores formatting, the trailing state token, and
  ZIP+4 precision so those don't read as false changes.
- **Show Differences…** — modeless side-by-side window (`k_DiffClassName`) listing
  each changed voter's differing fields (Voter ID | Field | file A | file B), from
  `EeVoterTable_CollectDifferences`; sortable headers, multi-select + Ctrl+C / copy.

Tunable knobs live in the two `EE_CMP_MINOR_MAX_*` constants in `voter_table.c`.
Known caveat: a single house-number change ("123→125 Main St") scores as minor.

<details>
<summary>Older committed history (Reports, duplicate detection) — retained below</summary>

**Two-file Compare by Voter ID (committed `7be360c`; extended above).** Compare two voter
lists open in separate viewer windows. Design decisions (confirmed with the user):
match on **normalized Voter ID** only; a matched voter is **Changed** when its
Precinct, Name, or Address differs (case-insensitive), else **Identical**; blank-ID
rows are **Only here**. Presentation is a modeless **Compare Summary** window
(counts per category × the two files) whose rows drive the existing per-window
**mark layer** to highlight the matching rows in each grid.

- **Core (GUI-free):** `EeVoterTable_CompareByVoterId` in `voter_table.{c,h}` +
  new `EeCompareResult` / `EE_CMP_*`. O(nA+nB) two-map hash join reusing
  `next_pow2_ge_u32`/`hash_cs_utf8`; progress/cancel via the existing
  `dup_scan_pump` pattern. Unit test `test_compare` (tag `cmp`) in
  `test/smoke_load.c` — passes.
- **GUI (`main.c`):** dynamic **Compare** menu (index `k_CompareMenuPos`=4, rebuilt
  in `WM_INITMENUPOPUP` via `App_BuildCompareMenu`, one "Compare with <file>" item
  per other viewer, IDs `IDM_COMPARE_WITH_FIRST..LAST`). `App_StartCompare(a,b)`
  runs sync (<250k rows) or on the reused scan thread behind the progress modal
  (`CompareThreadProc`/`App_OnCompareFinished`, posts `EEM_CMP_FINISHED`; both
  windows disabled during the run). `CompareWndProc` (class `k_CompareClassName`,
  global singleton `g_compare`) shows the summary; double-click a row (B-count
  column → B, else A) or right-click → "Show these rows in <file>" applies marks.
- **Mark plumbing:** factored `App_ApplyDuplicateMarks` into
  `App_ApplyMarks(app,marks,count,kind,sort_col,label)`; added `AppState.mark_label`
  so the status bar describes any mark view (dup or compare). New mark kinds
  `EE_SCAN_CMP_ONLY/CHANGED/IDENTICAL` sort compare views by Voter ID.
- **Teardown:** `App_CloseCompare(app)` closes the summary if it references a
  viewer that reloads or closes (called next to `App_CloseReports`); compare class
  buffers freed in the main `WM_DESTROY`.

Verified: all four configs (x64/ARM64 × Debug/Release) build **0 warnings**;
Code Analysis (`RunCodeAnalysis`) clean; all smoke tests pass. GUI click-tested
(counts correct; highlighting; large county lists).

Known v1 limitation: during a large (async) compare both windows are disabled and
re-enabled via `IsWindow` guards; a comparison uses the first row per duplicate ID
as the representative for the changed-field check.

**Reports feature — DONE, committed.** New "Reports" menu after "Filter"
with "Display Precinct Report…" and "Display Address Report…". Each opens a modeless,
**unowned** top-level window (so the main list can cover it; reselect brings it to
front; one of each kind per viewer). Two-column owner-data list view
(Precinct/Address + "Number of Voters"), one row per distinct value with its voter
count, initially sorted ascending by the value column (header-click re-sorts either
column). No data → info modal ("No precinct/address information available"), window
not opened. Right-click: Copy (all selected rows, on either column); on the value
column also Include/Exclude (adds an `is` rule for that column to the **parent**
window's filter) and, for Address, Show in Maps. Ctrl+C copies. Reports close when
the parent reloads a file or closes. Decisions: Address = normalized `EE_COL_ADDRESS`;
data = **all loaded rows** (ignores filters/duplicates view).
- **Blank rows:** empty precinct/address cells are tallied and shown as a "(blank)"
  row (underlying value stays "" so Include/Exclude add an "is (blank)" rule that
  matches incomplete records; Show-in-Maps hidden for it). Shown only when real
  values also exist — an all-blank/absent column still gives the "No … information
  available" modal. `EeVoterTable_CollectValueCounts` now returns the blank tally via
  a new `out_blank_count` param; `App_ShowReport` appends the row.
- New: `EeVoterTable_CollectValueCounts` / `EeVoterTable_FreeValueCounts`
  (`voter_table.{c,h}`, O(n) hash aggregation, reuses `hash_ci_fold`); test
  `test_value_counts`. `ReportWindow` + `ReportWndProc` + `App_ShowReport` /
  `App_CloseReports` in `main.c`; `k_ReportClassName` registered; `AppState` gained
  `report_precinct` / `report_address`. Files: `main.c`, `voter_table.{c,h}`,
  `resource.h`, `test/smoke_load.c`. Debug + Release build clean; smoke tests pass
  (incl. `valcount`). GUI itself not yet click-tested this session.

**Duplicates-view follow-ups — DONE, committed.**
- **Sort-on-show** (`b4823ba`): a duplicates view sorts ascending by its key so
  shared values are adjacent (Voter ID for the ID scan, Name for name+DOB), via
  `App_SortByTableColumnAscending` + factored `App_RefreshSortUi` in `main.c`.
- **Reset View** menu item (`c3fbfec`): bottom of Filter menu, enabled only while a
  duplicates view is active; clears marks + filter to show all records.

**Duplicate-detection speedup — DONE, committed `207da43` and pushed.** Made the
"duplicate voters (name + DOB)" and "duplicate Voter IDs" features fast and
GUI-responsive. Measured on real data: Dallas County 1.48M rows → name+DOB scan
**373 ms** (7,113 dup rows), Voter-ID **213 ms**; previously minutes / effectively
hung. Changed files: `voter_table.{c,h}`, `main.c`, `resource.h`,
`test/smoke_load.c`.

What changed:
- **Root cause was the display, not just the scan.** The old code turned results
  into one `EeRel_Is` filter rule per duplicate Voter ID, then evaluated every row
  against every rule — O(rows × dup_ids). Replaced with a per-window row-**mark**
  layer.
- New `EeVoterTable_MarkDuplicateVoterIds` / `...VotersByNameDob`: O(n)
  open-addressing hash grouping that marks physical rows directly, with progress +
  cancel callbacks. Old `Collect*` kept as thin wrappers (existing smoke tests
  still green); added `test_mark_duplicates`.
- `AppState` gained `mark_rows/mark_count/mark_active/mark_kind`, ANDed with the
  filter in `App_ApplyFilter`; Reset clears it. Marks are per physical row so they
  survive sorts.
- Large tables (≥ `k_ScanModalMinRows` = 250k) scan on a worker thread behind the
  existing progress modal (reused load machinery; `EEM_SCAN_PROGRESS/FINISHED`,
  `scan_*` fields), with a working Cancel. Smaller tables run synchronously.
- Design decisions confirmed with the user: duplicates AND with active filters;
  apply the same fast path to Voter-ID dupes too.

Verified: x64 Debug **and** Release build clean (0 warnings); smoke tests all pass
(`dupvuid`, `dupvoter`, new `markdup`, plus the rest). Plan file:
`~/.claude/plans/quiet-enchanting-lovelace.md`.

</details>

## Next steps

- **CVR is complete and committed** (Phases 1 & 2 — load/view/tabulate, write-in
  image + `No image found` variants, "vote for N" contests, multi-card detection —
  click-tested and validated exactly against nine official elections across Travis &
  Dallas counties). Possible follow-ups: blank vs undervote vs overvote rate
  summaries, ballot-style breakdowns, per-precinct cross-tabs; freeze the leading key
  columns in the CVR grid (a frozen/scroll split like the voter window); per-file
  byte progress instead of the marquee.
- **Store:** submit the built
  `Build/msix/ElectionExplorer.Package_1.0.1.0_x64_arm64_bundle.msixupload` in
  Partner Center; enable **GitHub Pages** so the privacy URL resolves
  (`https://wheelgrouptech.github.io/ElectionInfo/Applications/ElectionExplorer/PRIVACY`).
- Optional polish: eyeball the 16px app icon in Explorer/title bar (swap to a
  simplified small-size glyph if busy); `BackgroundColor` is `#FFFFFF` — switch to
  a brand color if desired; consider persisting **window size/position**.
- Tune `EE_CMP_MINOR_MAX_EDITS` / `EE_CMP_MINOR_MAX_PCT` in `voter_table.c`
  against real files if the minor/major split needs adjusting.
- Possible follow-ups: selectable match key (Name+DOB), a reaper-thread for
  responsive deletion of large row sets.

## Notes for the next session

- The user does all git commit/push/pull by hand (see
  [CLAUDE.md](CLAUDE.md)). Don't run git write commands.
- Build/test per [AGENTS.md](AGENTS.md): MSBuild via `Applications.sln`,
  e.g. `msbuild Applications.sln /p:Configuration=Debug /p:Platform=x64`.
