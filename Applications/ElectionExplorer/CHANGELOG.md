# Election Explorer change log

The engineering record of Election Explorer, one entry per change, newest first, grouped by the
Store version each change shipped in. Each entry says what changed, where in the code, and how it
was checked. The other records:

- [`RELEASES.md`](RELEASES.md): the Store "What's new" text for each version.
- `docs/*-design.md`: the full design, decisions and validation of each feature
  ([CVRs](docs/cvr-design.md), [XLSX import](docs/xlsx-import-design.md),
  [voter rosters](docs/voter-roster-design.md), [election results](docs/election-results-design.md),
  [audit logs](docs/audit-log-design.md)), plus the county surveys beside them.
- [`../SESSION-HANDOFF.md`](../SESSION-HANDOFF.md): the current working state only.
- git: the exact code history (`git show <hash>`).

## Adding an entry

Add the entry as the last step of a change, at the top of "Unreleased" until the version is
submitted; then rename "Unreleased" to the version and start a new one. Use this template, and
group small related fixes (for example several county roster fixes on one day) into one entry with
a bullet each.

```
### YYYY-MM-DD — Short title of the change
- **What:** one to three sentences on the behavior change, in plain words.
- **Where:** key files and functions (`src/ee_audit.c` `au_machine_deleted`, `main.c` Ballot Accounting report…).
- **Checked:** smoke test names, regression sets, validation numbers.
- **Commit:** short hash(es); omit the line when not yet committed.
- **Details:** link to the design doc section, when there is one.
```

## Unreleased (after 1.6.1.0)

### 2026-11-23 — Linux: Remove in the Filter window
- **What:** in an Ubuntu VM the user found that Remove in the GTK Filter window did nothing. Each rule row was a check button whose label was the rule, so clicking the text toggled the rule and never selected the row; with no row selected, Remove did nothing. A row is now a check box beside a separate label: clicking the text selects the rule (and fills the fields, as before), clicking the box turns the rule on or off and selects it too, and after Remove the next rule stays selected. The Mac's rule list (a check box column beside a text column) and Windows were not affected.
- **Where:** `src/ui/gtk/app_main.c` (`fd_refresh`, which now takes the row to select; `fd_on_toggled`; `fd_on_remove`).
- **Checked:** Linux Debug build with no warnings; `--selftest` on both samples; a `--filter-window` snapshot shows the new rows. The click itself is for the user to try in the VM (WSLg cannot be clicked from here).
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "The voter list window, prototype".

### 2026-11-23 — Linux: the voter list window tried in an Ubuntu VM; WSLg menus
- **What:** the user ran the GTK window in an Ubuntu 24.04 desktop VM (VMware) with no issues, menus included. Under WSLg its menus open behind the window from the second time on, so WSLg is not a place to try the menus and dialogs. No code change.
- **Where:** `docs/cross-platform-design.md` ("Checked by hand"), `test/README.md`.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "The voter list window, prototype".

### 2026-11-23 — macOS and Linux: Copy, Export and duplicates in the voter list window
- **What:**
  - **Copy** (Ctrl+C / ⌘C, and the right-click menu): the selected rows, as Windows copies them. A right-click outside the selection selects that row first.
  - **Export Voter List…** (File) and **Export Selected…** (right-click): "Include normalized data fields", then a UTF-8 CSV or TSV with a byte order mark and a heading row, under the Windows names (`-All_Voters`, `-Filtered_Voters`, `-Selected_Voters`).
  - **Show Duplicate Voter IDs… / Show Duplicate Voters…** (Filter menu) and **Reset View…**, with the Windows status line ("Showing 4 duplicate Voter IDs of 9 voters"). The scan runs on the worker thread as part of the view, and sorts and filters keep it.
  - **Snapshot options:** `--duplicates ids|voters` and `--export <file>` on both platforms.
- **Where:**
  - `src/ui/ee_ui_voters.{h,c}`: `EE_UI_MARKS_*`, `ComputeView`'s `marks`, `EeUiView_Marks`, `EeUiVoters_Marks` / `HasBirthDate` / `MarksLabel` / `MarksNoneText` / `CountText`, `CopyText`, `Export`, `ExportName`; the self-test also copies and finds duplicates.
  - `src/ui/gtk/app_main.c`: `show_duplicates`, `reset_view`, `copy_selection`, `export_rows` (Export Options window), `show_sort_arrow`; the `copy`, `export`, `export-selected`, `dup-ids`, `dup-voters` and `reset-view` actions.
  - `src/ui/macos/app_main.m`: `showDuplicates:`, `copy:`, `exportRows:` (the save sheet's accessory view), `resetView:`, an Edit menu.
  - `test/sample_duplicates.csv` (made up); CI runs the self-test on it and checks both counts.
- **Checked:**
  - Debug builds, no warnings, on WSL Ubuntu 24.04 (GTK 4.14.5) and the Intel Mac; Linux Release too.
  - The self-test and snapshots give the same results on both: 4 duplicate Voter IDs and 4 duplicate voters of 9 (the twins are not), the duplicate voters within Include Precinct "101" (2 rows), and their export (BOM, heading, 2 rows).
  - Dallas `CountyWide_20261101.txt`: duplicate Voter IDs (20 rows) in 4.6 s on Linux and 3.8 s on the Mac, duplicate voters in 3.3 s and 3.2 s (Debug). Exporting all 1,499,178 rows: 10.5 s and 10.1 s (Debug), 3.9 s on Linux Release, on the UI thread as on Windows.
  - Not checked yet: the clicks (the menus, Copy's paste, the Export dialogs, the Filter window's buttons).
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "The voter list window, prototype".

### 2026-11-23 — macOS and Linux: filters and background sorting in the voter list window
- **What:**
  - **Filters, as on Windows:** Filter… (Ctrl+L on Linux, ⌘L on the Mac) opens a Filter window with the Windows layout and rules. Reset Filter clears the rules. Right-clicking a cell offers Include "value" and Exclude "value". The status line says "Filtered: 3 of 5 voters" when rows are hidden.
  - **Sorting and filtering in the background:** both run on a worker thread while the table keeps showing the old order, then the result is swapped in. Requests made meanwhile are merged. A descending sort of 1.5 million rows no longer sorts twice (5.3 s became 1.7 s on Linux, filter included).
  - **Snapshot options:** `--include` / `--exclude <column>=<value>` and `--filter-window` on both platforms, and `--sort` now on the Mac too. The GTK snapshot waits until the window has drawn the new rows.
  - **Sierra County CVRs:** the Mac reads G20 and G24 with the same records and ballots as Windows, but G22 did not finish within 4 hours. Linux reads G22 with the same 1,582 records and ballots.
- **Where:**
  - `src/ui/ee_ui_voters.{h,c}`: `EeUiRule`, `EeUiView`, `EeUiVoters_ComputeView` / `SetView`, `RuleIsValid`, `RelationName`, `TotalRows`, `IsFiltered`; the self-test also filters.
  - `src/ui/gtk/app_main.c`: `request_view` / `start_view_job` / `view_done`, the Filter window (`show_filter_dialog`, `fd_*`), `on_cell_right_click`, `snapshot_actions`. It now needs GTK 4.12 (`CMakeLists.txt`).
  - `src/ui/macos/app_main.m`: `EeFilterSheet`, `startViewJob` / `requestView`, `menuNeedsUpdate:`, the Filter menu, `takeSnapshotWhenReady`.
  - `docs/cross-platform-design.md`: the Sierra results under Vision and Tesseract, and filters in "The voter list window, prototype".
- **Checked:**
  - Debug builds, no warnings, on WSL Ubuntu 24.04 (GTK 4.14.5) and the Intel Mac.
  - `--selftest` prints the same on both. Include Precinct "101" shows 3 of 5; adding Exclude "101" shows 0; disabling it, 3 again.
  - Snapshots on both: Include "101" and Exclude PCTSPT "B", sorted by Voter ID descending, shows 2 of 5 with the arrow and the status line right. The Filter window lists both rules.
  - Dallas `CountyWide_20261101.txt`, with a filter that tests every row, then a sort: Linux 0.9 s and 1.7 s; the Mac 0.3 s and 2.6 s.
  - Not checked yet: the clicks (right-click menu; Add, Remove and OK in the Filter window).
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "The voter list window, prototype".

### 2026-11-22 — Linux: the GTK voter list window builds and works
- **What:**
  - The GTK 4 window, written with the AppKit one, now builds and runs on Linux (WSL Ubuntu 24.04, GTK 4.14.5).
  - Measured columns get more room, so addresses and names are no longer cut short.
  - `--snapshot` takes `--sort <column>[:desc]`, which sorts the way a heading click does, through the column view's sorter, so the click path can be checked without a mouse. Sorting prints how long it took.
- **Where:** `src/ui/gtk/app_main.c` (`main` argument loop, `load_done`, `on_sorter_changed`, `rebuild_columns`); `docs/cross-platform-design.md`, "The voter list window, prototype".
- **Checked:**
  - Debug build, no warnings. `--selftest test/sample_voters.csv` prints the same as on the Mac.
  - Snapshot of the sample with `--sort 1:desc`: the arrow is on Precinct, the rows are by precinct descending, and the status line says so.
  - Dallas `CountyWide_20261101.txt` (1,499,178 rows, 216 columns): loaded in 31.5 s; the heading sort took 5.3 s; 2.6 GB peak memory.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "The voter list window, prototype".

### 2026-11-22 — macOS and Linux: the voter list window, prototype (AppKit, GTK 4)
- **What:**
  - **The UI decision:** a native UI on each platform, AppKit (Objective-C) on macOS and GTK 4 (C) on Linux, chosen by the user over wxWidgets (C++). Decision X7 of the cross-platform design now records it.
  - **The prototype:** one window that opens a voter list (File > Open, the command line, or the Finder on macOS) and loads it in the background with a progress bar. It shows the list in a table that asks only for the cells on screen. Clicking a heading sorts by that column (again to reverse); numbers and dates are right-aligned; a status line gives the counts and the sort.
  - **Checking without looking:** `--selftest <file>` loads and sorts the list without a window and prints what the window would show. `--snapshot <png> <file>` writes the window's content as a PNG and quits, which works over SSH.
  - **CI:** the Linux job installs GTK 4 and builds the window; both jobs run its `--selftest`.
- **Where:**
  - New `src/ui/ee_ui_voters.{h,c}`: a plain C bridge over `EeVoterTable` (`EeUiVoters_Load`, `Cell`, `IsNumeric`, `Sort`, `SortTo`, `SelfTest`), so the window code never includes the stand-in `windows.h`, whose `BOOL` clashes with Objective-C's.
  - New `src/ui/macos/app_main.m` (`EeVoterWindow`, `EeAppDelegate`) and `src/ui/gtk/app_main.c` (`EeRows`, a `GListModel` that makes row objects on demand; `on_sorter_changed` sorts with the engine).
  - `CMakeLists.txt`: target `ee_app`, built as `Election Explorer.app` on macOS and as `election-explorer` on Linux when pkg-config finds GTK 4.10 or later.
  - `.github/workflows/election-explorer.yml`; `docs/cross-platform-design.md` (X7, Layout, "The voter list window, prototype", next step 3).
- **Checked:**
  - **macOS** (Intel Mac mini, Debug, universal, no warnings): `--selftest test/sample_voters.csv` gives 5 rows and 24 columns, sorted by Precinct ascending, descending and ascending again. The snapshot showed the expected window. Dallas `CountyWide_20261101.txt` loads 1,499,178 rows and 216 columns in 35 s, with a 1.87 GB peak.
  - **Linux:** written but not yet built: GTK 4 is not installed in WSL yet.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "The voter list window, prototype".

### 2026-11-22 — CI on Linux and macOS; .gitattributes
- **What:**
  - **CI:** a GitHub Actions workflow builds the engine and runs the smoke suite on Ubuntu 24.04, with Tesseract, and on an Apple Silicon Mac, with Apple Vision. It runs on each push or pull request that changes `Applications/ElectionExplorer/`. The Mac job tests the arm64 half of the universal build, which the Intel Mac mini cannot run.
  - **No Windows job:** the project needs the VS 18 (v145) toolset, which GitHub's hosted runners do not have yet. Windows stays a local build.
  - **`.gitattributes`:** line endings no longer depend on each machine's `core.autocrlf`. Text files are stored with LF and checked out with the platform's endings. Shell scripts are always LF, and the Visual Studio, MSIX packaging, `.rc` and `.ps1` files always CRLF. PDFs, images, Office files and executables are binary. No stored file changes: every text file in the index was already LF.
- **Where:**
  - New `.github/workflows/election-explorer.yml` and `.gitattributes` at the repo root.
  - `docs/cross-platform-design.md`: next steps 1, 4 and 5.
  - `test/README.md`: a CI note.
- **Checked:**
  - The workflow parses.
  - Its build and `ctest` steps pass locally with the same presets: `linux-x64-release` in WSL Ubuntu 24.04 with Tesseract, and `mac-universal-release` on the Intel Mac.
  - The workflow itself runs once pushed.
  - **Scanned results on the Mac, now complete:** all 189 files read with Apple Vision. 125 load on both Vision and Windows (Windows loads 139); 1,124 totals are equal and 388 differ, mostly in Cameron's ERM scans.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "Known gaps and next steps".

### 2026-11-22 — Linux: the OCR smoke tests run; Tesseract reads halftone headings and small pages
- **What:**
  - **OCR smoke tests on Linux:** `hartocr`, `resglyph` and `ressos` now run and pass there. Their test pages and glyph outlines come from a TrueType font file, read and filled with no library (Liberation Sans, else DejaVu Sans). The smoke suite now runs every test on Windows, macOS and Linux.
  - **Halftone heading bands:** Tesseract took a heading band printed in halftone dots for a picture and did not read it. Such a band is now read again on its own, blurred and thresholded so the dots turn white, but only when the page's reading found no words in it. Guadalupe's 2026 Electionware reports (L26, LR26, PR26) now load on Linux.
  - **Small pages:** pages under 1,800 pixels on their long side are read at 2 or 3 times their size, and their words are scaled back. Hart's scanned CVR Report is read at 1,224 x 1,584, where Tesseract missed the heading, so Sierra County's scanned CVRs were refused.
- **Where:**
  - New `test/tdraw_ttf.c`; `test/tdraw.h` gains `TDraw_Available` (also in `test/tdraw_apple.c`).
  - `test/smoke_load.c`:
    - the OCR tests and their page helpers on every platform;
    - a skip when there is no OCR engine or no font;
    - the skip-only stubs are gone.
  - `CMakeLists.txt`: `tdraw_ttf.c` for `smoke_load` off Apple.
  - `src/platform/linux/ocr_tesseract.c`: `emit_words`, `picture_bands`, `read_band`, and the enlargement of small pages.
  - `test/README.md`, `docs/cross-platform-design.md`.
- **Checked:**
  - **Smoke suites:** Windows, Mac and Linux pass every test, the three OCR tests included.
  - **Tesseract against Windows OCR on the sample of 48 scanned files:** 47 load (was 44). 1,465 totals are equal and 50 differ (was 1,459 and 48). The one refused is Cameron's GR20 runoff summary.
    - A first version also read bands the page already read. That doubled Midland's headings and moved 45 of its counts; skipping those restored them.
  - **Sierra County's scanned CVRs on Linux:** G20 and G24 give Windows' ballot records and ballots (1,964 / 1,961 and 1,780 / 1,776). Their columns differ (31 against 35, 46 against 29), as contests OCR'd differently make columns of their own. G22 (2,109 pages) did not finish within the run's one-hour limit.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "Tesseract OCR on Linux".

### 2026-11-22 — Linux: scanned documents read through Tesseract; glyph reports and OCR tests without GDI
- **What:**
  - **Linux OCR:** scanned documents are now read on Linux through Tesseract and Leptonica, when they are installed. Pages are prepared as for Windows OCR: decoded, made gray, scaled, capped at 10,000 pixels and turned.
  - **Where Tesseract differs from Windows OCR:**
    - It reads sideways text in place, so sideways words are left out and a page scanned sideways is turned.
    - It gives printed rules as words of spaces, which are left out.
  - **One bridge for both engines:** Apple Vision and Tesseract sit behind one plain C engine interface (`ocr_engine.h`), on which `ocr_bridge.c` implements `ocr_win.h`. The first version's `ocr_apple.c` and `ocr_vision.h` are gone.
  - **Glyph reports without GDI:** reports printed through PScript5 (Travis L22, LR22, PR22) now load on macOS and Linux. `go_draw` fills their glyph outlines with a scanline rasterizer there; Windows keeps GDI.
  - **OCR smoke tests on the Mac:** `hartocr`, `resglyph` and `ressos` draw their test pages and Arial glyph outlines with CoreGraphics and CoreText, and run on macOS. On Linux they are still skipped.
- **Where:**
  - New `src/platform/posix/ocr_engine.h` and `ocr_bridge.c`, and new `src/platform/linux/ocr_tesseract.c`.
  - `src/platform/macos/ocr_vision.m` implements `ocr_engine.h`; `ocr_apple.c` and `ocr_vision.h` are removed.
  - `src/ee_glyph_ocr.c`: `go_edge`, `go_fill` and the non-Windows `go_draw`.
  - `CMakeLists.txt`:
    - the bridge on both platforms;
    - Tesseract through `pkg-config` (option `EE_WITH_TESSERACT`), else `ocr_none.c`;
    - `test/tdraw_apple.c` and CoreText for `smoke_load` on Apple.
  - `test/smoke_load.c`:
    - Apple versions of `TPage`;
    - the Type 3 font's glyphs through `T3Glyphs` (GDI on Windows, CoreText on Apple);
    - `hartocr` checks for `EeOcr_EngineName()`;
    - the Linux skip message.
  - New `test/tdraw.h` and `test/tdraw_apple.c`; `test/README.md`.
- **Checked:**
  - **Builds:** the Windows x64 Release app with no warnings, Mac universal Debug and Release, and Linux with Tesseract 5.3.4; no new warnings.
  - **Smoke suites:** Windows passes with all OCR tests, and so does the Mac. Linux passes with the three OCR tests skipped.
  - **Travis glyph reports:** all three give Windows' totals on the Mac and on Linux (199, 6 and 18).
  - **Tesseract against Windows OCR, `data_check` with every choice's total:**
    - **Sample** (41 scanned results files and the 7 Secretary of State forms): 44 of 48 load, 1,459 totals equal and 48 differ.
    - **Refused:** three Guadalupe 2026 reports, whose halftone heading band Tesseract takes for a picture, and one Cameron summary.
    - **The rest of the scanned results files** (189): 133 of Windows' 139 load.
  - **Apple Vision on the same 189 files** (182 read so far): 124 load on both.
    - On those 182 files Vision matches Windows on 1,123 totals and differs on 341; Tesseract matches on 780 and differs on 378.
    - The differences are mostly in Cameron's ERM scans, where Windows OCR itself leaves 7.5% of rows not adding up.
  - **Speed:** with seven processes at once on 8 cores, Tesseract's OpenMP threads slowed files from 30 seconds to over 50 minutes. Batch runs set `OMP_THREAD_LIMIT=1`; `test/README.md` says so.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md): "Glyph outlines without GDI" and "Tesseract OCR on Linux".

### 2026-11-22 — macOS: scanned documents read by OCR through Apple Vision
- **What:**
  - **OCR on the Mac:** scanned results reports, Secretary of State canvass forms and scanned Hart CVRs are now read on macOS through Apple's Vision framework, behind the same OCR interface as Windows OCR (`ocr_win.h`).
  - **Prepared as for Windows OCR:** each page is decoded, scaled, capped at 10,000 pixels, made gray and turned, as on Windows.
  - **Where Vision differs from Windows OCR:**
    - It skipped counts printed on an underline, so rules are whitened first.
    - Its word boxes are padded, so they are trimmed to their ink.
    - Its newest text model (revision 3) dropped letters on some scans, so revision 2 is used while macOS offers it.
    - It reads sideways text in place, so those lines are left out and a page scanned sideways is turned until it reads well, as on Windows.
  - **Engine name:** load notes and messages now name the engine in use ("Windows OCR", "Apple Vision") through new `EeOcr_EngineName()`. The Windows text is unchanged.
  - **Linux:** no OCR yet; scanned documents are still refused there.
- **Where:**
  - New `src/platform/macos/`:
    - `ocr_apple.c` implements `ocr_win.h`;
    - `ocr_vision.m` is the Vision code (Objective-C, ARC);
    - `ocr_vision.h` is the plain C bridge between them.
  - `CMakeLists.txt`: Objective-C and the Vision, CoreGraphics, ImageIO and Foundation frameworks on Apple; `ocr_none.c` elsewhere.
  - `src/ocr_win.{h,c}` and `src/platform/posix/ocr_none.c`: `EeOcr_EngineName`.
  - `src/ee_results_ocr.c` and `src/hart_cvr.c`: five messages name the engine.
  - `test/smoke_load.c`: on macOS and Linux, `hartocr`, `resglyph` and `ressos` say they are skipped because their test pages are drawn with GDI. `test/README.md` says so too.
- **Checked:**
  - **Builds:** Windows x64 Release app with no warnings. Mac Debug and Release (universal) with no new warnings. Linux with no new warnings.
  - **Smoke suites:** Windows passes, including all three OCR tests. Mac and Linux pass with those three skipped.
  - **Windows OCR against Apple Vision, `data_check` with every choice's total:**
    - **The 8 Secretary of State forms:** every count equal.
    - **41 other scanned results files:** 38 load on both. The 3 that do not are Travis's PScript5 reports, whose glyph drawing (GDI) is not ported.
    - **Totals:** 1,182 equal under the same names and 27 differ. Under names the two engines OCR'd differently, 234 more are equal and 35 differ. The differences are in poor scans (Bowie 2026 unofficial, Cameron's ERM precinct reports), where both engines misread.
    - **Not yet run on the Mac:** the other 139 Cameron files and the three Sierra County scanned CVRs, about 12,000 pages.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "Apple Vision OCR on macOS".

### 2026-11-22 — macOS: the engine and its tests build and pass on a Mac
- **What:**
  - **Build:** the engine and the console tests now also build and pass on macOS, on an Intel Mac mini running macOS 15 (Apple clang 17, Xcode). The `mac-universal-*` CMake presets build universal binaries (Intel and Apple Silicon) with no source changes.
  - **Real county files:** `data_check` gives the same output on the Mac as on Windows for every county data file that does not need OCR.
  - **Voter-list checksum:** `data_check voters` now also prints `cells_fnv`, a checksum of every cell (FNV-1a 64 over each cell's UTF-8). Two builds' readings of a voter list can be compared without printing any of it.
- **Where:**
  - `test/data_check.c`: `do_voters` prints `cells_fnv`; the usage comment says so.
  - `docs/cross-platform-design.md`: status, new "macOS (2026-11-22)" section, next steps (the macOS build is done; the stale `data_check` `%lu` item is gone).
- **Checked:**
  - **Smoke suite:** `smoke_load` passes in Debug and Release on the Mac, and `ctest` passes. The Debug output is identical line for line to Linux's. `hartocr`, `resglyph` and `ressos` are skipped (no OCR engine yet). Only the arm64 half is untested: an Intel Mac cannot run it.
  - **Copied data:** SHA-256 of all 16,743 data files on both machines. After 45 text files whose copy had turned CRLF into LF were copied again with `scp`, every file was identical.
  - **`data_check`, Windows against Mac, on 16,741 files** (all but `_Other-2022 Primary`):
    - identical on all 137 audit logs, 12,074 rosters and 61 voter lists (cell checksums included), on 171 of 174 CVRs and on 4,058 of 4,295 results files;
    - every other file needs OCR: 233 the Mac refuses for lack of it (Windows loads 183), and 7 Secretary of State forms with a Paper Capture text layer, which fall back to their earlier refusal.
  - **Windows:** `data_check` builds with no warnings.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md), "macOS (2026-11-22)".

### 2026-11-22 — Texas Secretary of State canvass reports (scanned) load
- **What:**
  - **The form:** the canvass report a county files with the Secretary of State, printed, signed and scanned. It gives county totals only: the voters voted, provisional votes, then ballot measures (FOR / AGAINST) and contests of candidate, party and votes. It is now read by OCR.
  - **Eight files load that were refused:**
    - Brazos G25, the only whole-county G25 result Brazos posted: 17 measures and 34 counts.
    - Angelina G25 (two copies), Angelina G24 (23 pages, 23 contests), and Henderson G20, G21, L22 and S21 (August 31).
  - **Format:** shown as "Texas SOS canvass report (PDF)".
  - **Scans with an OCR text layer:** five of the files are scans that Acrobat's Paper Capture gave a text layer with letters doubled and numbers on the wrong rows. When such a file's text is no report Election Explorer knows, its page images are now read by OCR for this form. Any other such scan is refused as before.
  - **Load Summary:** rows OCR could not read (a lone faint "0" or "1"), which show as 0. Also contests with more votes than voters voted.
  - **What the form says is kept:** Henderson's G21 form gives the county's registered voters (56,413) as its "Total # of Voters Voted".
- **Where:**
  - `src/ee_results_ocr.c`:
    - new `ro_sos_*` functions read the form: the heading, the fixed columns, rows across pages, and a measure's wording skipped;
    - `ro_sos_deskew` takes out a scan's slope, and the grouping is split out of `ro_build_rows` into `ro_group_rows`;
    - a page with rows missing their count is read again at 1.6x;
    - `EeRocr_Parse` checks the first three pages for the form;
    - new `EeRocr_ParseSosCanvass` (`src/ee_results_ocr.h`) reads only this form.
  - `src/ee_results_pdf.c`: new `rp_ocr_layer`; `EeRpdf_ParseEx` turns to `EeRocr_ParseSosCanvass` for a Paper Capture scan it cannot read.
  - New `EE_RPDF_SOS_CANVASS` (`src/ee_results_pdf.h`) and `EE_RES_FMT_SOS_CANVASS` (`src/ee_results.{h,c}`).
  - `src/pdf_reader.c`: `EePdf_GetPageImage` takes the largest image inside a form XObject (Brazos's scan is drawn through one).
  - `test/smoke_load.c`: new `test_results_sos` (tag `ressos`) and `tpdf_write_paper_capture`; `test/README.md`.
- **Checked:**
  - **Smoke suite:**
    - Windows x64: passes, including `ressos` (a GDI-drawn form loaded as a plain scan and as a Paper Capture file).
    - Linux: passes with no warnings; `ressos` is skipped there (no OCR yet).
  - **Against the scans:** every count of Brazos G25, Angelina G25 and Henderson G21 and S21.
  - **Against the counties' own reports:**
    - Henderson L22: its 4 counts equal the official summary's.
    - Henderson G21: the official report's text agrees where it can be read.
  - **Counts not read:** Angelina G24 misses 3 lone write-in zeros and Henderson G20 misses 6 write-in counts; the Load Summary says so.
  - **Old and new `data_check` builds:**
    - **The 30 Paper Capture results PDFs and Angelina's G24 scan:** only the 8 forms change.
    - **The 27 files whose page image now reads differently** (Collin, Ellis, Travis PR20; text pages): all load as before.
- **Details:** [docs/election-results-design.md](docs/election-results-design.md), "Texas Secretary of State canvass reports".

### 2026-11-22 — Brazos vote-center precinct reports load as one election
- **What:**
  - **Titles:** Brazos's "accessible" Hart precinct reports draw a wrapped contest title as pieces on one line: the first piece at the margin, and every other piece from the point where the first one ends. The words split at places that differ from file to file ("COMMISSIONER OF THE GENERAL LAND O FFICE - Republican Pa rty"), so each vote center's file named the same contest differently. Those pieces now join into the whole title. Its party suffix then reads too, so the title is "REP COMMISSIONER OF THE GENERAL LAND OFFICE".
  - **Vote centers added up:** each of these files holds only the ballots cast at one vote center, yet lists every contest. Loading several used to cross-check them against each other, which gave spurious "votes here" issues and repeated contests. Now Hart precinct reports of one election whose headings name different places ("Arena Hall ED Precinct Report"), with the same county-wide registered voters and mostly the same contests, are added together:
    - choice totals, precinct counts, over/under votes and ballots are summed;
    - a precinct in several files keeps its registered voters;
    - two reports naming the same place are snapshots and stay separate elections.
  - **Brazos P26, 28 files:** 1 election of 98 contests, 209 choices and no issues (was 1,448 contests, 2,648 choices, 2,229 issues). Ballots are 12,570, the files' sum.
  - **Brazos G25, 20 files:** 26 contests and no issues (was 404 contests, 194 issues).
  - **Coverage:** these files are not the whole county; the P26 cumulative report has 32,143 ballots. Loaded with them, it reports those differences.
- **Where:**
  - `src/ee_results_pdf.c`:
    - `rp_join` joins a piece drawn from the same point as a piece that already went on;
    - new `ht_place` (called from `ht_heading`) reads the place into new `EeRpdfDoc.place` (`src/ee_results_pdf.h`).
  - `src/ee_results.c`:
    - `ResBuilder.place` and `Parsed.places` carry the place;
    - new `places_apart` decides, in `combine_split`;
    - `combine_into` takes an `add` flag and calls new `res_feed_sum`;
    - new `feed_counts` is shared with `res_feed`.
  - `test/smoke_load.c`: new `test_results_hart_places` (tag `reshartplace`); `test/README.md`.
- **Checked:**
  - **Smoke suite:** passes on Windows x64 Release, including `reshartplace`. It covers a title split mid-word two ways, two places' reports summed, and a same-place copy kept apart.
  - **Brazos totals:** for P26 and G25, every choice total of the combined load equals the sum of that choice over the files loaded alone. Each file alone still has 98 (P26) or 26 (G25) contests and 209 or 51 choices.
  - **`data_check` sweep, old vs new:** run on all 2,101 `Election_Results` PDFs.
    - **Each file alone:** 1,698 load, as before. Only the 27 Brazos P26 vote-center files change, and only in their titles.
    - **Each county's loadable files of one election, together (350 sets):** only Brazos G25 and P26 change. A Cameron P22 set that timed out on the old build in the sweep gives the same output on old and new when run alone. The final build matches on all 350 sets.
- **Details:** [docs/election-results-design.md](docs/election-results-design.md), "One election from several files", Brazos vote centers.

### 2026-11-21 — macOS / Linux: the engine and its tests build and pass on Linux
- **What:** Step 1 of porting to macOS and Linux.
  - The engine (everything but the Win32 UI) and the console tests `smoke_load` and `data_check` now build with CMake on Linux. The same presets cover macOS, which is not yet tried.
  - The engine sources keep their Win32 calls. A stand-in `windows.h` / `strsafe.h` / `intsafe.h`, used only by that build, supplies them.
  - Wide strings hold UTF-16 units on every platform, and printf follows MSVC's `%s` rules.
  - Settings go to a text file. OCR reports "not available" for now.
  - The Windows build and app are unchanged, apart from the fixes below.
- **Fixed on Windows too:**
  - Brazos's "accessible" Hart precinct reports had scrambled wrapped contest titles, which split one contest into several. `rp_item_cmp` and `rp_ptr_x_cmp` left pieces drawn at the same point in whatever order the sort chose; they now keep the drawn order.
  - In `ee_results_ocr.c` `ro_pre_key`, `key[o++] = …key[o - 1]…` read and incremented `o` in one expression, an order C leaves undefined. It is now two statements.
  - Audit-log error messages now take the file name after `/` as well as `\`.
- **Where:**
  - New:
    - `CMakeLists.txt` and `CMakePresets.json`.
    - `src/platform/posix/`: `include/windows.h`, `strsafe.h`, `intsafe.h`, `ee_port.c`, `settings_posix.c`, `ocr_none.c` and `wmain_posix.c`.
  - Changed:
    - `src/ee_results_pdf.c` (`rp_drawn_order`, `rp_item_cmp`, `rp_ptr_x_cmp`);
    - `src/ee_results_ocr.c`;
    - `src/ee_audit.c` and `src/ee_audit_hart.c` (name helpers);
    - `src/ee_glyph_ocr.c` (`go_draw` returns FALSE off Windows);
    - `test/smoke_load.c`: the UTF-16 fixture is written unit by unit; `hartocr` and `resglyph` are skipped off Windows;
    - `test/data_check.c` (`base_name`, `%lu` casts);
    - `test/README.md`.
- **Checked:**
  - **Linux:** WSL Ubuntu 24.04, GCC 13, Debug and Release.
    - `smoke_load` passes. Its output is identical to Windows' except `hartocr` / `resglyph`, which are skipped.
  - **Windows:**
    - x64 and ARM64 Release build clean.
    - `smoke_load` passes, including both OCR tests.
  - **`data_check`** was run on old Windows, new Windows and Linux for every file the 2026-11-17 backup plan loads (voter lists excluded):
    - **Results, CVRs, audit logs:** 4,022 files. Windows and Linux agree except 179 scans that need OCR.
    - **Rosters:** 7,917 files. Windows and Linux agree on all of them.
    - **Old vs new Windows:** they differ only on the 27 Brazos accessible reports.
- **Details:** [docs/cross-platform-design.md](docs/cross-platform-design.md). It covers the decisions X1–X7, the platform layer's rules, and the next steps: macOS build, OCR engines, wxWidgets UI, CI, `.gitattributes`.

### 2026-11-18 — Window menu and the Windows… dialog
- **What:** a **Window** menu between Compare and Help in every window that has a menu bar. It
  numbers the open Election Explorer windows, most recently used first (so the current window is
  1 and the one you came from is 2), and a number — or Alt+W then the digit — brings that window
  to the front. The numbers are positions, so closing a window renumbers the rest; past ten
  windows the menu points at **Windows…**. The menu also cascades, tiles horizontally or
  vertically, minimizes all, and closes all windows of one type or all but the current one
  (asking first before it closes more than one). **Windows…** opens a resizable, sortable list of
  every open window: Name, Path (the folder of its first file), Type (Voter List, Voter Roster,
  Cast Vote Record, Election Results, Audit Logs, Report, Comparison), that file's Size and
  Modified time, and the Records loaded. A Find box narrows it; Activate brings a window forward
  and closes the dialog; Close window(s) closes every selected window and stays open; closing the
  window it was opened from closes the dialog too; OK just closes it. Right-clicking a row offers
  the same actions plus Close All of that row's type, Close All But This, and Copy Path (the full
  path of the window's file, to the clipboard). The dialog's size, position and column widths are
  remembered between runs. Report and comparison windows are listed under their own types and
  report the file of the window they belong to. New **Help → Managing Windows** in each Help menu.
- **Where:** `main.c` new "Window list" section (`EeWin_Describe`, `EeWin_Collect`,
  `EeWin_ShortTitle`, `EeWin_BuildWindowPopup`, `EeWin_OnCommand`, `EeWin_Arrange`,
  `EeWin_CloseSet`, `WinListWndProc`, `EeWin_ShowDialog`, `k_HelpWindows`), the `&Window` popup in
  `App_CreateMenu` / `App_CreateRosterMenu` / `App_CreateCvrMenu` / `App_CreateResultsMenu` /
  `App_CreateAuditMenu` / `App_CreateResCompareMenu`, two lines in each of the five window procs'
  `WM_INITMENUPOPUP` / `WM_COMMAND`, `App_RouteDialogMessage`, the new class in `wWinMain`, and a
  `load_path` field on `CvrWindow` / `ResultsWindow` / `AuditWindow`; `resource.h` `IDM_WINDOW_*`,
  `IDM_HELP_WINDOWS`, `IDC_WINLIST_*`; `settings.{c,h}` `winlist_x/y/w/h` and `winlist_cols[6]`;
  `test/smoke_load.c` `test_settings_roundtrip`.
- **Checked:** Release x64 and ARM64 build clean at /W4 with no warnings; the full smoke suite
  passes, including the settings round-trip extended for the dialog placement and the six column
  widths. The live feature was driven from another process against the Release x64 build: the menu
  bar reads File | Edit | Filter | Reports | Compare | Window | Help; the rebuilt popup reads
  Windows…, the numbered current window (check-marked), Cascade / Tile / Minimize All, and the two
  close-many items (Close All But This greyed with one window open); with a voter list, voter
  roster, CVR, election results window and a precinct report open, the dialog listed all five with
  the right Type and with Path, Size and Modified matching `Get-Item` on each file, and Records
  matching each window's own status bar; Find narrowed 5 rows to 1; Close window(s) closed one
  window and kept the dialog open, asked "Close 2 Election Explorer windows?" for two, and closed
  the dialog along with the window it was opened from; Cascade, the tiles and Minimize All moved
  the windows and a numbered item restored and activated one.
- **Details:** [docs/window-management-design.md](docs/window-management-design.md).

## 1.6.1.0 (submitted 2026-11-15)

New in 1.6.1: ES&S batches deleted at the scanner, Verity Build logs, Verity logs printed again from a
browser, Results Compare title and choice pairing, "PERMANENT VUID", Collin / Fort Bend / Bexar
roster fixes.

### 2026-11-15 — 1.6.1.0: help, version, release notes, Store bundle
- **What:** version 1.6.1.0. Help → Loading in the audit window says a Verity report printed again
  from a web browser loads, and no longer names a county ("some counties post it with an XML
  signature"); Help → Reports lists the VerityBuild log's official ballots printed after Election
  Day among Hart's Notable Events. RELEASES.md 1.6.1.0 "What's new" note (1,051 of 1,500
  characters). STORE-LISTING.txt names Verity Build logs, browser reprints and batches deleted at
  the scanner.
- **Where:** `main.c` `k_AudHelpLoading`, `k_AudHelpReports`; `res/ElectionExplorer.rc`,
  `res/app.manifest`, `ElectionExplorer.Package/Package.appxmanifest`; `RELEASES.md`,
  `STORE-LISTING.txt`; `../SESSION-HANDOFF.md`.
- **Checked:** bundle `ElectionExplorer.Package_1.6.1.0_x64_arm64_bundle.msixupload` (6.4 MB) built
  with `/t:Rebuild` after clearing the package's `bin`, `obj` and `BundleArtifacts`; bundle and both
  packages `WheelGroupTech.ElectionExplorer` 1.6.1.0, both EXEs 1.6.1.0 with the new help text;
  19 logo images and only runFullTrust in each package. Release x64 and ARM64 build clean. The
  smoke suite does not build `main.c`; nothing it covers changed since its full pass on dd12f1b.

### 2026-11-14 — Audit logs: Hart Verity reports printed again from a browser
- **What:** a Verity audit log report printed again through a web browser now loads. Hays County's G26
  VerityCount log is a Firefox (Skia) reprint, and 1.6.0.0 rejected it as "not a Hart Verity audit
  log report". In that file each glyph is its own text run and no spaces are drawn, and "Component:"
  is joined to its value. The Hart reader now rebuilds the words: on a page where most runs are single
  characters, it joins glyphs along a line by their widths. A gap over 0.15 em is a space; 0.6 em or
  more starts a new run. It also accepts "Component: VerityCount 2.7.5" as one run.
- **Where:** `src/ee_audit_hart.c` `hv_collect` (glyph joining, `hv_one_char`), new `hv_component`,
  used by `hv_header`, `AuHart_IsReport` and `hv_page_rows`.
- **Checked:**
  - **Hays G26:** 3,018 rows with no row numbers missing, equal in every field to an independent
    PyMuPDF column reading. The official task replays to 120,880 ballots, the official ballots cast.
  - **Regression:** all 35 other audit log sets (Travis, Dallas, Hays, Parker, Burnet) give identical
    events, copies checks and notes against HEAD.
  - **Smoke test:** `hartaud` writes the Verity fixture a glyph at a time (helper `hva_count_pages`,
    `g_rpdf_glyphs`, `g_tpdf_widths`) and gets the same 12 events. The full suite passes.
- **Commit:** dd12f1b
- **Details:** the review `State_Election_Info/Texas_Election_Info/Hays_County_G26_Audit_Log_Review_2026-11-14.md`.

### 2026-11-13 — Voter lists: "PERMANENT VUID (SC91)" is the VUID
- **What:** a voter list whose Voter ID column is titled "PERMANENT VUID (SC91)" (Travis County's TEAM
  FPCA spreadsheet of federal voters not intending to return) now loads with that column as the VUID,
  so it compares with a roster. Before, the file loaded with no Voter ID.
- **Where:** `src/voter_table.c` `classify_field` ("PERMANENTVUID", "PERMANENTVUIDSC91").
- **Checked:** the county's `2026 TEAM Spreadsheets - FPCA.csv` loads 1,449 rows. Smoke test `fpca`
  (FPCA layout with a redacted DOB); full suite passes.
- **Commit:** c83193f

### 2026-11-13 — Results Compare: contest titles with words added at the end
- **What:** a contest left unpaired pairs with one whose title is its own plus words at the end,
  when their choices agree and the pairing is unique (Dallas G24's CVR "State Senator, District 23
  State" = results "State Senator, District 23"; "… District 13- Unexpired Term" = "… Unexpired").
  Exact titles pair first. Help → Compare says so.
- **Where:** `src/ee_results.c` `EeResults_Compare` (two pairing passes, `apair`),
  `cmp_title_extends`, `cmp_choices_agree`, `cmp_title_fits`; `src/ee_results.h` comment;
  `main.c` `k_ResHelpCompare`.
- **Checked:** CVR ↔ results sweep of the 50 cross-checked elections (before vs after): only
  Dallas changes. G24 pairs its two contests (now 188 totals match, none on one side only); P26
  pairs "Dem Member, State Board of Education, District 9" with the CVR's "DEM Member, State Board
  of Education, District 9 Dem District 09" and a Justice of the Peace contest likewise (equal
  counts). The other 114 results groups are identical. Smoke test `rescmpgroup` (title cases added); full suite passes.
- **Commit:** 7748a0d
- **Details:** [Step 8, "Contest titles with words added at the end"](docs/election-results-design.md#step-8--ess-and-hart-results-report-pdfs-done-2026-10-26)

### 2026-11-13 — Results Compare: same-named choices summed, "For (A Favor)" = "For"
- **What:**
  - Compare sums a contest's choices of the same name as one choice (totals, vote types,
    precincts). Dallas's Clarity results list each CFBISD at-large candidate twice; the CVR once.
  - The Clarity summary.csv reader keeps a repeated choice name in its contest when its percentage
    still fits under 100 (it had split that contest in five).
  - Choices left unpaired pair when their names differ only by a parenthesis or "NP" at the end
    ("For (A Favor)" / "Against (En Contra)" vs a CVR's "For" / "Against"), when the pairing is
    unique. Help → Compare says so.
- **Where:** `src/ee_results.c` `cmp_contests`, `cmp_precincts`, `cmp_choice_groups`,
  `cmp_group_sum`, `cmp_group_cell`, `cmp_choice_core`, `cmp_choice_loose`, `sum_sink`
  (summary.csv contest split); `src/ee_results.h` `EeResults_Compare` comment; `main.c`
  `k_ResHelpCompare`.
- **Checked:** Dallas L26 CVR ↔ each of its four results files: 330 contest totals match, 3
  differ (Wilmer Council, a real count difference), none on one side only (before: 144 match, 368
  one side only). Of 497 summary files only five Dallas ones load differently, now with their
  detail.xml's contest count (L24's stays split by its own figures). CVR ↔ results sweep of the 50
  cross-checked elections (old vs new build): only Dallas changes, L26 as above and G24's
  propositions (113 → 188 match, one-side-only 156 → 6, no new differences); the other 112
  results groups are identical. Smoke test `rescmpgroup`; full suite passes.
- **Commit:** 03552f2
- **Details:** [Step 8, "Same-named choices and translated choice names"](docs/election-results-design.md#step-8--ess-and-hart-results-report-pdfs-done-2026-10-26)

### 2026-11-12 — Audit logs: scanner-deleted batches, lines lost at a page break, Verity Build logs
- **What:**
  - ES&S "Committing Machine Deleted Batch" events now remove ballots. A batch deleted at the
    scanner (and logged when that scanner's next media is loaded) is taken out of the session's
    count. Travis G26 had counted 367 ballots too many.
  - Copies Check: an event printed with a line of text lost at a page break is matched to its
    whole printing in another copy, instead of being counted missing (Travis G26 11/10 Election
    log, pages 696–697).
  - Hart Verity Build logs ("Build 2.5.0" in Verity 2.5, Hays G24) load as their own log instead
    of being refused, with a new Notable Events category "Official ballots printed after Election
    Day" (likely ballot-board remakes).
- **Where:**
  - `src/ee_audit.{c,h}`: `EE_AUDIT_BATCH_DELETED`, `EeAuditBatch.deleted`,
    `EeAuditAccounting.deletions` (`EeAuditDeletion`: scanner serial, media, poll ID, the load it
    came in, the batch removed, a possible rescan); `au_pair_partial`,
    `EeAuditMerged.partial`, `EeAuditCopyCheck.incomplete`; log kind `EE_AUDIT_HART_BUILD` (6),
    `EE_AUDIT_NLOGS` 6.
  - `src/ee_audit_hart.c`: accepts "VerityBuild" and "Build <version>" components.
  - `main.c`: Ballot Batches status "Deleted at the scanner"; Ballot Accounting summary line,
    "Deleted Batches" tab (ES&S) and "Deleted at Scanner" column on Poll Places; Notable Events
    category "Batches deleted at the scanner" (Review); Copies Check "Printed Incomplete" column;
    View → Log → VerityBuild and a VerityBuild column in Users and Workstations.
- **Checked:**
  - Travis G26 replays to 553,862 with the 11/10 7:17 PM log and 554,229 (mail 19,373) with the
    11/11 log, each equal to the results at the time. Each deleted G26 batch is matched to its
    rescan.
  - Across all 14 ES&S elections, only Travis G26 (13 Machine Deleted commits, 5 with ballots) and
    Travis PR20 (1) have deletions; the other 12 replay as before. The 22 Dallas tabulator logs
    have none.
  - G26's merged Election log has 150,221 events and 0 missing in every copy. Hays G24 Build log:
    12,129 rows, numbering complete; 43 printings, 88 official ballots on 11/12/2024.
  - Smoke tests `auditacct`, `auditnote`, `auditpart`, `auditbuild`. Accounting, copies-check and
    notable-events regressions over every log set in `Audit_Logs/` (only G26 and PR20 change).
    Debug/Release x64 and ARM64 builds clean.
- **Commit:** 7592215
- **Details:** [Batches deleted at the scanner](docs/audit-log-design.md#batches-deleted-at-the-scanner-done-2026-11-12),
  [Rows printed with text lost at a page break](docs/audit-log-design.md#rows-printed-with-text-lost-at-a-page-break-done-2026-11-12),
  [Verity Build logs](docs/audit-log-design.md#verity-build-logs-done-2026-11-12)

### 2026-11-05 — Roster loader: Collin's combined list and Fort Bend's mail file names
- **What:**
  - Collin's post-election `voters-through-november-3-2026.xlsx` names no method in its file
    name: its `VotedPhase` column ("Early Voting" / "Election Day") now gives the method, and
    VoteType "Provisional" still wins. Before, 449,586 regular rows loaded with no method.
  - Fort Bend's 2024 mail lists (`EV-BBM-<m>-<d>.pdf`) loaded as early voting in person: a file
    name's "BBM" now wins over an earlier EV/ED.
- **Where:** `src/voter_roster.c` (`HeaderMap.phase`, `method_from_name`).
- **Checked:** Collin 453,115 rows = 372,750 early + 76,836 Election Day + 3,529 provisional, equal
  to an independent count; the 50 counties' current files otherwise unchanged. Smoke test
  `counties`.
- **Commit:** 1d89b74, 878e82c
- **Details:** [Early voting update](docs/voter-roster-county-survey.md#early-voting-update-2026-10-20)
  ("Collin's combined list"), [Past elections](docs/voter-roster-county-survey.md#past-elections-p24-g24-p26-2026-11-05)

### 2026-10-30 — Roster loader: Bexar's spaced file-name date
- **What:** Bexar's 10/29 in-person file names its date "2026 - 10 - 29"; it is now read, so its
  40,159 rows get a Date Voted. Committed after 1.6.0.0 was submitted.
- **Where:** `src/voter_roster.c` `find_date_numeric`.
- **Checked:** only this file name among the roster files and ZIPs has the form. Smoke test
  `county50`.
- **Commit:** f229193
- **Details:** [Bexar's and Travis's October 29-30 file names](docs/voter-roster-county-survey.md#bexars-and-traviss-october-29-30-file-names-2026-10-30)

## 1.6.0.0 (submitted 2026-10-30)

New in 1.6: audit logs (ES&S Electionware and Hart Verity), Dominion and scanned results, Hart HVS
CVRs, multi-card CVR ballot counts. Submission recorded in 8665a4b.

### 2026-10-30 — Compare pairs primary contests by party; Travis's second roster corrections
- **What:**
  - Results Compare pairs primary contests named with their party another way: Clarity combined
    primaries (party="REP" on the choices), "REP X" (Hart CVR) and "X - Republican Party"
    (Electionware). A slash counts as a word break in names ("Trump/JD Vance").
  - Voter rosters rank corrections by revision: `_UPDATED_2` replaces `_UPDATED`, and "(UPDATE)"
    is read as a correction. Travis's G26 10-30 ZIP no longer lists 911 mail voters twice.
- **Where:** `src/ee_results.c` `EeResults_Compare`, `cmp_party_title`, `EeResults_NamesMatch`;
  `src/voter_roster.c` `parse_source_name`.
- **Checked:** Tarrant P26 pairs all 677 contest totals (all equal), PR26 40, P24 458. Smoke tests
  `rescmpparty`, `rosterrev`.
- **Commit:** 2bf4d46
- **Details:** [Step 8, "Primary contests named with their party another way"](docs/election-results-design.md#step-8--ess-and-hart-results-report-pdfs-done-2026-10-26)

### 2026-10-30 — Hart Verity precinct reports by precinct; Angelina's two-page-wide roster PDF
- **What:**
  - Hart Verity precinct reports load by precinct and vote type. They had been read as cumulative
    reports, so every precinct's contests became contests of their own (Burnet G25: 345 contests
    instead of 19). 2020's two-line table heading is read too.
  - Hart number pieces drawn from one point or touching join into one number (McLennan 2023).
  - Angelina's 10/29 roster, printed two pages wide with no title row, loads: each row on page p is
    joined with the line at its height on page p + half.
  - Also adds the data backup and load-check tools under `test/` (`data_check.c`,
    `backup_data.py`).
- **Where:** `src/ee_results_pdf.c` (`ht_precinct_line`, `rp_contest`,
  `EE_RES_FMT_HART_PCT_REPORT`); `src/voter_roster.c` `read_pdf_halves`.
- **Checked:** found by the backup cross-check: 162 reports of nine counties; each gives the same
  choice totals as a cumulative or canvass report of the same election. Old vs new reader on all
  893 Hart results files (only intended changes) and 371 roster PDFs (only Angelina 10/29: 1,368
  records, all dated). Smoke tests `reshartpct`, `rosterhalves`.
- **Commit:** 17ef272
- **Details:** [Step 8, "Precinct report (1.6)"](docs/election-results-design.md#step-8--ess-and-hart-results-report-pdfs-done-2026-10-26),
  [Angelina's October 29 layout](docs/voter-roster-county-survey.md#angelinas-october-29-layout-2026-10-30)

### 2026-10-30 — Hart Verity audit logs (VerityCount, VerityCentral, VerityData)
- **What:** Hart Verity "Audit Log Report" PDFs (and a county ZIP of them) load into the Audit Log
  window, one election per county and date. Ballot accounting counts each task's vDrive reads;
  test tasks are told by name (a setting); Central "CVRs written to vDrive" are matched to Count
  reads. Notable events add Central → Count transfer differences, manual vote records, overrides,
  rejected vDrives and registered-voter updates. The Ballot Reconciliation shows the log's print
  time and newest event beside the results file's timestamp. User decisions H1–H5.
- **Where:**
  - New `src/ee_audit_hart.{c,h}` (reader: detection by title / "Component: Verity…", columns by
    x, rows by row number, repeated rows at a page top skipped).
  - `src/ee_audit.{c,h}`: log kinds `EE_AUDIT_HART_COUNT/_CENTRAL/_DATA`, `logs[EE_AUDIT_NLOGS]`,
    `au_load_zip`, `EeAudit_HartAccounting`, `EeAudit_HartNotable`, `EeAudit_SetTestTaskWords` /
    `EeAudit_IsTestTask`.
  - `settings.{c,h}`: `AuditTestTaskWords` (default "LAT, L&A, Test, Training, Logic"), set in the
    audit window's Edit → Options.
  - `main.c`: View → vDrive Reads; Ballot Accounting tabs Tasks / Polling Places / Vote Types /
    Central Transfers; Hart columns.
- **Checked:** all 19 files read with complete row numbering. Official task totals equal the
  results: Burnet P26 12,585, Parker G24 91,317, L25 8,746, L26 2,425, P26 33,642. Burnet P26's
  Central → Count 536 vs 524 is flagged; Parker P26's hour-behind "vDrive saved" rows show as a
  clock anomaly. ES&S regression identical for 13 elections. Smoke test `hartaud`; Release x64 and
  ARM64 clean.
- **Commit:** b501d63
- **Details:** [Hart Verity audit logs](docs/audit-log-design.md#hart-verity-audit-logs-16-done-2026-10-30)

### 2026-10-30 — Hart HVS "Votes by Precinct" CVR reports
- **What:** Hart Voting System (HVS 6.x, before Verity) CVRs published as a "Votes by Precinct"
  PDF, one ballot per page, load as a CVR (Burnet 2024). Three printings are read by one reader;
  contest titles cut short at a fixed width are completed by ballot order. Results Compare matches
  "9-MFISD" = "9 - MFISD" and names that differ only by spaces beside a slash. A Hart primary
  report with no party in its headings takes it from the candidates or the file name.
- **Where:** new `src/hart_hvs.{c,h}`; `src/hart_cvr.c` (detection, `HartOcrStore` passes, key
  columns Report Page / Precinct / Party / Voting Type); `src/ee_results.c` `hart_primary_party`,
  `EeResults_MatchPrecinctName`, `EeResults_NamesMatch`.
- **Checked:** G24 pairs every contest and choice in 23 of 23 precincts; cells are 1–7 votes below
  the canvass (ballots counted after the report was printed), never above. P24 and PR24 likewise.
  G24 early voting: 47,164 cut titles completed. Smoke tests `hvspdf`, `reshart1`.
- **Commit:** 2c0fbd3
- **Details:** [Hart HVS "Votes by Precinct" report](docs/cvr-design.md#hart-hvs-votes-by-precinct-report-hart_hvsc-added-2026-10-30)

### 2026-10-30 — Rosters: mail lists with a certificate number instead of the VUID (Webb)
- **What:** Webb's mail list gives the county's ballot certificate number, not the VUID. A
  "Certificate" title beside a received-date title, with no Voter ID title, now marks the header;
  voters load with a blank Voter ID and a Certificate column, and the load summary names the file.
- **Where:** `src/voter_roster.c` `note_cert_title`; `main.c` help text.
- **Checked:** 52 records = the 52 numbers in the PDF. Re-reading 34 counties changed only Webb.
  Smoke test `county1028`.
- **Commit:** 5ed8228
- **Details:** [Early voting update](docs/voter-roster-county-survey.md#early-voting-update-2026-10-20)

### 2026-10-29 — Scanned and ASCII85 results PDFs (OCR for ES&S scans)
- **What:** results PDFs that were refused now load: text PDFs behind ASCII85 / ASCIIHex filter
  chains (Cameron's ReportLab reports), files with scanned cover pages (Liberty L24), and scanned
  ES&S reports (older ERM and Electionware) read with Windows OCR. Rows are checked and repaired
  by arithmetic (row and column sums), and the Load Summary says what was filled, corrected, or
  still does not add up.
- **Where:** new `src/ee_results_ocr.{c,h}` (`ro_repair`); `src/pdf_reader.c` (filter chains,
  CCITT images wrapped as TIFF); `src/ocr_win.c` (`EeOcr_SetRotation`, turns the decoded gray
  copy); `src/ee_results_pdf.c` (`EeRpdf_ParseEx`, `rp_aligned_col`, `rp_number_list`);
  `src/ee_results.c` (progress and Cancel through `t_load`).
- **Checked:** Cameron LR24 scanned precinct report vs its scanned summary: registered, ballots
  and 6 of 8 candidate totals equal, two one vote short. 166 of the 211 scanned files load; rows
  that do not add up: Electionware 4.3%, ERM 7.5% (most in three faint Cameron primaries). Other
  3,782 files: 3,373 load (41 more), none lost. Smoke test `respdf` (ASCII85 + Flate).
- **Commit:** ae7eb01
- **Details:** [Scanned and ASCII85 results PDFs](docs/election-results-design.md#scanned-and-ascii85-results-pdfs-16-done-2026-10-29),
  `docs/scanned-results-survey.md`

### 2026-10-29 — Dominion election results (San Francisco, DeKalb)
- **What:** Dominion Election Summary Reports (XML, XLSX, PDF) and the Statement of Votes Cast PDF
  load into the Election Results window. Vote For is derived from the XML's contest ballots;
  "****" withheld counts are filled from the precinct Total into a "Withheld" vote type. A CVR's
  "(Rank 1)" column compares as the contest. The merge no longer repeats a note another file gave.
- **Where:** new `src/ee_results_dom.{c,h}` (`EeRdom_PdfKind`); `src/ee_results.c`
  (`res_from_rpdf`, `EeResults_FromTally`); formats `EE_RES_FMT_DOM_SUMMARY_XML/_XLSX/_PDF`,
  `EE_RES_FMT_DOM_SOVC`.
- **Checked:** San Francisco 12 elections, three formats each, agree, with exact export round
  trips. DeKalb 6 elections: precinct sums equal the summary. SF results ↔ Dominion CVR exact for
  8 of 10 (G25: 15 CVR redactions; G19: the 2019 CVR has no card numbers). Sweep: 3,332 of 3,993
  results files load, none lost. Smoke test `resdom`.
- **Commit:** 5f88d66
- **Details:** [Dominion results](docs/election-results-design.md#dominion-results-san-francisco-and-dekalb-16-done-2026-10-29)

### 2026-10-29 — Audit logs step 10: help, version 1.6.0.0, release notes
- **What:** audit window help topics; the voter list, CVR and results help point to audit logs and
  Check Ballot Counts. Version 1.6.0.0. RELEASES.md and STORE-LISTING.txt audit log sections. A
  pre-release Store bundle was built and checked; it was rebuilt after the later 1.6 changes.
- **Where:** `main.c` help text; `res/ElectionExplorer.rc`, `res/app.manifest`,
  `ElectionExplorer.Package/Package.appxmanifest`.
- **Checked:** smoke suite passes; Release x64 and ARM64 clean; bundle and both packages 1.6.0.0,
  19 logo images, runFullTrust only.
- **Commit:** 84b70fc
- **Details:** [Step 10](docs/audit-log-design.md#step-10--help-version-1600-release-notes-done-2026-10-29-16-not-final)

### 2026-10-29 — Audit logs step 9: export and the reloadable Events export
- **What:** File → Export… in the audit window (Events: Displayed view or Reloadable; Batches:
  the displayed view plus Session), Export Selected…, and Export Selected / All in every report
  window. The Reloadable layout (Copy and Event records) loads back through File → Load Audit
  Logs… without the PDFs.
- **Where:** `src/ee_audit.{c,h}` `EeAudit_FormatExportUtf8`, `EeAudit_LoadExport`,
  `EeAudit_IsExportPath`; `main.c` (`ResExportDlgProc` now takes its texts).
- **Checked:** round trip with 0 differences for all 12 Travis and Dallas elections (events, copy
  masks, pages, accounting, notable events). Dallas G24 reloads in 0.2 s instead of 6.2 s. Smoke
  test `auditexp`.
- **Commit:** c8394d2
- **Details:** [Step 9](docs/audit-log-design.md#step-9--export-and-the-events-reload-round-trip-done-2026-10-29)

### 2026-10-29 — Audit logs step 8: Ballot Reconciliation and multi-card CVR ballot counts
- **What:** Compare → Check Ballot Counts with … in the audit, CVR and results windows opens a
  Ballot Reconciliation report: EMS ballots against a CVR's ballots, or against the results'
  ballots cast plus per-vote-type lower bounds. A multi-card CVR now counts ballots as its first
  cards, so Results ↔ CVR compare gives ballots for it too, and the CVR status bar says
  "multi-card: N ballots on M sheets".
- **Where:** `src/ee_cvr.{c,h}` `EeCvr_CardCounts`; `main.c` Compare menus and the report window.
- **Checked:** Dallas G24 873,455 rows → 858,562 ballots + 14,893 continuation sheets. EMS = CVR =
  results for Dallas G24 and Travis G24 / P26 / PR26 / L26 / GR25. Smoke tests `cvrmc` and the
  Hart test.
- **Commit:** 1f0e8a4
- **Details:** [Step 8](docs/audit-log-design.md#step-8--ballot-reconciliation-done-2026-10-29)

### 2026-10-29 — Audit logs step 7: L&A timeline, Notable Events, Users and Workstations
- **What:** three report windows. Notable Events sorts events by severity, with a red flag (the
  user's rule) for a results report printed, saved or exported before Election Day while real
  ballots were loaded. The L&A Testing Timeline lays out tests and counting periods. Users and
  Workstations lists each user's events, modules and workstations.
- **Where:** `src/ee_audit.{c,h}` `EeAudit_Notable`, `EeAudit_Timeline`,
  `EeAudit_IsTestCopyName`; `main.c` report windows and the status bar red-flag lead.
- **Checked:** red flags found in Travis L26 (36), Dallas P24 (1), Dallas L24 (1) and Travis L25
  (5, probably a misnamed test deck); none elsewhere. The PR26 timeline equals the design-doc
  trace. Smoke test `auditnote`.
- **Commit:** 70e7f97
- **Details:** [Step 7](docs/audit-log-design.md#step-7--la-testing-timeline-notable-events-users-and-workstations-done-2026-10-29)

### 2026-10-29 — Audit logs step 6: the Audit Log filter
- **What:** Filter → Filter… (Ctrl+L) / Reset Filter in the audit window: Timestamp on or after /
  before (applied together, giving a range); Log, Event, Module, User Id, Vote Type is / is not;
  Description contains / does not contain. Filter files export and load.
- **Where:** `main.c`; the shared rule window's `CvrFilterState` gained hooks `relations`,
  `relation_text`, `validate`, `relation_mask` (CVR and results windows pass none).
- **Checked:** in the app (UI Automation) on Dallas G24: a "cleared" + one-day filter leaves 8 of
  103,827 events; a bad time is refused.
- **Commit:** 66714f6
- **Details:** [Step 6](docs/audit-log-design.md#step-6--filter-done-2026-10-29)

### 2026-10-29 — Audit logs step 5: Ballot Batches, Ballot Accounting, Copies Check
- **What:** View → Ballot Batches lists one session's commits with their status (counted, removed
  by a media clear, replaced). Reports → Ballot Accounting… (Sessions / Poll Places / Vote Types /
  Media Clears tabs, Select Session…) and Reports → Copies Check… (one row per printed copy).
- **Where:** `main.c`; `src/ee_audit.{c,h}` `EeAuditBatch.clear`.
- **Checked:** Dallas G24 all 10 files: 858,562 ballots in 3,934 counted batches, with the
  alternative readings 906,179 / 857,597. Travis PR26: 97,460 ballots in session 31 of 32. Smoke
  test `auditacct`.
- **Commit:** c22eacc
- **Details:** [Step 5](docs/audit-log-design.md#step-5--ballot-batches-ballot-accounting-and-copies-check-done-2026-10-29)

### 2026-10-29 — Audit logs step 4: the Audit Log window
- **What:** File → Load Audit Logs… in every window. One window per election shows the merged
  Events view (Copies column; events the final copy lacks in bold red), View → Log / Copy, sort,
  copy and zoom. County, election and date are in the title bar.
- **Where:** `main.c` section "Audit Log window (1.6)"; `resource.h`.
- **Checked:** Travis GR25 (6 files) 5,036 events, EMS ballots 7,212; Dallas G24 start-dated
  copy shows "lacks 38,920 events (38,920 outside its date range)".
- **Commit:** dac51b3
- **Details:** [Step 4](docs/audit-log-design.md#step-4--the-audit-log-window-done-2026-10-29)

### 2026-10-29 — Roster PDFs: Guadalupe's split titles, Gregg's q/Q font change, Bexar's misdated name
- **What:**
  - Guadalupe (from 10/27) draws its "VUID" / "DATE" titles in two pieces: header titles now join
    across that.
  - Gregg 10/27 page 32 selects a one-glyph font between q and Q: the PDF reader now restores the
    text font, size and leading on Q.
  - Bexar "10272028": a file-name year after the file's write time becomes the year written
    (noted).
- **Where:** `src/pdf_reader.c`, `src/voter_roster.c`.
- **Checked:** only these three of 320 roster PDFs load differently. Smoke test `pdfpieces`.
- **Commit:** fbb54f1

### 2026-10-28 — ES&S audit log engine (steps 1–3): read, merge, account
- **What:** reads ES&S Electionware Election and Admin audit event PDFs (pages turned 90°),
  merges every printed copy of a log into one (from the last copy printed), and replays the
  Election log to count ballots by session, poll place and vote type, honoring media clears.
- **Where:** new `src/ee_audit.{c,h}` (`EeAudit_LoadCopy`, `EeAudit_LoadFiles`,
  `EeAudit_GroupCopies`, `EeAudit_Accounting`).
- **Checked:** all 70 Electionware PDFs read (1,875,864 events, 39 s). Merge: 0 missing events in
  all 24 logs. Replay equals the official ballots cast for 9 Travis elections, Dallas G24
  (858,562) and L24 (83,030); Dallas P24 one short (left open). Smoke tests `audit`,
  `auditmerge`, `auditacct`. AddressSanitizer probe over 23 PDFs clean.
- **Commit:** 3f44644 (design plan and county survey: 7ea9d1b)
- **Details:** [Steps 1–3](docs/audit-log-design.md#steps-13--the-engine-done-2026-10-28),
  `docs/audit-log-county-survey.md`

### 2026-10-28 — Rosters: no-break spaces in names (Bastrop)
- **What:** Bastrop's Windows-1252 CSVs pad some names with runs of no-break spaces; a long run
  overflowed the name buffer and was cut mid-character. U+00A0 is now whitespace, a full buffer
  ends on a character boundary, and no stray character is glued onto the last word.
- **Where:** `src/voter_roster.c` `clean_text`.
- **Commit:** fca8719

## 1.5.0.0 (submitted 2026-10-27)

New in 1.5: the Election Results window (Clarity files, ES&S and Hart results PDFs), filter files,
legacy `.xls` workbooks, rosters from 35 Texas counties.

### 2026-10-27 — 1.5.0.0: help, version, release notes, Store bundle
- **What:** the results Loading topic lists what is not read (scanned PDFs, Statement of Votes
  Cast, custom table, password-protected); voter list and CVR Loading topics point to Load
  Election Results; the roster topic lists 35 counties (Guadalupe added). Version 1.5.0.0.
- **Where:** `main.c` help text; the three version files; `RELEASES.md`, `STORE-LISTING.txt`,
  top-level `README.md`.
- **Checked:** bundle `ElectionExplorer.Package_1.5.0.0_x64_arm64_bundle.msixupload` (5.3 MB)
  built with `/t:Rebuild`; bundle, packages and EXEs 1.5.0.0; 19 logos; runFullTrust only. Smoke
  passes; Release x64 and ARM64 clean.
- **Commit:** 5971ff7
- **Details:** [Step 10](docs/election-results-design.md#step-10--help-version-1500-release-notes-done-2026-10-27)

### 2026-10-27 — Results step 9: more ES&S and Hart PDF layouts, Travis's printed PDFs
- **What:**
  - 9a: Electionware reports without the page footer.
  - 9b: older Hart reports before Verity (McLennan, Montgomery, Harris 2020–22), and an
    election's per-entity files combined into one election.
  - 9c: Travis reports printed through PScript5 (L22 Cumulative, LR22 Canvass, PR22 Republican
    Cumulative): Type 3 glyph drawings are rendered and read once each with Windows OCR.
- **Where:** `src/ee_results_pdf.{c,h}` (Hart legacy reader `hl_*`); `src/ee_results.c`
  `combine_split`; new `src/ee_glyph_ocr.{c,h}`; `src/pdf_reader.c` reads Type 3 fonts (blank glyph
  = space, widths from the drawings, `Tc` in the graphics state, a large `Tc` is a word gap).
- **Checked:** results PDF sweep 978 → 1,087 (9a) → 1,337 (9b) → 1,340 (9c) of 1,941 load, none
  lost. L22 and PR22 equal their precinct reports; LR22's 52 rows add up; Travis G20 vs CVR
  unchanged (399). Smoke tests `resglyph`, `reshart0`.
- **Commit:** 52312ca
- **Details:** [9a](docs/election-results-design.md#9a--electionware-reports-without-the-footer-72-files),
  [9b](docs/election-results-design.md#9b--older-hart-reports-before-verity-about-250-files-21-elections),
  [9c](docs/election-results-design.md#9c--travis-county-reports-printed-to-pdf-3-files)

### 2026-10-27 — Rosters: Henderson, Tom Green and Guadalupe PDFs
- **What:**
  - Henderson: a "Timestamp" column is a check-in time (Date Voted when nearly every row holds a
    date); a title containing "county voter" no longer ranks as a County Voter ID header.
  - Guadalupe: its check-in reports' title was taken as the header; they now load, and each
    file's "TOTAL" line is a non-voter row.
  - Tom Green: a mail list with no column titles gets its columns named from their contents
    (`pdf_untitled_columns`); lines above a later page's header that hold a Voter ID are data.
  - Fort Bend `EV-10-19_1.xlsx`: 38 "Total" subtotal rows are no longer voters.
- **Where:** `src/voter_roster.c`.
- **Checked:** regression over every Travis / Tarrant / Harris / Dallas election, the other
  counties and the SOS CSVs: only these files change. Henderson 10/25: 441 rows; Tom Green 1,510
  mail ballots; Guadalupe Oct 19–23 match the SOS side comparison. Smoke test `county1026`.
- **Commit:** fe26791
- **Details:** [Loader support](docs/voter-roster-county-survey.md#loader-support-2026-10-21)
  ("October 23-25 PDF layouts")

### 2026-10-26 — Results step 8: ES&S Electionware and Hart Verity results PDFs
- **What:** File → Load Election Results… accepts results report PDFs: ES&S Electionware summary
  and precinct reports (read by structure, so both templates load) and Hart Verity cumulative and
  canvass reports. ZIP members that are not results are skipped with a note. A Hart CVR precinct
  "315BS14" compares as precinct 315.
- **Where:** new `src/ee_results_pdf.{c,h}` (`EE_RES_FMT_ESS_SUMMARY/_PRECINCT`,
  `EE_RES_FMT_HART_SUMMARY/_PRECINCT`); `src/ee_results.c` `parse_results_pdf`, `align_rows`,
  `adopt_figures`, `EeResults_MatchPrecinctName`; `Cvr_BuildResults` in `main.c`.
- **Checked:** Travis 13 PDF elections; G20 ↔ CVR exact except the county's own Barton Springs
  precinct gap. Hays 7 Hart elections; G25 and PR26 ↔ CVR exact at every level. Smoke test
  `respdf`; Release x64 and ARM64 clean.
- **Commit:** 3264a72
- **Details:** [Step 8](docs/election-results-design.md#step-8--ess-and-hart-results-report-pdfs-done-2026-10-26)

### 2026-10-26 — Results: View → Reset View
- **What:** Reset View undoes a column sort, putting rows back in load order; greyed while
  unsorted.
- **Where:** `main.c`, `resource.h`.
- **Commit:** bbb0d87

### 2026-10-26 — Results steps 2–7: window, filter, turnout, compare, export, all Clarity counties
- **What:**
  - Step 2: the Election Results window (View Totals / By Precinct, vote-type columns, party
    first, Load Summary, copy, zoom); File → Load Election Results… in every window.
  - Step 3: Filter menu (precinct and vote-type rules re-tabulate; contest, choice, party rules
    hide rows), through the CVR rule window generalized to a host description.
  - Step 4: Reports → Display Turnout Report….
  - Step 5: Compare results with another results window or a CVR tabulation, with colors,
    Differences Only and Export.
  - Step 6: File → Export Election Results… (Displayed view / All detail); the displayed view
    loads back.
  - Step 7: every Clarity county loads: withheld ("protected") counts and registered voters of 0
    read as unknown; TXT fixed columns and long header labels; Dallas cumulative voting.
- **Where:** `main.c` "Election Results window (1.5)" (`ResTurnoutWindow`, `CvrFilterState`,
  `Filter_ShowRuleWindow`); `src/ee_results.{c,h}` (`EeResults_FromTally`, `EeResults_Compare`,
  `EE_RES_FMT_EE_VIEW`); `src/ee_cvr.{c,h}` (`EeCvrTally.group`); `settings.{c,h}`
  (`results_show_vote_types`).
- **Checked:** Travis L26 / PR26 / P26 / G24 results ↔ CVR identical both ways. Export → load →
  compare identical for G24 and P26 in all three layouts. All 471 Clarity elections of the 50
  largest Texas counties load as one each with identical round trips; the 82 with notes carry
  only genuine file notes. Smoke tests `rescmp`, `resview`, `rescum`.
- **Commit:** 04036c9
- **Details:** [Steps 2–7](docs/election-results-design.md#step-2--the-election-results-window-done-2026-10-26)

### 2026-10-26 — Results step 1: engine and Clarity loaders
- **What:** a new results engine reads Clarity summary.csv, detail.txt, detail.xls(x) and
  detail.xml plus our own long-form export, groups files into elections, cross-checks them, and
  tabulates by precinct and vote type.
- **Where:** new `src/ee_results.{c,h}`; `src/xlsx.c` / `src/xls.c` `EeXlsx_ReadAllSheetsMem` /
  `EeXls_ReadAllSheetsMem` (the `.xls` sheet list is now a heap array).
- **Checked:** Travis G24 / P26 / PR26 / L26: all four files agree; export round trips identical.
  All 471 Clarity elections of the 50 counties load. Smoke test `results`.
- **Commit:** 8d84f7e
- **Details:** [Step 1](docs/election-results-design.md#step-1--engine--clarity-loaders-done-2026-10-26),
  [Decisions D1–D8](docs/election-results-design.md#decisions-2026-10-26),
  `docs/election-results-county-survey.md`

### 2026-10-25 — Filter files: Export Filter / Load Filter
- **What:** the voter list, roster and CVR filter windows gain Export Filter… and Load Filter….
  The file is UTF-8 CSV (or TSV) with columns Column, Column Number, Relation, Value, Action,
  Enabled. Load matches columns by title and lists rules it cannot use.
- **Where:** `src/filter.{c,h}` `EeFilter_SaveFile` / `EeFilter_LoadFile`; `main.c`
  `FilterFile_Export` / `FilterFile_Load`, `IDC_FLT_EXPORT` / `IDC_FLT_LOAD`.
- **Checked:** smoke test `filterfile`; Release x64 and ARM64 clean.
- **Commit:** 3e49e68

### 2026-10-25 — Roster PDF fixes: October 23 layouts, stray pieces, Hunt's dotted dates
- **What:**
  - Hunt: a site title starting "polling pl" is the polling place, so its "BBM" rows are mail
    ballots (61 had counted as in person). Hunt's October 24 dates "10.24.26" are read
    (`slash_yy_cell` takes "/", "." or "-").
  - Gregg: a clipped last letter of a polling place continues that word, not the precinct.
  - Wichita: a number wholly left of the table is a row number unless column 0 is the precinct
    (50 voters recovered).
  - Rows dropped by a stray piece in the Voter ID column: a run touching the run drawn before it
    continues that cell, short letters under "VUID" join the previous run, and the Voter ID claims
    its column first (Brazos +1, Gregg +16 voters).
- **Where:** `src/voter_roster.c` (`pdf_runs_abut`, `pdf_run_short_letters`, `pdf_prev_drawn`,
  `slash_yy_cell`).
- **Checked:** regression: only the named counties change. Smoke tests `county1023`, `pdfstray`.
- **Commit:** 2e3662f, 5632c28, ece88c6 (the Hunt date fix; the rest of that commit is a report
  and a survey)
- **Details:** [Loader support](docs/voter-roster-county-survey.md#loader-support-2026-10-21)
  ("October 23 layouts", "Rows lost to a stray piece")

### 2026-10-24 — Legacy Excel 97-2003 (.xls) workbooks; Jefferson County rosters
- **What:** `.xls` workbooks open as voter lists and rosters and in the sheet picker. Jefferson
  County's rosters load: a `<date>to<date>` file-name range gives the end date, and an all-`*`
  VUID on a named row is a blank Voter ID. BIFF5 and encrypted files get a clear error.
- **Where:** new `src/xls.{c,h}` (OLE2 compound file, BIFF8 records); `EeXlsx_ListSheetsMem` /
  `EeXlsx_ReadSheetMem` hand off by signature; `src/voter_roster.c` `range_end_date`;
  `voter_table.c` and `main.c` dialogs and help.
- **Checked:** Jefferson 10/19–10/23: 29,106 in-person + 1,754 mail rows, equal to an
  independent count. Smoke test `xls`; Release x64 and ARM64 clean.
- **Commit:** d4d0fda
- **Details:** [Loader support](docs/voter-roster-county-survey.md#loader-support-2026-10-21)
  ("Jefferson and .xls")

## 1.4.1.0 (submitted 2026-10-22)

New in 1.4.1: rosters from 15 more Texas counties (33 in all) and more PDF layouts. Submission
recorded in 4e7faf9.

### 2026-10-22 — Wrapped roster rows (Wichita); version 1.4.1.0
- **What:** a PDF line with no Voter ID lying closer below the row above than 0.85 of the page's
  row spacing continues that row, and a lone row number left of the table is dropped (Wichita's
  absentee list from 10/21). Version 1.4.1.0; help and Store text name 33 counties.
- **Where:** `src/voter_roster.c`; the three version files.
- **Checked:** Wichita 10/21: 898 voters (its distinct IDs) instead of 993 rows with 95 empty.
  Every other county byte-identical. Bundle rebuilt clean (package bin/obj/BundleArtifacts
  deleted first); EXEs 1.4.1.0. Smoke 61/61.
- **Commit:** 60ee5ab
- **Details:** [Loader support](docs/voter-roster-county-survey.md#loader-support-2026-10-21)
  ("Wrapped rows")

### 2026-10-21 — Rosters of Texas counties 21–50
- **What:** rosters of McLennan, Smith, Brazos, Grayson, Randall, Wichita, Gregg, Hunt, Tom Green,
  Bastrop, Potter, Bowie, Victoria, Henderson and Angelina load (Guadalupe's scanned mail lists
  are not readable). PDF lines drawn as one TJ with kerned column gaps split into columns, rotated
  pages read upright, stacked headers, new titles, check-in times holding the date, and 2-digit
  year and weekday file-name dates.
- **Where:** `src/pdf_reader.{c,h}` (`EePdfPageText.split_gaps`, `EePdfTextRun.rot`);
  `src/voter_roster.c`; `main.c` help.
- **Checked:** per-county counts against independent ID counts and the SOS 10/19 file (for
  example Bastrop 3,143 = 3,143). Existing counties identical except Harris / Dallas row order.
  Smoke test `county50`; Release x64 and ARM64 clean.
- **Commit:** 3a6418c (survey: a6336fa)
- **Details:** [Counties 21-50](docs/voter-roster-county-survey.md#counties-21-50-2026-10-21),
  [Loader support](docs/voter-roster-county-survey.md#loader-support-2026-10-21)

## 1.4.0.0 (submitted 2026-10-20)

New in 1.4: the Voter Roster window, Voting Totals, roster compare, rosters from 18 Texas counties
and the Texas Secretary of State. Submission recorded in 4cc7b76.

### 2026-10-20 — 1.4.0.0 Store bundle
- **What:** help, release notes and Store listing name 18 counties (adding Collin, Fort Bend and
  Bell). The bundle was rebuilt clean after an incremental build reused a stale 1.3.0.0
  `AppxManifest.xml`.
- **Where:** `main.c` help; `RELEASES.md`.
- **Checked:** bundle and both packages 1.4.0.0; EXEs 1.4.0.0 (x64 and ARM64); 19 logos;
  runFullTrust only. Smoke 60/60.
- **Commit:** 18ee535

### 2026-10-20 — Rosters: Collin, Webb and Bell early voting
- **What:** a PDF table run under no column title goes to the nearest column not yet filled on
  its line (Bell lost 208 of 7,111 rows). `CertificateNumber` is a Voter ID title and `VoteDate`
  a Date Voted (Collin, Webb). A mail list row whose received date is "-" is a ballot not yet
  returned: skipped and noted (Collin 10,257).
- **Where:** `src/voter_roster.c` `read_pdf_table`; `main.c`.
- **Checked:** Bell = `pdftotext -raw` (7,111 / 1,685 lines); Collin, Galveston, Dallas,
  Williamson, Bexar equal independent counts. Smoke tests `counties`, `pdfroster`.
- **Commit:** 80962f3
- **Details:** [Early voting update](docs/voter-roster-county-survey.md#early-voting-update-2026-10-20)

### 2026-10-17 — Texas Secretary of State rosters
- **What:** the SOS statewide and county early voting CSVs and Election Day ZIPs load. Date Voted
  is blank except Election Day in-person rows, which get the election day from the election name
  (special elections stay blank, user decision). Adds a County column and party from the file
  name. Identical ZIP entries (same size and CRC) load once, for every county.
- **Where:** `src/voter_roster.{c,h}`; `main.c` help.
- **Checked:** found 118 Voter IDs on both the P26 DEM and REP Election Day rosters. Smoke test
  `sosroster`.
- **Commit:** 0d3344e
- **Details:** [Texas Secretary of State rosters](docs/voter-roster-county-survey.md#texas-secretary-of-state-rosters-done-2026-10-17)

### 2026-10-14 — Rosters: Williamson "AV" and a shorter early voting window
- **What:** a method value "AV" is a mail ballot; blank method cells take a column's single code.
  The election-calendar fallback counts only the 17 days before an election as early voting (was
  21), so mail ballots received earlier are no longer labeled early votes.
- **Where:** `src/voter_roster.c`; `main.c`.
- **Checked:** smoke test `counties` extended.
- **Commit:** 140b983
- **Details:** [Williamson snapshots](docs/voter-roster-county-survey.md#williamson-snapshots-2026-10-14)

### 2026-10-14 — Travis County Limited Ballot poll list PDFs
- **What:** the SOS Limited Ballot poll list (form 5-38) is recognized by its title, loose or
  inside the roster ZIPs. Rows by line number, names split by merge-field runs, Voter ID and
  Precinct blank, method Limited. A file-name date outside early voting is left blank and noted.
  Resolves decision D2.
- **Where:** `src/voter_roster.{c,h}`; `src/pdf_reader.{c,h}` `EePdf_OpenMem`; `main.c`.
- **Checked:** G25 123 + P26 443 rows equal an independent parse name for name; every other load
  byte-identical. Smoke test `limitedpdf`.
- **Commit:** b868b41
- **Details:** [Limited Ballot PDFs](docs/voter-roster-county-survey.md#limited-ballot-pdfs-done-2026-10-14)

### 2026-10-12 — Rosters of 11 more Texas counties
- **What:** Hidalgo, Bexar (CSV), Brazoria, Montgomery, Lubbock, Galveston, El Paso, Denton,
  Cameron, Williamson and Webb load: method from the site / mail columns or the Texas election
  calendar; Montgomery's per-jurisdiction rows merged; Brazoria 2026 swapped columns and
  translation row; Galveston "LAST  FIRST" names per sheet.
- **Where:** `src/voter_roster.c`; `main.c` help.
- **Checked:** counts equal an independent parse for every county; Travis / Tarrant / Harris /
  Dallas byte-identical. Smoke test `counties`.
- **Commit:** d934a55
- **Details:** [Remaining counties support](docs/voter-roster-county-survey.md#remaining-counties-support-done-2026-10-12)

### 2026-10-12 — Rosters printed to PDF
- **What:** rosters printed from spreadsheets to PDF load as tables (Bexar G26, Webb, Lubbock P24).
  Scanned PDFs stay "not readable".
- **Where:** `src/pdf_reader.{c,h}` (font size and glyph-width advance per run);
  `src/voter_roster.{c,h}` `read_pdf_table`; `main.c`.
- **Checked:** Bexar G26 2,129 rows = `pdftotext -raw` row for row; earlier counties
  byte-identical. Smoke test `pdfroster`.
- **Commit:** 32e1a39
- **Details:** [PDF rosters](docs/voter-roster-county-survey.md#pdf-rosters-done-2026-10-12)

### 2026-10-12 — Dallas County rosters
- **What:** mail, early voting and Election Day workbooks (State Voter ID over County Voter ID,
  unformatted Excel date serials), the 2026 layout (State ID / Date / one-column names), and the
  precinct ZIPs (P = in person, M = mail, "LAST - FIRST", DD-MON-YY).
- **Where:** `src/voter_roster.{c,h}`; `main.c` help.
- **Checked:** counts equal an independent parse for every file; earlier counties unchanged.
  Smoke test `dallas`.
- **Commit:** 92d8d02
- **Details:** [Dallas support](docs/voter-roster-county-survey.md#dallas-support-done-2026-10-12)

### 2026-10-12 — Harris County rosters
- **What:** daily mail / early voting / Election Day CSVs (ActivityDate = voting day), post-canvass
  voter history with Harris's own vote-type codes (E = Election Day, P = early), and the P26
  official roster; reference files skipped. `xlsx.c` reads namespace-prefixed workbooks.
- **Where:** `src/voter_roster.{c,h}`; `src/xlsx.c`; `main.c` help.
- **Checked:** counts equal an independent parse for every ZIP; Travis and Tarrant unchanged.
  Smoke tests `harris`, `xlsxns`.
- **Commit:** 5f9d3ff
- **Details:** [Harris support](docs/voter-roster-county-survey.md#harris-support-done-2026-10-12)

### 2026-10-10 — Tarrant County rosters; roster scrolling and multi-ZIP fixes
- **What:**
  - Tarrant rosters load: TSV inside ZIPs, two layouts, redacted voters, the vote-type full
    roster, party from file names, a P24 header repair.
  - Fixed a use-after-free when loading several ZIPs (all counties).
  - Fixed: the roster window could not scroll. `App_SyncVerticalScroll` synced the visible pane
    to the hidden left pane; roster windows now skip the vertical sync.
- **Where:** `src/voter_roster.{c,h}`, `src/csv_sheet.{c,h}`, `main.c`.
- **Checked:** every Tarrant election 2020–2026: counts equal an independent parse, round trips
  identical. Smoke test `tarrant`.
- **Commit:** 6327f74
- **Details:** [Tarrant support](docs/voter-roster-county-survey.md#tarrant-support-done-2026-10-10)

### 2026-10-10 — Voter Roster window, Voting Totals, roster compare (Travis County)
- **What:**
  - Step 1: a roster engine reads Travis County's roster ZIPs (corrections replace originals,
    misnamed copies skipped, a missing header row recovered, duplicate Voter IDs kept and flagged).
  - Step 2: the Voter Roster window (the voter viewer in single-pane roster mode) with File →
    Load Voter Roster… in every window, a load dialog with time remaining, and a Load Summary.
  - Step 3: Reports → Display Voting Totals… (voters per date × method, each Voter ID once, split
    by party in primaries).
  - Step 4: Compare roster ↔ roster and roster ↔ voter list. Every compare now pairs rows one to
    one, and left-over rows of a repeated ID form a "Voter ID repeated" category.
  - Step 5: roster help topics, version 1.4.0.0, Store text.
  - Also: Compare Summary Export Summary / Export All Records, Differences Export Selected / All,
    and Filter → Show Duplicate Voters Voting… (same name and birth date, different Voter IDs,
    in a chosen roster).
- **Where:** new `src/voter_roster.{c,h}` (`EeRoster_LoadFiles`, `EeRoster_ComputeTotals`);
  `src/xlsx.{c,h}` `EeXlsx_ListSheetsMem` / `EeXlsx_ReadSheetMem`; `src/voter_table.{c,h}`
  (`EeVoterTable_Builder*`, `EeVoterTable_CompareByVoterIdEx`, `EeCompareOptions`,
  `EE_CMP_REPEATED`, `EeVoterTable_MarkDuplicateVotersVoting`); `main.c` (`AppState.is_roster`,
  `App_CreateRosterMenu`, `RosterTotalsWindow`, `App_PickItem`).
- **Checked:** all 12 Travis ZIPs match an independent parse row for row; ZIP → TSV → reload
  identical (G24 583,202 records). G24 and P24 export → reload → compare all identical. Smoke
  tests `roster`, `rtotals`, `rcmp`, `dupvote`. User click-tests passed.
- **Commit:** e45f231
- **Details:** [Voter roster design, Implementation status](docs/voter-roster-design.md#implementation-status),
  [Decisions D1–D4](docs/voter-roster-design.md#decisions-2026-10-10)

## 1.3.0.0 (submitted 2026-10-10)

New in 1.3: Hart PDF CVR Reports (including scanned ones via Windows OCR), Dominion CVRs with
ranked-choice rounds, new CVR reports, El Paso voter lists.

### 2026-10-10 — 1.3.0.0: version, Loading help topics, unplated logos, release notes file
- **What:** version 1.3.0.0. New Help topics Loading Voter Lists and Loading Cast Vote Records;
  two stale CVR help sentences fixed. The Store package gains unplated small logos (16/24/32/48).
  New `RELEASES.md` holds every version's Store "What's new" text.
- **Where:** `main.c` (`k_HelpLoading`, `k_CvrHelpLoading`, `IDM_HELP_LOADING`); the three version
  files; `ElectionExplorer.Package/Images/`, `tools/generate-assets.ps1`, `.wapproj`;
  `RELEASES.md`; `STORE-LISTING.txt`.
- **Checked:** bundle 1.3.0.0 (3.8 MB); both packages hold all 19 logo images.
- **Commit:** a3aad99, c8a3c32, 30b7822 (project filters), 9287b3c (RELEASES.md)

### 2026-10-09 — OCR load progress per page
- **What:** loading a scanned Hart PDF showed 0% for minutes. During the OCR pass progress and
  Cancel now run every page, and the dialog shows "Reading scanned page N of M (OCR)…".
- **Where:** `src/hart_cvr.c` (`hart_tick`, `HartProgress.ocr_total`); `src/voter_table.h`
  (`EeLoadProgress.scanning = 2`); `main.c` (`CvrLoadJob.ocr_pages_total`).
- **Checked:** Sierra: first update at 0.4 s, then every page; Cancel works.
- **Commit:** df95e09

### 2026-10-09 — Scanned (image-only) Hart CVR Report PDFs via Windows OCR
- **What:** a Hart CVR Report made of page images loads through the OCR built into Windows, with
  repair passes (repeated headers, GUID repair, re-OCR at 1.5× / 2×, spelling correction) and a new
  key column OCR Status (OK / Corrected / Review / No Votes Read).
- **Where:** new `src/ocr_win.{c,h}` (`Windows.Media.Ocr` + WIC from C), new `src/hart_ocr.{c,h}`;
  `src/pdf_reader.c` `EePdf_GetPageImage`; `src/hart_cvr.c`; `EeCvrTable.load_note`.
- **Checked:** Sierra County, CA (1,272 pages) against the official canvass: 1,780 records = 1,780
  ballots; every precinct × contest × choice exact except the county-redacted precinct. Smoke test
  `hartocr`. User click-tested.
- **Commit:** 025879e
- **Details:** [Scanned Hart CVR Reports](docs/cvr-design.md#scanned-image-only-hart-cvr-reports--ocr-ocr_winc--hart_ocrc)

### 2026-10-09 — Dominion (Liberty Vote) CVRs and ranked-choice tabulation
- **What:** Dominion CVR exports (ZIPs of JSON) load, schemas 5.2, 5.10 and 5.19, streamed entry
  by entry. RCV contests become "<contest> (Rank N)" columns, and Reports → Tabulate (Filtered)
  Ranked-Choice Contests… runs instant-runoff rounds with San Francisco's rules.
- **Where:** new `src/dominion_cvr.c` (`EeCvr_LoadFromDominionZips`, `EeCvr_IsDominionZip`); new
  `src/ee_rcv.c` (`EeCvr_FindRcvContests`, `EeCvr_TabulateRcv`); `main.c` `CvrRcvWindow`.
- **Checked:** San Francisco: every contest exact for 10 elections, G25 / P26 differ only by
  county-redacted records; all 25 official RCV reports match every round. G24 (1,603,908
  sessions) loads in about 32 s. Smoke tests `dominion`, `rcv`.
- **Commit:** 2e308cb (its message repeats b9d392a's subject; the files are the Dominion and RCV
  change)
- **Details:** [Dominion CVRs](docs/cvr-design.md#dominion-voting-system-cvrs-dominion_cvrc),
  [Ranked-choice tabulation](docs/cvr-design.md#ranked-choice-instant-runoff-tabulation-ee_rcvc)

### 2026-10-08 — Redacted Hart PDF CVR Reports; repeated-title vote-for-N rows
- **What:** Burnet's redacted PDFs, whose redaction deletes header text, load: detection needs
  only the title, `Cvr Id:` and the table header, and key fields blank on every record get no
  column. A vote-for-N contest printed as one row per seat with the title repeated merges.
- **Where:** `src/hart_cvr.c` (`hart_key_layout` `present` mask, `HartMeta.present`,
  `sheet_find_contest`).
- **Checked:** Burnet L25 2,834 rows, G25 8,374, P26 13,137; Tarrant PDF vs ZIP tallies identical.
  Smoke test `hartpdf` extended.
- **Commit:** b9d392a
- **Details:** [Hart PDF CVR Report](docs/cvr-design.md#hart-pdf-cvr-report-pdf_readerc--hart_cvrc)

### 2026-10-08 — CVR reports: Polling Place, Device Serial, Voting Type
- **What:** Reports → Display Polling Place / Device Serial / Voting Type Report…, greyed unless
  the column has data. The Batch report also accepts Hart's "Batch Number".
- **Where:** `main.c` (`IDM_CVR_REPORT_POLLINGPLACE/DEVICESERIAL/VOTINGTYPE`,
  `Cvr_FindReportColumn`).
- **Checked:** smoke test `hartpdf` extended.
- **Commit:** 847e7be
- **Details:** [Per-column value reports](docs/cvr-design.md#per-column-value-reports-batch--precinct--ballot-style--polling-place--device-serial--voting-type)

### 2026-10-07 — Hart PDF CVR Reports, alone or merged with Hart ZIPs
- **What:** File → Load Cast Vote Records accepts Hart PDF CVR Reports. PDF alone gives the key
  columns from the report; ZIP + PDF decorates the ZIP's votes with polling place, voting type and
  device fields by Cvr Id. Mixing Hart and ES&S files is refused.
- **Where:** new `src/pdf_reader.{c,h}` (read-only PDF text extractor; repairs Hart's negative
  `startxref` in multi-GB files); `src/hart_cvr.c` (`EeCvr_LoadFromHartFiles`,
  `EeCvr_IsHartCvrPdf`); `src/ee_cvr.c` `cvr_is_key_header`; `main.c` dispatch.
- **Checked:** PDF-only tabulation byte-identical to ZIP-only for every Tarrant pair, including G24
  (828,544 sheets); ZIP + PDF tallies = ZIP. Smoke test `hartpdf`.
- **Commit:** 918e53a
- **Details:** [Hart PDF CVR Report](docs/cvr-design.md#hart-pdf-cvr-report-pdf_readerc--hart_cvrc)

### 2026-10-03 — Voter lists: El Paso County layout and header classification
- **What:** district columns such as "STATE BOARD OF EDU 23" no longer read as the residence state;
  an exact-name column beats a loose match; new header names (`VoterID`, `Apartment_Number`,
  post-direction, `City_Name`, house-number suffix). After loading, a city or state column whose
  values are not cities or states is dropped from the address and the state inferred.
- **Where:** `src/voter_table.{c,h}` (`classify_field`, `header_is_district_like`,
  `compose_address`, `voter_finalize_addresses`, `voter_validate_address_columns`,
  `addr_number_suffix_col`).
- **Checked:** old vs new on every row: El Paso xlsx 537,182 rows all with Voter IDs and clean
  addresses; El Paso `.txt` 529,674 fixed; Dallas 9,194 addresses regain a post-direction; Travis
  SOS and 09-23 lists unchanged. Smoke test `elpaso`.
- **Commit:** e1f83b2

## 1.2.0.0 (submitted 2026-10-02)

New in 1.2: Hart CVR ZIPs, primaries split by party, filtered tabulation, steadier load progress,
voter-list address and wide-file fixes.

### 2026-10-02 — 1.2.0.0 and the Store listing
- **What:** version 1.2.0.0 and the Store upload bundle; Store listing text kept in
  `STORE-LISTING.txt`.
- **Where:** the three version files; `STORE-LISTING.txt`.
- **Checked:** bundle (3.3 MB) and both packages 1.2.0.0; both EXEs 1.2.0.0.
- **Commit:** b762d6c, bf7f144

### 2026-09-28 — Confidential (masked) addresses compare as equal
- **What:** an address made only of masks compares as one redacted value, so lists that mask
  confidential voters differently no longer show them as address changes. Fully redacted
  addresses no longer get an inferred state.
- **Where:** `src/voter_table.c` (`ee_canon_address_for_compare`, `ee_is_street_word`,
  `ee_tokens_are_redacted`, `ee_address_is_redacted`).
- **Checked:** SOS 2026-09-16 vs 2026-09-23: redacted address flags 392 → 0. Smoke tests
  `cmpstate`, `resstate` extended.
- **Commit:** dd34369

### 2026-09-27 — Voter lists: residence city / state / ZIP and state inference
- **What:** a unit number at the end of a street line is no longer taken for a ZIP when a ZIP
  column has a value. A list with no residence-state column gets its state inferred from the
  ZIPs (plurality through a ZIP3 → state table).
- **Where:** `src/voter_table.c` (`compose_address` `fallback_state`, `voter_apply_inferred_state`,
  `zip3_to_state`).
- **Checked:** Texas SOS list (933,778 rows, about 6 s): every record `street, CITY, TX ZIP`.
  Smoke test `resstate`.
- **Commit:** 0d701aa

### 2026-09-27 — Hart CVR loading: scan status, determinate progress, wide CVRs, natural order
- **What:** "Scanning ballots…" during Hart's first pass; a determinate progress bar for all CVR
  loads; CVR column widths clamped under the 32,767 px header limit; Hart contests sorted
  numerically within a rank (District 6 before 26).
- **Where:** `src/voter_table.h` (`EeLoadProgress.scanning`); `src/hart_cvr.c` (`natural_cmp_ci`);
  `src/ee_cvr.c` (`cvr_multi_prog`); `main.c`.
- **Checked:** Tarrant G24 US Rep districts in order; P26 precinct chairs numeric. Smoke test
  `hart` extended.
- **Commit:** da4f318

### 2026-09-27 — Hart CVRs: primaries by party, party-first tabulation, filtered tabulation
- **What:** a Hart primary's contests are prefixed with their party, so each party tallies apart.
  Edit → Options… "Party to display first for tabulation". Reports → Tabulate Filtered CVR
  Votes…. The load dialog shows a running record count. A reloaded Hart CSV export freezes its key
  columns.
- **Where:** `src/hart_cvr.c` (`party_abbr`, `contest_display_name`); `src/ee_cvr.{c,h}`
  (`EeCvr_ReorderTallyByParty`, `EeCvr_TabulateRows`, `cvr_is_key_header`); `settings.{c,h}`
  (`CvrTabPartyFirst`); `main.c`.
- **Checked:** Tarrant P26 (488,862 sheets): U.S. Senator equals the certified totals for both
  parties. G24 load → export → reload → tabulate byte-identical. Smoke test `hart` extended.
- **Commit:** 9409c0f

### 2026-09-27 — Hart CVR ZIPs
- **What:** File → Load Cast Vote Records reads a Hart CVR ZIP (one XML per ballot sheet),
  streamed so multi-GB exports load. Vote-for-N, write-ins, undervotes and overvotes are handled;
  contests are ordered Federal → State → County → City → ISD → Other → MUD.
- **Where:** new `src/hart_cvr.c` (`EeCvr_LoadFromHartZips`); `src/ee_cvr.{c,h}`
  (`EeCvr_BuildBegin` / `EeCvr_BuildAppendRow`); `main.c`.
- **Checked:** Tarrant G24 (2.2 GB, 828,544 sheets, about 1m40s): Railroad Commissioner exact for
  all 4 candidates. Smoke test `hart`.
- **Commit:** a98837c
- **Details:** [Hart voting-system CVRs](docs/cvr-design.md#hart-voting-system-cvrs-hart_cvrc)

### 2026-09-27 — CVR export keeps vote-for-N contests together
- **What:** export wrote derived "(2)" titles for continuation columns, so a reload split
  multi-seat contests. Continuation columns are now exported with a blank header, as in the source.
- **Where:** `src/ee_cvr.c` `EeCvr_FormatDelimitedUtf8`.
- **Checked:** Travis G24 (587,090 rows) load → export → reload → tabulate byte-identical. Smoke
  test `cvrrt`.
- **Commit:** 1e951d5

### 2026-09-23 — Code-review hardening
- **What:** the voter window's `WM_DESTROY` waits for and closes the load thread; a CVR window's
  menu is destroyed if window creation fails.
- **Where:** `main.c` (`App_CreateCvrWindow`), `voter_table.c` (comment on `classify_field`).
- **Commit:** 8764f81

### 2026-09-23 — Very wide voter lists sort to the last column
- **What:** lists with hundreds of columns passed the ListView's 32,767 px header limit, so
  far-right headers desynced and could not sort. Scroll-pane column widths are now clamped so
  every column fits.
- **Where:** `main.c` `App_RebuildColumns`.
- **Checked:** Travis "With_History" file (389 columns): all columns sort.
- **Commit:** a82e8b8

### 2026-09-23 — "State ID" recognized as the Voter ID
- **What:** `STATEID`, `STATEIDNUMBER`, `STATEVOTERID`, `STATEVOTERIDNUMBER` headers map to the
  Voter ID instead of the address state (Dallas rosters).
- **Where:** `src/voter_table.c` `classify_field`.
- **Checked:** smoke test `idvoter` extended.
- **Commit:** e45f8c1

## 1.1.0.0 (built 2026-09-20)

New in 1.1: `.xlsx` voter lists, ES&S cast vote records with tabulation, reports and filters,
CSV/TSV export.

### 2026-09-20 — The CVR window becomes a standalone top-level window
- **What:** each CVR window owns its own resources, so closing the voter list leaves it running;
  the app quits when both voter and CVR windows are gone. Help and About center on the CVR window.
- **Where:** `main.c` (`AppState.is_cvr_ui`, `App_FreeCvrUi`, `g_cvr_windows`, `App_MaybeQuit`,
  `App_RunModalDialogOwned`).
- **Checked:** GUI click-tested; smoke suite unaffected.
- **Commit:** e924211
- **Details:** [Standalone CVR window](docs/cvr-design.md#standalone-cvr-window)

### 2026-09-20 — 1.1.0.0 and Help menus
- **What:** version 1.1.0.0; the voter Help menu gains an Export topic, and the CVR window gains
  its own Help menu (Options, Filters, Reports, Export, About).
- **Where:** `main.c` (`k_HelpExport`, `k_CvrHelp*`); the three version files.
- **Checked:** EXE 1.1.0.0; Store bundle 1.1.0.0 built.
- **Commit:** 9a494ca

### 2026-09-20 — CSV and TSV export for voter lists and CVRs
- **What:** every voter and CVR view and report exports to UTF-8 CSV or TSV (with BOM) in display
  order: File → Export Voter List / Cast Vote Records, right-click Export Selected / All, with an
  option to include the normalized fields.
- **Where:** `main.c` (`App_PromptExportPath`, `App_WriteExportUtf8`, `App_ExportReportModel`);
  `src/voter_table.c` `EeVoterTable_FormatDelimitedUtf8`; `src/ee_cvr.c`
  `EeCvr_FormatDelimitedUtf8`.
- **Checked:** smoke tests `vexport`, `cvrexp`.
- **Commit:** b6bb3d9
- **Details:** [Delimited-text export](docs/cvr-design.md#delimited-text-export-csv--tsv)

### 2026-09-20 — CVR Batch, Precinct and Ballot Style reports
- **What:** Reports → Display Batch / Precinct / Ballot Style Report… (value and ballot-record
  count, sort, copy, Include / Exclude into the CVR filter), greyed when the column has no data.
- **Where:** `main.c` `CvrValueReportWindow`; `src/ee_cvr.{c,h}` `EeCvr_FindColumnByTitle`,
  `EeCvr_ColumnHasReportableData`, `EeCvr_CollectColumnCounts`.
- **Checked:** smoke test `cvrcnt`.
- **Commit:** 5f338f2
- **Details:** [Per-column value reports](docs/cvr-design.md#per-column-value-reports-batch--precinct--ballot-style--polling-place--device-serial--voting-type)

### 2026-09-20 — CVRs from CSV and TSV
- **What:** the CVR loader reads delimited text (UTF-8 with or without BOM, UTF-16, RFC-4180
  quoting) as well as `.xlsx`; files of one election may mix formats. A row with no contest cell
  is dropped.
- **Where:** new `src/csv_sheet.{c,h}` (`EeCsv_ReadSheet`); `src/ee_cvr.c` `EeCvr_LoadFromFiles`.
- **Checked:** Travis L26 and P26 byte-identical across formats; Travis G24 and Dallas G24 match
  every count except image write-ins (Excel drops images in CSV). Smoke test `cvrcsv`.
- **Commit:** 6a430a7

### 2026-09-19 — CVR window Filter menu
- **What:** Filter… / Reset Filter in the CVR window: any column, is / is not, Include / Exclude
  with the voter filter's rules. Numeric columns take a typed value. Filtering hides rows only.
- **Where:** `main.c` (`Cvr_ApplyFilter`, `CvrWindow.disp`); `src/ee_cvr.{c,h}`
  `EeCvr_CollectColumnValues`.
- **Checked:** smoke test `cvrfilt`.
- **Commit:** 33ea85a
- **Details:** [Filtering (CVR window)](docs/cvr-design.md#filtering-cvr-window)

### 2026-09-19 — Multi-card ballot detection
- **What:** the CVR status bar reads "N ballot records" and adds "(multi-card ballots detected)"
  when a minority of rows lack a top-of-ballot contest (President, Governor, U.S. Senator). ES&S's
  "No image found" placeholder is a write-in variant. Informational only; tallies unchanged.
- **Where:** `src/ee_cvr.{c,h}` `EeCvr_HasMultiCard`; `main.c`.
- **Checked:** flags Dallas P26 Dem and G24, not Dallas P24 (combined primary) or Travis. Smoke
  test `cvrmc`.
- **Commit:** fa062ce, 5fc18d0
- **Details:** [Multi-card ballots](docs/cvr-design.md#multi-card--multi-page-ballots)

### 2026-09-19 — CVR tabulation, write-in merge, whitespace normalization
- **What:** Reports → Tabulate CVR Votes… (Contest | Selection | Votes; candidates, then write-in,
  overvote, undervote; vote-for-N summed across its columns). Edit → Options… "Merge image and text
  write-ins" (default on). Selections have space runs collapsed and ends trimmed.
- **Where:** `src/ee_cvr.{c,h}` (`EeCvr_Tabulate`, `merge_writeins`, `normalize_ws`); `main.c`
  `CvrReportWindow`; `settings.{c,h}` `CvrMergeWriteins`.
- **Checked:** exact against official results for nine elections: Travis L26, P26, PR26, G24, G20;
  Dallas P26 Rep, P26 Dem, P24, G24. Smoke tests `cvrtab`, `cvrmerge`, `cvrws`.
- **Commit:** 5a2844f, 354bf02, 5004146
- **Details:** [Phase 2 — vote tabulation](docs/cvr-design.md#phase-2--vote-tabulation-implemented)

### 2026-09-19 — "Vote for N" contests across several columns
- **What:** a multi-seat contest's blank-header continuation columns get the title "<contest> (2)"
  … and are grouped with it; a repeated identical title is a separate race. Fixed `xlsx.c`
  skipping the cell after a self-closed empty cell.
- **Where:** `src/ee_cvr.{c,h}` (`cvr_build_titles`, `col_group`); `src/xlsx.c`.
- **Checked:** Travis L26 groups checked against the official results. Smoke test `cvrmulti`.
- **Commit:** 1bafd47
- **Details:** [Multi-column contests](docs/cvr-design.md#multi-column-contests-vote-for-n)

### 2026-09-18 — Load and view ES&S cast vote records
- **What:** File → Load Cast Vote Records… opens one or more `.xlsx` CVR files of one election
  (identical headers, concatenated) into a CVR window: sparse storage, sortable columns, copy.
  `xlsx.c` shows integer numbers without ".0" and reads an image-only contest cell as
  `[write-in]`.
- **Where:** new `src/ee_cvr.{c,h}`; `main.c` (`CvrWndProc`, `App_CreateCvrMenu`,
  `Cvr_CopySelected`); `src/xlsx.c` (drawing anchors).
- **Checked:** Dallas P24 (99,999 rows, about 5 s), Travis G24 (587,090 ballots; 3,680 President
  write-ins). Smoke tests `cvr`, `writein`.
- **Commit:** eb6b70a
- **Details:** [CVR design](docs/cvr-design.md), [Write-in images](docs/xlsx-import-design.md#61-write-in-images-cells-whose-content-is-a-picture)

### 2026-09-17 — Excel (.xlsx) voter lists with a sheet picker; date and leading-zero fidelity
- **What:** `.xlsx` workbooks open as voter lists, with a sheet picker for several sheets. Dates
  show as `YYYY-MM-DD` and zero-padded numbers keep their zeros (from `styles.xml`).
- **Where:** vendored miniz 3.0.2 (`src/third_party/miniz/`); new `src/xlsx.{c,h}`
  (`EeXlsx_ListSheets`, `EeXlsx_ReadSheet`); `src/voter_table.c` shared row sink (`ingest_header`,
  `ingest_row`, `EeVoterTable_LoadXlsxSheet`); `main.c` `SheetPickerDlgProc`.
- **Checked:** smoke tests `xlsx`, `xlsxfmt`; click-tested with a 3-sheet fixture.
- **Commit:** d23839f, cc712a9
- **Details:** [XLSX import design](docs/xlsx-import-design.md),
  [Decisions](docs/xlsx-import-design.md#decisions-resolved-2026-09-17)

### 2026-09-16 — District codes no longer appended to a full address; Store association
- **What:** a jurisdiction column named `CITY` (`C10`) and a `STATE BOARD OF EDUCATION` column were
  appended to complete addresses (Travis 2026-07-06). They are skipped when the address already
  ends with its own ZIP. The package is associated with the Store; `*.pfx` is ignored.
- **Where:** `src/voter_table.c` `compose_address`, `last_token_is_zip`;
  `ElectionExplorer.Package/Package.StoreAssociation.xml`.
- **Checked:** Travis 929,662-row file. Smoke test `distcode`.
- **Commit:** b0f0494, 0a718bd

## 1.0.1.0 (initial release, September 2026; first Store bundle built 2026-09-11)

The first Store release: large voter lists, normalized data, filters, duplicates, reports, list
compare.

### 2026-09-11 — MSIX packaging for the Microsoft Store; privacy, trademarks, icon
- **What:** a packaging project builds one x64 + ARM64 `.msixupload`. The About dialog links the
  privacy policy; `PRIVACY.md` and `TRADEMARKS.md` added; logo and multi-size app icon rebuilt;
  synthetic sample data and generators for Store screenshots.
- **Where:** `ElectionExplorer.Package/` (`.wapproj`, `Package.appxmanifest`, `Images/`,
  `tools/generate-assets.ps1`, `tools/generate-icon.ps1`); `res/app.ico`; `main.c` About
  (`k_PrivacyUrl`); `res/ElectionExplorer.rc` brand strings; `docs/sample-data/`.
- **Checked:** bundle `…_1.0.1.0_x64_arm64_bundle.msixupload` built with the Store identity.
- **Commit:** 8025aad, f2b5d75, 03b85af, 553d8fa, 41a8317, 88f7bf6, 91444ae

### 2026-09-10 — Options saved in the registry
- **What:** zoom, map engine, copy-with-normalized-data and surname-first persist per user under
  `HKCU\Software\WheelGroupTech\ElectionExplorer` (virtualized transparently under MSIX).
- **Where:** new `src/settings.{c,h}` (`EeSettings_Load` / `Save`); `main.c` `g_settings`.
- **Checked:** smoke test `settings` (throwaway test key); Debug and Release x64 clean.
- **Commit:** 4fc4af6 (formatting cleanup: 5c6e623)

### 2026-09-08 — Version 1.0.1.0; About "Open Source" line
- **What:** version 0.1.0.0 → 1.0.1.0; the About dialog labels the GitHub link "Election Explorer
  is Open Source:".
- **Where:** `res/ElectionExplorer.rc`, `res/app.manifest`, `main.c` `AboutDlgProc`.
- **Commit:** e163c62

### 2026-09-07 — Compare: canonical addresses and the Differences view
- **What:** normalized addresses use one style (`street[, unit], City, STATE ZIP[-ZIP4]`); compare
  ignores formatting, the trailing state and ZIP+4 precision. Show Differences… lists each changed
  voter's differing fields side by side, sortable and copyable.
- **Where:** `src/voter_table.{c,h}` (`compose_address`, `ee_canon_for_compare`,
  `ee_canon_address_for_compare`, `EeVoterTable_CollectDifferences`); `main.c` `k_DiffClassName`.
- **Checked:** smoke tests `cmpfmt`, `cmpstate`, `cmpzip`, `cmpdiff`.
- **Commit:** cb21055, 74871da, e613c4b, 5ea9bd5

### 2026-09-06 — Compare grades name and address changes as minor or major
- **What:** name and address changes are graded by case-insensitive edit distance; a precinct
  change is flagged only when the address is unchanged.
- **Where:** `src/voter_table.c` (`ee_levenshtein_ci`, `EE_CMP_MINOR_MAX_EDITS` = 4,
  `EE_CMP_MINOR_MAX_PCT` = 25%).
- **Commit:** 8741f69

### 2026-09-05 — Help menu and About; Voter ID and banner-row fixes
- **What:** a Help menu with feature topics and an About dialog. `ID_VOTER` is a Voter ID header.
  A leading banner row before the header is skipped.
- **Where:** `main.c`; `src/voter_table.c`.
- **Checked:** smoke tests `idvoter`, `preamble`.
- **Commit:** 5fbcacb, 0df8210, 19e989d

### 2026-09-04 — Compare two voter lists by Voter ID
- **What:** a Compare menu lists the other open lists; the Compare Summary counts added, removed,
  changed and identical voters and highlights them in each grid.
- **Where:** `src/voter_table.{c,h}` `EeVoterTable_CompareByVoterId`; `main.c` (`CompareWndProc`,
  `App_StartCompare`, `App_ApplyMarks`).
- **Checked:** smoke test `cmp`; all four configurations build with 0 warnings; click-tested.
- **Commit:** 7be360c (build warnings: cbbf2a0)

### 2026-09-03 — Precinct and Address reports
- **What:** Reports → Display Precinct / Address Report… with voter counts per value, a
  "(blank)" row, sort, copy, Include / Exclude and Show in Maps; styled headers and a status bar.
- **Where:** `main.c` (`ReportWndProc`, `App_ShowReport`); `src/voter_table.{c,h}`
  `EeVoterTable_CollectValueCounts`.
- **Checked:** smoke test `valcount`.
- **Commit:** 98574f8, 278b775

### 2026-09-03 — Fast duplicate scans
- **What:** Show Duplicate Voters (same name and date of birth) added; both duplicate scans became
  O(n) with a row-mark view instead of one filter rule per ID, cancelable for large lists, sorted
  by their key, with Filter → Reset View.
- **Where:** `src/voter_table.{c,h}` (`EeVoterTable_MarkDuplicateVoterIds`,
  `…VotersByNameDob`); `main.c` (`AppState.mark_*`, `App_SortByTableColumnAscending`).
- **Checked:** Dallas 1.48M rows: name + DOB scan 373 ms, Voter ID 213 ms (were minutes). Smoke
  tests `dupvoter`, `markdup`.
- **Commit:** 20fe6aa, 207da43, c3fbfec, b4823ba

### 2026-08-31 — Duplicate Voter IDs; Precinct in the normalized pane
- **What:** Filter → Show Duplicate Voter IDs…. The frozen pane adds Precinct (integer precinct;
  letters and dotted subdivisions dropped).
- **Where:** `src/voter_table.{c,h}`; `main.c`.
- **Checked:** smoke tests `dupvuid`, `pct`.
- **Commit:** e2fd687, 8420609
- **Details:** `docs/voter-list.md`

## Development builds (0.1.0.0, August 2026)

Everything here shipped in 1.0.1.0.

### 2026-08-30 — Normalized addresses, block numbers, partial birth dates, links
- **What:** complete-address columns are used as the street line; `LOT` units dropped; ZIP tails
  tidied; "6007.0" house numbers shown as 6007; masked birth dates (`2005`, `*/*/2005`) sort and
  filter by year. Right-click an `https://` cell → Open link…; more name-part headers recognized;
  filter dialog layout adjusted.
- **Where:** `src/voter_table.c`; `main.c`.
- **Checked:** smoke tests `lotunit`, `zipdash`, `blkdot`, `dobpart`, `nameparts`.
- **Commit:** 8ad4e2e, fd735e3, 2e11608, 4b17cf9, 771e9f1
- **Details:** `docs/voter-list.md`

### 2026-08-27 — Date columns sort and filter as dates
- **What:** columns titled with "Date", EDR or DOB sort as calendar dates; less than / more than
  compare dates in the filter.
- **Where:** `src/voter_table.{c,h}`, `src/filter.{c,h}`.
- **Checked:** smoke test `datesort`.
- **Commit:** 402b9a9, 8646744

### 2026-08-25 — Show in Maps; faster filtering
- **What:** right-click an address → Show in Maps… (Google, Bing, Apple, OpenStreetMap). Filter
  performance improved; less than / more than offered only for number or date columns; the window
  redraws correctly after the filter dialog closes.
- **Where:** `main.c`; `src/filter.c`; `src/voter_table.c`.
- **Checked:** smoke tests `resdup`, `agehdr`.
- **Commit:** f8b4442, bf64c64, 56b2bf0, 7e20d73

### 2026-08-23 — Filter, several lists at once, normalized address
- **What:** a Process Monitor-style Filter dialog (Ctrl+L) with Include / Exclude rules. Several
  voter lists open in their own windows. The normalized address is built from residence columns,
  ignoring mailing columns.
- **Where:** new `src/filter.{c,h}`; `main.c`; `src/voter_table.c`.
- **Checked:** smoke tests `filter`, `resaddr`.
- **Commit:** d53a7e0, 06e37a8, a921fd2

### 2026-08-22 — Normalized address in the frozen pane
- **What:** the frozen pane shows a composed residence address; a ZIP+4 shows only when its
  extra digits exist and are not zeros.
- **Where:** `src/voter_table.{c,h}`; `main.c`.
- **Checked:** smoke test `zip4`.
- **Commit:** 45c9eeb

### 2026-08-20 — Copy, surname-first names, pane titles, zoom, selection, more columns
- **What:** Edit → Copy with an option to prepend the normalized data; a surname-first Name
  option; bold grey pane titles; zoom 50–250%; the system highlight across both panes; up to
  1,024 display columns.
- **Where:** `main.c`; `src/voter_table.{c,h}`.
- **Commit:** ecce05c, cadfe10, d4108a7, f639ff3, e35fabc, a3baa53

### 2026-08-19 — Load dialog and frozen pane default
- **What:** the load dialog's Cancel no longer overlaps the progress bar; the 320 DIP frozen-pane
  default is applied before the first layout.
- **Where:** `main.c`.
- **Commit:** f03d13b

### 2026-08-17 — Application icon
- **What:** the first application icon.
- **Where:** `res/app.ico`, `res/ElectionExplorer.rc`.
- **Commit:** 33556ec

### 2026-08-08 — Load and display large voter lists
- **What:** CSV / tab-delimited voter lists load on a background thread with a cancelable
  progress dialog into a DPI-aware dual-pane virtual grid (frozen Voter ID and Name, resizable
  splitter, sort).
- **Where:** `main.c`, `src/voter_table.{c,h}`; `test/smoke_load.c` and sample files.
- **Commit:** e43b5d7
- **Details:** `docs/voter-list.md`

### 2026-08-08 — Scaffold
- **What:** the Win32 GUI solution (C11, no MFC, static CRT, VS 2026 `v145`, x64 / ARM64 Debug and
  Release via `Applications.sln`).
- **Where:** `src/main.c`, `res/`.
- **Commit:** 29ca0bc
