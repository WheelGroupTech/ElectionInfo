# macOS and Linux — design (portable engine, step 1)

Status: **step 1 done** (2026-11-21). The engine and its console tests build and pass on Linux
(Ubuntu 24.04 in WSL2, GCC 13), and since 2026-11-22 on macOS 15 (Intel Mac mini, Apple clang
17, universal binaries). Scanned documents are read by OCR through Apple Vision on macOS and
Tesseract on Linux (2026-11-22). The Windows build is unchanged. The macOS and Linux UI has
started with a prototype of the voter list window (AppKit and GTK 4, decision X7).

## Goal

Ship Election Explorer on macOS and Linux as well as Windows, from one source tree, without
slowing or risking the Windows Store app.

## Decisions

| # | Decision | Why |
|---|---|---|
| X1 | Keep the Win32 UI (`main.c`) for Windows and the Store. | It ships. A rewrite gains Windows users nothing. |
| X2 | Port the engine first, as it is, and prove it with the existing tests. | The engine is ~66,000 of the ~96,000 lines and holds every file format. Once it passes on every platform, the UI work is only UI work. |
| X3 | No `#ifdef` forest in the engine: stand-in `windows.h`, `strsafe.h` and `intsafe.h` in `src/platform/posix/include/`, put on the include path only by the macOS/Linux build. | The engine sources keep their Win32 idioms (`StringCch*`, `MultiByteToWideChar`, `CreateFileW`…), and the Windows build never sees the stand-ins. |
| X4 | Wide strings hold **UTF-16 code units on every platform**. | `wchar_t` is 2 bytes on Windows and 4 on macOS/Linux. Keeping UTF-16 values (surrogate pairs included) means lengths, offsets and the engine's surrogate handling are the same everywhere. Only code that copies raw UTF-16 *bytes* into `wchar_t` must go unit by unit. |
| X5 | The printf family follows MSVC rules (`%s` in a wide format is a wide string). | The engine and tests use `StringCchPrintfW(L"%s", wide)` and `wprintf(L"%s", wide)` throughout. glibc's and macOS's `%s` means a narrow string there. |
| X6 | CMake for macOS and Linux; `Applications.sln` stays the Windows build and the MSIX packaging. | CMake also generates an Xcode project. The `.vcxproj` and `.wapproj` carry the Store packaging that CMake does not. |
| X7 | A native UI on each: **AppKit (Objective-C) on macOS, GTK 4 (C) on Linux**, over the plain C bridge `src/ui/ee_ui_voters.h`. Chosen by the user on 2026-11-22 (the sign-off `AGENTS.md` asks for), in place of the earlier wxWidgets recommendation. Prototype: the voter list window (below). | No C++ (`AGENTS.md` prefers C11), and each platform's own controls. Objective-C is already used for Apple Vision; GTK 4 is C and LGPL (linked dynamically, as Linux packages do). Three UIs over one engine: each is a thin layer of windows over the same calls. |

## Layout

```
ElectionExplorer/
  CMakeLists.txt, CMakePresets.json   macOS / Linux build (presets name the Build/ folder)
  src/                                engine + Win32 UI, as before
  src/platform/posix/
    include/windows.h                 the Win32 / MSVC CRT subset the engine uses
    include/strsafe.h, intsafe.h      forward to windows.h
    ee_port.c                         implementations: code pages, printf, strsafe, files, time…
    settings_posix.c                  settings.h on a text file (settings.c is the registry one)
    ocr_none.c                        ocr_win.h placeholder: "OCR is not available"
    wmain_posix.c                     main() -> wmain() for the console tests
  src/ui/ee_ui_voters.{h,c}           plain C bridge for the macOS / Linux windows
  src/ui/macos/app_main.m             AppKit window (prototype)
  src/ui/gtk/app_main.c               GTK 4 window (prototype)
```

Build output follows AGENTS.md: `Applications/Build/linux-x64-debug/ElectionExplorer/`
(`smoke_load`, `data_check`; objects under `obj/`).

## What the platform layer supplies

- **Types and constants:** `BOOL`, `DWORD`, `HANDLE`, `LARGE_INTEGER`, `FILETIME`, `SYSTEMTIME`, `OVERLAPPED`, `HRESULT` codes, `CP_*`, `GENERIC_*`, `MAX_PATH`, `ARRAYSIZE`, `min` / `max`, `__declspec(thread)`.
  - `DWORD`, `LONG` and `ULONG` are 32-bit, as on Windows (LLP64), so binary file layouts are unchanged.
- **Code pages:** `MultiByteToWideChar` and `WideCharToMultiByte` handle three code pages:
  - `CP_UTF8`: an invalid byte becomes U+FFFD, or the call fails with `MB_ERR_INVALID_CHARS`, as on Windows.
  - `CP_ACP` / 1252: Windows-1252, the code page of a US Windows machine.
  - 28591: ISO-8859-1.
- **printf:** one formatter behind `StringCchPrintfW/A`, `StringCchVPrintfW`, `wprintf` and `fwprintf`:
  - It follows MSVC's rules for `%s`, `%S`, `%hs`, `%ls`, `%c` and `%C`.
  - `%ld` and `%lu` are 32-bit, matching what the code passes.
  - Numbers are formatted by the C library.
  - Wide output to a `FILE` is written as UTF-8.
- **strsafe:** `StringCchCopy`, `CopyN`, `Cat`, `CatN` and `Length` (W and A). They have strsafe's truncation and return-code behavior.
- **MSVC CRT:**
  - `_wcsicmp`, `_wcsnicmp`, `_stricmp` and `_strnicmp` fold ASCII only, as in the "C" locale.
  - Also: `_wcsdup`, `_wtoi`, `_wcsupr_s`, `wcsncpy_s` (with `_TRUNCATE`), `wcstok_s`, `sscanf_s`, `swscanf_s` (numbers only), `_fseeki64` / `_ftelli64`, `_wfopen_s` and `qsort_s` (MSVC argument order, on `qsort_r`).
- **Character classes:** `iswalpha`, `iswalnum`, `iswupper` and `iswlower` know non-ASCII letters, as MSVC's do. The platform "C" locale knows only ASCII.
- **Case mapping:**
  - `CharUpperBuffW`, `CharLowerBuffW` and `CompareStringOrdinal(…, TRUE)` cover Latin-1, Latin Extended-A, Greek, Cyrillic and full-width Latin.
  - `towupper` and `towlower` stay the platform's, which are ASCII-only, like MSVC's in the "C" locale.
- **Files:**
  - `CreateFileW`, `ReadFile` / `WriteFile` (with an `OVERLAPPED` offset, via `pread` / `pwrite`), `GetFileSizeEx`, `GetFileAttributesExW`, `Get/SetFileTime`, `DeleteFileW` and `GetTempPathW`.
  - Paths convert to UTF-8 and `\` becomes `/`, so paths built with Windows separators work.
  - Opening a folder as a file fails, as on Windows.
- **Time and process:** `GetLocalTime`, `FileTimeToSystemTime`, `SystemTimeToFileTime`, `FileTimeToLocalFileTime`, `GetTickCount` and `GetCurrentProcessId`.
- **Interlocked:** `InterlockedExchange` and `InterlockedCompareExchange`, on the compiler's atomics.
- **Settings:** a registry key maps to `~/.config/<key without "Software\">.ini` on Linux and `~/Library/Application Support/…` on macOS. The value names are the registry's. `RegDeleteKeyW` removes the file, which is what the settings test uses.
- **OCR (`ocr_win.h`):** `ocr_bridge.c` implements it on top of an engine's plain C interface, `ocr_engine.h` (open, close, name, recognize).
  - The engines are Apple Vision on macOS (`src/platform/macos/ocr_vision.m`) and Tesseract on Linux (`src/platform/linux/ocr_tesseract.c`), both described below.
  - Without Tesseract installed, Linux uses `ocr_none.c`, which refuses scans.
  - The engine files never include the stand-in `windows.h`: Objective-C's `BOOL` and Leptonica's macros clash with it.
  - `EeOcr_EngineName()` names the engine in load notes and messages ("Windows OCR", "Apple Vision", "Tesseract").
- **Glyph outlines:** on macOS and Linux, `go_draw` (`ee_glyph_ocr.c`) fills the outlines of the PScript5 reports' Type 3 glyphs with its own scanline rasterizer in place of GDI (below).

## Source changes outside the platform folder

| File | Change |
|---|---|
| `src/ee_glyph_ocr.c` | `go_draw` (draws glyph outlines with GDI for OCR) returns FALSE off Windows. It is unreachable there until an OCR engine exists. |
| `src/ee_results_ocr.c` | Fixed a latent bug that GCC found: `key[o++] = …key[o - 1]…` read and incremented `o` in one expression, whose order C leaves undefined. MSVC and GCC/Clang may differ. It is now two statements, reading the previous character first. |
| `src/ee_results_pdf.c` | Fixed a Windows bug the port exposed. `rp_item_cmp` and `rp_ptr_x_cmp` returned 0 for two text pieces at the same point, so their order was whatever the sort left. MSVC's `qsort` and glibc's differ, and MSVC's varies from page to page. Brazos's "accessible" Hart precinct reports draw a wrapped title's pieces at one point, so on Windows titles came out scrambled ("LIEUTENANT GOVERNORty - Republican Par"), and one file gave 110 contests instead of 98. The pieces now keep the order the page drew them in (their place in the page's text pool). |
| `src/ee_audit.c`, `src/ee_audit_hart.c` | Three error-message helpers took the file name after the last `\` only. They now also accept `/`, which Windows paths may use too and macOS/Linux paths always do. |
| `test/smoke_load.c` | The UTF-16 CSV fixture is written unit by unit instead of `memcpy` (which assumed a 2-byte `wchar_t`). The two tests that draw their fixtures with GDI and read them with Windows OCR (`hartocr`, `resglyph`) say "skipped" off Windows, as they already did on a Windows machine without OCR. |
| `test/data_check.c` | `base_name` accepts `/` as well as `\`. The "ms" timings are printed with an explicit `unsigned long` cast, which `%lu` needs on LP64. |

## How it was checked (2026-11-21)

- **Linux:** `smoke_load` passes every test. `hartocr` and `resglyph` are skipped.
  - Its output is identical line for line to the Windows run except those two lines.
- **Windows:**
  - `ElectionExplorer.vcxproj` Release x64 builds with zero warnings.
  - `smoke_load` built with `cl` per `test/README.md` passes every test, including both OCR tests.
- **Linux release build (-O2):** `smoke_load` gives the same results.
- **Windows ARM64:** Release also builds clean.
- **Real county files:** `data_check` was run three ways on every file the 2026-11-17 backup plan says loads (voter lists excluded):
  - the Windows build before these changes;
  - the Windows build after them;
  - the Linux build.

  The JSON was compared with the timings removed.
  - **Audit logs, CVRs, and results from ZIP, XLSX and XML:** 4,022 results, CVR and audit files in all.
    - Windows and Linux agree on every file except the 179 that need OCR. The Linux build refuses those with "OCR is not available on this platform yet".
    - (One more file, whose name has parentheses, differed only because the harness passed its name through a shell. Run directly, it agrees.)
  - **Old and new Windows builds:** they differ only on the 27 Brazos "accessible" precinct reports, the comparator fix above.
  - **Rosters:** Windows and Linux agree on all 7,917 files: CSV, PDF, TXT, XLS, XLSX and ZIP. (One file, whose name has parentheses, needed the same direct rerun.)

## macOS (2026-11-22)

- **Machine:** an Intel Mac mini running macOS 15 with Xcode, Apple clang 17, CMake 4.5 and Ninja 1.13. There is no Homebrew; CMake and Ninja are the official downloads.
- **Driven from Windows over SSH:** the machine is reached with key authentication through Windows' `ssh-agent`. Commands run as `ssh macmini 'zsh -l -c "…"'`, because a non-login shell does not get `/usr/local/bin` on its PATH.
- **Build:** the `mac-universal-debug` and `mac-universal-release` presets build universal binaries (x86_64 and arm64) with no source changes. The only warnings are the three Linux shows, once per architecture.
- **Tests:** `smoke_load` passes in Debug and Release, and `ctest` passes. `hartocr`, `resglyph` and `ressos` are skipped (no OCR engine yet).
  - Its Debug output is identical line for line to Linux's.
  - An Intel Mac runs only the x86_64 half; the arm64 half is built but untested.
- **The data:** Voter_Lists, Voter_Rosters, Audit_Logs, Election_CVRs and Election_Results were copied to the Mac: 16,743 files, 56.2 GB.
  - SHA-256 of every file on both machines found 45 text files whose copy had CRLF turned into LF (the Dallas and El Paso countywide voter lists, El Paso rosters, two Travis CVRs, and Bexar and Williamson results). Nothing else differed.
  - Those 45 were copied again with `scp`, and then every file was identical.
- **Real county files:** `data_check` ran on the same 16,741 files on Windows and on the Mac (all but `_Other-2022 Primary`). The JSON was compared with the timings removed.
  - `data_check voters` now also prints `cells_fnv`, an FNV-1a checksum of every cell, so a voter list's contents are compared without printing any of them.
  - **Identical on every file that does not need OCR:**
    - audit logs: 137;
    - CVRs: 171 of 174;
    - results: 4,058 of 4,295;
    - rosters: 12,074;
    - voter lists: 61 (with matching cell checksums).
  - **The rest need OCR:** 233 files the Mac refuses with "OCR is not available on this platform yet" (Windows loads 183 of them), and the 7 Secretary of State canvass forms with a Paper Capture text layer. Without OCR, those 7 fall back to their earlier refusal, "not a results report this version can read".
  - **Time:** 43 minutes on the Mac (the 71 files over 200 MB one at a time took 36 of them) and 42 on Windows (which also ran OCR).

**Seen while checking, not fixed:** Brazos's 28 "accessible" Hart precinct reports also split words inside titles, at places that differ from file to file ("PLA CE 4", "R epublican Party"). Loaded together they give 1,448 contests and 2,229 issues where one election's ballot has about 100 contests. The comparator fix removed the scrambling (it was 1,535 and 2,457), but merging these reports needs its own work.

## Apple Vision OCR on macOS (2026-11-22)

- **Files:** `src/platform/macos/ocr_vision.m` (Objective-C, compiled with ARC), behind `src/platform/posix/ocr_engine.h` and `ocr_bridge.c`. (The first version's `ocr_apple.c` and `ocr_vision.h` became the shared bridge when Tesseract came.)
- **Build:** CMake enables Objective-C on Apple and links Vision, CoreGraphics, ImageIO and Foundation.
- **Same input as Windows OCR:** each image is decoded (ImageIO, including CCITT TIFF), scaled with high-quality interpolation and capped at 10,000 pixels (Windows OCR's `MaxImageDimension`), made 8-bit gray and turned. It is then read by `VNRecognizeTextRequest`: accurate level, language correction off, `en-US`.
- **Where Vision differs, and what the bridge does about it:** the readers in `ee_results_ocr.c` and `hart_ocr.c` were built on Windows OCR's output.
  - **Underlines:** accurate mode does not read a count printed on a rule. On page 1 of Angelina's Secretary of State form it read 4 of the 29 counts. Dark runs along a row longer than 1/25 of the page are whitened first, and then all 29 read.
  - **Revision 3** (macOS 13's default) drops letters at the ends of lines and invents words on some scans (Henderson's 2021 form: "ROPOSITION 1 (HJR 14", "CONOUCT OHAR FOBEE"). Revision 2 reads them and is used while macOS offers it. Apple marked it deprecated in macOS 15.
  - **Box size:** word boxes are a line's height, padded, about twice the letters', and a few pixels wider each side. That merged rows and moved numbers out of their columns. Each box is trimmed to the band of inked rows at its centre and then to its inked columns.
  - **Sideways text:** Vision reads it in place, Windows OCR does not, and the readers rely on that to turn a page scanned sideways (Midland's) until it reads well. Lines much taller than wide are left out.
- **Checked against Windows OCR:** `data_check` with every choice's total, on both machines.
  - **The 8 Secretary of State forms:** every count equal (Brazos, Angelina, Henderson). Vision misses fewer faint write-in zeros (Henderson G20: 1 row against 6; Angelina G24: none against 3).
  - **41 other scanned results files** (every scanned file Windows loads, outside Cameron and the Sierra CVRs, plus 12 Cameron files): 38 load on both.
    - The 3 that do not are Travis's PScript5 reports. Their glyph outlines were drawn with GDI (`go_draw`). With the rasterizer below they load, and match Windows on every total.
    - 1,182 choice totals are equal under the same names and 27 differ. Of the totals named differently by the two engines (a title or name OCR'd differently), 234 more are equal and 35 differ; 99 Windows totals have no partner.
    - The differences gather in poor scans, where both engines misread: Bowie's 2026 unofficial results, Cameron's ERM precinct reports. Clean scans agree closely (Guadalupe's primaries, Midland, Cameron's summaries, Ellis's ZIPs).
  - **The rest of the scanned results files** (189 more, about 8,000 pages, nearly all Cameron's): all read on the Mac. The first 182 took about 2½ hours (three processes); Cameron's 7 largest primaries took several hours more.
    - 125 load on both; 14 load on Windows only and 2 on the Mac only.
    - 1,124 totals are equal under the same names and 388 differ.
    - These are mostly Cameron's 2020–23 ERM monospaced scans. Windows OCR leaves 7.5% of their rows not adding up, so neither engine's reading is a reference there.
  - **Sierra County's scanned Hart CVRs** (`data_check cvr`, against Windows OCR; 4-hour limit a file):
    - **G20 and G24:** the same ballot records and ballots as Windows (1,964 / 1,961 and 1,780 / 1,776), with 29 columns each (Windows: 35 and 29).
    - Records needing review: 736 and 32, against Windows' 22 and 21. Vision misreads G20's marks far more often.
    - **G22 (2,109 pages):** not finished within 4 hours on the Intel Mac. Windows read it in about 20 minutes.
- **Smoke suite on macOS:** `hartocr`, `resglyph` and `ressos` run and pass.
  - Their test pages and Arial glyph outlines are drawn with CoreGraphics and CoreText (`test/tdraw_apple.c`, plain C behind `test/tdraw.h`, linked to `smoke_load` on Apple).
  - `hartocr` now looks for `EeOcr_EngineName()` in its load note, not "Windows OCR".

## Glyph outlines without GDI (2026-11-22)

- **The reports:** Travis's L22, LR22 and PR22 reports were printed through PScript5. Their text is a Type 3 font of unnamed glyph outlines, which `ee_glyph_ocr.c` draws and reads by OCR to learn each glyph's character.
- **`go_draw`:** on Windows it keeps GDI. On macOS and Linux it uses a scanline rasterizer:
  - Curves are flattened into pieces about two pixels long.
  - Each glyph is filled as GDI fills it: nonzero winding, a pixel black when its centre is inside, and open figures closed.
- **Checked:** the three Travis reports give the same totals as Windows on the Mac (Apple Vision) and on Linux (Tesseract): 199, 6 and 18. `resglyph` passes on the Mac.

## Tesseract OCR on Linux (2026-11-22)

- **Files:** `src/platform/linux/ocr_tesseract.c`, behind `ocr_engine.h` and `ocr_bridge.c`.
- **Build:** CMake finds Tesseract and Leptonica through `pkg-config` (Ubuntu: `libtesseract-dev libleptonica-dev tesseract-ocr-eng`) unless `EE_WITH_TESSERACT` is off. Without them it uses `ocr_none.c`. Built and checked with Tesseract 5.3.4 and Leptonica 1.82 (Ubuntu 24.04).
- **Same input as Windows OCR:** Leptonica decodes the image (JPEG, PNG, TIFF including CCITT fax), makes it 8-bit gray, scales it (smoothed down, linear up), caps it at 10,000 pixels and turns it. Tesseract reads it with automatic page layout (`PSM_AUTO`) and English data.
- **Where Tesseract differs:**
  - **Sideways text:** like Vision, it reads sideways text in place. Words of three or more characters whose box is more than 1.5 times taller than wide are left out, so Midland's sideways pages get turned.
  - **Printed rules:** it gives them as words of spaces, which are left out.
  - **Halftone headings:** its layout takes a heading band printed in halftone dots for a picture and does not read it (Guadalupe's 2026 Electionware reports L26, LR26 and PR26 were refused).
    - The fix (`picture_bands`, `read_band`): the page's layout is analysed first. A picture block shaped like a band (a quarter of the page wide or more, four times as wide as high) in which the page's reading found no words is read again on its own. It is blurred 2 pixels and thresholded at 140, so the dots turn white, and read as one block. Its words are put back at their places on the page.
    - Bands the page's reading already reads are skipped. Reading Midland's again added a second copy of its headings, which moved 45 counts out of their columns.
    - Tried and rejected: inverting mostly dark rows (no gain); blurring and thresholding the whole page (the band reads, but 18 of the sample's files fail instead of 4); single-block layout for the whole page (1,119 equal totals against automatic layout's 1,459).
  - **Small pages:** Tesseract reads letters about 20 pixels high or more; Windows OCR reads smaller ones. A page under 1,800 pixels on its long side is read at 2 times its size (3 under 900), and its words are scaled back.
    - Hart's scanned CVR Report is read at 1,224 x 1,584, with letters about 10 pixels high. There Tesseract missed the whole heading ("CVR Report", the run time and date), so Sierra County's CVRs were refused as "not a Hart Cast Vote Record report".
- **Threads:** Ubuntu's Tesseract uses OpenMP. With seven processes reading at once on 8 cores, files that take 30 seconds alone took over 50 minutes. Batch runs set `OMP_THREAD_LIMIT=1` (the sample of 48 files then takes under 3 minutes).
- **Checked against Windows OCR** (`data_check` with every choice's total; the 41 sample files above plus the 7 Secretary of State forms):
  - 47 of 48 load (44 before the band and small-page changes). The one that does not is Cameron's GR20 runoff summary.
  - 1,465 totals are equal under the same names and 50 differ (1,459 and 48 before).
  - The Secretary of State forms: Brazos, both Angelina G25 copies, Henderson G21 and L22 are equal count for count; Angelina G24 differs on 5 counts and Henderson S21 on 4.
  - The three Travis glyph reports: every total equal.
- **The rest of the scanned results files** (189, about 8,000 pages, 50 minutes with seven processes): 133 load on both. Windows loads 139, and 1 loads on Linux only.
  - **Against the Mac:** on the 182 files the Mac had read, Tesseract matches Windows on 780 totals and differs on 378. Apple Vision matches on 1,123 and differs on 341.
  - **Where:** as with Vision, the differences are mostly in Cameron's ERM scans.
- **Sierra County's scanned Hart CVRs** (`data_check cvr`, against Windows OCR; refused before the small-page change):
  - **G20 (1,964 pages) and G24 (1,272 pages):** the same ballot records and ballots as Windows (1,964 / 1,961 and 1,780 / 1,776).
    - Columns: 31 and 46, against Windows' 35 and 29. A contest or choice OCR'd differently makes a column of its own.
    - Records needing review: 45 and 63, against 22 and 21.
  - **G22 (2,109 pages):** with a 4-hour limit, 1,582 ballot records and ballots, as on Windows. 77 columns against Windows' 62; 975 records need review.
- **Smoke suite on Linux:** `hartocr`, `resglyph` and `ressos` run and pass with Tesseract.
  - Their test pages and glyph outlines come from `test/tdraw_ttf.c`, with no library: it reads a TrueType font file (Liberation Sans, else DejaVu Sans, or the file `TDRAW_FONT` names; cmap format 4, simple glyphs) and fills the outlines with a scanline rasterizer at 4 x 4 subsamples a pixel.
  - Without OCR or without a font, the tests say so and are skipped.

## The voter list window, prototype (2026-11-22)

One window on each platform: the loaded list in a table, File > Open (or a file named on the command line, or opened from the Finder on macOS), the load in the background with a progress bar, sorting by a column's heading (again to reverse), numbers and dates right-aligned, and a status line ("5 voters, 24 columns — sample_voters.csv"; "Filtered: 3 of 5 voters, …" when filtered).

Filters (added 2026-11-23), as on Windows:
- **Filter…** (Ctrl+L on Linux, ⌘L on the Mac): "Display entries matching these conditions:", then column / relation / value / "then" Include or Exclude, with Reset, Add and Remove; the rules listed with a check box each; OK, Cancel, Apply. A selected rule fills the fields. The engine's rules apply: includes on one column are ORed, different columns ANDed, and any matching exclude hides the row. A date column compared by less than or more than needs a date (the Windows message).
- **Reset Filter**, and **right-click a cell:** Include "value", Exclude "value" (at most 40 characters shown) and Filter….
- **In the background:** a sort or filter is a *view* (an order and a filter map) computed on a worker thread (`EeUiVoters_ComputeView`). It sorts a copy of the table's header with its own `view_index`, so the table on screen stays readable, then swaps it in on the UI thread (`EeUiVoters_SetView`). Requests made meanwhile are merged: the latest wins. Windows sorts on the UI thread.

Copy, Export and duplicates (added 2026-11-23), as on Windows:
- **Copy** (Ctrl+C on Linux, ⌘C on the Mac, and first in the right-click menu): the selected rows, a line each, with the fields separated as in the list's file (commas for a CSV), the Voter ID, Precinct, Name and Address columns first (the Windows Options default). A right-click outside the selection selects that row first, as on Windows.
- **File > Export Voter List…** (the shown rows) and **right-click > Export Selected…**: "Include normalized data fields" (off at first), then a UTF-8 CSV or TSV with a byte order mark and a heading row, named as on Windows (`<list>-All_Voters.csv`, `-Filtered_Voters`, `-Selected_Voters`). GTK asks in a small Export Options window, then a `GtkFileDialog`; the Mac puts the check box and the CSV / TSV choice on the save sheet. A name without `.csv` or `.tsv` gets one.
- **Filter > Show Duplicate Voter IDs… / Show Duplicate Voters…:** only the rows that share a Voter ID, or a first, middle and last name and a birth date, with another row (the engine's `EeVoterTable_MarkDuplicate*`), sorted by Voter ID or Name to keep them together, and within the filter. "Show Duplicate Voters" needs a birth date column ("No birth date data is available"); none found is said in the Windows words, and the shown view stays. The status line reads "Showing 4 duplicate Voter IDs of 9 voters", or "duplicate Voter IDs within filter: 2 of 9 voters". A duplicates view is part of the view: the scan runs on the worker thread, and later sorts and filters keep its marks. **Reset Filter** and **Reset View…** show all rows again.

The Precinct and Address reports (added 2026-11-23), as on Windows:
- **Reports > Display Precinct Report… / Display Address Report…** (grayed until a list is loaded): a window each, "Precinct Report - <file>", with a row per distinct value (case-insensitive, shown as first spelled) and its **Number of Voters**, over every loaded row: the filter and a duplicates view are left out. Empty cells make a "(blank)" row when other values exist; a list with none says "No precinct information available" (or address). The status line reads "732 unique precincts", or "… (includes blank)". Choosing the report again brings its window forward; it closes when the list is reloaded or its voter list window closes.
- **Sorted** by the value at first (precincts as numbers), or by a click on either heading (again to reverse), on the UI thread as on Windows.
- **Copy** (Ctrl+C / ⌘C and the right-click menu): "<value> tab <count>" a line. **Right-click** (Windows' order): Copy; Include "value" and Exclude "value" on the value column, which add a rule for each selected row to the voter list's filter ("(blank)" matches the empty cells); **Show in Maps…** for an address, which opens a search in the browser in the Options' map (Google Maps unless changed); Export Selected… and Export All… (`<list>-All_Precincts.csv`, `-Selected_Addresses.csv`; headings "Precinct" or "Address" and "Number of Voters"; no Export Options, as on Windows).
- **The bridge:** `EeUiVoters_Report` over the engine's `EeVoterTable_CollectValueCounts`, returning an `EeUiReport` that keeps its own copy (UTF-8 for showing, the engine's text for sorting with Windows' comparison), with `Sort`, `CopyText`, `Export`, `ExportName`, `Rule` (the Include / Exclude rule) and `MapUrl`.
- **GTK:** a plain `GtkWindow` (not kept above the voter list) with a two-column `GtkColumnView`; the report's actions are a `report.` action group, and Ctrl+C a shortcut on its window. **Mac:** `EeReportWindow`, an `NSTableView` with grid lines and the plain style (rows from edge to edge), the menu built in `menuNeedsUpdate:`, the save sheet with the CSV / TSV choice. Its columns are sized after the window's layout: sized before, the table came out wider than the window and hid the Number of Voters arrow.
- **One arrow:** GTK's column view sorter keeps the earlier column as a secondary sort and still draws its arrow. Both GTK windows' sorter handlers now keep only the clicked column, the voter list's too (where a duplicates view's arrow on Name stayed beside a clicked heading's).

Options (added 2026-11-24), as on Windows:
- **Edit > Options…** on Linux; on the Mac **Election Explorer > Settings…** (⌘,), where a Mac app keeps them, as a sheet on the voter list window. Windows' four options in its words: "Pre-pend normalized data for copies (ID, Precinct, Name, Address)", "Display name in surname-first format", **Zoom** 50–250 %, and **Map engine** (Google Maps, Bing Maps, Apple Maps, Open Street Maps); OK and Cancel (Enter and Esc). Available any time, as on Windows.
- **Kept between runs** in the settings file (`settings_posix.c`) under Windows' value names (`CopyPrependNormalized`, `NameSurnameFirst`, `ZoomPercent`, `MapEngine`): `~/.config/WheelGroupTech/ElectionExplorer.ini` on Linux, `~/Library/Application Support/WheelGroupTech/ElectionExplorer.ini` on the Mac. OK saves them and keeps the file's other settings. The defaults are Windows': both on, 100 %, Google Maps.
- **Copy** follows the first option. Export's "Include normalized data fields" is a separate choice, as on Windows.
- **Names:** OK rebuilds the Name column (`EeVoterTable_SetNameSurnameFirst`) on the UI thread, as on Windows: 0.28 s for Dallas on both platforms. The rebuild rewrites the cells, so it waits for a sort or filter that is being computed and runs once that is done. A load composes the names as the options were when it began. When the shown view is sorted by Name or has a rule on it, a new view is computed in the background. The bridge keeps the engine from re-sorting on the UI thread, since the view's filter map would no longer match.
- **Zoom:** the voter list's text size (Windows zooms the grid only).
  - GTK uses a CSS font size on the table, headings included.
  - The Mac uses the cells' font and the row height; the headings keep their size, and a column stays at least as wide as its heading.
  - The columns' widths scale with the text (the text part, not the padding).
  - The Mac's voter table now has the plain style, rows from edge to edge: the default style inset the rows, with round ends, once the columns no longer filled the window.
- **Map engine:** Show in Maps… opens a search in that map, with Windows' links and encoding (`EeUiOptions_MapUrl`). It is in the Address report, and now also in the voter list's right-click menu on an Address cell (Windows' order: after Include / Exclude).
- **The bridge:** `EeUiOptions` with `_Defaults`, `_Load`, `_Save`, `_ClampZoom`, `_MapName` and `_MapUrl`; `EeUiVoters_Load` takes the name format; `EeUiVoters_SetSurnameFirst` / `_SurnameFirst`; `EeUiReport_MapUrl` takes the map.
- **Digits of one width** (added 2026-11-24, after the user saw Voter IDs of the same length come out of different widths on the Mac): the tables use the system font with its fixed-width digits, as Windows' Segoe UI has them, so IDs, dates and counts line up; letters are unchanged. The Mac uses `monospacedDigitSystemFontOfSize:weight:` (San Francisco's own digits are proportional) in the voter list and the report windows. GTK sets `font-feature-settings: "tnum"` on every `columnview` (once for the display, `add_table_css`) and measures the columns with the same feature (`text_width`); Ubuntu's fonts and DejaVu already have such digits, other fonts may not.

Saved filters (added 2026-11-24), as on Windows:
- **Export Filter…** and **Load Filter…** at the left of the Filter window's bottom row (Windows' place, between Reset and Apply / OK / Cancel; the port keeps Reset by Add and Remove).
- **The file** is Windows' own (`EeFilter_SaveFile` / `EeFilter_LoadFile`), so a filter saved on one platform loads on the others: UTF-8 with a BOM, the heading `Column, Column Number, Relation, Value, Action, Enabled`, a row per listed rule, on or off. Columns are named by title, so a filter loads into a list whose columns are in another order.
- **Export Filter…** saves the listed rules, applied or not. The suggested name is "<list name> Filter.filter.csv". As on Windows, a name ending in .csv or .tsv decides the format; another gets the chosen type's ".filter.csv" or ".filter.tsv", and then the window asks before replacing a file of that name ("… already exists. Do you want to replace it?", Cancel first). With no rules: "There are no filter rules to export."
  - GTK's save dialog does not say which file type was chosen, so a name without an extension gets ".filter.csv" there; the Mac's save sheet has Windows' type list below it (`export_type_popup`), as Export does.
- **Load Filter…** replaces the listed rules; they take effect on Apply or OK. Rules the list cannot use are left out and listed, in Windows' words: "Loaded 1 rule. 1 rule could not be used here:", then a line for each (a column the list lacks, an unknown relation or action, a date that is not one for a date column). A file with no rules: "The file holds no filter rules." A file that is not a saved filter gets the engine's message.
- Both dialogs start in the folder of the last filter exported or loaded in this run, as on Windows.
- **The bridge:** `EeUiFilterFile_Name`, `EeUiFilterFile_Path` (the extension rule), `EeUiVoters_SaveFilter`, `EeUiVoters_LoadFilter` (with Windows' date check and messages).
- **Windows has no Find in the voter list window** (Edit has Copy and Options… only; the Filter window does the narrowing). "Find" was listed here by mistake from 2026-11-22 on, and is dropped (the user's decision, 2026-11-24). The only Find box on Windows is in Window > Windows…, which the port does not have yet (it has one window).
- Not yet: Show Duplicate Voters Voting (it needs a roster), Open link… in the right-click menu.

- **`src/ui/ee_ui_voters.{h,c}`:** the bridge, plain C types only. It wraps `EeVoterTable` and `EeFilterSet` (load with progress and cancel, UTF-8 titles and cells, numeric columns, rules as `EeUiRule`, views with duplicates marks, `CopyText`, `Export`, the status line's count in the Windows words, `Sort` that toggles and `SortTo` with a direction) and holds the shared `--selftest`, which also filters, copies and finds duplicates. The window code never includes the engine's headers: the stand-in `windows.h` defines `BOOL`, which clashes with Objective-C's.
- **macOS, `src/ui/macos/app_main.m`** (ARC): an `NSTableView` that asks for the visible cells only, programmatic menus (⌘O, ⌘W, ⌘C, ⌘A, ⌘L, ⌘, for Settings, recent documents), the Filter window as a sheet (`EeFilterSheet`), the right-click menu built in `menuNeedsUpdate:`, `NSAlert` for errors. CMake target `ee_app` builds `Election Explorer.app` (universal).
- **Linux, `src/ui/gtk/app_main.c`:** a `GtkColumnView` over a `GListModel` of our own that makes a row object only for the rows on screen. The column sorters only show the heading's arrow; the engine sorts, then the model reports its rows changed. Header bar with Open, Filter and a menu (Ctrl+O, Ctrl+W, Ctrl+Q, Ctrl+C, Ctrl+L), a Filter window, an Export Options window, an Options window, a right-click `GtkPopoverMenu` made for each click, `GtkFileDialog`, `GtkAlertDialog`. Needs GTK 4.12+ (Ubuntu 24.04: 4.14, `libgtk-4-dev`); CMake builds `election-explorer` only when pkg-config finds it.
- **Checking without looking:** `--selftest <file>` loads, sorts, filters, copies and finds duplicates without a window and prints what the window would show. `--snapshot <png> [--duplicates ids|voters] [--sort <column>[:desc]] [--include|--exclude <column>=<value>]... [--filter-window] [--export <file>] [--report precinct|address] <file>` opens the window, loads the file, shows a duplicates view as the Filter menu does, sorts it as a heading click does and filters it as the right-click menu does, exports the shown rows (as Export Voter List… without the dialogs), writes the window (or the Filter window) as a PNG and quits. With `--report`, the PNG is of that report's window, `--sort 0|1[:desc]` clicks its heading and `--export` exports the report (Export All… without the dialog). `--zoom <percent>` and `--names given|surname` set those options once the list is loaded, as the Options window's OK does but without saving them, and `--options-window` makes the PNG of the Options window (the Settings sheet on the Mac). `--load-filter <file>` does what the Filter window's Load Filter… does and `--export-filter <file>` its Export Filter… (without the file dialogs; either opens the Filter window, and the PNG is of it). The window draws itself, so this works over SSH, where macOS gives no screen-recording permission. CI runs `--selftest` on both.
  - The snapshot waits until the window has drawn: right after a background sort GTK has not yet laid out the new rows, and the first try finds nothing to draw.
- **Checked on the Mac** (Debug):
  - `test/sample_voters.csv`: 5 rows, 24 columns; sorted by Precinct ascending, descending, ascending again.
  - The snapshot showed the window as expected (dark mode).
  - Dallas `CountyWide_20261101.txt`: 1,499,178 rows and 216 columns in 35 s, 1.87 GB peak memory.
- **Checked on Linux** (WSL Ubuntu 24.04, GTK 4.14.5, Debug, no warnings; WSLg shows the window):
  - `--selftest test/sample_voters.csv` prints the same as on the Mac.
  - The snapshot with `--sort 1:desc` (the GTK window's extra option: it sorts as a heading click does, through the column view's sorter) showed the arrow on Precinct, the rows by precinct descending, and "sorted by Precinct, descending" in the status line.
  - Dallas `CountyWide_20261101.txt`: loaded in 31.5 s; the heading sort took 5.3 s; 2.6 GB peak memory with GTK.
  - WSLg prints libEGL / MESA warnings (no GPU in WSL); GTK falls back to software rendering, and the window is unaffected.
- **Checked with filters (2026-11-23),** on both platforms (Debug; Linux and the Mac give the same results):
  - `--selftest`: Precinct is "101" shows 3 of 5; adding Exclude "101" shows 0; disabling it, in file order, 3 again.
  - Snapshots of the sample: Include Precinct "101", sorted by Name: 3 of 5 voters; Include "101" and Exclude PCTSPT "B", sorted by Voter ID descending: 2 of 5, the arrow and status line right. The Filter window with those two rules matches the Windows layout.
  - Dallas (1,499,178 rows) with a filter that tests every row and a sort: Linux 0.9 s and 1.7 s, the Mac 0.3 s and 2.6 s, on the worker thread. The earlier 5.3 s on Linux was mostly the old descending sort, which sorted ascending first and then again.
- **Checked with Copy, Export and duplicates (2026-11-23),** on both platforms (Debug, no warnings; the same results):
  - `test/sample_duplicates.csv` (made up, 9 rows): 4 rows share a Voter ID (one person twice, and two people with one ID) and 4 share a name and birth date (one under two IDs, and the person listed twice); twins who differ only by middle name are not duplicates. `--selftest` prints "Showing 4 duplicate Voter IDs of 9 voters" and "Showing 4 duplicate voters (name + DOB) of 9 voters", and CI checks both lines.
  - Snapshots: the duplicate Voter IDs grouped by Voter ID; the duplicate voters with Include Precinct "101" (2 rows, sorted by Name); `sample_voters.csv` has none, which is said.
  - The export of that view: the byte order mark, the heading row and the 2 rows, the same on both.
  - Dallas (1,499,178 rows): duplicate Voter IDs (20 rows) in 4.6 s on Linux and 3.8 s on the Mac, duplicate voters in 3.3 s and 3.2 s (Debug, on the worker thread; Linux Release 2.9 s). Exporting every row: 1,499,179 lines (547 MB) in 10.5 s on Linux and 10.1 s on the Mac (Debug), 3.9 s on Linux Release.
- **Checked with the reports (2026-11-23),** on both platforms (Debug, no warnings):
  - `--selftest` on both samples prints the same on Linux and the Mac: `sample_voters.csv` has 3 precincts (101 with 3 voters) and 5 addresses; `sample_duplicates.csv` 5 and 7, and CI checks those two lines. A filter is in place while the reports are made, and they still count every row.
  - Snapshots: the Precinct report by Number of Voters, most first, and the Address report by address, with the arrow on one heading only; the exports of the Address report are byte for byte the same on both.
  - Dallas (1,499,178 rows): 732 precincts, whose counts add up to every row, in 0.2 s, and the export is the same on both; 779,668 addresses in 2.1 s on Linux and 1.9 s on the Mac (Debug), 1.3 s on Linux Release, on the UI thread as on Windows.
- **Checked with Options (2026-11-24),** on both platforms (Debug and Release, no warnings):
  - `--selftest` on both samples prints the same on Linux and the Mac. It covers:
    - the names given name first and back, with the commas counted;
    - sorted by Name, both changes ask for a new view;
    - Copy without the normalized fields (`sample_duplicates.csv`: 256 bytes for two rows, 128 without);
    - the four map links.
    CI checks four of those lines. The self-test uses the default options and reads no settings file.
  - Snapshots on both, with a scratch settings folder:
    - the Options window (Windows' layout);
    - `sample_voters.csv` at 150 % with the names given name first ("Names shown given-name first.");
    - `sample_duplicates.csv` at 60 %, sorted by Name, given name first.
  - Saving (a scratch settings folder on Linux): the options come back after a reload, a zoom of 999 is brought to 250, and a setting the options do not own is kept.
  - Dallas (1,499,178 rows): the Name column rebuilt in 0.28 s on both, then sorted by Name in 4.0 s on Linux and 3.2 s on the Mac (Debug, on the worker thread).
- **Checked with saved filters (2026-11-24),** on both platforms (Debug and Release, no warnings):
  - `--selftest` on both samples prints the same on Linux and the Mac: two rules (one off, with a comma, quotes and a trailing space in its value) saved as CSV and as TSV and loaded back the same; a typed ".TSV" kept; Windows' messages for a column the list lacks and a date that is not one, a file with no rules, no file, and no rules to save. CI checks four of those lines.
  - Snapshots on both: two rules from the right-click menu exported to a name without an extension (it became `mine.filter.csv`); that file loaded and exported again as TSV, with the same rules; exporting again to `mine` asked before replacing `mine.filter.csv` and left it as it was; a filter with a column the list lacks loaded its other rule (off, as saved), with the warning open (on the Mac the sheet is dimmed behind it).
- **Checked by hand (the user, 2026-11-23):**
  - On WSLg: sorting by a heading, and opening the Dallas list.
  - **WSLg shows the menus behind the window:** the header bar's menu opens in front the first time, and behind the window (a sliver showing) from the second time on. GTK draws each menu as a window of its own; WSLg seems to bring the main window back in front of it. The X11 backend (`GDK_BACKEND=x11`) was hard to try, since WSLg showed no mouse pointer over it.
  - In an Ubuntu 24.04 desktop VM (VMware, the repository on a shared folder): the window ran with no issues, its menus included. So the menus are a WSLg problem, not the window's: use a Linux desktop (or a VM) to try the menus and dialogs.
  - In the VM: Include and Exclude from the right-click menu, and Reset in the Filter window, work. **Remove did nothing** (fixed; the user tried it again in the VM and it works): each rule row was a check button labeled with the rule, so a click on the text toggled the rule instead of selecting the row, and Remove had no selected row. A row is now a check box beside a separate label: a click on the text selects the rule, a click on the box turns it on or off and selects it too, and after Remove the next rule is selected.
  - In the VM: the Precinct and Address report windows work.
  - **Saved filters (the user, 2026-11-24),** in the VM and on the Mac: Export Filter… and Load Filter… with their file dialogs, the replace question, and a filter file saved by Windows Election Explorer.
- **Not checked yet, one by one:** the other clicks (Add and OK in the Filter window; Copy's paste; the Export dialogs; the Options window's OK, and the options kept for the next run, which the user tried in the VM and on the Mac; Show in Maps… on an Address cell). Snapshots cannot open a menu or press a button; the commands they run are the ones the snapshot options run.
- **Known limits:** an export runs on the UI thread, as on Windows (4 s for 1.5 million rows in Release), and builds the whole file in memory first (2.9 GB peak with Dallas loaded, on Linux).

## Known gaps and next steps

1. **macOS build:** done on an Intel Mac (above). The arm64 half is tested by CI's Apple Silicon runner (4).
2. **OCR:** macOS has Apple Vision and Linux has Tesseract (above), and glyph outlines are drawn without GDI. Still to do:
   - **Poor scans:** tune the readers for Vision's and Tesseract's output, where both still disagree with Windows OCR (mostly Cameron's ERM scans).
3. **UI:** AppKit and GTK 4 (X7). The voter list window prototype, with filters and saved filters, Copy, Export, duplicates, the Precinct and Address reports and Options, is above. Next: try its menus and dialogs by hand; then the other windows (roster, results, CVRs, audit logs), each over a plain C bridge like `ee_ui_voters.h`.
4. **CI (added 2026-11-22):** `.github/workflows/election-explorer.yml` runs on pushes and pull requests that touch `Applications/ElectionExplorer/` (and by hand).
   - **Linux** (`ubuntu-24.04`): installs CMake, Ninja, Tesseract, GTK 4 and Liberation fonts, then builds `linux-x64-release`, runs `ctest` with `OMP_THREAD_LIMIT=1` and the window's `--selftest`.
   - **macOS** (`macos-14`, Apple Silicon): installs Ninja, builds `mac-universal-release`, runs `ctest` and the window's `--selftest`, so the arm64 half is tested. Apple Vision OCR is part of macOS.
   - **Windows** has no job: `ElectionExplorer.vcxproj` needs the VS 18 (v145) toolset, which GitHub's hosted runners do not have yet.
   - Not yet seen running: the workflow runs once the commit is pushed.
5. **`.gitattributes` (added 2026-11-22):**
   - `* text=auto` keeps the stored files LF, as they all are, and gives each platform its own line endings, whatever a machine's `core.autocrlf` says.
   - `*.sh` is always LF; the Visual Studio, MSIX packaging, `.rc` and `.ps1` files are always CRLF.
   - PDFs, images, Office files and executables are binary.
   - No stored file changes: every text file in the index was already LF.
