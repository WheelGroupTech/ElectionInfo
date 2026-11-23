# macOS and Linux — design (portable engine, step 1)

Status: **step 1 done** (2026-11-21). The engine and its console tests build and pass on Linux
(Ubuntu 24.04 in WSL2, GCC 13), and since 2026-11-22 on macOS 15 (Intel Mac mini, Apple clang
17, universal binaries). Scanned documents are read by OCR through Apple Vision on macOS and
Tesseract on Linux (2026-11-22). The Windows build is unchanged. There is no macOS or Linux UI
yet.

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
| X7 | One cross-platform UI for macOS and Linux (recommended: wxWidgets). Not started. | Native Cocoa controls on macOS, GTK on Linux, virtual list views, and a license that allows static linking and Store distribution. |

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
  - **The rest of the scanned results files** (189 more, about 8,000 pages, nearly all Cameron's): 182 were read on the Mac in about 2½ hours (three processes). The other 7 are Cameron's largest primaries, still running when this was written.
    - 124 load on both; 8 load on Windows only and 2 on the Mac only.
    - 1,123 totals are equal under the same names and 341 differ.
    - These are mostly Cameron's 2020–23 ERM monospaced scans. Windows OCR leaves 7.5% of their rows not adding up, so neither engine's reading is a reference there.
  - **Not yet run on the Mac:** the three Sierra County scanned CVRs (5,300 pages; see Tesseract below for Linux).
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
  - **G22 (2,109 pages):** not finished within the run's one-hour limit a file. Windows read it in about 20 minutes.
- **Smoke suite on Linux:** `hartocr`, `resglyph` and `ressos` run and pass with Tesseract.
  - Their test pages and glyph outlines come from `test/tdraw_ttf.c`, with no library: it reads a TrueType font file (Liberation Sans, else DejaVu Sans, or the file `TDRAW_FONT` names; cmap format 4, simple glyphs) and fills the outlines with a scanline rasterizer at 4 x 4 subsamples a pixel.
  - Without OCR or without a font, the tests say so and are skipped.

## Known gaps and next steps

1. **macOS build:** done on an Intel Mac (above). The arm64 half needs an Apple Silicon Mac or a CI runner to be tested.
2. **OCR:** macOS has Apple Vision and Linux has Tesseract (above), and glyph outlines are drawn without GDI. Still to do:
   - **Poor scans:** tune the readers for Vision's and Tesseract's output, where both still disagree with Windows OCR (mostly Cameron's ERM scans).
3. **UI:** a wxWidgets prototype of one window (the voter list grid) to settle the toolkit before the rest.
4. **CI:** GitHub Actions `ubuntu-24.04` and `macos-14` jobs running `ctest`.
5. **`.gitattributes`:** the repo has none.
   - Today the index is LF and Windows checkouts get CRLF from `core.autocrlf`.
   - An explicit `* text=auto` (with `*.sh eol=lf`) keeps macOS and Linux checkouts right whatever a machine's git settings are.
