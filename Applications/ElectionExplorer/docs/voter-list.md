# Voter list feature

## Overview

ElectionExplorer can load county / SOS-style registered-voter exports and
display them in a high-performance grid.

## User interface

| Action | How |
|--------|-----|
| Open a list | **File → Open Voter List…** (or `Ctrl+O`) |
| Formats | `.csv` (comma) or `.txt` (tab); delimiter also sniffed from the header |
| Progress | Modal progress window with percent complete and **Cancel** |
| Select | Click, **Ctrl+click**, and **Shift+click** to select one or more rows in either pane |
| Copy | **Edit → Copy** (`Ctrl+C`) or right-click the selection; uses the file delimiter |
| Copy option | **Edit → Options…** — **Pre-pend normalized data for copies** (default on) adds **Voter ID** and **Name** before the original columns |
| Grid | Virtual list (owner-data) with **grid lines**, sized for hundreds of thousands to millions of rows |
| Column headers | **Bold** captions, **light grey** background, **double underline** rule |
| Frozen columns | **Voter ID** and **Name** stay visible while the rest of the grid scrolls horizontally |
| Resize frozen pane | Drag the **vertical splitter** between the left and right panes (`↔` cursor) |
| Voter ID alignment | **Voter ID** header and cell text are **center**-aligned; other columns are left-aligned |
| Sort | Click a column header to toggle ascending / descending (arrow in header) |

Loading runs on a **background thread** so the UI stays responsive. Progress
reaches **100%** only after the grid has been populated and the app is ready
for input.

### Dual-pane layout

The grid uses two synchronized virtual list views:

1. **Left (frozen)** — Voter ID + Name  
2. **Right (scrollable)** — all remaining source columns  

Vertical scroll position and row selection stay in sync. When only one pane
shows a horizontal scrollbar, the other pane’s height is adjusted (with a
matching pad) so the last data row is not covered by the bar.

## Display columns

1. **Voter ID** — synthesized from the best available ID column (`VUID`,
   `VUIDNO`, `SOS_VoterID`, etc.; falls back to generic `ID` / legacy IDs).
2. **Name** — from a full-name field when present, otherwise composed from
   prefix / first / middle / last / suffix parts (`FSTNAM`, `LSTNAM`, …).
3. **All original file columns** — in file order, using the source headers.
   Historical Travis-style files with per-election `VOTED` / `PLACE` / `PARTY`
   fields are included (hundreds of columns). The loader accepts up to
   `EE_MAX_COLUMNS` display columns (1024, including Voter ID and Name).

## DPI

The process is **Per-Monitor V2** DPI aware (manifest +
`SetProcessDpiAwarenessContext`). Fonts, padding, default column widths,
splitter metrics, and window layout scale with `WM_DPICHANGED`. The frozen
pane width is preserved across DPI changes (scaled, not reset).

## Implementation notes

| Area | Location |
|------|----------|
| UI, menus, progress, dual grid, splitter, DPI | `src/main.c` |
| Load, parse, pool storage, sort | `src/voter_table.c` / `voter_table.h` |
| Command / message IDs | `src/resource.h` |

Cell text is stored in a shared **UTF-8** pool with `uint32_t` offsets. Display
converts to UTF-16 on demand in `LVN_GETDISPINFO` (and custom-draw for centered
Voter ID).

## Sample files

- `test/sample_voters.csv` — **Travis County** style headers (comma-delimited)  
- `test/sample_voters.txt` — **Dallas County** style headers (tab-delimited)  
- `test/smoke_load.c` — optional console smoke test for the loader API  

See `test/README.md`. Full registration files for local testing:

```text
C:\Library\Elections\VoterLists
```

(Typical layout: `Travis\`, `Dallas\`, and similar county folders.)

## Memory notes

Peak memory grows with (rows × columns) for the offset matrix plus total text
size. Very wide multi-million-row files may require a 64-bit process with ample
RAM (prefer the **x64** build).
