# Window management — design (Election Explorer 1.7)

Status: **implemented** (2026-11-18). The **Window** menu and the **Windows…** dialog are in
`src/main.c` ("Window list" section, before `wWinMain`). Built clean for x64 and ARM64 Release;
driven end to end against the running app (see "How it was checked").

## Goal

A working session routinely has 8–10 or more Election Explorer windows open — voter lists, voter
rosters, cast vote records, election results, audit logs — plus the report and comparison windows
those open. Windows are deliberately independent top-level windows (not MDI children, not tabs),
so the only ways to reach one were the taskbar and Alt+Tab, neither of which says which file a
window holds.

This feature gives every window a **Window** menu that switches to any other window by number,
and a resizable **Windows…** dialog that lists them with the file behind each one, so a window can
be found by name, folder, type, size, record count or modified time and then activated or closed.

Notepad++'s Window menu is the model; the difference is that Election Explorer has windows rather
than tabs, so the list also carries each window's file facts.

## What counts as a window

Membership is **read from the live windows**, not kept in a registry of its own: this thread's
top-level windows are walked in Z-order and each is recognized by its window class. That stays
correct however a window was opened or closed, with no bookkeeping to forget at a new call site.

| Class | Type shown | File it reports | Records it reports |
|---|---|---|---|
| `ElectionExplorerMainWindow` (`is_roster` false) | Voter List | `AppState.load_path` | `table.row_count` |
| `ElectionExplorerMainWindow` (`is_roster` true) | Voter Roster | `AppState.load_path` | `table.row_count` |
| `ElectionExplorerCvr` | Cast Vote Record | `CvrWindow.load_path` | `table.nrows` |
| `ElectionExplorerResults` | Election Results | `ResultsWindow.load_path` | `nrows` |
| `ElectionExplorerAudit` | Audit Logs | `AuditWindow.load_path` | `nrows` |
| `ElectionExplorerReport`, `…CvrReport`, `…CvrValueReport`, `…CvrRcv`, `…RosterTotals`, `…ResultsTurnout`, `…AuditReport` | Report | its owner window's file | its own row count |
| `ElectionExplorerCompare`, `…Diff`, `…ResultsCompare` | Comparison | file A's (none for Results Compare) | rows compared |

Deliberately **not** listed: the load progress window, the Filter and Options windows, the
Windows… dialog itself, and the modal help / about / sheet-picker dialogs. They are transient and
would churn the list.

`CvrWindow`, `ResultsWindow` and `AuditWindow` gained a `load_path[MAX_PATH]` field, set beside the
existing `base_name` at creation; every other window already had a path or an owner to ask.

## Order and numbering

The Z-order walk (`GetTopWindow(NULL)` then `GW_HWNDNEXT`) delivers the windows
**most-recently-used first**: the active window is always first, the one before it second, and
minimized windows sink to the end. That is the order the Window menu numbers and the order the
dialog opens in.

The numbers are **positions, not identities**: they always run from 1 with no gaps, so closing a
window renumbers the rest the next time the menu opens. Numbers 1–9 take the digit as their
mnemonic; the tenth is drawn `1&0:` so its mnemonic is `0`. An ampersand in a file name is doubled
so it draws as one.

## The Window menu

Inserted between **Compare** and **Help** in all five menu bars that have those (voter list, voter
roster, CVR, election results, audit logs); the Results Compare window has neither, so **Window**
goes last there. Rebuilt on every `WM_INITMENUPOPUP`:

```
&Windows…
--------------------
&1: <title>              (the current window, check-marked)
&2: <title>
…up to 10, then "(N more — choose Windows…)"
--------------------
&Cascade
Tile &Horizontally
Tile &Vertically
Mi&nimize All
--------------------
Close All <Type> Windows
Close All &But This
```

The popup is found by content, not position: its first item is always `IDM_WINDOW_LIST`
(`EeWin_IsWindowPopup`), so the menu bars stay free to differ. Each window proc offers the whole
feature with two lines — one in `WM_INITMENUPOPUP`, one in `WM_COMMAND` — which keeps the footprint
in five long window procs to ten lines in total.

**Help → Managing Windows** was added to the same five Help menus and is handled in the same place.

Cascade and the tiles use `CascadeWindows` / `TileWindows` over the work area of the monitor
holding the current window. A window the user minimized stays minimized.

## The Windows… dialog

A modeless, resizable window of its own class (`ElectionExplorerWindowList`), one at a time,
**owned by the window Windows… was chosen in** — so it closes with that window, which is what the
user expects when they close that window from the list. It is registered and styled like the
Filter window (`WS_THICKFRAME`, `WS_EX_CONTROLPARENT`), and the message loop routes dialog keys to
it through `App_RouteDialogMessage`, so Tab, Esc and Enter work.

Columns: **Name**, **Path**, **Type**, **Size**, **Records**, **Modified**.

- **Name** is the window title. A voter list titles itself `Election Explorer — <full path>`, which
  alone would fill the column and repeat the Path column, so the list shortens the window's own
  full path to its file name (`EeWin_ShortTitle`). The title bar is untouched; every other window
  already titles itself by file name.
- **Path** is the folder holding the window's first (or only) file, as specified.
- **Size** and **Modified** come from `GetFileAttributesExW` on that file; both are blank for a
  window with no file (an empty voter window, a Results Compare window).
- **Records** is what the window itself reports in its status bar — voters, ballot records, result
  rows, audit events, report rows. It is the one column that is about the data rather than the file.

Behavior:

| Action | Result |
|---|---|
| **Activate** (or double-click a row) | Brings that window to the front, restoring it if minimized, and closes the dialog |
| **Close window(s)** | Closes every selected window and leaves the dialog open. More than one asks first. Closing the window the dialog was opened from closes the dialog with it |
| **OK** | Closes the dialog |
| Selection | Activate needs exactly one row; Close window(s) needs at least one. Both are grey until then |
| Column heading | Sorts by that column, then reverses; no arrow means most-recently-used order |
| **Find** box | Narrows the rows by name, folder or type as you type |
| Right-click a row | Activate, Close window(s), Close All \<Type\> Windows, Close All But This, Copy Path |

The dialog's size, position and column widths persist in the registry (`settings.{c,h}`:
`winlist_x/y/w/h` and `winlist_cols[6]`, values `WindowListX…`, `WindowListCol0…5`). A saved
position whose monitor has gone away falls back to centering on the opener.

While the dialog is open it re-reads the windows when it is activated and on a 1-second timer. The
timer collects **handles only** (`EeWin_CollectHandles`), so an idle dialog does no file I/O; a full
re-read happens only when a window has opened or closed. The selection is kept on the same windows
across a re-read.

## Decisions

| # | Decision | Why |
|---|---|---|
| D1 | List the report and comparison windows too, as types **Report** and **Comparison** | They are real top-level windows that get lost behind others; the user asked for them |
| D2 | Both a **Size** (file bytes) and a **Records** column | Size/Modified describe the file, Records describes what is loaded; the user asked for both |
| D3 | Cap the menu at **10** numbered windows, then point at Windows… | Keeps the menu one screen tall; Notepad++ does the same |
| D4 | Order **most recently used** (Z-order), not creation order | With a cap of 10, the windows you are moving between are the ones that must be in the menu |
| D5 | Membership by **window class**, not a registration table | No call site to forget; correct by construction |
| D6 | The dialog is **owned by its opener** | Gives "close that window and the dialog goes with it" for free, and keeps one dialog per session |
| D7 | Shorten a window's own path inside **Name** | A voter list's title is otherwise an unreadable menu item; Path still shows the folder |
| D8 | Closing more than one window **asks first**; one does not | Closing several at once is the only part that is hard to undo (a file has to be re-loaded) |

## How it was checked

Automated, against the running Release x64 build, by reading its menus and controls from another
process and posting the commands the real menu posts. The three PowerShell harnesses
(`verify_window_menu.ps1`, `verify_dialog_rows.ps1`, `verify_close.ps1`) were written in the
session scratchpad and are not in the repo; they could be kept under `test/` if UI checks of this
kind are wanted again:

- menu bar reads `File | Edit | Filter | Reports | Compare | Window | Help` — Window between
  Compare and Help, at position 5;
- the rebuilt Window popup reads `&Windows…`, separator, `&1: Election Explorer` (check-marked),
  separator, Cascade / Tile Horizontally / Tile Vertically / Minimize All, separator,
  `Close All Voter List Windows`, `Close All But This` (greyed with one window open);
- Help menu reads `… | &Compare | &Managing Windows | - | &About Election Explorer…`;
- with a voter list, a voter roster, a Cast Vote Records window, an election results window and a
  precinct report open, the dialog listed all five with the right Type, and Path / Size / Modified
  matching `Get-Item` on each file (318 bytes and 2026-11-05 15:14 for the Angelina roster, 703 KB
  for the DeKalb workbook, 6 KB for the San Francisco summary, 144 KB for the sample voter list);
- Records matched each window's own status bar (1,526 voters for the voter list; 0 for the DeKalb
  window, which is what that window itself reports — the workbook used for the test is a ballot
  listing, not a CVR, so there was nothing for the CVR reader to load);
- Find `large` narrowed 5 rows to 1 and clearing it restored 5;
- Activate and Close window(s) were grey with no selection and enabled with one row;
- Close window(s) on one row closed that window and left the dialog open (5 rows → 4);
- Close window(s) on two rows asked
  "Close 2 Election Explorer windows? Their loaded files are not changed; reopening a window loads
  its file again." and closed both on Yes;
- closing the window the dialog was opened from closed the dialog with it, and the application then
  exited with no windows left;
- Cascade, Tile Horizontally, Tile Vertically and Minimize All moved / minimized the window, and the
  numbered menu item restored and activated it;
- the registry round-trip is covered by `test/smoke_load.c` `test_settings_roundtrip`, extended for
  the placement and the six column widths; the whole smoke suite passes.

## Possible next steps

- **Ctrl+Tab / Ctrl+Shift+Tab** to step through the windows in most-recently-used order without
  opening a menu.
- A **Records** figure for a window that is still loading (it reads 0 until the load finishes).
- Remember the dialog's **sort column** along with its size, if it turns out people re-sort every
  time.
