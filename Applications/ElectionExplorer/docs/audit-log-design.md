# ES&S audit logs — design (Election Explorer 1.6)

Status: **steps 1–3 done** (2026-10-28): the engine `src/ee_audit.{c,h}` reads, merges and
replays the logs, with no UI yet. The user started implementation on the proposed decisions
D1–D10 (at the end). See "Implementation status".

## Goal

ES&S Electionware keeps two audit logs and prints each as an "Audit Events Report" PDF:

- the **Election** audit log: everything done to one election's database (ballot layout,
  media packaging, results loading, adjudication, reports);
- the **Admin** audit log: everything done to the system (logins, election open/close, backups,
  restores, election deletes, user accounts).

Counties print each log several times during an election (before early voting, after early
voting, election night, canvass). Election Explorer 1.6 adds an **Audit Log** window, like the
voter list, roster, CVR and results windows, that:

1. loads all the copies of an election's logs at once and shows the events once, merged;
2. **proves that the last copy contains every event of the earlier copies** (or names the ones
   it lacks);
3. **rebuilds the number of ballots loaded into the EMS** from the batch events, and checks it
   against an open CVR window and an open Election Results window. This matters most with
   multi-sheet ballots (Dallas G24), where CVR rows count sheets and the EMS counts ballots.

## What the counties provided (surveyed 2026-10-28)

`Audit_Logs/` (gitignored): 68 PDFs, 356 MB, 82,633 pages, about 1.75 million event rows.

| County | Election | Election log copies | Admin log copies | Final election log |
|---|---|---|---|---|
| Dallas | P24 | 1 | 1 | 3,855 pages, 84,263 events |
| Dallas | G24 | 5 | 5 | 4,872 pages, 102,906 events |
| Travis | P24 | 1 | 1 | 4,884 pages, 104,960 events |
| Travis | G24 | 3 | 3 | 4,848 pages, 103,105 events |
| Travis | GR24 | 3 | 3 | 420 pages, 8,449 events |
| Travis | L25 | 3 | 3 | 983 pages, 20,098 events |
| Travis | G25 | 3 | 3 | 1,847 pages, 38,783 events |
| Travis | GR25 | 3 | 3 | 230 pages, 4,616 events |
| Travis | P26 | 4 | 4 | 6,197 pages, 134,291 events |
| Travis | L26 | 3 | 3 | 681 pages, 13,865 events |
| Travis | PR26 | 5 | 5 | 1,422 pages, 29,804 events |

The file names are the counties' own and are not reliable: Dallas's
`G24_Final Election Audit Log.pdf` is an **Admin** log. The report title on page 1 is what
counts.

### The PDF layout

Every file is the same JasperReports template (Creator `JasperReports (AuditEventsReport)`,
Producer `iText 2.1.7`). They are text PDFs, so no OCR is needed.

- **Landscape page, printed rotated.** The text runs come out turned, so the reader must use
  `EePdfTextRun.rot`, as the results PDFs already do.
- **Header on every page:**
  - the title (`Election Audit Events Report` / `Admin Audit Events Report`);
  - the print date and time (`06/03/2026 11:53AM`);
  - the customer (`Travis County`, `Dallas`, `Dallas County`);
  - the open election's name (`May 26, 2026 Joint`; it is cut at the field width);
  - the election date (`May 26, 2026`).
- **Column headings:** Event | Description | Timestamp | User Id | Module.
- **Footer:** `Page N of M` and `<title> - <print time>`.
- **One row per event, newest first:**
  - **Event** is the level: Info, Warning, Error, Fatal or Verbose.
  - **Description** is free text wrapped onto several lines.
  - **Timestamp** is `MM/DD/YYYY hh:mm:ssAM`, local time to the second.
  - **User Id** is the user or workstation account (`dgrant`, `CCS1`).
  - **Module** is Home, Setup, Configure, Package, Reporting, Paper Ballot, and so on.
- **The parser must place text by column position**, not by text order:
  - "Info" is its own run in the Event column. "Warning", "Verbose" and "Error" are drawn as one
    run with the description, so the level has to be split off.
  - A description can share a run with its timestamp
    (`... Number of Ballots 117 11/13/2024 02:50:54PM`).
  - The Module column wraps (`Paper Ballot` / `Application`, `ElectionWare Suite`).
  - **A row can break across pages.** Its last description lines, or its module's second line,
    land at the top of the next page above the first timestamp, and must be joined to the
    previous page's last event. A very long description (Ballot Review filters listing every
    contest) can fill a whole page that has no timestamp at all.
  - A naive text-order parse loses or mangles about 1 event in 1,000. Every case is a row cut at
    a page end.
- **Same-second events are common** (many per second while loading). Their order in the file is
  the only ordering, so the merge keeps it.

### Copies of one log

The research extraction is position-based, with page-break joining. With it, each copy was
checked against every later copy of the same log, matching on timestamp, level, user and
description (whitespace ignored; a description cut at a page end matches its full form):

- **Travis, all 9 elections, both logs:** every later copy contains every event of every earlier
  copy (0 missing).
- **Dallas G24:**
  - The Final election log contains every event of the four earlier copies (0 missing).
  - The After-EV and 11-14 copies were printed **with a start date**: they hold nothing before
    09/18/2024 08:30. So they "lack" 38,920 events that Before-EV and Election-Night have.
  - The same holds for the Admin logs (53 events, all on 09/17).
  - The check must tell "outside this copy's date range" apart from "missing".
- **Travis PR26 6/3/26:** this copy was printed from `PR26 Canvass CC 3rd Tab Test`, the copy of
  the election restored for the post-canvass L&A test (see "Logic and accuracy tests"), so its
  header names that election. It still contains every event of the four earlier copies, because
  a restored election carries its audit log. Its last events are the test itself. The window must
  group by county and election date and say when the names differ.
- **Clock anomalies are real and worth showing.** In Dallas G24, 189 events from user CCS1 on
  11/02/2024 are stamped exactly one hour ahead of their neighbours (a workstation clock off by
  an hour), so the newest-first order breaks there. Travis PR26 has one such event.
  DST fall-back nights repeat an hour.

### Ballot accounting from the Election log

These Reporting-module events carry the ballot count:

| Event | Meaning |
|---|---|
| `Committing new Batch <name> ID <n> Number of Ballots <k>` | A batch of k ballots loaded; ID is the EMS batch ID (unique) |
| `Updating Batch <name> ID <n> Number of Ballots <k> Revision <r>` | Manual results entry (MRE) batch revised (Travis P24 testing) |
| `All Electionware election results will be cleared…` + `User Response: Yes` + `Results successfully cleared` | **Full clear**: every batch gone |
| `Electionware election results for Central Count Media-<m> will be cleared…` + `Results successfully cleared` | **Media clear**: the batches from media m gone |
| `This batch was previously loaded. Results [and Ballot Images] have not changed.` / `These Results were previously loaded… Batch <name> Media ID <m> Machine SN <sn>` | Reload attempt, no change |
| `An error occurred while loading Ballot Sheet ID <n>` | Sheet-level load error |
| `N batch(es) found. Processing Central Count device type.`, `Starting/Finished Load of Results <drive>` | Load framing |

- **Batch names** are `<poll place> - <media ID>`, with an `A0009-`-style prefix for
  central-count batches:
  - Dallas: `A0013-EV Vote Center - 33`, `A0005-EV Mail - 62`, `ED Vote Center - 196`;
  - Travis: `Travis County Early Vote Center - 12`, `Vote By Mail - 3`, `EV_PfISD Rock Gym - 4`.
- **Media IDs are numbered per poll place.** Dallas has an EV Vote Center media 86 and an EV Mail
  media 86. The media-clear warning names only the number.
- **Logic and accuracy (L&A) test decks** are loaded into the election database and then
  cleared (see the next section). Their poll place names say so: `Logic EV`, `Logic By Mail`,
  `LA-Mail`, `LA ED`, `Logic/Acurracy-EV`.
- **Replay:** walk the events oldest first; a full clear starts a new **session**. In each
  session, keep the batches committed and not later removed by a media clear. A media clear
  removes that media's batches of the poll place whose batches are loaded next. The **election
  session** is the last session that loaded non-test batches. A test session after it is the
  post-count L&A test, not a loss of results.

Validation of the replay, using the latest copy of each election log:

| Election | Election session start | Live batches | EMS ballots | CVR | Results ballots cast |
|---|---|---|---|---|---|
| Travis G24 | 11/02/2024 11:57 AM | 314 (1 media clear: EV Provisional media 1, 5 batches, 709 ballots) | 587,090 | 587,090 | 587,090 |
| Travis P24 | 03/02/2024 09:39 AM | 208 | 153,590 | (REP only) | 153,590 |
| Travis GR24 | 12/11/2024 10:54 AM | 86 | 20,606 | — | 20,606 |
| Travis L25 | 04/30/2025 10:40 AM | 45 | 17,769 | — | 17,769 |
| Travis G25 | 11/01/2025 10:48 AM | 228 | 227,938 | — | 227,938 |
| Travis GR25 | 12/10/2025 09:50 AM | 15 | 7,212 | 7,212 | 7,212 |
| Travis P26 | 02/28/2026 11:07 AM | 257 | 274,443 | 274,443 | 274,443 |
| Travis L26 | 04/29/2026 11:01 AM | 37 | 12,710 | 12,710 | 12,710 |
| Travis PR26 | 05/23/2026 10:52 AM | 270 | 97,460 | 97,460 | 97,460 |
| Dallas P24 | 03/04/2024 09:56 AM | 1,145 | **231,464** | 231,465 | 231,465 (DEM 126,055 + REP 105,410) |
| Dallas L24 (found by the county survey) | 05/03/2024 09:48 AM | 1,081 (1,087 commits; 3 media clears) | 83,030 | — | 83,030 |
| Dallas G24 | 11/02/2024 11:27 AM | 3,934 (4,383 commits; 4 media clears on 11/14) | **858,562** | 873,455 rows = 858,562 ballots + 14,893 continuation sheets | 858,562 |

- **"Number of Ballots" counts ballots, not sheets.**
  - Dallas G24 used multi-sheet ballots. Its EMS total equals the official ballots cast and the
    CVR's page-1 rows, not the CVR's 873,455 rows.
  - Comparing the audit log with the CVR row count would show a false 14,893-ballot gap. The
    compare must use the CVR's ballot count (rows that carry a page-1 contest; see
    `EeCvr_HasMultiCard`).
- **The Dallas G24 media clears decide the answer.** They cleared Central Count Media 86, 24, 29
  and 67, then reloaded 47,993 ballots. The total depends on what each clear removed:

  | What each media clear removes | Ballots |
  |---|---|
  | nothing | 906,179 |
  | every central-count batch with that media number (EV Vote Center and EV Mail) | 857,597 |
  | the EV Vote Center batches of that media (the poll place reloaded next) | **858,562 = official** |

  The third rule also keeps EV Mail at or above the mail votes for President, which the second
  rule breaks.
- **Dallas P24 is one ballot short** (231,464 vs 231,465). No commit is malformed, and the
  election session has no media clear. This is open: a step 3 task.
- **Notable events found while checking:**
  - **Dallas G24:** 4,509 "error occurred while loading Ballot Sheet" events on 592 distinct
    sheet IDs. They are repeated at the start of later loads, on 11/04–11/14.
  - **Dallas G24 and P24** end with an L&A deck loaded in the election database after the
    canvass (11/22/2024; 03/11/2024 20:37). This is the post-count test, below.

### Logic and accuracy tests

Texas requires these tests, and the logs record them, so the window reports them.

- **The law:**
  - **Election Code §129.023:** a public logic and accuracy test, with public notice. This is the
    first test. Each election's test is held before mail ballots go to voters.
  - **Election Code §127.093:** the central counting station equipment is tested
    - (b) first, with the §129.023 public test;
    - (c) "immediately before each time the counting of ballots with the equipment begins";
    - (d) "immediately after each time the counting of ballots with the equipment is completed."
  - The "each time" wording came with SB 2166 (89th Legislature, 2025). Before that, §127.093
    called for three tests per election: one at least 48 hours before counting, one immediately
    before counting began, and one immediately after it was completed. The 2024 logs (Travis
    P24, G24, GR24; Dallas P24, G24) were under the older text.
- **Travis practice (from the user, 2026-10-28):**
  - an L&A test before any results are loaded into the EMS each counting day, and again when
    finished;
  - for G26: Saturday morning before scanning mail ballots and loading the early-voting result
    sticks, and Saturday when finished; Election Day before any EMS activity and at the end of
    the night; the same for the final canvass.
- **What the logs show for Travis** (PR26 traced in full; the other elections follow the same
  pattern):
  - **The public test:** test sessions in the election log weeks before the election (PR26:
    04/06/2026).
  - **Before counting:**
    - **First counting day:** a test session in the election database, then a full clear, just
      before the first load (PR26: test 05/23 10:48, clear 10:52, mail loads 13:33).
    - **Later days:** the Admin log shows the previous post-count test copy opened before loading
      begins (PR26: `PR26 EV CC 3rd Tab Test` opened 05/26 12:15, early-voting sticks loaded from
      12:25; `PR26 ED CC 3rd Tab Test` opened 06/03 10:48, canvass loads from 11:29).
  - **After counting:** the Admin log shows a backup of the election, then a copy restored from
    WORKSTATION-11 as `<election> EV CC 3rd Tab Test` / `ED CC 3rd Tab Test` /
    `Canvass CC 3rd Tab Test` and opened (PR26: 05/23 14:13, 05/27 00:21, 06/03 11:40).
  - **The test copies' own logs:** the tests run in those copies, and their Election logs are
    separate. They are not in these PDFs, except the 6/3 PR26 copy, which was printed from the
    Canvass test copy.
  - **Deleting a test copy deletes its log.** Electionware warns "Deleting the … election will
    also delete all the audit logs related to this election". The Admin log records each delete.
- **What the logs show for Dallas (2024):**
  - All tests ran in the election database itself: the 09/18–10/17 test sessions, the full clear
    on 11/02 before counting, and the post-canvass test on 11/22. That matches the three tests
    per election of the 2024 text.
  - Dallas's database therefore ends holding the 1,791-ballot test deck. The election's count is
    the earlier session.

### Results and CVR cross-checks available

- **Total ballots:** EMS ↔ results `ballotsCast` (Clarity XML/summary; Electionware PDF
  Statistics) ↔ CVR ballots. Exact.
- **By vote type, as a lower bound only:**
  - Batch poll place names map to vote types: Early Vote Center / EV Vote Center → early
    in-person; Vote Center / ED → Election Day; Mail / By Mail → mail; Provisional → provisional
    (EV / ED when named).
  - Clarity reports votes by vote type but not ballots by vote type. So the check is: ballots of
    a vote type ≥ the votes in any Vote-For-1 contest of that vote type, plus its over/under
    votes when reported.
  - Dallas G24 with the right media-clear rule: EV in-person 636,880 ≥ 633,355; mail 31,468 ≥
    31,206 (31,209 in U.S. Senator, which step 8 uses); Election Day 188,811 ≥ 187,379; provisional 1,403 ≥ 1,396 (President votes).
- **Not possible:** batch-level or precinct-level checks against the Travis and Dallas CVRs.
  Travis redacts Batch and Precinct; Dallas's CVR has no Batch column.

## User-visible design (proposed)

### Loading

- **File → Load Audit Logs…** in every primary window (voter list, roster, CVR, results, audit
  log).
- A multi-select open dialog for `*.pdf`.
- Each file is read on a worker thread, several files at once, behind the progress dialog with
  Cancel.
- Each file is recognized from its page-1 title. A PDF that is not an Electionware audit events
  report is refused, with its name given.
- **Grouping (D1):**
  - Files are grouped by county + election date. **One window per election** holds both of its
    logs (Election and Admin) and all their copies.
  - Copies are ordered by their print time, not by file name.
  - Differing election names within a group (Travis PR26 6/3) are noted in the Copies Check
    report.
- Text parsed from these PDFs never contains voter data. User IDs and names are county
  employees' accounts, kept as printed.

### The Audit Log window

A primary, standalone top-level window like the results window: its own `AppState`, counts
toward `App_MaybeQuit`, File → Close.

- **Events view (default):** one row per merged event, newest first, as printed (D3).

  | Timestamp | Log | Event | Module | User Id | Description | Copies |
  |---|---|---|---|---|---|---|

  - **Log:** Election / Admin.
  - **Copies (D2):** how many of the log's copies hold the event, as `3 of 5`, with the first
    copy that has it in a tooltip / status bar. An event the final copy lacks is marked (bold
    red, like the roster window's flags).
  - **View → Log → All / Election / Admin**, and **View → Copy → All copies merged / <each
    copy>** shows one copy exactly as printed.
- **Ballot Batches view:** one row per batch commit or update in the chosen session.

  | Committed | Batch ID | Poll Place | Media | Vote Type | Ballots | User Id | Status |
  |---|---|---|---|---|---|---|---|

  - **Status:** Counted / Removed by media clear at … / Replaced by update / L&A test deck.
- **Status bar:**
  - county, election and date;
  - copies loaded;
  - events shown of total;
  - the election session's EMS ballot total;
  - whether a filter is active;
  - a **"final copy lacks N events"** warning when N > 0.
- Sorting by header click, Reset View, Copy (Ctrl+C, right-click), Options (zoom), and Help
  work as in the other windows.

### Menus

- **File:** Load Voter List…, Load Cast Vote Records…, Load Voter Roster…, Load Election
  Results…, Load Audit Logs…; Close Audit Logs; Export…; Exit.
- **Edit:** Copy; Options… (zoom; the vote-type mapping of poll place names, D9).
- **Filter:** Filter… (Ctrl+L), Reset Filter, Save/Load filter file. Rules:
  - Event, Module, User Id, Log, Vote Type (Batches view): is / is not;
  - Description: contains / does not contain;
  - Timestamp: on or after / before.
  - Include/Exclude semantics, as in the CVR filter (D8).
- **Reports:**
  - **Copies Check:** one row per copy, in print order, with:
    - its file, print time, first and last event;
    - events, and new events since the previous copy;
    - the earlier copies' events it lacks, split into **outside its date range** and
      **missing**;
    - a **verdict** line: "The final Election log copy contains every event of the 4 earlier
      copies."
  - **Ballot Accounting:**
    - every session: start, end, why it started (full clear), commits, media clears, live
      batches, ballots, and L&A test or election;
    - the election session's totals by poll place and by vote type;
    - each media clear, with what it removed and the alternative readings when its poll place
      is ambiguous (D5);
    - Select Session… makes another session the one the Batches view and the checks use.
  - **L&A Testing Timeline (D10):** one row per test and per counting period, in time order.
    - **Counting period:** the batch loads between two consecutive tests. Each row also lists
      the pauses inside it: the election closed in the Admin log, or no loads for 2 hours or
      more. Travis's Election Day, for example, runs from the 12:15 test through the
      early-voting sticks, a five-hour pause, and the Election Day loads to the 00:21 test.
    - **Test:** an L&A test session in the election log (test-deck batches, then a full clear),
      or a test copy of the election (a name ending `Tab Test` / `Tabulation Test`, or with
      `L&A` / `Logic`) restored or opened in the Admin log.
    - **Each counting period shows:**
      - its first and last load, and its batches and ballots;
      - the last test found before it, and the first test found after it, with how long before
        or after each one was, and the log that shows it.
    - **The first test:** shown with its date and how many days before election day it was.
    - **Loads with no test found before or after them** are marked "No test found in these
      logs". The tests in a separate test copy appear only as Admin events. The window reports
      what the logs show; it does not judge compliance.
  - **Notable Events:** grouped counts with jump-to-event:
    - full and media clears;
    - ballot-sheet load errors (distinct sheet IDs), Error and Fatal events, previously-loaded
      batches, zero-ballot batches;
    - clock anomalies by user;
    - from the Admin log: restores, backups, election deletes (which delete that election's
      audit log), user-account changes, and failed restores.
  - **Users and Workstations:** events, first and last time, and modules, by User Id.
- **Compare (D6):** **Check Ballot Counts with `<window>`**, one item per open CVR window and
  results window, opens the **Ballot Reconciliation** window:

  | Measure | Audit log (EMS) | Other window | Difference | Status |
  |---|---|---|---|---|

  - **Rows:**
    - ballots;
    - CVR rows, CVR ballots, and continuation sheets (CVR);
    - ballots cast (results);
    - one lower-bound row per vote type (results with vote types).
  - **Status:** Match, Differs, Lower bound met, Lower bound not met, Not reported.
  - **Multi-sheet CVRs (D7):** the CVR side uses ballots (page-1 rows). The window explains a
    rows ≠ ballots gap as continuation sheets.
  - The CVR and results windows get a matching Compare → Check Ballot Counts with
    `<audit log window>`.
- **Help:** Loading Audit Logs, Copies Check, Ballot Accounting, L&A Testing, Filters, Reports,
  Compare, Export; About.

### Export

- UTF-8 CSV (default) or TSV with BOM, like every export, in the current view and filter.
- Events export columns:
  `Timestamp, Log, Event, Module, User Id, Description, Copies, First Copy, Page`.
  As built (step 9) the reloadable layout adds `Record Type`, `County` and `Election Date`.
- Batches export columns:
  `Committed, Batch ID, Poll Place, Media, Vote Type, Ballots, User Id, Status, Session`.
- Every report window exports its rows (Export Selected / Export All).
- The Events export reloads (detected by its header), so a large merged log can be saved and
  reopened without the PDFs. Copy membership is kept in the `Copies` / `First Copy` columns.

## Implementation status

### Steps 1–3 — the engine (done 2026-10-28)

`src/ee_audit.{c,h}` (in the project; smoke tests `audit`, `auditmerge`, `auditacct`). Release
x64 and ARM64 build with no warnings. An AddressSanitizer probe run over 23 PDFs is clean.

- **Reader (`EeAudit_LoadCopy`):**
  - `pdf_reader.c` gives these pages' runs turned 90° (`rot` 90): x runs down the page and y
    across. Each column starts at a fixed y, learned from the heading row of every page (Event 43,
    Description 95, Timestamp 439, User Id 574, Module 651).
  - Event levels are runs of their own in the Event column (unlike PyMuPDF's output).
  - Every run belongs to the row whose timestamp is level with it or above it.
  - Text above a page's first row is joined to the previous page's last row, column by column.
  - **JasperReports quirk:** when a row splits at a page end because another of its cells ran
    over (a wrapped module, "Paper Ballot" / "Application"), the next page prints the cells that
    fit again in full. Such a repeat is dropped, so "Read 112 Districts." does not double.
  - **Sweep:** all 70 Electionware PDFs read, 1,875,864 events, 39 s in all, at most 2.5 s a file
    (Travis P26, 6,197 pages).
  - Against the research extraction (68 files, 1,753,271 events), every timestamp, level, user and
    page agrees. The 206 differences are the reader being more complete: modules and long
    descriptions that ran onto the next page, which the research cut short.
  - Parker's Hart reports are refused with "is not an ES&S Electionware audit events report".
- **Merge (`EeAudit_LoadFiles` / `EeAudit_GroupCopies`):**
  - Copies are grouped by customer ("Dallas" = "Dallas County") and election date, and ordered by
    print time.
  - The merge starts from the last copy printed and merges the earlier ones into it. Events match
    on timestamp, level, user and description, ignoring whitespace; each copy's own order is kept,
    as are identical events in one second.
  - **Validated:**
    - every one of the 24 logs (12 elections × Election / Admin): the final copy holds every
      earlier event, **0 missing**;
    - Dallas G24's start-dated copies: 38,920 (Election) and 53 (Admin) events outside their date
      range;
    - the merged count equals the final copy's in every log.
- **Accounting (`EeAudit_Accounting`):** replays the merged Election log oldest first.
  - **The device type matters in a media clear.** "Central Count Media-24" removes only central
    count batches (`A0013-EV Vote Center - 24`), not a poll place tabulator's `EV Vote Center -
    24`, whose media are numbered apart.
  - The poll place is the next one loaded, looking past further media clears (Dallas cleared
    Media-86 and -24 back to back).
  - **Results:**
    - all 9 Travis elections, Dallas G24 (**858,562**) and Dallas L24 (83,030) equal the official
      ballots cast;
    - Dallas G24 by vote type: early voting in person 636,880, Election Day 188,811, mail 31,468,
      provisional 1,403.
  - **Dallas P24 is still one short** (231,464). Its election night has "Results validation
    error. Verify poll closed correctly. You may need to recollect these ballots." (CCount1 and
    CCount2), "Election is not closed or missing Poll Place collection file." and several
    "Media validation unsuccessful." events. These are a lead for the Notable Events report and
    are left open.
  - Dallas P24's Election Day batches are named by location ("Our Redeemer Lutheran Church") and
    read as vote type Other.

### Step 4 — the Audit Log window (done 2026-10-29)

`main.c`, section "Audit Log window (1.6)"; IDs in `resource.h`. Release x64 and ARM64 build with
no warnings.

- **Loading:** File → Load Audit Logs… in the voter list, roster, CVR, results and audit windows.
  `EeAudit_LoadFiles` runs on a worker thread behind a progress dialog with Cancel (up to 256
  files). Each election opens its own window. A file that is not an Electionware audit events
  report is refused by name (checked with a Parker Hart report).
- **Events view:** Timestamp, Log, Event, Module, User Id, Description, Copies ("3 of 5"), newest
  first; the two logs are interleaved by time.
  - Events the final copy lacks are drawn in bold red.
  - View → Log (All / Election / Admin) and View → Copy (All Copies Merged, or one copy as
    printed; the Log and Copies columns drop out there).
  - Header click sorts: Timestamp oldest first on the first click (printed order is newest first,
    and Reset View returns to it); Copies most first; text columns A–Z.
  - Edit → Copy / Ctrl+C / right-click copies rows as tab-separated text. Edit → Options… sets
    the zoom. Help: Loading Audit Logs, Views and Options, About.
- **Status bar, changed from the plan:** the county, election and date moved to the title bar,
  so the rest fits. Part 1 shows each log's copies, the events shown of the total, the election
  session's EMS ballots, and "final Election log copy lacks N events (M outside its date range)".
  Part 2 shows the focused event's first copy, file and page.
- The Log column is also hidden when only one log is loaded.
- **Checked in the app (driven through UI Automation, with screenshots):**
  - Travis GR25 (6 files): 5,036 events, EMS ballots 7,212.
  - Travis G25 + GR25 (12 files) open two windows.
  - Dallas G24 "before EV" + the start-dated "After EV" copy: "lacks 38,920 events (38,920
    outside its date range)", in red when sorted oldest first.
- **Not in step 4:** Filter, Reports, Compare and Export menus (steps 5–9).

### Step 5 — Ballot Batches, Ballot Accounting and Copies Check (done 2026-10-29)

`main.c` (same section). Engine: `EeAuditBatch.clear` names the media clear that removed a batch
(smoke test `auditacct`). Release x64 and ARM64 build with no warnings.

- **View → Events / Ballot Batches.** The Batches view lists one session's commits in load order.
  - Columns: Committed, Batch ID (with the revision of a manual update), Batch Name, Poll Place,
    Media ("67 (central)"), Vote Type, Ballots, User Id, Status. Status reads "Counted", "Removed
    by media clear at …", or "Replaced: its batch ID was committed again"; a test deck reads "L&A
    test deck — …". The plan's table had no Batch Name column; it was added so a row can be
    matched with its commit text.
  - Batches that no longer count are grey.
  - Double-click or right-click → Go to Event shows the commit, selected, in the merged Events view.
  - Status bar: the session ("Session 14 of 16 (Election session), start to end"), batches,
    counted, EMS ballots and media clears; part 2 shows the commit's first copy, file and page.
- **Reports → Ballot Accounting…** is a separate window. Its summary lines give:
  - the sessions;
  - the election session's totals by vote type;
  - the media clears' other readings;
  - the "previously loaded" and sheet-error counts;
  - a reminder that ballots are not sheets.
  - **Tabs:**
    - **Sessions:** start, end, started by, batches, counted, ballots, media clears, kind
      (Election session / L&A test / Other (not test decks) / No batches; "— shown" marks the
      session in use).
    - **Poll Places** and **Vote Types:** for the session shown, with a total row kept last when
      sorted.
    - **Media Clears:** for the session shown. Each clear lists the poll place it removed (or
      "not known: nothing removed"), and the session totals if it had removed nothing or every poll
      place on its media (D5).
  - Double-click a session, or right-click → Use This Session, to switch the Batches view and the
    tabs to it. **Reports → Select Session…** opens the Sessions tab.
- **Reports → Copies Check…:** one row per copy in print order. Columns: log, copy, file, print
  time, the election its header names, first and last event, events, new events, lacks outside
  its span, lacks missing. Missing events are red; the final copy is bold.
  - Summary lines give the verdict per log ("contains every event of the 4 earlier copies"; or "lacks
    N events …, all outside its own time span …, so none is missing"; or the split).
  - A summary line also flags headers that name more than one election.
- Help → Reports describes both reports.
- **Checked in the app** (UI Automation and screenshots):
  - **Dallas G24, all 10 files:** session 14 of 16 is the election session, with 858,562 ballots
    in 3,934 counted batches. By vote type: early voting 636,880, Election Day 188,811, mail 31,468,
    provisional 1,403.
    - Four central-count media clears removed 449 batches (14,593 + 3,088 + 12,265 + 17,671).
    - The other readings are 906,179 (if the clears removed nothing) and 857,597 (if each removed
      every poll place on its media). These are the numbers this plan predicted.
    - Copies Check: both final copies hold every earlier event.
    - Switching to session 12 shows its 5 test-deck batches.
    - Status sort puts the removed batches first, in grey.
    - Go to Event selects the commit.
  - **Dallas G24, "before EV" + "After EV":** "lacks 38,920 events …, all outside its own time span".
  - **Travis PR26 (10 files):**
    - 97,460 ballots in session 31 of 32, with 27 L&A test sessions.
    - Copies Check notes the final copies' headers name "PR26 Canvass CC 3rd", the restored test
      copy.
  - File → Exit closes the report windows with the audit window.

### Step 6 — Filter (done 2026-10-29)

`main.c`. Release x64 and ARM64 build with no warnings.

- **Filter → Filter… (Ctrl+L) / Reset Filter.** It uses the rule window shared with the CVR and
  results windows. That window gained optional host hooks in `CvrFilterState`:
  - `relations`: the relations a column offers;
  - `relation_text`: their labels;
  - `validate`: a check of a rule before it is added;
  - `relation_mask`: what a loaded filter file may use.
  - A column without "is" takes a typed value; any listed values are only suggestions.
  - The CVR and results windows pass no hooks and keep "is / is not". The Results filter was
    checked in the app.
- **Columns and relations (D8):**
  - Timestamp: on or after / before. A typed date (MM/DD/YYYY) or date and time (12- or 24-hour);
    also YYYY-MM-DD. The suggestions are the days that have events.
  - Log, Event, Module, User Id: is / is not.
  - Description: contains / does not contain (ASCII letters without case).
  - Vote Type: is / is not, in the Batches view only.
  - A bad time is refused with a message.
- **Semantics, changed from the plan:**
  - The CVR filter's rules hold for every column except Timestamp: Include rules on one column are
    alternatives; a matching Exclude hides the row; different columns all apply.
  - Timestamp Include rules all apply together, so "on or after 11/05/2024" plus "before
    11/06/2024" is one day. Treated as alternatives, they would match everything.
  - In the Batches view a batch is matched by its commit event (time, user, description) and its
    vote type. Vote Type rules are ignored in the Events view.
  - The reports always count everything.
- **Filter files:** Export / Load Filter in the rule window. The relations are saved under their
  standard names ("excludes", "more than", "less than"), so an audit filter file loads into
  another audit window.
- **Status bar:** "N of M events (filtered)"; in the Batches view, "filtered: N shown". The
  Batches status no longer shows the session's times; Ballot Accounting has them.
- **Checked in the app (UI Automation):**
  - **Dallas G24:** Description contains "cleared" with Timestamp on or after 11/14/2024 and
    before 11/15/2024 leaves 8 of 103,827 events, the four media clears' warnings and
    confirmations.
  - The time "notadate" is refused with the message.
  - **Batches view:** Vote Type is Mail plus Exclude User Id is not dgrant leaves 1,137 batches.

### Step 7 — L&A Testing Timeline, Notable Events, Users and Workstations (done 2026-10-29)

Engine (`ee_audit.{c,h}`, smoke test `auditnote`): `EeAudit_Notable`, `EeAudit_Timeline`,
`EeAudit_IsTestCopyName`. Window: three more report windows in `main.c`, plus a red-flag lead in the
status bar. Release x64 and ARM64 build with no warnings.

- **Red flag: results output before Election Day.** Added at the user's direction (2026-10-29),
  changing D10's "does not judge" for this one case.
  - **The user's account:** a county of 100,000 or more may start tabulating 3 days before Election
    Day, but results may not be printed before Election Day. On Election Day they may be printed,
    and the presiding judge keeps them confidential until the polls close. The legal citations are
    still to be confirmed.
  - **Rule:** an Election log event of the Reporting module before midnight starting Election Day,
    when the session since the last full clear holds batches (not test decks) with more than 0
    ballots. The events are:
    - "<report> was printed successfully.";
    - "<report or file> was saved successfully.";
    - "Updated Plain Text Export Results";
    - "Exporting Cast Vote Record 1 of …".
  - The same output with only test decks, or 0 ballots, loaded is information: L&A tests and zero
    reports, such as Travis P24's January manual-entry batches of 0 ballots.
  - **Found:**
    - **Travis L26:** 36. Early ballots (8,310) were tabulated 04/29; on 05/01, the day before
      Election Day, a canvass and a cumulative report were saved for every city, ISD and MUD
      (KlaasE).
    - **Dallas P24:** 1. An Election Summary was saved 03/04 with 12,187 ballots loaded.
    - **Dallas L24:** 1. Saved 05/03 with 786 ballots loaded.
    - **Travis L25:** 5. Two "EV Provisional" batches of 6 ballots, 04/11; probably a test deck not
      named as one.
    - **No red flag:** the other Travis elections and Dallas G24.
- **Another election's history.** The Election log records "Election Opened: <name>". Events while
  a name with a different election date was open are not checked.
  - Example: Travis's PR26 database began as a copy of P26 ("March 3, 2026 Joint Primary
    Election"), and its P26 canvass printouts of 03/11 would otherwise be 26 false red flags.
  - Undated names ("L26 Setup Election", "PR26 Prep Election") are the same database being set up,
    and are checked.
- **Notable Events window:**
  - Categories tab (severity, count, first, last, meaning). Double-click a category to see its
    events.
  - Events tab (severity, category, time, log, user, description, detail). Double-click an event to
    see it, selected, in the Events view.
  - Categories, in order:
    - **red flag:** results output before Election Day;
    - **review:**
      - ballots loaded more than 3 days early;
      - elections deleted;
      - failed restores and backups;
      - results validation errors;
      - other Error and Fatal events;
      - ballot sheet load errors;
      - media clears;
      - clock anomalies (an event later than the one printed above it, by more than 60 s);
    - **information:**
      - full clears;
      - zero-ballot batches;
      - previously loaded batches;
      - results output during testing;
      - restores;
      - user accounts.
- **L&A Testing Timeline window (D10):** the tests and the counting periods in time order.
  - **Tests:**
    - Election log test sessions (test decks only).
    - Admin log test copies restored or opened ("Test", "L&A", "Logic" in the name).
    - The Admin log covers the whole system, so a test copy counts only when one of its restores
      follows a backup of this election within 2 hours. The backup's path names the election.
    - A restore and the open that follows it within 10 minutes are one test.
  - **Counting periods:** loads (not test decks) split at each test.
    - Each lists its pauses of 2 hours or more, marked "(election closed)" when the Admin log closes
      the election during one.
    - It also gives the nearest test before and after it, and is red when one is missing.
  - The summary gives the first test and its days before Election Day, and what was left out.
  - **Travis PR26 gives exactly the trace in step 7 of the plan:**
    - 05/23 mail loads 13:33–14:02, between the 10:49 test session and the 14:13 restore.
    - 05/26 12:25 to 05/27 00:11, with the 13:11–18:45 pause (election closed), between the 12:15
      open and the 00:21 restore.
    - 06/03 canvass 11:29, between the 10:48 open and the 11:40 restore.
    - 32 other elections' test copies and the 5 P26 loads are left out.
  - **Dallas:** in-database tests. One counting period runs 11/02–11/14 (G24), with the
    post-canvass test after it.
- **Users and Workstations window:** per user:
  - events in both logs;
  - first and last event;
  - modules;
  - workstations the events name ("… on WORKSTATION-11.", "from machine …").
- Every report row that stands for an event jumps to it (double-click or Go to Event). The Events
  view's filter is reported when it hides the event.

### Step 8 — Ballot Reconciliation (done 2026-10-29)

Engine (`ee_cvr.{c,h}`, smoke test `cvrmc` and the Hart test): `EeCvr_CardCounts`. Window: a sixth
audit report and Compare menus in `main.c`. Release x64 and ARM64 build with no warnings.

- **`EeCvr_CardCounts(t, rows, nrows, &out)`** gives rows, ballots (first cards), continuation
  sheets and the method:
  - **Sheet column** (Hart "Sheet Number", Dominion "Card"): a row is a ballot when the cell is empty
    or ≤ 1.
  - **Page one** (an ES&S CVR where `EeCvr_HasMultiCard` holds). Ballot styles are grouped without
    their " [2]" suffix. A style's *common contests* are its non-key contests present on ≥ 99% of its
    rows that have a top-of-ballot contest (President, U.S. Senator, Governor). A row is a first card
    when:
    - it has a top-of-ballot contest; or
    - it holds at least half of its style's common contests; or
    - its style has no row with a top-of-ballot contest (nothing is known of its cards).
  - **One per row** otherwise.
  - **Results** (scratchpad probe and the app):
    - Dallas G24's 9 files: 873,455 rows → 858,562 ballots + 14,893 continuation sheets.
    - Travis G24: 587,090 rows, 587,090 ballots. Dallas's BMD rows carry every contest, and its
      continuation rows hold only City of Dallas Props G–U.
    - Travis G20, GR25, L26, P26, PR26 and Dallas P24: one ballot per row.
    - Earlier rules failed: page-one alone gave 858,171; a strict common-contest intersection was
      exact for Dallas but 235 short for Travis.
- **Results ↔ CVR compare** now counts a multi-card CVR's ballots as its first cards. Before, it
  reported them as unknown. Dallas G24: Ballots Cast 858,562 = 858,562 (Match). The status bar notes
  "multi-card CVR: ballots are its first cards", and the CVR window's status bar gives "multi-card:
  N ballots on M sheets".
- **Compare menus:**
  - The audit window's Compare menu lists each open CVR and results window: "Check Ballot Counts
    with …".
  - The CVR and results windows' Compare menus list each audit window the same way, beside their
    existing "Compare with …" items.
  - All three open the same report.
- **Ballot Reconciliation report** (one per audit window, refreshed by each check; title "Ballot
  Reconciliation - <election> with <window>"):
  - **Columns:** Measure, Audit Log (EMS), CVR or Results, Difference, Status, Note.
  - **Statuses:** Match, Differs, Lower bound met, Lower bound not met, Not reported.
  - The EMS figure is the selected session's ballots: the last session by default, or the one picked
    with Reports → Select Session….
  - **With a CVR:** Ballots (EMS vs the CVR's first cards), plus CVR rows and continuation sheets as
    information.
  - **With results:**
    - Ballots cast, exact.
    - Per EMS vote type, an exact row when the results give ballots by vote type. Otherwise a lower
      bound: the EMS ballots of that type must be at least the votes of the Vote-For-1 contest with
      the most votes in it. The note names that contest.
  - The summary states each side's source and the overall verdict.
- **Validated** (Release x64, all three windows):

  | Election | EMS = CVR = results ballots | Lower bounds (EV / ED / Mail / Prov) |
  |---|---|---|
  | Dallas G24 | 858,562 | 636,880 ≥ 633,355; 188,811 ≥ 187,379; 31,468 ≥ 31,209 (U.S. Senator); 1,403 ≥ 1,396 |
  | Travis G24 | 587,090 | 462,929 ≥ 460,832; 97,925 ≥ 97,089; 25,276 ≥ 25,079; 960 ≥ 956 |
  | Travis P26 | 274,443 | met; weak, since each party's contest counts only its own voters |
  | Travis PR26 | 97,460 | met; weak, as for P26 |
  | Travis L26 | 12,710 | met; no contest is countywide |
  | Travis GR25 | 7,212 | met; Provisional 2 ≥ 2 |

- **Possible refinement:** in a joint primary, a vote type's lower bound could add up the parties'
  top contests (DEM U.S. Senator + REP U.S. Senator), since no ballot votes in both.

### Step 9 — Export and the Events reload round trip (done 2026-10-29)

Engine (`ee_audit.{c,h}`, smoke test `auditexp`): `EeAudit_FormatExportUtf8`,
`EeAudit_LoadExport`, `EeAudit_IsExportPath`; `EeAudit_LoadFiles` takes exports beside PDFs.
Window: File → Export…, right-click Export Selected… in the audit window, and Export Selected… /
Export All… in every report window. Release x64 and ARM64 build with no warnings.

- **File → Export…, Events view:** a dialog offers two layouts, as the results window's export
  does (`ResExportDlgProc` now takes its texts).
  - **Displayed view:** the columns and rows shown (filter, log, copy, sort). Copies reads
    "3 of 5".
  - **Reloadable:** the events the filter shows, always in printed order (newest first, whatever
    the sort), with these columns:
    `Record Type, Timestamp, Log, Event, Module, User Id, Description, Copies, First Copy, Page,
    County, Election Date`.
    - **"Copy" records** come first, one per copy of each log exported: the print time, the log,
      the election in its header (Description), "2 of 5", the file name, its pages, the county
      and the election date.
    - **"Event" records:** the copies that hold the event ("1-3,5 of 5", quoted, since it holds
      a comma), the first of them, and its page there.
    - From a single copy (View → Copy) the file holds that copy alone, "1 of 1".
- **Ballot Batches view:** File → Export… saves the displayed view plus a last `Session` column.
- **Report windows:** the rows of the tab shown, in display order. The file is named
  `<first copy>-<report>-<tab>.csv`.
- **Reload:** File → Load Audit Logs… accepts `.csv` / `.tsv` beside PDFs.
  - Columns are found by title, so their order does not matter.
  - Each copy is built back from the events that list it. The event's page goes to its first
    holder, the copy whose strings the merge keeps.
  - The copies are then grouped and merged as if read from the PDFs.
  - **Refused, with the reason:**
    - a displayed-view export (it has no Record Type; the message says to export again choosing
      Reloadable);
    - a missing Copy record;
    - a Copies field that lists no copies;
    - a copy count that differs between records.
- **Validated (scratchpad `exp/roundtrip.exe`):** load the PDFs, export every merged event of
  both logs, reload, compare. All 12 Travis and Dallas elections come back with 0 differences:
  - merged events, their copy masks, first copies and pages;
  - every copy's check, print time, pages and event count;
  - the election session's ballots and the session count;
  - the notable events, red flags included;
  - the election name and date.
  - Dallas G24: 103,827 events, 17.0 MB. Reading the PDFs takes 6.2 s; reloading the export
    takes 0.2 s.
- **In the app:** Dallas G24 reloadable export (17,033,733 bytes, the probe's plus the BOM),
  displayed-view export, Batches export (TSV), then a reload into a second window.
  - The reloaded window shows the same status bar (5 + 5 copies, 103,827 events, 858,562 EMS
    ballots).
  - Its Copies Check is the same (38,920 outside the span, 0 missing).
  - Its Ballot Accounting is the same (alternative readings 906,179 / 857,597).
  - Travis L26's Notable Events exported from the report window.
- **Not kept:** a page for copies other than an event's first holder (it reloads as 0). The
  status bar shows only the first holder's page, so nothing visible changes.

## Implementation plan (proposed)

1. **Reader `src/ee_audit.{c,h}`: one PDF → one copy.**
   - Open with `EePdf_Open`. Detect the report from page 1 (title, Creator
     `JasperReports (AuditEventsReport)`).
   - Read the header fields and the footer page count.
   - Assign each run to a column by its rotated x range, learned from the heading row on each
     page.
   - Assemble events around Timestamp-column anchors. Split a level off a description run;
     strip a trailing timestamp from a description run.
   - Join page-top text (above the first timestamp, or a page with none) to the previous
     event, per column.
   - **Model:**
     - timestamp as seconds since 1970 (local, no zone);
     - level, module and user interned;
     - description as UTF-8 in one arena;
     - page number; file order index.
   - **Performance target:** the 6,197-page Travis P26 copy in ≤ 30 s on the desktop. Measure
     `EePdf_ExtractPageText` first; the 1.5 results-PDF sweep is the only prior data.
   - **Smoke test `audit`:** synthetic JasperReports-style PDFs in `smoke_load.c`, like
     `respdf`. Cover:
     - a rotated landscape page;
     - Info as its own run and Warning merged with its description;
     - a description sharing a run with its timestamp;
     - a wrapped module;
     - a row split across two pages;
     - a page with no timestamp;
     - an Admin and an Election report;
     - a non-audit PDF refused.
   - **Sweep:** all 68 PDFs load, and each file's event count equals the 2026-10-28 research
     extraction's (a PyMuPDF position-based script in the desktop session scratchpad, not in the
     repo). Its per-file counts are in the appendix.
2. **Grouping and merge (engine).**
   - Group copies by county + election date and log type. Order them by print time.
   - Merge into one event list:
     - match key: timestamp + level + user + description without whitespace, prefix-tolerant;
     - same-second ties keep file order;
     - each event gets a copy bitmask.
   - Per copy, compute its time window and the earlier copies' events it lacks, split into
     outside-window and missing.
   - **Validate:** Travis 0 missing in every election; Dallas G24 38,920 + 53 outside-window,
     0 missing.
3. **Ballot accounting (engine).**
   - Parse commit, update, full-clear and media-clear events (warning + confirmation), and the
     previously-loaded and sheet-error events.
   - Sessions, test-deck classification (poll-place name patterns), the media-clear rule (D5),
     and the vote-type mapping (D9).
   - **Validate** every row of the table above. Dallas G24 must give 858,562, with 906,179 /
     857,597 shown as the alternative readings.
   - **Investigate Dallas P24's missing ballot** (231,464 vs 231,465). Candidates: a commit
     split across a page with no timestamp, a batch revision, or a load outside the session.
4. **Window (`main.c`).** The Audit Log window with the Events view, status bar, sort, Copy,
   Options (zoom), and File → Load Audit Logs… in all windows. Load and group on the worker
   thread with the progress dialog.
5. **Batches view, Ballot Accounting and Copies Check reports.**
6. **Filter**, extending the CVR filter dialog with contains / does not contain and date
   relations; save/load filter files.
7. **L&A Testing Timeline, Notable Events and Users and Workstations reports.**
   - The timeline joins the Election log's test sessions and counting periods with the Admin
     log's test-copy restores and opens.
   - **Validate on Travis PR26 (traced 2026-10-28):**
     - the public test on 04/06/2026;
     - 05/23: test 10:48, mail loads 13:33–14:02, `EV CC 3rd Tab Test` restored 14:13;
     - 05/26: that copy opened 12:15, early-voting sticks 12:25–13:11; Election Day loads
       18:45–00:11, `ED CC 3rd Tab Test` restored 05/27 00:21;
     - 06/03: that copy opened 10:48, canvass loads 11:29, `Canvass CC 3rd Tab Test` restored
       11:40.
   - The expected result is three counting periods, each with a test before and after:
     - Saturday 05/23, mail;
     - Election Day 05/26 (the early-voting sticks, a pause from 13:16 to 18:38, then the
       Election Day loads);
     - the canvass, 06/03.
   - Then the other Travis elections.
   - Dallas G24 and P24 show the in-database pattern: one counting period from 11/02 to the
     11/22 post-canvass test, with its media clears.
8. **Ballot Reconciliation.**
   - Add `EeCvr_CardCounts(t, &rows, &ballots)`, generalizing `EeCvr_HasMultiCard`'s
     page-1 test and Hart / Dominion sheet columns.
   - Also use it in Results ↔ CVR compare, which today reports ballots as unknown for a
     multi-card CVR (`main.c`: `cw->multi_card ? EE_RES_UNKNOWN : nrows`).
   - The Compare menus in the audit, CVR and results windows.
   - **Validate:**
     - Dallas G24 audit ↔ the 9-file CVR (873,455 rows → 858,562 ballots) ↔ G24 Clarity XML;
     - Travis G24, P26, PR26, L26 and GR25 audit ↔ CVR ↔ results.
9. **Export** (events, batches, reports) and the Events reload round trip.
10. **Help topics, version 1.6.0.0, RELEASES.md, STORE-LISTING.txt**, README.

Steps 1–3 have no UI and can be checked against this document's numbers before any window
exists.

## Risks and open questions

- **Other Electionware versions** may change the template. These files span EVS 6.x from 2024
  to 2026 and are all one template. Another county's logs would be the test.
- **The media-clear rule is inferred**, checked on Dallas G24 (4 clears) and Travis G24 (1).
  When the next reload does not settle the poll place, the app shows every reading rather than
  guessing.
- **Date-filtered copies:** a copy printed with a start date is recognized only by its first
  event. If the same copy also used an end date or a module or user filter, it would show as
  "missing" events. The Copies Check names the copy and its window, so a person can tell.
- **Time zone and DST:** timestamps are local with no zone. A fall-back hour repeats, so
  ordering uses file order, not timestamps.
- **Memory:** the largest election (Travis P26, 4 copies, ~525,000 rows) merges to ~134,000
  events. Fine.

## Decisions (proposed 2026-10-28, to confirm)

- **D1 — Grouping:** one window per election (county + election date) holding both logs and all
  copies. Alternative: one window per log type.
- **D2 — Merged display:** each event shown once, with a Copies column; View → Copy shows a
  single copy as printed.
- **D3 — Order:** newest first, as printed; header click sorts.
- **D4 — Election session:** the last session that loaded non-test batches, changeable in
  Ballot Accounting. Test sessions are labeled L&A tests (first, before counting, after
  counting) by their place in the timeline.
- **D5 — Media clears:** remove the cleared media's batches of the poll place reloaded next,
  on the device type the clear names ("Central Count Media-n": central count batches, the
  `A0013-` names, only); show the other readings and their totals when ambiguous.
- **D6 — Reconciliation:** through a Compare menu in the audit, CVR and results windows, like
  1.5's D6.
- **D7 — Multi-sheet:** compare EMS ballots with CVR ballots (page-1 rows), show rows and
  continuation sheets beside them, and fix the results ↔ CVR compare the same way.
- **D8 — Filter:** CVR-style Include/Exclude rules, plus contains / does not contain for
  Description and on-or-after / before for Timestamp.
- **D9 — Scope:** ES&S Electionware Election and Admin audit event PDFs only. Built-in
  poll-place → vote-type keywords, editable in Options. Other vendors' logs (Hart Verity, DS200
  poll tapes, ExpressVote logs) later.
  - The 2026-10-28 county survey (`audit-log-county-survey.md`) found two more formats online,
    now in `Audit_Logs/`:
    - Hart VerityCount Audit Log Reports: Parker 2022–2026 (14 files) and Hays PR24.
    - ES&S Electionware Machine Log Reports: Dallas DS200 / DS850 tabulator logs for P24, L24
      and G24 (22 files).
  - Either can widen D9 if wanted. The machine logs would let ballot counts be checked per
    tabulator.
- **D10 — L&A Testing Timeline:**
  - It lists each counting period with the nearest test before and after, as found in the
    Election and Admin logs.
  - A missing test is worded "No test found in these logs", not as a violation, because a test
    copy's own log is usually not provided.
  - Counting periods are bounded by tests, not by clock gaps, so a long pause is shown inside
    a period rather than starting a new one.
  - The test-copy name patterns and the 2-hour pause threshold are settings.

## Appendix — research event counts (2026-10-28)

Events per file from the position-based research extraction (one event per Timestamp-column
anchor), the target for the step 1 sweep. File names are under `Audit_Logs/TX_<County>_County/`
with the `.pdf` extension dropped.

| County | File | Pages | Events |
|---|---|---|---|
| Dallas | G24 Admin Audit Before EV | 20 | 403 |
| Dallas | G24 Admin Audit Event Election Night | 30 | 623 |
| Dallas | G24 Admin Audit Events 11-14 | 36 | 739 |
| Dallas | G24 Admin Audit Events After EV | 25 | 523 |
| Dallas | G24 Election Audit Events 11-14 | 2,588 | 52,896 |
| Dallas | G24 Election Audit Events Election Night | 3,981 | 83,925 |
| Dallas | G24 Election Audit Events before EV | 2,942 | 62,077 |
| Dallas | G24 Election Audit Evernt After EV | 1,812 | 36,418 |
| Dallas | G24 Final Election Audit Events Log | 4,872 | 102,906 |
| Dallas | G24 Final Election Audit Log | 44 | 921 |
| Dallas | P24 Joint Primary Election- Admin Audit Events | 41 | 851 |
| Dallas | P24 Joint Primary Election-Election Audit Events  | 3,855 | 84,263 |
| Travis | G24 Admin Audit Log 11132024 | 49 | 994 |
| Travis | G24 Admin Audit Log 1122024 AM | 22 | 455 |
| Travis | G24 Admin Audit Log 1152024 | 39 | 804 |
| Travis | G24 Election Audit Log 11132024 | 4,848 | 103,105 |
| Travis | G24 Election Audit Log 1122024 AM | 4,138 | 88,341 |
| Travis | G24 Election Audit Log 1152024 | 4,687 | 99,653 |
| Travis | G25 Admin Audit Report 1112025 | 18 | 367 |
| Travis | G25 Admin Audit Report 1112025 2nd | 20 | 392 |
| Travis | G25 Admin Audit Report 11122025 Final | 29 | 590 |
| Travis | G25 Election Audit Report 1112025 | 1,620 | 33,957 |
| Travis | G25 Election Audit Report 1112025 2nd | 1,670 | 35,016 |
| Travis | G25 Election Audit Report 11122025 Final | 1,847 | 38,783 |
| Travis | GR24 Admin Audit Events Report 12.11.2024 | 13 | 263 |
| Travis | GR24 Admin Audit Events Report 12.11.2024 - Post EV Results | 14 | 283 |
| Travis | GR24 Admin Audit Events Report 12.20.2024 | 20 | 406 |
| Travis | GR24 Election Audit Events Report 12.11.2024 | 381 | 7,641 |
| Travis | GR24 Election Audit Events Report 12.11.2024 - Post EV Results | 397 | 7,946 |
| Travis | GR24 Election Audit Events Report 12.20.2024 | 420 | 8,449 |
| Travis | GR25 Admin Audit Report 12102025 | 15 | 294 |
| Travis | GR25 Admin Audit Report 12102025 2nd | 16 | 311 |
| Travis | GR25 Admin Audit Report 12192025 3rd | 21 | 420 |
| Travis | GR25 Election Audit Report 12102025 | 205 | 4,112 |
| Travis | GR25 Election Audit Report 12102025 2nd | 214 | 4,283 |
| Travis | GR25 Election Audit Report 12192025 3rd | 230 | 4,616 |
| Travis | L25 Admin Audit Report 2025-04-30 Post-EV | 30 | 610 |
| Travis | L25 Admin Audit Report 2025-04-30 Start | 29 | 591 |
| Travis | L25 Admin Audit Report 2025-05-09 | 37 | 763 |
| Travis | L25 Election Audit Report 2025-04-30 Post-EV | 954 | 19,495 |
| Travis | L25 Election Audit Report 2025-04-30 Start | 945 | 19,304 |
| Travis | L25 Election Audit Report 2025-05-09 | 983 | 20,098 |
| Travis | L26 Admin Audit Events Report 4.29.26 AM | 42 | 863 |
| Travis | L26 Admin Audit Events Report 4.29.26 PM | 43 | 885 |
| Travis | L26 Admin Audit Events Report 5.11.2026 | 59 | 1,199 |
| Travis | L26 Election Audit Events Report 4.29.26 AM | 641 | 13,007 |
| Travis | L26 Election Audit Events Report 4.29.26 PM | 654 | 13,286 |
| Travis | L26 Election Audit Events Report 5.11.2026 | 681 | 13,865 |
| Travis | P24 Admin Audit Event Report | 55 | 1,148 |
| Travis | P24 Election Audit Events Report | 4,884 | 104,960 |
| Travis | P26 Admin Audit Report 02282026 | 25 | 513 |
| Travis | P26 Admin Audit Report 02282026 2nd | 26 | 531 |
| Travis | P26 Admin Audit Report 03032026 3rd | 31 | 621 |
| Travis | P26 Admin Audit Report 03112026 Final | 40 | 811 |
| Travis | P26 Election Audit Report 02282026 | 5,970 | 129,394 |
| Travis | P26 Election Audit Report 02282026 2nd | 6,000 | 130,038 |
| Travis | P26 Election Audit Report 03032026 3rd | 6,063 | 131,363 |
| Travis | P26 Election Audit Report 03112026 Final | 6,197 | 134,291 |
| Travis | PR26 Admin Audit Event Report 5.23.26 | 67 | 1,341 |
| Travis | PR26 Admin Audit Event Report 5.23.26 2 | 68 | 1,363 |
| Travis | PR26 Admin Audit Event Report 5.26.26 | 70 | 1,398 |
| Travis | PR26 Admin Audit Event Report 5.27.26 | 72 | 1,457 |
| Travis | PR26 Admin Audit Event Report 6.3.26 | 77 | 1,559 |
| Travis | PR26 Election Audit Events Report 5.23.26 | 1,290 | 27,009 |
| Travis | PR26 Election Audit Events Report 5.23.26 2 | 1,312 | 27,451 |
| Travis | PR26 Election Audit Events Report 5.26.26 | 1,323 | 27,679 |
| Travis | PR26 Election Audit Events Report 5.27.26 | 1,364 | 28,548 |
| Travis | PR26 Election Audit Events Report 6.3.26 | 1,422 | 29,804 |
