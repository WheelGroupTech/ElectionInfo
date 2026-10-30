# Session Handoff — ElectionExplorer

> Live handoff note passed between the desktop and laptop via git.
> Update this at the end of each session; read it at the start of the next.
> Keep it short and current — git history is the permanent record.

**Last updated:** 2026-10-30. Election Explorer **1.5.0.0 certified and published on the
Microsoft Store**. Committed for 1.6: Dominion results, scanned/ASCII85 results PDFs (`ae7eb01`),
Webb certificate-number mail lists (`5ed8228`), Hart HVS "Votes by Precinct" CVRs (`2c0fbd3`).
**Uncommitted: Hart Verity audit logs (1.6, decisions H1–H5 confirmed by the user)** —
`src/ee_audit_hart.{c,h}` (new), `ee_audit.{c,h}` (5 log slots, zips, Hart accounting / notes /
timeline / export), `main.c` audit window, `settings.{c,h}` (test task words), `resource.h`,
.vcxproj(+filters), smoke `hartaud`, docs (`docs/audit-log-design.md` "Hart Verity audit logs"),
RELEASES.md 1.6 note (1,454 of 1,500 characters, now naming Hart Verity logs and HVS CVRs) and
STORE-LISTING.txt.
**1.6.0.0 is not final:** the user has more changes first; then rebuild the Store bundle (the
one built on 2026-10-29 predates Dominion, OCR, HVS and Hart audit logs).
This note was condensed from a long changelog on 2026-10-27; the old text is in git history.

## Current state

- **Released:** 1.5.0.0, submitted 2026-10-27, certified and published on the Store by
  2026-10-28. 1.4.1.0 (submitted 2026-10-22) is the version before it. Per-version notes:
  `ElectionExplorer/RELEASES.md`; Store description: `ElectionExplorer/STORE-LISTING.txt`.
- **Store bundle:** `Build/msix/ElectionExplorer.Package_1.5.0.0_x64_arm64_bundle.msixupload`.
  After a version bump, delete the package's `bin/`, `obj/`, `BundleArtifacts/` and build with
  `/t:Rebuild`, then check every inner `AppxManifest.xml` version (stale-manifest trap).
  `ElectionExplorer.Package/README.md` has the build command.

## What Election Explorer does (by version)

Win32 C11 desktop app (no MFC, static CRT, x64 + ARM64) under `ElectionExplorer/`.

- **1.0–1.1:** voter lists (CSV/TSV/XLSX; Travis, Dallas, El Paso layouts), filters, duplicates,
  reports, list compare; ES&S CVRs with tabulation.
- **1.2–1.3:** Hart CVR ZIPs and PDF CVR Reports (scanned ones via Windows OCR), Dominion CVRs with
  ranked-choice rounds, primaries split by party. Design: `docs/cvr-design.md`.
- **1.4–1.4.1:** Voter Roster window (who has voted), Voting Totals, roster compare. Design:
  `docs/voter-roster-design.md`; county formats: `docs/voter-roster-county-survey.md`.
- **1.5:** Election Results window (steps 1–10, all done). Design, decisions D1–D8 and per-step
  findings: `docs/election-results-design.md`. Also: filter export/load in every window, legacy
  `.xls` workbooks, rosters from 35 Texas counties plus the Texas Secretary of State.

## 1.5 Election Results — where things are

- **Engine** `src/ee_results.{c,h}`: Clarity summary.csv / detail.txt / detail.xls(x) / detail.xml,
  our long-form export, grouping into elections, cross-checks, combining one election's
  per-entity files (`combine_split`), tabulation, compare (`EeResults_Compare`).
- **Results PDFs** `src/ee_results_pdf.{c,h}`: ES&S Electionware (with or without the footer)
  and Hart (Verity, and the 2020-22 reports before Verity, `hl_*`).
- **Printed PDFs** `src/ee_glyph_ocr.{c,h}`: Type 3 glyph outlines (Travis PScript5 reports)
  rendered with GDI and read with Windows OCR; `pdf_reader.c` reads Type 3 fonts.
- **Window** in `main.c` ("Election Results window (1.5)"): views, filter, turnout report, compare
  (with results or CVR windows), export, help topics.
- **Validation baselines:**
  - all 471 Clarity elections of the 50 largest Texas counties load;
  - results PDF sweep: 1,340 of 1,941 load;
  - Travis G20 PDFs against the G20 CVR: 399 matches;
  - Hays G25 and PR26 match their CVRs exactly.
- **Scratch harness** (in the session scratchpad on the desktop, not in the repo): `res_probe.c`,
  `sweep9b.sh`/`sweep9c.sh`, `cvr_cmp.c`, `pdf_dump.c`. Freeze a probe copy after a step's last
  change before sweeping a baseline.
- **Results downloads:** `Election_Results/TX_<County>_County/` (gitignored, ~6.5 GB); survey and
  refresh commands in `docs/election-results-county-survey.md`; downloader
  `Election_Results/_download_tools/results_download.py` (gitignored, desktop only).

## Ongoing — Texas rosters for the Nov 3, 2026 election

- **Daily download:** scheduled task `tx-early-voting-roster-download` (recreated 2026-10-28 as a
  claude.ai scheduled task bound to the desktop) runs daily at 12:32 PM Central through Nov 4:
  the 50 largest counties' rosters plus the SOS county CSVs into
  `Voter_Rosters/TX_<County>_County/` (gitignored). Its runs use the desktop's device shell,
  where the repo is mounted at `$HOME/mnt/ElectionInfo`; the original task definition is backed
  up in `C:\Users\dteal\Claude-Backup\scheduled-tasks\`.
- **Wichita:** its PDFs sit behind a Cloudflare challenge; the user downloads them by hand
  (through 10/27 done).
- **Nightly review reports:**
  `State_Election_Info/Texas_Election_Info/Texas_Early_Voting_Roster_Review_<date>`
  (.md/.pdf, plus `_Supplement` and `_Supplement_Records.csv`); latest 2026-10-28 (data
  through 10/27).
  - **Loader gaps found 2026-10-28, fixed 2026-10-29 (uncommitted; smoke test `pdfpieces`):**
    - Guadalupe from 10/27 draws its "VUID"/"DATE" titles in two pieces (one in its cell's clip,
      one not): header titles now join across that;
    - Gregg 10/27 page 32 selects a one-glyph font between q and Q: `pdf_reader.c` now restores
      the text font, size and leading on Q (all 1,940 rows, names paired right);
    - Bexar "10272028": a file-name year after the file's write time becomes the year written
      (noted). Only these three of 320 roster PDFs load differently.
  - Report 100% county↔state agreement only when the county-only and state-only counts are
    both 0.
  - Supplements give VUIDs and file locations only (no names or addresses).

## G26 ballot scripts — `Scripts/Travis_County_Elections/2026-11-03_General/` (separate)

Python checks of the Travis County Nov 3, 2026 sample ballots against the ES&S Ballot Detail
Listing; all committed, lint 10/10. Run with `python -I`.

- **Scripts:** `process_g26_sample_bbm_ballots.py` (split the combined PDF),
  `parse_ballot_detail_listing.py`, `compare_listing_to_sample_ballots.py`,
  `compare_proposition_translations.py`.
- **Status (2026-10-22):** all 736 re-issued Full ballots match the 10.21 listing (80,907 ovals).
- **Still unfixed in the county's materials:**
  - Manor ISD Spanish header ("Proposition");
  - "Elección de Donos";
  - 126F/392E "Sheet 1 of 2" on 1-card ballots;
  - ESD 10 wording on 324B/D/H;
  - Manor A–D and Sunset Valley A missing from LB ballots;
  - ESD 9 Spanish "se escribe";
  - minor Spanish wording issues.
- **Next:** when the county re-issues the LB sample ballots, run
  `compare_listing_to_sample_ballots.py` against the 10.21 listing, and ask whether any mail
  ballots were printed from the old ESD 9 layout.

## Next steps

- **OneDrive data backup (2026-10-30, uncommitted tools):** `ElectionExplorer/test/data_check.c` +
  `build_data_check.ps1` (loader harness, JSON out) and `test/backup_data.py` + `backup_data.json`
  (dry run / `--copy`; docs in `test/README.md`) back up rosters, CVRs, results and audit logs that
  Election Explorer reads to `OneDrive\Documents\Elections\ElectionExplorerData` (never voter lists),
  load-check the backup and cross-check CVR / results / audit logs per election. On-demand task:
  `election-data-onedrive-backup`. Files that load with no data are not copied (user). Fixed the
  same day (uncommitted, 1.6): Hart Verity precinct reports now load by precinct and vote type
  (`ht_precinct_line` in ee_results_pdf.c; 162 reports had been read as totals; smoke `reshartpct`;
  docs/election-results-design.md "Precinct report (1.6)"), Hart number pieces shown from one point
  join (McLennan 2023), and Angelina's 10/29 roster printed two pages wide with no title row loads
  (`read_pdf_halves` in voter_roster.c; smoke `rosterhalves`; survey doc "Angelina's October 29
  layout"). Regression: old vs new reader on all 893 Hart results files (only intended changes) and
  371 roster PDFs (only Angelina 10/29). Still open: Compare cannot pair Clarity combined primaries
  without party prefixes (Tarrant, Hunt; choices carry party="REP") with Hart "REP X" CVR contests.
- **1.6 Hart Verity audit logs (done 2026-10-30, uncommitted):** VerityCount / VerityCentral /
  VerityData "Audit Log Report" PDFs (and the Hays zip) load into the audit window; one election
  per county and date. Accounting = "vDrive read" State Read per task (a read belongs to the last
  "Task opened"); test tasks by name (setting, default "LAT, L&A, Test, Training, Logic"); Central
  "CVRs written to vDrive" matched to Count reads by vDrive ID (or ballots + time when no "vDrive
  closed" was logged). All 19 files: rows complete; official totals equal the results (Burnet P26
  12,585, Parker P26 33,642, ...). Burnet P26's Central → Count 536 vs 524 is flagged. Parker P26's
  hour-behind "vDrive saved" rows show as a clock anomaly. The reconciliation now shows the log's
  print time / newest event and the results timestamp (a log printed before the final canvass is
  short). ES&S regression identical (13 elections). Release x64/ARM64 clean; smoke all pass.
  Scratch tools (desktop scratchpad): `hart_probe.c` + `build_hartprobe.ps1` (summary of any audit
  logs), `ess_probe.c` + `build_essprobe.ps1` (ES&S regression vs `git archive HEAD`),
  `hartaud/` (research parse), `uia/hart/` screenshots.

- **1.6 ES&S audit logs — steps 1–3 (engine) done 2026-10-28; steps 4–9 (window, batches,
  reports, filter, timeline / notable / users, ballot reconciliation, export) done 2026-10-29:**
  `ElectionExplorer/docs/audit-log-design.md` ("Implementation status"). Implementation was started
  on the proposed decisions D1–D10.
  - **Data:** Electionware Election/Admin audit event PDFs in `Audit_Logs/` (gitignored): Travis,
    Dallas.
  - **County survey** (`docs/audit-log-county-survey.md`): Dallas, Parker and Hays post logs.
    Added Dallas L24, 22 Dallas machine logs (`TX_Dallas_County/Machine_Logs/`), Parker's 14 Hart
    VerityCount reports and Hays PR24. Refresh: `python -I Audit_Logs/_download_tools/audit_download.py`.
  - **New:** `src/ee_audit.{c,h}` (in the .vcxproj), smoke tests `audit` / `auditmerge` / `auditacct`
    (test/README updated).
    - The reader reads all 70 Electionware PDFs (1,875,864 events).
    - The merge shows 0 missing events in all 24 logs.
    - The replay equals the official ballots cast for 9 Travis elections, Dallas G24 (858,562,
      multi-sheet) and L24 (83,030).
    - Dallas P24 is one short (open).
  - **Step 4:** the Audit Log window (`main.c` "Audit Log window (1.6)"): File → Load Audit Logs…
    in every window, one window per election, merged Events view with Copies and bold red for events
    the final copy lacks, View → Log / Copy, sort, Copy, zoom. Details in the design doc.
  - **Step 5 (done 2026-10-29, uncommitted):**
    - View → Ballot Batches (one session's commits with status; Go to Event).
    - Reports → Ballot Accounting… (Sessions / Poll Places / Vote Types / Media Clears tabs; Use This
      Session; Select Session…) and Reports → Copies Check….
    - Engine: `EeAuditBatch.clear`.
    - Dallas G24 shows 858,562 with the alternative readings 906,179 / 857,597.
  - **Step 6 (done 2026-10-29, uncommitted):** Filter → Filter… (Ctrl+L) / Reset Filter.
    - Rules: Timestamp on or after / before (these all apply together, giving a range); Log,
      Event, Module, User Id and Vote Type is / is not; Description contains / does not contain.
    - The shared rule window (`CvrFilterState`) gained optional hooks: `relations`,
      `relation_text`, `validate`, `relation_mask`.
  - **Step 7 (done 2026-10-29):** Reports → L&A Testing Timeline / Notable Events /
    Users and Workstations; engine `EeAudit_Notable` / `EeAudit_Timeline`; smoke test `auditnote`.
    - **Red flag (the user's rule):** a results report printed, saved or exported before Election
      Day while ballots (not test decks, more than 0) were loaded.
    - Found: Travis L26 (36, on 05/01), Dallas P24 (1), Dallas L24 (1), and Travis L25 (5,
      probably a misnamed provisional test deck).
    - Legal citations are to be confirmed by the user.
    - The PR26 timeline equals the design-doc trace.
    - Another election's history (a different dated name in "Election Opened:") is not checked.
  - **Dominion results (done 2026-10-29, uncommitted; for 1.6.0.0):** `src/ee_results_dom.{c,h}`
    reads the Dominion Election Summary Report (XML, XLSX, PDF) and the Statement of Votes Cast
    PDF into `EeRpdfDoc`. Details and validation are in `docs/election-results-design.md`
    ("Dominion results").
    - San Francisco: 12 elections, three formats each, agree. Results ↔ CVR is exact for 8 of 10
      (G25: 15 CVR redactions; G19: the 2019 CVR has no card numbers, so its ballots can't be
      counted).
    - DeKalb: 6 elections. "****" counts are filled from each precinct's Total into the vote type
      "Withheld".
    - Also: a CVR's "(Rank 1)" column compares as the contest (`EeResults_FromTally`), and the
      merge no longer repeats a note another file of the election gave.
    - The RELEASES.md 1.6 note and STORE-LISTING.txt mention Dominion results. The Store bundle
      must be rebuilt.
  - **Step 10 (done 2026-10-29):** help topics (voter list, CVR and results topics
    point to audit logs), version 1.6.0.0 in the three files, RELEASES.md note (1,190
    characters), STORE-LISTING.txt section, README.
    - A pre-release bundle `Build/msix/ElectionExplorer.Package_1.6.0.0_x64_arm64_bundle.msixupload`
      is built and checked (1.6.0.0 throughout).
    - Rebuild the bundle (clear the package's bin/obj/BundleArtifacts, `/t:Rebuild`) after the
      remaining 1.6 changes.
  - **Step 9 (done 2026-10-29):** File → Export… (Events: Displayed view or Reloadable;
    Batches: plus Session), Export Selected…, and report windows' Export Selected / All….
    - The reloadable file ("Record Type" Copy / Event records, copies as "1-3,5 of 5") loads back
      through File → Load Audit Logs….
    - Round trip exact for all 12 Travis and Dallas elections (scratchpad `exp/roundtrip.exe`).
      Dallas G24 reloads in 0.2 s instead of 6.2 s.
  - **Step 8 (done 2026-10-29):** Compare → Check Ballot Counts with … in the audit,
    CVR and results windows opens the Ballot Reconciliation report (EMS vs CVR ballots, or vs
    results ballots cast plus per-vote-type lower bounds).
    - `EeCvr_CardCounts` counts a multi-card CVR's first cards, so Results ↔ CVR compare now
      gives ballots for those CVRs too.
    - Dallas G24: 873,455 rows → 858,562 ballots.
    - EMS = CVR = results for Dallas G24 and Travis G24 / P26 / PR26 / L26 / GR25.
  - **Next:** the user's remaining 1.6.0.0 changes. Then update RELEASES.md / STORE-LISTING.txt
    if needed, rebuild and check the Store bundle, and submit.
    Scratch tools in the desktop session scratchpad: `audit_probe.c` (`-merge` mode), `cards/`
    (card-count probe) and `uia/drive_audit.ps1`, which drives the app through UI Automation and
    saves screenshots. Its `-Then` list takes:
    - command IDs;
    - `click`/`dbl<x>:<y>` in the audit window;
    - `rep`, `repclick`/`repdbl<x>:<y>` for report windows;
    - `load;<cmd>;<folder>;<files>;<class>`;
    - `cmp<n>` (the audit window's Compare menu) and `cmpw;<class>;<pos>;<n>` (another window's);
    - `shot;<class>`;
    - `exp;<0|1|b>;<path>` (File → Export…), `repexp;<x>:<y>;<path>` (a report's Export All),
      `newest`, `wait<s>`.
    The new-style Save dialog's file name box is found by UIA AutomationId 1001; set it with
    WM_SETTEXT.
    `uia/recon_travis.ps1` runs the Travis reconciliations.
- **Bastrop name fix:** committed 2026-10-28 (`fca8719`); candidate for 1.5.1.
- **Candidates for the next version** (design doc "After step 9" and "Implementation plan" 11):
  - results PDFs still refused: scanned Hart reports (9), Midland's scanned "Custom Table
    Report" grids, JPX/JBIG2 scans, Liberty's turned per-entity L24 files; Electionware
    "Statement of Votes Cast" and custom table grids (~78), the encrypted Ellis PDFs;
  - scanned results: Cameron's faint 2020–22 primary scans still leave many rows that do not add
    up (OCR misses single-digit counts at any scale); the precinct ballots check could fill
    more if it learned which contests are on every ballot;
  - Hart CSV exports, ES&S CSV exports (Collin, Bexar), Comal's Enhanced Voting, Dominion PDFs;
  - a vote-type breakdown for summary-only contests (the model keeps totals only);
  - in OCR-read reports, settle split glyph readings by context (only flagged today).
- **Smaller follow-ups:**
  - CVR: blank / undervote / overvote rate summaries, frozen key columns in the CVR grid;
  - persist window size and position;
  - enable GitHub Pages so the privacy URL resolves, if not done yet.

## Notes for the next session

- The user does all git commit/push/pull by hand (see [CLAUDE.md](CLAUDE.md)); never run git
  write commands or stage files.
- **Build:** MSBuild via `Applications.sln` (see [AGENTS.md](AGENTS.md)).
- **Smoke test:** the build line is in `ElectionExplorer/test/README.md`; run `smoke_load.exe`
  from `Applications/ElectionExplorer` (relative sample paths). Check that the exe you run is the
  one you just built.
- Don't whole-file clang-format existing C files; edit by hand.
- Run Python on downloaded county files with `python -I`; never execute downloaded files.
