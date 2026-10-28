# Session Handoff — ElectionExplorer

> Live handoff note passed between the desktop and laptop via git.
> Update this at the end of each session; read it at the start of the next.
> Keep it short and current — git history is the permanent record.

**Last updated:** 2026-10-28. Election Explorer **1.5.0.0 certified and published on the
Microsoft Store**. Uncommitted: the Bastrop name fix in `src/voter_roster.c` (see Next steps).
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
  (.md/.pdf, plus `_Supplement` and `_Supplement_Records.csv`); latest 2026-10-27 (data
  through 10/26).
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

- **Bastrop name fix (uncommitted, verified 2026-10-28):** Bastrop's Windows-1252 CSVs pad
  some names with runs of 0xA0, which become UTF-8 no-break spaces (C2 A0); a 63-space run
  overflowed the 128-byte name buffers and `clean_text` cut a character in half. `clean_text`
  in `src/voter_roster.c` now treats C2 A0 as whitespace, ends a full buffer on a character
  boundary, and no longer glues a stray character onto the last word when only one byte is
  left. Verified on Windows: `Applications.sln` Release x64 + ARM64 build clean, the smoke
  suite passes, and reloading Bastrop `G26_Monday-October-26.csv` with the loader gives all 12
  padded rows clean names (no no-break spaces, valid UTF-8, the cut middle name now empty),
  no invalid UTF-8 anywhere in the file, and no other record changed. Ready to commit;
  candidate for 1.5.1.
- **Candidates for the next version** (design doc "After step 9" and "Implementation plan" 11):
  - results PDFs still refused: scanned reports (~450), Electionware "Statement of Votes Cast"
    and custom table grids (~78), the encrypted Ellis PDFs;
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
