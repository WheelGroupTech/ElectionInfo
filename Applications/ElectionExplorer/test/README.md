# ElectionExplorer test samples

| File | Source style | Notes |
|------|----------------|-------|
| `sample_voters.csv` | **Travis County** style headers (`VUIDNO`, `LSTNAM`, `FSTNAM`, …) | Comma-delimited |
| `sample_voters.txt` | **Dallas County** style headers (`SOS_VoterID`, `lastname`, …) | Tab-delimited |
| `smoke_load.c` | — | Optional console harness for `EeVoterTable_LoadFromFile` (samples, generated 400-column history header, copy-text format, ProcMon-style filter logic) |

These are tiny synthetic rows for UI and parser smoke tests only. Do not commit
build artifacts (`*.obj`, `*.exe`, `*.pdb`) from this folder.

## Full voter registration files (local)

Actual county voter-registration exports for manual testing live on this machine at:

```text
C:\Library\Elections\VoterLists
```

Open them in the app with **File → Open Voter List…** (`.csv` or `.txt`).

### Optional loader smoke test

From a VS 2026 x64 developer prompt, with cwd `ElectionExplorer/`:

```bat
cl /nologo /W4 /std:c11 /TC /utf-8 /DWIN32_LEAN_AND_MEAN /DUNICODE /D_UNICODE ^
  /DWINVER=0x0A00 /D_WIN32_WINNT=0x0A00 /I src ^
  test\smoke_load.c src\voter_table.c src\filter.c src\settings.c src\xlsx.c src\csv_sheet.c src\ee_cvr.c src\hart_cvr.c src\pdf_reader.c src\ocr_win.c src\hart_ocr.c ^
  src\dominion_cvr.c src\ee_rcv.c src\voter_roster.c src\third_party\miniz\miniz.c /Fe:test\smoke_load.exe /link /SUBSYSTEM:CONSOLE user32.lib advapi32.lib gdi32.lib ole32.lib runtimeobject.lib
test\smoke_load.exe
```

(The `xlsx` round-trip test authors a tiny `.xlsx` in memory via miniz, so
`src\xlsx.c` and `src\third_party\miniz\miniz.c` are required to build the tests.
The `cvr`, `cvrmulti`, `cvrtab`, and `writein` tests also need `src\ee_cvr.c`;
`writein` authors a workbook with a worksheet-rels → drawing → anchor and checks a
blank contest cell carrying a write-in image reads back as `[write-in]`; `cvrmulti`
covers "vote for N" contests (blank-header continuation columns → derived
`<contest> (2)` titles + `col_group`, incl. self-closed empty cells) and checks
that a repeated identical title stays a *separate* race; `cvrtab` covers
`EeCvr_Tabulate` (per-contest selection counts summed across a contest's columns,
ordered by contest then count); `cvrmerge` covers the write-in merge option (image
`[write-in]`, text `Write-in`, and `No image found` collapse to one `write-in` row
when on, separate when off); `cvrmc` covers multi-card detection (`EeCvr_HasMultiCard`: a long ballot split
across two cards is flagged; a clean per-ballot CVR and a combined-party primary are
not); `cvrfilt` covers the CVR filter primitives (`EeCvr_CollectColumnValues` distinct
sorted selections, blanks excluded; `EeCvr_GetCellW` physical-row access);
`cvrcsv` covers delimited-text loading via `EeCsv_ReadSheet` (RFC-4180 quoted
header/value with embedded commas, a blank contest cell, tab-delimited `.tsv`, a
UTF-16LE BOM export, concatenating a `.csv` with a same-schema `.tsv`, and the
empty-ballot skip that drops export-artifact rows — a lone id line and a trailing
`,,,,` line);
`cvrcnt` covers the per-column value reports (`EeCvr_FindColumnByTitle` locates a key
column by header; `EeCvr_ColumnHasReportableData` is FALSE for an all-redacted column;
`EeCvr_CollectColumnCounts` returns per-value ballot-record counts plus a blank tally);
`vexport` covers voter delimited export (`EeVoterTable_FormatDelimitedUtf8` emits a
header row of column titles and honors an explicit CSV/TSV delimiter);
`cvrexp` covers CVR delimited export (`EeCvr_FormatDelimitedUtf8` header row +
RFC-4180 quoting: a comma-bearing contest name/value is quoted for CSV, not for TSV);
`cvrrt` covers export round-trip fidelity (a "vote for N" contest's continuation
columns export with blank headers so a re-import regroups the race unchanged);
`hart` covers the Hart loader (`EeCvr_LoadFromHartZips`, needs `src\hart_cvr.c`):
authors a zip of per-sheet XML files (two DEM sheets — one a continuation — and one
REP sheet) and checks one row per sheet, the frozen key columns, category ordering
(federal President sorts before state Governor despite XML order), vote-for-N
expansion, write-in/overvote/undervote mapping, multi-card via `SheetNumber >= 2`,
the primary per-party contest split (titles prefixed `DEM `/`REP `, so each party's
race tallies separately), `EeCvr_ReorderTallyByParty` (REP-first vs DEM-first
grouping of the tabulation output), and natural contest ordering (US Rep "District 6"
sorts before "District 33" although the XML lists 33 first). It also round-trips the loaded table through a CSV
export + reload (`EeCvr_LoadFromFiles`) and checks the Hart key columns stay frozen
(primary → 7 incl. `Party`; a general-election header with no `Party` → 6) rather than
being tabulated as contests;
`hartpdf` covers Hart PDF "CVR Report" loading (`EeCvr_LoadFromHartFiles`,
`EeCvr_IsHartCvrPdf`; needs `src\pdf_reader.c`): authors Hart-style PDFs (xref stream,
indirect `/Length`, WinAnsi + Type0/ToUnicode fonts, Reporting-Services clip-rectangle
cells) and checks one row per sheet with a record continued across pages, the PDF key
columns (Voting Type, Polling Place, Device Type/Serial/Data Id; precinct `101 - 001` ->
`101-001`), a wrapped title, vote-for-2, overvote/undervote/write-in, an accented name
decoded via ToUnicode, a CSV round trip, an object-index rebuild after a bogus
`startxref`, rejection of a non-Hart PDF, ZIP+PDF decoration by Cvr Id (votes from the
zip), the value-report data behind Reports → Polling Place / Device Serial / Voting
Type (`EeCvr_CollectColumnCounts` on those columns) and Hart's `Batch Number` (reportable
for a zip with batches, not for a PDF with a blank Central Batch Id), and the error when
a ZIP and PDF share no Cvr Id, and a county-redacted report (only Precinct + Cvr Id in
the header -> 2 key columns; a vote-for-3 contest printed as repeated title rows merges
into one grouped contest);
`cvrws` covers whitespace normalization of selection values (`John   Cornyn`
-> `John Cornyn`, trimmed ends, merged tally);
`hartocr` covers scanned (image-only) Hart CVR Reports read through Windows OCR
(needs `src\ocr_win.c`, `src\hart_ocr.c` and `ole32.lib`/`runtimeobject.lib`): draws two
Hart-style pages with GDI, embeds them as Flate gray images in a PDF with no text, and
checks detection, one row per sheet (two records on page 1, the second continued on
page 2 under a repeated header), a vote-for-2 contest from repeated title rows, an
undervote, the `OCR Status` key column and the load note. It prints `hartocr skipped`
(and passes) on a machine with no OCR-capable Windows language;
`dominion` covers the Dominion loader (`EeCvr_LoadFromDominionZips`,
`EeCvr_IsDominionZip`; needs `src\dominion_cvr.c`): authors JSON-export zips with miniz
(a 5.10-style export whose `CvrExport_10.json` precedes `CvrExport_2.json` in the zip,
and a 5.2-style export without Cards) and checks row order by export number, the key
columns (Record Id from ImageMask, tabulator-batch, Polling Place, Card, Adjudicated),
the adjudicated `Modified` version winning, a disabled contest left out, vote-for-2 with
overvote/undervote, an ambiguous mark ignored, ranked-choice `(Rank N)` columns (a
duplicate ranking, an overvoted rank, two write-in lines at one rank = overvote), a
county-redacted contest and ballot type (`<Redacted>`), a `\u00C9` JSON escape,
multi-card via `Card`, the 5.2 rules (lower rankings and overvoted marks flagged
`IsVote=false`), Hart-zip rejection by detection, a multi-zip layout mismatch, and a CSV
round trip that keeps the Dominion key columns frozen;
`rcv` covers ranked-choice tabulation (`EeCvr_FindRcvContests`, `EeCvr_TabulateRcv`):
rounds and transfers, ties for last broken by name and by an earlier round (flagged), a
skipped first ranking, an overvoted ranking stopping a ballot, an unresolved write-in
excluded (blank), exhausted ballots, the majority round, finishing order, and a filtered
subset;
`roster` covers the voter roster loader (`EeRoster_LoadFiles`, needs `src\voter_roster.c`):
authors a Travis-style ZIP of workbooks in memory with title rows and several header
layouts, an `_Updated` correction replacing its original, a primary workbook whose
Republican sheet lacks its header row (recovered from the Democrat sheet), pasted
Voter IDs, "No ballots received." and footer totals rows, blank and 9-digit Voter IDs,
an unlabeled notes column, a combined `LAST,FIRST MIDDLE` name, a Limited Ballot form,
a misnamed Early Vote copy of the Election Day roster (skipped), and a PDF (listed, not
read); checks rows, methods, dates, party, extras and the load-summary counts; then
exports TSV, reloads it, and requires every cell to match;
`rtotals` covers `EeRoster_ComputeTotals` (Voting Totals): a builder-made roster with a
Voter ID repeated later and earlier (each counted once, on the earliest record), a blank
Voter ID (counted), a record with no date, an unrecognized Voting Method, two parties, and
a table without a Party column;
`rcmp` covers voter roster compares (`EeVoterTable_CompareByVoterIdEx`): roster↔roster
(Voting Method / Date Voted changes, repeated Voter IDs in a different order paired with
their closest record, blank Voter IDs matched, "0101" == "101", an only-in-B voter,
`CollectDifferencesEx`), roster↔list with the first + last name rule (middle names
ignored, no address), a Travis-style list with one full-name column and "P nnn"
precincts, one-to-one pairing (a voter repeated in one list leaves a "Voter ID
repeated" row and equal identical counts; the 8-bit API reports it as only-here), and
the voter-list defaults for contrast;
`tarrant` covers Tarrant County rosters: tab-delimited files inside two ZIPs (the newer
layout with NPA party + `_Dem` file name, a 6-digit precinct split into precinct +
subcode, a repeated `City` title, a `--Redacted--` protected voter and an ISO mail
Return Date; the older layout with P24's damaged header and an anonymous protected row;
the full roster's Vote_Type F / L / Y) and a TSV round trip;
`dupvote` covers `EeVoterTable_MarkDuplicateVotersVoting` (Show Duplicate Voters Voting):
two voters with the same name and DOB who both voted, a group where only one voted, one
Voter ID on two rows (not two people), the same DOB written two ways, and a list without
birth dates.)
