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
  test\smoke_load.c src\voter_table.c src\filter.c src\settings.c src\xlsx.c src\xls.c src\csv_sheet.c src\ee_cvr.c src\hart_cvr.c src\pdf_reader.c src\ocr_win.c src\hart_ocr.c ^
  src\dominion_cvr.c src\ee_rcv.c src\voter_roster.c src\ee_results.c src\ee_results_pdf.c src\ee_glyph_ocr.c src\ee_audit.c src\third_party\miniz\miniz.c /Fe:test\smoke_load.exe /link /SUBSYSTEM:CONSOLE user32.lib advapi32.lib gdi32.lib ole32.lib runtimeobject.lib
test\smoke_load.exe
```

(The `xlsx` round-trip test authors a tiny `.xlsx` in memory via miniz, so
`src\xlsx.c` and `src\third_party\miniz\miniz.c` are required to build the tests.
`xls` authors legacy Excel 97-2003 workbooks in memory (a BIFF8 stream in an OLE2 compound
file, once in regular sectors and once in the mini stream) and needs `src\xls.c`; it checks
shared strings split across a CONTINUE record, RK / MULRK / NUMBER / FORMULA / BOOLERR cells,
date and zero-pad formats, a read of every sheet in one pass (`EeXlsx_ReadAllSheetsMem` handing the
compound file to `EeXls_ReadAllSheetsMem`), and a voter-list and a Jefferson County roster load of
`.xls` files.
`results` covers election results (`src\ee_results.c`):
- a Clarity summary.csv (Windows-1252, with a "?" for a character it lacks), detail.txt (30-character
  columns, a UTF-8 name), detail.xlsx (one sheet per contest) and detail.xml of one election load as one
  result with no differences, the XML shown;
- detail.xls as an Excel 2003 XML Spreadsheet (MergeAcross, ss:Index), and each other detail file alone,
  give the XML's counts;
- summary.csv alone splits two back-to-back contests with the same name;
- a changed total is flagged;
- ballots by vote type and contest figures are taken from the other files;
- over/under votes, and precinct and vote-type tabulation;
- a withheld ("protected") count;
- the long-form export reloads exactly (CSV, and TSV filtered to one precinct);
- another election groups apart;
- a summary inside a ZIP loads;
- a file that is not results is refused with its name.
`rescum` covers cumulative voting (Dallas CFBISD):
- a detail.txt with one column group per ballot oval and a "Total Cumulative" label longer than 30
  characters (which pushes the rest of the header right), with a line of spaces between the choice
  names and the header;
- an older detail.xls with the sums in the candidate's own group, followed by bare "Ballot Oval
  Selection N" groups;
- each alone and both together give each candidate the sum of its ovals, with no notes.
`respdf` covers results report PDFs (`src\ee_results_pdf.c`):
- an ES&S Electionware summary report and a two-precinct report load as one result with no
  differences. The fixtures have two-line column headings, a party code before the name, write-in
  rows, two numbers shown as one string, and contests met in another order precinct by precinct;
- a precinct report in the newer Electionware template loads alone. The fixture has a 12 pt heading,
  a 14 pt precinct line, "Statistics" below its own headings, a 9 pt title over 7 pt "Vote For", a
  heading wrapped inside a word ("Provision" / "al" = "Provisional") and candidate names wrapped onto
  a second line;
- a Hart cumulative report and canvass do the same. Their fixtures have turned column headings, a
  " - Democratic Party" title (read as "DEM ..."), "110 - DEM" precincts and a voting instruction
  read as Vote For;
- `EeResults_MatchPrecinctName` files a CVR's "110BS3" and "110-DEM" under precinct 110, but not
  "1100";
- an Electionware summary with no "Report generated with Electionware" footer loads from its
  JasperReports Creator. The fixture has a status ("Final Unofficial") beside an election name that
  names the county, a number shown in pieces from one point ("104" "," "443", as Acrobat edits write
  it), rows whose numbers are one string with the percentage ("4,219 59.75% 2,380 1,839") and
  three-line headings joined at a hyphen ("Election" / "Day In-" / "Person").
`reshart0` covers Hart reports from before Verity (McLennan, Montgomery, Harris 2020-22):
- a cumulative report, a precinct report and a canvass of one election load as one result with
  no differences. The fixtures have the "<County> - <election> - <date>" heading, counts with a
  percentage under Absentee / Early / Election, baselines that wander within a row, "Precinct 101
  (Ballots Cast: 120)" lines, and turned canvass headings with the party on the first line and a
  name wrapped onto the next;
- a second entity's cumulative report of the same election combines with the first into one
  election (`combine_split`).
`resglyph` covers reports printed to PDF through PScript5 (Travis L22, LR22, PR22;
`src\ee_glyph_ocr.c`, needs an OCR language and is skipped without one):
- an Electionware summary whose text is a Type 3 font of unnamed glyph outlines (Arial's, each
  declared 1 em wide and placed one at a time, a space an empty glyph) is read through OCR and
  carries the load note;
- two cells shown as one string spread by a character spacing, which a `Q` restored, read as two;
- the summary agrees with a plain-text precinct report of the same election.
`rescmp` covers `EeResults_Compare` and `EeResults_FromTally`:
- results compared with themselves all match;
- a summary with one changed total gives exactly one "differs" row, and its missing ballots cast is
  "not reported";
- a CVR-style tally (contest "(45918)" and choice "(EC1)" ids dropped, "undervote" / "overvote",
  two contests with the same name told apart by their key) matches the results;
- a per-precinct tally with one precinct changed flags that choice's total and that precinct;
- precinct masks on both sides leave no differences.
`resview` covers the results window's displayed-view export read back:
- a Totals file (CSV) and a By Precinct file (TSV, with vote type columns) load back;
- the two same-named contests are told apart;
- Over / Under Votes rows attach to their contest;
- each compares with the XML with no difference (precinct and vote type rows too for By Precinct).
`audit` covers ES&S Electionware audit event reports (`src\ee_audit.c`):
- a three-page Election Audit Events Report printed turned, as JasperReports prints it, reads its
  header (customer, election, election date, print time) and six events;
- a description wrapped onto a second line, Info / Warning / Verbose / Error levels, two rows in one
  second, a word broken at its hyphen ("re-" / "try" = "re-try"), and a wrapped module ("Paper
  Ballot" / "Application");
- a row whose description and module run onto the top of the next page is joined back, and so is
  a long description whose rest fills a page with no row of its own;
- 12:05 AM reads as hour 0;
- an Admin Audit Events Report reads as one;
- a results report is refused, naming the file.
`auditmerge` covers grouping and merging copies of a log (`EeAudit_GroupCopies`):
- copies given out of print order are ordered by print time; "Travis" and "Travis County" with one
  election date are one election, another date another;
- three copies merge into one list, newest first, each event once with the copies that hold it,
  including two identical events in one second (both kept) and a description wrapped at another
  word;
- a copy printed with a start date lacks an older event "outside its date range", and the last copy
  lacking an event inside its own span counts it as "missing".
`auditacct` covers the ballot accounting (`EeAudit_Accounting`):
- poll place names to vote types ("Travis County Early Vote Center", "Vote Center", "EV_..." /
  "ED_...", "Mail Central Count", a church name as Other) and L&A test decks ("Logic By Mail",
  "LA-EV Vote Center", "Logic/Acurracy-EV");
- a test deck, a full clear, four batches (one with "1,200" ballots), a "Central Count Media-5"
  clear that removes only the batch of the poll place loaded next (not EV Mail's media 5, nor a
  poll place tabulator's media 5), a declined full clear, a reload notice, sheet load errors
  (three, two sheet IDs), a batch ID loaded again (replacing the first), and an L&A test after
  counting: three sessions, the middle one the election's, 1,394 ballots by vote type.
`county1023` covers the October 23 layouts: a clipped polling-place letter (Gregg), row numbers on a
wrapped record's second line (Wichita) and a "Polling Pl" column whose "BBM" is a mail ballot (Hunt),
plus Hunt's October 24 date written "10.24.26" (a 2-digit year with dots).
`pdfstray` covers PDF rows that a stray piece of text knocked out of the Voter ID column: a name in two
touching pieces and a name's separate last letter (Gregg), a middle name's last letter at the left margin
(Gregg's mail list) and a "FED" precinct between the "Pct" and "VUID" titles (Brazos).
`county1026` covers the PDF layouts posted from October 23: Henderson's "Timestamp" column read as the
voting date and a "Henderson County Voter Turnout" title that is not a Voter ID title; Tom Green's mail
list with no title row (columns named from their contents, a 9-digit county number, titles printed once
on page 2 below some rows); Guadalupe's check-in report drawn one TJ per line (columns set apart by
kerns, the date a little lower, a voter with no Voter ID, a "TOTAL" line that is not a voter).
`filterfile` saves filter rules with `EeFilter_SaveFile` (CSV and tab-delimited, with quoted values: a
comma, quotes, a trailing space, an empty value) and loads them with `EeFilter_LoadFile` into columns in
another order, choosing the saved Column Number between two columns with one title, skipping a missing
column and (CVR) relations other than is / is not, and refusing a file that is not a saved filter.
`xlsxns` authors a workbook whose SpreadsheetML elements all carry a namespace prefix
(`<x:row>`, as the OpenXML SDK writes them), with a BOM and a package-absolute sheet
target, and checks cells, a shared string holding a literal `<x:b>`, and a date style.
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
`harris` covers Harris County rosters: CSVs inside two ZIPs (daily mail / early voting
files with ActivityDate as the voting day and N~P = no party, a two-digit-year file date,
a blank-VUID mail row, the Election Day roster's VoteType; the voter history's Harris
vote-type codes E / P / M / A / Z / 1, PRECINCT preferred over OLDPRECINCT and
NEWPRECINCT used when PRECINCT is missing; the official roster's "In Person" / "Mail"
with no voting date and an export timestamp in its name; a disclaimer and a
polling-place list skipped as reference files) and a TSV round trip;
`dallas` covers Dallas County rosters: the mail returns report (a TOTAL RECORDS line,
State Voter ID chosen over County Voter ID, unformatted Excel date serials, a blank
VUID), the Election-Day Website Roster (NP party), the 2026 layout (State ID, Date, one
FIRST MIDDLE LAST name, ED in the file name) and a precinct early-voting roster
(StateIDNumber, "LAST - FIRST" names, the P=InPerson M=Mail column, DD-MON-YY dates, a
shifted row, a two-space file name) and a TSV round trip;
`pdfroster` covers PDF rosters (`read_pdf_table`; needs `src\pdf_reader.c`): a
two-page Bexar-style mail list (cells centered under their headers via the font's
/Widths, a title line, no header on page 2, a "Page 2 of 2" footer, a suffix column, a
blank middle name), a Bell-style roster (a short name left-aligned under a centered NAME,
nearer the Voter ID title) and a PDF with no roster table (listed as not readable);
`limitedpdf` covers Limited Ballot poll lists (SOS form 5-38) printed to PDF (needs
`src\pdf_reader.c`): names as separate merge-field runs (a two-word first name, a two-word
surname, an empty middle name with "BABIN Jr", a surname drawn in two pieces) or one run
("THOMPSON, JR"), a symbol-glyph row number, an assisting person, an unused numbered line,
a second page, the election date drawn in pieces, and a form inside a ZIP whose file-name
year is wrong (Date Voted blank, noted), plus a TSV round trip;
`counties` covers the other county rosters as loose CSVs: Hidalgo (CERT / CHECK-IN with
"EV-" / "ED-" SITE values as the method, an ABBM list with RETURNDATE "11-FEB-20" and
STATE_VOTER_ID, a VoterName list whose file name spells out the election date), Bexar
(files named only by election and date: the Texas election calendar makes 10-21-2024
early voting and 11-05-2024 Election Day), Brazoria (an "IN PERSON" file with a mail
list's Ballot Received Date title; a 2026 bilingual roster with title rows, a translation
row and the VUID / name columns swapped), Montgomery (one row per jurisdiction, merged
with Election_Name joined; "BBM Recvd" dates; CountyElectionPrecinct), Galveston's
"LAST  FIRST MIDDLE" names vs a Dallas sheet's "SANDRA  GLEDHILL", Williamson's
Voting Method "AV" (mail) on a 10/13 ballot (outside the 17-day early-voting window) and a
blank method cell in an all-"AV" column, Collin's mail list with a ballot not received yet
("-": skipped, noted) and its check-in sheet (CertificateNumber = VUID, VoteDate);
`sosroster` covers the Texas Secretary of State's turnout rosters: a loose statewide early
voting CSV (`COUNTY`, `ID_VOTER`, MAIL-IN / IN-PERSON, a "Jr" suffix, no Date Voted) and
Election Day ZIPs whose roster is `STATEWIDE_VOTER_INFO.csv` for both parties (named after
the ZIP, so the party comes from it), a polling-place file beside it (reference, not read),
the same ZIP saved under two download dates (loaded once), a voter on both parties'
rosters, and Election Day dates from the election name (primary 03/03/2026, primary runoff
05/26/2026, 2024 general 11/05/2024; mail rows and a special election blank);
`dupvote` covers `EeVoterTable_MarkDuplicateVotersVoting` (Show Duplicate Voters Voting):
two voters with the same name and DOB who both voted, a group where only one voted, one
Voter ID on two rows (not two people), the same DOB written two ways, and a list without
birth dates.)
