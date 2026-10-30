# Scanned results PDFs — survey (2026-10-29)

What the results PDFs in `Election_Results/` that Election Explorer refuses as "no text to
read (a scanned report?)" actually are, and what reading them would take.

**Status (2026-10-29): steps 1–3 below are done for 1.6.0.0** (`src/ee_results_ocr.c`; see the
election-results design doc, "Scanned and ASCII85 results PDFs"). Not read: the scanned Hart reports,
the SOS forms, letters, tapes and tally sheets, JPX/JBIG2 images, Liberty's turned per-entity files.

Scratch tools for this survey are in the desktop session scratchpad (`scan/`), not in the repo:
`survey.py`, `encodings.py`, `classify.py`, `ocr_trial.py`, `fails.py`, and `ocr_png.c` (the app's
`ocr_win.c` on image files).

## The 262 refused files are three different problems

The whole-folder sweep (1.6 with Dominion, 3,332 of 3,993 files loading) refuses 262 files as
having no text: 261 PDFs and Ellis County's ZIP of one PDF.

| Kind | Files | Pages | Fix |
|---|---:|---:|---|
| Text PDFs with ASCII85-encoded content (Cameron, ReportLab) | 38 | ~600 | `pdf_reader.c`: add ASCII85Decode |
| Text reports behind a scanned cover page (Liberty L24 canvass) | 11 | 37 | look past image-only leading pages |
| True scans: page images only | 211 | 8,393 | OCR |
| Other (Gregg's hash-comparison sheet, Ellis's ZIP) | 2 | 26 | — |

### Text PDFs with ASCII85 content (38, Cameron)

- **What they are:** Cameron's per-entity reports from 2024–2026 (L24 "Precinct-by-Precinct",
  L26 "…_PCTxPCT" and "…_Summary", G25 "…_IAG"), produced by ReportLab.
- **The text is real:** PyMuPDF reads it, and the layout is an Electionware precinct summary:
  "Statistics / TOTAL MAIL EV ED", "Vote For 1", "Overvotes".
- **Why they are refused:** ReportLab compresses content streams as
  `/Filter [/ASCII85Decode /FlateDecode]`, and `pdf_reader.c` decodes only FlateDecode, so page
  extraction fails ("page 1: extract failed").
- **Fix:** add ASCII85Decode, and filter chains. These are the only ASCII85 PDFs in the folder.
  Then check that the ES&S reader takes their layout; their Producer is ReportLab, not iText or
  JasperReports.

### Text behind a scanned cover page (11, Liberty L24)

- **What they are:** "Canvass Results-5-10-2024 <entity>.pdf". Page 1 is a scanned, signed
  certificate (an image). Pages 2+ are a Hart canvass report as real text (Segoe UI fonts).
- **Why they are refused:** the reader decides from the first page or two, and gives up.
- **Fix:** skip leading pages that hold only an image before deciding.

## The true scans (211 files, 8,393 pages)

### By county

| County | Files | Pages | Notes |
|---|---:|---:|---|
| Cameron | 156 | 7,300 | RICOH copier scans, 200–260 dpi, many 1-bit |
| Liberty | 3 | 97 | |
| Midland | 13 | 209 | RICOH, 230–390 dpi |
| Angelina | 9 | 52 | Secretary of State canvass forms, letters, memos, Electionware |
| Guadalupe | 6 | 44 | |
| Grayson | 4 | 51 | |
| Tom Green | 4 | 260 | P26 is 222 pages of handwritten tally sheets |
| Williamson | 1 | 173 | G22: scanned tabulator results tapes |
| 13 others | 1–3 each | 1–24 | |

### By layout (OCR of a middle page)

| Layout | Files | Pages | Where |
|---|---:|---:|---|
| ES&S legacy (ERM) monospaced reports ("SUMMARY REPT-GROUP DETAIL", "(WITH n OF n PRECINCTS COUNTED)") | 119 | 5,860 | Cameron 2020–2023 |
| ES&S Electionware reports (Summary / Precinct Summary Results Report) | 58 | 1,837 | Cameron 1,567 pages; Midland, Guadalupe, Tom Green, Grayson, Webb, Walker, Angelina, Bowie |
| Hart reports (cumulative, canvass) | 9 | 42 | Angelina, Galveston, Coryell, Tom Green, Ector, Randall |
| Texas SOS canvass forms | 3 | 15 | Angelina |
| Other | 22 | 639 | see below |

The "other" group:
- **Content turned sideways inside the image** (OCR read it backwards): Midland P20 precinct
  reports, Grayson G20, Liberty L22.
- **Not results reports:** Williamson G22's results tapes (173 pages), Tom Green P26's handwritten
  tally sheets (222 pages), and emails and letters (Angelina, Bowie).
- **A few report styles of their own:** Cameron P24 "Election Summary Report", Travis LR22's
  reconciliation form, Montgomery's bond notice.

### Image encodings (first page)

| Encoding | Files | Pages | Read by `EePdf_GetPageImage` today |
|---|---:|---:|---|
| CCITT G4 (1-bit fax) | 112 | 6,480 | no |
| JPEG (DCTDecode), alone or after Flate | 95 | ~1,650 | JPEG alone yes; Flate+JPEG no |
| JPEG 2000 (JPX) | 3 | 38 | no |
| JBIG2 | 1 | 222 | no |

65 files have rotated pages (page /Rotate 90 or 270), and a few more have the content turned
inside an upright page.

### Coverage: is the scan the only source?

- **Only source:** 49 of the 63 elections with scans have no other file that loads. They hold
  8,046 of the 8,393 pages.
- **Cameron:** 26 of those elections (2020–2025), about 7,400 pages. P22 alone is 2,741 pages
  and P20 is 1,272.
- **The others:** Midland (6 elections), Guadalupe (5), Angelina (3), Grayson (2), Galveston (2),
  Tom Green (2), and one each for Brazos, Ector, Liberty, Potter, Walker, Webb and Williamson.
- **Already covered:** the other 14 elections also have a text PDF or Clarity file that loads.

## OCR trial

- **Method:** two pages of every true scan (the first and the middle), rendered at 300 dpi and read
  by Windows OCR through the app's `ocr_win.c`.
- **Speed:** about 0.2 s per page for OCR, plus rendering.
- **The check:** a results table row gives its total first and then its vote-type columns, so the
  first number should equal the sum of the rest (percentages left out). This uses raw OCR lines
  grouped into rows by position, with no knowledge of the layout.

| Layout | Files | Rows checked | Rows that add up |
|---|---:|---:|---:|
| ES&S legacy (ERM) | 119 | 2,056 | 1,749 (85%) |
| ES&S Electionware | 58 | 742 | 609 (82%) |
| Other | 21 | 91 | 38 |
| Hart | 9 | 88 | (not meaningful: Hart prints the total last) |

- **The digits read well.** On a Cameron G20 page every row checked by hand adds up, for example
  Sergio Lopez 38,898 = 29,395 + 2,765 + 6,644 + 94. OCR does add stray spaces inside numbers
  ("39 , 497").
- **The failures are mostly cells OCR left out**, not misread digits. For example, "Oscar Longoria
  203 | 60.24 | 126 | 10 | 0" lacks one vote-type count, and "Total 335 | 201 | 120" lacks 14.
- **A layout-aware reader would see these:** it places each word by its x position, sees the empty
  column, and can fill a single missing count from the row's total (and check it against the
  contest's Total row). It can also mark a row that still does not add up for review, as the Hart
  OCR reader does ("Corrected" / "Review").

## What reading the scans would take

1. **Page images:**
   - CCITT G4 needs decoding. `pdf_reader.c` could wrap the G4 data in a TIFF header and let WIC
     (which `ocr_win.c` already uses) decode it.
   - Flate+JPEG is a short step. JPX and JBIG2 are 4 files, so leave them.
2. **Orientation:**
   - Apply the page's /Rotate.
   - For content turned inside the image, try the four orientations on the first page and keep
     the one with the most words OCR is sure of.
3. **OCR words to positioned text:** each word becomes a text run at its pixel position scaled to
   page space, with a font size from its height. Then the existing readers can work on them.
4. **Readers:**
   - The ES&S legacy (ERM) layout is new: monospaced columns, "(VOTE FOR) n", "(WITH n OF n
     PRECINCTS COUNTED)", a precinct header per page, Total / Over Votes / Under Votes rows. It is
     5,860 pages of Cameron 2020–2023.
   - Electionware scans should go through the existing Electionware reader once the text is
     positioned (1,837 pages).
   - Hart scans through the existing Hart readers (42 pages).
5. **Checking:**
   - Every row's total must equal the sum of its vote types, and every contest's Total row the
     sum of its choices.
   - Fill a single missing count; mark the rest for review.
   - The Load Summary says the file was read by OCR, and how many rows were corrected or need
     review.
6. **Time:** about 0.3 s per page in all. Most files are under 50 pages. Cameron's two primaries
   (P22 2,741 pages, P20 1,272) would take several minutes each, behind the progress dialog with
   Cancel, as the scanned Hart CVR reports do today.

Not worth reading as results:
- the handwritten tally sheets (Tom Green P26);
- letters, emails and the SOS forms (a few pages);
- the results tapes, which are a separate feature (the repo's `Scripts/Results_Tapes/` covers
  tapes).

## Suggested order

1. **ASCII85 and the scanned cover page:** small reader changes, 49 more files. They are text,
   not OCR.
2. **OCR for Electionware and Hart scans**, using the existing readers: about 67 files, 1,880
   pages.
3. **The ES&S legacy (ERM) reader with OCR:** 119 files, 5,860 pages, Cameron 2020–2023.
