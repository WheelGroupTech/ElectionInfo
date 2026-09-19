# Design: XLSX import for ElectionExplorer

**Status:** Implemented — Phases 1–3 + the GUI sheet picker and `*.xlsx` File→Open
filter. Reads shared/inline strings, numbers, booleans, error cells; converts
date serials to `YYYY-MM-DD` and preserves zero-padded formats (e.g. ZIP
`00000`) via `styles.xml`; detects cells whose only content is an anchored
picture (see §Write-in images) and marks them `[write-in]`. Tested (`xlsx`,
`xlsxfmt`, `writein` round-trip tests; verified on a real workbook). Possible
future work: broader number-format coverage; streaming very large parts instead
of extract-to-heap.
**Date:** 2026-09-17
**Scope:** Add the ability to open and extract tabular data from Microsoft Excel
`.xlsx` workbooks, as an alternative input to the existing CSV/TSV loader.

> Decisions are recorded in [Decisions (resolved)](#decisions-resolved-2026-09-17).

---

## 1. Motivation

A large share of election data is distributed as Excel `.xlsx` — voter
registration lists, vote rosters, cast vote records, ePollbook exports, etc.
Today ElectionExplorer only reads delimited text (CSV/TSV), so users must first
re-export or convert those files, which is error-prone (Excel silently mangles
Voter IDs and ZIP codes on CSV export — see §6). Reading `.xlsx` natively removes
that step and preserves fidelity.

## 2. Goals / non-goals

**Goals**
- Open a `.xlsx` file and feed its rows into the **existing** load pipeline so all
  current behavior (header detection, Voter ID / Precinct / Name / Address roles,
  address normalization, duplicate/compare/report features) works unchanged.
- Stream with low memory, consistent with today's multi-million-row CSV handling.
- Stay within the tree's constraints: C11, no MFC, static CRT, avoid COM, with
  security validation at the input boundary (AGENTS.md).

**Non-goals (initially)**
- Writing/editing `.xlsx` (read-only).
- Formulas evaluation (we read cached values, which `.xlsx` always stores).
- `.xls` (legacy BIFF binary) and `.xlsb` (binary) — different formats; out of scope.
- Charts, pivot tables, formatting beyond what's needed to read values correctly.

## 3. Background: what an `.xlsx` is

An `.xlsx` is a ZIP archive (Open Packaging Convention) of XML parts. The parts
we need:

| Part | Why we read it |
|---|---|
| `xl/workbook.xml` | sheet list (names + `r:id`); date system (`date1904`) |
| `xl/_rels/workbook.xml.rels` | maps each sheet's `r:id` → `xl/worksheets/sheetN.xml` |
| `xl/sharedStrings.xml` | shared string table; most text cells reference it by index |
| `xl/worksheets/sheetN.xml` | the rows and cells |
| `xl/styles.xml` | `cellXfs` → `numFmtId`; needed to distinguish dates from numbers |

Everything else in the archive is ignored.

## 4. Architecture

A new module produces **rows of UTF-8 strings** — exactly the shape the CSV
tokenizer already yields — and hands them to a shared row consumer. The loader
dispatches on file extension.

```
                         .csv/.tsv ─► existing delimited tokenizer ─┐
EeVoterTable_LoadFromFile ┤                                          ├─► shared row sink ─► table
                         .xlsx ─────► EeXlsx_ReadSheet ─────────────┘   (header detect,
                                       (this design)                     column roles,
                                                                         compose_address, …)
```

Proposed public API (`src/xlsx.h`):

```c
/* Receives one row at a time. cells[i] is a NUL-terminated UTF-8 string
 * ("" for blank cells); ncells is the row width. Return FALSE to abort. */
typedef BOOL (*EeRowSink)(void *ctx, const char *const *cells, uint32_t ncells);

EeLoadStatus EeXlsx_ReadSheet(const wchar_t *path,
                              int sheet_index,          /* 0 = first sheet */
                              EeRowSink sink, void *ctx,
                              EeProgressFn progress, void *progress_ctx,
                              volatile LONG *cancel,    /* like the CSV path */
                              wchar_t *err, size_t err_cch);

/* Enumerate sheet names (for a picker); read from workbook.xml only. */
int EeXlsx_ListSheets(const wchar_t *path, wchar_t names[][64], int max, ...);
```

**Refactor required:** today the CSV loader tokenizes *and* ingests rows in one
place. We split out the ingestion half (header row → column roles → per-row
`compose_*`) into a reusable sink that both CSV and XLSX feed. This is the only
change to existing code; the delimited path keeps identical behavior.

## 5. Component design

### 5.1 OPC / ZIP layer

Windows has no clean public API for the raw DEFLATE used by ZIP (the Compression
API covers XPRESS/MSZIP/LZMS only), so this needs a small vendored decompressor.
Two candidates:

| | **Option A: miniz** | **Option B: puff.c + our ZIP reader** |
|---|---|---|
| Third-party code | ~a few thousand lines (single file, public domain) | ~2 KB `puff.c` (public domain) + ~150 lines we write |
| ZIP central directory | built in (`mz_zip_reader`) | we write it (local/central headers, method 0/8) |
| Speed | fast | adequate (puff is size-optimized, not speed) |
| Auditability | larger surface | small, fully ours except tiny puff |
| Build | one `.c`, compiles as C11 | two small `.c` |

Both compile with `/std:c11` and the static CRT and add no runtime DLL. Either is
a **new dependency requiring sign-off per AGENTS.md**; the vendored source would
live under `src/third_party/` with its license header preserved.

> **Decision 1: Option A (miniz).** Signed off 2026-09-17. Vendor the official
> single-file amalgamation (`miniz.c` / `miniz.h`, public domain / MIT-0) under
> `src/third_party/miniz/`, pinned to a specific release, license header kept.

Only these access patterns are needed: open archive → locate an entry by exact
name → inflate it into a bounded buffer (or stream). We never honor stored paths,
so no path-traversal risk.

### 5.2 XML layer

A **small streaming tag scanner** purpose-built for these parts — not a general
XML parser, and explicitly **not** MSXML/COM (heavy; DOM would load whole parts
into memory, bad for large `sharedStrings`). It recognizes only the elements we
need and unescapes the five predefined entities plus numeric character references
(`&#…;` / `&#x…;`). It ignores/《rejects》 DTDs and never expands external or
custom entities (see §7). This matches the codebase's existing hand-rolled-parser
style.

### 5.3 Cell model

Per-cell handling in `sheetN.xml` (`<c r="B5" t="…" s="…"><v>…</v></c>`):

- `t="s"` → `<v>` is an index into the shared string table.
- `t="str"` → `<v>` is an inline formula-result string.
- `t="inlineStr"` → text under `<is><t>…</t></is>`.
- `t="b"` → boolean → emit `TRUE` / `FALSE` (decision 5).
- `t="e"` → error → emit the error text verbatim, e.g. `#N/A` (decision 5).
- no `t` (default `n`) → number → see §6.
- **Empty cells are omitted.** Cells carry `r="B5"`; we parse the column letters
  (`A`, `B`, …, `AA`) to place values by column index and fill gaps with `""` so
  every row has a consistent width aligned to the header.

The shared string table is interned once into a UTF-8 pool (same idea as the
table's existing string pool) and indexed; cells reference by position.

## 6. Fidelity: numbers, dates, IDs, ZIPs (the important part)

Excel stores *values*, not the displayed text. Reading raw values corrupts
exactly the fields election data cares about:

- **Leading zeros / scientific notation.** A ZIP `07001` stored as a number reads
  back `7001`; a 10-digit Voter ID may read as `1.23457E+09`. The padded/normal
  display exists only via the cell's number format.
- **Dates as serials.** A date is a number (e.g. `45838`) with a date *format*.
  Without `styles.xml` we get the serial, not `2025-06-15`. Also: the 1900 vs
  1904 date system (`workbook.xml`), and Excel's 1900-is-a-leap-year bug.

Mitigation: read `styles.xml` `cellXfs[s].numFmtId`, classify the format (builtin
date IDs 14–22/45–47, or a custom `numFmt` whose code contains date tokens; else
numeric), and:
- date-formatted number → emit an ISO-ish date string the existing date parser
  understands;
- other numbers → emit a plain decimal string **without** scientific notation and
  **without** dropping integer precision (so IDs stay intact).

This is the fiddliest piece and is isolated in **Phase 3** so earlier phases can
land and be useful first. (Fully reproducing arbitrary custom number formats is
out of scope; we target the date/general/text cases that matter here.)

### 6.1 Write-in images (cells whose content is a picture)

Some exporters store a value as a *picture* anchored over the cell rather than as
cell text — notably ES&S CVR exports, which paste a scanned snippet of the
voter's handwritten **write-in** into the contest cell. The cell itself is empty,
so a naive reader shows it blank even though the ballot recorded a selection.

Detection (no OCR — presence only):

- A worksheet that has pictures references a drawing part via
  `xl/worksheets/_rels/sheetN.xml.rels` (`Relationship` `Type` ending `/drawing`
  → `Target`, resolved relative to `xl/worksheets/`).
- `xl/drawings/drawingM.xml` holds one anchor per picture; each
  `<xdr:twoCellAnchor>`'s `<xdr:from>` gives the 0-based `<xdr:col>` / `<xdr:row>`
  of the cell the image sits in.
- We collect those `(row, col)` anchors (sorted) and, while streaming rows, when a
  row's 0-based worksheet index carries an anchored image over an **empty** cell,
  emit the marker `[write-in]` instead of `""`. Cells that already have text are
  left untouched.

The handwritten text is a raster image, so only the *presence* of a write-in is
surfaced, not its value. This lives entirely in the reader, so any `.xlsx`
benefits; in practice it matters for CVRs. Tested by `writein` (authors a
workbook with a worksheet-rels → drawing → anchor) and verified on real Travis
County G24 CVRs (President write-ins that had read blank now show `[write-in]`).

## 7. Security (untrusted input)

- **Zip bombs:** cap per-entry uncompressed size and total, and the compression
  ratio; abort with a clear error rather than allocating unbounded memory.
- **XML entity expansion (billion laughs):** no DTD processing, no custom/external
  entities — only the 5 predefined + numeric refs, with a bound on expansion.
- **Malformed archive/XML:** every parse step is bounds-checked and fails to
  `EeLoadStatus_Error` with a message; never trusts lengths/offsets from the file.
- We read only fixed, known part names; stored entry paths are never used to open
  anything on disk.

## 8. Integration & build

- `EeVoterTable_LoadFromFile` gains an extension check → XLSX path vs delimited
  path; both converge on the shared row sink (§4).
- New files: `src/xlsx.c`, `src/xlsx.h`, vendored decompressor under
  `src/third_party/`. Add to `.vcxproj` + `.filters`.
- File-open dialog filter gains `*.xlsx` (and lists it alongside CSV/TSV).
- AGENTS.md: the vendored dependency is called out for sign-off in the PR; license
  header preserved; compiled as C11.

## 9. Testing

- Unit/smoke tests mirroring the CSV tests, asserting the same normalized output
  from an equivalent workbook (shared strings, inline strings, a numeric Voter ID,
  a text ZIP with a leading zero, a date-formatted DOB, gapped/empty cells).
- **Fixture strategy — decision 4: Option (b), the generator.** A Python
  (`openpyxl`) generator under `docs/sample-data/` produces the `.xlsx` fixtures,
  and/or a self-contained C test writes a minimal `.xlsx` at runtime, keeping the
  repo binary-free and reproducible.

## 10. Phasing

1. **ZIP/inflate** + read parts by exact name (vendored **miniz**).
2. **XML → rows:** shared strings, inline strings, values as text/number,
   gap-filling, `t="b"`→TRUE/FALSE, `t="e"`→error text, wired into the shared row
   sink; plus **sheet enumeration + a sheet picker** shown before load
   (decision 3); file dialog `*.xlsx` filter; progress + cancel; the Python
   fixture generator + tests. *Opens real spreadsheets — this is v1 (decision 2).*
3. **Fidelity (follow-up release):** `styles.xml` → dates (serial→text),
   leading-zero / no-scientific number formatting.

**v1 = Phases 1–2** (decision 2). Phase 3 follows. Known v1 limitation to
document in-app: numbers come through as stored, so a numeric Voter ID or ZIP may
lose leading zeros / show many digits until Phase 3 lands — text-formatted columns
are unaffected.

Estimated size: ~1,000–2,000 lines for v1 (Phases 1–2) including the sheet picker,
excluding vendored miniz — over the AGENTS.md escalation bar, hence this doc.

## 11. Risk / impact

Additive and dispatched by extension: the CSV/TSV path is untouched except for the
row-sink refactor (covered by existing tests). The main risks are the vendored
dependency (mitigated by sign-off + small surface) and number/date fidelity
(mitigated by isolating Phase 3 and testing IDs/ZIPs/dates explicitly).

## Decisions (resolved 2026-09-17)

1. **ZIP engine:** ✅ Option A — vendor **miniz**. (§5.1)
2. **Fidelity depth for v1:** ✅ Ship **Phases 1–2 first** (values as stored, no
   date/number conversion); Phase 3 follows. (§6, §10)
3. **Multiple sheets:** ✅ **Sheet picker up front** — enumerate sheets and let the
   user choose before load; part of v1. (§5.3, §10)
4. **Test fixtures:** ✅ Option (b) — a **generator** (Python/`openpyxl`) +
   runtime-built minimal `.xlsx`; no binaries checked in. (§9)
5. **Boolean/error cells:** ✅ `t="b"` → `TRUE`/`FALSE`; `t="e"` → error text
   verbatim. (§5.3)
