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
  test\smoke_load.c src\voter_table.c src\filter.c src\settings.c src\xlsx.c src\ee_cvr.c ^
  src\third_party\miniz\miniz.c /Fe:test\smoke_load.exe /link /SUBSYSTEM:CONSOLE user32.lib advapi32.lib
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
not); `cvrws` covers whitespace normalization of selection values (`John   Cornyn`
-> `John Cornyn`, trimmed ends, merged tally).)
