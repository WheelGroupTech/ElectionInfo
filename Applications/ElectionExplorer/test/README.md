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
  test\smoke_load.c src\voter_table.c src\filter.c /Fe:test\smoke_load.exe /link /SUBSYSTEM:CONSOLE user32.lib
test\smoke_load.exe
```
