# Session Handoff — ElectionExplorer

> Live handoff note passed between the desktop and laptop via git.
> Update this at the end of each session; read it at the start of the next.
> Keep it short and current — git history is the permanent record.

**Last updated:** 2026-09-03
**Updated on:** travel laptop
**Branch:** main — working tree clean at time of writing
**Latest commit:** `20fe6aa Initial check for duplicate voter records with same name and DOB`

---

## What ElectionExplorer is

Win32 GUI (C11, no MFC, static CRT, VS 2026 `v145`; x64/ARM64 Debug+Release via
`Applications/Applications.sln`). Loads large county voter-list exports
(CSV/TSV) in the background into an in-memory table and shows them in a
dual-pane virtual grid with frozen columns (Voter ID, Precinct, Name, Address),
sorting, filtering, and address normalization.

Source: [ElectionExplorer/src/](ElectionExplorer/src/) —
`main.c` (window/UI), `voter_table.c` (load + storage + normalization),
`filter.c` (filter dialog + predicates).

## Current state

Recent work (newest first) has centered on data-quality checks and normalized
display:

- Duplicate detection: duplicate Voter IDs, and an initial check for duplicate
  voter records sharing the same name + DOB.
- Normalized data pane: added Precinct; improved normalized address processing
  and block-number handling.
- Partial / imperfect birth dates handled during parse and filtering.

## In progress / open questions

- _(none recorded yet — first handoff note. Fill this in with whatever is
  mid-flight when you stop.)_

## Next steps

- _(Add the concrete next actions here before switching machines.)_

## Notes for the next session

- The user does all git commit/push/pull by hand (see
  [CLAUDE.md](CLAUDE.md)). Don't run git write commands.
- Build/test per [AGENTS.md](AGENTS.md): MSBuild via `Applications.sln`,
  e.g. `msbuild Applications.sln /p:Configuration=Debug /p:Platform=x64`.
