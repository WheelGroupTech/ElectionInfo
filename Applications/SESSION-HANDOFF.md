# Session Handoff — ElectionExplorer

> Live handoff note passed between the desktop and laptop via git.
> Update this at the end of each session; read it at the start of the next.
> Keep it short and current — git history is the permanent record.

**Last updated:** 2026-09-03
**Branch:** main — clean, pushed to origin
**Latest commit:** `207da43 perf(explorer): O(n) duplicate scans + row-mark view + cancelable modal`

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

**Duplicate-detection speedup — DONE, committed `207da43` and pushed.** Made the
"duplicate voters (name + DOB)" and "duplicate Voter IDs" features fast and
GUI-responsive. Measured on real data: Dallas County 1.48M rows → name+DOB scan
**373 ms** (7,113 dup rows), Voter-ID **213 ms**; previously minutes / effectively
hung. Changed files: `voter_table.{c,h}`, `main.c`, `resource.h`,
`test/smoke_load.c`.

What changed:
- **Root cause was the display, not just the scan.** The old code turned results
  into one `EeRel_Is` filter rule per duplicate Voter ID, then evaluated every row
  against every rule — O(rows × dup_ids). Replaced with a per-window row-**mark**
  layer.
- New `EeVoterTable_MarkDuplicateVoterIds` / `...VotersByNameDob`: O(n)
  open-addressing hash grouping that marks physical rows directly, with progress +
  cancel callbacks. Old `Collect*` kept as thin wrappers (existing smoke tests
  still green); added `test_mark_duplicates`.
- `AppState` gained `mark_rows/mark_count/mark_active/mark_kind`, ANDed with the
  filter in `App_ApplyFilter`; Reset clears it. Marks are per physical row so they
  survive sorts.
- Large tables (≥ `k_ScanModalMinRows` = 250k) scan on a worker thread behind the
  existing progress modal (reused load machinery; `EEM_SCAN_PROGRESS/FINISHED`,
  `scan_*` fields), with a working Cancel. Smaller tables run synchronously.
- Design decisions confirmed with the user: duplicates AND with active filters;
  apply the same fast path to Voter-ID dupes too.

Verified: x64 Debug **and** Release build clean (0 warnings); smoke tests all pass
(`dupvuid`, `dupvoter`, new `markdup`, plus the rest). Plan file:
`~/.claude/plans/quiet-enchanting-lovelace.md`.

## Next steps

- Future (explicitly deferred this round): multi-file compare between open windows
  (the mark layer is the intended primitive for it), and a reaper-thread for
  responsive deletion of large row sets.

## Notes for the next session

- The user does all git commit/push/pull by hand (see
  [CLAUDE.md](CLAUDE.md)). Don't run git write commands.
- Build/test per [AGENTS.md](AGENTS.md): MSBuild via `Applications.sln`,
  e.g. `msbuild Applications.sln /p:Configuration=Debug /p:Platform=x64`.
