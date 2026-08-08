# Applications

Compiled election tools live under this directory. Analysis scripts (Python)
remain in sibling top-level folders of the repository (`ES&S/`,
`Travis_County_Elections/`, and so on).

Agent policy for native Windows code is in [`AGENTS.md`](AGENTS.md).

---

## Layout overview

```text
Applications/
  Applications.sln                 # Visual Studio solution (all compiled tools)
  AGENTS.md                        # Agent / coding policy for this tree
  README.md                        # This file
  Directory.Build.props            # Shared MSBuild defaults (OutDir/IntDir)
  Build/                           # All build output (gitignored)
    win-x64-release/
      ElectionExplorer/
        ElectionExplorer.exe
        ...
    win-x64-debug/
      ElectionExplorer/
    win-arm64-release/
      ElectionExplorer/
    win-arm64-debug/
      ElectionExplorer/
  ElectionExplorer/                # One directory per application
    ElectionExplorer.vcxproj
    ElectionExplorer.vcxproj.filters
    src/
    test/
    docs/
```

---

## Solution and projects (Visual Studio)

| Item | Location | Notes |
|------|----------|--------|
| Solution | `Applications/Applications.sln` | Single solution for all compiled tools |
| App project | `Applications/<AppName>/<AppName>.vcxproj` | First app: `ElectionExplorer` |
| Shared MSBuild props | `Applications/Directory.Build.props` | Common `OutDir` / `IntDir` mapping |
| App source | `Applications/<AppName>/src/` | Production C sources and headers |
| App tests | `Applications/<AppName>/test/` | Unit / integration tests for that app |
| App docs | `Applications/<AppName>/docs/` | Design notes, user docs for that tool |

### Rules

1. **One solution** at `Applications/` for the compiled-tool family. Do not
   scatter independent `.sln` files unless a maintainer approves an exception.
2. **One directory per application**, named after the tool
   (`ElectionExplorer`, not `election_explorer` or `ee`).
3. **Project file lives with the app**, not under `Build/` and not at the
   solution root (except shared props / targets).
4. **Do not place production compiled-tool source** in the root Python folders
   (`ES&S/`, `Results_Tapes/`, `Travis_County_*`, etc.).
5. **Do not place source or project files under `Build/`.** That tree is
   output-only.

### Planned Visual Studio configurations

Projects should expose the usual Debug / Release configurations and at least
these platforms:

| VS Platform | VS Configuration | Output directory name |
|-------------|------------------|------------------------|
| `x64` | `Release` | `win-x64-release` |
| `x64` | `Debug` | `win-x64-debug` |
| `ARM64` | `Release` | `win-arm64-release` |
| `ARM64` | `Debug` | `win-arm64-debug` |

Directory names are always lowercase:

```text
<os>-<arch>-<config>
```

Examples: `win-x64-release`, `win-arm64-debug`. Prefer this pattern later for
other OSes (`linux-x64-release`, `macos-arm64-debug`) rather than inventing
ad-hoc names.

### Output and intermediate directories

Configure every project so artifacts land under `Applications/Build/`:

| Kind | Path pattern |
|------|----------------|
| Final outputs (`OutDir`) | `Build/<os>-<arch>-<config>/<AppName>/` |
| Intermediate objects (`IntDir`) | `Build/<os>-<arch>-<config>/<AppName>/obj/` |

Example for ElectionExplorer, x64 Release:

```text
Applications/Build/win-x64-release/ElectionExplorer/ElectionExplorer.exe
Applications/Build/win-x64-release/ElectionExplorer/obj/   (*.obj, *.tlog, ...)
```

`Directory.Build.props` maps `Platform` / `Configuration` to those folders and
sets `OutDir` / `IntDir` for every project under `Applications/`. Do not rely on
Visual Studio’s default `x64\Release\` next to the project file.

### Runtime footprint (no redistributable install)

ElectionExplorer is configured for portable Win32 deployment on **Windows 10/11**
(**x64** and **ARM64**):

| Setting | Value | Why |
|---------|--------|-----|
| CRT | Static (`/MT` Release, `/MTd` Debug) | No `VCRUNTIME` / `MSVCP` / UCRT redistributable DLLs |
| UI framework | Raw Win32 (no MFC, no ATL) | Only OS system DLLs (`user32`, `gdi32`, …) |
| Language | ISO C11 | Matches `AGENTS.md` C-first policy |
| Subsystem | Windows | GUI entry (`wWinMain`), not console |
| Toolset | `v145` (VS 2026) | Matches installed Enterprise 18.x |
| Min OS macros | `WINVER` / `_WIN32_WINNT` = `0x0A00` | Windows 10+ (includes Windows 11) |

### Build commands

From `Applications/` (or via the Visual Studio IDE):

```powershell
msbuild Applications.sln /p:Configuration=Release /p:Platform=x64
msbuild Applications.sln /p:Configuration=Release /p:Platform=ARM64
```

MSBuild path (VS 2026 Enterprise example):

`C:\Program Files\Microsoft Visual Studio\18\Enterprise\MSBuild\Current\Bin\MSBuild.exe`

### What belongs in the app output folder

| Include | Exclude / keep under `obj/` |
|---------|-----------------------------|
| `.exe` (or `.dll` for libraries) | Compiler/linker intermediates (`.obj`, `.tlog`) |
| Runtime dependencies required to run the tool | Browse/intellisense DB noise when avoidable |
| `.pdb` for the matching configuration (especially Debug) | Copies of source |

`Build/` is never committed (see repository `.gitignore`).

---

## Application directory contract

Each compiled tool directory SHOULD contain:

| Path | Purpose |
|------|---------|
| `src/` | Application source (C for Win32 / native code; see `AGENTS.md`) |
| `test/` | Tests for this application |
| `docs/` | App-specific documentation |
| `<AppName>.vcxproj` (+ `.filters`) | Visual Studio project |

Optional later, when needed:

| Path | Purpose |
|------|---------|
| `include/` | Public headers if this app exposes a library surface |
| `res/` | Resources (icons, manifests, `.rc`) |
| `bench/` | Microbenchmarks for performance-sensitive code |

Shared code used by more than one tool SHOULD live under a dedicated tree such
as `Applications/common/` or `Applications/libs/` (add when the second consumer
appears), not by copying files between apps.

---

## Language and policy

- Win32 / native Windows production code: **C11 first** — see [`AGENTS.md`](AGENTS.md).
- Python remains appropriate for one-off analysis scripts at the **repository
  root** folders, not as the runtime implementation of tools under
  `Applications/`.
- Build/test automation may use Python (Tier 3 tooling) where helpful.

---

## Adding a new compiled tool

1. Create `Applications/<AppName>/` with `src/`, `test/`, and `docs/`.
2. Add `<AppName>.vcxproj` (C project, C11, UNICODE) under that directory.
3. Add the project to `Applications.sln`.
4. Ensure `OutDir` / `IntDir` follow the `Build/<os>-<arch>-<config>/<AppName>/`
   layout (prefer inheriting `Directory.Build.props`).
5. Do not commit anything under `Build/`.

---

## First tool: ElectionExplorer

| Item | Path |
|------|------|
| Project | `ElectionExplorer/ElectionExplorer.vcxproj` |
| Source | `ElectionExplorer/src/` (`main.c`, `voter_table.c`, …) |
| Tests / samples | `ElectionExplorer/test/` |
| Docs | `ElectionExplorer/docs/voter-list.md` |
| Release x64 binary (after build) | `Build/win-x64-release/ElectionExplorer/ElectionExplorer.exe` |

**Current capability:** Win32 GUI (C11, no MFC, static CRT) that loads large
voter registration CSV/TXT files on a background thread and shows them in a
DPI-aware dual-pane grid (frozen Voter ID + Name, sortable columns). See
[`ElectionExplorer/docs/voter-list.md`](ElectionExplorer/docs/voter-list.md).

Open `Applications.sln` in Visual Studio 2026 to develop. Build outputs appear
under `Build/` as described above.
