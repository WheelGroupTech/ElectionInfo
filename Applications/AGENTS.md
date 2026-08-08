# AGENTS.md

This file defines the behavior, constraints, coding policies, and operational
guidelines for all AI agents (Copilot, automated pipelines, CI bots, etc.)
that read, write, or review code under the `Applications/` tree.

Human layout documentation (full tree diagrams, how to add a tool) lives in
[`README.md`](README.md). Keep this file **normative and short** for agents.

---

## Table of Contents

1. [Scope](#scope)
2. [Repository Layout](#repository-layout)
3. [General Agent Behavior](#general-agent-behavior)
4. [Constraints](#constraints)
5. [Language Policy](#language-policy)
   - [Win32 / Native Windows Code — C First](#win32--native-windows-code--c-first)
   - [Other Language Tiers](#other-language-tiers)
6. [Code Style & Formatting](#code-style--formatting)
7. [Security Policy](#security-policy)
8. [Testing Requirements](#testing-requirements)
9. [Pull-Request & Commit Policy](#pull-request--commit-policy)
10. [Escalation & Human Review](#escalation--human-review)
11. [Changelog](#changelog)

---

## Scope

These guidelines apply to:

- All work under `Applications/` (compiled election tools, Visual Studio
  solution/projects, shared build props, and app `src` / `test` / `docs`).
- AI-assisted generation, refactoring, or review of that tree.
- Automated CI/CD agents that open pull requests, apply patches, or run
  scripts affecting `Applications/`.

These guidelines do **not** replace conventions for top-level Python analysis
folders (`ES&S/`, `Results_Tapes/`, `Travis_County_*`, etc.). Agents MUST NOT
implement new compiled production tools in those Python folders.

Human contributors are encouraged (but not required) to follow the same
conventions when working alongside agents.

---

## Repository Layout

Full detail: [`README.md`](README.md). Agents MUST follow these rules:

### Source and projects

1. Each compiled tool lives in `Applications/<AppName>/` (e.g.
   `ElectionExplorer`).
2. Standard subdirectories: `src/`, `test/`, `docs/`. Create others only when
   needed (`include/`, `res/`, `bench/`).
3. The Visual Studio solution is `Applications/Applications.sln`.
4. Each app’s project is `Applications/<AppName>/<AppName>.vcxproj` (plus
   `.filters` as needed).
5. Shared MSBuild defaults belong in `Applications/Directory.Build.props`
   (or a checked-in `.props` imported by all projects)—not duplicated ad hoc
   per project when avoidable.
6. Agents MUST place new application source under that app’s tree. Agents
   MUST NOT put source, projects, or docs under `Build/`.
7. Shared code used by multiple apps MUST go under a dedicated shared tree
   (e.g. `Applications/common/` or `Applications/libs/`) once a second
   consumer exists—not by copying sources between apps.

### Build output

1. All build artifacts MUST go under `Applications/Build/` (gitignored at
   repo root as `Applications/Build/`).
2. Configuration directories use
   `<os>-<arch>-<config>` in lowercase, for example:
   - `win-x64-release`
   - `win-x64-debug`
   - `win-arm64-release`
   - `win-arm64-debug`
3. Per-app output:
   `Build/<os>-<arch>-<config>/<AppName>/`
   Final binary example:
   `Build/win-x64-release/ElectionExplorer/ElectionExplorer.exe`
4. Intermediate files (`IntDir`) MUST live under that app output tree, e.g.
   `Build/<os>-<arch>-<config>/<AppName>/obj/`.
5. Agents MUST configure new targets so `OutDir` / `IntDir` follow this layout.
   Agents MUST NOT rely on default in-project `x64\Release\` (or similar)
   output folders.
6. Agents MUST NOT commit binaries or other content under `Build/`.

---

## General Agent Behavior

### Principle of Least Surprise
Agents MUST produce changes that are consistent with the existing style,
architecture, and idioms of the surrounding code. When in doubt, imitate the
nearest existing pattern in the file rather than introducing new abstractions.

### Minimal Footprint
Agents MUST limit changes to what is strictly necessary to fulfil the task.
Unsolicited refactoring, renaming, or reformatting of unrelated code is
prohibited.

### Idempotency
Every automated action MUST be safe to re-run. Agents MUST NOT produce
side-effects that differ between the first and subsequent identical invocations
(e.g., appending duplicate entries, creating duplicate files).

### Transparency
Agents MUST clearly document every non-trivial decision in commit messages,
PR descriptions, or inline comments. The rationale for choosing one approach
over another MUST be stated explicitly.

### Reversibility
Agents MUST prefer reversible changes over destructive ones. File deletions,
schema migrations, and binary replacements require explicit human approval
before merging.

---

## Constraints

| Constraint | Rule |
|---|---|
| Repository scope | Agents operate only within this repository. Cross-repository writes require explicit opt-in. |
| Applications tree | Compiled tools, VS solution/projects, and their tests/docs live under `Applications/` only. |
| Secret handling | Agents MUST NOT read, log, print, or transmit secrets, tokens, or credentials found in files. |
| Network access | Agents MUST NOT make outbound network calls during code generation or test execution unless the task explicitly requires it. |
| Dependency pinning | Agents MUST NOT upgrade or add dependencies without human sign-off in the PR description. |
| Binary assets | Agents MUST NOT commit binary blobs (executables, DLLs, images > 100 KB) without prior approval. Includes `Applications/Build/`. |
| Force-push | Agents MUST NOT force-push to any protected branch (`main`, `release/*`). |
| Merge commits | Agents MUST rebase rather than merge when integrating upstream changes into a feature branch. |
| License headers | Agents MUST preserve existing license/copyright headers and add the project-standard header to every new source file. |

---

## Language Policy

### Win32 / Native Windows Code — **C First**

> **Policy: All new Win32 and native Windows system code MUST be written in C
> (ISO C11 or later) unless a specific technical exception is granted.**

#### Rationale

- **ABI stability.** The Win32 API is a C API. Calling it from C requires no
  name-mangling workarounds, COM shims, or `extern "C"` guards.
- **Minimal runtime dependency.** C code links against the CRT cleanly and
  produces smaller, more predictable binaries than C++ with exceptions or STL.
- **Toolchain portability.** MSVC, Clang-cl, and MinGW all compile C11 without
  feature flags; C++ conformance levels vary across versions.
- **Readability for systems contributors.** Win32 reference documentation and
  the majority of Driver Kit samples are written in C. Matching the language
  lowers the barrier for contributors consulting official documentation.
- **Security surface.** C++ features such as implicit constructors, operator
  overloading, and template instantiation can mask resource-management errors.
  Explicit C idioms make ownership and lifetime easier to audit.

#### Mandatory Rules for Win32 / Native Code

1. **Use C11 (`/std:c11` in MSVC, `-std=c11` in Clang/GCC).**  
   Agents MUST set the appropriate compiler flag in every new build target.

2. **C source files** under each app’s `src/` (and under any future
   `Applications/common/`, `Applications/libs/`, or Win32-specific subtrees
   such as `src/win32/`, `src/drivers/`, `src/shell/`) MUST use the `.c`
   extension. Introducing `.cpp` source files into these directories is
   prohibited unless a recorded language exception exists.

   Other file types are unrestricted by this rule, including:
   - C header files (`.h`, `.inl`)
   - Windows resource and manifest files (`.rc`, `.mc`, `.manifest`, `.def`)
   - Assembly source files (`.asm`)
   - Build system files (`Directory.Build.props`, `.props`, `.targets`,
     `CMakeLists.txt`, `.cmake`)
   - Visual Studio project and solution files (`.vcxproj`, `.vcxproj.filters`,
     `.sln`)
   - Debugger visualizer files (`.natvis`)

   Agents MUST NOT introduce `.cpp` files or C++-only constructs (templates,
   references, `new`/`delete`, `class` declarations, `std::` usage, etc.) into
   these directories. `//` single-line comments are permitted, as they are valid
   standard C since C99.

3. **Windows types take precedence over C standard types in Win32 calls.**  
   Use `DWORD`, `HANDLE`, `BOOL`, `LPWSTR`, etc. where the Win32 API expects
   them. Use `uint32_t` / `size_t` for internal logic that does not cross an
   API boundary.

4. **`UNICODE` and `_UNICODE` MUST be defined project-wide.**  
   All string literals passed to Win32 APIs MUST use the `L""` prefix or the
   `TEXT()` macro. Agents MUST NOT introduce bare `char*` paths through Win32
   string APIs.

5. **Error checking is mandatory.**  
   Every Win32 API call that returns `BOOL`, `HANDLE`, or `HRESULT` MUST be
   checked. Use the project-standard `CHECK_WIN32()` / `CHECK_HR()` macros
   defined in `include/check.h`. Agents MUST NOT leave unchecked return values.

6. **Structured resource cleanup with `goto cleanup`.**  
   Follow the `goto cleanup` pattern (documented in `docs/patterns/goto-cleanup.md`)
   for functions that acquire multiple resources. Agents MUST NOT use nested
   `if`-blocks for cleanup logic in Win32 code.

7. **No STL, no RTTI, no exceptions in Win32 modules.**  
   These features are off-limits even if a `.c` file were accidentally compiled
   as C++. Agents MUST verify build flags disable them (`/EHs-c-`, `/GR-`).

8. **COM usage.**  
   COM interfaces MAY be used where the Win32 subsystem requires them, but
   MUST be wrapped in thin C helper functions (e.g., `HrCreateInstance()`).
   Agents MUST NOT use smart COM wrappers (`_com_ptr_t`, ATL, WRL) in Win32
   modules.

#### Exception Process

If C is technically infeasible for a specific Win32 component (e.g., a
component already deeply integrated with a C++ codebase), open an issue with
the label `lang-exception-request` and include:

- The component path.
- The technical reason C cannot be used.
- The subset of C++ features required (MUST be minimal).
- A plan to isolate C++ from the C boundary via `extern "C"` headers.

An exception is only valid after approval from a project maintainer and MUST
be recorded in `docs/lang-exceptions.md`.

---

### Other Language Tiers

These tiers apply outside the Win32 / native layer:

| Tier | Languages | Scope |
|---|---|---|
| **1 — Preferred** | C (C11+) | Win32, drivers, shell extensions, low-level utilities |
| **2 — Permitted** | C++ (C++17, exceptions disabled) | Non-Win32 libraries that require OOP abstractions |
| **3 — Tooling only** | Python 3.11+ | Build scripts, test harnesses, CI automation |
| **4 — Restricted** | Any other language | Requires written approval; agents MUST NOT introduce without human sign-off |

Agents MUST NOT use Tier 3 or Tier 4 languages to implement production runtime
logic.

---

## Code Style & Formatting

- **C:** Conform to the style defined in the `.clang-format` file located in
  the same directory as this `AGENTS.md` file. Agents MUST run `clang-format`
  before committing any `.c` or `.h` file.
- **Line length:** 100 characters maximum.
- **Indentation:** 4 spaces. Tabs are prohibited.
- **Naming:**
  - Functions: `PascalCase` for public API; `snake_case` for internal helpers.
  - Macros: `SCREAMING_SNAKE_CASE`.
  - Types: `PascalCase` with a module prefix (e.g., `WndContext`, `IoPipe`).
  - Constants: `k_PascalCase` or `SCREAMING_SNAKE_CASE` for Win32 compatibility.
- **Header guards:** Use `#pragma once`. Agents MUST NOT use `#ifndef` guards.
- **Comments:** All public API functions MUST have a Doxygen-style block comment
  (`/** ... */`) documenting parameters, return values, and error conditions.

---

## Security Policy

1. **No hardcoded credentials.** Agents MUST reject or redact any secret,
   password, API key, or token discovered in code or configuration, and raise
   a review comment.

2. **Buffer safety.** Agents MUST use safe string functions (`StringCchCopy`,
   `StringCchPrintf`, `memcpy_s`) rather than their unsafe counterparts
   (`strcpy`, `sprintf`, `memcpy` without bounds). Any introduction of an
   unsafe function requires a `// SAFE: <justification>` comment.

3. **Integer overflow.** Agents MUST use `ULongAdd`, `SizeTAdd`, and related
   `intsafe.h` helpers for arithmetic involving sizes or counts passed to Win32.

4. **Privilege.** Code MUST request the minimum required Windows privileges.
   Agents MUST NOT introduce `SE_DEBUG_NAME`, `SeLoadDriverPrivilege`, or
   equivalent privileges without a documented threat model.

5. **Input validation.** All data crossing a trust boundary (user input, IPC,
   network) MUST be validated before use. Agents MUST insert validation at the
   entry point, not deep inside call chains.

---

## Testing Requirements

- Every new public function MUST have at least one corresponding unit test.
- Tests live in each app’s `test/` directory, mirroring the `src/` structure
  where practical (e.g. `ElectionExplorer/test/`).
- Primary build system: Visual Studio / MSBuild via `Applications.sln`.
  Agents MUST build and run the relevant test targets for the changed app
  (for example `msbuild Applications.sln /p:Configuration=Debug /p:Platform=x64`
  or the IDE equivalent) and confirm they pass before marking a PR ready for
  review.
- Performance-sensitive paths MUST include a microbenchmark under the app’s
  `bench/` directory when that directory is introduced.
- Agents MUST NOT disable, skip, or delete existing tests. If a test becomes
  invalid, it MUST be updated, not removed, with a comment explaining the
  change.

---

## Pull-Request & Commit Policy

### Commit Messages

Follow the Conventional Commits specification:


Valid types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`, `ci`,
`chore`.

Agents MUST NOT use vague summaries such as "Update files" or "Fix stuff".

### PR Descriptions

Agent-opened PRs MUST include:

1. **What** — a concise description of the change.
2. **Why** — the motivation or linked issue.
3. **How** — a brief explanation of the approach.
4. **Testing** — which tests were run and their outcomes.
5. **Risks** — any known side effects or areas needing careful review.

### Branch Naming

`agent/<type>/<short-slug>` — e.g., `agent/fix/handle-leak-in-pipe-open`.

---

## Escalation & Human Review

Agents MUST pause and request human review when:

- A change affects more than **10 files** or **500 lines** (net diff).
- A change modifies security-critical code (authentication, privilege
  management, cryptography, IPC dispatch).
- A build or test failure cannot be resolved in **3 automated retry attempts**.
- A dependency version conflict is detected.
- The task description is ambiguous and the agent cannot infer intent with
  high confidence.
- Any condition arises that is not covered by this document.

Escalation is performed by opening a draft PR, labeling it `needs-human-review`,
and posting a comment describing the blocker.

---

## Changelog

| Date | Author | Change |
|---|---|---|
| 2026-08-07 | Initial | Document created. Win32 C-first policy, general agent constraints, and security rules established. |
| 2026-08-08 | Layout | Scoped to `Applications/`; added Repository Layout (VS solution, per-app projects, `Build/<os>-<arch>-<config>/<App>/`); aligned testing with `test/` and MSBuild. |
| 2026-08-08 | Scaffold | ElectionExplorer Win32 GUI (C11, no MFC); static CRT; VS 2026 `v145`; x64/ARM64 Debug+Release via `Applications.sln`. |
