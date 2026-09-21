# ElectionInfo

Tools and reference data for working with U.S. election data. The centerpiece is
**Election Explorer**, a native Windows desktop application for viewing cast vote
records (CVRs), voter lists, and voter rosters. The repository also includes a
collection of Python analysis scripts and some public reference datasets.

## Start here: Election Explorer

Election Explorer lives under [`Applications/`](Applications/) — a Visual Studio
solution with the desktop app and its build outputs. See
[`Applications/README.md`](Applications/README.md) for what it does, how to build
it, and how to run it.

Sample data the app can open (cast vote records, voter lists, and voter rosters)
is kept locally in `Election_CVRs/`, `Voter_Lists/`, and `Voter_Rosters/`. These
folders are **not** in the repository — they are `.gitignore`d because the files
are too large for GitHub, so they only appear if you add them yourself. None of
the analysis scripts depend on them.

## Repository layout

| Directory | Purpose |
|-----------|---------|
| [`Applications/`](Applications/) | **Election Explorer** desktop app (Visual Studio solution, build outputs under `Build/`) |
| [`Scripts/`](Scripts/) | Python scripts for analyzing ES&S CVRs, results tapes, and Travis County voter/property data |
| [`State_Election_Info/`](State_Election_Info/) | Reference dataset of each state's election code, election-administration agency, and voter portal |

Local-only sample data for Election Explorer (`Election_CVRs/`, `Voter_Lists/`,
`Voter_Rosters/`) is `.gitignore`d and not part of the repository.

## Scripts

The [`Scripts/`](Scripts/) directory holds command-line Python tools for
processing ES&S CVRs and results tapes, and Travis County voter registration,
roster, and property (TCAD) data. Most visitors won't need these.

See [`Scripts/README.md`](Scripts/README.md) for the full list of scripts and
what each one does.

## Reference data

[`State_Election_Info/`](State_Election_Info/) is a reference **dataset** (not
scripts) of official state-government election resources for all 50 states: each
state's statutory election code (citation + source URL), its chief
election-administration agency and website, and its public voter portal. See
[`State_Election_Info/README.md`](State_Election_Info/README.md) for the record
schema and regeneration steps.

## License

See [`LICENSE`](LICENSE).
