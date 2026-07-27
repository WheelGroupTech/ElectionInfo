# ElectionInfo

This repository contains Python scripts used to analyze election data, including ES&S cast vote records (CVRs), scanner results tapes, Travis County voter registration and roster files, and Travis Central Appraisal District (TCAD) property exports.

Most tools are plain Python scripts run from a terminal. Some folders include their own detailed `README.md` files with setup steps, expected inputs, and sample commands.

## Repository layout

| Directory | Purpose |
|-----------|---------|
| [`ES&S/`](ES&S/) | Process and analyze ES&S CVR, ballot, and audit exports |
| [`Results_Tapes/`](Results_Tapes/) | OCR/process ES&S DS200 results-tape TIFF images and analyze captured totals |
| [`Travis_County_Appraisal_District/`](Travis_County_Appraisal_District/) | Parse TCAD certified appraisal exports into property CSV files |
| [`Travis_County_Elections/`](Travis_County_Elections/) | Election-specific Travis County roster, registration, RID, check-in, and ballot tools |
| [`Travis_County_Voter_Registration/`](Travis_County_Voter_Registration/) | General Travis County voter-registration list analysis and property matching |

---

## `ES&S/`

Scripts for working with **Election Systems & Software (ES&S)** cast vote record and related EMS/export files.

| File | Overview |
|------|----------|
| `process_cvr_files.py` | Processes ES&S cast vote record (CVR) export files |
| `analyze_cvr_data.py` | Analyzes CVR data previously processed into local database/output files |
| `process_ess_ballot_files.py` | Processes ES&S ballot-related export files |
| `process_ess_audit_file.py` | Processes ES&S audit export files |

---

## `Results_Tapes/`

Tools for **DS200 scanner results tapes** captured as TIFF images (for example from Dallas County or similar ES&S tape exports).

| File | Overview |
|------|----------|
| `process_tif_results_tapes.py` | Processes TIFF images of ES&S DS200 results tapes and stores extracted data |
| `analyze_tif_results_tapes_data.py` | Analyzes results-tape data produced by `process_tif_results_tapes.py` (uses the local `results_dbfile.*` shelve files) |
| `analyze_dallas_missing_tapes.py` | Compares expected vs captured Dallas results tapes to help identify missing tapes |

---

## `Travis_County_Appraisal_District/`

Utilities for Travis Central Appraisal District public appraisal exports. Detailed usage is in [`Travis_County_Appraisal_District/README.md`](Travis_County_Appraisal_District/README.md).

| File | Overview |
|------|----------|
| `parse_appraisal_data.py` | Reads TCAD fixed-width export files (`PROP.TXT`, `IMP_INFO.TXT`) and writes property address / improvement-type CSVs for voter-list verification |
| `README.md` | Source-data links, export layout notes, and script usage |

Typical outputs include `travis_properties.csv` and `travis_improv_types.csv`.

---

## `Travis_County_Elections/`

Election-cycle scripts for Travis County public data: Excel voter rosters, registered-voter lists, check-ins, RID lists, voter history, and some ES&S ballot/EMS helpers.

### Shared / older roster tools

| File | Overview |
|------|----------|
| `process_travis_voter_rosters.py` | Generic processor for Travis County Excel voter rosters |
| `process_travis_voter_rosters_2025_11_04.py` | Roster processor variant tied to the 2025-11-04 election data set |

### `2026-03-03_Primary/`

Scripts for the **March 3, 2026 Travis County Primary**. See [`Travis_County_Elections/2026-03-03_Primary/README.md`](Travis_County_Elections/2026-03-03_Primary/README.md) for full workflow details.

| File | Overview |
|------|----------|
| `process_p26_travis_voter_rosters.py` | Builds `VoterRosterDatabase.dat` / `voter_roster.csv` from BBM / EV / ED Excel rosters (usually run first) |
| `process_p26_travis_registered_voters.py` | Compares roster data to registered-voter CSV file(s) |
| `process_p26_travis_checkins.py` | Compares roster data to check-in CSV data |
| `process_p26_voter_history.py` | Merges voting history with 2026 primary participation and writes history output |
| `process_p26_rid_voters.py` | Summarizes Reasonable Impediment Declaration (RID) Excel lists against the roster |
| `generate_p26_conv_lists.py` | Builds Republican and Democratic convention voter lists |
| `generate_p26_rep_precinct_lists.py` | Builds per-precinct Republican primary voter CSV lists |
| `process_p26_sample_bbm_ballots.py` | Splits a sample BBM ballot PDF into individual sample ballot PDFs |
| `process_p26_ess_test_deck.py` | Splits ES&S EMS test-deck PDFs into per-page ballot-style PDFs |
| `process_p26_test_desk_ballots.py` | Reads barcodes from ballot-style PDF images into `ess_p26_ballots.dat` |
| `process_p26_ballot_detail_listing_csv.py` | Cleans ES&S Ballot Detail Listing CSV data and derives oval barcode values |
| `analyze_p26_datafiles.py` | Tallies selections / overvotes / undervotes from contest and ballot-style data |
| `README.md` | Primary-election pipeline documentation |

### `2026-05-02_Local/`

Scripts for the **May 2, 2026 Travis County local election**.

| File | Overview |
|------|----------|
| `process_l26_travis_voter_rosters.py` | Builds the local-election voter roster database from Travis Excel rosters |
| `process_l26_travis_registered_voters.py` | Compares the local-election roster to registered-voter list data |

### `2026-05-26_Primary_Runoff/`

Scripts for the **May 26, 2026 Travis County primary runoff**.

| File | Overview |
|------|----------|
| `process_pr26_travis_voter_rosters.py` | Builds the runoff voter roster database from Travis Excel rosters |
| `process_pr26_travis_registered_voters.py` | Compares the runoff roster to registered-voter list data |
| `merge_sup_into_pr26_travis_registered_voters.py` | Merges a supplemental registered-voter list into the main runoff registered-voter file |
| `process_pr26_rid_voters.py` | Summarizes RID lists against the runoff voter roster |

---

## `Travis_County_Voter_Registration/`

General-purpose tools for Travis County **voter registration CSV** files (not tied to a single election day). Detailed usage is in [`Travis_County_Voter_Registration/README.md`](Travis_County_Voter_Registration/README.md).

| File | Overview |
|------|----------|
| `process_registered_voters.py` | Counts unique voters, finds duplicate VUIDs / possible multi-registrations, and can diff two registration lists |
| `analyze_travis_precinct_changes.py` | Summarizes precincts and State Senate districts; with two files, estimates precinct changes by address |
| `process_registered_voters_with_property_data.py` | Matches voter addresses to TCAD-style property data and flags non-residential or unmatched addresses |
| `README.md` | Beginner and advanced usage, column expectations, and data-source notes |

Public Travis County voter registration files are available from:

https://voter-registration-maps-traviscountytx.hub.arcgis.com/pages/data-files-and-reference

TCAD property extracts used with the property-matching script are described under [`Travis_County_Appraisal_District/`](Travis_County_Appraisal_District/).

---

## License

See [`LICENSE`](LICENSE).
