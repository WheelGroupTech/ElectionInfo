# Travis County 2026 Primary Data Analysis Scripts

This folder contains Python scripts used to analyze public election data for the **Travis County, Texas Primary Election on March 3, 2026**.

The scripts work with published voter registration lists, voting rosters, check-in data, Reasonable Impediment Declaration (RID) lists, and ballot/EMS (Election Management System) files from ES&S equipment.

Most of the voter-analysis scripts share a common workflow:

1. Build a local voter roster database from Travis County Excel rosters.
2. Compare that roster to other published lists (registered voters, check-ins, history, RID, etc.).
3. Print summaries and/or write CSV output files.

Developed and run with **Python 3.14.2**.

---

## File Overview

### Voter data scripts

| Script | What it does |
|--------|----------------|
| `process_p26_travis_voter_rosters.py` | Reads Travis County Excel voter rosters (BBM / EV / ED) from the current folder and builds `VoterRosterDatabase.dat` plus `voter_roster.csv`. **Run this first** before most other voter scripts. |
| `process_p26_travis_registered_voters.py` | Compares the voter roster to one or two registered-voter CSV files. Checks name matches, unknown VUIDs, and possible multiple registrations (same name + DOB, different VUID). |
| `process_p26_travis_checkins.py` | Compares the voter roster to a check-in CSV. Summarizes check-ins and looks for non-secret ballots (unique location + party + precinct combinations). |
| `process_p26_voter_history.py` | Loads a registered-voter file that includes voting history, merges 2026 primary participation, scores Republican primary history, and writes `voter_history_records.csv`. |
| `process_p26_rid_voters.py` | Reads RID (Reasonable Impediment Declaration) Excel files from the current folder and summarizes RID usage by reason and party against the voter roster. |
| `generate_p26_conv_lists.py` | Builds Republican and Democratic convention voter lists from the roster + registered voter file(s). |
| `generate_p26_rep_precinct_lists.py` | Builds per-precinct CSV lists of Republican primary voters. |

### Ballot / ES&S EMS scripts

| Script | What it does |
|--------|----------------|
| `process_p26_sample_bbm_ballots.py` | Splits a single ES&S sample BBM ballot PDF into individual sample ballot PDFs. |
| `process_p26_ess_test_deck.py` | Splits ES&S EMS test-deck PDFs into individual page PDFs named by ballot style. |
| `process_p26_test_desk_ballots.py` | Reads barcodes from ballot-style PDF images and saves them to `ess_p26_ballots.dat`. |
| `process_p26_ballot_detail_listing_csv.py` | Cleans an ES&S Ballot Detail Listing CSV and calculates six-digit oval barcode values for each selection. |
| `analyze_p26_datafiles.py` | Uses contest/ballot-style CSVs plus `ess_p26_ballots.dat` to tally selections, overvotes, and undervotes. |

### Documentation

| File | What it is |
|------|------------|
| `README.md` | This file. |

### Important intermediate files created by the scripts

| File | Created by | Used by |
|------|------------|---------|
| `VoterRosterDatabase.dat` | `process_p26_travis_voter_rosters.py` | Most voter-analysis and list-generation scripts |
| `voter_roster.csv` | `process_p26_travis_voter_rosters.py` | Human-readable roster export |
| `ess_p26_ballots.dat` | `process_p26_test_desk_ballots.py` | `analyze_p26_datafiles.py` |
| `voter_history_records.csv` | `process_p26_voter_history.py` | Output for further review |
| Per-precinct / convention CSVs | `generate_p26_rep_precinct_lists.py`, `generate_p26_conv_lists.py` | Output for conventions and precinct work |

**Note:** `shelve` database files may also create companion files such as `.dat.bak`, `.dir`, or `.db` depending on your operating system. Keep those together with the main `.dat` file.

---

## For Experienced Python Users

**Dependencies**

```bash
pip install --upgrade PyMuPDF Pillow pyzbar pandas pypdf openpyxl
```

(`PyMuPDF` provides the `fitz` module used by the barcode script.)

**Typical voter pipeline**

```bash
# 1) Put Travis County roster .xlsx files in the current directory, then:
python process_p26_travis_voter_rosters.py

# 2) Downstream analyses (require VoterRosterDatabase.dat in CWD)
python process_p26_travis_registered_voters.py registered_voters.csv [registered_voters_2.csv]
python process_p26_travis_checkins.py checkins.csv
python process_p26_voter_history.py registered_voters_with_history.csv
python process_p26_rid_voters.py
python generate_p26_conv_lists.py registered_voters.csv [registered_voters_2.csv]
python generate_p26_rep_precinct_lists.py ./precinct_out registered_voters.csv [registered_voters_2.csv]
```

**Typical ballot / EMS pipeline**

```bash
python process_p26_sample_bbm_ballots.py sample_bbm.pdf
python process_p26_ess_test_deck.py ./test_deck_source ./test_deck_pages
python process_p26_test_desk_ballots.py ./ballot_pdf_dir
python process_p26_ballot_detail_listing_csv.py ballot_detail.csv [extracted_ballot_data.csv]
python analyze_p26_datafiles.py contests.csv ballot_details.csv
```

**Notes**

- Many scripts expect `VoterRosterDatabase.dat` or `ess_p26_ballots.dat` in the **current working directory**.
- CSV readers try multiple encodings (`utf-8`, `cp1252`, `utf-8-sig`, `latin-1`).
- Roster Excel filenames are expected to encode date and ballot type (BBM / EV / ED).
- Ballot barcode layout is CCRRSP (column, row, side, page).
- No packaging/CLI framework: arguments are plain `sys.argv`.

---

## For Beginners (Step-by-Step)

You do **not** need to be a programmer to run these scripts, but you do need Python installed and a willingness to use a simple command window.

### 1. What you need installed

1. **Python 3** (this project was developed with Python 3.14.2).
   - During install on Windows, check the box **"Add python.exe to PATH"** if you see it.
2. A way to open a terminal:
   - **Windows:** Command Prompt or PowerShell
   - **macOS/Linux:** Terminal
3. The data files from Travis County / ES&S that the script you want to run expects (Excel, CSV, or PDF).

### 2. Install required Python packages

Open a terminal and run these commands one at a time:

```bash
pip install --upgrade PyMuPDF
pip install --upgrade Pillow
pip install --upgrade pyzbar
pip install --upgrade pandas
pip install --upgrade pypdf
pip install --upgrade openpyxl
```

If `pip` is not recognized, try:

```bash
python -m pip install --upgrade PyMuPDF Pillow pyzbar pandas pypdf openpyxl
```

**Barcode reading note:** `process_p26_test_desk_ballots.py` also needs the **zbar** system library used by `pyzbar`. If barcode reading fails after installing the Python packages, you may need to install zbar separately for your operating system.

### 3. Open a terminal in this folder

You should be "inside" the folder that contains these scripts before you run them.

**Windows PowerShell example:**

```powershell
cd "H:\Dev\github\WheelGroupTech\ElectionInfo\Travis_County_Elections\2026-03-03_Primary"
```

To confirm you are in the right place:

```powershell
dir
```

You should see the `.py` files listed in this README.

### 4. Understand the two main workflows

There are two separate families of scripts:

#### A) Voter list analysis (most common)

These scripts answer questions like:

- Who is on the published voter rosters?
- Do roster names match the registered voter file?
- Who checked in?
- Who used a Reasonable Impediment Declaration?
- Can we build convention or precinct contact lists?

**Almost all of these require one first step:**

```bash
python process_p26_travis_voter_rosters.py
```

Before running that:

1. Copy the Travis County roster Excel (`.xlsx`) files into this same folder.
2. Run the command above.
3. When it finishes successfully, you should see files such as:
   - `VoterRosterDatabase.dat`
   - `voter_roster.csv`

Then you can run the other voter scripts.

#### B) Ballot / EMS file analysis

These scripts work with sample ballots, test decks, barcode images, and ballot detail listings from ES&S systems. They are mostly independent from the voter-roster database (except conceptually).

### 5. Beginner-friendly command guide

In the examples below, replace names like `registered_voters.csv` with your actual file names. If a file is not in the current folder, use the full path in quotes.

#### Build the voter roster database (do this first for voter work)

```bash
python process_p26_travis_voter_rosters.py
```

- **Input:** `.xlsx` roster files in the current folder
- **Output:** `VoterRosterDatabase.dat`, `voter_roster.csv`
- **Also prints:** counts by ballot type (BBM / EV / ED) and party, plus any duplicate VUIDs

#### Compare roster to registered voters

```bash
python process_p26_travis_registered_voters.py registered_voters.csv
```

Optional second registered-voter file (useful if some voters are missing from the first file):

```bash
python process_p26_travis_registered_voters.py registered_voters.csv registered_voters_2.csv
```

#### Compare roster to check-ins

```bash
python process_p26_travis_checkins.py checkins.csv
```

#### Process voter history and export a CSV

```bash
python process_p26_voter_history.py registered_voters_with_history.csv
```

- **Output:** `voter_history_records.csv` in the current folder

#### Analyze RID voters

```bash
python process_p26_rid_voters.py
```

- **Input:** RID Excel files in the current folder, plus existing `VoterRosterDatabase.dat`
- **Output:** printed totals by RID reason and party

#### Create convention lists

```bash
python generate_p26_conv_lists.py registered_voters.csv
```

#### Create Republican per-precinct lists

First create an output folder, then run:

```bash
mkdir precinct_out
python generate_p26_rep_precinct_lists.py precinct_out registered_voters.csv
```

#### Split sample BBM ballots from one PDF

```bash
python process_p26_sample_bbm_ballots.py sample_bbm.pdf
```

- **Output:** files like `Sample-100AD.pdf` in the current folder

#### Split an ES&S test deck into single pages

```bash
python process_p26_ess_test_deck.py source_folder destination_folder
```

#### Read barcodes from ballot PDFs

```bash
python process_p26_test_desk_ballots.py folder_with_ballot_pdfs
```

- **Output:** `ess_p26_ballots.dat`

#### Clean a Ballot Detail Listing CSV and compute oval barcodes

```bash
python process_p26_ballot_detail_listing_csv.py ballot_detail.csv extracted_ballot_data.csv
```

If you omit the second file name, it defaults to `extracted_ballot_data.csv`.

#### Analyze ballot selections / undervotes

```bash
python analyze_p26_datafiles.py contests.csv ballot_details.csv
```

This expects `ess_p26_ballots.dat` to already exist in the current folder.

### 6. How to read common messages

| Message / situation | What it usually means |
|---------------------|------------------------|
| `Usage: python ...` | You left out a required file name or folder name. Re-run with the arguments shown. |
| `Cannot open database file 'VoterRosterDatabase...'` | You have not successfully run `process_p26_travis_voter_rosters.py` yet, or you are not in the folder that contains the database. |
| `File not found: ...` | The path you typed is wrong, or the file is not in the current folder. |
| `Successfully processed N Excel workbooks` | Roster import worked. |
| Lots of printed counts and summaries | Normal. Many scripts report results in the terminal instead of creating a spreadsheet. |

### 7. Suggested order for a first successful run

If your goal is basic voter-roster analysis:

1. Install Python and the packages listed above.
2. Copy roster `.xlsx` files into this folder.
3. Run `process_p26_travis_voter_rosters.py`.
4. Open `voter_roster.csv` in Excel to sanity-check the extract.
5. Run `process_p26_travis_registered_voters.py` with your registered voter CSV.
6. Optionally run check-ins, history, RID, convention, or precinct scripts.

If your goal is ballot/EMS work:

1. Start with the PDF splitters (`process_p26_sample_bbm_ballots.py` or `process_p26_ess_test_deck.py`).
2. Use `process_p26_test_desk_ballots.py` to build `ess_p26_ballots.dat`.
3. Use `process_p26_ballot_detail_listing_csv.py` to prepare detail data.
4. Run `analyze_p26_datafiles.py` last.

### 8. Plain-language glossary

| Term | Meaning |
|------|---------|
| **VUID** | Texas Voter Unique Identifier (voter ID number). |
| **BBM** | Ballot by mail. |
| **EV** | Early voting in person. |
| **ED** | Election Day in-person voting. |
| **RID** | Reasonable Impediment Declaration (used when a voter has an impediment to presenting standard photo ID). |
| **Roster** | Published list of people who voted / are recorded as voting in the election materials being analyzed. |
| **Shelve database** | A simple on-disk Python data file (here, `VoterRosterDatabase.dat` or `ess_p26_ballots.dat`) used so later scripts do not have to re-read every Excel file. |
| **ES&S EMS** | Election Systems & Software Election Management System exports/files. |
| **Ballot style** | The specific ballot version for a party/precinct/combination of contests. |

### 9. Tips that prevent most beginner problems

- Run commands from **this folder**, not from your home directory.
- Keep `VoterRosterDatabase.dat` in the same folder when running later voter scripts.
- Use quotes around paths that contain spaces.
- Start with one script and one small success before chaining many steps.
- If a script prints an error, read the last 10-20 lines of output first; the useful message is usually near the end.

---

## License

Copyright (c) 2024-2026 Daniel M. Teal  
License: MIT License
