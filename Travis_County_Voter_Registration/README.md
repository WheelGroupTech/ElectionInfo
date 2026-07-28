# Travis County Voter Registration Tools

This folder contains three Python scripts that help you work with **Travis County, Texas voter registration lists** (CSV files). You can use them to:

- Count registered voters and look for possible duplicate registrations
- Compare two voter lists from different dates
- See how precincts and addresses line up with Texas State Senate districts
- Track how voters appear to move between precincts over time
- Match voter addresses to county property (appraisal) data and flag non-residential matches

These scripts are designed to run from a command prompt (Terminal on Mac, Command Prompt or PowerShell on Windows). They only need **Python 3** and the Python **standard library** — no extra packages to install.

---

## What’s in this folder

| File | What it does (plain English) |
|------|------------------------------|
| `process_registered_voters.py` | Reads one (or two) voter registration CSV files. Counts unique voters, reports duplicate ID rows, optionally finds people who may be registered more than once (same name + date of birth), and can compare two lists to show who was added or removed. |
| `analyze_travis_precinct_changes.py` | Reads one (or two) voter lists and summarizes precincts, addresses, and State Senate districts. With **two** lists, it estimates how voters moved from old precincts to new ones (by matching addresses) and writes a CSV report. |
| `process_registered_voters_with_property_data.py` | Reads a voter list **and** a property/appraisal CSV. Matches each voter’s home address to property records, then reports residential vs non-residential matches and addresses that could not be matched. |

---

## What you need before you start

1. **Python 3** installed on your computer  
   - Check by opening a terminal and typing:  
	 `python --version`  
	 or, on some systems:  
	 `python3 --version`  
   - You want something like `Python 3.10.x` or newer.
2. Your **voter registration list** as a **CSV** file (comma-separated values), exported from Travis County / election data sources.  
   The Travis County Voter Registration List can be downloaded from:  
   https://voter-registration-maps-traviscountytx.hub.arcgis.com/pages/data-files-and-reference
3. For the property script only: a **property data CSV** with situs (site) address fields and improvement/state code fields.

> **Tip:** CSV files open in Excel, Google Sheets, or any text editor. The scripts expect a header row (column names in the first line).

---

## For experienced Python users

**Runtime:** Python 3.x, stdlib only (`csv`, `sys`, `re`, `os`, `collections`).

**Usage:**

```bash
# Unique VUIDs, optional duplicate-name/DOB groups, optional two-file VUID diff
python process_registered_voters.py <voter_csv> [voter_csv_2]

# Precinct / SD summary; with two files, address-based precinct migration + precinct_changes_report.csv
python analyze_travis_precinct_changes.py <voter_csv> [voter_csv_2]

# Address-normalize voters against TCAD-style property extract; write match side-car CSVs
python process_registered_voters_with_property_data.py <voter_csv> <property_csv>
```

**Notes:**

- All three tolerate mixed encodings (`utf-8`, `cp1252`, `utf-8-sig`, `latin-1`, then utf-8 with replacement).
- Column detection is case-insensitive and supports alternate Travis header names (`VUID` / `VUIDNO`, `DATE_OF_BIRTH` / `DOB`, split street fields, etc.).
- Precinct change analysis normalizes addresses (case/whitespace) and maps old address → new precinct; unmapped addresses are reported as `UNMAPPED` / `None`.
- Property matching builds multi-level indexes and tries, in order:  
  `num+street+unit+zip` → `num+street+zip` → `num+street+unit` → `num+street`.  
  If still unmatched, the same sequence is retried with a leading N/S/E/W stripped from **named** streets only (not numbered streets or highways), so voter `W WELLS BRANCH PKWY` can hit TCAD `WELLS BRANCH PKWY`.  
  Street normalization also aligns common Travis/TCAD spelling differences: interstate (`IH 35` / `INTERSTATE HY 35`), FM/Ranch Road (`FM 620 RD` / `RANCH RD 620`), US/SH highways, ordinal streets (`E 21ST ST` / `E 21 ST`), Mc-names (`MC NEIL` → `MCNEIL`), MLK name variants, and `PLZ`/`PLAZA`.  
  **Confidential voters** (residential address redacted as `***` or similar multi-token `***` groupings) are counted in the summary but excluded from property matching and from the unmatched / non-residential CSVs.  
  Residential = any `imprv_state_cd` token starting with `A`, `B`, `E`, or `O`; also `F1` when `improv_type_desc` contains a residential-use marker (e.g. `SFR COMM`, `DORMITORY`, `ASSISTED LIVING/MEMORY`, `OFF HI-RISE`); also `M1` when `improv_type_desc` contains `MOHO`; blank codes count as non-residential.
- Property script outputs (same directory as the voter file, based on voter basename):  
  `*_unmatched_properties.csv`, `*_nonresidential_matches.csv`, `*_matched_by_improv_type.csv`,
  `*_nonresidential_matches_improv_types.csv`.

---

## For beginners: step-by-step guide

### 1. Open a terminal in this folder

**Windows (PowerShell):**

1. Open File Explorer and go to this folder:  
   `Travis_County_Voter_Registration`
2. Click the address bar, type `powershell`, and press Enter  
   **or** right-click in the folder → “Open in Terminal” / “Open PowerShell window here”.

**Mac / Linux:**

1. Open Terminal.
2. Change into this folder, for example:

```bash
cd /path/to/Travis_County_Voter_Registration
```

### 2. Put your data files somewhere easy to find

Examples:

- Same folder as the scripts, or  
- Another folder, as long as you know the full path.

When you run a script, you will type the **path to your CSV file(s)** after the script name.

**Paths with spaces:** put the path in quotes:

```bash
python process_registered_voters.py "C:\Users\You\Documents\My Voter File.csv"
```

---

## Script 1 — `process_registered_voters.py`

### Purpose

This is the best starting point. It answers questions like:

- How many **unique** registered voters are in this file?
- Are there **duplicate rows** with the same voter ID (VUID)?
- Are there people who look registered more than once (same last/first/middle name and date of birth, but different VUIDs)?
- What changed between **two** voter lists (who is only in list 1, only in list 2, or in both)?

### How to run it

**One file:**

```bash
python process_registered_voters.py path\to\your_voter_list.csv
```

**Two files (compare them):**

```bash
python process_registered_voters.py path\to\older_list.csv path\to\newer_list.csv
```

If `python` is not found, try `python3` instead.

### What it does behind the scenes

1. Opens the CSV and tries several text encodings so Windows and Mac exports both work.
2. Finds the voter ID column (`VUID` or `VUIDNO`).
3. Builds a dictionary of unique voters keyed by VUID.
4. Counts rows that reuse the same VUID (duplicate entries in the file).
5. If a date-of-birth column exists, groups records by name + DOB and prints groups that have more than one VUID (possible multiple registrations).
6. If you pass a second file, it compares the two sets of VUIDs and prints who is unique to each file.

### What you will see

Something like:

- How many rows were read
- How many duplicate VUID rows
- How many unique voters
- Groups of possible multiple registrations (if DOB data is present)
- If two files were given: counts of shared vs unique VUIDs, and listings of differences

### Common mistakes

| Problem | What to try |
|---------|-------------|
| `Usage: python process_registered_voters.py ...` | You forgot to type the CSV path after the script name. |
| `File not found` | Check the path and spelling. Use quotes if the path has spaces. |
| No multiple-registration section | The file may not include a date-of-birth column the script recognizes. |
| Garbled names | Rare; the script already tries multiple encodings. Re-export the CSV as UTF-8 if needed. |

---

## Script 2 — `analyze_travis_precinct_changes.py`

### Purpose

Use this when you care about **where** voters live in the election map:

- Which **precincts** appear in the file?
- How many **unique addresses** and **registered voters** are in each precinct?
- Which **Texas State Senate district** each precinct is associated with?
- Between two snapshots of the roll: did addresses stay in the same precinct, or move to another?

With two input files, it also writes:

**`precinct_changes_report.csv`**

in the folder where you ran the command. That spreadsheet is a matrix of “old precinct → new precinct” percentages.

### How to run it

**One file (summary only):**

```bash
python analyze_travis_precinct_changes.py path\to\your_voter_list.csv
```

**Two files (compare precincts over time):**

```bash
python analyze_travis_precinct_changes.py path\to\older_list.csv path\to\newer_list.csv
```

Convention: treat the **first** file as the “old” list and the **second** as the “new” list.

### What it does behind the scenes

1. Reads the voter CSV (same encoding strategy as the other scripts).
2. Locates columns for VUID, residential address, precinct, and state senate district (flexible header names).
3. Cleans precinct labels that start with things like `P ` or `P Z`.
4. Builds maps:
   - State Senate district → set of precincts  
   - Precinct → addresses and voter counts at each address  
5. Prints per-precinct stats.
6. If a second file is provided:
   - Matches addresses from the old file to precincts in the new file (after simple address normalization)
   - Prints how many voters from each old precinct went to each new precinct
   - Labels addresses that could not be found in the new file as **UNMAPPED**
   - Writes `precinct_changes_report.csv`

### How to read the results

On screen you might see lines like:

```text
Old precinct '314': 1200 registered voters
  -> 314: 900 voters (75.0%)
  -> 315: 250 voters (20.8%)
  -> UNMAPPED: 50 voters (4.2%)
```

That means: of the voters who lived in precinct 314 in the old file, most still map to 314, some addresses now sit in 315, and some addresses from the old file were not found in the new file.

The CSV report is useful in Excel: rows are old precincts, columns are new precincts, and cells are percentages of the old precinct’s voters.

### Important limitation (please read)

This comparison is based on **addresses**, not individual people moving house. If someone moves to a new address, or an address is written differently in the two files, they may show up as **UNMAPPED** or appear to “change precinct” even when redistricting is not the cause. Use the report as a strong clue, then verify important cases by hand.

---

## Script 3 — `process_registered_voters_with_property_data.py`

### Purpose

This script asks: **Does each registered voter’s address look like a normal residential property in the county property data?**

That can help spot registrations at commercial sites, vacant land, or other non-residential improvement types (or addresses that simply do not match the property file).

### How to run it

You **must** provide two files:

```bash
python process_registered_voters_with_property_data.py path\to\voter_list.csv path\to\property_data.csv
```

### What property file columns are expected

The script looks for flexible header names. At minimum it needs property id and situs street number/name. It also uses (when present):

- Prefix / suffix, unit, city, ZIP  
- `IMPROV_TYPE_CD` / `IMPROV_TYPE_DESC`  
- `IMPRV_STATE_CD` (or similar)

Voter side: it accepts either a single residential address field or split fields (street number, name, type, unit, city, ZIP, etc.).

> **Note on `City` vs `CITY`:** Some Travis voter extracts include a residential **`City`** column with the address fields and a later **`CITY`** column (after district fields such as `US CONGRESS`) that is a jurisdiction **code**, not a city name. The script uses the earlier residential city field and ignores the later code column when both are present.

### What it does behind the scenes

1. Loads property rows and builds several **address indexes** with normalized text (uppercase; street types like ST/STREET; directions like N/NORTH; units like APT 2 → 2; plus highway/ordinal/Mc-name aliases described above).
2. Loads unique voters by VUID.
3. Skips **confidential** voters whose residential address is redacted with one or more `***` groupings (they are counted in the summary but not written to the unmatched or non-residential CSVs, and no property match is attempted).
4. For each remaining voter, tries to match an address from most specific to least specific:
   1. Number + street + unit + ZIP  
   2. Number + street + ZIP  
   3. Number + street + unit  
   4. Number + street only  
   Then, if needed, repeats that sequence after stripping a leading direction from named streets (see experienced-user notes).
5. Classifies matches using property **state codes** and improvement descriptions:
   - Codes starting with **A**, **B**, **E**, or **O** → treated as **residential** (excluded from the non-residential report)
   - Code **F1** whose `improv_type_desc` contains any of these markers (prefix/extra `;`-separated text allowed) → also **residential**:
     `TREATMENT/REHAB`, `SFR COMM`, `DUPLEX COMM`, `GARAGE APT COMM`, `DORMITORY`, `FRAT/SORORITY`, `INDEPENDENT LIVING`, `ASSISTED LIVING/MEMORY`, `SKILLED NURSING`, `ALT LIVING CTR`, `MOHO`, `CONTINUING CARE`, `OFF HI-RISE`, `DWELLING`
   - Code **M1** whose `improv_type_desc` contains `MOHO` (prefix/extra `;`-separated text allowed) → also **residential**
   - Other codes or blank → **non-residential** (for this script’s purposes)
6. Writes four CSV files next to your voter file (names based on the voter file name).

### Output files

If your voter file is named `travis_voters_2026.csv`, you will get something like:

| Output file | Contents |
|-------------|----------|
| `travis_voters_2026_unmatched_properties.csv` | Voters whose addresses could not be matched to any property row (**excludes** confidential `***` addresses) |
| `travis_voters_2026_nonresidential_matches.csv` | Voters matched to a property that does not look residential (**excludes** confidential `***` addresses) |
| `travis_voters_2026_matched_by_improv_type.csv` | Counts of **all** matched properties grouped by improvement type description and state code |
| `travis_voters_2026_nonresidential_matches_improv_types.csv` | Same grouping as above, but only for **non-residential** matches |

The script also prints a short summary in the terminal (confidential excluded / matched / residential / non-residential / unmatched, and match-level counts).

### Common mistakes

| Problem | What to try |
|---------|-------------|
| Usage message asking for two paths | Provide **both** the voter CSV and the property CSV. |
| `Property file missing required columns` | Confirm the property extract includes situs number/street (and ideally ZIP/unit). |
| Very high unmatched count | Address formats may still differ (missing house numbers in the property extract, apartment complexes under a different situs number, vacant / non-situs rows). Check a few unmatched rows by hand against the property file. Normalization covers many highway, ordinal, and Mc-name differences, but not every edge case. |
| Slow run on large files | Normal for big county extracts; let it finish. |
| Example: `1109 N IH 35` unmatched before fix | Voter rolls often say `IH 35` while TCAD situs uses `INTERSTATE HY 35`. Current normalization maps both to `IH 35` so they match. |

---

## Choosing which script to run

```text
Do you want to count voters, find duplicate IDs, or compare two rolls?
  → process_registered_voters.py

Do you want precinct / senate-district totals, or precinct change over time?
  → analyze_travis_precinct_changes.py

Do you want to check voter addresses against property / appraisal data?
  → process_registered_voters_with_property_data.py
```

You can run more than one script on the same voter file; they do not depend on each other.

---

## Beginner FAQ

### What is a VUID?

**VUID** (Voter Unique ID) is the voter’s unique identifier in the registration data. These scripts use it as the main key for “one person / one registration record.”

### What is a precinct?

A **precinct** is a small geographic area used to assign voters to polling places and report results. Precinct lines can change over time; that is one reason the precinct-change script exists.

### What is a CSV file?

**CSV** means “Comma-Separated Values.” It is a plain-text table: the first row is usually column names, and each following row is one record. Excel can “Save As” CSV.

### Do I need to know how to program?

No. You only need to:

1. Install Python 3  
2. Open a terminal in this folder  
3. Type a `python ...` command with your file path(s)

You do **not** need to edit the `.py` files for normal use.

### Will these scripts change my original voter file?

No. They **read** your inputs and print results. The precinct script may create `precinct_changes_report.csv`, and the property script creates additional `*_unmatched_*.csv` / related files. Your original lists are not modified.

### What if I double-click a `.py` file?

On many computers nothing useful happens, or a window flashes and closes. Always run these from a terminal so you can see the messages and pass the file paths.

### Privacy reminder

Voter registration files can contain sensitive personal information. Store them securely, avoid emailing them casually, and follow your organization’s rules for election data.

---

## Troubleshooting quick reference

1. **`python` not recognized** — Install Python from [python.org](https://www.python.org/downloads/) and ensure “Add Python to PATH” is checked on Windows. Then close and reopen the terminal. Try `py` or `python3` if needed.
2. **Wrong folder** — The command should be run from this directory, **or** you must type the full path to the `.py` script.
3. **File not found** — Drag-and-drop the CSV onto the terminal window (on many systems) to paste its full path.
4. **Permission / file in use** — Close the CSV in Excel before running; Excel sometimes locks the file.
5. **Empty or odd results** — Open the CSV and confirm the first row has headers and that voter ID / address / precinct columns are present.

---

## Example session (Windows)

```powershell
cd H:\Dev\github\WheelGroupTech\ElectionInfo\Travis_County_Voter_Registration

python process_registered_voters.py .\data\voters_jan.csv

python process_registered_voters.py .\data\voters_jan.csv .\data\voters_july.csv

python analyze_travis_precinct_changes.py .\data\voters_jan.csv .\data\voters_july.csv

python process_registered_voters_with_property_data.py .\data\voters_july.csv .\data\property_extract.csv
```

Replace the `.\data\...` paths with the real locations of your files.

---

## Summary

| Script | Inputs | Main outputs |
|--------|--------|--------------|
| `process_registered_voters.py` | 1–2 voter CSVs | Terminal report: unique voters, duplicate VUIDs, possible multi-registrations, optional VUID diff |
| `analyze_travis_precinct_changes.py` | 1–2 voter CSVs | Terminal precinct/SD summary; with 2 files also `precinct_changes_report.csv` |
| `process_registered_voters_with_property_data.py` | 1 voter CSV + 1 property CSV | Terminal match summary + unmatched / non-residential / all-match and non-residential improvement-type CSV files |

If you are new to Python, start with **`process_registered_voters.py`** and a single CSV. Once that works, try a two-file comparison, then the precinct and property tools as needed.
