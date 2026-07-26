#-----------------------------------------------------------------------------
# process_travis_voter_rosters.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to process Excel voter rosters from Travis County Elections.
#-----------------------------------------------------------------------------
"""process_travis_voter_rosters.py""" # for pylint
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=too-many-statements

import os
import re
import shelve

import pandas as pd


# Published roster of voters for the election
VOTER_ROSTER = []

# Specify the database version for the voter roster
VOTER_ROSTER_VERSION = 1

# Dictionary of voter VUIDs
ROSTER_VUIDS = {}


#-----------------------------------------------------------------------------
# analyze_roster_vuid_numbers()
#-----------------------------------------------------------------------------
def analyze_roster_vuid_numbers():
    """Analyze all of the VUID numbers from the voter roster list"""

    num_duplicates = 0
    num_bbm = 0
    num_ev = 0
    num_ed = 0

    for voter in VOTER_ROSTER:

        # Get the voter information for the current voter roster record
        vuid_number = str(voter['VUID'])
        precinct = voter['Precinct']
        first_name = voter['FirstName']
        last_name = voter['LastName']
        ballot_type = voter['BallotType']
        vote_date = voter['VoteDate']
        notes = voter['Notes']

        # Aggregate the ballot types
        if ballot_type == 'BBM':
            num_bbm += 1
        elif ballot_type == 'EV':
            num_ev += 1
        elif ballot_type == 'ED':
            num_ed += 1

        try:
            # Find the vuid number in the list of all VUIDs
            vuid_record = ROSTER_VUIDS[vuid_number]

            # We found it
            print(f"Found duplicate VUID in the list {vuid_number}")
            print(f"Original:  {vuid_record['VoterRecord']}")
            print(f"Duplicate: {voter}")
            num_duplicates = num_duplicates + 1

        except KeyError:

            # The vuid number was not found in the list (as expected), so we add it
            voter_record = {'VoterRecord':voter}
            ROSTER_VUIDS[vuid_number] = voter_record

    print(f"Found {num_duplicates} duplicate entries in the voter roster lists")

    print(f"BBM Roster Voters: {num_bbm}")
    print(f"ED  Roster Voters: {num_ed}")
    print(f"EV  Roster Voters: {num_ev}")


#-----------------------------------------------------------------------------
# parse_info_from_workbook_filename()
#
# This function parses the date and voter type from the provided workbook
# pathname.
#-----------------------------------------------------------------------------
def parse_info_from_workbook_filename(filename):
    """Parses ballot type and voter date from the workbook pathname"""

    ret_val = True

    # Determine if the workbook filename is for:
    #  BBM - Ballot by mail
    #  EV  - In-person early vote
    #  ED  - In-person election day vote
    words = filename.split(r' ')
    if re.search(r'Mail', filename, flags=re.IGNORECASE):
        ballot_type = 'BBM'
    elif re.search(r'Early', filename, flags=re.IGNORECASE):
        ballot_type = 'EV'
    else:
        ballot_type = 'ED'

    # Convert the date from the filename
    workbook_date = words[0]
    words = workbook_date.split(r'.')
    if len(words) != 3:
        print(f"File name '{filename}' is not of the format MM.DD.YYYY.Type!")
        ret_val = False
        return ret_val, "", ""
    vote_date = f"{words[0]}/{words[1]}/{words[2]}"

    return ret_val, ballot_type, vote_date


#-----------------------------------------------------------------------------
# process_excel_workbook()
#
# This function prcesses the specified Excel workbook.  It will identify
# the worksheet containing voter information and process the contents.
#
# Pseudocode / Plan:
#
# 1. Read workbook with pandas and ensure there is only one sheet.
# 2. Load the sheet into a DataFrame and determine row/column counts.
# 3. Attempt to auto-detect the header row by scanning the first N rows
#    for expected header keywords: 'vuid', 'pct'/'precinct', and either
#    'first' and 'last' or a combined 'name' (e.g. "Last, First").
# 4. Build a mapping from role -> column index:
#      - col_vuid: header contains 'vuid'
#      - col_precinct: header contains 'pct' or 'precinct'
#      - col_first: header contains 'first'
#      - col_last: header contains 'last'
#      - col_name: header contains 'name' or header looks like combined "last, first"
#      - col_notes: header contains 'note' (optional)
# 5. If detection fails, fall back to previous heuristics (existing logic) but attempt
#    to be tolerant (case-insensitive, substring matches).
# 6. Validate that we found the required columns (vuid and precinct and either first+last or name).
#    If not, print an error and return False.
# 7. Iterate rows after the detected header row:
#      - Extract vuid, precinct, names and notes using the mapped columns.
#      - For combined name column, split on comma into last, first (strip).
#      - Build voter dict and append to VOTER_ROSTER.
# 8. Preserve existing behaviors such as rstrip on names and printing progress.
#-----------------------------------------------------------------------------
def process_excel_workbook(pathname, ballot_type, vote_date):
    """Process the specified Excel workbook"""

    ret_val = True

    # Use pandas to read in the Excel workbook
    xlsx = pd.ExcelFile(pathname)

    # Get the number of sheets in the workbook
    num_sheets = len(xlsx.sheet_names)
    if num_sheets > 1:
        print(f"There are {num_sheets} in {pathname} - expecting only a single sheet!")
        ret_val = False
        return ret_val

    # Read the Excel worksheet into a Pandas data frame
    sheet = xlsx.sheet_names[0]
    excel_data = pd.read_excel(xlsx, sheet)

    # Get the number of rows and columns from the Excel data
    num_rows, num_columns = excel_data.shape

    # Attempt to find the header row by searching the first few rows for expected headers
    header_row = None
    max_header_scan = min(10, num_rows)
    for r in range(0, max_header_scan):
        try:
            row_vals = [str(excel_data.iat[r, c]).strip().lower() for c in range(num_columns)]
        except Exception:
            # If iat fails for this row, skip it
            continue

        has_vuid = any('vuid' in v for v in row_vals)
        has_pct = any(('pct' in v) or ('precinct' in v) for v in row_vals)
        has_first = any('first' in v for v in row_vals)
        has_last = any('last' in v for v in row_vals)
        has_name = any('name' in v for v in row_vals) or any(',' in v for v in row_vals)

        # If we see vuid and precinct and either first+last or a name column, choose this row
        if has_vuid and has_pct and ( (has_first and has_last) or has_name ):
            header_row = r
            break

    # Fallback: if no header was found, use row index 2 (legacy behavior)
    if header_row is None:
        if num_rows > 2:
            header_row = 2
        else:
            print(f"Unable to determine header row for '{pathname}'")
            return False

    # Map column indices to roles
    col_vuid = None
    col_precinct = None
    col_first = None
    col_last = None
    col_name = None
    col_notes = None

    for c in range(num_columns):
        try:
            header = str(excel_data.iat[header_row, c]).strip().lower()
        except Exception:
            header = ''
        if 'vuid' in header:
            col_vuid = c
        elif 'pct' in header or 'precinct' in header:
            col_precinct = c
        elif 'first' in header:
            col_first = c
        elif 'last' in header:
            col_last = c
        elif 'name' in header:
            # could be "Name" which might be "Last, First"
            col_name = c
        elif 'note' in header:
            col_notes = c
        else:
            # If header contains a comma and looks like "Last, First" treat as name
            if ',' in header and col_name is None:
                col_name = c

    # Some sheets may have no explicit 'notes' header but have a 5th column; detect by position
    if col_notes is None and num_columns >= 5:
        # Prefer the last column if it doesn't look like a known field
        potential = num_columns - 1
        if potential not in (col_vuid, col_precinct, col_first, col_last, col_name):
            col_notes = potential

    # Validate required columns: VUID and Precinct and name components
    has_name_components = (col_name is not None) or (col_first is not None and col_last is not None)
    if col_vuid is None or col_precinct is None or not has_name_components:
        print(f"Failed to detect required columns in '{pathname}' header row {header_row}")
        print(f"Detected columns: vuid={col_vuid}, precinct={col_precinct}, first={col_first}, last={col_last}, name={col_name}, notes={col_notes}")
        ret_val = False
        return ret_val

    # Iterate through all of the data rows starting after the header
    cur_row = header_row + 1
    while cur_row < num_rows:
        try:
            # Read values using detected column mapping
            vuid = excel_data.iat[cur_row, col_vuid]
            precinct = excel_data.iat[cur_row, col_precinct]
            notes = excel_data.iat[cur_row, col_notes] if col_notes is not None else ""
            if col_name is not None:
                name = excel_data.iat[cur_row, col_name]
                # Split "Last, First" into components
                if isinstance(name, str) and ',' in name:
                    parts = [p.strip() for p in name.split(',', 1)]
                    if len(parts) == 2:
                        last_name = parts[0]
                        first_name = parts[1]
                    else:
                        # fall back to raw values
                        first_name = str(name).strip()
                        last_name = ""
                else:
                    # If no comma, try to treat as single name in first position
                    first_name = str(name).strip()
                    last_name = ""
            else:
                first_name = excel_data.iat[cur_row, col_first]
                last_name = excel_data.iat[cur_row, col_last]

        except Exception:
            # If row access fails (e.g., blank trailing rows), break loop
            break

        # If this is the header validation row (legacy check), compare values
        if cur_row == header_row + 0:
            # For compatibility with original code, check that detected header values look correct
            hdr_vuid = str(vuid)
            hdr_first = str(first_name)
            hdr_last = str(last_name)
            if 'vuid' not in hdr_vuid.lower() and 'vuid' not in str(excel_data.iat[header_row, col_vuid]).lower():
                print(f"Row {header_row} header mismatch: {hdr_vuid} {hdr_first} {hdr_last}")
                ret_val = False
                break
        else:
            # Append the voter to the voter roster
            voter = {'VUID': vuid}
            voter['Precinct'] = str(precinct)
            voter['FirstName'] = str(first_name).rstrip()
            voter['LastName'] = str(last_name).rstrip()
            voter['BallotType'] = str(ballot_type)
            voter['VoteDate'] = str(vote_date)
            voter['Notes'] = str(notes)

            VOTER_ROSTER.append(voter)

        cur_row = cur_row + 1

    # Print out a status update
    print(f"Processing Excel workbook '{pathname}' TotalVoterCount: {len(VOTER_ROSTER)}")

    return ret_val


#-----------------------------------------------------------------------------
# process_files()
#
# This function prcesses all of the files in the specified directory and
# subdirectories looking for Excel workbooks.
#-----------------------------------------------------------------------------
def process_files(dirname):
    """Process files in the specified directory"""

    ret_val = True
    num_files = 0

    for dirpath, dirnames, filenames in os.walk(dirname):
        for filename in filenames:

            # Skip temporary or lock files that often begin with '~'
            if filename.startswith('~'):
                continue

            if re.search(r'.xlsx', filename, flags=re.IGNORECASE):

                # Create the pathname to the Excel workbook
                pathname = os.path.join(dirpath, filename)

                # Get the ballot type and vote date from the filename
                result, ballot_type, vote_date = parse_info_from_workbook_filename(filename)
                if result is False:
                    print(f"Error occurred getting ballot type and vote date from {pathname}")
                    ret_val = False
                    break

                # Process the workbook
                num_files = num_files + 1
                result = process_excel_workbook(pathname, ballot_type, vote_date)
                if result is False:
                    print(f"Error occurred when processing workbook {pathname}")
                    ret_val = False
                    break

    return ret_val, num_files


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Analyze the Excel files in the current directory
    result, num_files = process_files(r".")

    # Analyze the voter roster data
    analyze_roster_vuid_numbers()

    if result is True:
        print(f"Successfully processed {num_files} Excel workbooks")

        # Save the data for additional processing
        db = shelve.open('VoterRosterDatabase.dat')
        db['Version'] = VOTER_ROSTER_VERSION
        db['VoterRoster'] = VOTER_ROSTER
        db.close()

    else:
        print(r"Error encounted when processing Excel workbooks")

main()
