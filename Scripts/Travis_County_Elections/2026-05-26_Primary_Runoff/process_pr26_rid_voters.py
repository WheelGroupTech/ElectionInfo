#-----------------------------------------------------------------------------
# process_p26_rid_voters.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to load and process the voter RID list from Travis County
# and analyze the voter roster data previously processed by the
# process_p26_travis_voter_rosters.py script and stored in the shelve database
# 'VoterRosterDatabase.dat'.
#
# RID stands for "Reasonable Impediment Declaration" and is a list of voters who
# have submitted a declaration of reasonable impediment to displaying acceptable
# voter identification for voting.
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements
#-----------------------------------------------------------------------------
"""process_p26_rid_voters.py"""

import os
import re
import shelve

# 3rd Party imports
import pandas as pd


# Published roster of voters using RID for the election
VOTER_RID_ROSTER = []

# Specify the database version for the voter roster data in the shelve database
VOTER_ROSTER_VERSION = 1


#-----------------------------------------------------------------------------
# process_excel_workbook()
#
# This function prcesses the specified Excel workbook.  It will identify
# the worksheet containing voter information and process the contents.
#-----------------------------------------------------------------------------
def process_excel_workbook(pathname):
    """Process the specified Excel workbook"""

    ret_val = True

    # Use pandas to read in the Excel workbook
    xlsx = pd.ExcelFile(pathname)

    num_voters = 0

    # Get the number of sheets in the workbook
    num_sheets = len(xlsx.sheet_names)
    if num_sheets != 1:
        print(f"There are {num_sheets} in {pathname} - expecting only one sheet!")
        return False

    # Read the Excel worksheet into a Pandas data frame
    # IMPORTANT: use header=None so DataFrame row 0 corresponds to Excel row 1.
    # Also read as strings to avoid dtype surprises.
    sheet = xlsx.sheet_names[0]
    excel_data = pd.read_excel(xlsx, sheet, header=None, dtype=str)

    # Get the number of rows and columns from the Excel data
    num_rows, num_columns = excel_data.shape

    # Attempt to find the header row by searching the first few rows for expected headers
    header_row = None
    max_header_scan = min(10, num_rows)
    for r in range(0, max_header_scan):
        try:
            # Convert each cell to a safe lowercase string; pandas may contain NaN
            row_vals = [str(excel_data.iat[r, c]).strip().lower() for c in range(num_columns)]
        except Exception:
            # If iat fails for this row, skip it
            continue

        # Log using 1-based Excel row numbers to avoid confusion
        has_vuid = any('state voter id' in v for v in row_vals)
        has_first = any('first name' in v for v in row_vals)
        has_last = any('last name' in v for v in row_vals)
        has_rid = any('no photo id reason' in v for v in row_vals)

        # If we see vuid and rid and first&last name column, choose this row
        if has_vuid and has_rid and has_first and has_last:
            header_row = r
            break

    if header_row is None:
        if num_rows > 1:
            header_row = 1
            print(f"No explicit header found; falling back to Excel row {header_row}")
        else:
            print(f"Unable to determine header row for '{pathname}'")
            return False

    # Map column indices to roles
    col_vuid = None
    col_first = None
    col_last = None
    col_middle = None
    col_rid = None
    col_id_type = None
    col_location = None
    col_check_in_date = None
    for c in range(num_columns):
        try:
            val = str(excel_data.iat[header_row, c]).strip().lower()
        except Exception:
            continue
        if 'voter id' in val:
            col_vuid = c
        elif 'first name' in val:
            col_first = c
        elif 'last' in val:
            col_last = c
        elif 'middle' in val:
            col_middle = c
        elif 'no photo id reason' in val:
            col_rid = c
        elif 'id type' in val:
            col_id_type = c
        elif 'location' in val:
            col_location = c
        elif 'check-in date' in val or 'checkin date' in val:
            col_check_in_date = c

    # Validate required columns: VUID, RID, and name components
    if col_vuid is None or col_rid is None or col_first is None or col_last is None:
        print(f"Missing required columns in '{pathname}' {col_vuid=}, {col_rid=}, {col_first=}, {col_last=}")
        return False

    # Iterate through all of the data rows starting after the header
    cur_row = header_row + 1
    while cur_row < num_rows:
        try:
            # Read values using detected column mapping
            vuid = excel_data.iat[cur_row, col_vuid]
            first = excel_data.iat[cur_row, col_first]
            last = excel_data.iat[cur_row, col_last]
            middle = excel_data.iat[cur_row, col_middle] if col_middle is not None else ''
            rid = excel_data.iat[cur_row, col_rid]
            id_type = excel_data.iat[cur_row, col_id_type] if col_id_type is not None else ''
            location = excel_data.iat[cur_row, col_location] if col_location is not None else ''
            check_in_date = excel_data.iat[cur_row, col_check_in_date] if col_check_in_date is not None else ''

        except Exception:
            # If row access fails (e.g., blank trailing rows), break loop
            break

        # Append the voter to the voter roster
        voter = {'VUID': vuid}
        voter['FirstName'] = first
        voter['LastName'] = last
        voter['MiddleName'] = middle
        voter['RID'] = rid
        voter['IDType'] = id_type
        voter['Location'] = location
        voter['VoteDate'] = check_in_date

        VOTER_RID_ROSTER.append(voter)
        num_voters = num_voters + 1

        cur_row = cur_row + 1

    # Print out a status update
    print(f"Processing Excel workbook {pathname} NumVoters: {num_voters} TotalVoterCount: {len(VOTER_RID_ROSTER)}")

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

            # Only process files that include 'RID' in the filename (case-insensitive)
            if re.search(r'RID', filename, flags=re.IGNORECASE) is None:
                continue

            # Only process files that end with .xlsx (case-insensitive)
            if re.search(r'\.xlsx$', filename, flags=re.IGNORECASE):

                # Create the pathname to the Excel workbook
                pathname = os.path.join(dirpath, filename)

                # Process the workbook
                num_files = num_files + 1
                result = process_excel_workbook(pathname)
                if result is False:
                    print(f"Error occurred when processing workbook {pathname}")
                    ret_val = False
                    break

    return ret_val, num_files


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def analyze_rid_voters(voter_roster):
    """Analyze the RID voters against the voter roster"""

    rid_reasons = {}
    rep_rid_reasons = {}
    dem_rid_reasons = {}

    total_rid_voters = 0
    total_rep_rid_voters = 0
    total_dem_rid_voters = 0

    if not voter_roster:
        print(r"No voter_roster provided for analysis")
        return False

    # Count the unique values of RID reasons in the VOTER_RID_ROSTER
    for rid_voter in VOTER_RID_ROSTER:
        total_rid_voters = total_rid_voters + 1
        rid_reason = rid_voter['RID']

        # Find the voter in the voter roster by VUID
        rid_vuid = rid_voter['VUID']
        roster_voter = None
        for voter in voter_roster:
            roster_vuid = str(voter['VUID'])
            if roster_vuid == rid_vuid:
                roster_voter = voter
                break

        # Get the party affiliation from the roster voter if found, otherwise use 'Unknown'
        party = roster_voter['Party'] if roster_voter is not None else 'Unknown'

        # Find the rid reason in the dictionary and increment the count, or add it if not found
        if rid_reason in rid_reasons:
            rid_reasons[rid_reason] = rid_reasons[rid_reason] + 1
        else:
            rid_reasons[rid_reason] = 1

        # Also count by party affiliation
        if party == 'REP':
            total_rep_rid_voters = total_rep_rid_voters + 1
            if rid_reason in rep_rid_reasons:
                rep_rid_reasons[rid_reason] = rep_rid_reasons[rid_reason] + 1
            else:
                rep_rid_reasons[rid_reason] = 1
        elif party == 'DEM':
            total_dem_rid_voters = total_dem_rid_voters + 1
            if rid_reason in dem_rid_reasons:
                dem_rid_reasons[rid_reason] = dem_rid_reasons[rid_reason] + 1
            else:
                dem_rid_reasons[rid_reason] = 1
        else:
            print(f"Warning: Voter with VUID {rid_vuid} has unknown party affiliation {rid_voter}")

    print(f"Total RID voters: {total_rid_voters}")
    print(f"Total REP RID voters: {total_rep_rid_voters}")
    print(f"Total DEM RID voters: {total_dem_rid_voters}")

    # Print out the counts of RID reasons
    print("RID Reason Counts:")
    for reason, count in rid_reasons.items():
        print(f"  {reason}: {count}")

    print("REP RID Reason Counts:")
    for reason, count in rep_rid_reasons.items():
        print(f"  {reason}: {count}")

    print("DEM RID Reason Counts:")
    for reason, count in dem_rid_reasons.items():
        print(f"  {reason}: {count}")

    return True


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Read in the processed voter roster from the election - it must be in the
    # current directory and named 'VoterRosterDatabase.dat' (created by process_p26_travis_voter_roster.py)
    try:
        # Load in the data from the database
        db = shelve.open('VoterRosterDatabase.dat')
        voter_roster_version = db['Version']
        voter_roster = db['VoterRoster']
        db.close()

    except KeyError:

        # Return if we cannot open the database file
        print(r"Cannot open database file 'VoterRosterDatabase.dat'")
        return False

    # Check the database version
    if voter_roster_version != VOTER_ROSTER_VERSION:
        print(f"Voter roster database version {voter_roster_version} does not match expected version {VOTER_ROSTER_VERSION}")
        return False

    # Analyze the Excel files in the current directory
    result, num_files = process_files(r".")

    # Read in the voter checkin list
    analyze_rid_voters(voter_roster)

    return True


main()
