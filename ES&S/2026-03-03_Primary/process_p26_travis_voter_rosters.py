#-----------------------------------------------------------------------------
# process_p26_travis_voter_rosters.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to process Excel voter rosters from Travis County Elections.
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=too-many-statements
#-----------------------------------------------------------------------------
# PSEUDOCODE / IMPLEMENTATION PLAN:
#
# 1. Walk through directory tree and find .xlsx files (skip temp files starting with '~').
# 2. For each workbook file:
#    a. Parse ballot type and vote date from the filename (expect format MM.DD.YYYY.Type).
#    b. Open workbook with pandas.ExcelFile.
#    c. Ensure workbook has expected number of sheets (2).
#    d. For each sheet:
#       i. Determine party from sheet name (DEM or REP).
#       ii. Read sheet into DataFrame.
#       iii. Determine header row offset depending on ballot type (BBM uses row index 3, EV/ED use 2).
#       iv. Validate header labels in the header row.
#       v. For each data row after headers:
#          - Extract VUID, precinct, first name, last name, optional notes.
#          - Only append a voter record when:
#              * headers previously validated (valid_headers is True), AND
#              * precinct is not empty/NaN, AND
#              * last_name is not empty/NaN.
#          - Normalize fields (convert to str, strip whitespace) and store BallotType, VoteDate, Party.
#    e. Keep counts and print processing status.
# 3. After processing all files, analyze roster VUIDs to detect duplicates and aggregate ballot types.
# 4. If processing succeeded, persist VOTER_ROSTER and version to a shelve database.
#
# Implementation notes:
# - Use pandas.isna to detect NaN values and also check stripped strings to avoid empty values.
#-----------------------------------------------------------------------------
"""process_p26_travis_voter_rosters.py"""

import os
import re
import shelve

# 3rd Party imports
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
    num_bbm_rep = 0
    num_bbm_dem = 0
    num_ev = 0
    num_ev_rep = 0
    num_ev_dem = 0
    num_ed = 0
    num_ed_rep = 0
    num_ed_dem = 0

    for voter in VOTER_ROSTER:

        # Get the voter information for the current voter roster record
        vuid_number = str(voter['VUID'])
        party = voter['Party']
        precinct = voter['Precinct']
        first_name = voter['FirstName']
        last_name = voter['LastName']
        ballot_type = voter['BallotType']
        vote_date = voter['VoteDate']
        notes = voter['Notes']

        # Aggregate the ballot types
        if ballot_type == 'BBM':
            num_bbm += 1
            if party == 'REP':
                num_bbm_rep += 1
            else:
                num_bbm_dem += 1

        elif ballot_type == 'EV':
            num_ev += 1
            if party == 'REP':
                num_ev_rep += 1
            else:
                num_ev_dem += 1

        elif ballot_type == 'ED':
            num_ed += 1
            if party == 'REP':
                num_ed_rep += 1
            else:
                num_ed_dem += 1

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

    print(f"BBM Roster Voters: {num_bbm}  Rep / Dem:  {num_bbm_rep} / {num_bbm_dem}")
    print(f"ED  Roster Voters: {num_ed}  Rep / Dem:  {num_ed_rep} / {num_ed_dem}")
    print(f"EV  Roster Voters: {num_ev}  Rep / Dem:  {num_ev_rep} / {num_ev_dem}")


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
#-----------------------------------------------------------------------------
def process_excel_workbook(pathname, ballot_type, vote_date):
    """Process the specified Excel workbook"""

    ret_val = True

    # Use pandas to read in the Excel workbook
    xlsx = pd.ExcelFile(pathname)

    num_voters = 0
    num_rep_voters = 0
    num_dem_voters = 0
    num_data_errors = 0

    # Get the number of sheets in the workbook
    num_sheets = len(xlsx.sheet_names)
    if num_sheets != 2:
        print(f"There are {num_sheets} in {pathname} - expecting only a single sheet!")
        ret_val = False
        return ret_val

    for sheet in xlsx.sheet_names:

        if re.search(r'Dem', sheet, flags=re.IGNORECASE):
            party = 'DEM'
        elif re.search(r'Rep', sheet, flags=re.IGNORECASE):
            party = 'REP'
        else:
            print(f"Sheet name '{sheet}' in {pathname} does not contain DEM or REP!")
            ret_val = False
            return ret_val

        # Read the Excel worksheet into a Pandas data frame
        excel_data = pd.read_excel(xlsx, sheet)

        # Get the number of rows and columns from the Excel data
        num_rows, num_columns = excel_data.shape

        # Iterate through all of the data rows, we start with row 2 for early vote and
        # election day rosters and row 3 for ballot by mail rosters because of the
        # different header formats in the data.
        #
        # The column headers are validated in the code below.  Once the headers are validated,
        # we will start appending the voter information to the voter roster list.
        if ballot_type == 'BBM':
            cur_row = 3
        else:
            cur_row = 2
        valid_headers = False
        while cur_row < num_rows:

            if ballot_type in ('EV','ED'):
                vuid = excel_data.iat[cur_row, 0]
                precinct = excel_data.iat[cur_row, 3]
                first_name = excel_data.iat[cur_row, 2]
                last_name = excel_data.iat[cur_row, 1]
            else:
                vuid = excel_data.iat[cur_row, 0]
                precinct = excel_data.iat[cur_row, 1]
                first_name = excel_data.iat[cur_row, 2]
                last_name = excel_data.iat[cur_row, 3]

            # The fifth column is added in data returned after election day to
            # highlight voters under the following conditions:
            #  Chapter 102:  Late voting by a disabled voter
            #  Chapter 103:  Late voting because of death in immediate family
            if num_columns == 5:
                notes = excel_data.iat[cur_row, 4]
            else:
                notes = ""

            if ballot_type == 'BBM' and cur_row == 3:

                # Validate the column headers in the data.  Some worksheets have "Precinct" or "PCT"
                if vuid != 'VUID' or first_name != 'First Name' or last_name != 'Last Name':
                    print(f"Row 2 is {vuid} {precinct} {first_name} {last_name}")
                    ret_val = False
                    break
                valid_headers = True

            elif ballot_type in ('EV','ED') and cur_row == 2:

                # Validate the column headers in the data.  Some worksheets have "Precinct" or "PCT"
                if vuid != 'VUID' or first_name != 'First Name' or last_name != 'Last Name':
                    print(f"Row 1 is {vuid} {precinct} {first_name} {last_name}")
                    ret_val = False
                    break
                valid_headers = True

            elif valid_headers is True:

                # Only append the voter to the voter roster if precinct and name fields are not empty
                # Travis County has added partially empty rows by mistake in some of the rosters, so
                # we need to skip those rows.  We will keep track of the number of errors encountered
                # in the data for reporting purposes.  Sometimes the voter record will have either the
                # first name or last name missing - although this is very rare.
                precinct_empty = pd.isna(precinct) or str(precinct).strip() == ""
                last_name_empty = pd.isna(last_name) or str(last_name).strip() == ""
                first_name_empty = pd.isna(first_name) or str(first_name).strip() == ""
                name_empty = last_name_empty and first_name_empty

                if not precinct_empty and not name_empty:
                    voter = {'VUID': vuid}
                    voter['Party'] = party
                    voter['Precinct'] = str(precinct)
                    voter['FirstName'] = str(first_name).strip()
                    voter['LastName'] = str(last_name).strip()
                    voter['BallotType'] = str(ballot_type)
                    voter['VoteDate'] = str(vote_date)
                    voter['Notes'] = str(notes)

                    VOTER_ROSTER.append(voter)
                    num_voters = num_voters + 1
                    if party == 'REP':
                        num_rep_voters = num_rep_voters + 1
                    else:
                        num_dem_voters = num_dem_voters + 1

                else:

                    # Skip rows missing critical data and keep track of the number of errors encountered
                    num_data_errors = num_data_errors + 1

            cur_row = cur_row + 1

    # Print out a status update
    print(f"Processing Excel workbook {pathname} NumVoters: {num_voters} NumRep: {num_rep_voters} TotalVoterCount: {len(VOTER_ROSTER)}")

    if num_data_errors > 0:
        print(f"Encountered {num_data_errors} rows with missing data in {pathname}")

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

            # Only process files that end with .xlsx (case-insensitive)
            if re.search(r'\.xlsx$', filename, flags=re.IGNORECASE):

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
