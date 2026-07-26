#-----------------------------------------------------------------------------
# process_l26_travis_registered_voters.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to load and process the registered voter list from Travis County
# and analyze the voter roster data previously processed by the
# process_p26_travis_voter_rosters.py script and stored in the shelve database
# 'VoterRosterDatabase.dat'
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements
#-----------------------------------------------------------------------------
"""process_l26_travis_registered_voters.py"""

import csv
import sys
import shelve


# Specify the database version for the voter roster data in the shelve database
VOTER_ROSTER_VERSION = 1

# Set of all VUID numbers from the registered voter list
VUIDS = {}


#-----------------------------------------------------------------------------
# process_registered_voter_list()
#-----------------------------------------------------------------------------
def process_registered_voter_list(pathname):
    """Process the specified voter registration list with robust encoding handling"""

    print(f"Reading data from '{pathname}'...")

    registered_voters = []
    voter_count = 0

    # Try encodings in this order to handle files that aren't valid UTF-8
    candidate_encodings = ['utf-8',  'cp1252', 'utf-8-sig','latin-1']
    used_encoding = None

    for enc in candidate_encodings:

        # Clear any existing data in case function is called multiple times
        registered_voters.clear()
        voter_count = 0

        try:
            with open(pathname, 'r', encoding=enc, errors='strict') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
                for record in reader:
                    registered_voters.append(record)
                    voter_count += 1
            used_encoding = enc
            break

        except UnicodeDecodeError:

            # Start over with the next encoding if we encounter a decoding error
            continue

        except FileNotFoundError:
            print(f"File not found: {pathname}")
            registered_voters.clear()
            return registered_voters

        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Error reading file {pathname} with encoding {enc}: {exc}")
            registered_voters.clear()
            return registered_voters

    # If no encoding succeeded, do a final attempt with replacement to avoid crashing
    if used_encoding is None:

        # Clear any existing data since we are starting over again
        registered_voters.clear()
        voter_count = 0

        try:
            with open(pathname, 'r', encoding='utf-8', errors='replace') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
                for record in reader:
                    registered_voters.append(record)
                    voter_count += 1
            used_encoding = 'utf-8 (replace)'

        except FileNotFoundError:
            print(f"File not found: {pathname}")
            registered_voters.clear()
            return registered_voters

        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Failed to read file {pathname} even with replacement errors: {exc}")
            registered_voters.clear()
            return registered_voters

    print(f"Read in {voter_count} voters from '{pathname}' (encoding={used_encoding})")

    return registered_voters


#-----------------------------------------------------------------------------
# analyze_vuid_numbers()
#-----------------------------------------------------------------------------
def analyze_vuid_numbers(registered_voters, voter_list_pathname):
    """Analyze all of the VUID numbers from the registered voter list"""

    vuids = {}
    num_duplicates = 0
    dob_available = False

    # Determine which VUID header is present: 'VUID' or 'VUIDNO' (case-insensitive)
    vuid_field = None
    if registered_voters:

        # Generate the key map from the first record's keys, normalizing them for comparison
        first_record = registered_voters[0]
        key_map = { (k or '').strip().upper(): k for k in first_record.keys() }

        # Get the field values for the VUID
        if 'VUID' in key_map:
            vuid_field = key_map['VUID']
        elif 'VUIDNO' in key_map:
            vuid_field = key_map['VUIDNO']

        # Get the field values for the voter's full name
        if 'NAME' in key_map:
            fullname_field = key_map['NAME']

        # Get the field values for the voter's last name
        if 'LAST_NAME' in key_map:
            lastname_field = key_map['LAST_NAME']
        elif 'LSTNAM' in key_map:
            lastname_field = key_map['LSTNAM']

        # Get the field values for the voter's first name
        if 'FIRST_NAME' in key_map:
            firstname_field = key_map['FIRST_NAME']
        elif 'FSTNAM' in key_map:
            firstname_field = key_map['FSTNAM']

        # Get the field values for the voter's middle name
        if 'MIDDLE_NAME' in key_map:
            middlename_field = key_map['MIDDLE_NAME']
        elif 'MIDNAM' in key_map:
            middlename_field = key_map['MIDNAM']

        # Get the field value for the voter's date of birth
        if 'DATE_OF_BIRTH' in key_map:
            dob_field = key_map['DATE_OF_BIRTH']
            dob_available = True

        # Get the field value for the voter's school district
        if 'SCHOOL' in key_map:
            isd_field = key_map['SCHOOL']

    for record in registered_voters:
        vuid_number = str(record['VUID'])

        # Use the detected header name to extract the voter record information safely
        # pylint: disable=used-before-assignment
        vuid_number = str(record.get(vuid_field, '')).strip()
        full_name = str(record.get(fullname_field, '')).strip() if 'fullname_field' in locals() else ''
        last_name = str(record.get(lastname_field, '')).strip() if 'lastname_field' in locals() else ''
        first_name = str(record.get(firstname_field, '')).strip() if 'firstname_field' in locals() else ''
        middle_name = str(record.get(middlename_field, '')).strip() if 'middlename_field' in locals() else ''
        dob = str(record.get(dob_field, '')).strip() if 'dob_field' in locals() else ''
        isd = str(record.get(isd_field, '')).strip() if 'isd_field' in locals() else ''

        try:
            # Find the vuid number in the list of all VUIDs
            vuid_record = vuids[vuid_number]

            # We found it
            #print(f"Found duplicate VUID in the list {vuid_number}")
            #print(f"Original:  {vuid_record['VoterRecord']}")
            #print(f"Duplicate: {record}")
            num_duplicates = num_duplicates + 1

        except KeyError:

            # The vuid number was not found in the list (as expected), so we add it
            vuid_record = {'VoterRecord': record}
            vuid_record['FullName'] = full_name
            vuid_record['LastName'] = last_name
            vuid_record['FirstName'] = first_name
            vuid_record['MiddleName'] = middle_name
            vuid_record['DOB'] = dob
            vuid_record['ISD'] = isd
            vuids[vuid_number] = vuid_record

    print(f"Found {num_duplicates} duplicate entries in the registered voter list '{voter_list_pathname}'")

    print(f"There are {len(vuids)} voters in the registered voter list '{voter_list_pathname}'")

    return vuids, dob_available


#-----------------------------------------------------------------------------
# analyze_roster_vuid_numbers()
#-----------------------------------------------------------------------------
def analyze_roster_vuid_numbers(voter_roster):
    """Analyze all of the VUID numbers from the voter roster list"""

    # Set things up for processing the voter roster list
    roster_vuids = {}
    num_duplicates = 0
    num_bbm = 0
    num_ev = 0
    num_ed = 0

    print(f"Analyzing VUID numbers for {len(voter_roster)} voters in the voter roster list")

    for voter in voter_roster:

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
            vuid_record = roster_vuids[vuid_number]

            # We found it
            print(f"Found duplicate VUID in the list {vuid_number}")
            print(f"Original:  {vuid_record['VoterRecord']}")
            print(f"Duplicate: {voter}")
            num_duplicates = num_duplicates + 1

        except KeyError:

            # The vuid number was not found in the list (as expected), so we add it
            voter_record = {'VoterRecord': voter}
            roster_vuids[vuid_number] = voter_record

    print(f"Found {num_duplicates} duplicate entries in the voter roster lists")


#-----------------------------------------------------------------------------
# analyze_voter_roster()
#-----------------------------------------------------------------------------
def analyze_voter_roster(voter_roster, registered_vuids, voter_list_pathname, show_voters=False):
    """Analyze the provided voter roster against the registered voter list"""

    if not voter_roster:
        print(r"No voter_roster provided for analysis")
        return []

    if not registered_vuids:
        print(r"No registered VUIDs provided for analysis")
        return []

    print(f"Analyzing {len(voter_roster)} voter roster entries against {len(registered_vuids)} registered VUIDs")

    # Set things up for processing the voter roster list
    unknown_voter_roster = []
    num_unknown_voter = 0
    num_name_changes = 0
    num_name_correct = 0
    num_voters = 0
    eanes_isd_voters_bbm = 0
    eanes_isd_voters_ev = 0
    eanes_isd_voters_ed = 0

    for voter in voter_roster:
        num_voters = num_voters + 1

        # Get the voter information for the current voter roster record
        vuid_number = str(voter['VUID'])
        precinct = voter['Precinct']
        first_name = voter['FirstName'].strip()
        last_name = voter['LastName'].strip()
        ballot_type = voter['BallotType']
        vote_date = voter['VoteDate']
        notes = voter['Notes']

        try:
            vuid_record = registered_vuids[vuid_number]

            # We found the voter record in the registered voter list
            record = vuid_record['VoterRecord']
            rv_full_name = vuid_record['FullName'] if 'FullName' in vuid_record else ''
            rv_first_name = vuid_record['FirstName'] if 'FirstName' in vuid_record else ''
            rv_last_name = vuid_record['LastName'] if 'LastName' in vuid_record else ''
            rv_middle_name = vuid_record['MiddleName'] if 'MiddleName' in vuid_record else ''
            rv_dob = vuid_record['DOB'] if 'DOB' in vuid_record else ''
            rv_isd = vuid_record['ISD'] if 'ISD' in vuid_record else ''

            # Handle the case where the registered voter list has a single NAME field with "LAST_NAME, FIRST_NAME MIDDLE_NAME" format and we need to parse it
            if not rv_first_name and not rv_last_name and 'NAME' in record:
                name_parts = [item.strip() for item in rv_full_name.split(",")]
                rv_last_name = name_parts[0].strip()
                first_mid_name = name_parts[1].strip()

                if len(first_mid_name) > 1:
                    rv_first_name = first_mid_name.split()[0].strip()
                else:
                    rv_first_name = first_mid_name

            # Compare first and last names in a case-insensitive manner (normalize with .casefold())
            if rv_last_name.casefold() != last_name.casefold():
                num_name_changes = num_name_changes + 1
                if show_voters:
                    print(f"Name mismatch for VUID {vuid_number} [{precinct}]: '{rv_first_name} {rv_last_name}' '{first_name} {last_name}'")
            else:
                num_name_correct = num_name_correct + 1

            # Look for Eanes ISD voters and aggregate by ballot type
            if rv_isd == 'S04':
                if ballot_type == 'BBM':
                    eanes_isd_voters_bbm += 1
                elif ballot_type == 'EV':
                    eanes_isd_voters_ev += 1
                elif ballot_type == 'ED':
                    eanes_isd_voters_ed += 1

        except KeyError:

            # We did not find the voter record in the registered voter list
            num_unknown_voter = num_unknown_voter + 1
            unknown_voter_roster.append(voter)
            if show_voters:
                print(f"Did not find VUID,{vuid_number},{precinct},{ballot_type},{vote_date},{first_name},{last_name}")

    print(f"Analyzed {num_voters} voters from the voter roster against registered voter list '{voter_list_pathname}'")
    print(f"Found {num_name_correct} voters with no name changes")
    print(f"Found {num_name_changes} voter records with name changes")
    print(f"Found {num_unknown_voter} private/unknown voter records")

    print(f"Found {eanes_isd_voters_bbm} Eanes ISD voters in the BBM ballot type")
    print(f"Found {eanes_isd_voters_ev} Eanes ISD voters in the EV ballot type")
    print(f"Found {eanes_isd_voters_ed} Eanes ISD voters in the ED ballot type")

    return unknown_voter_roster


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 2:
        print("Usage: python process_l26_travis_registered_voters.py <registered_voter_list_file_path>")
        return False
    voter_list_pathname_1 = sys.argv[1]

    # Read in the processed voter roster from the election - it must be in the
    # current directory and named 'VoterRosterDatabase.dat' (created by process_l26_travis_voter_roster.py)
    try:
        # Load in the data from the database
        db = shelve.open('VoterRosterDatabase.dat')
        voter_roster_version = db['Version']
        voter_roster = db['VoterRoster']
        db.close()

    except KeyError:

        # Return if we cannot open the database file
        print(r"Cannot open database file 'VoterRosterDatabase'")
        return False

    # Check the database version
    if voter_roster_version != VOTER_ROSTER_VERSION:
        print(f"Voter roster database version {voter_roster_version} does not match expected version {VOTER_ROSTER_VERSION}")
        return False

    # Analyze the voter roster list
    analyze_roster_vuid_numbers(voter_roster)

    # Read in the registered voter list
    registered_voters = process_registered_voter_list(voter_list_pathname_1)

    # Analyze and sort the registered voter list
    registered_vuids, dob_available = analyze_vuid_numbers(registered_voters, voter_list_pathname_1)

    # Analyze the voter roster against the registered voter list
    unknown_voter_roster = analyze_voter_roster(voter_roster, registered_vuids, voter_list_pathname_1, True)

    # Read in optional second registered voter list
    if (len(sys.argv) == 3) and (len(unknown_voter_roster) > 0):
        voter_list_pathname_2 = sys.argv[2]

        print(f"\n\nAnalyzing unknown voter roster records against second registered voter list '{voter_list_pathname_2}'")
        registered_voters_2 = process_registered_voter_list(voter_list_pathname_2)

        # Analyze and sort the registered voter list
        registered_vuids_2, dob_available_2 = analyze_vuid_numbers(registered_voters_2, voter_list_pathname_2)

        # Analyze the voter roster against the registered voter list
        analyze_voter_roster(unknown_voter_roster, registered_vuids_2, voter_list_pathname_2, True)

    return True


main()
