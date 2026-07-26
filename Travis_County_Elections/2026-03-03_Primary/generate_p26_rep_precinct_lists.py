#-----------------------------------------------------------------------------
# generate_p26_rep_precinct_lists.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to load the voter roster data previously processed by the
# process_p26_travis_voter_roster.py script and stored in the shelve database
# 'VoterRosterDatabase.dat' and generate lists of republican voters for each precinct
# who voted in the 2026 primary election.
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements
#-----------------------------------------------------------------------------
"""generate_p26_rep_precinct_lists.py"""

import csv
import copy
import os
import sys
import shelve


# Specify the database version for the voter roster data in the shelve database
VOTER_ROSTER_VERSION = 1

# Set of all VUID numbers from the registered voter list
VUIDS = {}

# Set of precincts with at least one republican voter in the voter roster list
PRECINCTS = {}


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

        # Get the field value for the voter's residence address
        if 'RES_ADDR' in key_map:
            res_street_field = key_map['RES_ADDR']
        if 'RESIDENT_CITY' in key_map:
            res_city_field = key_map['RESIDENT_CITY']
        if 'RESIDENT_ZIP_CODE' in key_map:
            res_zip_field = key_map['RESIDENT_ZIP_CODE']

        if 'RESIDENTIAL ADDRESS' in key_map:
            res_full_addr_field = key_map['RESIDENTIAL ADDRESS']

    for record in registered_voters:
        vuid_number = str(record['VUID'])

        full_name = ''
        last_name = ''
        first_name = ''
        middle_name = ''
        dob = ''
        res_full_addr = ''
        res_street = ''
        res_city = ''
        res_zip = ''

        # Use the detected header name to extract the voter record information safely
        # pylint: disable=used-before-assignment
        vuid_number = str(record.get(vuid_field, '')).strip()
        full_name = str(record.get(fullname_field, '')).strip() if 'fullname_field' in locals() else ''
        last_name = str(record.get(lastname_field, '')).strip() if 'lastname_field' in locals() else ''
        first_name = str(record.get(firstname_field, '')).strip() if 'firstname_field' in locals() else ''
        middle_name = str(record.get(middlename_field, '')).strip() if 'middlename_field' in locals() else ''
        dob = str(record.get(dob_field, '')).strip() if 'dob_field' in locals() else ''
        res_full_addr = str(record.get(res_full_addr_field, '')).strip() if 'res_full_addr_field' in locals() else ''
        res_street = str(record.get(res_street_field, '')).strip() if 'res_street_field' in locals() else ''
        res_city = str(record.get(res_city_field, '')).strip() if 'res_city_field' in locals() else ''
        res_zip = str(record.get(res_zip_field, '')).strip() if 'res_zip_field' in locals() else ''

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
            vuid_record['ResFullAddr'] = res_full_addr
            vuid_record['ResStreet'] = res_street
            vuid_record['ResCity'] = res_city
            vuid_record['ResZip'] = res_zip
            vuids[vuid_number] = vuid_record

    print(f"Found {num_duplicates} duplicate entries in the registered voter list '{voter_list_pathname}'")

    print(f"There are {len(vuids)} voters in the registered voter list '{voter_list_pathname}'")

    return vuids


#-----------------------------------------------------------------------------
# analyze_roster_vuid_numbers()
#-----------------------------------------------------------------------------
def analyze_roster_vuid_numbers(voter_roster):
    """Analyze all of the VUID numbers from the voter roster list"""

    # Set things up for processing the voter roster list
    roster_vuids = {}
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

    print(f"Analyzing VUID numbers for {len(voter_roster)} voters in the voter roster list")

    for voter in voter_roster:

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
    num_voters = 0

    for voter in voter_roster:
        num_voters = num_voters + 1

        # Get the voter information for the current voter roster record
        vuid_number = str(voter['VUID'])
        precinct = voter['Precinct']
        party = voter['Party']
        first_name = voter['FirstName'].strip()
        last_name = voter['LastName'].strip()
        ballot_type = voter['BallotType']
        vote_date = voter['VoteDate']
        notes = voter['Notes']

        # Only process republican voters from the voter_roster
        if party != 'REP':
            continue

        try:
            vuid_record = registered_vuids[vuid_number]

            # Make a deep copy of the voter record to add to the precinct list, since we will
            # modify it with address information from the registered voter list
            precinct_voter = copy.deepcopy(voter)

            # Setup the address to add to the precinct voter record
            res_full_addr = vuid_record['ResFullAddr']
            res_street = vuid_record['ResStreet']
            res_city = vuid_record['ResCity']
            res_zip = vuid_record['ResZip']
            if len(res_full_addr) > 0:
                precinct_voter['ResAddress'] = res_full_addr
            elif len(res_street) > 0 and len(res_city) > 0 and len(res_zip) > 0:
                precinct_voter['ResAddress'] = f"{res_street}, {res_city}, TX {res_zip}"
            else:
                precinct_voter['ResAddress'] = 'Unknown'

            # print(f"Voter Address: {precinct_voter['ResAddress']}")

            try:
                # Find the precinct record in the list of all precincts
                precinct_record = PRECINCTS[precinct]

            except KeyError:

                # The precinct was not found in the list, so we add it
                precinct_record = {'Precinct': precinct}
                precinct_record['Voters'] = []
                PRECINCTS[precinct] = precinct_record

            # Now we add the voter record to the precinct record
            precinct_record['Voters'].append(precinct_voter)

        except KeyError:

            # We did not find the voter record in the registered voter list
            num_unknown_voter = num_unknown_voter + 1
            unknown_voter_roster.append(voter)
            if show_voters:
                print(f"Did not find VUID,{vuid_number},{precinct},{ballot_type},{vote_date},{first_name},{last_name},{party}")

    print(f"Analyzed {num_voters} voters from the voter roster against registered voter list '{voter_list_pathname}'")
    print(f"Found {num_unknown_voter} private/unknown voter records")

    return unknown_voter_roster


#-----------------------------------------------------------------------------
# add_unknown_voters_to_precincts()
#-----------------------------------------------------------------------------
def add_unknown_voters_to_precincts(unknown_voter_roster):
    """This function adds the unknown voters from the voter roster to the
    precincts with republican voters, since they voted in the republican primary
    election but we could not find them in the registered voter list"""

    if not unknown_voter_roster:
        return

    print(f"Adding {len(unknown_voter_roster)} unknown voters from the voter roster to the precincts with republican voters")

    for voter in unknown_voter_roster:

        # The voter address is unknown since we could not find the voter in the registered voter list
        voter['ResAddress'] = ''
        precinct = voter['Precinct']

        try:
            # Find the precinct record in the list of all precincts
            precinct_record = PRECINCTS[precinct]

        except KeyError:

            # The precinct was not found in the list, so we add it
            precinct_record = {'Precinct': precinct}
            precinct_record['Voters'] = []
            PRECINCTS[precinct] = precinct_record

        # Now we add the voter record to the precinct record
        precinct_record['Voters'].append(voter)


#-----------------------------------------------------------------------------
# write_precinct_voters_to_disk()
#-----------------------------------------------------------------------------
def write_precinct_voters_to_disk(precinct_output_dir):
    """Write the precinct voter lists to disk as CSV files in the specified output directory"""

    total_voters = 0

    for precinct, precinct_record in PRECINCTS.items():
        voters = precinct_record['Voters']

        # Sort voters alphabetically A->Z by LastName then FirstName (case-insensitive)
        sorted_voters = sorted(
            voters,
            key=lambda v: (
                (v.get('LastName') or '').casefold(),
                (v.get('FirstName') or '').casefold()
            )
        )

        # Create the output file path for the current precinct and write the sorted voters to a CSV file
        output_pathname = os.path.join(precinct_output_dir, f"{precinct}_rep_voters.csv")
        try:
            with open(output_pathname, 'w', encoding='utf-8', newline='') as csv_file:
                fieldnames = ['Precinct', 'VUID', 'LastName', 'FirstName', 'BallotType', 'VoteDate', 'ResAddress']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                for voter in sorted_voters:
                    total_voters += 1

                    writer.writerow({
                        'Precinct': voter['Precinct'],
                        'VUID': voter['VUID'],
                        'LastName': voter['LastName'],
                        'FirstName': voter['FirstName'],
                        'BallotType': voter['BallotType'],
                        'VoteDate': voter['VoteDate'],
                        'ResAddress': voter.get('ResAddress', '')
                    })
            print(f"Wrote {len(voters)} republican voters for precinct '{precinct}' to '{output_pathname}'")
        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Error writing precinct voters to file {output_pathname}: {exc}")

    print(f"Wrote a total of {total_voters} republican voters across {len(PRECINCTS)} precincts to disk in '{precinct_output_dir}'")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 2:
        print("Usage: python generate_p26_rep_precinct_lists.py <precinct_dir> <registered_voter_list_file_path>")
        return False
    precinct_output_dir = sys.argv[1]
    voter_list_pathname_1 = sys.argv[2]

    # Check to ensure that the precinct output directory exists and is a directory
    if not os.path.exists(precinct_output_dir):
        print(f"Precinct output directory '{precinct_output_dir}' does not exist")
        return False
    if not os.path.isdir(precinct_output_dir):
        print(f"Precinct output directory '{precinct_output_dir}' is not a directory")
        return False

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
    registered_vuids = analyze_vuid_numbers(registered_voters, voter_list_pathname_1)

    # Analyze the voter roster against the registered voter list
    unknown_voter_roster = analyze_voter_roster(voter_roster, registered_vuids, voter_list_pathname_1, True)

    # Read in optional second registered voter list
    if (len(sys.argv) == 4) and (len(unknown_voter_roster) > 0):
        voter_list_pathname_2 = sys.argv[3]

        print(f"\n\nAnalyzing unknown voter roster records against second registered voter list '{voter_list_pathname_2}'")
        registered_voters_2 = process_registered_voter_list(voter_list_pathname_2)

        # Analyze and sort the registered voter list
        registered_vuids_2 = analyze_vuid_numbers(registered_voters_2, voter_list_pathname_2)

        # Analyze the voter roster against the registered voter list
        unknown_voter_roster_2 = analyze_voter_roster(unknown_voter_roster, registered_vuids_2, voter_list_pathname_2, True)

        add_unknown_voters_to_precincts(unknown_voter_roster_2)

    else:
        add_unknown_voters_to_precincts(unknown_voter_roster)

    # Save the precinct voter lists to disk as CSV files in the specified output directory
    write_precinct_voters_to_disk(precinct_output_dir)


    return True


main()
