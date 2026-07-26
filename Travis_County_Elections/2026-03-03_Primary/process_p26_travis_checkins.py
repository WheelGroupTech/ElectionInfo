#-----------------------------------------------------------------------------
# process_p26_travis_checkins.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to load and process the voter check-in list from Travis County
# and analyze the voter roster data previously processed by the
# process_p26_travis_voter_rosters.py script and stored in the shelve database
# 'VoterRosterDatabase.dat'
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements
#-----------------------------------------------------------------------------
"""process_p26_travis_checkins.py"""


import csv
from datetime import datetime
import math
import sys
import shelve

import pandas as pd


# Specify the database version for the voter roster data in the shelve database
VOTER_ROSTER_VERSION = 1

# Set of all VUID numbers from the voter check-in list
VUIDS = {}


#-----------------------------------------------------------------------------
# process_checkins()
#-----------------------------------------------------------------------------
def process_checkins(pathname):
    """Process the specified voter checkin list with robust encoding handling"""

    print(f"Reading data from '{pathname}'...")

    voter_checkins = []
    voter_count = 0

    # Try encodings in this order to handle files that aren't valid UTF-8
    candidate_encodings = ['utf-8',  'cp1252', 'utf-8-sig','latin-1']
    used_encoding = None

    for enc in candidate_encodings:

        # Clear any existing data in case function is called multiple times
        voter_checkins.clear()
        voter_count = 0

        try:
            with open(pathname, 'r', encoding=enc, errors='strict') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
                for record in reader:
                    voter_checkins.append(record)
                    voter_count += 1
            used_encoding = enc
            break

        except UnicodeDecodeError:

            # Start over with the next encoding if we encounter a decoding error
            continue

        except FileNotFoundError:
            print(f"File not found: {pathname}")
            voter_checkins.clear()
            return voter_checkins

        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Error reading file {pathname} with encoding {enc}: {exc}")
            voter_checkins.clear()
            return voter_checkins

    # If no encoding succeeded, do a final attempt with replacement to avoid crashing
    if used_encoding is None:

        # Clear any existing data since we are starting over again
        voter_checkins.clear()
        voter_count = 0

        try:
            with open(pathname, 'r', encoding='utf-8', errors='replace') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
                for record in reader:
                    voter_checkins.append(record)
                    voter_count += 1
            used_encoding = 'utf-8 (replace)'

        except FileNotFoundError:
            print(f"File not found: {pathname}")
            voter_checkins.clear()
            return voter_checkins

        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Failed to read file {pathname} even with replacement errors: {exc}")
            voter_checkins.clear()
            return voter_checkins

    print(f"Read in {voter_count} voter check-ins from '{pathname}' (encoding={used_encoding})")

    return voter_checkins


#-----------------------------------------------------------------------------
# analyze_vuid_numbers()
#-----------------------------------------------------------------------------
def analyze_vuid_numbers(voter_checkins, voter_list_pathname):
    """Analyze all of the VUID numbers from the voter check-in list"""

    vuids = {}
    prov_vuids = {}
    num_duplicates = 0

    # Determine which VUID header is present: 'VUID' or 'VUIDNO' (case-insensitive)
    vuid_field = None
    if voter_checkins:

        # Generate the key map from the first record's keys, normalizing them for comparison
        first_record = voter_checkins[0]
        key_map = { (k or '').strip().upper(): k for k in first_record.keys() }

        # Get the field values for the VUID
        if 'VOTERID' in key_map:
            vuid_field = key_map['VOTERID']

        # Get the field values for the voter's last name
        if 'LAST_NAME' in key_map:
            lastname_field = key_map['LAST_NAME']
        elif 'LASTNAME' in key_map:
            lastname_field = key_map['LASTNAME']

        # Get the field values for the voter's first name
        if 'FIRST_NAME' in key_map:
            firstname_field = key_map['FIRST_NAME']
        elif 'FIRSTNAME' in key_map:
            firstname_field = key_map['FIRSTNAME']

        # Get the field values for the voter's middle name
        if 'MIDDLE_NAME' in key_map:
            middlename_field = key_map['MIDDLE_NAME']
        elif 'MIDDLENAME' in key_map:
            middlename_field = key_map['MIDDLENAME']

        # Get the precinct
        if 'PRECINCT' in key_map:
            precinct_field = key_map['PRECINCT']

        # Get the field values for provisional voting
        if 'PROVISIONAL' in key_map:
            provisional_field = key_map['PROVISIONAL']

        # Get the declared party field if it exists
        if 'PARTYDECLARED' in key_map:
            party_declared_field = key_map['PARTYDECLARED']

        # Get the location name field if it exists
        if 'LOCATIONNAME' in key_map:
            location_name_field = key_map['LOCATIONNAME']

        # Get the date/time field if it exists
        if 'DATETIMERECEIVED' in key_map:
            datetime_received_field = key_map['DATETIMERECEIVED']

        # Get the consolidation code if it exists
        if 'CONSOLIDATIONCODE' in key_map:
            consolidation_code_field = key_map['CONSOLIDATIONCODE']

    for record in voter_checkins:

        # Use the detected header name to extract the voter record information safely
        # pylint: disable=used-before-assignment
        vuid_number = str(record.get(vuid_field, '')).strip()
        last_name = str(record.get(lastname_field, '')).strip() if 'lastname_field' in locals() else ''
        first_name = str(record.get(firstname_field, '')).strip() if 'firstname_field' in locals() else ''
        middle_name = str(record.get(middlename_field, '')).strip() if 'middlename_field' in locals() else ''
        precinct = str(record.get(precinct_field, '')).strip() if 'precinct_field' in locals() else ''
        provisional = str(record.get(provisional_field, '')).strip() if 'provisional_field' in locals() else ''
        party = str(record.get(party_declared_field, '')).strip() if 'party_declared_field' in locals() else ''
        location = str(record.get(location_name_field, '')).strip() if 'location_name_field' in locals() else ''
        date_time = str(record.get(datetime_received_field, '')).strip() if 'datetime_received_field' in locals() else ''
        consolidation_code = str(record.get(consolidation_code_field, '')).strip() if 'consolidation_code_field' in locals() else ''

        # Skip provisional voters
        if provisional and provisional.casefold() == 'n':
            prov = False
        else:
            prov = True

        # Get the separate date and time fields if the datetime field is present
        vote_date = ''
        vote_time = ''
        if date_time:
            date_time_parts = date_time.split()
            if len(date_time_parts) >= 2:
                vote_date = date_time_parts[0].strip()
                vote_time = date_time_parts[1].strip()
            else:
                vote_date = date_time.strip()

        # Get the ballot type from the consolidation code if it is present.  The format of the
        # consolidation code is EDxxxxx or EVxxxxx where 'xxxxx' is a number, so we can use
        # the first two characters to determine the ballot type.
        ballot_type = ''
        if consolidation_code and len(consolidation_code) >= 2:
            ballot_type_code = consolidation_code[:2].strip().upper()
            if ballot_type_code == 'ED':
                ballot_type = 'ED'
            elif ballot_type_code == 'EV':
                ballot_type = 'EV'

        #print(f"VUID: {vuid_number}, Name: '{first_name} {last_name}', Precinct: '{precinct}', Party: '{party}', Provisional: '{provisional}', Location: '{location}', VoteDate: '{vote_date}', VoteTime: '{vote_time}', BallotType: '{ballot_type}'")
        try:
            # Find the vuid number in the list of all VUIDs
            if prov:
                vuid_record = prov_vuids[vuid_number]
            else:
                vuid_record = vuids[vuid_number]

            # We found it
            if prov:
                print(f"Found duplicate provisional VUID in the list {vuid_number}")
            else:
                print(f"Found duplicate VUID in the list {vuid_number}")
            print(f"Original:  {vuid_record['VoterRecord']}")
            print(f"Duplicate: {record}")
            num_duplicates = num_duplicates + 1

        except KeyError:

            # The vuid number was not found in the list (as expected), so we add it
            vuid_record = {'VoterRecord': record}
            vuid_record['LastName'] = last_name
            vuid_record['FirstName'] = first_name
            vuid_record['MiddleName'] = middle_name
            vuid_record['BallotType'] = ballot_type
            vuid_record['Precinct'] = precinct
            vuid_record['Party'] = party
            vuid_record['Provisional'] = provisional
            vuid_record['Location'] = location
            vuid_record['VoteDate'] = vote_date
            vuid_record['VoteTime'] = vote_time
            vuid_record['InVoterRoster'] = False

            if prov:
                prov_vuids[vuid_number] = vuid_record
            else:
                vuids[vuid_number] = vuid_record

    print(f"Found {num_duplicates} duplicate entries in the voter check-in list '{voter_list_pathname}'")

    print(f"There are {len(vuids)} non-provisional voters in the voter check-in list '{voter_list_pathname}'")
    print(f"There are {len(prov_vuids)} provisional voters in the voter check-in list '{voter_list_pathname}'")

    return vuids, prov_vuids


#-----------------------------------------------------------------------------
# nan_to_empty()
#-----------------------------------------------------------------------------
def nan_to_empty(value):
    """
    Converts NaN (float or string) to an empty string.
    Handles:
      - float('nan')
      - numpy.nan / pandas.NaT
      - string 'nan' (case-insensitive)
    """
    # Check for pandas/NumPy NaN or float NaN
    if pd.isna(value) or (isinstance(value, float) and math.isnan(value)):
        return ""

    # Check for string 'nan' (case-insensitive)
    if isinstance(value, str) and value.strip().lower() == "nan":
        return ""

    return value  # Return unchanged if not NaN


#-----------------------------------------------------------------------------
# compare_dates()
#-----------------------------------------------------------------------------
def compare_dates(date_str1, date_str2, date_format="%m/%d/%y"):
    """
    Compare two date strings and return:
    -1 if date1 < date2
     0 if date1 == date2
     1 if date1 > date2

    :param date_str1: First date string
    :param date_str2: Second date string
    :param date_format: Format of the date strings (default: MM/DD/YY)
    """
    try:
        # Convert strings to datetime objects
        date1 = datetime.strptime(date_str1, date_format)
        date2 = datetime.strptime(date_str2, date_format)
    except ValueError as e:
        print(f"Error: {e}")
        return None

    # Compare the dates
    if date1 < date2:
        return -1
    if date1 > date2:
        return 1
    return 0


#-----------------------------------------------------------------------------
# analyze_voter_checkins()
#-----------------------------------------------------------------------------
def analyze_voter_checkins(voter_roster, checkin_vuids, prov_vuids, voter_list_pathname, show_voters=False):
    """Analyze the provided voter roster against the voter check-in list"""

    if not voter_roster:
        print(r"No voter_roster provided for analysis")
        return []

    if not checkin_vuids:
        print(r"No voter check-in VUIDs provided for analysis")
        return []

    if not prov_vuids:
        print(r"No provisional voter check-in VUIDs provided for analysis")
        return []

    print(f"Analyzing {len(voter_roster)} voter roster entries against {len(checkin_vuids)} voter check-in records")

    # Set things up for processing the voter roster list
    unknown_voter_roster = []
    num_unknown_voter = 0
    num_name_changes = 0
    num_name_correct = 0
    num_voters = 0

    # Process each voter in the voter roster list and compare against the voter check-in list
    for voter in voter_roster:

        # Skip voters who voted via BBM since they would not be expected to
        # appear in the voter check-in list
        if voter['BallotType'] == 'BBM':
            continue

        num_voters = num_voters + 1

        # Get the voter information for the current voter roster record
        vuid_number = str(voter['VUID'])
        precinct = voter['Precinct']
        party = voter['Party']
        first_name = nan_to_empty(voter['FirstName'])
        last_name = nan_to_empty(voter['LastName'])
        ballot_type = voter['BallotType']
        vote_date = voter['VoteDate']
        notes = nan_to_empty(voter['Notes'])

        try:
            vuid_record = checkin_vuids[vuid_number]

            # We found the voter record in the voter checkin list
            vuid_record['InVoterRoster'] = True

            rv_last_name = vuid_record['LastName']
            rv_first_name = vuid_record['FirstName']
            rv_middle_name = vuid_record['MiddleName']
            rv_party = vuid_record['Party']
            rv_vote_date = vuid_record['VoteDate']
            rv_vote_time = vuid_record['VoteTime']

            # Compare first and last names in a case-insensitive manner (normalize with .casefold())
            if rv_last_name.casefold() != last_name.casefold():
                num_name_changes = num_name_changes + 1
                if show_voters:
                    print(f"Name mismatch for VUID {vuid_number} [{precinct}]: '{rv_first_name} {rv_last_name}' '{first_name} {last_name}'")
            else:
                num_name_correct = num_name_correct + 1

            # Check the party affiliation if it is present in the voter check-in record
            if rv_party and party and rv_party.casefold() != party.casefold():
                if show_voters:
                    print(f"Party mismatch for VUID {vuid_number} [{precinct}]: '{rv_party}' vs '{party}'")

            # Check the vote date if it is present in the voter check-in record
            if compare_dates(rv_vote_date, vote_date, date_format="%m/%d/%Y") != 0:
                if show_voters:
                    print(f"Vote date mismatch for VUID {vuid_number} [{precinct}]: '{rv_vote_date}' vs '{vote_date}'")

        except KeyError:

            # We did not find the voter record in the voter check-in list
            num_unknown_voter = num_unknown_voter + 1
            unknown_voter_roster.append(voter)
            if show_voters:
                print(f"Did not find VUID {vuid_number} in voter check-ins: {first_name},{last_name},{precinct},{ballot_type},{vote_date},{party}")

    # Process each voter in the voter check-in list to find any that were not found in the voter roster list
    for vuid_number, vuid_record in checkin_vuids.items():
        if not vuid_record['InVoterRoster']:
            if show_voters:
                print(f"Did not find VUID {vuid_number} in voter roster: '{vuid_record['FirstName']},{vuid_record['LastName']},{vuid_record['Precinct']},{vuid_record['BallotType']},{vuid_record['VoteDate']},{vuid_record['Party']}")

    print(f"Analyzed {num_voters} EV/ED voters from the voter roster against voter check-in list '{voter_list_pathname}'")
    print(f"Found {num_name_correct} voters with no name changes")
    print(f"Found {num_name_changes} voter records with name changes")
    print(f"Found {num_unknown_voter} private/unknown voter records")

    return unknown_voter_roster


#-----------------------------------------------------------------------------
# analyze_polling_locations()
#-----------------------------------------------------------------------------
def analyze_polling_locations(checkin_vuids, provisional_vuids):
    """Analyze the voter check-ins at polling locations"""

    ev_sites = {}
    ed_sites = {}

    # Sort the voter check-ins into the EV and ED sites based on the ballot type and location
    for vuid_number, vuid_record in checkin_vuids.items():
        location = vuid_record['Location']
        precinct = vuid_record['Precinct']
        ballot_type = vuid_record['BallotType']

        if ballot_type == 'EV':
            if location not in ev_sites:
                ev_sites[location] = {'Voters': [], 'Precincts': set(), 'ProvVoters': [], 'ProvPrecincts': set()}
            ev_sites[location]['Voters'].append(vuid_record)
            ev_sites[location]['Precincts'].add(precinct)
        else:
            if location not in ed_sites:
                ed_sites[location] = {'Voters': [], 'Precincts': set(), 'ProvVoters': [], 'ProvPrecincts': set()}
            ed_sites[location]['Voters'].append(vuid_record)
            ed_sites[location]['Precincts'].add(precinct)

    # Sort the provisional voter check-ins into the EV and ED sites based on the ballot type and location
    for vuid_number, vuid_record in provisional_vuids.items():
        location = vuid_record['Location']
        precinct = vuid_record['Precinct']
        ballot_type = vuid_record['BallotType']
        if ballot_type == 'EV':
            if location not in ev_sites:
                ev_sites[location] = {'Voters': [], 'Precincts': set(), 'ProvVoters': [], 'ProvPrecincts': set()}
            ev_sites[location]['ProvVoters'].append(vuid_record)
            ev_sites[location]['ProvPrecincts'].add(precinct)
        else:
            if location not in ed_sites:
                ed_sites[location] = {'Voters': [], 'Precincts': set(), 'ProvVoters': [], 'ProvPrecincts': set()}
            ed_sites[location]['ProvVoters'].append(vuid_record)
            ed_sites[location]['ProvPrecincts'].add(precinct)

    # Print out the list of EV sites and the number of voters at each site in alphabetical order
    print(f"There are {len(ev_sites)} EV Sites:")
    for location in sorted(ev_sites.keys()):
        num_voters = len(ev_sites[location]['Voters'])
        num_precincts = len(ev_sites[location]['Precincts'])
        print(f"  {location}: {num_voters} voters from {num_precincts} precincts")

    # Print out the list of ED sites and the number of voters at each site in alphabetical order
    print(f"There are {len(ed_sites)} ED Sites:")
    for location in sorted(ed_sites.keys()):
        num_voters = len(ed_sites[location]['Voters'])
        num_precincts = len(ed_sites[location]['Precincts'])
        print(f"  {location}: {num_voters} voters from {num_precincts} precincts")

    # Open a CSV file named "polling_locations.csv" to write the detailed site information
    with open("polling_locations.csv", "w", encoding='utf-8', newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["Location", "Voters", "Provisional Voters", "Precincts", "Provisional Precincts"])
        for location in sorted(ev_sites.keys()):
            num_voters = len(ev_sites[location]['Voters'])
            num_prov_voters = len(ev_sites[location]['ProvVoters'])
            num_precincts = len(ev_sites[location]['Precincts'])
            num_prov_precincts = len(ev_sites[location]['ProvPrecincts'])
            csvwriter.writerow([location, num_voters, num_prov_voters, num_precincts, num_prov_precincts])
        for location in sorted(ed_sites.keys()):
            num_voters = len(ed_sites[location]['Voters'])
            num_prov_voters = len(ed_sites[location]['ProvVoters'])
            num_precincts = len(ed_sites[location]['Precincts'])
            num_prov_precincts = len(ed_sites[location]['ProvPrecincts'])
            csvwriter.writerow([location, num_voters, num_prov_voters, num_precincts, num_prov_precincts])


#-----------------------------------------------------------------------------
# identify_non_secret_ballots()
#
# This is a joint election so voters from both parties vote at the same polling
# locations, but their ballots are different based on their party affiliation.
#
# We sort ballots into groups based on the polling location, party affiliation,
# and precinct.  If the count of ballots in any group is 1, then we can identify
# the ballot as non-secret.
#-----------------------------------------------------------------------------
def identify_non_secret_ballots(checkin_vuids):
    """Analyze the voter check-ins at polling locations to identify non-secret ballots"""

    ev_groups = {}
    ed_groups = {}
    non_secret_ballot_count = 0

    for vuid_number, vuid_record in checkin_vuids.items():

        location = vuid_record['Location']
        precinct = vuid_record['Precinct']
        party = vuid_record['Party']
        ballot_type = vuid_record['BallotType']

        # We can use the combination of location, party, and precinct to identify non-secret ballots
        group_key = f"{location}|{party}|{precinct}"
        if ballot_type == "EV":
            if group_key not in ev_groups:
                ev_groups[group_key] = []
            ev_groups[group_key].append(vuid_number)
        else:
            if group_key not in ed_groups:
                ed_groups[group_key] = []
            ed_groups[group_key].append(vuid_number)

    # Print out the non-secret ballots based on the groups with only one ballot
    print("Non-secret ballots based on polling location, party, and precinct:")
    for group_key, vuid_numbers in ev_groups.items():
        if len(vuid_numbers) == 1:
            #print(f"EV Non-secret ballot: {group_key} (VUID: {vuid_numbers[0]})")
            non_secret_ballot_count += 1
    for group_key, vuid_numbers in ed_groups.items():
        if len(vuid_numbers) == 1:
            #print(f"ED Non-secret ballot: {group_key} (VUID: {vuid_numbers[0]})")
            non_secret_ballot_count += 1

    print(f"Total non-secret ballots identified: {non_secret_ballot_count}")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 2:
        print("Usage: python process_p26_travis_checkins.py <checkins_file_path>")
        return False
    voter_list_pathname_1 = sys.argv[1]

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

    # Read in the voter checkin list
    voter_checkins = process_checkins(voter_list_pathname_1)

    # Analyze and sort the voter checkin list
    checkin_vuids, prov_vuids = analyze_vuid_numbers(voter_checkins, voter_list_pathname_1)

    # Analyze the voter roster against the voter check-in list
    analyze_voter_checkins(voter_roster, checkin_vuids, prov_vuids, voter_list_pathname_1, True)

    # Analyze the polling locations for the non-provisional voters
    analyze_polling_locations(checkin_vuids, prov_vuids)

    # Identify non-secret ballots based on the combination of polling location, party affiliation, and precinct
    identify_non_secret_ballots(checkin_vuids)

    return True


main()
