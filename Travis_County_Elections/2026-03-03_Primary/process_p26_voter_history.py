#-----------------------------------------------------------------------------
# process_p26_voter_history.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to load and process the registered voter list from Travis County
# with history and analyze the voter roster data previously processed by the
# process_p26_travis_voter_rosters.py script and stored in the shelve database
# 'VoterRosterDatabase.dat'
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements,unused-argument
#-----------------------------------------------------------------------------
"""process_p26_voter_history.py"""

import csv
import sys
import shelve
from datetime import datetime, timedelta


# Specify the database version for the voter roster data in the shelve database
VOTER_ROSTER_VERSION = 1

# Dates of the primaries for 2012 through 2026
P26_PRIMARY_DATE = '2026-03-03'
P24_PRIMARY_DATE = '2024-03-05'
P22_PRIMARY_DATE = '2022-03-01'
P20_PRIMARY_DATE = '2020-03-03'
P18_PRIMARY_DATE = '2018-03-06'
P16_PRIMARY_DATE = '2016-03-01'
P14_PRIMARY_DATE = '2014-03-04'
P12_PRIMARY_DATE = '2012-05-29'

#-----------------------------------------------------------------------------
# process_registered_voter_list_with_history()
#-----------------------------------------------------------------------------
def process_registered_voter_list_with_history(pathname):
    """Process the specified voter registration list with robust encoding handling"""

    print(f"Reading data from '{pathname}'...")

    # Create a set to track all VUIDs
    vuids = {}
    num_duplicates = 0
    num_skipped = 0

    registered_voters = []
    voter_count = 0

    # Try encodings in this order to handle files that aren't valid UTF-8
    candidate_encodings = ['utf-8',  'cp1252', 'utf-8-sig', 'latin-1']
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
            return vuids

        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Error reading file {pathname} with encoding {enc}: {exc}")
            registered_voters.clear()
            return vuids

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
            return vuids

        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Failed to read file {pathname} even with replacement errors: {exc}")
            registered_voters.clear()
            return vuids

    print(f"Read in {voter_count} voters from '{pathname}' (encoding={used_encoding})")

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

        # Get the effective date of the voter registration
        if 'EDRDAT' in key_map:
            edrdate_field = key_map['EDRDAT']

        # Get the primary voting party fields
        if 'P24PARTY' in key_map:
            p24_party_field = key_map['P24PARTY']
        if 'P22PARTY' in key_map:
            p22_party_field = key_map['P22PARTY']
        if 'P20PARTY' in key_map:
            p20_party_field = key_map['P20PARTY']
        if 'P18PARTY' in key_map:
            p18_party_field = key_map['P18PARTY']
        if 'P16PARTY' in key_map:
            p16_party_field = key_map['P16PARTY']
        if 'P14PARTY' in key_map:
            p14_party_field = key_map['P14PARTY']
        if 'P12PARTY' in key_map:
            p12_party_field = key_map['P12PARTY']

    if vuid_field is None:
        print(f"No 'VUID' or 'VUIDNO' header found in '{pathname}'.")
        return vuids


    for record in registered_voters:

        last_name = ''
        first_name = ''
        middle_name = ''
        edrdate = ''
        p2024_party = ''
        p2022_party = ''
        p2020_party = ''
        p2018_party = ''
        p2016_party = ''
        p2014_party = ''
        p2012_party = ''

        # Use the detected header name to extract the voter record information safely
        # pylint: disable=used-before-assignment
        vuid_number = str(record.get(vuid_field, '')).strip()
        last_name = str(record.get(lastname_field, '')).strip() if 'lastname_field' in locals() else ''
        first_name = str(record.get(firstname_field, '')).strip() if 'firstname_field' in locals() else ''
        middle_name = str(record.get(middlename_field, '')).strip() if 'middlename_field' in locals() else ''
        edrdate = str(record.get(edrdate_field, '')).strip() if 'edrdate_field' in locals() else ''
        p2024_party = str(record.get(p24_party_field, '')).strip() if 'p24_party_field' in locals() else ''
        p2022_party = str(record.get(p22_party_field, '')).strip() if 'p22_party_field' in locals() else ''
        p2020_party = str(record.get(p20_party_field, '')).strip() if 'p20_party_field' in locals() else ''
        p2018_party = str(record.get(p18_party_field, '')).strip() if 'p18_party_field' in locals() else ''
        p2016_party = str(record.get(p16_party_field, '')).strip() if 'p16_party_field' in locals() else ''
        p2014_party = str(record.get(p14_party_field, '')).strip() if 'p14_party_field' in locals() else ''
        p2012_party = str(record.get(p12_party_field, '')).strip() if 'p12_party_field' in locals() else ''

        if not vuid_number:
            # Skip records with missing/empty VUID values but count them
            num_skipped += 1
            continue

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
            vuid_record['LastName'] = last_name
            vuid_record['FirstName'] = first_name
            vuid_record['MiddleName'] = middle_name
            vuid_record['EDRDate'] = edrdate
            vuid_record['History'] = ''
            vuid_record['RepScore'] = ''
            vuid_record['FieldRepScore'] = ''
            vuid_record['P2026Party'] = ''
            vuid_record['P2024Party'] = p2024_party
            vuid_record['P2022Party'] = p2022_party
            vuid_record['P2020Party'] = p2020_party
            vuid_record['P2018Party'] = p2018_party
            vuid_record['P2016Party'] = p2016_party
            vuid_record['P2014Party'] = p2014_party
            vuid_record['P2012Party'] = p2012_party

            vuids[vuid_number] = vuid_record

    print(f"Found {num_duplicates} duplicate entries in '{pathname}'")
    if num_skipped:
        print(f"Skipped {num_skipped} records with missing/empty '{vuid_field}' values in '{pathname}'")

    print(f"There are {len(vuids)} voters in '{pathname}'")

    return vuids


#-----------------------------------------------------------------------------
# update_voter_history_with_p26_data()
#-----------------------------------------------------------------------------
def update_voter_history_with_p26_data(voter_roster, vuids):
    """Update the voter history with the 2026 primary data from the VUID index"""

    for voter in voter_roster:

        vuid_number = str(voter['VUID'])
        party = voter['Party']

        # Update the voter history record for this VUID with the 2026 primary party
        if vuid_number in vuids:
            vuids[vuid_number]['P2026Party'] = party


#-----------------------------------------------------------------------------
# create_voting_history_string()
#
# Create a string representation of the voter's history of primary voting based
# on the available data for the voter record.  The string has the format:
# PPPPPPPP where each P is the party voted in the primary for that year,
# starting with 2026 and going back to 2012.
#
# The effective date of registration (EDRDate) is used to determine if the voter
# was elegible to vote in the primary for that year.  The effective date of
# registration must 30 days before the primary election of that year for the
# voter to be eligible to vote.
#
# The party is represented by the first letter of the party name (e.g. 'R' for
# Republican, 'D' for Democrat, etc.).  If the voter did not vote in the primary
# for that year, it is represented by an underscore '_'.  If the voter was not
# eligible to vote in the primary for that year based on their effective date of
# registration, it is represented by a tilde '~'.
#
# The string is constructed in reverse chronological order, starting with the
# most recent primary (2026) and going back to the oldest primary (2012).
# This allows for a quick visual representation of the voter's history of primary
# voting, with the most recent primary on the left and the oldest primary on the right.
#
# For example, a voter who voted in the Republican
# primary in 2026, 2024, and 2020, but did not vote in the primaries in 2022, 2018,
# 2016, 2014, or 2012 would have the history string 'RR_R____'.
#
# A voter who voted in the Republican primary in 2026, voted in the Democrat primary
# in 2024 and 2022, did not vote in the primaries in 2020 and 2018, voted Republican
# in 2016, and was not eligible to vote in the primaries in 2014 and 2012 based on their effective
# date of registration would have the history string 'RDD__R~~'.
#-----------------------------------------------------------------------------
def create_voting_history_string(vuid_number, vuid_record):
    """Create a string representation of the voter's history of primary voting based on the available data for the voter record"""

    history = ''

    # Define the primary election dates and corresponding party fields in reverse chronological order
    primary_data = [
        (P26_PRIMARY_DATE, 'P2026Party'),
        (P24_PRIMARY_DATE, 'P2024Party'),
        (P22_PRIMARY_DATE, 'P2022Party'),
        (P20_PRIMARY_DATE, 'P2020Party'),
        (P18_PRIMARY_DATE, 'P2018Party'),
        (P16_PRIMARY_DATE, 'P2016Party'),
        (P14_PRIMARY_DATE, 'P2014Party'),
        (P12_PRIMARY_DATE, 'P2012Party')
    ]

    # Parse the EDRDate into a date object if possible
    edr_date_str = vuid_record.get('EDRDate', '').strip()
    edr_dt = None
    if edr_date_str:
        try:
            edr_dt = datetime.strptime(edr_date_str, '%m/%d/%Y').date()
        except Exception:
            # If parsing fails, treat as no valid EDR (not eligible)
            edr_dt = None

    for primary_date, party_field in primary_data:
        party = vuid_record.get(party_field, '').upper()
        if party:
            history += party[0]  # Use the first letter of the party name
        else:

            try:
                primary_dt = datetime.strptime(primary_date, '%Y-%m-%d').date()
            except Exception:
                # If primary date parsing fails, mark as not eligible to be safe
                history += '%'
                continue

            # Voter is eligible only if EDRDate exists and is at least 30 days
            # before the primary date (EDRDate <= primary_date - 30 days)
            eligibility_cutoff = primary_dt - timedelta(days=30)
            if edr_dt and edr_dt <= eligibility_cutoff:

                # Voter was eligible to vote in this primary
                history += '_'  # Did not vote in this primary
            else:
                history += '~'  # Not eligible to vote in this primary

    return history


#-----------------------------------------------------------------------------
# calculate_weighted_sequence_score()
#
# Calculate the weighted percentage score for a sequence of up to 8 letters.
# Stops calculation at the first '~' (if present) and uses only the prefix.
#
#   Values:
#       'R' = 1.0
#       '_' = 0.5
#       'D' = 0.0
#       '~' = terminator (ignore this and all following characters)
#
#    Weights: 1, 1/2, 1/4, 1/8, ... (halving each position)
#    The score is normalized using only the weights of positions actually used.
#
#   Examples:
#       "RDRR____"  ->  71.96%
#       "RDRR~~~~"  ->  73.33% higher than full sequence (ignores trailing ____)
#       "R~~~~~~~"  -> 100.00% (only first R)
#       "~~~~~~~~"  ->   0.00% (empty prefix)
#
#   Args:
#       sequence: string of length exactly 8 (R, D, _, ~ allowed)
#
#   Returns:
#       float: score as percentage (0.0 to 100.0), rounded to 2 decimal places
#-----------------------------------------------------------------------------
def calculate_weighted_sequence_score(sequence: str) -> float:
    """
    Calculate the weighted percentage score for a sequence of up to 8 letters.
    Stops calculation at the first '~' (if present) and uses only the prefix.
    """

    if len(sequence) != 8:
        raise ValueError("Sequence must be exactly 8 characters long")

    value_map = {'R': 1.0, '_': 0.5, 'D': 0.0}

    weighted_sum = 0.0
    total_weight = 0.0

    for i, char in enumerate(sequence.upper()):
        if char == '~':
            break

        if char not in value_map:
            raise ValueError(f"Invalid character '{char}' at position {i+1}. Allowed: R, D, _, ~")

        weight = 1.0 / (2 ** i)          # 1, 0.5, 0.25, 0.125, ...
        weighted_sum += value_map[char] * weight
        total_weight += weight

    if total_weight == 0:
        return 0.0  # all ~ or empty effective sequence

    score = (weighted_sum / total_weight) * 100

    return round(score, 2)


#-----------------------------------------------------------------------------
# calculate_field_rep_score()
#
#   Thanks to Greg Field for this Republican score algorithm.
#
#   Calculate score starting at 50 with factor 25, adjusting by +factor/-factor/0
#   for R/D/_ and halving the factor each step. Stops early at '~'.
#
#   Args:
#       sequence: Exactly 8 characters (R, D, _, ~ allowed; case-insensitive)
#
#   Returns:
#       float: Final score rounded to 2 decimal places
#
#   Examples:
#       "RDRR____"  -> 71.88
#       "R~______"  -> 75.00
#       "________"  -> 50.00
#       "RRRRRRRR"  -> 99.80
#       "DDDDDDDD"  -> 0.20
#       "~_______"  -> 50.00
#-----------------------------------------------------------------------------
def calculate_field_rep_score(sequence: str) -> float:
    """
    Calculate score starting at 50 with factor 25, adjusting by +factor/-factor/0
    for R/D/_ and halving the factor each step. Stops early at '~'.
    """

    if len(sequence) != 8:
        raise ValueError("Sequence must be exactly 8 characters long")

    score = 50.0
    factor = 25.0
    seq = sequence.upper()  # make case-insensitive

    for char in seq:
        if char == '~':
            break

        if char == 'R':
            score += factor
        elif char == 'D':
            score -= factor
        elif char == '_':
            pass  # no change
        else:
            raise ValueError(f"Invalid character '{char}'. Allowed: R, D, _, ~")

        factor /= 2  # halve for next position (even after last used letter)

    return round(score, 2)


#-----------------------------------------------------------------------------
# update_voter_record_with_history()
#-----------------------------------------------------------------------------
def update_voter_record_with_history(vuids):
    """Update the voter history with the history string representation of the voter's primary voting history"""
    for vuid_number, vuid_record in vuids.items():
        history_string = create_voting_history_string(vuid_number, vuid_record)
        vuids[vuid_number]['History'] = history_string
        vuids[vuid_number]['RepScore'] = calculate_weighted_sequence_score(history_string)
        vuids[vuid_number]['FieldRepScore'] = calculate_field_rep_score(history_string)


#-----------------------------------------------------------------------------
# write_conv_voters_to_disk()
#-----------------------------------------------------------------------------
def write_conv_voters_to_disk(vuids):
    """Write the voter history records to disk as a CSV file in the current directory"""

    # Write the voter history records to the CSV file (sorted)
    output_pathname = r"voter_history_records.csv"
    try:
        with open(r"voter_history_records.csv", 'w', encoding='utf-8', newline='') as csv_file:
            fieldnames = ['VUID', 'FirstName', 'LastName', 'MiddleName', 'EDRDate', 'History',
                          'RepScore', 'FieldRepScore', 'P2026Party']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for vuid_number, vuid_record in sorted(vuids.items()):
                writer.writerow({
                    'VUID': vuid_number,
                    'FirstName': vuid_record.get('FirstName', ''),
                    'LastName': vuid_record.get('LastName', ''),
                    'MiddleName': vuid_record.get('MiddleName', ''),
                    'EDRDate': vuid_record.get('EDRDate', ''),
                    'History': vuid_record.get('History', ''),
                    'RepScore': vuid_record.get('RepScore', ''),
                    'FieldRepScore': vuid_record.get('FieldRepScore', ''),
                    'P2026Party': vuid_record.get('P2026Party', '')
                })
    except Exception as exc:  # pragma: no cover - unexpected IO error
        print(f"Error writing precinct voters to file {output_pathname}: {exc}")

    print(f"Wrote a total of {len(vuids)} voter history records")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 2:
        print("Usage: python process_p26_voter_history.py <registered_voter_list_file_path>")
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
        print(r"Cannot open database file 'VoterRosterDatabase'")
        return False

    # Check the database version
    if voter_roster_version != VOTER_ROSTER_VERSION:
        print(f"Voter roster database version {voter_roster_version} does not match expected version {VOTER_ROSTER_VERSION}")
        return False

    print(f"Loaded voter roster database version {voter_roster_version} with {len(voter_roster)} voters")

    # Process the registered voter list with history and build the VUID index
    vuids = process_registered_voter_list_with_history(voter_list_pathname_1)

    # Update the voter history with the 2026 primary data from the VUID index
    update_voter_history_with_p26_data(voter_roster, vuids)

    # Update the voter history with the history string representation of the voter's primary voting history
    # and a representative score based on the voter's history of voting in Republican primaries
    update_voter_record_with_history(vuids)

    # Write the voter history records to disk as a CSV file in the current directory
    write_conv_voters_to_disk(vuids)

    return True


main()
