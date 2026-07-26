#-----------------------------------------------------------------------------
# process_registered_voters_with_property_data.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to load and process the registered voter list from Travis County
# and analyze it against a list of property addresses and descriptions.
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements
#-----------------------------------------------------------------------------
"""process_registered_voters_with_property_data.py"""

import csv
import sys


#-----------------------------------------------------------------------------
# process_registered_voter_list()
#-----------------------------------------------------------------------------
def process_registered_voter_list(pathname, property_data_path):
    """Process the specified voter registration list with robust encoding handling"""

    print(f"Reading voter registration data from '{pathname}'...")

    registered_voters = []
    voter_count = 0
    dob_available = False

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

    print(f"Reading property data from '{property_data_path}'...")

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

    if vuid_field is None:
        print(f"No 'VUID' or 'VUIDNO' header found in '{pathname}'.")
        return registered_voters

    # Create a set to track all VUIDs
    vuids = {}
    num_duplicates = 0
    num_skipped = 0

    for record in registered_voters:

        full_name = ''
        last_name = ''
        first_name = ''
        middle_name = ''
        dob = ''

        # Use the detected header name to extract the voter record information safely
        # pylint: disable=used-before-assignment
        vuid_number = str(record.get(vuid_field, '')).strip()
        full_name = str(record.get(fullname_field, '')).strip() if 'fullname_field' in locals() else ''
        last_name = str(record.get(lastname_field, '')).strip() if 'lastname_field' in locals() else ''
        first_name = str(record.get(firstname_field, '')).strip() if 'firstname_field' in locals() else ''
        middle_name = str(record.get(middlename_field, '')).strip() if 'middlename_field' in locals() else ''
        dob = str(record.get(dob_field, '')).strip() if 'dob_field' in locals() else ''

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
            vuid_record['FullName'] = full_name
            vuid_record['LastName'] = last_name
            vuid_record['FirstName'] = first_name
            vuid_record['MiddleName'] = middle_name
            vuid_record['DOB'] = dob

            vuids[vuid_number] = vuid_record

    print(f"Found {num_duplicates} duplicate entries in '{pathname}'")
    if num_skipped:
        print(f"Skipped {num_skipped} records with missing/empty '{vuid_field}' values in '{pathname}'")

    print(f"There are {len(vuids)} voters in '{pathname}'")

    return vuids, dob_available


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 3:
        print("Usage: python process_registered_voters_with_property_data.py <registered_voter_list_file_path> <property_data_file_path>")
        return False

    voter_list_pathname = sys.argv[1]
    property_data_path = sys.argv[2]

    process_registered_voter_list(voter_list_pathname, property_data_path)


    return True


main()
