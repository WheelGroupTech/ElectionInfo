#-----------------------------------------------------------------------------
# process_registered_voters.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to load and process the registered voter list from Travis County
# and analyze it.
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements
#-----------------------------------------------------------------------------
"""process_registered_voters.py"""

import csv
import sys


#-----------------------------------------------------------------------------
# process_registered_voter_list()
#-----------------------------------------------------------------------------
def process_registered_voter_list(pathname):
    """Process the specified voter registration list with robust encoding handling"""

    print(f"Reading data from '{pathname}'...")

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
# compare_vuid_sets()
#-----------------------------------------------------------------------------
def compare_vuid_sets(vuid_1, vuid_2):
    """Compare the VUID sets and enumerate the differences

    Pseudocode / Plan:
    - Inputs: vuid_1 (dict), vuid_2 (dict). Each maps vuid->record-dict with keys like
      'FullName', 'LastName', 'FirstName', 'MiddleName', 'DOB', 'VoterRecord'.
    - Initialize counters:
        same_vuids = 0
        unique_vuids_1 = 0
        unique_vuids_2 = 0
    - If either input is falsy (None or empty), treat as empty dict.
    - Build sets of keys:
        set1 = set(vuid_1.keys())
        set2 = set(vuid_2.keys())
    - Intersection gives same_vuids; increment same_vuids accordingly.
    - For keys only in set1 (set1 - set2):
        - For each vuid in sorted order:
            - Retrieve record from vuid_1
            - Extract name information (prefer 'FullName' if present, else compose from Last/First/Middle)
            - Print line with vuid and name info
            - Increment unique_vuids_1
    - For keys only in set2 (set2 - set1):
        - Same as above but for vuid_2 and increment unique_vuids_2
    - Print summary counts:
        - Number in both sets
        - Number unique to first set
        - Number unique to second set
    - Return a summary dict or tuple (same_vuids, unique_vuids_1, unique_vuids_2)
    """

    if not vuid_1:
        vuid_1 = {}
    if not vuid_2:
        vuid_2 = {}

    set1 = set(vuid_1.keys())
    set2 = set(vuid_2.keys())

    same = set1.intersection(set2)
    only1 = set1.difference(set2)
    only2 = set2.difference(set1)

    same_vuids = len(same)
    unique_vuids_1 = len(only1)
    unique_vuids_2 = len(only2)

    # Helper to format name info from a vuid record
    def format_name_info(vrecord):
        if not vrecord:
            return ''
        full = vrecord.get('FullName', '') or ''
        if full:
            return full
        last = vrecord.get('LastName', '') or ''
        first = vrecord.get('FirstName', '') or ''
        middle = vrecord.get('MiddleName', '') or ''
        parts = []
        if last:
            parts.append(last)
        name_rest = ' '.join(p for p in (first, middle) if p)
        if name_rest:
            parts.append(name_rest)
        if parts:
            return ', '.join(parts)
        return ''

    # Print unique entries in set1
    if unique_vuids_1:
        print(f"VUIDs only in first set ({unique_vuids_1}):")
        for v in sorted(only1):
            name_info = format_name_info(vuid_1.get(v))
            dob = (vuid_1.get(v) or {}).get('DOB', '') or ''
            if dob:
                print(f"  {v} - {name_info} - DOB: {dob}")
            else:
                print(f"  {v} - {name_info}")

    # Print unique entries in set2
    if unique_vuids_2:
        print(f"VUIDs only in second set ({unique_vuids_2}):")
        for v in sorted(only2):
            name_info = format_name_info(vuid_2.get(v))
            dob = (vuid_2.get(v) or {}).get('DOB', '') or ''
            if dob:
                print(f"  {v} - {name_info} - DOB: {dob}")
            else:
                print(f"  {v} - {name_info}")

    print(f"VUIDs in both sets: {same_vuids}")
    print(f"Unique VUIDs in first set: {unique_vuids_1}")
    print(f"Unique VUIDs in second set: {unique_vuids_2}")

    return {'same_vuids': same_vuids,
            'unique_vuids_1': unique_vuids_1,
            'unique_vuids_2': unique_vuids_2}


#-----------------------------------------------------------------------------
# find_multiple_registrations()
#-----------------------------------------------------------------------------
def find_multiple_registrations(vuids):
    """Find multiple voter registrations with different VUID and the same
       date of birth, last name, and first name

    Pseudocode / Plan:
    - Input: 'vuids' dict mapping vuid -> record dict with keys:
        'FullName', 'LastName', 'FirstName', 'MiddleName', 'DOB', 'VoterRecord'
    - Normalize last name, first name, and DOB for grouping:
        - Trim whitespace from last name, first name, and DOB.
        - Use uppercase last and first name for case-insensitive matching.
        - Treat empty last name, first name, or empty DOB as ineligible for grouping.
    - Build a grouping dictionary 'groups' keyed by (LAST_UPPER, FIRST_UPPER, DOB) mapping to
      a list of tuples (vuid, record).
    - Iterate over all items in 'vuids':
        - Extract and normalize last name, first name and dob.
        - Skip records missing last, first, or dob.
        - Append (vuid, record) to groups[(last_upper, first_upper, dob)].
    - After grouping, iterate groups and select only those with length > 1 (possible multiples).
    - Print a human-readable report:
        - Total number of groups found and total records involved.
        - For each group print:
            - Group header with LastName, FirstName and DOB and group size.
            - List each VUID and a best-effort name string (FullName if present else "Last, First Middle").
    - Return a list of groups (each group is a list of (vuid, record)) for programmatic use.
    """

    if not vuids:
        print("No VUIDs provided to find_multiple_registrations().")
        return []

    # Build mapping: (LASTNAME_UPPER, FIRSTNAME_UPPER, DOB) -> list of (vuid, record)
    grouping = {}
    for vuid, record in vuids.items():
        if not record or not isinstance(record, dict):
            continue
        last = (record.get('LastName', '') or '').strip()
        first = (record.get('FirstName', '') or '').strip()
        dob = (record.get('DOB', '') or '').strip()
        if not last or not first or not dob:
            # We require last name, first name, and DOB to consider potential duplicate registrations
            continue
        key = (last.upper(), first.upper(), dob)
        grouping.setdefault(key, []).append((vuid, record))

    # Filter groups with more than one distinct VUID
    suspect_groups = [grp for grp in grouping.values() if len(grp) > 1]

    if not suspect_groups:
        print("No multiple registrations found (same last name, first name, and DOB).")
        return []

    total_records = sum(len(g) for g in suspect_groups)
    print(f"Found {len(suspect_groups)} groups of possible multiple registrations ({total_records} total records)")

    def format_name(rec):
        if not rec:
            return ''
        full = (rec.get('FullName') or '').strip()
        if full:
            return full
        last = (rec.get('LastName') or '').strip()
        first = (rec.get('FirstName') or '').strip()
        middle = (rec.get('MiddleName') or '').strip()
        name_parts = []
        if last:
            name_parts.append(last)
        rest = ' '.join(p for p in (first, middle) if p)
        if rest:
            name_parts.append(rest)
        return ', '.join(name_parts) if name_parts else ''

    # Sort groups for deterministic output: by last name then first name then DOB
    def group_sort_key(group):
        # group is list of (vuid, record)
        first_rec = group[0][1] if group else {}
        return ((first_rec.get('LastName') or '').upper(),
                (first_rec.get('FirstName') or '').upper(),
                first_rec.get('DOB') or '')

    for idx, group in enumerate(sorted(suspect_groups, key=group_sort_key), start=1):
        # Use data from the first record to display group header fields
        first_rec = group[0][1] if group else {}
        display_last = first_rec.get('LastName', '') or ''
        display_first = first_rec.get('FirstName', '') or ''
        display_dob = first_rec.get('DOB', '') or ''
        print(f"Group {idx}: LastName='{display_last}', FirstName='{display_first}', DOB='{display_dob}' - {len(group)} records")
        # Sort entries by VUID for deterministic listing
        for vuid, rec in sorted(group, key=lambda x: x[0]):
            print(f"  {vuid} - {format_name(rec)}")

    return suspect_groups


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 2:
        print("Usage: python process_registered_voters.py <registered_voter_list_file_path>")
        return False

    voter_list_pathname_1 = sys.argv[1]
    vuids_1, dob_available = process_registered_voter_list(voter_list_pathname_1)

    # Analyze records for duplicates where the DOB and name are identical
    if dob_available is True:
        find_multiple_registrations(vuids_1)

    # Read in optional second registered voter list
    if len(sys.argv) == 3:
        voter_list_pathname_2 = sys.argv[2]
        vuids_2 = process_registered_voter_list(voter_list_pathname_2)

        # Compare the two VUID sets and enumerate the differences
        compare_vuid_sets(vuids_1, vuids_2)


    return True


main()
