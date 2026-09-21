#-----------------------------------------------------------------------------
# merge_sup_into_pr26_travis_registered_voters.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to merge a supplement registered voter list into the main
# registered voter list for Travis County, Texas for the 2026 primary election
# runoff.
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements
#-----------------------------------------------------------------------------
"""merge_sup_into_pr26_travis_registered_voters.py"""

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

    # Try encodings in this order to handle files that aren't valid UTF-8
    # Prefer 'utf-8-sig' to remove BOM from headers when present.
    candidate_encodings = ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1']
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
# merge_supplement_voters()
#
# This function will merge the supplement registered voters into the main
# registered voter list.  There may be some duplicates between the two lists,
# so we will use the VUID as the unique identifier to avoid adding duplicate voters.
# If a voter in the supplement list has the same VUID as a voter in the main list,
# we will replace the existing voter with the one from the supplement list.  We will
# track the number of voters added, updated, and skipped due to duplicates for
# reporting purposes.
#
# We will assume that the registered voter list and the supplement registered voter
# list have the exact same field names and structure, so we can directly merge the
# records based on the VUID.
#-----------------------------------------------------------------------------
def merge_supplement_voters(registered_voters, sup_registered_voters, voter_list_pathname):
    """Merge the supplement registered voters into the registered voter list"""

    vuids = {}
    num_duplicates = 0
    num_replacements = 0
    num_added = 0

    for record in registered_voters:
        vuid_number = str(record['VUID'])

        try:
            # Find the vuid number in the list of all VUIDs
            vuid_record = vuids[vuid_number]

            # We found it and it is a duplicate, so we will skip it and report the duplicate
            #print(f"Found duplicate VUID in the list {vuid_number}")
            #print(f"Original:  {vuid_record['VoterRecord']}")
            #print(f"Duplicate: {record}")
            num_duplicates = num_duplicates + 1

        except KeyError:

            # The vuid number was not found in the list (as expected), so we add it
            vuid_record = {'VoterRecord': record}
            vuids[vuid_number] = vuid_record

    print(f"Found {num_duplicates} duplicate entries in the registered voter list '{voter_list_pathname}'")

    print(f"There are {len(vuids)} voters in the registered voter list '{voter_list_pathname}'")

    for record in sup_registered_voters:
        vuid_number = str(record['VUID'])
        try:
            # Find the vuid number in the list of all VUIDs
            vuid_record = vuids[vuid_number]

            # We found it, so we will replace the existing record with the one from the supplement list
            vuids[vuid_number] = {'VoterRecord': record}
            num_replacements = num_replacements + 1

        except KeyError:

            # The vuid number was not found in the list, so we add it
            vuids[vuid_number] = {'VoterRecord': record}
            num_added = num_added + 1

    print(f"Added {num_added} new voters from the supplement list to the registered voter list '{voter_list_pathname}'")

    print(f"Replaced {num_replacements} existing voters in the registered voter list '{voter_list_pathname}' with records from the supplement list")

    # Now we will write the merged list back to the new registered voter
    # list file with the new records from the supplement list.  We will write
    # it in the same format as the original file, so we will use the fieldnames
    # from the first record in the registered voter list where possible.  We will
    # normalize header keys by stripping any leading BOM (U+FEFF) and ensure that
    # all record keys are normalized before writing.
    # The output file will be named the same as the original file with "_merged" appended
    # to the filename before the extension.

    # First we will determine the output filename based on the input filename
    if voter_list_pathname.endswith('.csv'):
        output_pathname = voter_list_pathname[:-4] + '_merged.csv'
    else:
        output_pathname = voter_list_pathname + '_merged'

    # Now we will write the merged list to the output file
    try:
        if not vuids:
            print(f"No voters to write for '{voter_list_pathname}'")
            return

        with open(output_pathname, 'w', encoding='utf-8', errors='replace', newline='') as csv_file:
            # Build normalized, ordered fieldnames:
            # Use the first record's key order (normalized), then append any other keys encountered.
            first_key = next(iter(vuids))
            first_record = vuids[first_key]['VoterRecord']
            fieldnames = []
            for k in first_record.keys():
                nk = k.lstrip('\ufeff')
                if nk not in fieldnames:
                    fieldnames.append(nk)

            for vr in vuids.values():
                for k in vr['VoterRecord'].keys():
                    nk = k.lstrip('\ufeff')
                    if nk not in fieldnames:
                        fieldnames.append(nk)

            writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()

            for vuid_number, vuid_record in vuids.items():
                # Normalize the record's keys to match the normalized fieldnames
                record_out = {}
                for k, v in vuid_record['VoterRecord'].items():
                    nk = k.lstrip('\ufeff')
                    record_out[nk] = v
                writer.writerow(record_out)

        print(f"Wrote merged registered voter list to '{output_pathname}'")
    except Exception as exc:  # pragma: no cover - unexpected IO error
        print(f"Error writing merged registered voter list to '{output_pathname}': {exc}")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 3:
        print("Usage: python merge_sup_into_pr26_travis_registered_voters.py <registered_voter_list_file_path> <sup_list_file_path>")
        return False
    voter_list_pathname = sys.argv[1]
    sup_list_pathname = sys.argv[2]

    # Read in the registered voter list
    registered_voters = process_registered_voter_list(voter_list_pathname)

    # Read in the supplement registered voter list (if provided)
    sup_registered_voters = process_registered_voter_list(sup_list_pathname)

    # Merge the supplement registered voters into the provided registered voter list
    merge_supplement_voters(registered_voters, sup_registered_voters, voter_list_pathname)

    return True


main()
