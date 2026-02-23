#-----------------------------------------------------------------------------
# process_p26_travis_registered_voters.py
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
# PSEUDOCODE / IMPLEMENTATION PLAN:
#
# 1. Load a processed voter roster (from a shelve database) into voter_roster.
#    - Open shelve 'VoterRosterDatabase.dat' and read 'Version' and 'VoterRoster'.
#    - If KeyError occurs, print error and exit.
# 2. Analyze roster vuids for duplicates and aggregate ballot types:
#    - For each voter in voter_roster:
#        - Extract vuid_number, Party, Precinct, FirstName, LastName, BallotType, VoteDate, Notes.
#        - Tally ballot type counts and party splits (BBM/EV/ED and REP/DEM).
#        - Detect duplicate VUIDs in roster_vuids.
#        - This step is also done in the process_p26_travis_voter_rosters.py script.
# 3. Read and parse the registered voter CSV into a registered_voter list.
#    - Open file with various encoding types until successful.
#    - Use csv.DictReader to read each row as a dict.
#    - Append each record to registered_voters and count voters.
#    - Return registered_voters and print count and encoding used.
# 4. Build a dictionary vuids mapping VUID strings to the original voter registration record
#    from the registered voter list CSV.
#    - Iterate the provided registered_voters.
#    - Convert record['VUID'] to string (vuid_number).
#    - If vuid_number exists in vuids, increment duplicate counter.
#    - Otherwise add an entry vuids[vuid_number] = {'VoterRecord': record}.
#    - Return a dictionary of vuids and print summary counts (total voters, duplicates).
# 5. Compare the roster voter against the registered voter list:
#    - For each roster voter:
#        - Extract vuid_number, precinct, first_name, last_name (strip whitespace).
#        - Look up vuid_number in vuids from the registered voter list.
#        - If found:
#            - Extract registered rv_first_name and rv_last_name.
#            - Normalize the last names with .casefold() for case-insensitive comparison.
#            - If either last names differ (case-insensitive), treat as name mismatch and increment counter.
#            - Otherwise treat as name-correct match.
#        - If not found in vuids increment unknown counter and print missing info.
#        - Return a list of unknown voters for optional second pass against a second registered voter list.
# 6. Print summary counts (total roster voters, name-correct, name-changes, unknown).
# 7. Perform the same analysis against a second registered voter list (if provided)
# 8. Analyze the first registered voter list for potential multiple registrations of a voter
#    with the same first name, last name, and date of birth but different VUID.  If multiple
#    registrations are found, determine how many of the voters have voted and report if any have
#    voted more than once using their different VUIDs.
#
# Implementation notes:
# - Use .strip() to remove surrounding whitespace before comparisons.
# - Use .casefold() for robust case-insensitive comparisons.
#   any mismatch in either field counts as a name change.
#-----------------------------------------------------------------------------
"""process_p26_travis_registered_voters.py"""

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

    for record in registered_voters:
        vuid_number = str(record['VUID'])

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

    print(f"Found {num_duplicates} duplicate entries in the registered voter list '{voter_list_pathname}'")

    print(f"There are {len(vuids)} voters in the registered voter list '{voter_list_pathname}'")

    return vuids, dob_available


#-----------------------------------------------------------------------------
# find_multiple_registrations()
#-----------------------------------------------------------------------------
def find_multiple_registrations(vuids, showdups=False):
    """Find multiple voter registrations with different VUID and the same
       date of birth, last name, and first name"""

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

    if showdups:
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
# analyze_suspect_groups_against_voter_roster()
#-----------------------------------------------------------------------------
def analyze_suspect_groups_against_voter_roster(suspect_groups, voter_roster):
    """Lookup each VUID from each suspect group to determine if a voter may
       have voted more than once

    PSEUDOCODE / PLAN (detailed):
    - Validate inputs: if no suspect_groups or no voter_roster, print message and return [].
    - Build a mapping `roster_map` from VUID string -> list of roster voter records.
      This handles cases where the roster itself may contain duplicate VUID entries
      (multiple vote records for the same VUID).
    - For each suspect_group (a list of tuples (vuid, registration_record)):
        - Collect all roster entries that match any VUID in the suspect_group by looking
          up each vuid in `roster_map`.
        - If the total number of matched roster entries for that group is greater than 1:
            - This indicates that more than one VUID from the suspect_group appears in the
              voter roster (possible multiple votes). Print a descriptive header that includes:
              - A group index and the representative name/DOB from the registration records.
              - Number of distinct VUIDs in the group and number of matching roster entries.
            - For each matching roster entry print a concise line with:
              VUID, Precinct, BallotType, VoteDate, Party, FirstName, LastName.
            - Add the group and its matched roster entries to a `flagged` list for return.
    - After processing all groups print a summary of how many suspect groups were flagged.
    - Return the `flagged` list (each element is a tuple (group, matched_roster_entries)).
    """

    if not suspect_groups:
        print("No suspect groups provided to analyze_suspect_groups_against_voter_roster().")
        return []

    if not voter_roster:
        print("No voter roster provided to analyze_suspect_groups_against_voter_roster().")
        return []

    # Build mapping from VUID -> list of roster entries (to handle multiple rows per VUID)
    roster_map = {}
    for voter in voter_roster:
        try:
            vuid = str(voter.get('VUID', '')).strip()
        except Exception:
            vuid = ''
        if not vuid:
            continue
        roster_map.setdefault(vuid, []).append(voter)

    flagged_groups = []
    total_groups = len(suspect_groups)
    print(f"Analyzing {total_groups} suspect groups against voter roster for possible multiple votes...")

    # Helper to produce a sort key for deterministic output (use last, first, dob from group's first record)
    def _group_sort_key(group):
        first_rec = group[0][1] if group else {}
        return ((first_rec.get('LastName') or '').upper(),
                (first_rec.get('FirstName') or '').upper(),
                first_rec.get('DOB') or '')

    voter_count = 0
    for idx, group in enumerate(sorted(suspect_groups, key=_group_sort_key), start=1):

        # group is list of (vuid, registration_record)
        # Collect all roster matches for any VUID in this group
        matches = []
        distinct_vuids_in_group = set()
        for vuid, reg_rec in group:
            v = (vuid or '').strip()
            if not v:
                continue
            distinct_vuids_in_group.add(v)
            if v in roster_map:
                for roster_rec in roster_map[v]:
                    matches.append((v, roster_rec))

        # If more than one roster entry matched any VUIDs in this group, flag for potential multiple votes
        if len(matches) > 1:

            # Use representative registration record for header display
            rep_rec = group[0][1] if group else {}
            display_last = (rep_rec.get('LastName') or '').strip()
            display_first = (rep_rec.get('FirstName') or '').strip()
            display_dob = (rep_rec.get('DOB') or '').strip()

            print("=" * 80)
            print(f"Potential multiple votes detected for suspect group {idx}: "
                  f"Name='{display_first} {display_last}', DOB='{display_dob}'")
            print(f"  Distinct VUIDs in group: {len(distinct_vuids_in_group)}; "
                  f"Roster matches: {len(matches)}")
            print("  Matching roster entries (VUID, Precinct, BallotType, VoteDate, Party, First Last):")

            # Sort matches for deterministic output: by VUID then VoteDate then Precinct
            def _match_sort_key(item):
                vuid_val, roster_rec = item
                return (vuid_val or '', roster_rec.get('VoteDate') or '', roster_rec.get('Precinct') or '')

            for vuid_val, roster_rec in sorted(matches, key=_match_sort_key):
                precinct = roster_rec.get('Precinct', '') or ''
                ballot_type = roster_rec.get('BallotType', '') or ''
                vote_date = roster_rec.get('VoteDate', '') or ''
                party = roster_rec.get('Party', '') or ''
                r_first = (roster_rec.get('FirstName') or '').strip()
                r_last = (roster_rec.get('LastName') or '').strip()
                print(f"    {vuid_val}, {precinct}, {ballot_type}, {vote_date}, {party}, {r_first} {r_last}")

            flagged_groups.append((group, matches))

        elif len(matches) == 1:
            voter_count = voter_count + 1

    if not flagged_groups:
        print("No potential multiple votes found when comparing suspect groups to the voter roster.")
    else:
        print(f"Flagged {len(flagged_groups)} suspect group(s) with potential multiple votes.")
    print(f"{voter_count} voters with multiple VUIDs voted once")

    return flagged_groups


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
    num_unknown_voter_rep = 0
    num_unknown_voter_dem = 0
    num_name_changes = 0
    num_name_correct = 0
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

        try:
            vuid_record = registered_vuids[vuid_number]

            # We found the voter record in the registered voter list
            record = vuid_record['VoterRecord']

            # Handle both types of name formats in the registered voter list: either separate
            # FIRST_NAME and LAST_NAME fields or a single NAME field with "LAST_NAME, FIRST_NAME MIDDLE_NAME"
            try:
                rv_last_name = record['LAST_NAME'].strip()
                rv_first_name = record['FIRST_NAME'].strip()
                rv_middle_name = record['MIDDLE_NAME'].strip()
            except KeyError:
                name = record['NAME'].strip()
                name_parts = [item.strip() for item in name.split(",")]
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

        except KeyError:

            # We did not find the voter record in the registered voter list
            num_unknown_voter = num_unknown_voter + 1
            if party == 'REP':
                num_unknown_voter_rep += 1
            else:
                num_unknown_voter_dem += 1
            unknown_voter_roster.append(voter)
            if show_voters:
                print(f"Did not find VUID,{vuid_number},{precinct},{ballot_type},{vote_date},{first_name},{last_name},{party}")

    print(f"Analyzed {num_voters} voters from the voter roster against registered voter list '{voter_list_pathname}'")
    print(f"Found {num_name_correct} voters with no name changes")
    print(f"Found {num_name_changes} voter records with name changes")
    print(f"Found {num_unknown_voter} private/unknown voter records")
    print(f"Found {num_unknown_voter_rep} private/unknown REP voter records")
    print(f"Found {num_unknown_voter_dem} private/unknown DEM voter records")

    return unknown_voter_roster


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 2:
        print("Usage: python process_p26_travis_registered_voters.py <registered_voter_list_file_path>")
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

    # Analyze records for duplicates where the DOB and name are identical
    if dob_available is True:
        suspect_groups = find_multiple_registrations(registered_vuids, showdups=False)
        analyze_suspect_groups_against_voter_roster(suspect_groups, voter_roster)


    return True


main()
