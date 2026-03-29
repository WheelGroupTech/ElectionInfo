#-----------------------------------------------------------------------------
# analyze_travis_precinct_changes.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to analyze changes in voter precincts in Travis County, Texas by comparing
# registered voter lists from different time periods.
#-----------------------------------------------------------------------------
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-statements
#-----------------------------------------------------------------------------
"""analyze_travis_precinct_changes.py"""

# PSEUDOCODE / PLAN (detailed)
# 1. Read CSV records robustly trying several encodings.
# 2. For each successful CSV parse, build:
#    - sd_to_precincts: mapping state senate district -> set(precinct)
#    - precinct_to_addresses: mapping precinct -> dict(address -> voter_count)
#    - precinct_to_sd: mapping precinct -> state senate district (ensure one-to-one)
# 3. Field detection:
#    - Detect VUID field name ('VUID' or 'VUIDNO', case-insensitive)
#    - Detect 'RESIDENTIAL ADDRESS', 'PRECINCT', 'STATE SENATE' header names (case-insensitive)
# 4. For each record:
#    - Extract vuid, address, precinct, state_senate (use detected header names)
#    - Skip records missing VUID
#    - Skip records missing precinct (warn)
#    - If state_senate:
#        - Ensure precinct_to_sd consistency
#        - Add precinct to sd_to_precincts[state_senate]
#    - If address:
#        - Use precinct_to_addresses[precinct] (dict) and increment count for address
# 5. Return the two mappings for reporting.
# 6. Main prints:
#    - For each SD: number of precincts
#    - For each precinct: number of unique addresses (len of dict)
# Note: The address tracking now includes counts of registered voters per address.

import csv
import sys


#-----------------------------------------------------------------------------
# process_registered_voter_list()
#-----------------------------------------------------------------------------
def process_registered_voter_list(pathname):
    """Process the specified voter registration list with robust encoding handling"""

    print(f"Reading data from '{pathname}'...")

    # Map state senate district -> set of precinct identifiers
    sd_to_precincts = {}

    # Map precinct identifier -> map(residential address -> registered voter count)
    precinct_to_addresses = {}

    # Map precinct identifier -> associated state senate district (to ensure one-to-one association)
    precinct_to_sd = {}

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
            return sd_to_precincts, precinct_to_addresses

        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Error reading file {pathname} with encoding {enc}: {exc}")
            registered_voters.clear()
            return sd_to_precincts, precinct_to_addresses

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
            return sd_to_precincts, precinct_to_addresses

        except Exception as exc:  # pragma: no cover - unexpected IO error
            print(f"Failed to read file {pathname} even with replacement errors: {exc}")
            registered_voters.clear()
            return sd_to_precincts, precinct_to_addresses

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

        # Get the field values for the residental address
        if 'RESIDENTIAL ADDRESS' in key_map:
            residential_address_field = key_map['RESIDENTIAL ADDRESS']

        # Get the field values for the precinct
        if 'PRECINCT' in key_map:
            precinct_field = key_map['PRECINCT']

        # Get the field values for the state senate district
        if 'STATE SENATE' in key_map:
            state_senate_field = key_map['STATE SENATE']

    if vuid_field is None:
        print(f"No 'VUID' or 'VUIDNO' header found in '{pathname}'.")
        return sd_to_precincts, precinct_to_addresses

    for record in registered_voters:

        # Use the detected header name to extract the voter record information safely
        # pylint: disable=used-before-assignment
        vuid_number = str(record.get(vuid_field, '')).strip()
        address = str(record.get(residential_address_field, '')).strip() if 'residential_address_field' in locals() else ''
        precinct = str(record.get(precinct_field, '')).strip() if 'precinct_field' in locals() else ''
        state_senate = str(record.get(state_senate_field, '')).strip() if 'state_senate_field' in locals() else ''

        # Sometimes the precinct number may start with a leading 'P Z' or 'P ' so we need to remove it
        if precinct.startswith('P Z'):
            precinct = precinct[3:]
        elif precinct.startswith('P '):
            precinct = precinct[2:]

        if not vuid_number:
            # Skip records with missing/empty VUID values but count them
            continue

        if not precinct:
            print(f"Warning: record with VUID '{vuid_number}' is missing precinct information.")
            continue

        # Only consider non-empty precincts and state senate associations for mapping precinct -> SD
        if state_senate:
            # Ensure precinct maps to a single state senate district
            existing_sd = precinct_to_sd.get(precinct)
            if existing_sd is None:
                precinct_to_sd[precinct] = state_senate
            elif existing_sd != state_senate:
                # Inconsistent mapping detected; warn and keep the first seen association
                print(f"Warning: precinct '{precinct}' seen in state senate '{state_senate}' but previously associated with '{existing_sd}'. Keeping '{existing_sd}'.")

            # Add precinct to sd_to_precincts
            sd_set = sd_to_precincts.get(precinct_to_sd[precinct])
            if sd_set is None:
                sd_set = set()
                sd_to_precincts[precinct_to_sd[precinct]] = sd_set
            sd_set.add(precinct)
        else:
            print(f"Warning: record with VUID '{vuid_number}' is missing state senate information. Skipping SD association.")

        # Track addresses associated with precincts (addresses are only associated with a single precinct)
        # Now track count of registered voters per address
        if address:
            addr_map = precinct_to_addresses.get(precinct)
            if addr_map is None:
                addr_map = {}
                precinct_to_addresses[precinct] = addr_map
            # Increment the count for this address
            addr_map[address] = addr_map.get(address, 0) + 1
        else:
            print(f"Warning: record with VUID '{vuid_number}' is missing address information. Skipping address association.")

    return sd_to_precincts, precinct_to_addresses


#-----------------------------------------------------------------------------
# find_senate_district_for_precinct()
#-----------------------------------------------------------------------------
def find_senate_district_for_precinct(sd_to_precincts, precinct):
    """Return the senate district (key) that contains the given precinct, or None if not found."""
    for sd, precincts in sd_to_precincts.items():
        if precinct in precincts:
            return sd
    return None


#-----------------------------------------------------------------------------
# determine_precinct_changes()
#
# This function is used to determine the changes that occurred when the
# county updated the precinct map.  Many precincts remained unchanged, but
# there were precincts that were joined together into one precinct or
# split into separate precincts.
#
# This function will use all of the addresses from the old precinct map and
# identify the new precinct that address is now in.  This will allow us to determine
# how many addresses were moved from each old precinct to each new precinct, and thus determine
# which precincts were joined together or split apart.
#
# The function will use the number of voters registered at each address to weight the changes,
# so we can determine how many voters were moved from each old precinct to each new precinct.
#-----------------------------------------------------------------------------
def determine_precinct_changes(old_precinct_to_addresses, new_precinct_to_addresses):
    """Determine and report how voters moved from old precincts to new precincts.

    old_precinct_to_addresses and new_precinct_to_addresses are mappings:
      precinct -> { address_string -> voter_count }

    The function prints a human-readable summary and returns a tuple:
      (changes, old_totals, new_totals)
    where `changes` is a dict mapping old_precinct -> { new_precinct_or_None -> voter_count }.
    """
    # Helper: normalize addresses for matching between files
    def normalize_address(addr):
        if not addr:
            return ''
        # Collapse whitespace and uppercase for robust matching
        # Replace common separators with space, strip, then collapse multiple spaces
        s = str(addr).strip()
        # Normalize whitespace
        parts = s.split()
        if not parts:
            return ''
        return ' '.join(parts).upper()

    # Build mapping from normalized address -> new_precinct
    new_address_to_precinct = {}
    for new_precinct, addr_map in (new_precinct_to_addresses or {}).items():
        if not addr_map:
            continue
        for addr in addr_map.keys():
            norm = normalize_address(addr)
            if not norm:
                continue
            existing = new_address_to_precinct.get(norm)
            if existing is None:
                new_address_to_precinct[norm] = new_precinct
            elif existing != new_precinct:
                # Address appears in multiple new precincts; warn and keep first seen
                print(f"Warning: address '{addr}' (normalized '{norm}') appears in new precincts '{existing}' and '{new_precinct}'. Keeping '{existing}'.")

    changes = {}           # old_precinct -> { new_precinct_or_None -> voter_count }
    old_totals = {}        # old_precinct -> total voters
    new_totals = {}        # new_precinct_or_None -> total voters (None = unmapped)

    # Iterate old precincts and map addresses to new precincts
    for old_precinct, addr_map in (old_precinct_to_addresses or {}).items():
        if not addr_map:
            old_totals[old_precinct] = 0
            changes[old_precinct] = {}
            continue

        old_total = 0
        dest_counts = {}

        for addr, count in addr_map.items():
            try:
                voter_count = int(count)
            except Exception:
                # If count is not an integer, attempt float then round, otherwise treat as 1
                try:
                    voter_count = int(float(count))
                except Exception:
                    voter_count = 1

            old_total += voter_count

            norm = normalize_address(addr)
            dest_precinct = new_address_to_precinct.get(norm)

            # Use None key for unmapped addresses
            dest_counts[dest_precinct] = dest_counts.get(dest_precinct, 0) + voter_count
            new_totals[dest_precinct] = new_totals.get(dest_precinct, 0) + voter_count

        changes[old_precinct] = dest_counts
        old_totals[old_precinct] = old_total

    # Print detailed report
    print("\nPrecinct change summary (based on address-level voter counts):\n")

    grand_old_total = sum(old_totals.values())
    grand_new_total = sum(v for k, v in new_totals.items() if k is not None)
    grand_unmapped = new_totals.get(None, 0)

    for old_precinct in sorted(old_totals.keys(), key=lambda x: (str(x))):
        old_total = old_totals[old_precinct]
        print(f"Old precinct '{old_precinct}': {old_total} registered voters")

        dest_counts = changes.get(old_precinct, {})
        if not dest_counts:
            print("  No address data for this precinct.")
            continue

        # Prepare sorted list of destinations by voter count descending
        sorted_dests = sorted(dest_counts.items(), key=lambda x: x[1], reverse=True)

        for dest_precinct, voters in sorted_dests:
            pct = (voters / old_total * 100) if old_total > 0 else 0.0
            dest_label = dest_precinct if dest_precinct is not None else "UNMAPPED"
            print(f"  -> {dest_label}: {voters} voters ({pct:.1f}%)")

        # Interpret topology: majority stayed or split/merged
        stayed = dest_counts.get(old_precinct, 0)
        if stayed == old_total:
            print("  ==> All voters remained in the same precinct (unchanged).")
        else:
            stayed_pct = (stayed / old_total * 100) if old_total > 0 else 0.0
            if stayed_pct >= 50.0:
                print(f"  ==> Majority ({stayed_pct:.1f}%) remained in the same precinct; others moved.")
            else:
                # If the old precinct distributed across multiple new precincts, it's likely split
                dest_nonnull = [d for d in sorted_dests if d[0] is not None]
                if len(dest_nonnull) == 1:
                    # Most moved to a single new precinct => likely merged into that precinct
                    print("  ==> Majority moved into a single new precinct (likely merged).")
                else:
                    print("  ==> Voters distributed across multiple new precincts (likely split).")

        print("")  # blank line between precinct summaries

    # Print new precinct totals summary
    print("New precinct totals (based on mapped old addresses):")
    for new_precinct in sorted([k for k in new_totals.keys() if k is not None], key=lambda x: (str(x))):
        print(f"  {new_precinct}: {new_totals.get(new_precinct,0)} mapped voters")
    print(f"  UNMAPPED (addresses from old list not found in new list): {grand_unmapped} voters\n")

    print(f"Grand totals: old list={grand_old_total}, new-list-mapped={grand_new_total}, unmapped={grand_unmapped}")

    return changes, old_totals, new_totals


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 2:
        print("Usage: python analyze_travis_precinct_changes.py <registered_voter_list_file_path>")
        return False

    voter_list_pathname_1 = sys.argv[1]
    sd_to_precincts_1, precinct_to_addresses_1 = process_registered_voter_list(voter_list_pathname_1)

    print(f"\nState Senate Districts and precinct counts in '{voter_list_pathname_1}':")
    for sd, precincts in sorted(sd_to_precincts_1.items(), key=lambda x: x[0]):
        print(f"  {sd}: {len(precincts)} precinct(s)")

    print(f"\nPrecincts and address counts in '{voter_list_pathname_1}':")
    total_addresses = 0
    total_voters = 0
    for precinct, addresses in sorted(precinct_to_addresses_1.items(), key=lambda x: x[0]):
        # 'addresses' is now a dict mapping address -> voter_count; len(addresses) is unique address count
        unique_addresses = len(addresses)
        address_voters = sum(addresses.values()) if addresses else 0
        total_addresses += unique_addresses
        total_voters += address_voters
        sd = find_senate_district_for_precinct(sd_to_precincts_1, precinct)
        print(f"  {precinct}: {unique_addresses} addresses, {address_voters} registered voters, {sd}")
    print(f"\nTotal unique addresses: {total_addresses}")
    print(f"Total registered voters: {total_voters}")

    # Read in optional second registered voter list
    if len(sys.argv) == 3:
        voter_list_pathname_2 = sys.argv[2]
        sd_to_precincts_2, precinct_to_addresses_2 = process_registered_voter_list(voter_list_pathname_2)

        print(f"\nState Senate Districts and precinct counts in '{voter_list_pathname_2}':")
        for sd, precincts in sorted(sd_to_precincts_2.items(), key=lambda x: x[0]):
            print(f"  {sd}: {len(precincts)} precinct(s)")

        print(f"\nPrecincts and address counts in '{voter_list_pathname_2}':")
        total_addresses = 0
        total_voters = 0
        for precinct, addresses in sorted(precinct_to_addresses_2.items(), key=lambda x: x[0]):
            unique_addresses = len(addresses)
            address_voters = sum(addresses.values()) if addresses else 0
            total_addresses += unique_addresses
            total_voters += address_voters
            sd = find_senate_district_for_precinct(sd_to_precincts_1, precinct)
            print(f"  {precinct}: {unique_addresses} addresses, {address_voters} registered voters, {sd}")
        print(f"\nTotal unique addresses: {total_addresses}")
        print(f"Total registered voters: {total_voters}")

        # Determine the changes between the two precinct maps
        determine_precinct_changes(precinct_to_addresses_1, precinct_to_addresses_2)

    return True


main()
