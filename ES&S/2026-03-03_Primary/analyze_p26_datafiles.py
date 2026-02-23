#-----------------------------------------------------------------------------
# analyze_p26_datafiles.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to analyze P26 data files, which are CSV files with ballot
# details for each ballot style.
#-----------------------------------------------------------------------------
"""analyze_p26_datafiles.py"""
# pylint: disable=line-too-long,unused-variable,too-many-branches
# pylint: disable=too-many-locals, broad-exception-caught, consider-using-dict-items
import csv
import sys
import shelve


# PSEUDOCODE / DETAILED PLAN
# 1. For each ballot processed we must determine which contests are present on
#    that ballot style, and which of those contests received a selection on
#    the particular ballot.
# 2. Build a set `contests_in_style` from the ballot style mapping (all contests
#    that could appear on that ballot style).
# 3. While iterating through the ballot's `Barcodes`, record each contest that
#    had a selection in `selected_contests`.
# 4. After processing the barcode selections for the ballot, any contest in
#    `contests_in_style` that is not in `selected_contests` is considered to
#    have "no selection" for this ballot; increment the contest's 'Undervote'
#    count by 1 (per the user's instruction).
# 5. Preserve existing behavior: increment selection counts for found barcodes,
#    print warnings for missing barcode values, and skip ballots when their
#    ballot style is missing.
# 6. Ensure the global `VOTE_COUNTS` is updated accordingly for each ballot.
# 7. Track which ballot styles were actually used while processing ballots in
#    `used_ballot_styles`. After processing all ballots, print any ballot style
#    names present in `BALLOT_STYLES` that are not in `used_ballot_styles`.
# 8. When identifying unused ballot styles, iterate over the ballot styles
#    dictionary directly instead of calling `.keys()`; dictionary iteration
#    yields keys by default, satisfying the requirement to avoid `.keys()`.
# End pseudocode


# Specify the database version for the ES&S ballot data
ESS_BALLOT_DATA_VERSION = 2

# Master list of contests for the election
CONTESTS = {}

# Master list of ballot styles for the election
BALLOT_STYLES = {}

# Vote counts for each contest and selection
VOTE_COUNTS = {}


#-----------------------------------------------------------------------------
# parse_csv_contest_list()
#-----------------------------------------------------------------------------
def parse_csv_contest_list(input_file):
    """Parses the input csv contest list file"""

    print(f"Parsing contest list from {input_file}...")

    # Parse in the contest data from the CSV file
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)

        # Read in the CSV header row to skip over it
        row = next(reader)

        while True:
            try:
                row = next(reader)
            except StopIteration:
                break

            # Get the contest name and selection (i.e. candidate)
            contest_name=row[0]
            selection_name=row[1]

            try:
                # Find the contest in the master list of contests
                contest = CONTESTS[contest_name]

                if not selection_name in contest:
                    contest.append(selection_name)

            except KeyError:

                contest = []

                if not selection_name in contest:
                    contest.append(selection_name)

                # Add the contest to the master list
                CONTESTS[contest_name] = contest

    # Add selections for over vote and undervote for each contest
    for contest in CONTESTS:
        if 'Overvote' not in CONTESTS[contest]:
            CONTESTS[contest].append('Overvote')
        if 'Undervote' not in CONTESTS[contest]:
            CONTESTS[contest].append('Undervote')

    # Setup the vote counts
    for contest, selections in CONTESTS.items():

        # Initialize vote counts for this contest: each selection -> 0
        VOTE_COUNTS[contest] = {}
        for selection in selections:
            VOTE_COUNTS[contest][selection] = 0


#-----------------------------------------------------------------------------
# parse_csv_ballot_details_listing()
#-----------------------------------------------------------------------------
def parse_csv_ballot_details_listing(input_file):
    """Parses the input csv ballot listing details file"""

    print(f"Parsing ballot details listing from {input_file}...")

    # Parse in the data from the CSV file
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)

        # Read in the CSV header row to skip over it
        row = next(reader)

        while True:
            try:
                row = next(reader)
            except StopIteration:
                break

            # Get the contest name and selection (i.e. candidate)
            ballot_style_name=row[0] + row[1]
            contest_name=row[5]
            selection_name=row[7]
            barcode_value=int(row[10])

            # Validate that the contest and selection are in the master list of contests
            try:
                contest = CONTESTS[contest_name]
                if not selection_name in contest:
                    print(f"{ballot_style_name} {contest_name} {selection_name} not in master list")
            except KeyError:
                print(f"{ballot_style_name} {contest_name} not in master list")

            #print(f"{ballot_style_name}")

            # Find the ballot style
            try:
                ballot_style = BALLOT_STYLES[ballot_style_name]
            except KeyError:
                ballot_style = {}
                BALLOT_STYLES[ballot_style_name] = ballot_style

            # Find the barcode value in the ballot style
            try:
                barcode = ballot_style[barcode_value]
                # The barcode value should be unique within the ballot style, so if we find it again, print a warning
                print(f"Warning: duplicate barcode value {barcode_value} found in ballot style {ballot_style_name}")
            except KeyError:
                barcode = {}
                ballot_style[barcode_value] = barcode

            # Add the contest and selection to the barcode value
            barcode['Contest'] = contest_name
            barcode['Selection'] = selection_name


#-----------------------------------------------------------------------------
# analyze_ballots()
#-----------------------------------------------------------------------------
def analyze_ballots(ballots):
    """Analyze the ballots from the ballot database file"""

    # Track which ballot styles were actually used while processing ballots
    used_ballot_styles = set()

    for ballot in ballots:

        ballot_style_name = ballot['BallotStyle']
        master_barcode = ballot['MasterBarcode']
        precinct_id = ballot['PrecinctId']
        ballot_style_id = ballot['BallotStyleId']
        num_write_in_votes = ballot['NumWriteInVotes']
        num_vote_selections = ballot['NumVoteSelections']
        barcode_values = ballot['Barcodes']

        # Check the number of selections
        if num_vote_selections != len(barcode_values):
            print(f"Mismatch on vote selections {num_vote_selections} {len(barcode_values)}")

        # Find the ballot style
        try:
            ballot_style = BALLOT_STYLES[ballot_style_name]
            # Mark this ballot style as used
            used_ballot_styles.add(ballot_style_name)
        except KeyError:
            print(f"Missing ballot style {ballot_style_name}")
            # Cannot determine contests for this ballot without the ballot style; skip processing this ballot
            continue

        # Build the set of contests that exist on this ballot style
        contests_in_style = set()
        for entry in ballot_style.values():
            try:
                contests_in_style.add(entry['Contest'])
            except Exception:
                # malformed entry; ignore
                pass

        # Track which contests received a selection on this ballot
        selected_contests = set()

        # Find the contests and selections for each barcode value
        for barcode_value in barcode_values:
            try:
                barcode = ballot_style[barcode_value]
                contest_name = barcode['Contest']
                selection_name = barcode['Selection']
                # Increment the vote count for this contest and selection
                VOTE_COUNTS[contest_name][selection_name] += 1
                selected_contests.add(contest_name)
            except KeyError:
                print(f"Missing barcode value {barcode_value} in ballot style {ballot_style_name}")

        # For every contest on this ballot style that had NO selection, increment the Undervote count by 1
        for contest_name in contests_in_style:
            if contest_name not in selected_contests:
                # Ensure the contest and Undervote selection exist in VOTE_COUNTS before incrementing
                try:
                    VOTE_COUNTS[contest_name]['Undervote'] += 1
                except KeyError:
                    # If something is missing, print a warning and skip
                    print(f"Warning: cannot increment Undervote for contest {contest_name} (missing in VOTE_COUNTS)")

    # Print the vote counts for each contest and selection
    for contest, selections in VOTE_COUNTS.items():
        print(f"{contest}:")
        total_votes = sum(selections.values())
        for selection, count in selections.items():
            print(f"  {selection}: {count}")
        print(f"  Total votes: {total_votes}")

    # Identify and print ballot styles that were not used while processing ballots
    # Iterate the BALLOT_STYLES dictionary directly (dict iteration yields keys),
    # avoiding the use of .keys()
    unused_styles = [bs for bs in BALLOT_STYLES if bs not in used_ballot_styles]
    if unused_styles:
        print("Unused ballot styles:")
        for bs_name in sorted(unused_styles):
            print(bs_name)
    else:
        print("No unused ballot styles found")

    print(f"Analyzed {len(ballots)} ballots")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Get the arguments
    csv_contest_list = sys.argv[1]
    csv_ballot_details_file = sys.argv[2]

    # Parse the contest list
    parse_csv_contest_list(csv_contest_list)

    # Parse the ballot details listing
    parse_csv_ballot_details_listing(csv_ballot_details_file)

    # Open the database file with the ballot data
    try:
        # Load in the data from the database
        db = shelve.open('ess_p26_ballots.dat')
        ballots = db['Ballots']
        version = db['Version']
        db.close()

        print(r"Loading ballot database from ess_p26_ballots.dat")
        if version != ESS_BALLOT_DATA_VERSION:
            print(f"Ballot data is incorrect version {version}")
        print(r"Successfully loaded ballot database from ess_p26_ballots.dat")

        # Analyze the ballots
        analyze_ballots(ballots)

    except KeyError:
        print(r"Cannot open database file 'ess_p26_ballots.dat'")


main()
