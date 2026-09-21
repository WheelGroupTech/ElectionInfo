#-----------------------------------------------------------------------------
# analyze_dallas_missing_tapes.py
#
# Copyright (c) 2024 Daniel M. Teal
#
# License: MIT License
#
# Python script to analyze cast vote record data captured with
# "process_cvr_files.py".  It must be run in the same directory the
# process script ran and includes the python shelve database files:
#
#    dbfile.dat, dbfile.dir, dbfile.bak
#-----------------------------------------------------------------------------
"""analyze_dallas_missing_tapes.py""" # for pylint
# pylint: disable=line-too-long,unused-variable,too-many-branches,too-many-locals
import shelve


CONTESTS = {}

# These serial numbers were found on ballots from election day that do not
# correspond to any serial number on an election tape.  Twenty locations did
# not have a results tape.  Two locations are scans of another location.
MACHINE_NO_RESULTS = ['DS200 - 0319331858',
                      'DS200 - 0319371110',
                      'DS200 - 0319371813',
                      'DS200 - 0319371377', # V1020 Zero tape correct but results tape is not
                      'DS200 - 0319371390',
                      'DS200 - 0319331992',
                      'DS200 - 0319310529',
                      'DS200 - 0319320758',
                      'DS200 - 0319331820',
                      'DS200 - 0319341091',
                      'DS200 - 0319310432',
                      'DS200 - 0319371573',
                      'DS200 - 0319371600',
                      'DS200 - 0319310329',
                      'DS200 - 0319332091',
                      'DS200 - 0319330689',
                      'DS200 - 0319332083',
                      'DS200 - 0319371712',
                      'DS200 - 0319320790',
                      'DS200 - 0319341112',
                      'DS200 - 0319371510',
                      'DS200 - 0319331329']

ANALYSIS_BALLOTS = []
BASELINE_BALLOTS = []


#-----------------------------------------------------------------------------
# seperate_ballot_cvrs()
#-----------------------------------------------------------------------------
def seperate_ballot_cvrs(ballot_cvr_data):
    """Seperates the ballot CVR objects into two groups for analysis"""

    # Process every ballot CVR object in the data
    for ballot_cvr in ballot_cvr_data:

        serial = ballot_cvr['MachineSerial']

        if serial in MACHINE_NO_RESULTS:
            ANALYSIS_BALLOTS.append(ballot_cvr)
        else:
            BASELINE_BALLOTS.append(ballot_cvr)


#-----------------------------------------------------------------------------
# process_ballot_cvr_for_contests()
#-----------------------------------------------------------------------------
def process_ballot_cvr_for_contests(ballot_cvr_data):
    """Processes all of the ballot CVR objects to obtain all of the contests and selections"""

    # Set things up to process the ballot CVR objects
    contests = {}

    # Process every contest in every ballot_cvr object
    for ballot_cvr in ballot_cvr_data:
        for cvr_contest in ballot_cvr['Contests']:

            # Get the contest name, voter selection, and count status
            contest_name = cvr_contest['Contest']
            contest_selection = cvr_contest['Selection']
            contest_status = cvr_contest['Status']

            # Ensure that the contest status is only counted, overvoted, or undervoted
            if contest_status not in ('Counted', 'Overvoted', 'Undervoted'):
                print(r"ERROR!")
                break

            try:
                # Find the contest in the master list of contests
                contest = contests[contest_name]

                # We found it in the master list
                try:
                    # Find the voter selection in the contest
                    if contest_status == 'Counted':
                        current_vote_count = contest[contest_selection]
                        contest[contest_selection] = current_vote_count + 1
                    elif contest_status == 'Undervoted':
                        current_vote_count = contest['Undervoted']
                        contest['Undervoted'] = current_vote_count + 1
                    elif contest_status == 'Overvoted':
                        current_vote_count = contest['Overvoted']
                        contest['Overvoted'] = current_vote_count + 1

                except KeyError:

                    # The voter selection has not been added yet, so we will
                    # add it and a count of 1
                    contest[contest_selection] = 1

            except KeyError:

                # The contest has not been added yet, so we will add it
                # with the voter selection and a count of 1
                if contest_status == 'Counted':
                    contest = {contest_selection:1}
                    contest['Undervoted'] = 0
                    contest['Overvoted'] = 0
                elif contest_status == 'Undervoted':
                    contest = {}
                    contest['Undervoted'] = 1
                    contest['Overvoted'] = 0
                elif contest_status == 'Overvoted':
                    contest = {}
                    contest['Undervoted'] = 0
                    contest['Overvoted'] = 1
                contests[contest_name] = contest

    # Return the contests
    return contests


#-----------------------------------------------------------------------------
# generate_contest_totals()
#-----------------------------------------------------------------------------
def generate_contest_totals(contests):
    """Processes all of the contests and generates a total count of votes for each one"""

    # Look at every contest in the election
    for contest, contest_details in sorted(contests.items()):
        contest_total = 0

        # We'll look at every selection for the contest
        for selection, value in sorted(contest_details.items()):
            contest_total = contest_total + value

        # Save the total number of votes cast for this contest
        CONTESTS[contest] = contest_total


#-----------------------------------------------------------------------------
# compare_analysis_cvrs_to_baseline()
#-----------------------------------------------------------------------------
def compare_analysis_cvrs_to_baseline(all_contests, analysis_contests, baseline_contests):
    """Compares the analysis ballot CVR objects to the baseline data"""

    # We'll analyze every contest for the election
    for contest, contest_details in sorted(all_contests.items()):

        # Set things up for this contest
        analysis_batch_total = 0
        baseline_batch_total = 0

        # Get the total number of votes cast for this contest
        contest_total = CONTESTS[contest]

        # We will see if the analysis group of CVRs has this contest.  If so,
        # add up all of the vote totals for every selection in the contest.
        try:
            analysis_contest_details = analysis_contests[contest]
            analysis_batch_has_contest = True
            for selection, value in analysis_contest_details.items():
                analysis_batch_total = analysis_batch_total + value
        except KeyError:
            analysis_batch_total = 0
            analysis_batch_has_contest = False

        # We will also see if the baseline group of CVRs has this contest.
        try:
            baseline_contest_details = baseline_contests[contest]
            baseline_batch_has_contest = True
            for selection, value in baseline_contest_details.items():
                baseline_batch_total = baseline_batch_total + value
        except KeyError:
            baseline_batch_has_contest = False

        if analysis_batch_total + baseline_batch_total != contest_total:
            print(f"Contest totals do not match for {contest}")

        # We then look at every selection for this contest in the election
        for selection, value in sorted(contest_details.items()):

            analysis_batch_value = 0
            baseline_batch_value = 0

            # Check if the analysis contest has this selection
            if analysis_batch_has_contest:
                try:
                    analysis_batch_value = analysis_contest_details[selection]
                except KeyError:
                    analysis_batch_value = 0

            # Check if the baseline contest has this selection
            if baseline_batch_has_contest:
                try:
                    baseline_batch_value = baseline_contest_details[selection]
                except KeyError:
                    baseline_batch_value = 0

            # Calculate the percent of the analysis votes the selection received
            if analysis_batch_total > 0:
                analysis_percent = (float(analysis_batch_value) / float(analysis_batch_total)) * 100
            else:
                analysis_percent = 0

            # Calculate the percent of the baseline votes the selection received
            if baseline_batch_total > 0:
                baseline_percent = (float(baseline_batch_value) / float(baseline_batch_total)) * 100
            else:
                baseline_percent = 0

            # Print out the selection for this contest with the analysis and baseline data
            print(f"{contest}\t{selection}\t{analysis_batch_value}\t{analysis_percent:.3f}%\t{baseline_batch_value}\t{baseline_percent:.3f}%\t{value}")


#-----------------------------------------------------------------------------
# analyze_ballot_cvr_machines()
#-----------------------------------------------------------------------------
def analyze_ballot_cvr_machines(ballot_cvr_data):
    """Analyzes all of the ballot CVR objects looking at the machine data"""

    # Separate the ballot CVRs into the analysis group and the baseline group
    seperate_ballot_cvrs(ballot_cvr_data)

    # Collect the contests and selections for all data
    all_contests = process_ballot_cvr_for_contests(ballot_cvr_data)

    # Generate the contest totals for all of the data
    generate_contest_totals(all_contests)

    # Collect the contests and selections for the analysis data
    analysis_contests = process_ballot_cvr_for_contests(ANALYSIS_BALLOTS)

    # Collect the contests and selections for the baseline data
    baseline_contests = process_ballot_cvr_for_contests(BASELINE_BALLOTS)

    # Do the comparison and print out the results
    compare_analysis_cvrs_to_baseline(all_contests, analysis_contests, baseline_contests)


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    try:
        # Load in the data from the database
        db = shelve.open('dbfile')
        ballot_cvr_data = db['data']
        db.close()

    except KeyError:

        # Return if we cannot open the database file
        print(r"Cannot open database file 'dbfile'")
        return

    # Analyze the ballot scanners
    analyze_ballot_cvr_machines(ballot_cvr_data)


main()
