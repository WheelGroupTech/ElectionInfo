#-----------------------------------------------------------------------------
# analyze_cvr_data.py
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
"""analyze_cvr_data.py""" # for pylint
# pylint: disable=line-too-long,unused-variable,too-many-branches
# pylint: disable=too-many-nested-blocks
import pprint
import re
import shelve


MACHINES = {}
CONTESTS = {}


#-----------------------------------------------------------------------------
# process_ballot_cvr_contests()
#-----------------------------------------------------------------------------
def process_ballot_cvr_contests(ballot_cvr_data):
    """Processes all of the ballot CVR objects looking at the contest data"""

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
                contest = CONTESTS[contest_name]

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
                CONTESTS[contest_name] = contest

    pprint.pp(CONTESTS)


#-----------------------------------------------------------------------------
# analyze_ballot_cvr_machines()
#-----------------------------------------------------------------------------
def analyze_ballot_cvr_machines(ballot_cvr_data):
    """Analyzes all of the ballot CVR objects looking at the machine data"""

    ds200_ballot_count = 0
    express_ballot_count = 0
    central_ballot_count = 0
    total_ballot_count = 0

    # Process every ballot CVR object in the data
    for ballot_cvr in ballot_cvr_data:
        serial = ballot_cvr['MachineSerial']
        poll_place = ballot_cvr['PollPlace']
        reporting_group = ballot_cvr['ReportingGroup']

        try:

            # Find the machine serial number in the list of machines
            machine = MACHINES[serial]

            # We found it, check to see if the reporting group is different
            if machine['ReportingGroup'] != reporting_group:
                print(f"Machine {serial} report group error {machine['ReportingGroup']} != {reporting_group}")
            if machine['PollPlace'] != poll_place:
                print(f"Machine {serial} poll place error {machine['PollPlace']} != {poll_place}")

            # Update the count of ballots for the machine
            current_ballot_count = machine['BallotCount']
            machine['BallotCount'] = current_ballot_count + 1

        except KeyError:

            # The machine was not found in the list, so we will add it
            machine = {'PollPlace':poll_place}
            machine['ReportingGroup'] = reporting_group
            machine['BallotCVRs'] = []
            machine['BallotCount'] = 1
            MACHINES[serial] = machine

        # Increment the counter that best matches the machine serial
        total_ballot_count += 1
        if re.search(r"DS200", serial):
            ds200_ballot_count += 1
        elif re.search(r"ExpressTouch", serial):
            express_ballot_count += 1
        else:
            central_ballot_count += 1

        # Append the ballot CVR to the list for this machine
        machine['BallotCVRs'].append(ballot_cvr)

    # Now we can process every machine in the list to print out information
    # for it which includes the count of ballots it scanned
    for machine, details in MACHINES.items():
        print(f"{machine},{details['PollPlace']},{details['ReportingGroup']},{details['BallotCount']}")

    print(f"DS200 Scanned Ballots {ds200_ballot_count}")
    print(f"ExpressTouch Scanned Ballots {express_ballot_count}")
    print(f"Central Count Scanned Ballots {central_ballot_count}")
    print(f"Total Scanned Ballots {total_ballot_count}")


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
