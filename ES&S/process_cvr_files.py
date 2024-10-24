#-----------------------------------------------------------------------------
# process_cvr_files.py
#
# Copyright (c) 2024 Daniel M. Teal
#
# License: MIT License
#
# Python script to process cast vote record PDF files generated by
# ES&S ballot scanners.
#-----------------------------------------------------------------------------
"""process_cvr_files.py""" # for pylint
# pylint: disable=line-too-long,unused-variable,too-many-branches
# pylint: disable=too-many-nested-blocks,too-many-statements
import os
import re
import pprint
import shelve

from pypdf import PdfReader


BALLOT_CVR_LIST = []
MACHINES = {}
CONTESTS = {}

#-----------------------------------------------------------------------------
# parse_cvr_header_line()
#
# This function parses the ballot cvr header line to add it to the ballot_cvr
# object.
#
# Example:
#
#   Cast Vote Record: 153,023
#   Poll Place: ED Vote Center
#   Precinct: 1752R
#   Ballot Style: REP 1752R [ Sheet Number 1 ]
#   Party: Republican
#   Tabulator CVR: 06fde5b4f6b37a43
#   Machine Serial: DS200 - 0319330715
#   Blank Ballot: NO
#   Reporting Group: Election Day
#-----------------------------------------------------------------------------
def parse_cvr_header_line(ballot_cvr, words):
    """Parses the CVR header line from the provided data"""

    is_contest_section = False

    # Strip of leading whitespace from the value
    if len(words) > 1:
        value = words[1].strip()

        if words[0] == 'Cast Vote Record':
            ballot_cvr['CastVoteRecord'] = value

        elif words[0] == 'Poll Place':
            ballot_cvr['PollPlace'] = value

        elif words[0] == 'Precinct':
            ballot_cvr['Precinct'] = value

        elif words[0] == 'Ballot Style':
            ballot_cvr['BallotStyle'] = value

        elif words[0] == 'Tabulator CVR':
            ballot_cvr['TabulatorCVR'] = value

        elif words[0] == 'Machine Serial':
            ballot_cvr['MachineSerial'] = value

        elif words[0] == 'Blank Ballot':
            ballot_cvr['Blank'] = value

        elif words[0] == 'Reporting Group':
            ballot_cvr['ReportingGroup'] = value

    if words[0] == 'Contests':
        is_contest_section = True

    return is_contest_section


#-----------------------------------------------------------------------------
# obtain_ballot_from_cvr()
#
# This function obtains the ballot info from the specified cvr, parses it, and
# creates a ballot_cvr object to return
#
# Example of a contest:
#
#   REP President (20020)
#   Vote For: 1
#   Donald J. Trump (20555)
#   Counted
#
# Example of an undervote:
#
#   Rep United States Senator (20122)
#   Vote For: 1
#   Undervoted
#   Undervoted
#
# Example of an overvote:
#
#   REP Proposition 6 (26626)
#   Vote For: 1
#   Yes (26628)
#   Overvoted
#
#   No (26630)
#   Overvoted
#-----------------------------------------------------------------------------
def obtain_ballot_from_cvr(pathname):
    """Obtains the text from the specified pathname"""

    # Initial the ballot cvr record with the provided pathname
    ballot_cvr = {'Pathname':pathname}

    # Parse the CVR PDF file to obtain the text
    reader = PdfReader(pathname)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"

    # Set things up for parsing
    contest_section = False
    cvr_contest_part = 1
    contests = []

    # Parse the text into the ballot CVR record
    for line in text.splitlines():
        line = line.strip()
        words = line.split(r':')

        # Parse the top of the CVR
        if contest_section is False:
            contest_section = parse_cvr_header_line(ballot_cvr, words)

        # Parse the contests from the CVR
        else:
            length = len(words[0])
            if length < 3:

                # We have a blank line so we will reset for the next contest unless
                # we are immediately following the 'Vote For:" line.  Sometimes the
                # CVR may have a blank line between that and the selection.
                if cvr_contest_part != 3:
                    cvr_contest_part = 1
                    cvr_contest = {}

            else:

                # This is the start of a new contest
                if cvr_contest_part == 1:
                    cvr_contest = {'Contest': line}
                    cvr_contest_part = 2

                elif cvr_contest_part == 2:
                    if len(words) > 1:
                        vote_for = words[1].strip()
                        cvr_contest['VoteFor'] = vote_for

                        # Unopposed candidates may have a vote for of 0 so we are
                        # finished with this contest
                        if vote_for == '0':

                            # We'll set the contest part to 5 to catch errors
                            cvr_contest_part = 5
                        else:
                            cvr_contest_part = 3

                    elif line == 'Overvoted':

                        # This is an additional overvote record that was already
                        # recorded from the previous contest.  The 'Contest' section
                        # has the additional selection and we'll drop this one.
                        # We'll set the contest part to 5 to catch errors
                        cvr_contest_part = 5
                        cvr_contest = {}

                    else:
                        print(f"Error parsing contests 1 in {pathname}")
                        break

                elif cvr_contest_part == 3:
                    cvr_contest['Selection'] = line
                    cvr_contest_part = 4

                elif cvr_contest_part == 4:

                    # Check for parenthesis from an overflow of the selection onto the next line
                    if re.search(r'\(.*?\)', line):

                        # Append the overflow text
                        selection = cvr_contest['Selection']
                        cvr_contest['Selection'] = f"{selection} {line}"

                    else:

                        # Check for an overvote.  If so, replace the selection
                        # to indicate an overvote
                        if line == 'Overvoted':
                            cvr_contest['Selection'] = line

                        cvr_contest['Status'] = line

                        # We're done parsing this contest so we'll add it to the list
                        contests.append(cvr_contest)

                        # We'll set the contest part to 5 to catch errors
                        cvr_contest_part = 5

                else:
                    print(f"Error parsing contests 2 in {pathname}")
                    break

    # Add the list of contests to the ballot cvr
    ballot_cvr['Contests'] = contests

#    pprint.pp(ballot_cvr)

    return ballot_cvr


#-----------------------------------------------------------------------------
# analyze_files()
#
# This function analyzes all of the files in the specified directory and
# subdirectories.
#-----------------------------------------------------------------------------
def analyze_files(dirname):
    """Analyze files in the specified directory"""

    num_ballot_cvrs = 0

    for dirpath, dirnames, filenames in os.walk(dirname):
        for filename in filenames:
            if re.search(r'c.pdf', filename, flags=re.IGNORECASE):

                # Create the pathname to the CVR file
                pathname = os.path.join(dirpath, filename)

                # Obtain a ballot CVR from the pathname
                ballot_cvr = obtain_ballot_from_cvr(pathname)

                # Add it to the list
                BALLOT_CVR_LIST.append(ballot_cvr)

                num_ballot_cvrs = len(BALLOT_CVR_LIST)
                if num_ballot_cvrs % 1000 == 0:
                    print(f"Processed {num_ballot_cvrs}")

    print(f"Processed a total of {num_ballot_cvrs} ballot CVRs")


#-----------------------------------------------------------------------------
# process_ballot_cvr_contests()
#-----------------------------------------------------------------------------
def process_ballot_cvr_contests():
    """Processes all of the ballot CVR objects looking at the contest data"""

    # Process every contest in every ballot_cvr object
    for ballot_cvr in BALLOT_CVR_LIST:
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
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Analyze the files in the current directory and subdirectories
    analyze_files(r".")

    # Analyze the ballot CVRs parsed in from the files
    process_ballot_cvr_contests()

    db = shelve.open('dbfile')
    db['data'] = BALLOT_CVR_LIST
    db.close()


main()
