#-----------------------------------------------------------------------------
# analyze_tif_results_tapes_data.py
#
# Copyright (c) 2024 Daniel M. Teal
#
# License: MIT License
#
# Python script to analyze TIF results tapes data captured with
# "process_tif_results_tapes.py".  It must be run in the same directory the
# process script ran and includes the python shelve database files:
#
#    results_dbfile.dat, results_dbfile.dir, results_dbfile.bak
#-----------------------------------------------------------------------------
"""analyze_tif_results_tapes_data.py""" # for pylint
# pylint: disable=line-too-long,unused-variable,too-many-branches
# pylint: disable=too-many-nested-blocks
import shelve


# Specify the list of candidates we are checking the results tape for
CANDIDATES = ['Trump', 'Haley', 'Cruz', 'Biden', 'Allred',
              'Blacklock','Jones','Devine','Weems','Bland','Goldstein']


#-----------------------------------------------------------------------------
# analyze_tif_results_tape_data()
#-----------------------------------------------------------------------------
def analyze_tif_results_tape_data(results_tape_data):
    """Analyzes all of the TIF results tape data"""

    # Print out the header for the output
    headings = "Ballots,Pathname,SerialNumber,PublicCount,ExpressVoteCards,Sheets Processed"
    for candidate in CANDIDATES:
        headings = headings + ',' + candidate
    print(headings)

    # Process every ballot CVR object in the data
    for results_info in results_tape_data:

        pathname = results_info['Pathname']
        image_size = results_info['Image Size']

        # Look for the serial number
        try:
            serial_number = results_info['Serial Number']
        except KeyError:
            serial_number = 0

        # Look for the public count
        try:
            public_count = results_info['Public Count']
        except KeyError:
            public_count = 0

        # Look for the Expressvote Cards
        try:
            expressvote_cards = results_info['ExpressVote Cards']
        except KeyError:
            expressvote_cards = 0

        # Look for the Sheets processed
        try:
            sheets = results_info['Sheets Processed']
        except KeyError:
            sheets = 0

        # Determine the number of ballots counted
        ballots = max(public_count, expressvote_cards, sheets)

        data = f"{ballots},{pathname},{serial_number},{public_count},{expressvote_cards},{sheets}"

        # Look for the candidates data
        for candidate in CANDIDATES:
            try:
                votes = results_info[candidate]
            except KeyError:
                votes = 0
            data = data + "," + str(votes)

        print(data)


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    try:
        # Load in the data from the database
        db = shelve.open('results_dbfile')
        version = db['Version']
        results_tape_data = db['results_tapes']
        db.close()

    except KeyError:

        # Return if we cannot open the database file
        print(r"Cannot open database file 'results_dbfile'")
        return

    # Analyze the ballot scanners
    analyze_tif_results_tape_data(results_tape_data)


main()
