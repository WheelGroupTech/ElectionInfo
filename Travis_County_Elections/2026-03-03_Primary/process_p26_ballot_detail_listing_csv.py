#-----------------------------------------------------------------------------
# process_P26_ballot_detail_listing_csv.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to process a ballot detail listing CSV file from an ES&S EMS.
#
# The primary goal is to determine the barcode value for every selection in
# every contest for every ballot style.  We will take the input CSV file,
# clean up the rows and calculate the barcode values, and output the result
# into an output CSV file.
#
# The six digit barcode values represent the location of the ovals on a mark-
# sense ballot (i.e. mail/pre-printed ballot).  The ballot is organized into
# a grid of rows 0.5 cm tall and 0.8 cm wide.  The six digits follow the
# pattern CCRRSP:
#   - CC:  two-digits representing the grid column
#   - RR:  two-digits representing the grid row
#   - S:   The side of the ballot, 1 for the front, 2 for the back
#   - P:   The page of the ballot, 1 unless it is a multi-page ballot
#-----------------------------------------------------------------------------
"""process_P26_ballot_detail_listing_csv.py"""
# pylint: disable=line-too-long,unused-variable,too-many-branches,unused-argument
# pylint: disable=too-many-nested-blocks,too-many-statements,too-many-locals
# pylint: disable=broad-exception-caught
import csv
import sys
import re


#-----------------------------------------------------------------------------
# parse_ballot_style()
#-----------------------------------------------------------------------------
def parse_ballot_style(row, line_num):
    """Extract D/R/DZ/RZ and e.g. 100A from Ballot Style line"""
    if len(row) < 3 or not row[2].strip():
        print(f"Warning line {line_num}: No style text in Ballot Style row")
        return None, None

    text = row[2].strip()
    match = re.search(
        r'(\d+)\s*-\s*([DRZ]{1,2})\s+(\d{3})\s+(\d{3}[A-Fa-f])',
        text,
        re.IGNORECASE
    )
    if match:
        ballot_type = match.group(2).upper()
        style_id   = match.group(4).upper()
        if ballot_type in ['D', 'R', 'DZ', 'RZ']:
            return ballot_type, style_id

    print(f"Warning line {line_num}: Could not parse style from: {text}")
    return None, None


#-----------------------------------------------------------------------------
# skip_to_header()
#
# This code reads up to four lines looking for data column titles.  Example:
#
#   Precinct ID,,,,,Precinct Name,,,,,,,,
#   0001-01,,,,,100 100A,,,,,,,,
#   Order,Vote For,,Term,Contest,,,Rotation,Candidate,,,Row,Col,
#
# There will be multiple empty fields between the titles
#-----------------------------------------------------------------------------
def skip_to_header(reader, line_num_start):
    """Skip up to ~4 lines looking for the header with data column titles"""

    for i in range(4):
        try:
            row = next(reader)
            stripped = [str(c).strip() for c in row]
            if len(stripped) >= 10 and stripped[0] == 'Order' and stripped[1] == 'Vote For':

                # Get the positions of non-empty fields
                # Example result: [0, 1, 3, 4, 7, 8, 11, 12]
                non_empty_positions = [i for i, field in enumerate(stripped) if field]

                # Get the titles of the non-empty fields
                # Example result: [{0: 'Order', 1: 'Vote For', 3: 'Term', 4: 'Contest', 7: 'Rotation', 8: 'Candidate', 11: 'Row', 12: 'Col'}]
                pos_to_name = {i: stripped[i] for i in non_empty_positions}

                #print(f"Non-Empty:  [{non_empty_positions}]")
                #print(f"Pos-ToName: [{pos_to_name}]")

                return True, non_empty_positions

        except StopIteration:
            return False
    return False


#-----------------------------------------------------------------------------
# parse_csv_file()
#-----------------------------------------------------------------------------
def parse_csv_file(input_file, output_file='extracted_ballot_data.csv'):
    """Parses the csv file"""

    data = []  # list of (ballot_type, ballot_style_id, data_row)

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)

        current_type = None
        current_style = None
        in_data_section = False
        page_num = 0
        positions = []
        saved_row_output = []

        line_number = 0  # for better debugging messages

        while True:
            try:
                row = next(reader)
                line_number += 1
            except StopIteration:
                break

            # Skip fully empty rows
            if not any(cell.strip() for cell in row):
                continue

            first_cell = row[0].strip() if row else ""

            # -----------------------------------------------
            # Start of new a new page
            # -----------------------------------------------
            if first_cell == "Ballot Detail Listing":
                in_data_section = False

                # Skip 2 election info lines
                for _ in range(2):
                    try:
                        next(reader)
                        line_number += 1
                    except StopIteration:
                        return

                # Portrait note line
                try:
                    note_row = next(reader)
                    line_number += 1
                    note_text = ' '.join(str(c).strip() for c in note_row)
                    if 'portrait orientation' not in note_text.lower():
                        print(f"Warning line {line_number}: Expected portrait note missing")
                except StopIteration:
                    return

                # Ballot Style line (first one of the page)
                try:
                    style_row = next(reader)
                    line_number += 1
                    if style_row and style_row[0].strip() == 'Ballot Style:':
                        current_type, current_style = parse_ballot_style(style_row, line_number)
                        if not current_type:
                            continue  # skip this page if style invalid
                except StopIteration:
                    return

                # Skip optional lines and find header with data column headings
                result, positions = skip_to_header(reader, line_number)
                if not result:
                    print(f"Warning: Header not found after initial style (line ~{line_number})")
                    continue

                in_data_section = True
                page_num += 1
                continue

            # End of the page
            if "Ballot Detail Listing - " in first_cell:
                in_data_section = False
                continue

            # -----------------------------------------------
            # New style inside the data section of current page
            # -----------------------------------------------
            if in_data_section and row and row[0].strip() == 'Ballot Style:':
                current_type, current_style = parse_ballot_style(row, line_number)
                if not current_type:
                    print(f"Warning line {line_number}: Invalid mid-page style - keeping previous")
                    continue

                # Skip optional lines and find header with data column headings again
                result, positions = skip_to_header(reader, line_number)
                if not result:
                    print(f"Warning line {line_number}: No header after mid-page style change")
                    in_data_section = False
                    continue

                print(f"  New style mid-page: {current_type} / {current_style} (line {line_number})")
                continue

            # -----------------------------------------------
            # Normal data row
            #
            # Each contest with multiple selections only lists the
            # fields like "Order,Vote For,Term,Contest,Rotation" on the first
            # line, additional candidates have those fields blank.
            #
            # There will also be blank fields in the row that were identified by
            # the 'positions' values from skip_to_header()
            #
            # We want to copy the appropriate fields to each line where needed so
            # that every line has the complete set of data.
            #
            # Example Input lines:
            #   2,1,,6,REP United States Senator,,,1,Candidate 1,,,32,1,
            #   ,,,,,,,,Candidate 2,,,34,1,
            #   ,,,,,,,,Candidate 3,,,36,1,
            #
            # Output lines:
            #   5,R,100A,2,1,6,REP United States Senator,1,Candidate 1,32,1
            #   5,R,100A,2,1,6,REP United States Senator,1,Candidate 2,34,1
            #   5,R,100A,2,1,6,REP United States Senator,1,Candidate 3,36,1
            # -----------------------------------------------
            if in_data_section and any(cell.strip() for cell in row):

                # Ensure that we have a current style
                if current_type is None or current_style is None:
                    print(f"Warning line {line_number}: Data row without active style - skipped")
                    continue

                # Extract only the row fields that have positions
                row_output = []
                for pos in positions:
                    if pos < len(row):
                        row_output.append(row[pos])
                    else:
                        row_output.append('')

                # Skip data rows that do not have the row and col fields set
                # We are only interested in contest selections and the bubble positions
                if row_output[6] == '' and row_output[7] == '':
                    continue

                # Check the first position, if it is not blank save the fields
                # for the rows below.  If it is blank, use the fields from the
                # row that had them.
                if row_output[0] == '':
                    row_output[:5] = saved_row_output
                else:
                    saved_row_output = row_output[:5]

                # Append the data
                data.append((current_type, current_style, row_output))

    # -----------------------------------------------
    # Write output
    # -----------------------------------------------
    if not data:
        print("No data rows extracted.")
        return

    with open(output_file, 'w', encoding='utf-8', newline='') as outf:
        writer = csv.writer(outf)
        writer.writerow(['Ballot Type', 'Ballot Style ID',
                         'Order', 'Vote For', 'Term', 'Contest', 
                         'Rotation', 'Candidate', 'Row', 'Col', 'Barcode'])

        # The row output looks like:
        #
        # Order,Vote For,Term,Contest,Rotation,Candidate,Row,Col
        # ------------------------------------------------------
        # 2,1,6,REP United States Senator,1,Candidate 1,32,1
        # 2,1,6,REP United States Senator,1,Candidate 2,34,1
        # 2,1,6,REP United States Senator,1,Candidate 3,36,1
        #
        # The order value increases with each contest, when it
        # goes back to a low value (1 or 2), then we have a new
        # ballot so we start the ballot side at 1.  If the column
        # number drops, we change from one ballot side to another
        ballot_page=1
        ballot_side=1
        prev_order_value=100
        prev_col_value=100
        for typ, sty, row_output in data:

            # Determine the ballot page, size, column, and row
            order_value = int(row_output[0])
            row_value = int(row_output[6])
            col_value = int(row_output[7])

            if order_value < prev_order_value:
                ballot_page = 1
                ballot_side = 1
            elif col_value < prev_col_value:
                if ballot_side == 1:
                    ballot_side = 2
                else:
                    ballot_side = 1
                    ballot_page += 1

            prev_order_value = order_value
            prev_col_value = col_value

            # Determine the six digit barcode value:
            #   - CC:  two-digits representing the grid column
            #   - RR:  two-digits representing the grid row
            #   - S:   The side of the ballot, 1 for the front, 2 for the back
            #   - P:   The page of the ballot, 1 unless it is a multi-page ballot
            barcode = (col_value * 10000) + (row_value * 100) + (ballot_side * 10) + ballot_page
            row_output.append(barcode)

            # Ballot code barcode values are
            writer.writerow([typ, sty] + row_output)

        for typ, sty, row_output in data:
            print(f"{typ},{sty}, {row_output}")

    print(f"\nDone. Extracted {len(data)} data rows from {page_num} pages to {output_file}")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Check args
    if len(sys.argv) < 2:
        print("Usage: python process_ballot_detail_listing_csv.py input.csv [output.csv]")
        sys.exit(1)

    # The first arg is the input file
    input_file_arg = sys.argv[1]

    # The second optional arg is the output file
    output_file_arg = sys.argv[2] if len(sys.argv) > 2 else 'extracted_ballot_data.csv'

    # Parse the data
    parse_csv_file(input_file_arg, output_file_arg)


main()
