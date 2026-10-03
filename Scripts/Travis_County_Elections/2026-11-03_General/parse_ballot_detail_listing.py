#-----------------------------------------------------------------------------
# parse_ballot_detail_listing.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to parse the Ballot Detail Listing (Excel format) exported by
# the ES&S EVS 6.3 for the November 3, 2026 Joint General and Special
# Elections, and write one CSV row per selection oval.
#
# The listing is a sequence of report pages.  Each card (sheet) of each
# ballot starts with a card header and a Precinct ID / Precinct Name block,
# followed by the contests and selections.  A card that does not fit on one
# report page continues on the next page under a repeated card header.
#
# Card header examples:
#   1 - 100 100A          Card style 1, full ballot, style 100A, card 1
#   1 - FED 100 100A      Federal offices only ballot
#   1 - LB 100 100A       Limited ballot
#   1 - 100 100A [2]      Second card (sheet) of a two card ballot
#
# The listing does not identify the side of the card.  The Row/Col values
# run down each column of ovals in turn, so when the column (or the row
# within the same column) goes backwards, the selections have moved from the
# front of the card to the back.
#
# The six digit barcode values represent the location of the ovals on a mark-
# sense ballot (i.e. mail/pre-printed ballot).  The ballot is organized into
# a grid of rows 0.5 cm tall and 0.8 cm wide.  The six digits follow the
# pattern CCRRSP:
#   - CC:  two-digits representing the grid column
#   - RR:  two-digits representing the grid row
#   - S:   The side of the ballot, 1 for the front, 2 for the back
#   - P:   The page of the ballot, 1 unless it is a multi-page ballot
#
# The barcode is written as a zero-padded string (e.g. "012911") so that the
# leading zero of columns 0-9 is kept.
#-----------------------------------------------------------------------------
"""parse_ballot_detail_listing.py"""
# pylint: disable=line-too-long,too-many-locals,too-many-branches,too-many-statements

import collections
import csv
import re
import sys
import warnings

# 3rd Party imports
import openpyxl


# Card header, e.g. "1 - 100 100A", "1 - LB 100 100A [2]"
CARD_PATTERN = re.compile(r"^(\d+) - (?:(FED|LB) )?(\S+) (\S+)(?: \[(\d+)\])?$")

# Ballot type for each card header prefix
BALLOT_TYPES = {None: "Full", "FED": "FED", "LB": "LB"}

# Column titles of the selection table in the listing
TABLE_TITLES = ["Order", "Vote For", "Term", "Contest", "Rotation", "Candidate", "Row", "Col"]

OUTPUT_FIELDS = ["Card Style Number", "Ballot Type", "Ballot Style ID", "Precinct ID", "Precinct Name",
                 "Page", "Side", "Order", "Vote For", "Term", "Contest", "Rotation", "Candidate",
                 "Row", "Col", "Barcode"]


#-----------------------------------------------------------------------------
# cell_text()
#-----------------------------------------------------------------------------
def cell_text(row, index):
    """Returns the stripped text of a cell, or '' if empty or missing"""

    if index is None or index >= len(row) or row[index] is None:
        return ""
    return str(row[index]).strip()


#-----------------------------------------------------------------------------
# read_cards()
#
# This function reads the listing and returns a list of cards, each with its
# header fields, precinct, and the list of selections in listing order.
#-----------------------------------------------------------------------------
def read_cards(pathname):
    """Reads the cards and selections from the listing workbook"""

    with warnings.catch_warnings():
        # The EVS export has no default style, which openpyxl warns about
        warnings.simplefilter("ignore")
        workbook = openpyxl.load_workbook(pathname, read_only=True)
    sheet = workbook.worksheets[0]

    # The export does not record the sheet dimensions correctly
    sheet.reset_dimensions()

    cards = []
    card_header = None       # most recent card header match
    precinct_columns = None  # columns of "Precinct ID" and "Precinct Name"
    columns = None           # column of each TABLE_TITLES entry
    contest = None           # fields of the current contest

    for line_number, row in enumerate(sheet.iter_rows(values_only=True), start=1):
        cells = [(i, str(c).strip()) for i, c in enumerate(row) if c is not None and str(c).strip()]
        if not cells:
            continue
        first = cells[0][1]

        # Card header (also repeated at the top of continuation pages)
        match = next((CARD_PATTERN.match(text) for i, text in cells if CARD_PATTERN.match(text)), None)
        if match:
            card_header = match
            continue

        # Precinct block, which starts a new card
        if first == "Precinct ID":
            precinct_columns = (cells[0][0], next((i for i, text in cells if text == "Precinct Name"), None))
            continue
        if precinct_columns:
            if card_header is None:
                print(f"WARNING: line {line_number}: precinct without a card header")
            else:
                cards.append({
                    "header": card_header.group(0),
                    "Card Style Number": int(card_header.group(1)),
                    "Ballot Type": BALLOT_TYPES[card_header.group(2)],
                    "Ballot Style ID": card_header.group(4),
                    "Page": int(card_header.group(5) or 1),
                    "Precinct ID": cell_text(row, precinct_columns[0]),
                    "Precinct Name": cell_text(row, precinct_columns[1]),
                    "selections": [],
                })
            precinct_columns = None
            contest = None
            continue

        # Selection table column titles
        if first == "Order":
            columns = {text: i for i, text in cells}
            missing = [title for title in TABLE_TITLES if title not in columns]
            if missing:
                print(f"WARNING: line {line_number}: missing table columns {missing}")
            continue

        # Skip the report page headers and footers
        if columns is None or not cards or first.startswith("Ballot Detail Listing") or first.startswith("Note:"):
            continue

        # A row with an Order value starts a contest; the following rows of
        # the contest only have the candidate, row, and column
        order = cell_text(row, columns.get("Order"))
        if order:
            contest = {title: cell_text(row, columns.get(title)) for title in TABLE_TITLES[:5]}

        candidate = cell_text(row, columns.get("Candidate"))
        grid_row = cell_text(row, columns.get("Row"))
        grid_col = cell_text(row, columns.get("Col"))

        # Only keep selections that have an oval (e.g. skip the Party Legend)
        if not grid_row and not grid_col:
            continue
        if contest is None or not grid_row.isdigit() or not grid_col.isdigit():
            print(f"WARNING: line {line_number}: unexpected selection row {[text for i, text in cells]}")
            continue
        if card_header.group(0) != cards[-1]["header"]:
            print(f"WARNING: line {line_number}: selection under header '{card_header.group(0)}' added to card '{cards[-1]['header']}'")

        cards[-1]["selections"].append(dict(contest, **{"Candidate": candidate, "Row": int(grid_row), "Col": int(grid_col)}))

    workbook.close()
    return cards


#-----------------------------------------------------------------------------
# assign_sides_and_barcodes()
#
# This function determines the side of each selection and its barcode value.
#-----------------------------------------------------------------------------
def assign_sides_and_barcodes(cards):
    """Adds the Side and Barcode values to each selection"""

    for card in cards:
        side = 1
        previous = None
        for selection in card["selections"]:
            position = (selection["Col"], selection["Row"])
            if previous and position < previous:
                side += 1
                if side > 2:
                    print(f"WARNING: card {card['Card Style Number']} {card['Ballot Type']} {card['Ballot Style ID']} page {card['Page']} has more than two sides")
            previous = position

            selection["Side"] = side
            selection["Barcode"] = f"{selection['Col']:02d}{selection['Row']:02d}{side}{card['Page']}"


#-----------------------------------------------------------------------------
# write_csv()
#-----------------------------------------------------------------------------
def write_csv(cards, output_pathname):
    """Writes one CSV row per selection"""

    # utf-8-sig so Excel displays the accented names correctly
    with open(output_pathname, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for card in cards:
            card_fields = {k: v for k, v in card.items() if k in OUTPUT_FIELDS}
            for selection in card["selections"]:
                writer.writerow(dict(card_fields, **selection))


#-----------------------------------------------------------------------------
# print_summary()
#-----------------------------------------------------------------------------
def print_summary(cards):
    """Prints counts and sanity checks"""

    counts = collections.Counter((card["Ballot Type"], card["Page"]) for card in cards)
    for (ballot_type, page), count in sorted(counts.items()):
        print(f"  {ballot_type:4} page {page}: {count} cards")

    selections = sum(len(card["selections"]) for card in cards)
    print(f"  {len(cards)} cards, {selections} selections")

    for card in cards:
        barcodes = collections.Counter(s["Barcode"] for s in card["selections"])
        duplicates = [barcode for barcode, count in barcodes.items() if count > 1]
        if duplicates:
            print(f"WARNING: card {card['Card Style Number']} {card['Ballot Type']} {card['Ballot Style ID']} page {card['Page']} has duplicate oval positions {duplicates}")
        if not card["selections"]:
            print(f"WARNING: card {card['Card Style Number']} {card['Ballot Type']} {card['Ballot Style ID']} page {card['Page']} has no selections")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    if len(sys.argv) not in (2, 3):
        print("Usage: python parse_ballot_detail_listing.py <listing.xlsx> [output.csv]")
        return False
    input_pathname = sys.argv[1]
    output_pathname = sys.argv[2] if len(sys.argv) == 3 else "ballot_detail_listing.csv"

    cards = read_cards(input_pathname)
    assign_sides_and_barcodes(cards)
    write_csv(cards, output_pathname)
    print_summary(cards)
    print(f"Wrote {output_pathname}")

    return True

main()
