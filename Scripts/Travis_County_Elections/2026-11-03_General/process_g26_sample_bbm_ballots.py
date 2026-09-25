#-----------------------------------------------------------------------------
# process_g26_sample_bbm_ballots.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to extract individual sample BBM ballots from a single PDF
# exported by the ES&S 6.3.0.0 EMS for the November 3, 2026 Joint General
# and Special Elections.
#
# A ballot is either 2 pages (1 sheet) or 4 pages (2 sheets).  The ballot
# style (e.g. "100A") is printed in the header of the first page of each
# sheet, directly below "Travis County Joint General and Special Elections".
#
# The exported PDF contains the full set of ballot styles three times, one
# after the other:
#   1. Full ballots
#   2. Federal offices only ballots
#   3. Ballots without the U.S. Representative contest (these pages do not
#      have the "Sheet X of Y, Page N of M" footer)
#
# The Nth time a ballot style is seen, the ballot is written to the Nth
# subdirectory in SECTION_DIRS.
#
# This script will write the output PDFs into subdirectories of the current
# directory.
#-----------------------------------------------------------------------------
"""process_g26_sample_bbm_ballots.py"""
# pylint: disable=line-too-long,too-many-locals

import os
import re
import sys

# 3rd Party imports
import pymupdf


OUTPUT_DIR = "."

# Subdirectory for each successive occurrence of the ballot styles
SECTION_DIRS = ["Full", "Federal", "NoUSRep"]

# Ballot style on the line following the election title / Spanish title
STYLE_PATTERN = re.compile(r"Travis County Joint General and Special Elections\s*\n[^\n]*\n\s*(\d+[A-Z]+)\s*\n")

# Footer present on the full and federal only ballots
PAGE_PATTERN = re.compile(r"Sheet\s+(\d+)\s+of\s+(\d+),\s*Page\s+(\d+)\s+of\s+(\d+)")


#-----------------------------------------------------------------------------
# find_ballots()
#
# This function groups the pages of the document into ballots.  A new
# ballot begins on a page whose header contains a ballot style different
# from the ballot currently being collected (the first page of sheet 2
# repeats the ballot style of sheet 1).
#
# Returns a list of (style, [page indexes], expected page count or None).
#-----------------------------------------------------------------------------
def find_ballots(doc):
    """Groups the pages of the document into ballots"""

    ballots = []
    current_style = None

    for i in range(doc.page_count):
        text = doc[i].get_text()

        style_match = STYLE_PATTERN.search(text)
        if style_match and style_match.group(1) != current_style:
            current_style = style_match.group(1)
            ballots.append((current_style, [], []))
        elif current_style is None:
            print(f"Skipping page {i+1}: no ballot style found")
            continue

        style, pages, totals = ballots[-1]
        pages.append(i)

        page_match = PAGE_PATTERN.search(text)
        if page_match:
            totals.append(int(page_match.group(4)))

    # Reduce the page footer totals to a single expected page count
    return [(style, pages, max(totals) if totals else None) for style, pages, totals in ballots]


#-----------------------------------------------------------------------------
# obtain_ballots_from_file()
#
# This function obtains the individual ballots from the provided file.
#-----------------------------------------------------------------------------
def obtain_ballots_from_file(pathname):
    """Obtains individual ballots from a single file"""

    doc = pymupdf.open(pathname)

    occurrences = {}
    written = 0

    for style, pages, expected in find_ballots(doc):

        # Determine which section this ballot belongs to
        section = occurrences.get(style, 0)
        occurrences[style] = section + 1
        if section >= len(SECTION_DIRS):
            print(f"WARNING: Ballot style {style} seen more than {len(SECTION_DIRS)} times (page {pages[0]+1}); skipping")
            continue
        section_dir = f"{OUTPUT_DIR}/{SECTION_DIRS[section]}"
        os.makedirs(section_dir, exist_ok=True)

        # Check that the ballot is complete
        if expected is not None and len(pages) != expected:
            print(f"WARNING: {SECTION_DIRS[section]} ballot {style} (page {pages[0]+1}) has {len(pages)} pages, but is marked as {expected} pages")
        elif len(pages) not in (2, 4):
            print(f"WARNING: {SECTION_DIRS[section]} ballot {style} (page {pages[0]+1}) has {len(pages)} pages")

        # Generate the output filename to contain the sample ballot
        output_sample_filename = f"{section_dir}/Sample-{style}.pdf"

        # Create the PDF to write out
        writer = pymupdf.open()
        writer.insert_pdf(doc, from_page=pages[0], to_page=pages[-1])

        # Subset the embedded fonts (otherwise each file carries full fonts, ~575KB)
        writer.subset_fonts()

        # Write out the PDF with the sample filename
        writer.save(output_sample_filename, garbage=3, deflate=True)
        writer.close()
        written += 1

        print(f"Wrote {output_sample_filename}")

    doc.close()
    print(f"Wrote {written} sample ballots")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Obtain the ballots from the provided PDF
    if len(sys.argv) != 2:
        print("Usage: python process_g26_sample_bbm_ballots.py <file_path>")
        return False
    pathname = sys.argv[1]
    obtain_ballots_from_file(pathname)

    return True

main()
