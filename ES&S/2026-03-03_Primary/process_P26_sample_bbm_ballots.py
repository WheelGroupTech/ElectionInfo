#-----------------------------------------------------------------------------
# process_p26_sample_bbm_ballots.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to extract individual sample BBM ballots from a single PDF
# exported by the ES&S 6.3.0.0 EMS for an election.
#
# This script will write the output PDFs into the current directory.
#-----------------------------------------------------------------------------
"""process_p26_sample_bbm_ballots.py"""
# pylint: disable=line-too-long,unused-variable,too-many-branches, too-many-locals
# pylint: disable=broad-exception-caught,too-many-nested-blocks

import sys
import re

# 3rd Party imports
from pypdf import PdfReader, PdfWriter


OUTPUT_DIR = "."

PATTERN = r"Travis County\s+(Democratic|Republican)\s+Party\s+Primary\s+Election\s+([^ \t\r\n]+)\s*([DR][Zz]?)"


#-----------------------------------------------------------------------------
# obtain_ballots_from_file()
#
# This function obtains the individual ballots from the provided file.
#-----------------------------------------------------------------------------
def obtain_ballots_from_file(pathname):
    """Obtains individual ballots from a single file"""

    reader = PdfReader(pathname)

    # Each file = 2 pages
    for i in range(0, len(reader.pages), 2):

        first_page = reader.pages[i]
        text = first_page.extract_text()

        match = re.search(PATTERN, text, re.IGNORECASE)
        if not match:
            print(f"Could not find match on page {i+1}")
            print(f"{text}")
            continue

        party  = match.group(1)           # 'Republican' or 'Democratic"
        number = match.group(2)           # the part before D/R/DZ/RZ
        letter = match.group(3).upper()   # will be "D", "R", "DZ", or "RZ"

        # Generate the output filename to contain the sample ballot
        output_sample_filename = f"{OUTPUT_DIR}/Sample-{number}{letter}.pdf"

        # Create the PDF to write out
        writer = PdfWriter()
        writer.add_page(reader.pages[i])
        writer.add_page(reader.pages[i+1])

        # Write out the PDF with the sample filename
        with open(output_sample_filename, "wb") as f:
            writer.write(f)

        print(f"Wrote {output_sample_filename}")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Obtain the ballots from the provided PDF
    if len(sys.argv) != 2:
        print("Usage: python process_sample_bbm_ballots.py <file_path>")
        return False
    pathname = sys.argv[1]
    obtain_ballots_from_file(pathname)

    return True

main()
