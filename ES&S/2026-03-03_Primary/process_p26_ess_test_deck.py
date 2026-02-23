#-----------------------------------------------------------------------------
# process_p26_ess_test_deck.py
#
# Copyright (c) 2024-2026 Daniel M. Teal
#
# License: MIT License
#
# Python script extract individual ballots cards from an ES&S EMS test deck.
#-----------------------------------------------------------------------------
"""process_p26_ess_test_deck.py"""
# pylint: disable=too-many-locals,unused-variable,broad-exception-caught
import os
import re
import sys

# 3rd Party imports
from pypdf import PdfReader, PdfWriter


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main(source_dir, dest_dir):
    """main function"""

    # Create destination directory if it doesn't exist
    os.makedirs(dest_dir, exist_ok=True)

    # Regex pattern to match the described filename structure
    pattern = re.compile(r'^(D|R)\s+(\d{3})\s+(\d{3})([A-F])(?:[-_.].*)?\.pdf$', re.IGNORECASE)

    # Enumerate all files in the source directory
    for filename in os.listdir(source_dir):
        filepath = os.path.join(source_dir, filename)
        if not os.path.isfile(filepath):
            continue

        match = pattern.match(filename)
        if not match:
            print(f"Skipping file '{filename}' as it does not match the expected pattern.")
            continue

        prefix, num1, num2, letter = match.groups()

        # Use the second three-digit number as per the output description
        base_name = f"{prefix}{num2}{letter}"

        # Open the PDF
        try:
            reader = PdfReader(filepath)
            num_pages = len(reader.pages)
            for page_num in range(1, num_pages + 1):
                writer = PdfWriter()
                writer.add_page(reader.pages[page_num - 1])

                # ESS test deck has the first page select nothing, the second
                # page is all position 1, the third page all position 2, etc..
                output_filename = f"{base_name}-{page_num - 1}.pdf"
                output_path = os.path.join(dest_dir, output_filename)

                with open(output_path, 'wb') as output_file:
                    writer.write(output_file)

            print(f"Processed '{filename}' into {num_pages} individual pages.")
        except Exception as e:
            print(f"Error processing '{filename}': {e}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <source_directory> <destination_directory>")
        sys.exit(1)

    source_directory = sys.argv[1]
    dest_directory = sys.argv[2]

    if not os.path.isdir(source_directory):
        print(f"Error: Source directory '{source_directory}' does not exist.")
        sys.exit(1)

    main(source_directory, dest_directory)
