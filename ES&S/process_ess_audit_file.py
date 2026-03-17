#-----------------------------------------------------------------------------
# process_ess_audit_file.py
#
# Copyright (c) 2025 Daniel M. Teal
#
# License: MIT License
#
# Python script to process the ESS Election Audit Events Report
#-----------------------------------------------------------------------------
"""process_ess_audit_file.py""" # for pylint

# Standard imports
import os
import sys

# 3rd Party imports
from pypdf import PdfReader


#-----------------------------------------------------------------------------
# write_to_file()
#-----------------------------------------------------------------------------
def write_to_file(filename, text, mode='w'):
    """
    Write text to a file.
    
    :param filename: Name of the file (string)
    :param text: Text content to write (string)
    :param mode: File mode - 'w' (overwrite) or 'a' (append)
    """
    if not isinstance(filename, str) or not filename.strip():
        raise ValueError("Filename must be a non-empty string.")
    if not isinstance(text, str):
        raise ValueError("Text must be a string.")
    if mode not in ('w', 'a'):
        raise ValueError("Mode must be 'w' (write) or 'a' (append).")

    try:
        # Use UTF-8 encoding for compatibility
        with open(filename, mode, encoding='utf-8') as file:
            file.write(text)
        print(f"Text successfully written to '{filename}' in mode '{mode}'.")
    except OSError as e:
        print(f"Error writing to file: {e}")


#-----------------------------------------------------------------------------
# obtain_text_from_audit_file()
#-----------------------------------------------------------------------------
def obtain_text_from_audit_file(pathname):
    """Obtains the text from the specified pathname"""

    # Parse the CVR PDF file to obtain the text
    reader = PdfReader(pathname)
    text = ""
    page_num = 0
    for page in reader.pages:
        page_num += 1
        print(f"Processing page {page_num}")
        text += page.extract_text() + "\n"

    return text


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    # Get the name of the specified audit file to process
    if len(sys.argv) != 2:
        print("Usage: python process_ess_audit_file.py <file_path>")
        return False
    src_pathname = sys.argv[1]

    # Validate input type
    if not isinstance(src_pathname, str):
        raise TypeError("Pathname must be a string.")

    # Split into name and extension
    base, ext = os.path.splitext(src_pathname)

    # Check if it's a PDF (case-insensitive)
    if ext.lower() != ".pdf":
        raise ValueError("The file does not have a .pdf extension.")

    # Create the destination pathname for the output text file
    dst_pathname = base + ".txt"

    # Obain the text from the audit file
    text = obtain_text_from_audit_file(src_pathname)

    # Save the text to the output file, overwriting anything already there
    write_to_file(dst_pathname, text, 'w')

    return True


main()
