#-----------------------------------------------------------------------------
# process_p26_test_deck_ballots.py
#
# Copyright (c) 2024-2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to process Ballot Style sample ballot cards from an ES&S EMS.
#-----------------------------------------------------------------------------
"""process_p26_test_deck_ballots.py"""
# pylint: disable=line-too-long,unused-variable,too-many-branches
# pylint: disable=too-many-nested-blocks,too-many-statements,too-many-locals
# pylint: disable=broad-exception-caught

import io
import os
import re
import sys
import shelve

# 3rd Party imports
import fitz
from PIL import Image
from pyzbar.pyzbar import decode


# Specify the database version for the ES&S ballot data
ESS_BALLOT_DATA_VERSION = 2

# Complete list of ballots that were read in and processed
BALLOTS = []


#-----------------------------------------------------------------------------
# obtain_barcodes_from_ballot_image()
#
# This function obtains the barcode values from the specified ballot image PDF.
#-----------------------------------------------------------------------------
def obtain_barcodes_from_ballot_image(pathname):
    """Obtains barcodes from a ballot image"""

    barcode_values = []

    # Open the specified PDF file
    pdf_image = fitz.open(pathname)

    # Load the first page of the ballot image and get the image info from it
    page = pdf_image.load_page(0)
    image_list = page.get_images(full=True)

    # Ensure that we have a single image
    if len(image_list) != 1:
        return False

    # Use the image and get the xref for it
    image = image_list[0]
    xref = image[0]

    # Extract the image from the PDF
    base_image = pdf_image.extract_image(xref)

    # Get the PNG bytes from the image
    image_bytes = base_image["image"]

    # Convert the PNG bytes to an Image object
    png_buffer = io.BytesIO(image_bytes)
    image_png = Image.open(png_buffer)
    image_png = image_png.convert("RGBA")

    # Create a new background image
    background = Image.new("RGB", image_png.size, (255, 255, 255))

    # Paste the PNG onto the background, using its alpha channel as mask
    background.paste(image_png, mask=image_png.split()[3])  # 3 is the alpha channel

    # Save the background in JPEG format to the JPEG buffer
    jpg_buffer = io.BytesIO()
    background.save(jpg_buffer, "JPEG", quality=100)
    jpg_buffer.seek(0)

    # Open the JPEG buffer as an Image object
    image_file_jpg = Image.open(jpg_buffer)

    # Parse the barcodes - ignoring any exceptions generated
    try:
        barcodes = decode(image_file_jpg)
    except Exception:
        pass

    # Process each detected barcode
    barcode_values = []
    master_barcode = ''
    for barcode in barcodes:

        # Extract barcode data and type
        barcode_data = barcode.data.decode("utf-8")
        barcode_type = barcode.type

        # Add the barcode if it is CODE128 to the front of the list
        if barcode_type == 'CODE128':
            barcode_data_len = len(barcode_data)
            if barcode_data_len == 6:
                barcode_values.insert(0, int(barcode_data))
            elif barcode_data_len == 28:
                master_barcode = barcode_data
        else:
            raise ValueError(f"Uknown barcode type {barcode_type}")

    ballot = {}
    ballot['Pathname'] = pathname
    ballot['Basename'] = os.path.basename(pathname)
    ballot['BallotStyle'] = ballot['Basename'][:5]
    ballot['MasterBarcode'] = master_barcode
    ballot['PrecinctId'] = int(master_barcode[:10])
    ballot['BallotStyleId'] = int(master_barcode[10:20])
    ballot['NumWriteInVotes'] = int(master_barcode[21:23])
    ballot['NumVoteSelections'] = int(master_barcode[24:26])
    ballot['JudgeInitialStatus'] = int(master_barcode[26])
    ballot['ReviewBoxStatus'] = int(master_barcode[27])
    ballot['Barcodes'] = barcode_values
    BALLOTS.append(ballot)

    print(f"{ballot['BallotStyle']} {master_barcode} {ballot['PrecinctId']} {ballot['BallotStyleId']} {ballot['NumVoteSelections']} {barcode_values}")

    return True


#-----------------------------------------------------------------------------
# analyze_files()
#
# This function analyzes all of the files in the specified directory and
# subdirectories.
#-----------------------------------------------------------------------------
def analyze_files(dirname):
    """Analyze files in the specified directory"""

    for dirpath, dirnames, filenames in os.walk(dirname):
        for image_filename in filenames:
            if re.search(r'.pdf', image_filename, flags=re.IGNORECASE):

                # Create the pathname to the ballot image and CVR
                image_pathname = os.path.join(dirpath, image_filename)

                obtain_barcodes_from_ballot_image(image_pathname)


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

        # Get the arguments
    test_deck_ballot_dir = sys.argv[1]

    # Analyze the files in the current directory and subdirectories
    analyze_files(test_deck_ballot_dir)

    # Save the ballot data to disk
    db = shelve.open('ess_p26_ballots.dat')
    db['Version'] = ESS_BALLOT_DATA_VERSION
    db['Ballots'] = BALLOTS
    db.close()


main()
