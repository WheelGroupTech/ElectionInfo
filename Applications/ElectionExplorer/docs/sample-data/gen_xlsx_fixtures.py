#!/usr/bin/env python3
"""Generate a SYNTHETIC .xlsx fixture for testing ElectionExplorer's XLSX import.

All data is fabricated. The workbook deliberately exercises the tricky cases the
importer must handle (see docs/xlsx-import-design.md):

  * shared strings (the default for text in openpyxl)
  * a numeric Voter ID stored as an integer  -> would show scientific/no-leading-
    zero until Phase 3 style handling
  * a ZIP stored as TEXT with a leading zero  ("07001") vs one stored as a number
  * a date-formatted DOB (stored as a serial + date format)
  * blank / gapped cells inside a row
  * a boolean cell (t="b")  -> importer renders TRUE/FALSE
  * multiple worksheets (to exercise the sheet picker)

Note: openpyxl always writes text as shared strings and cannot emit a genuine
cached error cell (t="e") or inline strings (t="inlineStr"); those two cases are
covered by the C round-trip test that authors a minimal .xlsx with miniz.

Usage (needs openpyxl:  pip install openpyxl):
    python gen_xlsx_fixtures.py [out.xlsx]
"""
import datetime
import sys

try:
    from openpyxl import Workbook
    from openpyxl.styles import numbers
except ImportError:
    sys.exit("openpyxl not installed. Run: pip install openpyxl")

out_path = sys.argv[1] if len(sys.argv) > 1 else "election_sample.xlsx"

wb = Workbook()

# --- Sheet 1: Voters (the main list) --------------------------------------
ws = wb.active
ws.title = "Voters"
ws.append(["VUID", "LSTNAM", "FSTNAM", "Residential Address", "Precinct",
           "DOB", "ZIP"])

# A numeric Voter ID (int), a text ZIP with a leading zero, a real date DOB.
rows = [
    (2128393968, "Abagaro", "Mosisa", "1109 N IH 35 NB Austin TX 78702", "P 100",
     datetime.date(1979, 4, 12), "78702"),
    (1004390230, "Smith", "John", "123 Main St Austin TX 78701", "P 101",
     datetime.date(1962, 11, 3), "78701"),
    # ZIP as TEXT preserving a leading zero (Excel would drop it if numeric):
    (1017128762, "Jones", "Maria", "456 Oak Ave Round Rock TX 78664", "P 205",
     datetime.date(1990, 7, 20), "07001"),
    # A row with gaps: no Precinct, no ZIP (blank cells inside the row).
    (1058475118, "Garcia", "Carlos", "789 Congress Ave Austin TX 78701", None,
     datetime.date(1985, 1, 30), None),
]
for r in rows:
    ws.append(r)

# Force the DOB column (F) to a date number format so it stores as a serial.
for row in ws.iter_rows(min_row=2, min_col=6, max_col=6):
    for cell in row:
        cell.number_format = "yyyy-mm-dd"
# Keep the ZIP column (G) as text so leading zeros survive.
for row in ws.iter_rows(min_row=2, min_col=7, max_col=7):
    for cell in row:
        cell.number_format = numbers.FORMAT_TEXT

# --- Sheet 2: Roster (a second sheet for the picker) ----------------------
ws2 = wb.create_sheet("Roster")
ws2.append(["Voter ID", "Name", "Checked In"])
ws2.append([2128393968, "Abagaro, Mosisa", True])   # boolean cell
ws2.append([1004390230, "Smith, John", False])
ws2.append([1017128762, "Jones, Maria", True])

# --- Sheet 3: EdgeCases ---------------------------------------------------
ws3 = wb.create_sheet("EdgeCases")
ws3.append(["Note", "Value"])
ws3.append(["unicode", "José Ávila – café"])   # non-ASCII + en dash
ws3.append(["ampersand & <angle>", "a<b>&c\"d'e"])  # entities to unescape
ws3.append(["empty next", None])

wb.save(out_path)
print("wrote", out_path, "with sheets:", wb.sheetnames)
