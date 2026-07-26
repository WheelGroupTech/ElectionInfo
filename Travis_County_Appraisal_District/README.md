# Travis County Appraisal District

Python utilities for extracting property address and property-type information
from Travis County Appraisal District (TCAD) certified appraisal export files.

## Purpose

The primary goal of `parse_appraisal_data.py` is to produce a list of property
addresses and property-type descriptions for Travis County that can be used to
help verify the registered voter list.

## Source Data

Public appraisal export data is available from the Travis County Appraisal
District public information page:

- [https://traviscad.org/publicinformation](https://traviscad.org/publicinformation)

### Example Certified Appraisal Export

An example certified appraisal export zip file:

- [2026 Certified Appraisal Export Supp 0](https://traviscad.org/wp-content/largefiles/2026%20Certified%20Appraisal%20Export%20Supp%200_07182026.zip)

Extract the zip file and run the parse script against the directory that
contains the resulting fixed-width text files (or run the script from inside
that directory and pass `.` as the input path).

### Export Layout Documentation

The layout of the exported fixed-width files is documented in an Excel workbook
included in:

- [Website_Legacy8.0.33-AppraisalExportLayout](https://traviscad.org/wp-content/largefiles/Website_Legacy8.0.33-AppraisalExportLayout_06182026.zip)

This project uses fields from:

| Export file   | Layout worksheet | Purpose |
|---------------|------------------|---------|
| `PROP.TXT`    | Property         | Property ID and situs/location address fields |
| `IMP_INFO.TXT`| Improvement / Information | Improvement type code, description, and state code |

## Script

### `parse_appraisal_data.py`

Reads `PROP.TXT` and `IMP_INFO.TXT` from an input directory and joins them on
`prop_id`.

**Inputs (required in the export directory):**

- `PROP.TXT`
- `IMP_INFO.TXT`

**Outputs:**

1. `travis_properties.csv`  
   One row per property, sorted by `prop_id` ascending.

   Columns:

   - `prop_id`
   - `situs_num`
   - `situs_street_prefx`
   - `situs_street`
   - `situs_street_suffix`
   - `situs_unit`
   - `situs_city`
   - `situs_zip`
   - `improv_type_cd`
   - `improv_type_desc`
   - `imprv_state_cd`

   When a property has multiple improvement types, codes/descriptions/state
   codes are joined with `;`.

2. `travis_improv_types.csv`  
   One row per unique improvement type combination found in the properties
   output.

   Columns:

   - `improv_type_cd`
   - `improv_type_desc`
   - `imprv_state_cd`
   - `count`

   If the same `improv_type_cd` / `improv_type_desc` pair appears with more than
   one `imprv_state_cd`, each combination is written as a separate row and a
   message is printed to the console.

### Address city fallback

If `situs_city` is blank on a property record, the script attempts to fill it
from the property-year owner address or the January 1 owner address when the
owner street appears to match the situs street.

### Usage

```bash
python parse_appraisal_data.py /path/to/export_dir
```

Optional custom properties CSV path:

```bash
python parse_appraisal_data.py /path/to/export_dir /path/to/travis_properties.csv
```

`travis_improv_types.csv` is always written to the input export directory.

Example when the current working directory is the extracted export folder:

```bash
python /path/to/ElectionInfo/TravisCountyAppraisalDistrict/parse_appraisal_data.py .
```

### Requirements

- Python 3
- Standard library only (`csv`, `os`, `sys`)

No third-party packages are required to run the parser.

## Notes

- Export files are fixed-width mainframe-style text files.
- Field positions are 1-based inclusive, matching the TCAD layout workbook.
- Large exports can contain hundreds of thousands of property records; allow
  sufficient time and disk space for CSV generation.
