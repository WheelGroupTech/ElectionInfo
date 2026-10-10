# Election Explorer releases

Microsoft Store releases, newest first. Each entry's text is the Store listing's
"What's new in this version" note and can be pasted into Partner Center as is.

## 1.3.0.0 (submitted 2026-10-10)

New in 1.3: Load Hart InterCivic PDF CVR Reports, on their own or together with the Hart ZIP export (adding each ballot's polling place, voting type, and scanning device), including scanned, image-only reports read with the OCR built into Windows. Load Dominion (Liberty Vote) Democracy Suite CVR exports, with round-by-round ranked-choice (instant runoff) tabulation. New polling place, voting device, and voting type reports. Voter lists: support for the El Paso County, Texas, export layout, plus more complete addresses (house-number suffixes, trailing street directions, and apartment numbers). New Help topics explain loading voter lists and cast vote records.

## 1.2.0.0 (submitted 2026-10-02)

New in 1.2: Load Hart InterCivic cast vote records from the CVR ZIP export, including multi-gigabyte files. Primary elections are split into each party's contests, and you choose which party is listed first in the tabulation. Tabulate only the ballots that match your current filter. Hart contests are listed in natural order (District 6 before District 26). Loading now shows a running count of ballot records and a progress bar that moves steadily from start to finish. Voter lists: addresses in the Texas Secretary of State list format keep their city, state, and ZIP code; "State ID" columns are recognized as the Voter ID; very wide lists, such as those with hundreds of voter-history columns, can be scrolled and sorted all the way to the last column; and confidential (masked) addresses no longer show up as differences when comparing two lists.

## 1.1.0.0 (built 2026-09-20)

New in 1.1: Open Excel (.xlsx) voter lists, with a sheet picker for workbooks that have several sheets, and dates and leading zeros kept as they appear in Excel. Explore ES&S cast vote records from Excel, CSV, or TSV files, combining several files from one election into a single view: browse and filter every ballot record, tabulate every contest (with write-ins, overvotes, and undervotes, and multi-seat races summed correctly), optionally merge the different kinds of write-in, detect ballots that span several cards, and run batch, precinct, and ballot-style reports. Export voter lists, ballot records, and reports to CSV or TSV. New Help topics cover exporting and cast vote records. Fixed: district-code columns are no longer added to voter addresses.

## 1.0.1.0 (initial release, September 2026)

Initial release. Open the large voter-registration lists that counties publish, as CSV or tab-delimited text, even with millions of voters and hundreds of columns, in a fast two-pane grid that keeps Voter ID, Precinct, Name, and Address pinned in place while you scroll. Addresses and names are normalized to a consistent format. Sort by any column, narrow the list with rule-based Include/Exclude filters, find duplicate Voter IDs or duplicate voters who share a name and date of birth, and see voters per precinct or per address. Compare two versions of a list by Voter ID to see who was added, removed, or changed, with name and address edits graded as minor or major, precinct changes flagged, and a side-by-side Differences view. Copy rows to the clipboard, show any address in Google, Bing, Apple, or OpenStreetMap, adjust the zoom, and keep your preferences between sessions. Everything runs locally, with no accounts, ads, or telemetry, on both x64 and ARM64 PCs.
