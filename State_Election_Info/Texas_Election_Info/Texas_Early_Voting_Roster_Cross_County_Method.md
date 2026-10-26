# Texas Early Voting Rosters: Method for Finding VUIDs on Two Counties' Rosters

This note describes how the early voting roster reviews (`Texas_Early_Voting_Roster_Review_<date>.md`)
compare each county's rosters with every other county's rosters and with the Texas Secretary of State
(SOS) files, to find Voter Unique Identifiers (VUIDs) that appear in two counties. The results are
reported in sections 6.1, 6.7 and 6.8 of each review, and the records are listed in its supplement.

## 1. Load everything into one layout

- **County files:** each county's own roster files for the review period load through Election
  Explorer's roster loader. This includes every daily file, or the latest copy of a cumulative list.
  The loader turns each file into the same columns: VUID, precinct, last, first and middle name,
  voting method, date voted and source file.
- **State files:** each county's SOS daily CSVs load the same way, kept separate from the county's
  own files.

## 2. Keep only real VUIDs

A record takes part only if its ID is exactly 10 digits. These are left out:

- blank IDs, such as Harris's mail rows with no ID and Jefferson's masked "**********";
- Tarrant's redacted rows;
- county-internal 6-9 or 11-digit numbers, which the reviews report separately (§6.11).

## 3. Build one index across all county rosters

The index is a single table: VUID → county → the first record in that county. Each county appears at
most once per VUID, because repeats within one county are separate checks (§6.10 duplicates, §6.7
within-county).

## 4. Flag VUIDs in more than one county

Every VUID listed under two or more counties is a candidate, whatever the methods: in person in both,
mail in both, or mail in one and in person in the other.

## 5. Same person or two people?

- The two records' name words are compared: first, middle and last name, upper-cased, letters only.
- If the two records share at least one name word, it is treated as the same voter in two counties
  (§6.8).
- If they share none, it is one VUID used for two different people (§6.7).

## 6. Add when, how and where from each county's record

- **Dated lists:** the date comes straight from the file.
- **Undated cumulative lists** (Tarrant, Hidalgo, Denton, El Paso, Potter): the date is the first daily
  snapshot of the list that contains the VUID. This is how a review states, for example, "in El
  Paso's list from October 21".
- **Method and precinct:** taken from each county's record.

## 7. Check what the state shows

- The same index is built over all 50 counties' SOS files, and each flagged VUID is looked up there:
  which county's state file lists it, on what date, with what precinct.
- In every in-person case so far, the second county's state file carries the first county's vote,
  with the first county's precinct.

## 8. Related checks on the same two indexes

- **No VUID is in two state county files.** The state never lists one voter under two counties: none
  found through October 24.
- **State file holds another county's voter (§6.1).** County X's state file lists a VUID that is not on
  X's own roster but is on county Y's, with the same first name.
- **State file vs county roster, different people (§6.7).** A VUID in a state file names someone who
  shares no name word with the person under that VUID on a county roster, in the same county or
  another.

## 9. Locate each record for the supplement

For every flagged VUID, the supplement records where both records sit in the raw files: file name, ZIP
entry, and line, worksheet row or PDF page. County staff can then find them. Flagged cases were
spot-checked against the original files: the PDF pages and the workbook rows.

## Limits

- **Only counties posting their own readable rosters can be checked roster to roster:** 33 of the 50.
  The 16 portal-only counties appear only through the state files (§8 above), and Guadalupe's scanned
  mail lists cannot be read.
- **Records without a real 10-digit VUID cannot be matched:** blank, redacted or county-internal IDs. A
  few of the internal IDs line up by name and precinct with a state VUID, but that is an inference,
  not a match.
- **McLennan's list has no method or date,** so its matches show only that the voter appears.
- **Most counties are cross-checked only for the review period:** October 14 onward for the state
  files, plus each county's own mail lists back to late September. A voter whose second record falls
  outside that window will not be caught.
- **"Same person" rests on names only.** A shared common name word can make two different people look
  like one, and name changes or typos can make one person look like two. Only the counties can
  confirm.
- **Counties revise files,** so a pair can appear or disappear between reviews.
