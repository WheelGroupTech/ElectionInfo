# Hays County November 2026 General Election: Cast Vote Records Compared with the Official Results

**Election:** November 3, 2026 General Election (G26)\
**County:** Hays County, Texas (Hart InterCivic Verity voting system)\
**Data:**

- The official canvass results reports, run November 9, 2026: the Cumulative Results Report (5:35 PM) and the Precinct Results Report (5:37 PM).
- The county's cast vote record (CVR) export, `G26 CVR.zip`.

**Date:** November 10, 2026

This review compares counts only. A cast vote record holds no voter identity, and none is reported here.

## 1. Summary

**The cast vote records match the official results.** Every count reconciles, once overvotes and write-ins are handled as the canvass handles them.

| Check | Official results | Cast vote records | Result |
|---|---:|---:|---|
| Ballots cast | 120,880 | 120,880 | Match |
| Ballots by precinct (85 precincts with ballots) | 119,658 | 119,658 | All 85 match |
| Limited ballots | 1,132 | 1,132 | Match |
| Federal overseas ballots | 90 | 90 | Match |
| Votes for ballot candidates and propositions | 177 choices | 177 choices | 170 equal; 7 differ by 1–2 votes (write-ins) |
| Overvotes, by contest | 91 contests | 91 contests | All 91 match |
| Undervotes, by contest | 91 contests | 91 contests | 86 match; 5 differ by the uncounted write-ins |

- **The only differences come from write-ins,** in the five contests that had write-in candidates:
  - **What the CVR records:** each write-in as a mark, with no name.
  - **What the canvass did with each mark:** counted it for a certified write-in candidate, credited it to a candidate already on the ballot, or treated it as an undervote.
  - **The arithmetic is exact:** in all five contests, the official undervotes equal the CVR undervotes plus the write-in marks not counted for a candidate (section 5).
- **No ballots are missing or extra:** none in the county as a whole, and none in any precinct.
- **One trap for anyone tallying this CVR format:** the record of an overvoted contest still lists the candidates the voter marked. Counting them as votes overstates both candidates (section 6).

## 2. Data and method

**Official results:** Hays County posted its unofficial results reports on its Document Center. After the canvass it replaced them, at the same links, with the official reports used here:

- `G26_Cumulative Results 20261103 General - official_202611091749348875.pdf`: county totals, by contest and choice, with absentee (mail), early voting and Election Day columns, undervotes and overvotes.
  - **Turnout:** 120,880 ballots cast of 204,327 registered voters (59.16%).
- `G26_Precinct Results 20261103 General - official_202611091750062784.pdf`: the same for each of 90 reporting units.
  - **Ballots:** each precinct's ballots cast and registered voters.
  - **The 90 units:** 88 precincts, plus "Limited Ballot" and "Federal Overseas".

**Cast vote records:** `G26 CVR.zip` (376 MB) holds:

- **Ballot sheets:** 124,378 Hart CVR XML files, one per ballot sheet.
  - **Sheet 1:** 120,880 files, one for every ballot.
  - **Sheet 2:** 3,498 files, the second sheet of a two-sheet ballot.
- **What each file records:**
  - the precinct split (for example `109BS01`, or `Limited BallotState Only`);
  - the batch number (128 batches in all);
  - every contest on the sheet, with the options marked, undervotes, and an `<Overvoted />` flag.
- **Write-ins:** 1,221 write-in images (PNG), one for each write-in mark.
- **Blank sheets:** 508 sheets are marked blank, 5 first sheets and 503 second sheets.

**How the comparison was made:**

- **Election Explorer:** it read both official reports and the CVR. It counts 120,880 ballots in the CVR, one per first sheet.
- **An independent tally:** a separate script read every CVR file and counted, by contest and option:
  - votes;
  - undervotes;
  - overvoted contests;
  - write-in marks;
  - ballots by precinct split.
- **Matching:** each official choice total, overvote count and undervote count was compared with the CVR tally. Each precinct's ballots cast was compared with the CVR's first sheets in that precinct.
- **Precincts:** a CVR precinct is the first three digits of the split name; `109`, `109BS01` and `109BS02` are all precinct 109.

## 3. Ballots

- **The county:** 120,880 ballots in both sources.
- **By precinct:** every one of the 88 precincts matches. 85 have ballots; precincts 200, 400 and 453 report zero ballots and have no CVR records.
- **Limited and overseas ballots:** the CVR's limited-ballot and federal-overseas splits match the report's two separate units.

  | Reporting unit | Official ballots | CVR ballots | CVR precinct splits |
  |---|---:|---:|---|
  | Limited Ballot | 1,132 | 1,132 | "Limited BallotState Only" 1,020; "Limited BallotUS 27" 59; "Limited BallotUS 21" 53 |
  | Federal Overseas | 90 | 90 | "Federal Overseas-US 21" 59; "Federal Overseas-US 27" 31 |

The full precinct table is in Appendix B.

## 4. Votes by contest

All 91 contests in the official reports appear in the CVR.

- **170 of the 177 choices that received votes match exactly.** Every choice in 86 of the 91 contests matches exactly: every contest without write-in candidates, including all propositions.
- **The other 7 choices differ by 1 or 2 votes,** all in contests with write-in candidates. The official count is higher each time:

  | Contest | Candidate | Official | CVR | Difference |
  |---|---|---:|---:|---:|
  | U.S. Senator | James Talarico | 78,094 | 78,092 | +2 |
  | U.S. Representative, District 27 | Tanya Lloyd | 41,458 | 41,457 | +1 |
  | Governor | Greg Abbott | 43,094 | 43,093 | +1 |
  | Governor | Gina Hinojosa | 75,457 | 75,455 | +2 |
  | Lieutenant Governor | Dan Patrick | 41,894 | 41,893 | +1 |
  | Lieutenant Governor | Vikki Goodwin | 73,799 | 73,798 | +1 |
  | City of Woodcreek, City Council at-Large | Jeff Rasco | 700 | 699 | +1 |

  These are write-ins naming a candidate who was already on the ballot, which the canvass credits to that candidate.
- **The remaining official choices are not in the CVR by name:**
  - **Certified write-in candidates:** the CVR records write-ins without names (section 5).
  - **Choices with zero votes:** for example, the "Against" side of the La Cima and Sunfield district propositions, and the two City of Buda races reported with 0 votes (Mayor, and City Council at-Large Position 2).

## 5. Write-ins

**The CVR records a write-in as a mark only.** The write-in name exists only in the scanned image. At the canvass, each write-in mark became one of three things:

1. **A vote for a certified write-in candidate.**
2. **A vote for a candidate already on the ballot,** when the voter wrote in that candidate's name.
3. **An undervote:** a name that is not a certified candidate, or a blank or unreadable entry.

The CVR undervotes do not include write-in marks, so the official undervotes should equal the CVR undervotes plus the marks counted for no candidate (type 3). In all five contests they do:

| Contest | Write-in marks (CVR) | Certified write-in votes (official) | Credited to a ballot candidate | Counted as undervotes | CVR undervotes | Official undervotes |
|---|---:|---:|---:|---:|---:|---:|
| U.S. Senator | 187 | 63 | 2 | 122 | 568 | 690 = 568 + 122 |
| U.S. Representative, District 27 | 27 | 3 | 1 | 23 | 841 | 864 = 841 + 23 |
| Governor | 98 | 46 | 3 | 49 | 311 | 360 = 311 + 49 |
| Lieutenant Governor | 39 | 3 | 2 | 34 | 1,193 | 1,227 = 1,193 + 34 |
| City of Woodcreek, City Council at-Large (vote for 3) | 870 | 822 | 1 | 47 | 1,752 | 1,799 = 1,752 + 47 |
| **Total** | **1,221** | **937** | **9** | **275** | | |

- **Certified write-in votes:**
  - **U.S. Senator:** Ayers 24, Hall 14, Simmons 25, Mohamed 0.
  - **District 27:** Raasch 3.
  - **Governor:** Mack 27, Miller 17, Majoishingra 2, Giles 0.
  - **Lieutenant Governor:** Espinosa 2, Samuelson 1.
  - **Woodcreek:** Burnett 238, Brizendine 186, Savage 247, Lingren 151.
- **The images match the marks:** the 1,221 write-in images equal the 1,221 write-in marks.
- **Not checked:** the canvass's reading of each image (which certified candidate, or none) cannot be verified from the CVR. It would need the images reviewed.

## 6. Overvotes

The official overvotes match the CVR in all 91 contests. 23 contests had overvotes; the largest counts were:

| Contest | Overvotes |
|---|---:|
| State Representative, District 73 | 84 |
| State Senator, District 21 | 69 |
| Member, State Board of Education, District 5 | 46 |
| Chief Justice, 3rd Court of Appeals District | 24 |
| County Judge | 15 |
| Comptroller of Public Accounts | 6 |
| U.S. Senator | 5 |

**A caution for tallying this format:**

- **What the record holds:** when a contest is overvoted, the Hart CVR XML still lists every option the voter marked, beside the `<Overvoted />` flag.
- **The effect of a naive count:** counting those options as votes adds the overvotes to each marked candidate. For State Senate District 21, each candidate would gain 69 votes.
- **The correct handling:** the official results count no vote in an overvoted contest. Neither does Election Explorer, which records such a contest as "overvote". This review does the same.

## 7. Limitations

- **Voting method:** the CVR does not record whether a ballot was cast by mail, early in person or on Election Day. The official split of each contest into absentee, early voting and Election Day votes therefore cannot be checked against the CVR. Only the totals can.
- **Write-in adjudication:** see section 5.
- **Precinct contest totals:** precinct-level ballots were compared, but not every contest's precinct-level votes. The county totals and the precinct ballot counts already match exactly.
- **Scope:** this review compares the CVR with the reported results. It does not examine the paper ballots, ballot images other than write-ins, the tabulation equipment, or the poll books.

## Appendix A. All contests

† marks the five contests with write-in candidates (section 5).

- **Votes (official):** the contest's total votes, including certified write-ins.
- **Votes (CVR):** every option marked outside overvoted contests, including write-in marks.

| # | Contest | Votes (official) | Votes (CVR) | Write-in marks (CVR) | Overvotes (official / CVR) | Undervotes (official / CVR) |
|--:|----------------------------------------|-------:|-------:|-----:|-------:|---------:|
| 1 | U.S. Senator † | 120,185 | 120,307 | 187 | 5 / 5 | 690 / 568 |
| 2 | U.S. Representative, District 21 | 53,929 | 53,929 |  | 1 / 1 | 750 / 750 |
| 3 | U.S. Representative, District 27 † | 64,316 | 64,339 | 27 | 0 / 0 | 864 / 841 |
| 4 | Governor † | 120,427 | 120,476 | 98 | 3 / 3 | 360 / 311 |
| 5 | Lieutenant Governor † | 119,561 | 119,595 | 39 | 2 / 2 | 1,227 / 1,193 |
| 6 | Attorney General | 119,259 | 119,259 |  | 2 / 2 | 1,529 / 1,529 |
| 7 | Comptroller of Public Accounts | 118,873 | 118,873 |  | 6 / 6 | 1,911 / 1,911 |
| 8 | Commissioner of the General Land Office | 118,564 | 118,564 |  | 2 / 2 | 2,224 / 2,224 |
| 9 | Commissioner of Agriculture | 118,516 | 118,516 |  | 1 / 1 | 2,273 / 2,273 |
| 10 | Railroad Commissioner | 118,152 | 118,152 |  | 1 / 1 | 2,637 / 2,637 |
| 11 | Chief Justice, Supreme Court of Texas | 118,686 | 118,686 |  | 1 / 1 | 2,103 / 2,103 |
| 12 | Justice, Supreme Court of Texas, Place 2 - Unexpired Term | 118,287 | 118,287 |  | 0 / 0 | 2,503 / 2,503 |
| 13 | Justice, Supreme Court of Texas, Place 7 | 118,180 | 118,180 |  | 1 / 1 | 2,609 / 2,609 |
| 14 | Justice, Supreme Court of Texas, Place 8 | 118,058 | 118,058 |  | 0 / 0 | 2,732 / 2,732 |
| 15 | Judge, Court of Criminal Appeals, Place 3 | 117,671 | 117,671 |  | 0 / 0 | 3,119 / 3,119 |
| 16 | Judge, Court of Criminal Appeals, Place 4 | 117,534 | 117,534 |  | 1 / 1 | 3,255 / 3,255 |
| 17 | Judge, Court of Criminal Appeals, Place 9 | 117,448 | 117,448 |  | 0 / 0 | 3,342 / 3,342 |
| 18 | Member, State Board of Education, District 5 | 116,792 | 116,792 |  | 46 / 46 | 2,932 / 2,932 |
| 19 | State Senator, District 21 | 71,452 | 71,452 |  | 69 / 69 | 1,643 / 1,643 |
| 20 | State Representative, District 45 | 91,918 | 91,918 |  | 0 / 0 | 2,094 / 2,094 |
| 21 | State Representative, District 73 | 25,095 | 25,095 |  | 84 / 84 | 579 / 579 |
| 22 | Chief Justice, 15th Court of Appeals District | 117,269 | 117,269 |  | 0 / 0 | 3,521 / 3,521 |
| 23 | Justice, 15th Court of Appeals District, Place 2 | 117,048 | 117,048 |  | 1 / 1 | 3,741 / 3,741 |
| 24 | Justice, 15th Court of Appeals District, Place 3 | 116,876 | 116,876 |  | 0 / 0 | 3,914 / 3,914 |
| 25 | Chief Justice, 3rd Court of Appeals District | 115,828 | 115,828 |  | 24 / 24 | 3,918 / 3,918 |
| 26 | District Judge, 207th District | 65,429 | 65,429 |  | 0 / 0 | 54,341 / 54,341 |
| 27 | District Judge, 274th District | 64,454 | 64,454 |  | 0 / 0 | 55,316 / 55,316 |
| 28 | District Judge, 428th District | 115,510 | 115,510 |  | 3 / 3 | 4,145 / 4,145 |
| 29 | Criminal District Attorney | 106,714 | 106,714 |  | 3 / 3 | 12,941 / 12,941 |
| 30 | County Judge | 117,172 | 117,172 |  | 15 / 15 | 2,471 / 2,471 |
| 31 | Judge, County Court-at-Law #1 | 82,694 | 82,694 |  | 0 / 0 | 36,964 / 36,964 |
| 32 | Judge, County Court-at-Law #2 | 115,547 | 115,547 |  | 1 / 1 | 4,110 / 4,110 |
| 33 | District Clerk | 82,167 | 82,167 |  | 0 / 0 | 37,491 / 37,491 |
| 34 | County Clerk | 81,582 | 81,582 |  | 0 / 0 | 38,076 / 38,076 |
| 35 | County Treasurer | 81,442 | 81,442 |  | 0 / 0 | 38,216 / 38,216 |
| 36 | County Commissioner, Precinct 2 | 26,219 | 26,219 |  | 0 / 0 | 684 / 684 |
| 37 | County Commissioner, Precinct 4 | 34,008 | 34,008 |  | 0 / 0 | 857 / 857 |
| 38 | Justice of the Peace, Precinct 1, Place 2 | 25,958 | 25,958 |  | 0 / 0 | 7,682 / 7,682 |
| 39 | Justice of the Peace, Precinct 2, Place 1 | 26,801 | 26,801 |  | 0 / 0 | 768 / 768 |
| 40 | Justice of the Peace, Precinct 3 | 6,636 | 6,636 |  | 0 / 0 | 3,907 / 3,907 |
| 41 | Justice of the Peace, Precinct 4 | 16,252 | 16,252 |  | 0 / 0 | 9,349 / 9,349 |
| 42 | Justice of the Peace, Precinct 5 | 16,215 | 16,215 |  | 0 / 0 | 6,089 / 6,089 |
| 43 | City of Austin, City Council, District 8 | 258 | 258 |  | 0 / 0 | 105 / 105 |
| 44 | City of Austin Proposition A | 346 | 346 |  | 0 / 0 | 17 / 17 |
| 45 | City of Austin Proposition B | 345 | 345 |  | 0 / 0 | 18 / 18 |
| 46 | City of Austin Proposition C | 299 | 299 |  | 0 / 0 | 64 / 64 |
| 47 | City of Austin Proposition D | 306 | 306 |  | 0 / 0 | 57 / 57 |
| 48 | City of Austin Proposition E | 294 | 294 |  | 0 / 0 | 69 / 69 |
| 49 | City of Austin Proposition F | 285 | 285 |  | 0 / 0 | 78 / 78 |
| 50 | City of Austin Proposition G | 275 | 275 |  | 0 / 0 | 88 / 88 |
| 51 | City of Austin Proposition H | 285 | 285 |  | 0 / 0 | 78 / 78 |
| 52 | City of Austin Proposition I | 300 | 300 |  | 0 / 0 | 63 / 63 |
| 53 | City of Austin Proposition J | 278 | 278 |  | 0 / 0 | 85 / 85 |
| 54 | City of Buda, City Council, Single Member District B | 1,218 | 1,218 |  | 0 / 0 | 409 / 409 |
| 55 | City of Buda, Mayor | 0 | 0 |  | 0 / 0 | 0 / 0 |
| 56 | City of Buda, City Council, at-Large Position 2 | 0 | 0 |  | 0 / 0 | 0 / 0 |
| 57 | City of Buda, City Council, at-Large Position 1 (unexpired term) | 5,072 | 5,072 |  | 1 / 1 | 2,043 / 2,043 |
| 58 | City of Kyle, Mayor | 16,725 | 16,725 |  | 0 / 0 | 2,315 / 2,315 |
| 59 | City of Kyle, City Council, District 2 | 5,089 | 5,089 |  | 0 / 0 | 1,100 / 1,100 |
| 60 | City of Kyle, City Council, District 4 | 5,803 | 5,803 |  | 0 / 0 | 1,763 / 1,763 |
| 61 | City of San Marcos, Mayor | 25,673 | 25,673 |  | 0 / 0 | 3,066 / 3,066 |
| 62 | City of San Marcos, City Council, Place 3 | 24,253 | 24,253 |  | 0 / 0 | 4,486 / 4,486 |
| 63 | City of San Marcos, City Council, Place 4 | 23,095 | 23,095 |  | 0 / 0 | 5,644 / 5,644 |
| 64 | City of San Marcos, City Council, Place 6 | 23,391 | 23,391 |  | 0 / 0 | 5,348 / 5,348 |
| 65 | City of Woodcreek, City Council at-Large, Three Council Members † | 1,522 | 1,569 | 870 | 0 / 0 | 1,799 / 1,752 |
| 66 | Village of Bear Creek, Mayor | 189 | 189 |  | 0 / 0 | 34 / 34 |
| 67 | Village of Bear Creek, City Commissioner | 292 | 292 |  | 0 / 0 | 154 / 154 |
| 68 | Austin Community College, Trustee, Place 1 | 37,004 | 37,004 |  | 0 / 0 | 17,765 / 17,765 |
| 69 | Austin Community College, Trustee, Place 2 | 31,337 | 31,337 |  | 0 / 0 | 23,432 / 23,432 |
| 70 | Austin Community College, Trustee, Place 3 | 33,297 | 33,297 |  | 0 / 0 | 21,472 / 21,472 |
| 71 | Comal Independent School District Proposition A | 111 | 111 |  | 0 / 0 | 5 / 5 |
| 72 | Comal Independent School District Proposition B | 110 | 110 |  | 0 / 0 | 6 / 6 |
| 73 | Crystal Clear Special Utility District Board of Directors | 349 | 349 |  | 1 / 1 | 433 / 433 |
| 74 | Hays Trinity Groundwater Conservation District Board Director for Single Member District #2 | 4,389 | 4,389 |  | 0 / 0 | 3,171 / 3,171 |
| 75 | Hays Trinity Groundwater Conservation District Board Director for Single Member District #3 | 4,428 | 4,428 |  | 0 / 0 | 2,345 / 2,345 |
| 76 | Hays Trinity Groundwater Conservation District Board Director for Single Member District #4 | 4,478 | 4,478 |  | 0 / 0 | 1,519 / 1,519 |
| 77 | La Cima Municipal Management District No. 1-A Proposition A | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 78 | La Cima Municipal Management District No. 1-A Directors | 5 | 5 |  | 0 / 0 | 0 / 0 |
| 79 | La Cima Municipal Management District No. 1-A Proposition B | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 80 | La Cima Municipal Management District No. 1-A Proposition C | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 81 | La Cima Municipal Management District No. 1-A Proposition D | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 82 | La Cima Municipal Management District No. 1-A Proposition E | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 83 | La Cima Municipal Management District No. 1-A Proposition F | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 84 | La Cima Municipal Management District No. 1-A Proposition G | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 85 | La Cima Municipal Management District No. 1-A Proposition H | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 86 | Sunfield Municipal Utility District No. 2 Proposition A | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 87 | Sunfield Municipal Utility District No. 2 Proposition B | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 88 | Sunfield Municipal Utility District No. 2 Proposition C | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 89 | Sunfield Municipal Utility District No. 2 Proposition D | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 90 | Sunfield Municipal Utility District No. 2 Proposition E | 1 | 1 |  | 0 / 0 | 0 / 0 |
| 91 | Sunfield Municipal Utility District No. 2 Proposition F | 1 | 1 |  | 0 / 0 | 0 / 0 |

## Appendix B. Ballots by precinct

| Precinct | Registered | Ballots (official) | Ballots (CVR) | | Precinct | Registered | Ballots (official) | Ballots (CVR) |
|---|---:|---:|---:|---|---|---:|---:|---:|
| 109 | 1,657 | 895 | 895 | | 333 | 2,989 | 2,080 | 2,080 |
| 110 | 3,914 | 2,014 | 2,014 | | 335 | 3,661 | 2,452 | 2,452 |
| 112 | 2,633 | 1,259 | 1,259 | | 336 | 4,073 | 2,638 | 2,638 |
| 113 | 2,860 | 1,260 | 1,260 | | 337 | 3,440 | 2,222 | 2,222 |
| 114 | 2,503 | 1,209 | 1,209 | | 338 | 2,944 | 1,978 | 1,978 |
| 115 | 3,353 | 1,679 | 1,679 | | 339 | 1,383 | 951 | 951 |
| 120 | 2,956 | 1,442 | 1,442 | | 340 | 3,050 | 1,762 | 1,762 |
| 121 | 3,660 | 1,667 | 1,667 | | 342 | 3,775 | 2,488 | 2,488 |
| 125 | 2,369 | 1,247 | 1,247 | | 344 | 3,517 | 2,252 | 2,252 |
| 128 | 4,374 | 2,328 | 2,328 | | 347 | 2,368 | 1,387 | 1,387 |
| 132 | 745 | 417 | 417 | | 351 | 1,912 | 851 | 851 |
| 134 | 3,345 | 1,640 | 1,640 | | 359 | 213 | 148 | 148 |
| 136 | 3,585 | 1,629 | 1,629 | | 360 | 189 | 151 | 151 |
| 140 | 3,818 | 1,749 | 1,749 | | 371 | 3,482 | 2,194 | 2,194 |
| 141 | 799 | 447 | 447 | | 372 | 375 | 150 | 150 |
| 142 | 5,209 | 2,673 | 2,673 | | 374 | 307 | 227 | 227 |
| 146 | 25 | 10 | 10 | | 375 | 47 | 37 | 37 |
| 147 | 8 | 1 | 1 | | 376 | 126 | 69 | 69 |
| 200 | 0 | 0 | 0 | | 400 | 0 | 0 | 0 |
| 203 | 3,281 | 1,745 | 1,745 | | 422 | 1,946 | 1,081 | 1,081 |
| 204 | 2,106 | 1,029 | 1,029 | | 423 | 2,296 | 1,206 | 1,206 |
| 205 | 3,918 | 1,977 | 1,977 | | 424 | 2,989 | 1,994 | 1,994 |
| 206 | 3,663 | 1,905 | 1,905 | | 425 | 1,235 | 849 | 849 |
| 216 | 910 | 492 | 492 | | 426 | 2,862 | 1,933 | 1,933 |
| 218 | 2,963 | 1,600 | 1,600 | | 430 | 1,632 | 1,129 | 1,129 |
| 219 | 947 | 529 | 529 | | 432 | 2,268 | 1,547 | 1,547 |
| 220 | 3,498 | 2,143 | 2,143 | | 435 | 3,346 | 2,303 | 2,303 |
| 221 | 1,649 | 791 | 791 | | 436 | 3,013 | 2,101 | 2,101 |
| 223 | 3,131 | 1,621 | 1,621 | | 440 | 2,065 | 1,277 | 1,277 |
| 225 | 528 | 330 | 330 | | 441 | 2,844 | 1,823 | 1,823 |
| 227 | 3,350 | 1,824 | 1,824 | | 443 | 2,802 | 1,936 | 1,936 |
| 228 | 3,540 | 2,107 | 2,107 | | 444 | 1,158 | 741 | 741 |
| 231 | 1,736 | 961 | 961 | | 445 | 2,861 | 1,743 | 1,743 |
| 237 | 4,623 | 2,608 | 2,608 | | 446 | 3,414 | 2,283 | 2,283 |
| 252 | 3,063 | 1,608 | 1,608 | | 448 | 3,211 | 2,126 | 2,126 |
| 253 | 3,331 | 1,662 | 1,662 | | 449 | 3,268 | 2,115 | 2,115 |
| 254 | 3,524 | 1,971 | 1,971 | | 451 | 1,291 | 875 | 875 |
| 314 | 3,585 | 2,824 | 2,824 | | 452 | 3,715 | 2,475 | 2,475 |
| 315 | 1,942 | 1,446 | 1,446 | | 453 | 2 | 0 | 0 |
| 316 | 3,507 | 1,966 | 1,966 | | 458 | 40 | 31 | 31 |
| 319 | 1,836 | 1,139 | 1,139 | | 461 | 91 | 62 | 62 |
| 325 | 563 | 377 | 377 | | 462 | 101 | 67 | 67 |
| 331 | 1,168 | 823 | 823 | | 463 | 2,209 | 1,392 | 1,392 |
| 332 | 2,958 | 1,712 | 1,712 | | 464 | 2,684 | 1,776 | 1,776 |

Plus the Limited Ballot (1,132 = 1,132) and Federal Overseas (90 = 90) units, for 120,880 ballots in all. Registered voters are as printed in the Precinct Results Report.
