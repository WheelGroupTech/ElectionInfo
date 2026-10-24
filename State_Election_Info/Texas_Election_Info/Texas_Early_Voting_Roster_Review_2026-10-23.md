# Texas Early Voting Rosters: County and State Data Review

**Election:** November 3, 2026 General Election\
**Data through:** Thursday, October 22, 2026 (fourth day of in-person early voting)\
**Prepared:** October 23, 2026 (updates the reviews of October 20-22; county and state data as posted by 8:20 PM October 23)\
**Scope:** the 50 most populous Texas counties and the Texas Secretary of State (SOS) roster portal

## 1. Summary

Texas Election Code §87.121 requires each county to post its early voting rosters (in-person and by mail) daily and to submit them for statewide elections to the Secretary of State, which republishes them. This update compares the county and state lists voter by voter (by Voter Unique Identifier, VUID) for the first four days of in-person voting, October 19-22, in the 50 most populous counties.

Key findings:

1. **The two sources agree.** Over every day for which both are complete, the county's and the state's lists of early in-person voters agree on 99.3% or more of voters in all 31 counties with dated county rosters. No voter is an in-person voter in one source and a mail voter in the other.
2. **The state corrected its turnout table.** The October 22 review reported that the portal's table overstated Dallas by about 35%; the table now matches the state's Dallas files (77,829, 65,066, 55,881 and 49,018 for October 19-22), and Hidalgo's smaller overstatement is also gone. The table now shows 782,856 in-person voters statewide for October 19, 741,583 for October 20, 677,634 for October 21 and 563,642 for October 22.
3. **The state has caught up on Harris and Fort Bend for earlier days**, but its October 22 files are short for Harris (47,563 of 86,328 in-person voters) and Fort Bend (593 of 23,066), and it still has only 1 Webb voter for each of October 21 and 22. Nine counties that post their own mail lists still report almost no mail ballots to the state.
4. **One voter is recorded as voting in person in two counties.** The voter is in Tarrant County's cumulative early voting list from October 20, with a Tarrant precinct. Smith County's October 22 roster lists the same VUID and name as an in-person voter that day in a Smith precinct. The state's Smith file for October 20 carries this voter, with the Tarrant precinct.
5. **Williamson's October 22 file gives wrong precincts for its mail voters.** It changed the precinct of 4,053 of the 4,352 mail voters in its October 21 file (names and in-person rows unchanged). Only 223 of the 3,152 mail voters also in the state's files now have the state's precinct, compared with all of them a day earlier.
6. **Bowie's newest report fixes its precincts; Bexar's has new differences.** Bowie's October 22 early vote report matches the state's precinct for all 1,859 voters; its October 20 and 21 reports still disagree for 3,277. Bexar's October 22 check-in report gives a different precinct from the state's for 64 voters, against one in its first three reports.
7. **The state's county files contain voters of other counties.** Across October 14-22, 815 voters (798 in-person, 17 mail) appear in an SOS county file while being on a different county's own roster, with that county's precinct.
8. **The state's files continue to name voters that Tarrant County redacts.** Tarrant withholds the name and VUID of 284 protected voters (215 mail, 69 in-person). The SOS Tarrant files list 147 mail voters and 84 in-person voters who are not on Tarrant's own rosters. Of these, 133 mail voters match a redacted Tarrant record by precinct and date, and 68 in-person voters match one by precinct.
9. **Travis's posted roster ZIP still has no mail lists for October 15 and 16, nor yet for October 22.** The state lists 1,505 Travis mail voters who are on none of Travis's mail lists, 1,208 of them dated October 19-20 by the state.
10. **Records that warrant review by the counties** (none is proof of wrongdoing; most have clerical explanations):
    - 13 voters listed both as a returned mail ballot and as an early in-person voter (El Paso 5, Dallas 3, Tarrant 2, Bastrop 1, Grayson 1, Travis 1);
    - 4 VUIDs on two counties' own rosters: Smith/Tarrant (finding 4), Bexar/Webb and Cameron/Hidalgo (same person), and Bell/Randall (two different people);
    - 17 VUIDs that name entirely different people in two records, 7 of them on Bell's roster;
    - 10 Bexar mail ballots listed as received on two different days, and 22 same-day duplicate in-person check-ins across 8 counties;
    - precinct differences between the county and the state for 527 Harris, 124 Galveston, 89 Tarrant, 65 Bexar and 52 El Paso voters, besides Williamson and Bowie.

## 2. Scope and method

**Counties (by 2025 Census population):**

- Ranks 1-20: Harris, Dallas, Tarrant, Bexar, Travis, Collin, Denton, Fort Bend, Hidalgo, El Paso, Montgomery, Williamson, Cameron, Brazoria, Bell, Galveston, Nueces, Lubbock, Hays, Webb.
- Ranks 21-50: McLennan, Jefferson, Smith, Brazos, Ellis, Johnson, Kaufman, Comal, Guadalupe, Midland, Parker, Ector, Grayson, Randall, Taylor, Rockwall, Wichita, Gregg, Hunt, Liberty, Tom Green, Bastrop, Potter, Bowie, Victoria, Henderson, Angelina, Orange, Coryell, Walker.

**Sources:**

- each county's own roster files, downloaded daily October 20-23. 33 counties post their own rosters; 32 can be read (Guadalupe's mail lists are scanned images). The other 17 (Nueces, Lubbock, Hays, Jefferson, Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker) link to the SOS portal. Files that counties posted on October 23 for that day's voting were left out;
- the SOS portal's per-county early voting CSVs for October 14-22, for all 50 counties, as posted at 8:20 PM October 23 (earlier days re-downloaded, since the state revises them);
- the SOS turnout tables for October 19-22.

**Method.** All files were read with Election Explorer's roster loader, which normalizes each county's layout to VUID, precinct, name, voting method and date. For every county file, the number of VUIDs loaded matches an independent count of the 10-digit identifiers in the raw file within 0.5%, and exactly for 235 of the 245 files (the other 10 differ by 1 to 8 identifiers). The lists were then compared by VUID:

- *early in-person, by day*: the county's in-person voters for each of October 19-22 vs. the SOS's IN-PERSON voters for that date. For counties whose lists are cumulative and undated (Tarrant, Hidalgo, Denton, El Paso, Potter), each day was recovered from consecutive daily snapshots (a day's voters = that day's snapshot minus the previous day's). A day counts toward the totals only when both sources have it and the state's file holds at least 90% of the county's count (50% in the earlier reviews; the stricter test keeps Harris's half-complete October 22 file out of the totals);
- *mail*: the county's latest mail list (or all daily files) vs. the SOS's MAIL-IN voters for October 14-22.

"County-only" and "state-only" voters were checked against the other 49 counties' files. Name and precinct comparisons ignore formatting (case, spacing, leading zeros, "Precinct"/"P"/"S" prefixes, and a two-digit precinct split such as "418603" for precinct 4186). Voter names and VUIDs are deliberately not reproduced in this report; the companion supplement lists the VUIDs and file locations of the anomalies for county and state staff.

## 3. The Secretary of State portal

**Where:** "Election Information & Turnout Data", `https://goelect.txelections.civixapps.com/ivis-evr-ui/evr`. Pick an election and an early voting date, then *Submit*: a table of registered voters, in-person and mail turnout per county, with a **CSV Download** per county under *Voter Details Report*.

**How the data is served.** The page fetches JSON whose `upload` field is the file in base64:

| Data | Request |
|---|---|
| Elections, early voting dates and county ids | `/api-ivis-system/api/v1/getFile?type=EVR_ELECTION` |
| Turnout table for one date | `/api-ivis-system/api/v1/getFile?type=EVR_EARLYVOTING&electionId=53815&electionDate=10/22/2026` |
| One county's voter CSV for one date | `/api-ivis-system/api/v1/getFileByFormat?type=EVR_EARLYVOTING&electionId=53815&countyId=<id>&county=<NAME>&electionDate=<MM/DD/YYYY>&format=CSV` |

County ids follow alphabetical order, except that the portal sorts McLennan after McCulloch (id 161). An empty response means the county reported nothing for that day.

**File contents.** `VOTER_NAME` ("LAST, FIRST MIDDLE"), `ID_VOTER` (VUID), `VOTING_METHOD` (`IN-PERSON` / `MAIL-IN`), `PRECINCT`. There is **no address and no date column**. A county CSV holds only the voters credited on that date.

**Timeliness and revisions.** A day's files fill in over the following day or two. By the evening of October 23, 49 of the 50 counties had an in-person file for October 22 (all but Webb, which also has none for October 21). Files keep changing after posting: across the two October 23 downloads, 90 county-day files changed. The state's October 21 Harris file grew from 26,071 to 95,230 rows, Fort Bend's from 2 to 25,360, and Gregg's from 4 to 3,027. Nueces's October 22 file grew from 105 to 5,923 rows between 1 PM and 8 PM.

**Mail dates.** The date of an SOS mail file is not the day the county received the ballot. In Travis, the state's October 16 mail voters are almost all ballots the county lists as received October 13, and its October 19 mail voters mostly the county's October 14 ballots.

**Turnout table vs. CSVs.** The state corrected its table on October 23: Dallas and Hidalgo now match the state's CSVs every day (in the October 22 review Dallas was about 35% and Hidalgo about 3% higher), and the statewide October 19 total fell from 806,265 to 782,856 (Callahan County, outside this review, was also halved). The remaining differences are Coryell (October 20: table 1,867, CSV 1,442), Webb (October 20: table 3,365, CSV 4,285) and Hays (October 22: table 10,860, CSV 8,790 when downloaded).

**Name formatting.** The SOS appends name suffixes to the middle name without a space ("RAYJR"), which causes apparent name mismatches against county files.

## 4. How each county posts its rosters

"Cumulative" means each new file contains the earlier ones; "daily" means each file holds only that day.

### 4.1 Counties 1-20

| County | Where | Format | Granularity | Voting date per voter | Download daily? |
|---|---|---|---|---|---|
| Harris | harrisvotes.com, Election Rosters | ZIP of CSV | Early voting one CSV per day, all in one ZIP replaced daily; mail cumulative (named "Cumulative_BBM_1126" on October 22, "Cumulative_BBM_ToPost_1126" again on October 23) | Yes | Recommended |
| Dallas | dallascountyvotes.org, election page | XLSX (CSV on October 21 only) | Cumulative, with earlier days revised | Yes | Recommended |
| Tarrant | tarrantcountytx.gov, current election | ZIP of TSV | Cumulative, same file name | Mail yes; **early voting no** | **Yes** |
| Bexar | elections.bexar.gov Document Center | PDF | Daily | Yes | No |
| Travis | votetravis.gov, one ZIP of daily workbooks | ZIP of XLSX | Daily workbooks in one ZIP | Yes | No (but see §6.4) |
| Collin | collincountytx.gov/Elections/rosters | XLSX | Cumulative, posted the same day | Yes | Recommended |
| Denton | votedenton.gov, rosters page | XLSX | Early voting cumulative, **undated from October 20**, posted the same day and re-posted the next morning; mail cumulative | October 19 only | **Yes** |
| Fort Bend | fortbendcountytx.gov, Early Voting Statistics | XLSX | Cumulative from October 21 | Yes | Recommended |
| Hidalgo | hidalgocounty.us Document Center (updated in place) | CSV / XLSX | Cumulative | **No** | **Yes** |
| El Paso | epcountyvotestx.gov, Current Election | CSV (mail), XLSX | Mail cumulative; early voting **cumulative from day 2, undated** | Mail yes; early voting no | **Yes** |
| Montgomery | elections.mctx.org, Early Voting Roster | ZIP of CSV per day | Daily | Yes | No |
| Williamson | wilcotx.gov Document Center (one document replaced) | XLSX | Cumulative | Yes | Recommended |
| Cameron | cameroncountytx.gov, Elections results | XLSX (per jurisdiction) | Cumulative, replaced | Yes | Recommended |
| Brazoria | brazoriacountyclerktx.gov, Voter Lists | XLSX | Daily; the October 21 in-person and October 22 mail lists were re-posted October 23 | Yes | No |
| Bell | bellcountytx.com, Daily Early Voting Reports | PDF | Daily | Yes | No |
| Galveston | galvestonvotes.org, Current Elections | CSV (mail), XLSX | Daily | Yes | No |
| Webb | webbcountytx.gov, Daily Voted List | PDF | Daily, posted the same day | Yes | No |
| Nueces, Lubbock, Hays | link to the SOS portal | SOS CSV | Daily (SOS) | File date | Re-download periodically |

### 4.2 Counties 21-50

| County | Own roster | Format and posting |
|---|---|---|
| McLennan | Yes | One PDF list of early voters, replaced each evening under a timestamped name; no method or date per voter |
| Smith | Yes | Daily PDFs in a Laserfiche public portal; in-person lists daily, absentee lists cumulative |
| Brazos | Yes | Daily PDFs |
| Grayson | Yes | Daily XLSX check-in reports and daily absentee lists; also check-in summaries by location |
| Randall | Yes | Two PDFs re-posted daily (cumulative early voting roster with check-in time; mail roster printed sideways) |
| Wichita | Yes | Daily PDFs behind a bot challenge that blocks automated downloads; the absentee list changed layout on October 21 (no received date, wrapped rows) |
| Gregg | Yes | PDFs (check-ins, mail list); October 21 still not posted by October 23 evening |
| Hunt | Yes | PDFs; cumulative through October 20, daily from October 21 |
| Tom Green | Yes | PDFs; the October 22 roster and mail list are cumulative (October 20 and 21 were never posted separately) |
| Bastrop | Yes | Daily CSVs; mail CSV re-posted under a new name each day |
| Potter | Yes | Two PDFs replaced daily after the polls close; early voting roster cumulative and undated |
| Bowie | Yes | Daily PDFs (ballots by mail received, early vote history; precinct added October 20) |
| Victoria | Yes | One PDF and XLSX ("mail and in person"), cumulative, posted under each day's date |
| Henderson | Yes | Daily PDFs, named for the morning they are posted |
| Angelina | Yes | Daily PDFs (early voting history; October 20-22 posted October 23); no mail list |
| Guadalupe | Mail only | Daily returned-mail PDFs, scanned images (not machine-readable) |
| Jefferson, Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker | No | Link to the SOS portal |

## 5. County data compared with state data

### 5.1 Early in-person voters by day

| County | Oct 19 county | Oct 19 state | Oct 20 county | Oct 20 state | Oct 21 county | Oct 21 state | Oct 22 county | Oct 22 state | On both | County only | State only (other county) | Agree |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Harris | 104,658 | 104,606 | 99,907 | 99,832 | 93,411 | 93,350 | 86,328 | 47,563 (partial) | 297,737 | 239 | 51 (27) | 99.9% |
| Dallas | 77,853 | 77,829 | 65,084 | 65,066 | 56,092 | 55,881 | 49,039 | 49,018 | 247,692 | 376 | 102 (82) | 99.8% |
| Tarrant | 50,294 | 50,142 | 51,008 | 50,722 | 48,930 | 48,631 | 46,363 | 46,123 | 195,376 | 1,219 | 242 (34) | 99.4% |
| Bexar | 42,764 | 42,039 | 44,733 | 44,404 | 45,076 | 45,029 | 44,151 | 44,134 | 175,507 | 1,217 | 99 (47) | 99.3% |
| Travis | 40,776 | 40,791 | 40,359 | 40,375 | 39,490 | 39,500 | 37,774 | 37,771 | 158,363 | 36 | 74 (64) | 100.0% |
| Collin | 34,388 | 34,258 | 33,875 | 33,749 | 31,135 | 31,027 | 29,452 | 29,347 | 128,102 | 748 | 279 (59) | 99.4% |
| Denton | 34,711 | 34,569 | 31,551 | 31,474 | 28,344 | 28,269 | 25,940 | 25,828 | 119,921 | 625 | 219 (80) | 99.5% |
| Fort Bend | 31,743 | 31,605 | 28,420 | 28,267 | 24,874 | 24,706 | 23,066 | 593 (partial) | 84,479 | 558 | 99 (17) | 99.3% |
| Williamson | 25,695 | 25,712 | 24,237 | 24,281 | 22,195 | 22,217 | 20,906 | 20,931 | 92,991 | 42 | 150 (25) | 99.8% |
| Hidalgo | 17,424 | 17,319 | 17,379 | 17,279 | 15,963 | 15,877 | 14,489 | 14,404 | 64,788 | 467 | 91 (5) | 99.3% |
| El Paso | 17,515 | 17,437 | 16,742 | 16,649 | 14,850 | 14,771 | 13,596 | 13,525 | 62,371 | 332 | 11 (5) | 99.5% |
| Montgomery | 13,568 | 13,561 | 14,162 | 14,168 | 14,407 | 14,408 | 13,991 | 13,986 | 56,101 | 27 | 22 (19) | 100.0% |
| Brazoria | 14,321 | 14,328 | 12,687 | 12,694 | 11,137 | 11,141 | 9,906 | 9,909 | 48,050 | 1 | 22 (17) | 100.0% |
| Galveston | 13,653 | 13,654 | 12,017 | 12,014 | 9,970 | 9,975 | 8,700 | 8,699 | 44,313 | 27 | 29 (18) | 99.9% |
| Cameron | 9,424 | 9,428 | 8,643 | 8,653 | 7,720 | 7,730 | 6,518 | 6,541 | 32,283 | 22 | 69 (18) | 99.8% |
| Bell | 7,111 | 7,095 | 7,145 | 7,132 | 7,059 | 7,056 | 6,779 | 6,767 | 28,043 | 51 | 7 (5) | 99.8% |
| Smith | 5,861 | 5,827 | 5,980 | 5,944 | 6,016 | 5,969 | 5,806 | 5,770 | 23,503 | 160 | 7 (3) | 99.3% |
| Grayson | 5,467 | 5,457 | 4,769 | 4,793 | 4,190 | 4,179 | 3,663 | 3,659 | 18,014 | 75 | 74 (10) | 99.6% |
| Webb | 4,943 | 4,917 | 4,308 | 4,285 | 3,971 | 1 (partial) | 3,269 | 1 (partial) | 9,201 | 50 | 1 (0) | 99.5% |
| Brazos | 4,561 | 4,564 | 4,706 | 4,704 | 4,672 | 4,678 | 4,692 | 4,695 | 18,624 | 7 | 17 (13) | 99.9% |
| Randall | 4,394 | 4,399 | 4,126 | 4,131 | 3,733 | 3,734 | 3,365 | 3,365 | 15,616 | 2 | 13 (1) | 99.9% |
| Gregg | 3,676 | 2 (partial) | 3,526 | 3,523 | not posted | 2,909 | 2,485 | 2,470 | 5,979 | 32 | 14 (2) | 99.5% |
| Henderson | 3,257 | 3,257 | 2,823 | 2,819 | 2,313 | 2,311 | 1,995 | 1,999 | 10,375 | 13 | 11 (10) | 99.9% |
| Bastrop | 3,143 | 3,142 | 2,888 | 2,888 | 2,480 | 2,479 | 2,376 | 2,377 | 10,882 | 5 | 4 (2) | 100.0% |
| Wichita | 2,945 | 2,945 | 2,783 | 2,784 | 2,615 | 2,617 | 2,228 | 2,228 | 10,571 | 0 | 3 (2) | 100.0% |
| Tom Green | 2,634 | 2,622 | 2,714 | 2,709 | 2,468 | 2,466 | 2,449 | 2,449 | 10,236 | 29 | 10 (5) | 99.7% |
| Angelina | 2,566 | 2,567 | 2,352 | 2,353 | 2,006 | 2,006 | 1,795 | 1,795 | 8,719 | 0 | 2 (2) | 100.0% |
| Bowie | 2,023 | 2,027 | 1,963 | 1,961 | 1,917 | 1,917 | 1,859 | 1,859 | 7,760 | 2 | 4 (0) | 99.9% |
| Hunt | 1,839 | 1,827 | 2,044 | 1,724 (partial) | 2,032 | 2,043 | 1,789 | 1,798 | 5,639 | 21 | 29 (8) | 99.5% |
| Victoria | 1,660 | 1,660 | 1,758 | 1,758 | 1,799 | 1,802 | 1,568 | 1,566 | 6,781 | 4 | 5 (0) | 99.9% |
| Potter | (cumulative) | 2,180 | (cumulative) | 1,970 | 1,737 | 1,739 | 1,482 | 1,482 | 3,219 | 0 | 2 (2) | 99.9% |
| McLennan | no method or date | 4,912 | | 4,963 | | 4,874 | | 4,627 | | | | |

Counts are distinct VUIDs (county voters without a VUID are excluded). "On both", "County only" and "State only" add up the days on which both sources are complete; "Agree" is the voters on both lists as a share of the larger list. McLennan's list (21,638 voters, posted the evening of October 22) contains all but 57 of the state's 20,377 McLennan in-person and mail voters for October 14-22. Potter's first two snapshots were posted on the same day, so its October 19 and 20 cannot be separated.

Most state-only voters are voters of other counties (§6.1) or, in Tarrant, voters the county lists a day later than the state (88 on October 19, 34 on October 20) and redacted voters (§6.2). Most county-only voters are a few dozen to a few hundred per day that the state has not yet received.

### 5.2 Mail ballots

The SOS mail data begins October 14 and is dated by when the state received it, days after the county (§3); county mail lists go back to the first ballots received (September 21-22).

| County | County mail | State mail (Oct 14-22) | On both | State only (other county) |
|---|---:|---:|---:|---:|
| Harris | 31,045 | 11,909 | 11,832 | 77 |
| Tarrant | 14,506 | 9,018 | 8,870 | 148 (1); 133 match redacted voters, §6.2 |
| Dallas | 14,271 | 5,629 | 5,553 | 76 (1) |
| Travis | 9,226 | 6,158 | 4,648 | 1,510 (5); see §6.4 |
| Montgomery | 6,415 | 2,607 | 2,518 | 89 |
| El Paso | 5,894 | 1,035 | 1,025 | 10 |
| Williamson | 5,131 | 3,156 | 3,152 | 4 |
| Denton | 4,644 | 3,054 | 3,040 | 14 (1) |
| Collin | 4,623 | 2,817 | 2,011 | 806 (772 dated Oct 22, after the county's list) |
| Fort Bend | 3,887 | 3,040 | 3,019 | 21 |
| Hidalgo | 3,535 | 1,367 | 1,365 | 2 |
| Galveston | 2,697 | 267 | 264 | 3 (1) |
| Bell | 2,469 | 2,219 | 2,150 | 69 |
| Randall | 1,737 | 230 | 227 | 3 |
| Smith | 1,729 | 968 | 951 | 17 (1) |
| Tom Green | 1,378 | 539 | 530 | 9 |
| Gregg | 1,207 | 925 | 914 | 11 |
| Grayson | 1,082 | 482 | 480 | 2 |
| Bastrop | 914 | 375 | 374 | 1 (1) |
| Webb | 463 | 267 | 239 | 28 (all dated Oct 14) |

Counties that post mail lists but report almost none to the state: Bexar (17,645 in the county's list, 1 at the state), Brazoria (2,477), Brazos (1,349), Victoria (1,219, 1 at the state), Cameron (1,167), Wichita (898), Potter (861), Bowie (772) and Hunt (158). Angelina posts no mail list; the state has 707 Angelina mail voters, 650 of them dated October 21.

### 5.3 Counties on the state portal only

| County | SOS days on file | In person Oct 19 | Oct 20 | Oct 21 | Oct 22 | Mail Oct 14-22 |
|---|---|---:|---:|---:|---:|---:|
| Hays | Oct 14, 19-22 | 10,256 | 9,910 | 9,416 | 8,790 | 486 |
| Lubbock | Oct 19-22 | 8,856 | 8,642 | 7,902 | 7,291 | 1 |
| Ellis | Oct 14-16, 19-22 | 7,902 | 7,123 | 6,234 | 5,518 | 514 |
| Nueces | Oct 14-17, 19-22 | 7,617 | 6,929 | 6,444 | 5,614 | 2,498 |
| Jefferson | Oct 14, 19-22 | 6,992 | 6,567 | 5,504 | 4,800 | 1,073 |
| Parker | Oct 19-22 | 5,953 | 5,711 | 5,203 | 4,991 | 0 |
| Comal | Oct 14-17, 19-22 | 5,876 | 5,803 | 5,816 | 5,635 | 1,269 |
| Johnson | Oct 18-22 | 5,871 | 5,440 | 4,891 | 4,357 | 0 |
| Rockwall | Oct 19-22 | 4,968 | 4,338 | 3,857 | 3,304 | 0 |
| Guadalupe | Oct 14-16, 19-22 | 4,780 | 4,762 | 4,617 | 4,385 | 535 |
| Kaufman | Oct 19-22 | 3,904 | 4,235 | 3,955 | 3,710 | 710 |
| Taylor | Oct 14, 15, 19-22 | 3,186 | 3,202 | 2,910 | 2,673 | 536 |
| Midland | Oct 14-16, 19-22 | 2,963 | 3,117 | 2,863 | 2,581 | 426 |
| Orange | Oct 19-22 | 2,200 | 2,236 | 1,881 | 1,567 | 0 |
| Liberty | Oct 19-22 | 1,952 | 1,879 | 1,690 | 1,656 | 0 |
| Ector | Oct 19-22 | 1,809 | 1,853 | 1,660 | 1,562 | 0 |
| Coryell | Oct 19-22 | 1,802 | 1,442 | 1,233 | 1,096 | 0 |
| Walker | Oct 15, 19-22 | 1,740 | 1,717 | 1,563 | 1,381 | 2 |

For these counties the state is the only published source. All 18 have state files for October 19-22; seven have reported no mail ballots to the state, and Lubbock and Walker only one or two.

## 6. Anomalies between county and state data

### 6.1 State county files include voters of other counties

Across October 14-22, 815 voters (798 in-person, 17 mail) in the SOS county files appear on a different county's own roster, with the same first name and that county's precinct. The largest numbers are in the SOS files for Dallas (83), Denton (81), Travis (69), Collin (59), Bexar (47), Tarrant (35), Hays (34), Parker (33), Harris (32) and Johnson (30). No voter appears in two SOS county files, so these are records assigned to the wrong county's file, not duplicates. Anyone using SOS county files alone will slightly overcount each county.

### 6.2 State files identify voters Tarrant County redacts

Tarrant replaces the name, VUID and address of protected voters with "--Redacted--", keeping only precinct and date: 284 records in its October 23 files (215 mail, 69 early in-person). No other county redacts identities; Harris hides the address (only) of 1,783 named voters.

- **Mail:** the SOS Tarrant mail files list 147 mail voters who are not on Tarrant's mail roster (after excluding one voter of another county). For 133 of them there is a redacted Tarrant mail record with the same precinct and the same date.
- **In person:** the SOS Tarrant files list 84 in-person voters (besides 34 of other counties) who are not on Tarrant's roster on any day; 68 of them match a redacted Tarrant in-person record by precinct.
- **Fewer redacted in-person records each day:** Tarrant's cumulative in-person list held 104 redacted records on October 20, 99 on October 21, 79 on October 22 and 69 on October 23, although a cumulative list should not shrink.

With four days of data, the match remains close for both voting methods. This continues to indicate that the state publishes the name, VUID and precinct of voters whose identities Tarrant withholds. It cannot be proven record by record, because the redacted rows carry no identifier. Neither source exposes an address.

### 6.3 Reporting gaps in the state data

- **In-person:** Harris's October 22 (47,563 of 86,328); Fort Bend's October 22 (593 of 23,066); Webb's October 21 and 22 (1 voter each); Gregg's October 19 (2 of 3,676); Hunt's October 20 (1,724 of 2,044).
- **Mail:** nine counties that post mail lists report almost none to the state (§5.2), and seven portal-only counties report none (§5.3).
- **Turnout table vs. CSV:** Coryell, Webb, Hays (§3).

### 6.4 Mail ballots in state data but not in the county's mail list

- **Travis:** Travis's roster ZIP holds mail lists for October 14 and October 19-21, but none for October 15, 16 or 22. The state lists 1,505 Travis mail voters who are on none of Travis's lists, including 1,208 dated October 19-20 by the state. These are most likely the ballots received October 15-16; Travis should post those days' lists.
- **Timing:** Collin's 772 and Travis's 27 state-only mail voters dated October 22 are dated after the county lists used here.
- **Elsewhere:** Montgomery 89, Harris 77, Dallas 75, Bell 69, Webb 28 (all dated October 14), Fort Bend 21, Smith 16, Denton 13, Gregg 11, El Paso 10, Tom Green 9, and 1-4 in six other counties. None of these voters appears in the county's files under another voting method.

### 6.5 Precinct differences

- **Williamson:** the October 22 file gives a precinct different from the state's for 2,929 mail voters. The precincts of 4,053 of the 4,352 mail voters already in the October 21 file changed overnight, with the same names and dates, while every in-person row kept its precinct; the October 21 file matched the state for all mail voters. The county's mail precinct column appears to have been scrambled.
- **Bowie:** the October 20 and 21 early vote reports give a precinct that matches the state's for 395 of 1,961 and 206 of 1,917 voters (3,277 differences); the October 22 report matches the state for all 1,859 voters. The county should check the two earlier reports.
- **Bexar:** 64 voters in the October 22 check-in report, spread across 59 pages, have a different precinct from the state's (one in the first three reports).
- **Elsewhere,** on matched voters: Harris 527 (510 in-person), Galveston 124 (111 of them in its October 19 file), Tarrant 89, El Paso 52 (all in-person), Smith 31, Hunt 14, Montgomery 8, Denton 4, Travis 4, and one each in Bastrop, Dallas and Henderson.

Denton and Gregg write the precinct with a two-digit split appended ("418603" for 4186); this is formatting, not a difference.

### 6.6 Surnames

On matched voters, the surname differs between the county and the state in 26 Collin, 23 Tarrant, 20 Williamson, 19 Cameron, 18 Bexar and 17 Denton records, and in fewer than 10 elsewhere (213 in all). In all but 4 of these records the two names share at least one name: they are name changes recorded in one system and not the other, or formatting (counties that write names "FIRST MIDDLE LAST").

### 6.7 One VUID, two different people

- Within **Harris's** cumulative mail file and a **Brazoria** mail file, one VUID is each listed for two different voters.
- One VUID is a mail ballot received October 21 on both **Bell's** and **Randall's** mail lists, for two people with different names. (The Bell/Randall VUID reported on October 22 is no longer on Randall's roster; the state's Randall file still lists it.)
- Fifteen VUIDs name a voter in an SOS file and an entirely different person (no name in common) on a county's roster. Four are within one county (Bexar, El Paso, Montgomery and Tarrant). Eleven are across counties, seven of them on Bell's roster, against the SOS Coryell, Fort Bend (2), Lubbock, Randall (2) and Victoria files. The others are between El Paso and Harris, Harris and Hidalgo, Tarrant and Hidalgo, and El Paso and Montgomery.

All seven Bell VUIDs and the Hidalgo/Harris, Hidalgo/Tarrant and Montgomery/El Paso VUIDs are recent registrations in one numeric range (2223365544-2223382143). Bell County's roster gives these VUIDs to different people than the state's files do, so the Bell County Elections office and the SOS should compare them first.

### 6.8 One voter on two counties' own rosters

- One VUID is an early in-person voter in both **Tarrant's** cumulative list (from October 20, precinct 1224) and **Smith's** October 22 roster (precinct 245), with the same name. The state's Smith October 20 file lists the voter with the Tarrant precinct; the state's Tarrant files and Smith October 22 file do not list the voter.
- One VUID is an early in-person voter on October 19 in both **Bexar's** and **Webb's** rosters (same name).
- One VUID is a returned mail ballot on both **Hidalgo's** and **Cameron's** mail lists (same name).

### 6.9 Mail ballot and in-person vote by the same voter

Thirteen voters are listed both as a returned mail ballot and as an early in-person voter in the same county:

- **El Paso 5:** mail returned October 6-20; in person by October 22. Two were reported in the first review, two more by October 21 and one new on October 22.
- **Dallas 3:** mail October 9; in person October 19, 20 and 22. The October 22 voter is new.
- **Tarrant 2:** in person by October 19 and by October 20; mail ballots recorded October 21 and 22.
- **Bastrop 1:** mail October 16; in person October 22. New.
- **Grayson 1:** mail October 2; in person October 20.
- **Travis 1:** mail October 7; in person October 21.

A voter whose mail ballot has been accepted may not vote in person; these may be cancelled mail ballots, provisional votes or data errors.

### 6.10 Duplicates within county files

- **Same-day duplicate check-ins:** Grayson 8, Bexar 6, Collin 3, and 1 each in Denton, Galveston, Tom Green, Travis and Webb.
- **Mail ballot received twice:** 10 Bexar voters appear on two days' received-ballot lists; Travis 2, Gregg 2, and 1 each in Brazoria, Brazos, Galveston, Harris and Montgomery.

### 6.11 Identifiers that are not state VUIDs

| County | Records | ID length | Lists |
|---|---:|---|---|
| Tarrant | 289 | 8 digits | mail (234), in-person (55) |
| Collin | 123 | 6, 7, 9 digits | in-person (118), mail (3), provisional (2) |
| Denton | 79 | 7 digits | in-person |
| El Paso | 59 | 7 digits | mail (53), in-person (6) |
| Grayson | 40 | 6, 8, 9 digits | in-person (36), mail (4) |
| Hidalgo | 39 | 8-9 digits | mail (29), in-person (10) |
| Webb | 22 | 8 digits | mail (18), in-person (4) |
| Smith | 12 | 7 digits | in-person |
| Bowie | 7 | 9 or 11 digits | mail |
| Bexar | 6 | 8, 9, 11 digits | in-person |
| Gregg | 6 | 6, 7, 11 digits | in-person |
| Tom Green | 3 | 9 digits | mail |
| Henderson | 1 | 8 digits | in-person |

These are likely county-internal numbers for voters whose state VUID had not been assigned or synchronized, or (for the 9- and 11-digit cases) a VUID with a digit missing or added. The state files contain none, so these voters cannot be matched by ID; for 11 of them the state lists a voter with the same name and precinct under a 10-digit VUID (see the supplement).

## 7. Other county data quality notes

- **Blank VUIDs** (named voters with no ID): Harris 375 mail ballots; Collin 117 (105 provisional); Grayson 50 in-person; Bell 23 mail; Bexar 18 in-person; Dallas 16 mail; Fort Bend 8 mail; Hunt 7; Hidalgo 2. Travis's 332 and Montgomery's 95 Limited Ballot voters have no VUID by design. Tarrant's 284 redacted records also have no VUID (§6.2).
- **No voting date:** Tarrant's, Hidalgo's and Potter's early voting lists, Denton's from October 20, El Paso's from day 2 and McLennan's list carry no date per voter. Wichita's October 21 absentee list no longer shows the date each ballot was received, and Smith's and Wichita's absentee lists are cumulative, so the date in their file names is the list date.
- **Not yet posted** by the evening of October 23: Gregg's October 21 roster and Travis's October 22 mail list; Wichita's absentee list was last posted for October 21.
- **Files replaced or re-posted:** Victoria, Potter and Tom Green post cumulative files; Randall re-posts its two rosters; McLennan posts one list under a new timestamped name; Bastrop renames its mail CSV daily; Brazoria re-posted its October 21 in-person list (one voter fewer) and its October 22 mail list on October 23.
- **Same-day posting:** Collin, Denton, Webb, McLennan, Potter and Bastrop post lists for the current day during that day or that evening; Collin's and Denton's are not final until the next day's posting.
- **Hard-to-read formats:** Guadalupe's mail lists are scanned images; Wichita's PDFs sit behind a bot challenge, and its October 21 absentee list wraps each record onto two lines; Smith's lists are in a JavaScript document portal; Randall prints its mail roster sideways; Grayson's check-in workbook is a printed report with its column titles on separate rows.

## 8. Recommendations

1. **Download daily** through Election Day: Tarrant, Hidalgo, Denton, El Paso and Potter (no voting date per voter), Victoria, Tom Green and McLennan (cumulative files replaced), and Collin, Williamson, Cameron, Dallas, Fort Bend and Harris (latest file only). Save same-named files with the download time.
2. **Prefer county files** for counts and voter lists; use the SOS portal for the 17 counties that post nothing else and as a cross-check. Re-download SOS days for at least two days after the date, since the state fills in and revises them, and do not read SOS mail dates as received dates.
3. **When using SOS county files,** expect voters of other counties in each file (§6.1) and check them against neighboring counties before treating them as anomalies.
4. **Refer to the counties:**
   - the Smith/Tarrant voter (§6.8);
   - the 13 voters with both a mail ballot and an in-person vote;
   - the shared VUIDs (§6.7), first those on Bell's roster;
   - Williamson's October 22 mail precincts;
   - Bowie's October 20-21 precincts and Bexar's October 22 precincts;
   - Travis's missing October 15-16 mail lists;
   - Bexar's mail ballots received twice;
   - the precinct differences in Harris, Galveston, Tarrant and El Paso.
5. **Refer to Tarrant County and the Secretary of State:**
   - the publication by the SOS of voters Tarrant redacts, and the shrinking count of redacted in-person records (§6.2);
   - the SOS files' inclusion of other counties' voters (§6.1);
   - the counties whose mail ballots are missing at the state (§6.3);
   - Webb's missing October 21-22 files.
6. **Repeat this comparison** after early voting closes and after the canvass.

## Appendix A. Files

Downloaded files are saved per county:

- county files are saved as `G26_<county file name>`; files a county posts under the same name every day get the download date (`_2026-10-23`), or the download date and time (`_2026-10-23-13-20-49`) for cumulative lists that carry no voting dates (Hidalgo, Potter);
- SOS files are saved as `G26_<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv`; when the state revises a day, the earlier copy is kept.

The files load in Election Explorer (File → Load Voter Roster); the comparisons in this report were produced from the loader's normalized output. The companion supplement (`Texas_Early_Voting_Roster_Review_2026-10-23_Supplement`) lists each anomalous record's VUID and its location in the county's and the state's files.

## Appendix B. Caveats

- In-person comparisons cover October 19-22; where the state's file for a day is partial, that day is shown but left out of the totals.
- County files were downloaded October 20-23 at different times; several counties post same-day lists that are later replaced, and counties keep revising earlier days.
- Day-level counts for cumulative, undated county lists were recovered from consecutive snapshots and attribute voters added late to the day they first appear (Tarrant, §5.1).
- A match between two records means the same VUID; name and precinct differences were checked separately. A match by name, precinct or date without a common VUID (§6.2, §6.11) is an inference, not a confirmed identity.
- This review describes data as published. It does not establish that any voter voted improperly; the records flagged in §6.5-§6.10 have ordinary clerical explanations as well as serious ones, and only the counties can resolve them.
