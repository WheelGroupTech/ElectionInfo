# Texas Early Voting Rosters: County and State Data Review

**Election:** November 3, 2026 General Election\
**Data through:** Friday, October 23, 2026 (fifth day of in-person early voting)\
**Prepared:** October 24, 2026 (updates the reviews of October 20-23; county and state data as posted by 8:05 PM October 24)\
**Scope:** the 50 most populous Texas counties and the Texas Secretary of State (SOS) roster portal

## 1. Summary

Texas Election Code §87.121 requires each county to post its early voting rosters (in-person and by mail) daily and to submit them for statewide elections to the Secretary of State, which republishes them. This update compares the county and state lists voter by voter (by Voter Unique Identifier, VUID) for the first five days of in-person voting, October 19-23, in the 50 most populous counties. Jefferson County, previously counted among the counties that post only on the state portal, publishes its own rosters and is now included.

Key findings:

1. **The two sources agree.** Over every day for which both are complete, the county's and the state's lists of early in-person voters agree on 99.2% or more of voters in all 32 counties with dated county rosters. Only four voters are an in-person voter in one source and a mail voter in the other (Hunt 2, Hidalgo 1, Cameron 1).
2. **The state's files are now complete for every county and day.** All 50 counties have a state in-person file for each of October 19-23 (Webb's missing October 21 and 22 files have arrived). Only Harris's October 23 file is still short (60,245 of 95,108 voters).
3. **The state now reports mail ballots for nearly every county.** On October 24 the state added mail voters to many counties' earlier files: Bexar now has 7,310 at the state (1 in the October 23 review), and Brazoria, Cameron, Potter, Victoria, Wichita, Bowie and Hunt now have mail ballots there. Eight counties that post only on the portal reported their first mail ballots (Parker 636, Lubbock 592, Orange 542, Rockwall 313, Liberty 216, Johnson 208, Coryell 183, Walker 129). Only Brazos (8 of 1,428) and Ector (2) still report almost none.
4. **Six voters are recorded as voting in two counties.**
   - Three are on one county's in-person roster and on a second county's in-person roster a day or more later: Tarrant (from October 20) and Smith (October 22), and Tarrant (October 22) and Dallas (October 23). Bexar and Hidalgo list the third on the same day, October 23. In all three cases, the state's file for the second county lists the first county's vote, with the first county's precinct.
   - One voter's mail ballot was received by Harris on September 30, and the same voter voted in person in Travis on October 23.
   - Two, reported since October 20, are an in-person voter on October 19 in both Bexar and Webb, and a returned mail ballot on both Cameron's and Hidalgo's lists.
5. **The turnout table now overstates Travis.** For October 23 the portal's table shows 50,267 Travis in-person voters, against 40,483 in the state's own Travis file and 40,453 in Travis County's roster. Ellis (8,108 vs. 6,326) and Hays (October 22: 10,862 vs. 8,792) are also overstated.
6. **Williamson's mail precincts are still wrong.** Its October 23 file, like the October 22 file, gives a precinct different from the state's for 4,384 mail voters; in-person rows match.
7. **Bowie's October 23 report disagrees with the state again** for 1,441 of 1,888 voters, after its October 22 report matched for all of them.
8. **Hunt's October 23 list includes mail ballots.** 61 of its 2,165 voters have "BBM" (ballot by mail) as their polling place; the state lists 54 of them as mail voters on October 23.
9. **The state's county files contain voters of other counties.** Across October 14-23, 1,065 voters (1,046 in-person, 19 mail) appear in an SOS county file while being on a different county's own roster, with that county's precinct.
10. **The state's files continue to name voters that Tarrant County redacts,** while the number of redacted records in Tarrant's own cumulative lists keeps falling: in-person 104 on October 20, 62 on October 24; mail 215 in the October 23 morning file, 202 that evening.
11. **Records that warrant review by the counties** (none is proof of wrongdoing; most have clerical explanations):
    - 17 voters listed both as a returned mail ballot and as an early in-person voter in the same county (El Paso 5, Dallas 3, Randall 3, Tarrant 2, Bastrop, Grayson, Jefferson and Travis 1 each);
    - 24 VUIDs that name entirely different people in two records, 7 of them on Bell's roster;
    - 26 same-day duplicate in-person check-ins in 10 counties, and 22 mail ballots listed as received on two days (10 in Bexar);
    - precinct differences besides Williamson and Bowie: Harris 771, Galveston 142, Tarrant 118, Bexar 67, El Paso 61.

## 2. Scope and method

**Counties (by 2025 Census population):**

- Ranks 1-20: Harris, Dallas, Tarrant, Bexar, Travis, Collin, Denton, Fort Bend, Hidalgo, El Paso, Montgomery, Williamson, Cameron, Brazoria, Bell, Galveston, Nueces, Lubbock, Hays, Webb.
- Ranks 21-50: McLennan, Jefferson, Smith, Brazos, Ellis, Johnson, Kaufman, Comal, Guadalupe, Midland, Parker, Ector, Grayson, Randall, Taylor, Rockwall, Wichita, Gregg, Hunt, Liberty, Tom Green, Bastrop, Potter, Bowie, Victoria, Henderson, Angelina, Orange, Coryell, Walker.

**Sources:**

- each county's own roster files, downloaded daily October 20-24. 34 counties post their own rosters; 33 can be read (Guadalupe's mail lists are scanned images). The other 16 (Nueces, Lubbock, Hays, Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker) link to the SOS portal. Files that counties posted on October 24 for that day's voting were left out;
- the SOS portal's per-county early voting CSVs for October 14-23, for all 50 counties, as posted at 8:05 PM October 24 (earlier days re-downloaded, since the state revises them);
- the SOS turnout tables for October 19-23.

**Method.** All files were read with Election Explorer's roster loader, which normalizes each county's layout to VUID, precinct, name, voting method and date. Three reading problems found in the October 23 files were corrected in the loader for this review: Hunt's "BBM" polling place (a mail ballot), Gregg's polling-place names whose last letter is printed outside the cell (it was read into the precinct), and Wichita's row numbers printed on a record's second line (they were read into the precinct, and 50 voters were lost). Jefferson's Excel 97-2003 (.xls) files are read by the loader since October 24. For every county file, the number of VUIDs loaded matches an independent count of the 10-digit identifiers in the raw file within 0.5%, and exactly for 264 of the 275 files (the other 11 differ by 1 to 8 identifiers). The lists were then compared by VUID:

- *early in-person, by day*: the county's in-person voters for each of October 19-23 vs. the SOS's IN-PERSON voters for that date. For counties whose lists are cumulative and undated (Tarrant, Hidalgo, Denton, El Paso, Potter), each day was recovered from consecutive daily snapshots (a day's voters = that day's snapshot minus the previous day's). A day counts toward the totals only when both sources have it and the state's file holds at least 90% of the county's count;
- *mail*: the county's latest mail list (or all daily files) vs. the SOS's MAIL-IN voters for October 14-23.

"County-only" and "state-only" voters were checked against the other 49 counties' files. Name and precinct comparisons ignore formatting (case, spacing, leading zeros, "Precinct"/"P"/"S" prefixes, and a two-digit precinct split such as "418603" for precinct 4186). Voter names and VUIDs are deliberately not reproduced in this report; the companion supplement lists the VUIDs and file locations of the anomalies for county and state staff.

## 3. The Secretary of State portal

**Where:** "Election Information & Turnout Data", `https://goelect.txelections.civixapps.com/ivis-evr-ui/evr`. Pick an election and an early voting date, then *Submit*: a table of registered voters, in-person and mail turnout per county, with a **CSV Download** per county under *Voter Details Report*.

**How the data is served.** The page fetches JSON whose `upload` field is the file in base64:

| Data | Request |
|---|---|
| Elections, early voting dates and county ids | `/api-ivis-system/api/v1/getFile?type=EVR_ELECTION` |
| Turnout table for one date | `/api-ivis-system/api/v1/getFile?type=EVR_EARLYVOTING&electionId=53815&electionDate=10/23/2026` |
| One county's voter CSV for one date | `/api-ivis-system/api/v1/getFileByFormat?type=EVR_EARLYVOTING&electionId=53815&countyId=<id>&county=<NAME>&electionDate=<MM/DD/YYYY>&format=CSV` |

County ids follow alphabetical order, except that the portal sorts McLennan after McCulloch (id 161). An empty response means the county reported nothing for that day.

**File contents.** `VOTER_NAME` ("LAST, FIRST MIDDLE"), `ID_VOTER` (VUID), `VOTING_METHOD` (`IN-PERSON` / `MAIL-IN`), `PRECINCT`. There is **no address and no date column**. A county CSV holds only the voters credited on that date.

**Timeliness and revisions.** A day's files fill in over the following day or two. By the evening of October 24, all 50 counties had an in-person file for each of October 19-23. Across the October 24 downloads, 140 county-day files changed. Between 1 PM and 8 PM, the state's October 23 files grew from 28,321 to 61,275 rows for Harris, from 2,922 to 26,532 for Fort Bend, from 17,743 to 46,973 for Bexar and from 94 to 6,686 for Nueces. Earlier days gained the newly reported mail ballots: Bexar's October 21 file grew from 45,071 to 46,367 rows and Wichita's from 2,674 to 2,918.

**Mail dates.** The date of an SOS mail file is not the day the county received the ballot. In Travis, the state's October 16 mail voters are almost all ballots the county lists as received October 13, and its October 19 mail voters mostly the county's October 14 ballots.

**Turnout table vs. CSVs.** The statewide table now shows 782,978 in-person voters for October 19, 742,645 for October 20, 681,979 for October 21, 631,410 for October 22 and 659,389 for October 23. It agrees with the state's CSVs for every county and day except Travis (October 23: table 50,267, CSV 40,483), Ellis (October 23: 8,108 vs. 6,326), Hays (October 22: 10,862 vs. 8,792) and Coryell (October 20: 1,867 vs. 1,442). The Dallas and Hidalgo overstatements reported on October 22 remain corrected.

**Name formatting.** The SOS appends name suffixes to the middle name without a space ("RAYJR"), which causes apparent name mismatches against county files.

## 4. How each county posts its rosters

"Cumulative" means each new file contains the earlier ones; "daily" means each file holds only that day.

### 4.1 Counties 1-20

| County | Where | Format | Granularity | Voting date per voter | Download daily? |
|---|---|---|---|---|---|
| Harris | harrisvotes.com, Election Rosters | ZIP of CSV | Early voting one CSV per day, all in one ZIP replaced daily; mail cumulative (the CSV inside the mail ZIP was renamed "Cumulative_BBM_1126_102426" on October 24) | Yes | Recommended |
| Dallas | dallascountyvotes.org, election page | XLSX (CSV on October 21 only) | Cumulative, with earlier days revised; file names change often ("In-Person-Early-Voting-Turnout-Details-Report_10.23.26") | Yes | Recommended |
| Tarrant | tarrantcountytx.gov, current election | ZIP of TSV | Cumulative, same file name | Mail yes; **early voting no** | **Yes** |
| Bexar | elections.bexar.gov Document Center | PDF | Daily | Yes | No |
| Travis | votetravis.gov, one ZIP of daily workbooks | ZIP of XLSX | Daily workbooks in one ZIP | Yes | No (but see §6.4) |
| Collin | collincountytx.gov/Elections/rosters | XLSX | Cumulative, posted the same evening | Yes | Recommended |
| Denton | votedenton.gov, rosters page | XLSX | Early voting cumulative, **undated from October 20**, posted the same evening; mail cumulative | October 19 only | **Yes** |
| Fort Bend | fortbendcountytx.gov, Early Voting Statistics | XLSX | Cumulative from October 21 | Yes | Recommended |
| Hidalgo | hidalgocounty.us Document Center (updated in place) | CSV / XLSX | Cumulative | **No** | **Yes** |
| El Paso | epcountyvotestx.gov, Current Election | CSV (mail), XLSX | Mail cumulative (the newest file's timestamp, 20261023060029, is earlier than the previous one's); early voting **cumulative from day 2, undated** | Mail yes; early voting no | **Yes** |
| Montgomery | elections.mctx.org, Early Voting Roster | ZIP of CSV per day | Daily | Yes | No |
| Williamson | wilcotx.gov Document Center (one document replaced) | XLSX | Cumulative | Yes | Recommended |
| Cameron | cameroncountytx.gov, Elections results | XLSX (per jurisdiction) | Cumulative, replaced; mail list not updated after October 22 | Yes | Recommended |
| Brazoria | brazoriacountyclerktx.gov, Voter Lists | XLSX | Daily; earlier days' files re-posted (October 21 and 22 again on October 24) | Yes | No |
| Bell | bellcountytx.com, Daily Early Voting Reports | PDF | Daily | Yes | No |
| Galveston | galvestonvotes.org, Current Elections | CSV (mail), XLSX | Daily | Yes | No |
| Webb | webbcountytx.gov, Daily Voted List | PDF | Daily, posted the same day | Yes | No |
| Nueces, Lubbock, Hays | link to the SOS portal | SOS CSV | Daily (SOS) | File date | Re-download periodically |

### 4.2 Counties 21-50

| County | Own roster | Format and posting |
|---|---|---|
| McLennan | Yes | One PDF list of early voters, replaced each evening under a timestamped name; no method or date per voter |
| Jefferson | Yes | Excel 97-2003 (.xls) files on jeffcotxvotes.gov: one in-person list per day (VUID and name only) and a cumulative mail list renamed for the day it runs through; six in-person rows mask the VUID as "**********" |
| Smith | Yes | Daily PDFs in a Laserfiche public portal; in-person lists daily, absentee lists cumulative |
| Brazos | Yes | Daily PDFs |
| Grayson | Yes | Daily XLSX check-in reports and absentee lists; October 23 not posted by October 24 evening |
| Randall | Yes | Two PDFs re-posted daily (cumulative early voting roster with check-in time; mail roster printed sideways) |
| Wichita | Yes | PDFs behind a bot challenge that blocks automated downloads; daily check-ins, absentee lists every other day (October 19, 21, 23) |
| Gregg | Yes | PDFs (check-ins, mail list); October 21 still not posted |
| Hunt | Yes | PDFs; cumulative through October 20, daily from October 21; mail ballots appear in the October 23 list with "BBM" as the polling place |
| Tom Green | Yes | PDFs; the October 22 roster and mail list are cumulative; October 23 not posted by October 24 evening |
| Bastrop | Yes | Daily CSVs; mail CSV re-posted under a new name each day |
| Potter | Yes | Two PDFs replaced daily after the polls close; early voting roster cumulative and undated |
| Bowie | Yes | Daily PDFs (ballots by mail received, early vote history with precinct) |
| Victoria | Yes | One PDF and XLSX ("mail and in person"), cumulative, posted under each day's date |
| Henderson | Yes | Daily PDFs, named for the morning they are posted; October 23 not posted by October 24 evening |
| Angelina | Yes | Daily PDFs (early voting history); no mail list; October 23 not posted by October 24 evening |
| Guadalupe | Mail only | Daily returned-mail PDFs, scanned images (not machine-readable) |
| Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker | No | Link to the SOS portal |

## 5. County data compared with state data

### 5.1 Early in-person voters by day

Each day's cell gives the county's count / the state's count.

| County | Oct 19 | Oct 20 | Oct 21 | Oct 22 | Oct 23 | On both | County only | State only (other county) | Agree |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Harris | 104,658 / 104,606 | 99,906 / 99,832 | 93,411 / 93,350 | 86,325 / 86,250 | 95,108 / 60,245 (partial) | 383,972 | 328 | 66 (33) | 99.9% |
| Dallas | 77,854 / 77,829 | 65,084 / 65,066 | 56,092 / 55,881 | 49,469 / 49,446 | 54,294 / 54,270 | 302,357 | 436 | 135 (103) | 99.9% |
| Tarrant | 50,294 / 50,142 | 51,008 / 50,722 | 48,930 / 48,631 | 46,363 / 46,124 | 50,943 / 50,639 | 245,990 | 1,548 | 268 (43) | 99.4% |
| Bexar | 42,764 / 42,039 | 44,733 / 44,442 | 45,076 / 45,071 | 44,151 / 44,182 | 45,636 / 45,660 | 221,077 | 1,283 | 317 (74) | 99.4% |
| Travis | 40,776 / 40,791 | 40,359 / 40,375 | 39,490 / 39,500 | 37,774 / 37,774 | 40,453 / 40,483 | 198,807 | 45 | 116 (96) | 99.9% |
| Collin | 34,386 / 34,258 | 33,875 / 33,749 | 31,135 / 31,027 | 29,459 / 29,349 | 34,586 / 34,482 | 162,521 | 920 | 344 (71) | 99.4% |
| Denton | 34,711 / 34,569 | 31,551 / 31,474 | 28,344 / 28,269 | 25,940 / 25,828 | 30,112 / 29,969 | 149,870 | 788 | 239 (95) | 99.5% |
| Fort Bend | 31,743 / 31,605 | 28,420 / 28,267 | 24,874 / 24,706 | 23,066 / 22,861 | 26,681 / 26,352 | 133,671 | 1,113 | 120 (23) | 99.2% |
| Williamson | 25,695 / 25,712 | 24,237 / 24,281 | 22,195 / 22,217 | 20,908 / 20,931 | 21,896 / 21,922 | 114,875 | 56 | 188 (29) | 99.8% |
| Hidalgo | 17,424 / 17,319 | 17,379 / 17,279 | 15,963 / 15,877 | 14,489 / 14,406 | 14,874 / 14,816 | 79,561 | 568 | 136 (6) | 99.3% |
| El Paso | 17,515 / 17,437 | 16,742 / 16,649 | 14,850 / 14,773 | 13,596 / 13,530 | 13,647 / 13,575 | 75,943 | 407 | 21 (7) | 99.5% |
| Montgomery | 13,568 / 13,561 | 14,162 / 14,168 | 14,407 / 14,408 | 13,991 / 13,987 | 15,210 / 15,210 | 71,310 | 28 | 24 (21) | 100.0% |
| Brazoria | 14,321 / 14,328 | 12,687 / 12,694 | 11,137 / 11,141 | 9,905 / 9,913 | 11,195 / 11,197 | 59,243 | 2 | 30 (24) | 99.9% |
| Galveston | 13,653 / 13,654 | 12,017 / 12,014 | 9,970 / 9,975 | 8,700 / 8,700 | 10,106 / 10,107 | 54,416 | 30 | 34 (22) | 99.9% |
| Cameron | 9,424 / 9,428 | 8,643 / 8,653 | 7,719 / 7,730 | 6,518 / 6,543 | 6,820 / 6,830 | 39,096 | 28 | 88 (25) | 99.8% |
| Bell | 7,111 / 7,095 | 7,145 / 7,132 | 7,059 / 7,056 | 6,779 / 6,768 | 7,335 / 7,345 | 35,371 | 58 | 25 (9) | 99.8% |
| Jefferson | 6,987 / 6,992 | 6,566 / 6,567 | 5,509 / 5,504 | 4,797 / 4,800 | 5,234 / 5,232 | 29,078 | 15 | 17 (5) | 99.9% |
| Smith | 5,861 / 5,827 | 5,980 / 5,944 | 6,016 / 5,969 | 5,806 / 5,770 | 5,975 / 5,931 | 29,434 | 204 | 7 (3) | 99.3% |
| Webb | 4,943 / 4,917 | 4,308 / 4,285 | 3,971 / 3,946 | 3,269 / 3,248 | 3,547 / 3,526 | 19,916 | 122 | 6 (0) | 99.4% |
| Brazos | 4,561 / 4,564 | 4,706 / 4,704 | 4,672 / 4,678 | 4,692 / 4,696 | 5,026 / 5,039 | 23,649 | 8 | 32 (23) | 99.9% |
| Grayson | 5,467 / 5,457 | 4,769 / 4,793 | 4,190 / 4,179 | 3,663 / 3,659 | not posted / 3,862 | 18,014 | 75 | 74 (10) | 99.6% |
| Randall | 4,394 / 4,399 | 4,126 / 4,131 | 3,733 / 3,734 | 3,365 / 3,366 | 3,939 / 3,939 | 19,554 | 3 | 15 (2) | 99.9% |
| Gregg | 3,676 / 2 (partial) | 3,526 / 3,523 | not posted / 2,909 | 2,485 / 2,470 | 2,769 / 2,761 | 8,738 | 42 | 16 (2) | 99.5% |
| Bastrop | 3,143 / 3,142 | 2,888 / 2,889 | 2,480 / 2,479 | 2,376 / 2,377 | 2,484 / 2,483 | 13,363 | 8 | 7 (4) | 99.9% |
| Wichita | 2,945 / 2,945 | 2,783 / 2,784 | 2,615 / 2,617 | 2,228 / 2,228 | 2,223 / 2,223 | 12,794 | 0 | 3 (2) | 100.0% |
| Henderson | 3,257 / 3,257 | 2,823 / 2,819 | 2,313 / 2,311 | 1,995 / 1,999 | not posted / 2,121 | 10,375 | 13 | 11 (10) | 99.9% |
| Tom Green | 2,634 / 2,622 | 2,714 / 2,709 | 2,468 / 2,466 | 2,449 / 2,449 | not posted / 2,392 | 10,236 | 29 | 10 (5) | 99.7% |
| Bowie | 2,023 / 2,027 | 1,963 / 1,961 | 1,917 / 1,917 | 1,859 / 1,859 | 1,888 / 1,892 | 9,647 | 3 | 9 (1) | 99.9% |
| Angelina | 2,566 / 2,567 | 2,352 / 2,353 | 2,006 / 2,006 | 1,795 / 1,795 | not posted / 1,954 | 8,719 | 0 | 2 (2) | 100.0% |
| Victoria | 1,660 / 1,660 | 1,758 / 1,758 | 1,799 / 1,802 | 1,568 / 1,569 | 1,719 / 1,728 | 8,503 | 1 | 14 (1) | 99.8% |
| Hunt | 1,839 / 1,827 | 2,044 / 1,724 (partial) | 2,032 / 2,043 | 1,789 / 1,798 | 2,104 / 2,118 | 7,741 | 23 | 45 (9) | 99.4% |
| Potter | (cumulative) / 2,180 | (cumulative) / 1,970 | 1,737 / 1,739 | 1,482 / 1,482 | 1,709 / 1,710 | 4,928 | 0 | 3 (2) | 99.9% |
| McLennan | no method or date / 4,912 | / 4,963 | / 4,874 | / 4,627 | / 4,935 | | | | |

Counts are distinct VUIDs (county voters without a VUID are excluded). "On both", "County only" and "State only" add up the days on which both sources are complete; "Agree" is the voters on both lists as a share of the larger list. Hunt's October 23 count excludes its 61 mail ballots. McLennan's list (26,692 voters, posted the evening of October 23) contains all but 79 of the state's 25,312 McLennan in-person and mail voters for October 14-23. Potter's first two snapshots were posted on the same day, so its October 19 and 20 cannot be separated.

Most state-only voters are voters of other counties (§6.1) or, in Tarrant, voters the county lists a day later than the state (91 on October 19, 43 on October 20) and redacted voters (§6.2). Most county-only voters are a few dozen to a few hundred per day that the state has not yet received.

### 5.2 Mail ballots

The SOS mail data begins October 14 and is dated by when the state received it, days after the county (§3); county mail lists go back to the first ballots received (September 21-22).

| County | County mail | State mail (Oct 14-23) | On both | State only (other county) |
|---|---:|---:|---:|---:|
| Harris | 32,315 | 12,940 | 12,844 | 96 |
| Bexar | 18,953 | 7,310 | 7,196 | 114 (82 dated Oct 15) |
| Tarrant | 15,871 | 10,304 | 10,156 | 148 (1); 130 match redacted voters, §6.2 |
| Dallas | 15,247 | 6,533 | 6,452 | 81 (1) |
| Travis | 9,993 | 7,992 | 5,995 | 1,997 (5); see §6.4 |
| Montgomery | 6,958 | 3,128 | 3,028 | 100 |
| El Paso | 6,358 | 1,466 | 1,456 | 10 |
| Williamson | 5,671 | 5,260 | 5,245 | 15 |
| Collin | 5,403 | 3,430 | 2,786 | 644 (622 dated Oct 23, after the county's list) |
| Denton | 5,390 | 4,026 | 3,766 | 260 (1; 245 dated Oct 23) |
| Fort Bend | 4,068 | 3,245 | 3,224 | 21 |
| Hidalgo | 3,768 | 1,593 | 1,590 | 3 |
| Galveston | 2,860 | 1,110 | 1,100 | 10 (1) |
| Bell | 2,703 | 2,442 | 2,360 | 82 |
| Brazoria | 2,604 | 711 | 711 | 0 |
| Smith | 1,910 | 1,148 | 1,132 | 16 (1) |
| Randall | 1,811 | 676 | 672 | 4 |
| Jefferson | 1,754 | 1,655 | 1,647 | 8 |
| Tom Green | 1,378 | 635 | 569 | 66 (56 dated Oct 23; county list through Oct 22) |
| Victoria | 1,345 | 505 | 502 | 3 |
| Gregg | 1,207 | 1,013 | 1,002 | 11 |
| Cameron | 1,167 | 1,213 | 809 | 404 (county list through Oct 22) |
| Grayson | 1,082 | 658 | 480 | 178 (176 dated Oct 23; county list through Oct 22) |
| Bastrop | 990 | 557 | 556 | 1 (1) |
| Wichita | 971 | 548 | 412 | 136 (101 dated Oct 22) |
| Potter | 937 | 332 | 332 | 0 |
| Bowie | 800 | 207 | 179 | 28 |
| Webb | 529 | 410 | 381 | 29 (28 dated Oct 14) |
| Hunt | 219 | 468 | 208 | 260 |

Brazos (1,428 in the county's lists) has 8 mail ballots at the state. Henderson and Angelina post no mail list; the state has 225 and 746 mail voters for them.

### 5.3 Counties on the state portal only

| County | SOS days on file | Oct 19 | Oct 20 | Oct 21 | Oct 22 | Oct 23 | Mail Oct 14-23 |
|---|---|---:|---:|---:|---:|---:|---:|
| Hays | Oct 14-16, 19-23 | 10,256 | 9,910 | 9,417 | 8,792 | 8,807 | 611 |
| Lubbock | Oct 14-16, 19-23 | 8,856 | 8,642 | 7,902 | 7,291 | 7,152 | 592 |
| Ellis | Oct 14-16, 19-23 | 7,902 | 7,123 | 6,234 | 5,518 | 6,326 | 577 |
| Nueces | Oct 14-17, 19-23 | 7,617 | 6,929 | 6,444 | 5,614 | 6,474 | 2,710 |
| Comal | Oct 14-17, 19-23 | 5,876 | 5,803 | 5,816 | 5,635 | 5,885 | 1,693 |
| Parker | Oct 14-16, 19-23 | 5,953 | 5,711 | 5,203 | 4,991 | 5,673 | 636 |
| Johnson | Oct 15, 16, 18-23 | 5,871 | 5,440 | 4,891 | 4,358 | 5,118 | 208 |
| Rockwall | Oct 14-16, 19-23 | 4,968 | 4,338 | 3,857 | 3,305 | 3,977 | 313 |
| Guadalupe | Oct 14-16, 19-23 | 4,780 | 4,762 | 4,617 | 4,387 | 4,617 | 659 |
| Kaufman | Oct 19-23 | 3,904 | 4,235 | 3,955 | 3,710 | 4,184 | 711 |
| Taylor | Oct 14, 15, 19-23 | 3,186 | 3,202 | 2,910 | 2,673 | 2,828 | 642 |
| Midland | Oct 14-16, 19-23 | 2,963 | 3,117 | 2,863 | 2,581 | 3,143 | 466 |
| Orange | Oct 19-23 | 2,200 | 2,236 | 1,881 | 1,567 | 1,956 | 542 |
| Liberty | Oct 14-16, 19-23 | 1,952 | 1,881 | 1,690 | 1,657 | 1,923 | 216 |
| Ector | Oct 15, 19-23 | 1,809 | 1,853 | 1,660 | 1,562 | 1,824 | 2 |
| Coryell | Oct 14-16, 19-23 | 1,802 | 1,442 | 1,233 | 1,096 | 1,242 | 183 |
| Walker | Oct 14-16, 19-23 | 1,740 | 1,717 | 1,563 | 1,382 | 1,494 | 129 |

For these counties the state is the only published source. All 17 have state files for October 19-23 and now report mail ballots (Ector only 2).

## 6. Anomalies between county and state data

### 6.1 State county files include voters of other counties

Across October 14-23, 1,065 voters (1,046 in-person, 19 mail) in the SOS county files appear on a different county's own roster, with the same first name and that county's precinct. The largest numbers are in the SOS files for Dallas (103), Travis (101), Denton (96), Bexar (74), Collin (71), Hays (53), Tarrant (44), Harris (43), Johnson (43) and Parker (42). No voter appears in two SOS county files, so these are records assigned to the wrong county's file, not duplicates. Anyone using SOS county files alone will slightly overcount each county.

### 6.2 State files identify voters Tarrant County redacts

Tarrant replaces the name, VUID and address of protected voters with "--Redacted--", keeping only precinct and date: 264 records in its current files (202 mail, 62 early in-person). No other county redacts identities; Harris hides the address (only) of 2,095 named voters.

- **Mail:** the SOS Tarrant mail files list 147 mail voters who are not on Tarrant's mail roster (after excluding one voter of another county). For 130 of them there is a redacted Tarrant mail record with the same precinct and the same date.
- **In person:** the SOS Tarrant files list 84 in-person voters (besides 43 of other counties) who are not on Tarrant's roster on any day; 61 of them match a redacted Tarrant in-person record by precinct.
- **Fewer redacted records each day:** Tarrant's cumulative in-person list held 104 redacted records on October 20, 99 on October 21, 79 on October 22, 69 on October 23 and 62 on October 24; its cumulative mail list held 215 in the October 23 afternoon file and 202 in the evening file. A cumulative list should not shrink.

With five days of data, the match remains close for both voting methods. This continues to indicate that the state publishes the name, VUID and precinct of voters whose identities Tarrant withholds. It cannot be proven record by record, because the redacted rows carry no identifier. Neither source exposes an address.

### 6.3 Reporting gaps in the state data

- **In-person:** Harris's October 23 (60,245 of 95,108); Gregg's October 19 (2 of 3,676); Hunt's October 20 (1,724 of 2,044). Every county has a state file for every day.
- **Mail:** Brazos (8 of 1,428) and Ector (2) report almost none to the state.
- **Turnout table vs. CSV:** Travis, Ellis, Hays, Coryell (§3).

### 6.4 Mail ballots in state data but not in the county's mail list

- **Travis:** Travis's roster ZIP holds mail lists for October 14, 19, 20, 21 and 23, but none for October 15, 16 or 22. The state lists 1,992 Travis mail voters who are on none of Travis's lists, including 1,208 dated October 19-20 and 486 dated October 23 by the state. These are most likely the ballots of the missing days; Travis should post those days' lists.
- **Lists not yet updated:** Collin 622, Grayson 176, Cameron 115 and Tom Green 56 state-only mail voters are dated after the county's latest list. Denton's 245 dated October 23 are not in its October 23 list, posted the same evening.
- **Cameron:** besides those, 289 more Cameron mail voters (215 dated October 21) are not on the county's mail list.
- **Hunt:** the state has 468 Hunt mail voters, the county's files 219 (its mail lists and the October 23 "BBM" rows); 260 state mail voters are in neither.
- **Wichita:** 136 state mail voters (101 dated October 22) are not on Wichita's cumulative October 23 absentee list.
- **Elsewhere:** Bexar 114 (82 dated October 15), Montgomery 100, Harris 96, Bell 82, Dallas 80, Webb 29, Bowie 28, Fort Bend 21, Smith 15, Williamson 15, Gregg 11, El Paso 10, Galveston 9, Jefferson 8, and 1-4 in three other counties.

### 6.5 Precinct differences

- **Williamson:** the October 23 file, like the October 22 file, gives a precinct different from the state's for 4,384 mail voters; the October 21 file matched the state for all of them, and every in-person row still matches. The county's mail precinct column appears to have been scrambled since October 22.
- **Bowie:** the October 20, 21 and 23 early vote reports give a precinct that differs from the state's for 1,566, 1,711 and 1,441 voters (4,718); the October 22 report matched for all 1,859. The values were checked against the PDFs.
- **Bexar:** 65 voters in the October 22 check-in report (one or none in each other report).
- **Elsewhere,** on matched voters: Harris 771 (741 in-person), Galveston 142 (111 of them in its October 19 file), Tarrant 118, El Paso 61 (all in-person), Smith 37, Hunt 14, Montgomery 8, Travis 6, Denton 4, Brazoria and Dallas 2, and one each in Bastrop, Henderson and Potter.

Denton and Gregg write the precinct with a two-digit split appended ("418603" for 4186); this is formatting, not a difference.

### 6.6 Surnames

On matched voters, the surname differs between the county and the state in 35 Collin, 33 Tarrant, 25 Williamson, 23 Bexar, 23 Denton and 21 Cameron records, and in 15 or fewer elsewhere (300 in all). In all but 8 of these records the two names share at least one name: they are name changes recorded in one system and not the other, or formatting (counties that write names "FIRST MIDDLE LAST").

### 6.7 One VUID, two different people

- **Within one county's files:** one VUID is listed for two different voters in Harris's cumulative mail file and in a Brazoria mail file, and for two different people checking in at Bexar on October 19 and October 23.
- **On two counties' rosters:** one VUID is a mail ballot received October 21 on both Bell's and Randall's mail lists, for two people with different names.
- **SOS file vs. a county roster:** 21 VUIDs name a voter in an SOS file and an entirely different person (no name in common) on a county's roster. Eight are within one county (Bexar 2, Brazoria, Dallas, El Paso, Harris, Montgomery, Tarrant). Thirteen are across counties: seven on Bell's roster (against the SOS Coryell, Fort Bend (2), Lubbock, Randall (2) and Victoria files), and one each between Harris and Bell, Dallas and Collin, El Paso and Harris, Harris and Hidalgo, Tarrant and Hidalgo, and El Paso and Montgomery.

All seven Bell VUIDs and the Hidalgo/Harris, Hidalgo/Tarrant and Montgomery/El Paso VUIDs are recent registrations in one numeric range (2223365544-2223382143). Bell County's roster gives these VUIDs to different people than the state's files do, so the Bell County Elections office and the SOS should compare them first.

### 6.8 One voter in two counties' own rosters

All six are the same person in both counties (same first and last name):

| Counties | First county | Second county | State's files |
|---|---|---|---|
| Tarrant, Smith | in person, in Tarrant's list from October 20 (precinct 1224) | in person October 22 (Smith precinct 245) | SOS Smith October 20 file, with the Tarrant precinct |
| Tarrant, Dallas | in person, in Tarrant's list from October 22 (precinct 1010) | in person October 23 (Dallas precinct 1146) | SOS Dallas October 22 file, with the Tarrant precinct |
| Hidalgo, Bexar | in person, in Hidalgo's list from October 23 (precinct 151) | in person October 23 (Bexar precinct 2143) | SOS Bexar October 23 file, with the Hidalgo precinct |
| Harris, Travis | mail ballot received September 30 | in person October 23 | SOS Travis October 23 file (in person) |
| Bexar, Webb | in person October 19 | in person October 19 | reported since October 20 |
| Cameron, Hidalgo | mail ballot received October 13 | mail ballot (no date) | reported since October 20 |

### 6.9 Mail ballot and in-person vote by the same voter

Seventeen voters are listed both as a returned mail ballot and as an early in-person voter in the same county:

- **El Paso 5:** mail returned October 6-20; in person by October 19 (2), October 21 (2) and October 22 (1).
- **Dallas 3:** mail October 9; in person October 19, 20 and 22.
- **Randall 3:** on the mail roster with no received date; in person October 20 (2) and October 23.
- **Tarrant 2:** in person by October 19 and by October 20; mail ballots recorded October 21 and 22.
- **Bastrop 1:** mail October 16; in person October 22.
- **Grayson 1:** mail October 2; in person October 20.
- **Jefferson 1:** on the cumulative mail list; in person October 20. The state lists only the in-person vote.
- **Travis 1:** mail October 7; in person October 21.

A voter whose mail ballot has been accepted may not vote in person; these may be cancelled mail ballots, provisional votes or data errors.

### 6.10 Duplicates within county files

- **Same-day duplicate check-ins:** Grayson 8, Bexar 6, Collin 3, Jefferson 3, and 1 each in Dallas, Denton, Galveston, Tom Green, Travis and Webb. Jefferson also lists one VUID in its October 20 and October 23 in-person files (the state lists only October 20).
- **Mail ballot received twice:** 10 Bexar voters appear on two days' received-ballot lists; Bastrop 3 (its cumulative mail CSV lists each with two dates), Travis 2, Gregg 2, and 1 each in Brazoria, Brazos, Galveston, Harris and Montgomery.

### 6.11 Identifiers that are not state VUIDs

| County | Records | ID length | Lists |
|---|---:|---|---|
| Tarrant | 334 | 8 digits (one 7) | mail (266), in-person (68) |
| Collin | 166 | 6, 7, 9 digits | in-person (160), mail (3), provisional (3) |
| Denton | 102 | 7 digits | in-person |
| El Paso | 53 | 7 digits | mail (44), in-person (9) |
| Hidalgo | 43 | 8-9 digits | mail (33), in-person (10) |
| Grayson | 40 | 6, 8, 9 digits | in-person (36), mail (4) |
| Webb | 26 | 8 digits | mail (22), in-person (4) |
| Smith | 16 | 7 digits | in-person |
| Bowie | 8 | 9 or 11 digits | mail |
| Gregg | 8 | 6, 7, 11 digits | in-person |
| Bexar | 7 | 8, 9, 11 digits | in-person |
| Jefferson | 4 | 6, 7, 8 digits | in-person |
| Tom Green | 3 | 9 digits | mail |
| Henderson | 1 | 8 digits | in-person |

These are likely county-internal numbers for voters whose state VUID had not been assigned or synchronized, or (for the 9- and 11-digit cases) a VUID with a digit missing or added. The state files contain none, so these voters cannot be matched by ID; for 13 of them the state lists a voter with the same name and precinct under a 10-digit VUID (see the supplement). Jefferson also masks six VUIDs as "**********" (the names are shown).

## 7. Other county data quality notes

- **Blank VUIDs** (named voters with no ID): Harris 397 mail ballots; Collin 161 (147 provisional); Grayson 50 in-person; Bell 24 mail; Dallas 19; Bexar 18 in-person; Fort Bend 8 mail; Hunt 7; Jefferson 6 (masked); Bastrop and Hidalgo 2. Travis's 417 and Montgomery's 143 Limited Ballot voters have no VUID by design. Tarrant's 264 redacted records also have no VUID (§6.2).
- **No voting date:** Tarrant's, Hidalgo's and Potter's early voting lists, Denton's from October 20, El Paso's from day 2 and McLennan's list carry no date per voter. Jefferson's lists carry neither date nor precinct (the in-person date is the file's). Smith's, Wichita's and Jefferson's mail lists are cumulative, so the date in their file names is the list date; Randall's mail roster lists three voters without a received date.
- **Not yet posted** by the evening of October 24: Gregg's October 21 roster; Grayson's, Tom Green's, Henderson's and Angelina's October 23 rosters; Travis's October 15, 16 and 22 mail lists; Cameron's mail list after October 22.
- **Files replaced or re-posted:** Victoria, Potter, Tom Green and Jefferson (mail) post cumulative files; Randall re-posts its two rosters; McLennan posts one list under a new timestamped name; Bastrop renames its mail CSV daily; Brazoria re-posts earlier days' lists.
- **Same-day posting:** Collin, Denton, Webb, McLennan, Potter, Randall and Bastrop post lists for the current day during that day or that evening; they may change when re-posted.
- **Hard-to-read formats:** Guadalupe's mail lists are scanned images; Wichita's PDFs sit behind a bot challenge and wrap each absentee record onto two lines; Gregg prints long polling-place names past their cells; Smith's lists are in a JavaScript document portal; Randall prints its mail roster sideways; Grayson's check-in workbook is a printed report with its column titles on separate rows; Jefferson posts Excel 97-2003 (.xls) files.

## 8. Recommendations

1. **Download daily** through Election Day: Tarrant, Hidalgo, Denton, El Paso and Potter (no voting date per voter), Victoria, Tom Green, Jefferson and McLennan (cumulative files replaced), and Collin, Williamson, Cameron, Dallas, Fort Bend and Harris (latest file only). Save same-named files with the download time.
2. **Prefer county files** for counts and voter lists; use the SOS portal for the 16 counties that post nothing else and as a cross-check. Re-download SOS days for at least two days after the date, since the state fills in and revises them, and do not read SOS mail dates as received dates or the turnout table as a substitute for the county CSVs.
3. **When using SOS county files,** expect voters of other counties in each file (§6.1) and check them against neighboring counties before treating them as anomalies.
4. **Refer to the counties:**
   - the six voters recorded in two counties (§6.8);
   - the 17 voters with both a mail ballot and an in-person vote;
   - the shared VUIDs (§6.7), first those on Bell's roster;
   - Williamson's mail precincts since October 22, and Bowie's October 20, 21 and 23 precincts;
   - Hunt's mail ballots in its in-person list;
   - Travis's missing October 15, 16 and 22 mail lists;
   - Bexar's mail ballots received twice;
   - the precinct differences in Harris, Galveston, Tarrant, Bexar and El Paso.
5. **Refer to Tarrant County and the Secretary of State:**
   - the publication by the SOS of voters Tarrant redacts, and the shrinking counts of redacted records (§6.2);
   - the SOS files' inclusion of other counties' voters (§6.1), including the votes filed under a second county in §6.8;
   - the turnout table's Travis, Ellis and Hays figures (§3).
6. **Repeat this comparison** after early voting closes and after the canvass.

## Appendix A. Files

Downloaded files are saved per county:

- county files are saved as `G26_<county file name>`; files a county posts under the same name every day get the download date (`_2026-10-24`), or the download date and time (`_2026-10-24-13-11-59`) for cumulative lists that carry no voting dates (Hidalgo, Potter);
- SOS files are saved as `G26_<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv`; when the state revises a day, the earlier copy is kept.

The files load in Election Explorer (File → Load Voter Roster); the comparisons in this report were produced from the loader's normalized output. The companion supplement (`Texas_Early_Voting_Roster_Review_2026-10-24_Supplement`) lists each anomalous record's VUID and its location in the county's and the state's files.

## Appendix B. Caveats

- In-person comparisons cover October 19-23; where the state's file for a day is partial, that day is shown but left out of the totals.
- County files were downloaded October 20-24 at different times; several counties post same-day lists that are later replaced, and counties keep revising earlier days.
- Day-level counts for cumulative, undated county lists were recovered from consecutive snapshots and attribute voters added late to the day they first appear (Tarrant, §5.1).
- A match between two records means the same VUID; name and precinct differences were checked separately. A match by name, precinct or date without a common VUID (§6.2, §6.11) is an inference, not a confirmed identity.
- This review describes data as published. It does not establish that any voter voted improperly; the records flagged in §6.5-§6.10 have ordinary clerical explanations as well as serious ones, and only the counties can resolve them.
