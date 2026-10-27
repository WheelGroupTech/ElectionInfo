# Texas Early Voting Rosters: County and State Data Review

**Election:** November 3, 2026 General Election\
**Data through:** Sunday, October 25, 2026 (seventh day of in-person early voting)\
**Prepared:** October 26, 2026 (updates the reviews of October 20-25; county files as posted by 3:02 PM October 26, state files as posted by 9:50 PM October 26)\
**Scope:** the 50 most populous Texas counties and the Texas Secretary of State (SOS) roster portal

## 1. Summary

Texas Election Code §87.121 requires each county to post its early voting rosters (in-person and by mail) daily and to submit them for statewide elections to the Secretary of State, which republishes them. This update compares the county and state lists voter by voter (by Voter Unique Identifier, VUID) for the first seven days of in-person voting, October 19-25, in the 50 most populous counties.

Key findings:

1. **The two sources agree closely, but not completely, in every county.** Over every day for which both are complete, the county's and the state's lists of early in-person voters agree on 99.1% or more of voters in all 32 counties with dated county rosters. In none of them do the two lists match exactly: every county has at least one voter on one list and not the other. (In this review a 100% figure is shown only when no voter is on one list and not the other.) Seven voters are an in-person voter in one source and a mail voter in the other (Hunt 5, Hidalgo 1, Cameron 1).
2. **The state's files for October 24 are now complete, and October 25 is in.** Harris (67,150 of the county's 67,242 voters), Fort Bend (17,874 of 18,169), Bexar (40,710 of 40,624), Galveston (7,260 of 7,267), Lubbock and Ector, partial or missing in the last review, now have full October 24 files. Every one of the 50 counties has a state in-person file for each of October 19-25, and for October 25 every county's state file holds at least 90% of the county's own count.
3. **The state keeps revising earlier days, and moves mail ballots between days.** Since the evening of October 25, 214 county-day counts for October 14-24 changed in the state's files: besides the October 24 files filling in, 161 mail counts changed (net +7,376 mail voters), 62 of them downward. Williamson's mail voters for October 21 fell from 399 to 54 and for October 22 from 1,201 to 708 while October 24 rose from 694 to 1,030; Cameron's October 14-16 mail files went from 334 voters to 21; Bexar's October 19 mail file went from 1 voter to 1,833.
4. **Eight voters are recorded as voting in two counties.** New since the last review: one voter on Bexar's in-person roster for October 19 and on Cameron's for October 25 (same first name, different last name), whom the state lists only in Cameron's October 25 file. The other seven are unchanged (§6.8).
5. **The turnout table still overstates Travis, Ellis, Hays and Coryell, and now Henderson.** For October 23 the portal's table shows 50,267 Travis in-person voters, against 40,483 in the state's own Travis file and 40,453 in Travis County's roster. For October 25 it shows 513 Henderson voters, against 441 in both the state's file and the county's list.
6. **Henderson changed its roster layout and posted October 23-25 at once.** From October 23 its daily PDF has the columns Voter_ID, Voter_Name, Voting Precinct and Timestamp (October 23) or Date (October 24-25). Election Explorer's roster loader does not yet read the October 23 dates or the October 25 list; for this review they were read from the PDFs' text. Henderson's lists agree with the state's on 99.8% of voters.
7. **Guadalupe now posts its in-person rosters.** On October 26 it posted early voter check-in PDFs for October 19-23. They are text PDFs that print each line as a single run of text, which the roster loader cannot read yet; read separately for this review, they agree with the state's files on 98.8% of voters (23,132 on both, 263 only in Guadalupe's lists, 37 only in the state's).
8. **Williamson's mail precincts are still wrong.** Its October 25 file gives a precinct different from the state's for 4,689 mail voters; in-person rows match.
9. **Bowie's October 20, 21 and 23 reports disagree with the state** on the precinct of 4,718 voters; its October 24 and 25 reports, posted October 26, match.
10. **Collin's mail list was not updated.** Its latest mail list still runs "through October 24"; 602 state mail voters (besides one voter of another county) are not on it, 425 of them dated October 24 and 155 October 25 by the state.
11. **The state's county files contain voters of other counties.** Across October 14-25, 1,500 voters (1,475 in-person, 25 mail) appear in an SOS county file while being on a different county's own roster, with that county's precinct.
12. **The state's files continue to name voters that Tarrant County redacts,** while Tarrant's own cumulative in-person list holds fewer redacted records each day: 104 on October 20, 55 on October 26.
13. **Records that warrant review by the counties** (none is proof of wrongdoing; most have clerical explanations):
    - 15 voters listed both as a returned mail ballot and as an early in-person voter in the same county (El Paso 5, Dallas 4, Tarrant 2, Bastrop, Grayson, Jefferson and Travis 1 each);
    - 35 VUIDs that name entirely different people in two records, 7 of them on Bell's roster;
    - 27 same-day duplicate in-person check-ins in 10 counties, and 27 mail ballots listed as received on two days (10 in Bexar);
    - precinct differences besides Williamson and Bowie: Harris 1,150, Tarrant 164, Galveston 157, El Paso 83, Bexar 68.

## 2. Scope and method

**Counties (by 2025 Census population):**

- Ranks 1-20: Harris, Dallas, Tarrant, Bexar, Travis, Collin, Denton, Fort Bend, Hidalgo, El Paso, Montgomery, Williamson, Cameron, Brazoria, Bell, Galveston, Nueces, Lubbock, Hays, Webb.
- Ranks 21-50: McLennan, Jefferson, Smith, Brazos, Ellis, Johnson, Kaufman, Comal, Guadalupe, Midland, Parker, Ector, Grayson, Randall, Taylor, Rockwall, Wichita, Gregg, Hunt, Liberty, Tom Green, Bastrop, Potter, Bowie, Victoria, Henderson, Angelina, Orange, Coryell, Walker.

**Sources:**

- each county's own roster files, downloaded daily October 20-26. 34 counties post their own rosters; 33 are compared in the tables below. Guadalupe's mail lists are scanned images and its new in-person lists are compared separately (§1, item 7). The other 16 (Nueces, Lubbock, Hays, Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker) link to the SOS portal;
- the SOS portal's per-county early voting CSVs for October 14-25, for all 50 counties, all days re-downloaded on the evening of October 26, since the state revises them;
- the SOS turnout tables for October 19-25.

**Method.** All files were read with Election Explorer's roster loader, which normalizes each county's layout to VUID, precinct, name, voting method and date. Three files the loader does not yet read were read from the PDFs' text instead: Henderson's October 23 list (its dates are in a "Timestamp" column), Henderson's October 25 list (titled "Voter Turnout") and Tom Green's October 25 mail list (printed without column titles). For every county file, the number of VUIDs loaded matches an independent count of the 10-digit identifiers in the raw file exactly for 325 of the 327 files; the other two differ by one identifier each. The lists were then compared by VUID:

- *early in-person, by day*: the county's in-person voters for each of October 19-25 vs. the SOS's IN-PERSON voters for that date. For counties whose lists are cumulative and undated (Tarrant, Hidalgo, Denton, El Paso, Potter), each day was recovered from consecutive daily snapshots (a day's voters = that day's snapshot minus the previous day's). A day counts toward the totals only when both sources have it and the state's file holds at least 90% of the county's count;
- *mail*: the county's latest mail list (or all daily files) vs. the SOS's MAIL-IN voters for October 14-25.

"County-only" and "state-only" voters were checked against the other 49 counties' files. Name and precinct comparisons ignore formatting (case, spacing, leading zeros, "Precinct"/"P"/"S" prefixes, and a two-digit precinct split such as "418603" for precinct 4186). Voter names and VUIDs are deliberately not reproduced in this report; the companion supplement lists the VUIDs and file locations of the anomalies for county and state staff.

## 3. The Secretary of State portal

**Where:** "Election Information & Turnout Data", `https://goelect.txelections.civixapps.com/ivis-evr-ui/evr`. Pick an election and an early voting date, then *Submit*: a table of registered voters, in-person and mail turnout per county, with a **CSV Download** per county under *Voter Details Report*.

**How the data is served.** The page fetches JSON whose `upload` field is the file in base64:

| Data | Request |
|---|---|
| Elections, early voting dates and county ids | `/api-ivis-system/api/v1/getFile?type=EVR_ELECTION` |
| Turnout table for one date | `/api-ivis-system/api/v1/getFile?type=EVR_EARLYVOTING&electionId=53815&electionDate=10/25/2026` |
| One county's voter CSV for one date | `/api-ivis-system/api/v1/getFileByFormat?type=EVR_EARLYVOTING&electionId=53815&countyId=<id>&county=<NAME>&electionDate=<MM/DD/YYYY>&format=CSV` |

County ids follow alphabetical order, except that the portal sorts McLennan after McCulloch (id 161). An empty response means the county reported nothing for that day.

**File contents.** `VOTER_NAME` ("LAST, FIRST MIDDLE"), `ID_VOTER` (VUID), `VOTING_METHOD` (`IN-PERSON` / `MAIL-IN`), `PRECINCT`. There is **no address and no date column**. A county CSV holds only the voters credited on that date.

**Timeliness and revisions.** A day's files fill in over the following day or two. By the evening of October 26, all 50 counties had an in-person file for each of October 19-25, and none of the October 24 or 25 files was partial against the county's own count. Re-downloading every day for every county on October 26 changed 203 files; compared with the files used in the October 25 review, 214 county-day counts for October 14-24 differ. Besides the October 24 in-person files filling in (Harris from 14,826 to 67,150 voters, Bexar from 77 to 40,710, Fort Bend from 733 to 17,874, Galveston from 9 to 7,260, Lubbock from 10 to 4,580, Ector from none to 1,598), 161 county-day mail counts changed, 62 of them downward: the state re-dates mail ballots as well as adding them (Williamson October 21: 399 to 54, October 22: 1,201 to 708, October 24: 694 to 1,030; Cameron October 14-16: 334 to 21; Bexar October 19: 1 to 1,833; Randall October 14: 122 to 992).

**Mail dates.** The date of an SOS mail file is not the day the county received the ballot, and, as above, the state moves ballots from one day's file to another. In Travis, the state's October 16 mail voters are almost all ballots the county lists as received October 13, and its October 19 mail voters mostly the county's October 14 ballots. Mail comparisons in this review are therefore made over all days together.

**Turnout table vs. CSVs.** The statewide table now shows 784,088 in-person voters for October 19, 743,976 for October 20, 683,007 for October 21, 632,672 for October 22, 696,897 for October 23, 516,360 for October 24 and 267,116 for October 25. It agrees with the state's CSVs for every county and day except Travis (October 23: table 50,267, CSV 40,483), Ellis (October 23: 8,108 vs. 6,326), Hays (October 22: 10,862 vs. 8,792), Coryell (October 20: 1,867 vs. 1,442) and Henderson (October 25: 513 vs. 441).

**Name formatting.** The SOS appends name suffixes to the middle name without a space ("RAYJR"), which causes apparent name mismatches against county files.

## 4. How each county posts its rosters

"Cumulative" means each new file contains the earlier ones; "daily" means each file holds only that day.

### 4.1 Counties 1-20

| County | Where | Format | Granularity | Voting date per voter | Download daily? |
|---|---|---|---|---|---|
| Harris | harrisvotes.com, Election Rosters | ZIP of CSV | Early voting one CSV per day, all in one ZIP replaced daily; mail cumulative | Yes | Recommended |
| Dallas | dallascountyvotes.org, election page | XLSX (CSV on October 21 only) | Cumulative, with earlier days revised; file names change often ("In_Person-Early-Voting-Deatails-REport_10.25.26") | Yes | Recommended |
| Tarrant | tarrantcountytx.gov, current election | ZIP of TSV | Cumulative, same file name; no new mail file on October 26 | Mail yes; **early voting no** | **Yes** |
| Bexar | elections.bexar.gov Document Center | PDF | Daily | Yes | No |
| Travis | votetravis.gov, one ZIP of daily workbooks | ZIP of XLSX | Daily workbooks in one ZIP, replaced each morning | Yes | No |
| Collin | collincountytx.gov/Elections/rosters | XLSX | Cumulative, posted the same evening; mail list not updated after October 24 | Yes | Recommended |
| Denton | votedenton.gov, rosters page | XLSX | Early voting cumulative, **undated from October 20**, posted the same evening; mail cumulative | October 19 only | **Yes** |
| Fort Bend | fortbendcountytx.gov, Early Voting Statistics | XLSX | Cumulative from October 21; no mail list after October 23 | Yes | Recommended |
| Hidalgo | hidalgocounty.us Document Center (updated in place) | CSV / XLSX | Cumulative | **No** | **Yes** |
| El Paso | epcountyvotestx.gov, Current Election | CSV (mail), XLSX | Mail cumulative; early voting **cumulative from day 2, undated** | Mail yes; early voting no | **Yes** |
| Montgomery | elections.mctx.org, Early Voting Roster | ZIP of CSV per day | Daily | Yes | No |
| Williamson | wilcotx.gov Document Center (one document replaced) | XLSX | Cumulative | Yes | Recommended |
| Cameron | cameroncountytx.gov, Elections results | XLSX (per jurisdiction) | Cumulative, replaced; mail list through October 23 | Yes | Recommended |
| Brazoria | brazoriacountyclerktx.gov, Voter Lists | XLSX | Daily; earlier days' files re-posted (October 21 and 22 again on October 26) | Yes | No |
| Bell | bellcountytx.com, Daily Early Voting Reports | PDF | Daily | Yes | No |
| Galveston | galvestonvotes.org, Current Elections | CSV (mail), XLSX | Daily | Yes | No |
| Webb | webbcountytx.gov, Daily Voted List | PDF | Daily, posted the same day | Yes | No |
| Nueces, Lubbock, Hays | link to the SOS portal | SOS CSV | Daily (SOS) | File date | Re-download periodically |

### 4.2 Counties 21-50

| County | Own roster | Format and posting |
|---|---|---|
| McLennan | Yes | One PDF list of early voters, replaced each evening under a timestamped name; no method or date per voter |
| Jefferson | Yes | Excel 97-2003 (.xls) files on jeffcotxvotes.gov: one in-person list per day (VUID and name only) and a cumulative mail list renamed for the day it runs through (latest: through October 23); eight in-person rows mask the VUID as "**********" |
| Smith | Yes | Daily PDFs in a Laserfiche public portal; in-person lists daily, absentee lists cumulative |
| Brazos | Yes | Daily PDFs |
| Grayson | Yes | Daily XLSX check-in reports and absentee lists |
| Randall | Yes | Two PDFs re-posted daily (cumulative early voting roster with check-in time; mail roster printed sideways); mail roster not updated for October 25 |
| Wichita | Yes | PDFs behind a bot challenge that blocks automated downloads; daily check-ins, absentee lists every other day (latest October 23) |
| Gregg | Yes | PDFs (check-ins, mail list); October 21 still not posted |
| Hunt | Yes | PDFs; cumulative through October 20, daily from October 21; mail ballots appear in the October 23 and 24 lists with "BBM" as the polling place; dates written "10.24.26" from October 24 |
| Tom Green | Yes | PDFs; the roster and mail list are cumulative (latest posted October 26, through October 25); the October 25 mail list has no column titles |
| Bastrop | Yes | Daily CSVs; mail CSV re-posted under a new name each day |
| Potter | Yes | Two PDFs replaced daily after the polls close; early voting roster cumulative and undated; mail roster not updated since October 23 |
| Bowie | Yes | Daily PDFs (ballots by mail received, early vote history with precinct); October 24 and 25 early vote history posted October 26 |
| Victoria | Yes | One PDF and XLSX ("mail and in person"), cumulative, posted under each day's date |
| Henderson | Yes | Daily PDFs; October 23-25 posted together on October 26 in a new layout (Voter_ID, Voter_Name, Voting Precinct, Timestamp or Date; the October 25 list is titled "Voter Turnout") |
| Angelina | Yes | Daily PDFs (early voting history); no mail list; October 23-25 not posted by October 26 afternoon |
| Guadalupe | Yes | Daily returned-mail PDFs, scanned images (not machine-readable); from October 26, early voter check-in PDFs (October 19-23) whose lines are single text runs |
| Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker | No | Link to the SOS portal |

## 5. County data compared with state data

### 5.1 Early in-person voters by day

Each day's cell gives the county's count / the state's count.

| County | Oct 19 | Oct 20 | Oct 21 | Oct 22 | Oct 23 | Oct 24 | Oct 25 | On both | County only | State only (other county) | Agree |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Harris | 104,658 / 104,606 | 99,906 / 99,832 | 93,411 / 93,350 | 86,326 / 86,250 | 95,108 / 95,046 | 67,242 / 67,150 | 40,865 / 40,831 | 586,970 | 546 | 95 (55) | 99.90% |
| Dallas | 77,854 / 77,830 | 65,084 / 65,066 | 56,092 / 55,881 | 49,469 / 49,446 | 54,382 / 54,358 | 36,686 / 36,687 | 19,180 / 19,172 | 358,261 | 486 | 179 (144) | 99.8% |
| Tarrant | 50,294 / 50,141 | 51,008 / 50,722 | 48,930 / 48,632 | 46,363 / 46,124 | 50,943 / 50,639 | 51,063 / 50,758 | 24,796 / 24,654 | 321,377 | 2,020 | 293 (54) | 99.3% |
| Bexar | 42,764 / 42,070 | 44,733 / 44,443 | 45,076 / 45,071 | 44,151 / 44,182 | 45,636 / 45,663 | 40,624 / 40,710 | 20,753 / 20,825 | 282,392 | 1,345 | 572 (118) | 99.5% |
| Travis | 40,776 / 40,791 | 40,359 / 40,375 | 39,490 / 39,500 | 37,774 / 37,774 | 40,453 / 40,483 | 24,575 / 24,605 | 18,612 / 18,633 | 241,976 | 63 | 185 (162) | 99.92% |
| Collin | 34,386 / 34,260 | 33,875 / 33,749 | 31,135 / 31,028 | 29,459 / 29,349 | 34,586 / 34,482 | 33,927 / 33,769 | 16,573 / 16,512 | 212,703 | 1,238 | 446 (87) | 99.4% |
| Denton | 34,711 / 34,571 | 31,551 / 31,474 | 28,344 / 28,272 | 25,940 / 25,860 | 30,112 / 30,023 | 27,834 / 27,715 | 13,616 / 13,554 | 191,081 | 1,027 | 388 (138) | 99.4% |
| Fort Bend | 31,743 / 31,605 | 28,420 / 28,267 | 24,874 / 24,706 | 23,066 / 22,861 | 26,681 / 26,353 | 18,169 / 17,874 | 6,535 / 6,536 | 158,064 | 1,424 | 138 (30) | 99.1% |
| Hidalgo | 17,424 / 17,319 | 17,379 / 17,279 | 15,963 / 15,877 | 14,489 / 14,406 | 14,874 / 14,823 | 11,633 / 11,579 | 5,507 / 5,478 | 96,583 | 686 | 178 (8) | 99.2% |
| El Paso | 17,515 / 17,437 | 16,742 / 16,656 | 14,850 / 14,775 | 13,596 / 13,533 | 13,647 / 13,575 | 8,033 / 7,983 | 4,602 / 4,577 | 88,502 | 483 | 34 (8) | 99.4% |
| Montgomery | 13,568 / 13,561 | 14,162 / 14,168 | 14,407 / 14,408 | 13,991 / 13,987 | 15,210 / 15,210 | 16,360 / 16,359 | 9,176 / 9,175 | 96,842 | 32 | 26 (23) | 99.96% |
| Williamson | 25,694 / 25,712 | 24,237 / 24,281 | 22,195 / 22,216 | 20,907 / 20,931 | 21,894 / 21,922 | 12,914 / 12,935 | 8,502 / 8,530 | 136,273 | 70 | 254 (39) | 99.8% |
| Cameron | 9,424 / 9,436 | 8,643 / 8,653 | 7,719 / 7,730 | 6,518 / 6,543 | 6,820 / 6,830 | 4,590 / 4,594 | 2,496 / 2,501 | 46,186 | 24 | 101 (34) | 99.7% |
| Brazoria | 14,321 / 14,328 | 12,687 / 12,694 | 11,137 / 11,141 | 9,905 / 9,913 | 11,195 / 11,200 | 7,638 / 7,638 | 3,479 / 3,481 | 70,357 | 5 | 38 (32) | 99.94% |
| Bell | 7,111 / 7,095 | 7,145 / 7,132 | 7,059 / 7,056 | 6,779 / 6,768 | 7,335 / 7,345 | 7,523 / 7,512 | 3,302 / 3,298 | 46,178 | 76 | 28 (11) | 99.8% |
| Galveston | 13,653 / 13,654 | 12,017 / 12,014 | 9,970 / 9,975 | 8,700 / 8,700 | 10,106 / 10,109 | 7,267 / 7,260 | 3,080 / 3,078 | 64,752 | 41 | 38 (26) | 99.93% |
| Webb | 4,943 / 4,917 | 4,308 / 4,285 | 3,971 / 3,946 | 3,269 / 3,248 | 3,547 / 3,528 | 3,784 / 3,763 | 1,912 / 1,903 | 25,580 | 154 | 10 (0) | 99.4% |
| Jefferson | 6,987 / 6,992 | 6,566 / 6,567 | 5,509 / 5,504 | 4,797 / 4,800 | 5,234 / 5,233 | 4,067 / 4,072 | 2,091 / 2,091 | 35,233 | 18 | 26 (9) | 99.92% |
| Smith | 5,861 / 5,827 | 5,980 / 5,944 | 6,016 / 5,969 | 5,806 / 5,770 | 5,975 / 5,931 | 5,910 / 5,859 | 2,796 / 2,779 | 38,071 | 273 | 8 (3) | 99.2% |
| Brazos | 4,561 / 4,564 | 4,706 / 4,704 | 4,672 / 4,678 | 4,692 / 4,696 | 5,026 / 5,040 | 4,074 / 4,089 | 2,239 / 2,250 | 29,952 | 18 | 69 (58) | 99.7% |
| Grayson | 5,467 / 5,457 | 4,769 / 4,793 | 4,190 / 4,179 | 3,663 / 3,659 | 3,871 / 3,862 | 1,763 / 1,752 | 952 / 952 | 24,571 | 104 | 83 (12) | 99.5% |
| Randall | 4,394 / 4,399 | 4,126 / 4,131 | 3,733 / 3,747 | 3,365 / 3,375 | 3,939 / 3,949 | 2,955 / 2,973 | 1,040 / 1,047 | 23,550 | 2 | 71 (2) | 99.6% |
| Wichita | 2,945 / 2,945 | 2,783 / 2,784 | 2,615 / 2,618 | 2,228 / 2,229 | 2,223 / 2,224 | 1,542 / 1,544 | 792 / 796 | 15,127 | 1 | 13 (8) | 99.91% |
| Gregg | 3,677 / 2 (partial) | 3,534 / 3,523 | not posted / 2,909 | 2,488 / 2,470 | 2,769 / 2,761 | 1,656 / 1,647 | 810 / 809 | 11,205 | 52 | 5 (2) | 99.5% |
| Hunt | 1,839 / 1,827 | 2,044 / 1,724 (partial) | 2,032 / 2,043 | 1,789 / 1,798 | 2,104 / 2,119 | 1,964 / 1,976 | 1,004 / 1,017 | 10,705 | 27 | 75 (13) | 99.3% |
| Tom Green | 2,634 / 2,622 | 2,714 / 2,709 | 2,468 / 2,466 | 2,449 / 2,449 | 2,392 / 2,392 | 1,785 / 1,776 | 779 / 778 | 15,178 | 43 | 14 (7) | 99.7% |
| Bastrop | 3,143 / 3,142 | 2,888 / 2,889 | 2,480 / 2,480 | 2,376 / 2,377 | 2,484 / 2,483 | 1,915 / 1,915 | 919 / 918 | 16,195 | 10 | 9 (5) | 99.93% |
| Potter | (cumulative) / 2,180 | (cumulative) / 1,970 | 1,737 / 1,739 | 1,482 / 1,482 | 1,709 / 1,710 | 1,147 / 1,148 | 487 / 487 | 6,562 | 0 | 4 (2) | 99.93% |
| Bowie | 2,023 / 2,027 | 1,963 / 1,961 | 1,917 / 1,917 | 1,859 / 1,859 | 1,888 / 1,892 | 896 / 893 | 191 / 192 | 10,731 | 6 | 10 (1) | 99.90% |
| Victoria | 1,660 / 1,660 | 1,758 / 1,758 | 1,799 / 1,802 | 1,568 / 1,569 | 1,719 / 1,728 | 1,126 / 1,130 | 760 / 760 | 10,387 | 3 | 20 (1) | 99.8% |
| Henderson | 3,257 / 3,257 | 2,823 / 2,819 | 2,313 / 2,311 | 1,995 / 1,999 | 2,122 / 2,121 | 918 / 919 | 441 / 441 | 13,853 | 16 | 14 (13) | 99.8% |
| Angelina | 2,566 / 2,567 | 2,352 / 2,353 | 2,006 / 2,006 | 1,795 / 1,795 | not posted / 1,954 | not posted / 826 | not posted / 433 | 8,719 | 0 | 2 (2) | 99.97% |
| McLennan | no method or date / 4,912 | / 4,963 | / 4,874 | / 4,627 | / 4,935 | / 4,704 | - / 2,054 |  |  |  |  |

Counts are distinct VUIDs (county voters without a VUID are excluded). "On both", "County only" and "State only" add up the days on which both sources are complete. "Agree" is the voters on both lists as a share of the larger list; it is 100% only when the county-only and state-only counts are both 0, and otherwise is rounded down (to two decimals from 99.9%), so a near-match is never shown as 100%. Hunt's October 23 and 24 counts exclude its mail ballots. McLennan's list (33,514 voters, posted the evening of October 25) contains all but 79 of the state's 32,442 McLennan in-person and mail voters for October 14-25. Potter's first two snapshots were posted on the same day, so its October 19 and 20 cannot be separated. Angelina has not posted its October 23-25 lists (the state lists 1,954, 826 and 433 voters).

Most state-only voters are voters of other counties (§6.1) or, in Tarrant, voters the county lists a day later than the state (96 on October 19, 45 on October 20) and redacted voters (§6.2). Most county-only voters are a few dozen to a few hundred per day that the state has not received.

### 5.2 Mail ballots

The SOS mail data begins October 14 and is dated by when the state received it, days after the county, and the state moves ballots between days (§3); county mail lists go back to the first ballots received (September 21-22).

| County | County mail | State mail (Oct 14-25) | On both | State only (other county) |
|---|---:|---:|---:|---:|
| Harris | 36,564 | 16,795 | 16,627 | 168 |
| Bexar | 19,866 | 10,054 | 9,926 | 128 (82 dated Oct 15) |
| Tarrant | 16,839 | 11,147 | 10,997 | 150 (1); 128 match redacted voters, §6.2 |
| Dallas | 16,261 | 7,338 | 7,252 | 86 (1) |
| Travis | 13,379 | 8,009 | 7,907 | 102 (8; 57 dated Oct 19) |
| Montgomery | 7,066 | 3,225 | 3,125 | 100 |
| El Paso | 6,691 | 2,679 | 2,668 | 11 |
| Denton | 6,211 | 4,446 | 4,409 | 37 (2; 21 dated Oct 25) |
| Collin | 6,038 | 4,031 | 3,428 | 603 (1; 426 dated Oct 24 and 155 Oct 25, not on the county's list through Oct 24) |
| Williamson | 5,944 | 5,874 | 5,856 | 18 |
| Fort Bend | 4,068 | 3,260 | 3,231 | 29 (county list through Oct 23) |
| Hidalgo | 3,784 | 1,712 | 1,709 | 3 |
| Galveston | 3,042 | 2,136 | 2,127 | 9 (1) |
| Bell | 2,927 | 2,622 | 2,521 | 101 |
| Brazoria | 2,716 | 1,326 | 1,324 | 2 |
| Smith | 2,209 | 1,426 | 1,402 | 24 (1) |
| Randall | 1,882 | 1,815 | 1,810 | 5 |
| Jefferson | 1,754 | 1,682 | 1,674 | 8 |
| Cameron | 1,564 | 1,581 | 1,556 | 25 (11 dated Oct 25, 6 Oct 24; county list through Oct 23) |
| Tom Green | 1,507 | 740 | 730 | 10 |
| Victoria | 1,345 | 624 | 603 | 21 (14 dated Oct 24; county list through Oct 23) |
| Gregg | 1,317 | 1,113 | 1,103 | 10 |
| Grayson | 1,293 | 686 | 684 | 2 |
| Bastrop | 990 | 680 | 679 | 1 (1) |
| Wichita | 971 | 946 | 780 | 166 (101 dated Oct 22, 60 Oct 23) |
| Potter | 937 | 376 | 376 | 0 |
| Bowie | 845 | 319 | 287 | 32 |
| Webb | 532 | 511 | 461 | 50 (49 dated Oct 14) |
| Hunt | 255 | 562 | 239 | 323 (316 dated Oct 14-19) |

Brazos (1,473 in the county's lists) has 8 mail ballots at the state. McLennan's list does not separate mail voters; the state has 1,373 McLennan mail voters, all but 11 of them on McLennan's list. Henderson and Angelina post no mail list; the state has 303 and 764 mail voters for them.

### 5.3 Counties on the state portal only

| County | SOS days on file | Oct 19 | Oct 20 | Oct 21 | Oct 22 | Oct 23 | Oct 24 | Oct 25 | Mail Oct 14-25 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Hays | Oct 14-16, 19-25 | 10,256 | 9,910 | 9,417 | 8,792 | 8,808 | 5,100 | 2,989 | 719 |
| Lubbock | Oct 14-16, 19-25 | 8,856 | 8,642 | 7,902 | 7,291 | 7,152 | 4,580 | 2,626 | 919 |
| Ellis | Oct 14-16, 19-25 | 7,902 | 7,123 | 6,234 | 5,518 | 6,326 | 4,257 | 2,152 | 634 |
| Nueces | Oct 14-17, 19-25 | 7,617 | 6,929 | 6,444 | 5,614 | 6,474 | 5,569 | 534 | 2,816 |
| Parker | Oct 14-16, 19-25 | 5,953 | 5,711 | 5,203 | 4,991 | 5,673 | 4,097 | 2,160 | 918 |
| Comal | Oct 14-17, 19-25 | 5,876 | 5,803 | 5,816 | 5,635 | 5,885 | 5,199 | 2,842 | 2,023 |
| Johnson | Oct 15, 16, 18-25 | 5,871 | 5,440 | 4,891 | 4,439 | 5,153 | 3,934 | 1,990 | 224 |
| Rockwall | Oct 14-16, 19-25 | 4,968 | 4,338 | 3,857 | 3,305 | 3,979 | 3,304 | 982 | 314 |
| Guadalupe | Oct 14-16, 19-25 | 4,780 | 4,762 | 4,617 | 4,387 | 4,623 | 4,451 | 2,050 | 701 |
| Kaufman | Oct 19-25 | 3,904 | 4,235 | 3,955 | 3,710 | 4,184 | 4,129 | 2,053 | 867 |
| Taylor | Oct 14, 15, 19-25 | 3,186 | 3,202 | 2,910 | 2,673 | 2,828 | 2,193 | 1,043 | 748 |
| Midland | Oct 14-16, 19-25 | 2,963 | 3,117 | 2,863 | 2,581 | 3,167 | 2,234 | 1,017 | 472 |
| Orange | Oct 15, 19-25 | 2,198 | 2,236 | 1,881 | 1,567 | 1,956 | 1,248 | 624 | 543 |
| Liberty | Oct 14-16, 19-25 | 1,954 | 1,881 | 1,690 | 1,657 | 1,923 | 1,214 | 555 | 232 |
| Ector | Oct 15, 19-25 | 1,809 | 1,853 | 1,660 | 1,562 | 1,824 | 1,598 | 729 | 2 |
| Coryell | Oct 14-16, 19-25 | 1,802 | 1,442 | 1,233 | 1,096 | 1,246 | 897 | 364 | 240 |
| Walker | Oct 14-16, 19-25 | 1,740 | 1,717 | 1,563 | 1,382 | 1,494 | 1,027 | 527 | 319 |

For these counties the state is the only machine-readable published source (Guadalupe's own check-in PDFs are compared in §1, item 7). All 17 have state files for each of October 19-25. All report mail ballots (Ector only 2). Nueces's October 25 file holds 534 voters, against 5,569 for October 24; the turnout table shows the same figure.

## 6. Anomalies between county and state data

### 6.1 State county files include voters of other counties

Across October 14-25, 1,500 voters (1,475 in-person, 25 mail) in the SOS county files appear on a different county's own roster, with the same first name and that county's precinct. The largest numbers are in the SOS files for Travis (170), Dallas (144), Denton (140), Bexar (117), Collin (88), Hays (66), Johnson (62), Brazos (58), Harris (55) and Tarrant (55). No voter appears in two SOS county files, so these are records assigned to the wrong county's file, not duplicates. Anyone using SOS county files alone will slightly overcount each county.

### 6.2 State files identify voters Tarrant County redacts

Tarrant replaces the name, VUID and address of protected voters with "--Redacted--", keeping only precinct and date: 247 records in its current files (192 mail, 55 early in-person). No other county redacts identities; Harris hides the address (only) of 2,494 named voters.

- **Mail:** the SOS Tarrant mail files list 149 mail voters who are not on Tarrant's mail roster (after excluding one voter of another county). For 128 of them there is a redacted Tarrant mail record with the same precinct and the same date.
- **In person:** the SOS Tarrant files list 82 in-person voters (besides 54 of other counties) who are not on Tarrant's roster on any day; 54 of them match a redacted Tarrant in-person record by precinct.
- **Fewer redacted records each day:** Tarrant's cumulative in-person list held 104 redacted records on October 20, 99 on October 21, 79 on October 22, 69 on October 23, 62 on October 24, 56 on October 25 and 55 on October 26; its cumulative mail list held 215 in the October 23 afternoon file, 202 in the evening file and 192 on October 25 (no newer mail file has been posted). A cumulative list should not shrink.

With seven days of data, the match remains close for both voting methods. This continues to indicate that the state publishes the name, VUID and precinct of voters whose identities Tarrant withholds. It cannot be proven record by record, because the redacted rows carry no identifier. Neither source exposes an address.

### 6.3 Reporting gaps in the state data

- **In-person:** every county has a state file for every day of October 19-25, and the October 24 and 25 files are complete against the counties' own lists. Two earlier days remain partial: Gregg's October 19 (2 of 3,677) and Hunt's October 20 (1,724 of 2,044).
- **Mail:** Brazos (8 of 1,473) and Ector (2) report almost none to the state.
- **Turnout table vs. CSV:** Travis, Ellis, Hays, Coryell and Henderson (§3).

### 6.4 Mail ballots in state data but not in the county's mail list

- **Lists not yet updated:** Collin's 602 (425 dated October 24 and 155 October 25 by the state) are not in its list "through October 24", the latest posted. Cameron 25 (11 dated October 25, 6 October 24; its list runs through October 23), Victoria 21 (14 dated October 24; list through October 23), Denton 35 (21 dated October 25) and Fort Bend 29 (list through October 23) include ballots dated after the county's latest list.
- **Hunt:** the state has 562 Hunt mail voters, the county's files 255 (its mail lists and the October 23 and 24 "BBM" rows); 323 state mail voters are in neither, 316 of them dated October 14-19 by the state.
- **Wichita:** 166 state mail voters (101 dated October 22, 60 October 23) are not on Wichita's cumulative October 23 absentee list, still the latest.
- **Travis:** 94 state mail voters are on none of Travis's lists (56 dated October 19 by the state, 24 October 22).
- **Elsewhere:** Harris 168 (35 dated October 25), Bexar 128 (82 dated October 15), Bell 101, Montgomery 100, Dallas 85, Webb 50 (49 dated October 14), Bowie 32, Smith 23, Williamson 18, El Paso 11, Gregg 10, Tom Green 10, Galveston 8, Jefferson 8, Randall 5, Hidalgo 3, Brazoria 2 and Grayson 2.

### 6.5 Precinct differences

- **Williamson:** the October 25 file, like the October 22-24 files, gives a precinct different from the state's for its mail voters (4,689); the October 21 file matched the state for all of them, and every in-person row still matches. The county's mail precinct column appears to have been scrambled since October 22.
- **Bowie:** the October 20, 21 and 23 early vote reports give a precinct that differs from the state's for 1,566, 1,711 and 1,441 voters (4,718); the October 22, 24 and 25 reports match for all voters. The values were checked against the PDFs.
- **Bexar:** 65 voters in the October 22 check-in report (one or none in each other report).
- **Elsewhere,** on matched voters: Harris 1,150 (1,115 in-person), Tarrant 164, Galveston 157 (111 of them in its October 19 file), El Paso 83 (all in-person), Smith 52, Hunt 14, Montgomery 8, Travis 6, Denton 5, Bastrop, Brazoria and Dallas 2 each, and one each in Henderson and Potter.

Denton and Gregg write the precinct with a two-digit split appended ("418603" for 4186); this is formatting, not a difference.

### 6.6 Surnames

On matched voters, the surname differs between the county and the state in 61 Collin, 45 Tarrant, 37 Denton, 34 Bexar, 32 Cameron, 28 Williamson and 27 Dallas records, and in 15 or fewer elsewhere (430 in all). In all but 17 of these records the two names share at least one name: they are name changes recorded in one system and not the other, or formatting (counties that write names "FIRST MIDDLE LAST").

### 6.7 One VUID, two different people

- **Within one county's files:** one VUID is listed for two different voters in Harris's cumulative mail file, in a Brazoria mail file and in Bowie's October 22 and October 24 mail lists, and for two different people checking in at Bexar on October 19 and October 23.
- **On two counties' rosters:** one VUID is a mail ballot received October 21 on both Bell's and Randall's mail lists, for two people with different names.
- **SOS file vs. a county roster:** 32 VUIDs name a voter in an SOS file and an entirely different person (no name in common) on a county's roster. Seventeen are within one county (Harris 4, Bexar 3, Dallas 3, and Bowie, Brazoria, Collin, Denton, El Paso, Montgomery and Tarrant 1 each). Fifteen are across counties: seven on Bell's roster (against the SOS Coryell (2), Fort Bend (2), Lubbock, Randall and Victoria files), two on Denton's roster against the SOS Dallas file, and one each on Harris's roster against the SOS Bell and Hidalgo files, Dallas's against Collin, El Paso's against Harris and Montgomery, and Tarrant's against Hidalgo.

All seven Bell VUIDs, the two Denton/Dallas VUIDs and the Hidalgo/Harris, Hidalgo/Tarrant and Montgomery/El Paso VUIDs are recent registrations in one numeric range (2223365544-2223382143). Bell County's roster gives these VUIDs to different people than the state's files do, so the Bell County Elections office and the SOS should compare them first.

### 6.8 One voter in two counties' own rosters

Seven are the same person in both counties (same first and last name); the eighth, new, has the same first name and a different last name:

| Counties | First county | Second county | State's files |
|---|---|---|---|
| Tarrant, Smith | in person, in Tarrant's list from October 20 (precinct 1224) | in person October 22 (Smith precinct 245) | SOS Smith October 20 file, with the Tarrant precinct |
| Tarrant, Dallas | in person, in Tarrant's list from October 22 (precinct 1010) | in person October 23 (Dallas precinct 1146) | SOS Dallas October 22 file, with the Tarrant precinct |
| El Paso, Bexar | in person, in El Paso's list from October 21 (precinct 164) | in person October 24 (Bexar precinct 4031) | SOS Bexar October 21 file, with the El Paso precinct |
| Hidalgo, Bexar | in person, in Hidalgo's list from October 23 (precinct 151) | in person October 23 (Bexar precinct 2143) | SOS Bexar October 23 file, with the Hidalgo precinct |
| Harris, Travis | mail ballot received September 30 | in person October 23 | SOS Travis October 23 file (in person) |
| Bexar, Webb | in person October 19 | in person October 19 | reported since October 20 |
| Cameron, Hidalgo | mail ballot received October 13 | mail ballot (no date) | reported since October 20 |
| Bexar, Cameron (new) | in person October 19 (Bexar precinct 1045.01) | in person October 25 (Cameron precinct 21) | SOS Cameron October 25 file only; not in the SOS Bexar files |

### 6.9 Mail ballot and in-person vote by the same voter

Fifteen voters are listed both as a returned mail ballot and as an early in-person voter in the same county (unchanged since the last review):

- **El Paso 5:** mail returned October 6-20; in person by October 19 (2), October 21 (2) and October 22 (1).
- **Dallas 4:** mail October 9 and in person October 19, 20 and 22; and one voter in person October 19 whose mail ballot is listed as received October 24.
- **Tarrant 2:** in person by October 19 and by October 20; mail ballots recorded October 21 and 22.
- **Bastrop 1:** mail October 16; in person October 22.
- **Grayson 1:** mail October 2; in person October 20.
- **Jefferson 1:** on the cumulative mail list; in person October 20. The state lists only the in-person vote.
- **Travis 1:** mail October 7; in person October 21.

A voter whose mail ballot has been accepted may not vote in person; these may be cancelled mail ballots, provisional votes or data errors.

### 6.10 Duplicates within county files

- **Same-day duplicate check-ins:** Grayson 8, Bexar 6, Collin 3, Jefferson 3, Dallas 2, and 1 each in Denton, Galveston, Tom Green, Travis and Webb. Jefferson also lists one VUID in its October 20 and October 23 in-person files (the state lists only October 20).
- **Mail ballot received twice:** 10 Bexar voters appear on two days' received-ballot lists; Travis 5, Bastrop 3 (its cumulative mail CSV lists each with two dates), Gregg 2, Montgomery 2, and 1 each in Bowie, Brazoria, Brazos, Galveston and Harris.

### 6.11 Identifiers that are not state VUIDs

| County | Records | ID length | Lists |
|---|---:|---|---|
| Tarrant | 383 | 8 digits (one 7) | mail (290), in-person (93) |
| Collin | 214 | 6, 7, 9 digits | in-person (208), mail (3), provisional (3) |
| Denton | 139 | 7 digits | in-person |
| Grayson | 55 | 6, 8, 9 digits | in-person (48), mail (7) |
| El Paso | 54 | 7 digits | mail (44), in-person (10) |
| Hidalgo | 48 | 8-9 digits | mail (35), in-person (13) |
| Webb | 28 | 8 digits | mail (22), in-person (6) |
| Smith | 27 | 7 digits (one 8) | in-person |
| Gregg | 12 | 6, 7, 11 digits | in-person |
| Jefferson | 9 | 6, 7, 8 digits | in-person |
| Bexar | 8 | 8, 9, 11 digits | in-person |
| Bowie | 8 | 9 or 11 digits | mail |
| Tom Green | 3 | 9 digits | mail |
| Henderson | 1 | 8 digits | in-person |

These are likely county-internal numbers for voters whose state VUID had not been assigned or synchronized, or (for the 9- and 11-digit cases) a VUID with a digit missing or added. The state files contain none, so these voters cannot be matched by ID; for 17 of them the state lists a voter with the same name and precinct under a 10-digit VUID (see the supplement). Jefferson also masks eight VUIDs as "**********" (the names are shown).

## 7. Other county data quality notes

- **Blank VUIDs** (named voters with no ID): Harris 452 mail ballots; Collin 237 (220 provisional); Grayson 80 in-person; Tom Green 46 in-person; Bell 27 mail; Dallas 20; Bexar 18 in-person; Fort Bend 8 mail; Hunt 8; Jefferson 8 (masked); Montgomery 3; Wichita 3; Bastrop and Hidalgo 2; Brazoria and Randall 1. Travis's 733 and Montgomery's 247 Limited Ballot voters have no VUID by design. Tarrant's 247 redacted records also have no VUID (§6.2).
- **No voting date:** Tarrant's, Hidalgo's and Potter's early voting lists, Denton's from October 20, El Paso's from day 2 and McLennan's list carry no date per voter. Jefferson's lists carry neither date nor precinct (the in-person date is the file's). Smith's, Wichita's and Jefferson's mail lists are cumulative, so the date in their file names is the list date.
- **Not yet posted** by the afternoon of October 26: Gregg's October 21 roster; Angelina's October 23-25 rosters; the October 25 mail lists of Collin, Fort Bend, Cameron, Potter, Randall, Jefferson and Wichita (Fort Bend, Cameron, Potter, Jefferson and Wichita last posted for October 23).
- **Files replaced or re-posted:** Victoria, Potter, Tom Green and Jefferson (mail) post cumulative files; Randall re-posts its two rosters; McLennan posts one list under a new timestamped name; Bastrop renames its mail CSV daily; Brazoria re-posts earlier days' lists; Henderson posted three days at once.
- **Same-day posting:** Collin, Denton, Webb, McLennan, Potter, Randall and Bastrop post lists for the current day during that day or that evening; they may change when re-posted.
- **Hard-to-read formats:** Guadalupe's mail lists are scanned images and its check-in lists print each line as one run of text; Wichita's PDFs sit behind a bot challenge and wrap each absentee record onto two lines; Gregg prints long polling-place names past their cells; Smith's lists are in a JavaScript document portal; Randall prints its mail roster sideways; Grayson's check-in workbook is a printed report with its column titles on separate rows; Jefferson posts Excel 97-2003 (.xls) files; Hunt changed its date format on October 24; Henderson changed its layout on October 23 and its title on October 25; Tom Green's October 25 mail list has no column titles.

## 8. Recommendations

1. **Download daily** through Election Day: Tarrant, Hidalgo, Denton, El Paso and Potter (no voting date per voter), Victoria, Tom Green, Jefferson and McLennan (cumulative files replaced), and Collin, Williamson, Cameron, Dallas, Fort Bend and Harris (latest file only). Save same-named files with the download time.
2. **Prefer county files** for counts and voter lists; use the SOS portal for the 16 counties that post nothing else and as a cross-check. Re-download every SOS day before each comparison, since the state fills in, revises and re-dates them, and do not read SOS mail dates as received dates or the turnout table as a substitute for the county CSVs.
3. **When using SOS county files,** expect voters of other counties in each file (§6.1) and check them against neighboring counties before treating them as anomalies.
4. **Refer to the counties:**
   - the eight voters recorded in two counties (§6.8);
   - the 15 voters with both a mail ballot and an in-person vote;
   - the shared VUIDs (§6.7), first those on Bell's roster;
   - Williamson's mail precincts since October 22, and Bowie's October 20, 21 and 23 precincts;
   - Hunt's mail ballots in its in-person lists;
   - Bexar's mail ballots received twice;
   - the precinct differences in Harris, Tarrant, Galveston, El Paso and Bexar;
   - the mail lists not updated since October 23 or 24 (Collin, Fort Bend, Cameron, Potter, Jefferson, Wichita).
5. **Refer to Tarrant County and the Secretary of State:**
   - the publication by the SOS of voters Tarrant redacts, and the shrinking counts of redacted records (§6.2);
   - the SOS files' inclusion of other counties' voters (§6.1), including the votes filed under a second county in §6.8;
   - the re-dating of mail ballots between daily files (§3);
   - the turnout table's Travis, Ellis, Hays, Coryell and Henderson figures (§3).
6. **Repeat this comparison** after early voting closes and after the canvass.

## Appendix A. Files

Downloaded files are saved per county:

- county files are saved as `G26_<county file name>`; files a county posts under the same name every day get the download date (`_2026-10-26`), or the download date and time (`_2026-10-26-14-40-09`) for cumulative lists that carry no voting dates (Hidalgo, Potter);
- SOS files are saved as `G26_<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv`; when the state revises a day, the earlier copy is kept.

The files load in Election Explorer (File → Load Voter Roster); the comparisons in this report were produced from the loader's normalized output, except the three files noted in §2, which were read from their text. The companion supplement (`Texas_Early_Voting_Roster_Review_2026-10-26_Supplement`) lists each anomalous record's VUID and its location in the county's and the state's files.

## Appendix B. Caveats

- In-person comparisons cover October 19-25; where the state's file for a day is partial, that day is shown but left out of the totals.
- County files were downloaded October 20-26 at different times; several counties post same-day lists that are later replaced, and counties keep revising earlier days. The state files were re-downloaded about seven hours after the county files on October 26.
- Day-level counts for cumulative, undated county lists were recovered from consecutive snapshots and attribute voters added late to the day they first appear (Tarrant, §5.1).
- A match between two records means the same VUID; name and precinct differences were checked separately. A match by name, precinct or date without a common VUID (§6.2, §6.11) is an inference, not a confirmed identity.
- This review describes data as published. It does not establish that any voter voted improperly; the records flagged in §6.5-§6.10 have ordinary clerical explanations as well as serious ones, and only the counties can resolve them.
