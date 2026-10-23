# Texas Early Voting Rosters: County and State Data Review

**Election:** November 3, 2026 General Election\
**Data through:** Wednesday, October 21, 2026 (third day of in-person early voting)\
**Prepared:** October 22, 2026 (updates the reviews of October 20 and 21; county and state data as posted by 7:50 PM October 22)\
**Scope:** the 50 most populous Texas counties and the Texas Secretary of State (SOS) roster portal

## 1. Summary

Texas Election Code §87.121 requires each county to post its early voting rosters (in-person and by mail) daily and to submit them for statewide elections to the Secretary of State, which republishes them. This update compares the county and state lists voter by voter (by Voter Unique Identifier, VUID) for the first three days of in-person voting, October 19-21, in the 50 most populous counties.

Key findings:

1. **The state has caught up, and the two sources agree.** On October 21 the state's data was largely complete for October 19 and 20: the SOS turnout table now shows 806,265 in-person voters statewide for October 19, 742,656 for October 20 and 600,636 for October 21. Over every day for which both sources are complete, the county's and the state's lists of early in-person voters agree on 99% or more of voters in 30 of the 31 counties with dated county rosters (the exception is Hunt, 94%, where the state's October 20 file is short by 320 voters). No voter is an in-person voter in one source and a mail voter in the other.
2. **A few counties still lag at the state.** For October 21, the state lists 24,189 of Harris's 93,414 in-person voters and 2 of Fort Bend's 24,874 (Fort Bend's October 20 is also short: 9,522 of 28,420); Webb has no state file for October 20 and 1 voter for October 21. Nine counties that post their own mail lists still report almost no mail ballots to the state.
3. **The state's turnout table overstates Dallas.** The portal's table shows 105,015, 87,451 and 75,127 Dallas in-person voters for October 19-21, about 35% more than both the state's own Dallas files (77,829, 64,874, 55,695) and Dallas County's rosters (77,853, 64,892, 55,906).
4. **The state's county files contain voters of other counties.** Across the three days, 631 voters (618 in-person, 13 mail) appear in an SOS county file while being on a different county's own roster, with that county's precinct.
5. **The state's files continue to name voters that Tarrant County redacts.** Tarrant withholds the name and VUID of 287 protected voters (208 mail, 79 in-person). The SOS Tarrant files list 134 mail voters and 93 in-person voters who are not on Tarrant's own rosters; 123 of the mail voters match a redacted Tarrant record by precinct and date, and 78 of the in-person voters match one by precinct. The state files contain no addresses.
6. **Travis's posted roster ZIP still has no mail lists for October 15 and 16.** The state lists 1,484 Travis mail voters who are on none of Travis's mail lists, 1,211 of them dated October 19-20 by the state; the state's mail dates run several days behind the counties' received dates.
7. **Bowie's precincts disagree with the state's.** Bowie's October 20 and 21 early vote reports (the first to include a precinct) give a precinct that matches the state's for only 395 of 1,961 and 206 of 1,917 voters.
8. **Counties keep revising earlier days.** Dallas listed 76,243 in-person voters for October 19 in its October 19 report, 77,548 in its October 20 report and 77,853 in its October 21 file. Tarrant's cumulative list added 77 October 19 voters only on later days.
9. **Records that warrant review by the counties** (none is proof of wrongdoing; most have clerical explanations):
   - 9 voters listed both as a returned mail ballot and as an early in-person voter (El Paso 4, Dallas 2, Grayson 1, Tarrant 1, Travis 1);
   - 3 VUIDs on two counties' own rosters (Bexar/Webb and Hidalgo/Cameron, same person; Bell/Randall, two different people);
   - 10 VUIDs that name entirely different people in a county's roster and an SOS file (2 within one county; 8 across counties, 4 of them on Bell's roster), and 2 VUIDs used for two people within one county's file (Harris, Brazoria);
   - 10 Bexar mail ballots listed as received on two different days, and 19 same-day duplicate in-person check-ins across 6 counties;
   - precinct differences between the county and the state for 333 Harris, 120 Galveston, 67 Tarrant and 42 El Paso voters.
10. **Several counties changed their files this week**, which matters for anyone downloading them: Hunt's list became daily on October 21, Grayson's absentee list became daily, Dallas switched to a CSV report, Fort Bend replaced its October 21 file, and Wichita's absentee list lost its received-date column. Tarrant, Hidalgo, Denton, El Paso and Potter post cumulative early voting lists without the day each voter voted, so daily downloads remain necessary.

## 2. Scope and method

**Counties (by 2025 Census population):**

- Ranks 1-20: Harris, Dallas, Tarrant, Bexar, Travis, Collin, Denton, Fort Bend, Hidalgo, El Paso, Montgomery, Williamson, Cameron, Brazoria, Bell, Galveston, Nueces, Lubbock, Hays, Webb.
- Ranks 21-50: McLennan, Jefferson, Smith, Brazos, Ellis, Johnson, Kaufman, Comal, Guadalupe, Midland, Parker, Ector, Grayson, Randall, Taylor, Rockwall, Wichita, Gregg, Hunt, Liberty, Tom Green, Bastrop, Potter, Bowie, Victoria, Henderson, Angelina, Orange, Coryell, Walker.

**Sources:**

- each county's own roster files, downloaded daily October 20-22. 33 counties post their own rosters; 32 can be read (Guadalupe's mail lists are scanned images). The other 17 (Nueces, Lubbock, Hays, Jefferson, Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker) link to the SOS portal;
- the SOS portal's per-county early voting CSVs for October 14-21, for all 50 counties, as posted at 7:50 PM October 22 (earlier days re-downloaded, since the state revises them);
- the SOS turnout tables for October 19, 20 and 21.

**Method.** All files were read with Election Explorer's roster loader, which normalizes each county's layout to VUID, precinct, name, voting method and date. For every county file, the number of VUIDs loaded matches an independent count of the 10-digit identifiers in the raw file (exactly, except Gregg's PDFs, within 0.5%). The lists were then compared by VUID:

- *early in-person, by day*: the county's in-person voters for each of October 19, 20 and 21 vs. the SOS's IN-PERSON voters for that date. For counties whose lists are cumulative and undated (Tarrant, Hidalgo, Denton, El Paso, Potter), each day was recovered from consecutive daily snapshots (a day's voters = that day's snapshot minus the previous day's). A day counts toward the totals only when both sources have it and the state's file holds at least half the county's count;
- *mail*: the county's latest mail list (or all daily files) vs. the SOS's MAIL-IN voters for October 14-21.

"County-only" and "state-only" voters were checked against the other 49 counties' files. Name and precinct comparisons ignore formatting (case, spacing, leading zeros, "Precinct"/"P"/"S" prefixes, and a two-digit precinct split such as "418603" for precinct 4186). Voter names and VUIDs are deliberately not reproduced in this report.

## 3. The Secretary of State portal

**Where:** "Election Information & Turnout Data", `https://goelect.txelections.civixapps.com/ivis-evr-ui/evr`. Pick an election and an early voting date, then *Submit*: a table of registered voters, in-person and mail turnout per county, with a **CSV Download** per county under *Voter Details Report*.

**How the data is served.** The page fetches JSON whose `upload` field is the file in base64:

| Data | Request |
|---|---|
| Elections, early voting dates and county ids | `/api-ivis-system/api/v1/getFile?type=EVR_ELECTION` |
| Turnout table for one date | `/api-ivis-system/api/v1/getFile?type=EVR_EARLYVOTING&electionId=53815&electionDate=10/21/2026` |
| One county's voter CSV for one date | `/api-ivis-system/api/v1/getFileByFormat?type=EVR_EARLYVOTING&electionId=53815&countyId=<id>&county=<NAME>&electionDate=<MM/DD/YYYY>&format=CSV` |

County ids follow alphabetical order, except that the portal sorts McLennan after McCulloch (id 161). An empty response means the county reported nothing for that day.

**File contents.** `VOTER_NAME` ("LAST, FIRST MIDDLE"), `ID_VOTER` (VUID), `VOTING_METHOD` (`IN-PERSON` / `MAIL-IN`), `PRECINCT`. There is **no address and no date column**. A county CSV holds only the voters credited on that date.

**Timeliness and revisions.** A day's files fill in over the following day or two. On the evening of October 21, only 15 of the 50 counties had a file for October 20; by the evening of October 22, 49 had one. Files are also revised after posting: in the October 22 refresh, the state's October 21 Bexar file grew from 9,779 to 45,026 rows and Harris's from 7,819 to 26,071, and 49 county-day files changed.

**Mail dates.** The date of an SOS mail file is not the day the county received the ballot. In Travis, the state's October 16 mail voters are almost all ballots the county lists as received October 13, and its October 19 mail voters mostly the county's October 14 ballots.

**Turnout table vs. CSVs.** The table and the state's own CSVs agree for every county except Dallas (table about 35% higher every day, see §1), Hidalgo (table about 3% higher every day: 17,831 vs. 17,319 for October 19), Coryell (October 20: 1,867 vs. 1,442) and Webb (October 19: table 0, CSV 4,921).

**Name formatting.** The SOS appends name suffixes to the middle name without a space ("RAYJR"), which causes apparent name mismatches against county files.

## 4. How each county posts its rosters

"Cumulative" means each new file contains the earlier ones; "daily" means each file holds only that day.

### 4.1 Counties 1-20

| County | Where | Format | Granularity | Voting date per voter | Download daily? |
|---|---|---|---|---|---|
| Harris | harrisvotes.com, Election Rosters | ZIP of CSV | Early voting one CSV per day, all in one ZIP replaced daily; mail cumulative (renamed "Cumulative_BBM_1126" on October 22) | Yes | Recommended |
| Dallas | dallascountyvotes.org, election page | XLSX; CSV from October 21 | Cumulative, with earlier days revised | Yes | Recommended |
| Tarrant | tarrantcountytx.gov, current election | ZIP of TSV | Cumulative, same file name | Mail yes; **early voting no** | **Yes** |
| Bexar | elections.bexar.gov Document Center | PDF | Daily | Yes | No |
| Travis | votetravis.gov, one ZIP of daily workbooks | ZIP of XLSX | Daily workbooks in one ZIP | Yes | No (but see §6.4) |
| Collin | collincountytx.gov/Elections/rosters | XLSX | Cumulative, sometimes posted the same day | Yes | Recommended |
| Denton | votedenton.gov, rosters page | XLSX | Early voting cumulative, **undated from October 20**; mail cumulative | October 19 only | **Yes** |
| Fort Bend | fortbendcountytx.gov, Early Voting Statistics | XLSX | Cumulative from October 21 (that file was replaced the same day) | Yes | Recommended |
| Hidalgo | hidalgocounty.us Document Center (updated in place) | CSV / XLSX | Cumulative | **No** | **Yes** |
| El Paso | epcountyvotestx.gov, Current Election | CSV (mail), XLSX | Mail cumulative; early voting **cumulative from day 2, undated** | Mail yes; early voting no | **Yes** |
| Montgomery | elections.mctx.org, Early Voting Roster | ZIP of CSV per day | Daily | Yes | No |
| Williamson | wilcotx.gov Document Center (one document replaced) | XLSX | Cumulative | Yes | Recommended |
| Cameron | cameroncountytx.gov, Elections results | XLSX (per jurisdiction) | Cumulative, replaced | Yes | Recommended |
| Brazoria | brazoriacountyclerktx.gov, Voter Lists | XLSX | Daily | Yes | No |
| Bell | bellcountytx.com, Daily Early Voting Reports | PDF | Daily | Yes | No |
| Galveston | galvestonvotes.org, Current Elections | CSV (mail), XLSX | Daily | Yes | No |
| Webb | webbcountytx.gov, Daily Voted List | PDF | Daily, posted the same day | Yes | No |
| Nueces, Lubbock, Hays | link to the SOS portal | SOS CSV | Daily (SOS) | File date | Re-download periodically |

### 4.2 Counties 21-50

| County | Own roster | Format and posting |
|---|---|---|
| McLennan | Yes | One PDF list of early voters, replaced daily under a timestamped name; no method or date per voter |
| Smith | Yes | Daily PDFs in a Laserfiche public portal; in-person lists daily, absentee lists cumulative |
| Brazos | Yes | Daily PDFs |
| Grayson | Yes | Daily XLSX check-in reports and (from October 20) daily absentee lists; also check-in summaries by location |
| Randall | Yes | Two PDFs re-posted daily (cumulative early voting roster with check-in time; mail roster printed sideways) |
| Wichita | Yes | Daily PDFs behind a bot challenge that blocks automated downloads; the absentee list changed layout on October 21 (no received date, wrapped rows) |
| Gregg | Yes | PDFs (check-ins, mail list); nothing posted for October 21 by October 22 evening |
| Hunt | Yes | PDFs; cumulative through October 20, daily from October 21 |
| Tom Green | Yes | PDFs; only October 19 posted by October 22 evening |
| Bastrop | Yes | Daily CSVs; mail CSV re-posted under a new name each day |
| Potter | Yes | Two PDFs replaced daily after the polls close; early voting roster cumulative and undated |
| Bowie | Yes | Daily PDFs (ballots by mail received, early vote history; precinct added October 20) |
| Victoria | Yes | One PDF and XLSX replaced daily ("mail and in person") |
| Henderson | Yes | Daily PDFs, named for the morning they are posted |
| Angelina | Yes | Daily PDF; only October 19 posted by October 22 evening |
| Guadalupe | Mail only | Daily returned-mail PDFs, scanned images (not machine-readable) |
| Jefferson, Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker | No | Link to the SOS portal |

## 5. County data compared with state data

### 5.1 Early in-person voters by day

| County | Oct 19 county | Oct 19 state | Oct 20 county | Oct 20 state | Oct 21 county | Oct 21 state | On both | County only | State only (other county) | Agree |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Harris | 104,658 | 104,606 | 99,909 | 99,830 | 93,414 | 24,189 (partial) | 204,407 | 160 | 29 (17) | 99.9% |
| Dallas | 77,853 | 77,829 | 64,892 | 64,874 | 55,906 | 55,695 | 198,319 | 332 | 79 (66) | 99.8% |
| Tarrant | 50,294 | 50,142 | 51,008 | 50,722 | 48,930 | 48,631 | 149,273 | 959 | 222 (28) | 99.4% |
| Bexar | 42,764 | 42,036 | 44,733 | 44,397 | 45,076 | 45,026 | 131,393 | 1,180 | 66 (33) | 99.1% |
| Travis | 40,776 | 40,791 | 40,359 | 40,375 | 39,490 | 39,498 | 120,598 | 27 | 66 (57) | 99.9% |
| Collin | 34,389 | 34,257 | 33,875 | 33,749 | 31,135 | 31,028 | 98,831 | 568 | 203 (45) | 99.4% |
| Denton | 34,711 | 34,566 | 31,551 | 31,453 | 28,344 | 28,232 | 94,127 | 479 | 124 (61) | 99.5% |
| Fort Bend | 31,743 | 31,516 | 28,420 | 9,522 (partial) | 24,874 | 2 (partial) | 31,473 | 270 | 43 (5) | 99.1% |
| Williamson | 25,694 | 25,712 | 24,236 | 24,280 | 22,195 | 22,216 | 72,102 | 23 | 106 (16) | 99.9% |
| Hidalgo | 17,424 | 17,319 | 17,379 | 17,279 | 15,963 | 15,876 | 50,415 | 351 | 59 (4) | 99.3% |
| El Paso | 17,515 | 17,436 | 16,742 | 16,649 | 14,850 | 14,771 | 48,848 | 259 | 8 (3) | 99.5% |
| Brazoria | 14,321 | 14,328 | 12,687 | 12,693 | 11,138 | 11,139 | 38,145 | 1 | 15 (12) | 100.0% |
| Montgomery | 13,568 | 13,561 | 14,162 | 14,168 | 14,407 | 14,407 | 42,119 | 18 | 17 (15) | 100.0% |
| Galveston | 13,653 | 13,652 | 12,017 | 12,014 | 9,970 | 9,974 | 35,614 | 26 | 26 (15) | 99.9% |
| Cameron | 9,424 | 9,428 | 8,643 | 8,653 | 7,720 | 7,729 | 25,769 | 18 | 41 (13) | 99.8% |
| Bell | 7,111 | 7,095 | 7,145 | 7,132 | 7,059 | 7,056 | 21,277 | 38 | 6 (4) | 99.8% |
| Smith | 5,861 | 5,827 | 5,980 | 5,944 | 6,016 | 5,967 | 17,734 | 123 | 4 (2) | 99.3% |
| Grayson | 5,467 | 5,457 | 4,769 | 4,793 | 4,190 | 4,179 | 14,362 | 64 | 67 (9) | 99.5% |
| Webb | 4,943 | 4,921 | 4,308 | no file | 3,971 | 1 (partial) | 4,920 | 23 | 1 (0) | 99.5% |
| Brazos | 4,561 | 4,564 | 4,706 | 4,704 | 4,672 | 4,678 | 13,933 | 6 | 13 (10) | 99.9% |
| Randall | 4,394 | 4,399 | 4,126 | 4,126 | 3,733 | 3,734 | 12,251 | 2 | 8 (1) | 99.9% |
| Gregg | 3,676 | 2 (partial) | 3,526 | 3,523 | not posted | 4 | 3,513 | 13 | 10 (1) | 99.6% |
| Henderson | 3,257 | 3,257 | 2,823 | 2,819 | 2,313 | 2,311 | 8,380 | 13 | 7 (6) | 99.8% |
| Bastrop | 3,143 | 3,142 | 2,888 | 2,888 | 2,480 | 2,479 | 8,506 | 5 | 3 (1) | 99.9% |
| Wichita | 2,945 | 2,945 | 2,783 | 2,784 | 2,615 | 2,616 | 8,343 | 0 | 2 (1) | 100.0% |
| Tom Green | 2,634 | 2,622 | not posted | 2,709 | not posted | 2,466 | 2,617 | 17 | 5 (3) | 99.4% |
| Angelina | 2,566 | 2,567 | not posted | 2,353 | not posted | 2,006 | 2,566 | 0 | 1 (1) | 100.0% |
| Bowie | 2,023 | 2,027 | 1,963 | 1,961 | 1,917 | 1,917 | 5,901 | 2 | 4 (0) | 99.9% |
| Hunt | 1,839 | 1,827 | 2,044 | 1,724 | 2,032 | 2,043 | 5,568 | 347 | 26 (8) | 94.1% |
| Victoria | 1,660 | 1,660 | 1,758 | 1,758 | 1,799 | 1,799 | 5,216 | 1 | 1 (0) | 100.0% |
| Potter | (cumulative) | 2,180 | (cumulative) | 1,970 | 1,737 | 1,739 | 1,737 | 0 | 2 (2) | 99.9% |
| McLennan | no method or date | 4,912 | | 4,963 | | 4,874 | | | | |

Counts are distinct VUIDs (county voters without a VUID are excluded). "On both", "County only" and "State only" add up the days on which both sources are complete; "Agree" is the voters on both lists as a share of the larger list. McLennan's list (21,638 voters, posted the evening of October 22) contains all but 50 of the state's 15,750 McLennan in-person and mail voters for October 14-21.

Most state-only voters are voters of other counties (§6.1) or, in Tarrant, voters the county lists a day later than the state (77 on October 19) and redacted voters (§6.2). Most county-only voters are a few dozen to a few hundred per day that the state has not yet received.

### 5.2 Mail ballots

The SOS mail data begins October 14 and is dated by when the state received it, days after the county (§3); county mail lists go back to the first ballots received (September 21-22).

| County | County mail | State mail (Oct 14-21) | On both | State only (other county) |
|---|---:|---:|---:|---:|
| Harris | 29,375 | 10,493 | 10,447 | 46 |
| Tarrant | 13,344 | 7,948 | 7,813 | 135 (1); 123 match redacted voters, §6.2 |
| Dallas | 12,890 | 5,627 | 5,551 | 76 (1) |
| Travis | 9,226 | 4,634 | 3,150 | 1,484 (5); see §6.4 |
| Montgomery | 5,982 | 2,189 | 2,112 | 77 |
| El Paso | 5,827 | 978 | 969 | 9 |
| Denton | 4,644 | 2,677 | 2,663 | 14 (1) |
| Williamson | 4,362 | 2,393 | 2,392 | 1 |
| Collin | 3,914 | 2,044 | 1,312 | 732 (715 dated Oct 21, after the county's list) |
| Fort Bend | 3,202 | 1,793 | 1,787 | 6 |
| Hidalgo | 3,192 | 1,030 | 1,028 | 2 |
| Galveston | 2,388 | 206 | 203 | 3 (1) |
| Bell | 2,136 | 1,908 | 1,855 | 53 |
| Smith | 1,668 | 961 | 892 | 69 (63 dated Oct 21) |
| Randall | 1,604 | 126 | 123 | 3 |
| Tom Green | 1,131 | 341 | 327 | 14 |
| Grayson | 1,070 | 470 | 468 | 2 |
| Gregg | 1,048 | 701 | 690 | 11 |
| Bastrop | 914 | 372 | 371 | 1 (1) |
| Webb | 432 | 207 | 179 | 28 (all dated Oct 14) |

Counties that post mail lists but report almost none to the state: Bexar (16,386 in the county's list, 1 at the state), Brazoria (2,277), Cameron (1,174), Victoria (1,172), Brazos (1,111), Wichita (898), Potter (850), Bowie (716) and Hunt (110). Bastrop, which reported none on October 21, now has 372 at the state.

### 5.3 Counties on the state portal only

| County | SOS days on file | In person Oct 19 | Oct 20 | Oct 21 | Mail Oct 14-21 |
|---|---|---:|---:|---:|---:|
| Hays | Oct 14, 19-21 | 10,256 | 9,909 | 9,416 | 483 |
| Lubbock | Oct 19-21 | 8,856 | 8,642 | 7,900 | 0 |
| Ellis | Oct 14-16, 19-21 | 7,902 | 7,123 | 6,234 | 431 |
| Nueces | Oct 14-17, 19-21 | 7,617 | 6,929 | 6,444 | 2,189 |
| Jefferson | Oct 14, 19-21 | 6,992 | 6,567 | 5,502 | 1,073 |
| Parker | Oct 19-21 | 5,953 | 5,711 | 5,203 | 0 |
| Comal | Oct 14-17, 19-21 | 5,876 | 5,803 | 5,815 | 1,045 |
| Johnson | Oct 18-21 | 5,871 | 5,440 | 4,891 | 0 |
| Rockwall | Oct 19-21 | 4,968 | 4,338 | 3,857 | 0 |
| Guadalupe | Oct 14-16, 19-21 | 4,780 | 4,762 | 4,612 | 508 |
| Kaufman | Oct 19-21 | 3,904 | 4,235 | 3,955 | 654 |
| Taylor | Oct 14, 15, 19-21 | 3,185 | 3,202 | 2,910 | 387 |
| Midland | Oct 14-16, 19-21 | 2,963 | 3,117 | 2,863 | 212 |
| Orange | Oct 19-21 | 2,200 | 2,236 | 1,881 | 0 |
| Liberty | Oct 19-21 | 1,952 | 1,879 | 1,687 | 0 |
| Ector | Oct 19-21 | 1,809 | 1,853 | 1,660 | 0 |
| Coryell | Oct 19-21 | 1,802 | 1,442 | 1,233 | 0 |
| Walker | Oct 15, 19-21 | 1,740 | 1,717 | 1,563 | 2 |

For these counties the state is the only published source. All 18 now have state files for October 19-21; eight have reported no mail ballots to the state.

## 6. Anomalies between county and state data

### 6.1 State county files include voters of other counties

Across October 14-21, 631 voters (618 in-person, 13 mail) in the SOS county files appear on a different county's own roster, with the same first name and that county's precinct. The largest numbers are in the SOS files for Dallas (67), Travis (62), Denton (62), Collin (45), Bexar (33), Tarrant (29), Hays (25) and Harris (23). No voter appears in two SOS county files, so these are records assigned to the wrong county's file, not duplicates. Anyone using SOS county files alone will slightly overcount each county.

### 6.2 State files identify voters Tarrant County redacts

Tarrant replaces the name, VUID and address of 287 protected voters with "--Redacted--" (208 mail, 79 early in-person), keeping only precinct and date. No other county redacts identities; Harris hides the address (only) of named voters.

- **Mail:** the SOS Tarrant mail files list 134 mail voters who are not on Tarrant's mail roster (after excluding one voter of another county). For 123 of them there is a redacted Tarrant mail record with the same precinct and the same date.
- **In person:** the SOS Tarrant files list 93 in-person voters (besides 28 of other counties) who are not on Tarrant's roster on any day; 78 of them match a redacted Tarrant in-person record by precinct.

With three days of data, the match is close for both voting methods. This continues to indicate that the state publishes the name, VUID and precinct of voters whose identities Tarrant withholds; it cannot be proven record by record, because the redacted rows carry no identifier. Neither source exposes an address.

### 6.3 Reporting gaps in the state data

- **In-person:** Harris's October 21 (24,189 of 93,414); Fort Bend's October 20 (9,522 of 28,420) and October 21 (2 of 24,874); Webb's October 20 (no file) and October 21 (1); Gregg's October 19 (2 of 3,676); Hunt's October 20 (1,724 of 2,044).
- **Mail:** nine counties that post mail lists report almost none to the state (§5.2), and eight portal-only counties report none (§5.3).
- **Turnout table vs. CSV:** Dallas, Hidalgo, Coryell, Webb (§3).

### 6.4 Mail ballots in state data but not in the county's mail list

- **Travis:** Travis's roster ZIP holds mail lists for October 14 and October 19-21, but none for October 15 or 16. The state lists 1,484 Travis mail voters who are on none of Travis's lists, including 439 dated October 19 and 772 dated October 20 by the state. These are most likely the ballots received October 15-16; Travis should post those days' lists.
- **Timing:** Collin's 715 and Smith's 63 state-only mail voters are all dated October 21 by the state, after the county lists used here were produced.
- **Elsewhere:** Montgomery 77, Dallas 76, Bell 53, Harris 46, Webb 28 (all dated October 14), Denton 14, Tom Green 14, Gregg 11. None of these voters appears in the county's files under another voting method.

### 6.5 Precinct differences

- **Bowie:** the October 20 and 21 early vote reports, the first with a precinct column, give a precinct that matches the state's for 395 of 1,961 and 206 of 1,917 voters. Most differ by a whole precinct (for example 7 vs. 6A), so one of the two sources assigns precincts incorrectly; the county should check its report.
- **Elsewhere,** on matched voters: Harris 345 (333 in-person), Galveston 120 in-person, Tarrant 67, El Paso 42 in-person, Smith 20, Hunt 14, Montgomery 7, and 1-3 in five other counties.

Denton and Gregg write the precinct with a two-digit split appended ("418603" for 4186); this is formatting, not a difference.

### 6.6 Surnames

On matched voters, the surname differs between the county and the state in 20 Tarrant, 17 Collin, 15 Cameron, 12 Williamson, 11 Bexar and 11 Denton records, and in fewer than 8 elsewhere. In all but two of these records (§6.7) the two names share at least one name: they are name changes recorded in one system and not the other, or formatting (counties that write names "FIRST MIDDLE LAST").

### 6.7 One VUID, two different people

- Within **Harris's** cumulative mail file and a **Brazoria** mail file, one VUID is each listed for two different voters.
- One VUID is a mail ballot received October 15 on both **Bell's** and **Randall's** mail lists, for two people with different names.
- Ten VUIDs name a voter in an SOS file and an entirely different person (no name in common) on a county's roster: one each within Montgomery and Tarrant, and eight across counties, four of them on Bell's roster (against the SOS Coryell, Fort Bend, Randall and Victoria files) and the others between El Paso and Harris, Harris and Hidalgo, Tarrant and Hidalgo, and El Paso and Montgomery.

### 6.8 One voter on two counties' own rosters

- One VUID is an early in-person voter on October 19 in both **Bexar's** and **Webb's** rosters (same name).
- One VUID is a returned mail ballot on both **Hidalgo's** and **Cameron's** mail lists (same name).
- The voter reported on October 21 as a Cameron mail ballot and a Fort Bend in-person voter is no longer in Fort Bend's replaced October 21 file.

### 6.9 Mail ballot and in-person vote by the same voter

Nine voters are listed both as a returned mail ballot and as an early in-person voter in the same county: El Paso 4 (mail returned October 6-13; in person by October 21, two of them reported in the first review), Dallas 2 (mail October 9; in person October 19 and 20), Grayson 1 (mail October 2; in person October 20), Travis 1 (mail October 7; in person October 21) and Tarrant 1 (in person, and a mail ballot recorded October 21). A voter whose mail ballot has been accepted may not vote in person; these may be cancelled mail ballots, provisional votes or data errors.

### 6.10 Duplicates within county files

- **Same-day duplicate check-ins:** Bexar 6, Grayson 6, Collin 4, and 1 each in Travis, Galveston and Webb.
- **Mail ballot received twice:** 10 Bexar voters appear on two days' received-ballot lists; Travis 2, Gregg 2, and 1 each in Harris, Brazoria, Galveston, Brazos and Randall.

### 6.11 Identifiers that are not state VUIDs

| County | Records | ID length | Lists |
|---|---:|---|---|
| Tarrant | 261 | 8 digits | mail (218), in-person (43) |
| Collin | 100 | 6-9 digits | in-person (96), mail (2), provisional (2) |
| Denton | 79 | 7 digits | in-person |
| El Paso | 56 | 7 digits | mail (51), in-person (5) |
| Hidalgo | 35 | 8-9 digits | mail (27), in-person (8) |
| Grayson | 30 | 6, 8, 9 digits | in-person |
| Webb | 20 | 8 digits | mail (18), in-person (2) |
| Smith | 9 | 7 digits | in-person |
| Bowie | 7 | 9 or 11 digits | mail |
| Bexar | 6 | 8, 9, 11 digits | in-person |
| Gregg | 6 | 6, 7, 11 digits | in-person |
| Tom Green | 2 | 9 digits | mail |
| Henderson | 1 | 8 digits | in-person |

These are likely county-internal numbers for voters whose state VUID had not been assigned or synchronized, or (for the 9- and 11-digit cases) a VUID with a digit missing or added. The state files contain none, so these voters cannot be matched by ID.

## 7. Other county data quality notes

- **Blank VUIDs** (named voters with no ID): Harris 330 mail ballots; Collin 91 (81 provisional); Grayson 34 in-person; Bell 21 mail; Bexar 17 in-person; Dallas 14 mail; Hunt 7; Fort Bend 6. Travis's 247 and Montgomery's 61 Limited Ballot voters have no VUID by design. Tarrant's 287 redacted records also have no VUID (§6.2).
- **No voting date:** Tarrant's, Hidalgo's and Potter's early voting lists, Denton's from October 20, El Paso's from day 2 and McLennan's list carry no date per voter. Wichita's October 21 absentee list no longer shows the date each ballot was received, and Smith's and Wichita's absentee lists are cumulative, so the date in their file names is the list date.
- **Not yet posted** by the evening of October 22: Gregg's October 21 roster, Tom Green's and Angelina's October 20-21 rosters.
- **Files replaced in place:** Victoria and Potter replace one file daily, Randall re-posts its two rosters, McLennan posts one list under a new timestamped name, Bastrop renames its mail CSV daily, and Fort Bend replaced its October 21 file the same day.
- **Same-day posting:** Collin, Denton and Webb post lists for the current day during that day; they are not final until the next day's posting.
- **Hard-to-read formats:** Guadalupe's mail lists are scanned images; Wichita's PDFs sit behind a bot challenge, and its October 21 absentee list wraps each record onto two lines; Smith's lists are in a JavaScript document portal; Randall prints its mail roster sideways; Grayson's check-in workbook is a printed report with its column titles on separate rows.

## 8. Recommendations

1. **Download daily** through Election Day: Tarrant, Hidalgo, Denton, El Paso and Potter (no voting date per voter), Victoria and McLennan (files replaced), and Collin, Williamson, Cameron, Dallas, Fort Bend and Harris (latest file only). Save same-named files with the download time.
2. **Prefer county files** for counts and voter lists; use the SOS portal for the 17 counties that post nothing else and as a cross-check. Re-download SOS days for at least two days after the date, since the state fills in and revises them, and do not read SOS mail dates as received dates.
3. **When using SOS county files,** expect voters of other counties in each file (§6.1) and check them against neighboring counties before treating them as anomalies.
4. **Refer to the counties:** the nine voters with both a mail ballot and an in-person vote; the cross-county and shared VUIDs (§6.7-§6.8); Bowie's precincts; Travis's missing October 15-16 mail lists; Bexar's mail ballots received twice; and the precinct differences in Harris, Galveston, Tarrant and El Paso.
5. **Refer to Tarrant County and the Secretary of State:** the publication by the SOS of voters Tarrant redacts (§6.2), the SOS files' inclusion of other counties' voters (§6.1), the counties whose mail ballots are missing at the state (§6.3), and the Dallas turnout-table overstatement (§3).
6. **Repeat this comparison** after early voting closes and after the canvass.

## Appendix A. Files

Downloaded files are kept under `Voter_Rosters/TX_<County>_County/`:

- county files are saved as `G26_<county file name>`; files a county posts under the same name every day get the download date (`_2026-10-22`), or the download date and time (`_2026-10-22-13-38-21`) for cumulative lists that carry no voting dates (Hidalgo, Potter);
- SOS files are saved as `G26_<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv`; when the state revises a day, the earlier copy is kept in `_sos_superseded`.

The files load in Election Explorer (File → Load Voter Roster); the comparisons in this report were produced from the loader's normalized output.

## Appendix B. Caveats

- In-person comparisons cover October 19-21; where the state's file for a day is partial, that day is shown but left out of the totals.
- County files were downloaded October 20-22 at different times; several counties post same-day lists that are later replaced, and counties keep revising earlier days.
- Day-level counts for cumulative, undated county lists were recovered from consecutive snapshots and attribute voters added late to the day they first appear (Tarrant, §5.1).
- A match between two records means the same VUID; name and precinct differences were checked separately. A match by name, precinct or date without a common VUID (§6.2, §6.11) is an inference, not a confirmed identity.
- This review describes data as published. It does not establish that any voter voted improperly; the records flagged in §6.5-§6.10 have ordinary clerical explanations as well as serious ones, and only the counties can resolve them.
