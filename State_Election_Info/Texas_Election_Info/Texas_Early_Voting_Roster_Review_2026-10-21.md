# Texas Early Voting Rosters: County and State Data Review

**Election:** November 3, 2026 General Election\
**Data through:** Tuesday, October 20, 2026 (second day of in-person early voting)\
**Prepared:** October 21, 2026 (updates the review of October 20; state data as posted by 8:30 PM October 21)\
**Scope:** the 50 most populous Texas counties and the Texas Secretary of State (SOS) roster portal

## 1. Summary

Texas Election Code §87.121 requires each county to post its early voting rosters (in-person and by mail) daily and to submit them for statewide elections to the Secretary of State, which republishes them. This update extends the October 20 review from the 20 largest counties to the 50 largest, adds the second day of in-person voting, and compares the county and state lists voter by voter (by Voter Unique Identifier, VUID).

Key findings:

1. **Where the state has a county's data, the two sources agree closely.** For October 19, the county's and the state's lists of early in-person voters agree on more than 99% of voters in 20 counties (Travis, Denton, Hidalgo, Montgomery, Williamson, Cameron, Brazoria, Bell, Galveston, Webb, Smith, Brazos, Grayson, Randall, Wichita, Tom Green, Bastrop, Bowie, Henderson, Angelina) and on 97-98% in Dallas, Bexar and Hunt. No voter is an in-person voter in one source and a mail voter in the other.
2. **The state has almost none of October 20.** As of the evening of October 21, only 15 of the 50 counties had an SOS file for October 20, and those were mostly partial (Harris 1,723 of the county's 99,910 voters; Collin 2,293 of 33,875; Fort Bend 2,592 of 28,420). The portal's turnout table, last updated October 20, shows 18,710 in-person voters statewide for October 20 against 628,347 for October 19, with 193 of 254 counties at zero.
3. **The state caught up on October 19 for most counties, but not all.** Since the first review, the state's October 19 counts rose to match the counties for Bexar, Williamson and Galveston; Harris (28% of the county's count), Collin (47%), Fort Bend (24%), El Paso (17%) and Tarrant (7%) still lag, and Gregg's state file lists 2 of the county's 3,676 voters.
4. **The state's county files contain voters of other counties.** Across 38 SOS county files, 175 voters (166 in-person, 9 mail) appear who are on a different county's own roster, with that county's precinct. This accounts for most "state-only" voters in each county comparison.
5. **The state's files still name voters that Tarrant County redacts.** Tarrant withholds the name and VUID of 307 protected voters. The SOS Tarrant mail files list 87 mail voters who are not in Tarrant's mail roster; 81 of them match a redacted Tarrant record by precinct and date. The state files contain no addresses.
6. **Travis's posted roster ZIP has no mail lists for October 15 and 16.** The state lists about 1,200 Travis mail voters (dated October 19 and 20 by the state) who are on none of Travis's mail lists. The state's mail dates run three to six days behind the counties' received dates.
7. **Counties revise earlier days.** Dallas's October 20 report lists 1,305 more October 19 in-person voters than its October 19 report did; the state still has the original count.
8. **Records that warrant review by the counties** (none is proof of wrongdoing; most have clerical explanations):
   - 6 voters listed both as a returned mail ballot and as an early in-person voter (Dallas 2, El Paso 2, Grayson 1, Tarrant 1);
   - 4 VUIDs on two counties' own rosters, including one voter with a mail ballot returned in Cameron and an in-person vote in Fort Bend, and one VUID used for two different people (Bell and Randall mail lists);
   - 2 VUIDs used for two different people within one county's file (Harris, Brazoria), and 3 VUIDs that one county's state file assigns to a different person than another county's roster;
   - 9 Bexar mail ballots listed as received on two different days;
   - 16 same-day duplicate early in-person check-ins across 7 counties (and 5 voters listed twice in Hidalgo's cumulative list);
   - 110 Galveston and 55 Harris voters whose precinct differs between the county and the state. Williamson's 842 mail-ballot precinct differences from the first review are gone in its latest file.
9. **Several counties must be downloaded every day**, now including Tarrant, Hidalgo, Denton and El Paso (cumulative early voting lists without the day each voter voted), Victoria and Potter (one file replaced daily), McLennan (one undated list) and Collin, Williamson and Cameron (latest file only).

## 2. Scope and method

**Counties (by 2025 Census population):**

- Ranks 1-20: Harris, Dallas, Tarrant, Bexar, Travis, Collin, Denton, Fort Bend, Hidalgo, El Paso, Montgomery, Williamson, Cameron, Brazoria, Bell, Galveston, Nueces, Lubbock, Hays, Webb.
- Ranks 21-50: McLennan, Jefferson, Smith, Brazos, Ellis, Johnson, Kaufman, Comal, Guadalupe, Midland, Parker, Ector, Grayson, Randall, Taylor, Rockwall, Wichita, Gregg, Hunt, Liberty, Tom Green, Bastrop, Potter, Bowie, Victoria, Henderson, Angelina, Orange, Coryell, Walker.

**Sources:**

- each county's own roster files for the November 3 election, downloaded October 20-21. 33 counties post their own rosters; 32 could be read (Guadalupe's mail lists are scanned images). The other 17 (Nueces, Lubbock, Hays, Jefferson, Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker) link to the SOS portal;
- the SOS portal's per-county early voting CSV for every early voting date from October 14 to October 20 that had data, for all 50 counties, as posted at 8:30 PM October 21;
- the SOS turnout table for October 19 and 20.

**Method.** All files were read with Election Explorer's roster loader, which normalizes each county's layout to VUID, precinct, name, voting method and date. For every county file, the number of VUIDs loaded was checked against an independent count of the 10-digit identifiers in the raw file; they match exactly except Gregg's PDFs (within 0.5%). The lists were then compared by VUID:

- *early in-person, by day*: the county's in-person voters on October 19 (and October 20) vs. the SOS's IN-PERSON voters for that date. For counties whose lists are cumulative and undated (Tarrant, Hidalgo, Denton's October 20 list, El Paso's day 2 file), the day was recovered from consecutive daily snapshots (October 20 = the October 20 snapshot minus the October 19 one);
- *mail*: the county's latest mail list (or all daily files) vs. the SOS's MAIL-IN voters for October 14-20.

"County-only" and "state-only" voters were checked against the other 49 counties' files. Name and precinct comparisons ignore formatting (case, spacing, leading zeros, "Precinct"/"P"/"S" prefixes, and a two-digit precinct split such as "418603" for precinct 4186). Voter names and VUIDs are deliberately not reproduced in this report.

## 3. The Secretary of State portal

**Where:** "Election Information & Turnout Data", `https://goelect.txelections.civixapps.com/ivis-evr-ui/evr` (an Angular application by Civix). Pick an election and an early voting date, then *Submit*: a table of registered voters, in-person and mail turnout per county, with a **CSV Download** per county under *Voter Details Report*.

**How the data is served.** The page fetches JSON whose `upload` field is the file in base64:

| Data | Request |
|---|---|
| Elections, early voting dates and county ids | `/api-ivis-system/api/v1/getFile?type=EVR_ELECTION` |
| Turnout table for one date | `/api-ivis-system/api/v1/getFile?type=EVR_EARLYVOTING&electionId=53815&electionDate=10/20/2026` |
| One county's voter CSV for one date | `/api-ivis-system/api/v1/getFileByFormat?type=EVR_EARLYVOTING&electionId=53815&countyId=<id>&county=<NAME>&electionDate=<MM/DD/YYYY>&format=CSV` |

County ids follow alphabetical order, except that the portal sorts McLennan after McCulloch (id 161). An empty response means the county reported nothing for that day.

**File contents.** `VOTER_NAME` ("LAST, FIRST MIDDLE"), `ID_VOTER` (VUID), `VOTING_METHOD` (`IN-PERSON` / `MAIL-IN`), `PRECINCT`. There is **no address and no date column**. A county CSV holds only the voters credited on that date; the statewide report is cumulative.

**Timeliness.** By the evening of October 21, the portal had October 20 files for 15 of the 50 counties, and the turnout table still carried the October 20 update time, with 193 of 254 counties showing zero October 20 voters. The statute's 11 a.m. next-day posting schedule is not reflected in the state's data for October 20.

**Mail dates.** The date of an SOS mail file is not the day the county received the ballot. Travis's case shows the lag: the state's October 16 mail voters are almost all ballots Travis lists as received October 13, and its October 19 mail voters are mostly Travis's October 14 ballots.

**Turnout table vs. CSVs.** The portal's table and its own CSVs still disagree for some counties: Dallas 103,060 in-person voters for October 19 in the table vs. 76,204 in the CSV (the county itself lists 77,548); Hidalgo 17,828 vs. 17,316; Harris 28,755 vs. 29,134; Tarrant 3,000 vs. 3,301; Fort Bend 7,541 vs. 7,693; for October 20, Coryell 1,868 vs. 1,443 and Johnson 28 vs. 49.

**Name formatting.** The SOS appends name suffixes to the middle name without a space ("RAYJR"), which causes apparent name mismatches against county files.

## 4. How each county posts its rosters

"Cumulative" means each new file contains the earlier ones; "daily" means each file holds only that day.

### 4.1 Counties 1-20

| County | Where | Format | Granularity | Voting date per voter | Download daily? |
|---|---|---|---|---|---|
| Harris | harrisvotes.com, Election Rosters | ZIP of CSV | Mail cumulative; early voting one CSV per day, all in one ZIP replaced daily (since October 21) | Yes | Recommended |
| Dallas | dallascountyvotes.org, election page | XLSX | Mail cumulative; in-person report cumulative from October 20 | Yes | Recommended |
| Tarrant | tarrantcountytx.gov, current election | ZIP of TSV | Cumulative, same file name | Mail yes; **early voting no** | **Yes** |
| Bexar | elections.bexar.gov Document Center | PDF | Daily | Yes | No |
| Travis | votetravis.gov, one ZIP of daily workbooks | ZIP of XLSX | Daily workbooks in one ZIP | Yes | No (but see §6.4) |
| Collin | collincountytx.gov/Elections/rosters | XLSX | Cumulative, posted the same day ("through October 21") | Yes | Recommended |
| Denton | votedenton.gov, rosters page | XLSX | Mail cumulative; early voting dated on October 19, **cumulative and undated from October 20** | October 19 only | **Yes** |
| Fort Bend | fortbendcountytx.gov, Early Voting Statistics | XLSX | Mail cumulative; early voting cumulative from October 21 (the October 19 file was one day, undated) | Yes, from October 21 | Recommended |
| Hidalgo | hidalgocounty.us Document Center (updated in place) | CSV / XLSX | Cumulative | **No** | **Yes** |
| El Paso | epcountyvotestx.gov, Current Election | CSV (mail), XLSX | Mail cumulative; early voting **cumulative from day 2, undated** | Mail yes; early voting no | **Yes** |
| Montgomery | elections.mctx.org, Early Voting Roster | ZIP of CSV per day | Daily | Yes | No |
| Williamson | wilcotx.gov Document Center (one document replaced) | XLSX | Cumulative | Yes | Recommended |
| Cameron | cameroncountytx.gov, Elections results | XLSX (per jurisdiction) | Cumulative, replaced | Yes | Recommended |
| Brazoria | brazoriacountyclerktx.gov, Voter Lists | XLSX | Daily | Yes | No |
| Bell | bellcountytx.com, Daily Early Voting Reports | PDF | Daily | Yes | No |
| Galveston | galvestonvotes.org, Current Elections | CSV (mail), XLSX | Daily | Yes | No |
| Webb | webbcountytx.gov, Daily Voted List | PDF | Daily (posted the same day) | Yes | No |
| Nueces, Lubbock, Hays | link to the SOS portal | SOS CSV | Daily (SOS) | File date | Re-download periodically |

### 4.2 Counties 21-50

| County | Own roster | Format and posting |
|---|---|---|
| McLennan | Yes | One PDF "List of Early Voters: In-Person and By-Mail", replaced with a timestamped name; no method or date per voter |
| Smith | Yes | Daily PDFs in a Laserfiche public portal; in-person lists daily, **absentee lists cumulative** |
| Brazos | Yes | Daily PDFs (mail from September 23, in-person from October 19) |
| Grayson | Yes | Daily XLSX check-in reports (a printed report saved as a workbook); absentee list |
| Randall | Yes | Two PDFs re-posted daily (early voting roster with check-in time; mail roster printed sideways) |
| Wichita | Yes | Daily PDFs (check-ins, absentee), behind a bot challenge that blocks automated downloads |
| Gregg | Yes | Daily PDFs (check-ins with polling place; mail list with mail and received dates) |
| Hunt | Yes | Daily PDFs; each later file also lists the earlier days |
| Tom Green | Yes | Daily PDFs (roster, ballots by mail) |
| Bastrop | Yes | Daily CSVs; mail CSV re-posted under a new name each day |
| Potter | Yes | Two PDFs replaced daily; early voting roster cumulative and undated |
| Bowie | Yes | Daily PDFs (ballots by mail received, early vote history) |
| Victoria | Yes | One PDF and XLSX replaced daily ("mail and in person"); in-person voters of the latest day only |
| Henderson | Yes | Daily PDFs |
| Angelina | Yes | Daily PDF (voter history) |
| Guadalupe | Mail only | Daily returned-mail PDFs, scanned images (not machine-readable) |
| Jefferson, Ellis, Johnson, Kaufman, Comal, Midland, Parker, Ector, Taylor, Rockwall, Liberty, Orange, Coryell, Walker | No | Link to the SOS portal (Midland posted its own list for the May runoff, not for this election) |

## 5. County data compared with state data

### 5.1 Early in-person voters by day

| County | Oct 19 county | Oct 19 state | Both | County only | State only (other county) | Oct 20 county | Oct 20 state |
|---|---:|---:|---:|---:|---:|---:|---:|
| Harris | 104,659 | 29,134 | 29,119 | 75,540 | 15 (9) | 99,910 | 1,723 |
| Dallas | 77,548 | 76,204 | 76,191 | 1,357 | 13 (8) | 63,570 | no file |
| Tarrant | 50,294 | 3,301 | 3,273 | 47,021 | 28 (13) | 51,008 | no file |
| Bexar | 42,764 | 42,032 | 42,016 | 748 | 16 (8) | 44,733 | no file |
| Travis | 40,776 | 40,783 | 40,766 | 10 | 17 (15) | 40,359 | 1 |
| Collin | 34,389 | 16,239 | 16,198 | 18,191 | 41 (17) | 33,875 | 2,293 |
| Denton | 34,711 | 34,539 | 34,527 | 184 | 12 (7) | 31,551 | no file |
| Fort Bend | 31,743 | 7,693 | 7,676 | 24,067 | 17 (2) | 28,420 | 2,592 |
| Williamson | 25,694 | 25,702 | 25,680 | 14 | 22 (5) | 24,239 | 0 |
| Hidalgo | 17,424 | 17,316 | 17,313 | 111 | 3 (2) | 17,379 | no file |
| El Paso | 17,515 | 3,055 | 3,054 | 14,461 | 1 (0) | 16,742 | no file |
| Brazoria | 14,321 | 14,314 | 14,311 | 10 | 3 (2) | 12,687 | no file |
| Galveston | 13,653 | 13,651 | 13,640 | 13 | 11 (3) | 12,017 | 0 |
| Montgomery | 13,568 | 13,560 | 13,551 | 17 | 9 (7) | 14,162 | 0 |
| Cameron | 9,424 | 9,428 | 9,411 | 13 | 17 (5) | 8,643 | no file |
| Bell | 7,111 | 7,095 | 7,093 | 18 | 2 (1) | 7,145 | no file |
| Smith | 5,861 | 5,827 | 5,827 | 34 | 0 | 5,980 | 0 |
| Grayson | 5,467 | 5,457 | 5,445 | 22 | 12 (6) | 4,769 | no file |
| Webb | 4,943 | 4,921 | 4,920 | 23 | 1 (0) | 4,308 | no file |
| Brazos | 4,561 | 4,563 | 4,560 | 1 | 3 (1) | 4,706 | no file |
| Randall | 4,394 | 4,399 | 4,392 | 2 | 7 (0) | 4,126 | 0 |
| Gregg | 3,676 | 2 | 0 | 3,676 | 2 (2) | 3,526 | no file |
| Henderson | 3,257 | 3,257 | 3,254 | 3 | 3 (3) | 2,823 | no file |
| Bastrop | 3,143 | 3,143 | 3,141 | 2 | 2 (1) | 2,888 | 6 |
| Wichita | 2,945 | 2,942 | 2,942 | 3 | 0 | 2,783 | no file |
| Tom Green | 2,634 | 2,620 | 2,617 | 17 | 3 (1) | not posted | no file |
| Angelina | 2,566 | 2,566 | 2,566 | 0 | 0 | not posted | no file |
| Bowie | 2,023 | 2,027 | 2,023 | 0 | 4 (0) | not posted | no file |
| Hunt | 1,839 | 1,798 | 1,792 | 47 | 6 (4) | 2,044 | no file |
| Victoria | (not in latest file) | 1,655 | | | | 1,758 | no file |
| Potter | 4,148 through Oct 20 (undated) | 2,179 | 2,178 | | 1 | | no file |
| McLennan | 16,829 through Oct 21, no method or date | 4,912 | | | | | 3,317 |

Counts are distinct VUIDs (county voters without a VUID are excluded). "County only" is large where the state has not received the county's data (Harris, Tarrant, Collin, Fort Bend, El Paso, Gregg). Dallas's 1,357 county-only voters are mostly the 1,305 October 19 voters Dallas added in its October 20 report. Every October 20 state file is partial, so no October 20 comparison is meaningful yet. McLennan's list matches the state's October 19-20 in-person and mail voters for all but 29 of 9,004.

### 5.2 Mail ballots

The SOS mail data begins October 14 and is dated by when the state received it, days after the county (§3); county mail lists go back to the first ballots received (September 21-22).

| County | County mail | State mail (Oct 14-20) | On both | State only (other county) |
|---|---:|---:|---:|---:|
| Harris | 26,389 | 6,083 | 6,075 | 8 |
| Tarrant | 13,344 | 5,422 | 5,334 | 88 (1); 81 match redacted voters, §6.2 |
| Dallas | 11,340 | 4,325 | 4,292 | 33 (1) |
| Travis | 8,090 | 3,281 | 2,060 | 1,221 (2); see §6.4 |
| Montgomery | 5,559 | 1,778 | 1,718 | 60 |
| Denton | 4,246 | 2,168 | 2,154 | 14 (1) |
| Collin | 3,730 | 911 | 884 | 27 |
| Williamson | 3,526 | 1,094 | 1,093 | 1 |
| Hidalgo | 3,179 | 945 | 944 | 1 |
| Fort Bend | 2,431 | 1,793 | 1,787 | 6 |
| Galveston | 2,170 | 205 | 203 | 2 (1) |
| Bell | 1,696 | 1,475 | 1,435 | 40 |
| Smith | 1,570 | 703 | 693 | 10 |
| Randall | 1,503 | 125 | 123 | 2 |
| Tom Green | 1,131 | 207 | 204 | 3 |
| Gregg | 1,048 | 552 | 542 | 10 |
| Grayson | 916 | 324 | 322 | 2 |
| El Paso | 5,410 | 100 | 100 | 0 |
| Webb | 373 | 207 | 179 | 28 (all dated Oct 14) |

Ten counties that post mail lists report essentially no mail ballots to the state: Bexar (15,083 in the county's list, 1 at the state), Brazoria (2,277), Cameron (1,174), Victoria (1,114), Brazos (1,111), Bastrop (869), Potter (812), Wichita (698), Bowie (688) and Hunt (37).

### 5.3 Counties on the state portal only

| County | SOS days on file | In person Oct 19 | In person Oct 20 | Mail Oct 14-20 |
|---|---|---:|---:|---:|
| Hays | Oct 14, 19 | 10,252 | no file | 477 |
| Lubbock | Oct 19 | 8,855 | no file | 0 |
| Ellis | Oct 14-16, 19 | 7,902 | no file | 280 |
| Nueces | Oct 14-17, 19, 20 | 7,617 | 0 | 1,938 |
| Jefferson | Oct 14, 19 | 6,988 | no file | 481 |
| Parker | Oct 19 | 5,945 | no file | 0 |
| Comal | Oct 14-17, 19 | 5,876 | no file | 785 |
| Johnson | Oct 18-20 | 5,857 | 49 | 0 |
| Rockwall | Oct 19, 20 | 4,969 | 11 | 0 |
| Guadalupe | Oct 14-16, 19 | 4,780 | no file | 358 |
| Kaufman | Oct 19 | 3,904 | no file | 66 |
| Taylor | Oct 14, 15, 19 | 3,185 | no file | 237 |
| Midland | Oct 14-16, 19 | 2,963 | no file | 46 |
| Orange | Oct 19 | 2,201 | no file | 0 |
| Liberty | Oct 19 | 1,952 | no file | 0 |
| Ector | Oct 19 | 1,809 | no file | 0 |
| Coryell | Oct 19, 20 | 1,805 | 1,443 | 0 |
| Walker | Oct 15, 19 | 1,740 | no file | 2 |

For these counties the state is the only published source. Eight of them have reported no mail ballots to the state.

## 6. Anomalies between county and state data

### 6.1 State county files include voters of other counties

175 voters (166 in-person, 9 mail) in 38 SOS county files appear on a different county's own roster with the same first name and that county's precinct. The largest numbers are in the SOS files for Travis (17), Collin (17), Tarrant (14), Harris (9), Dallas (9), Bexar (8) and Denton (8). No voter appears in two SOS county files, so these are records assigned to the wrong county's file, not duplicates. Anyone using SOS county files alone will slightly overcount each county.

### 6.2 State files identify voters Tarrant County redacts

Tarrant replaces the name, VUID and address of 307 protected voters with "--Redacted--" (208 mail, 99 early in-person), keeping only precinct and date. No other county redacts identities; Harris hides the address (only) of 1,073 named voters.

The SOS Tarrant mail files list 87 Tarrant mail voters who are not in Tarrant's own mail roster (after excluding one voter of another county). For 81 of the 87 there is a redacted Tarrant mail record with the same precinct and date. For in-person voting, the state lists 12 Tarrant voters (besides 13 of other counties) who are not on Tarrant's roster, and 6 of them match a redacted in-person record by precinct. This continues to indicate that the state publishes the name, VUID and precinct of voters whose identities Tarrant withholds; it cannot be proven record by record, because the redacted rows carry no identifier. Neither source exposes an address.

### 6.3 Reporting gaps in the state data

- **In-person, October 19:** Harris 29,134 of 104,659; Tarrant 3,301 of 50,294; Collin 16,239 of 34,389; Fort Bend 7,693 of 31,743; El Paso 3,055 of 17,515; Gregg 2 of 3,676.
- **In-person, October 20:** 15 of 50 counties have a state file, all partial (§5.1).
- **Mail:** ten counties that post their own mail lists report essentially none to the state (§5.2), as do nine portal-only counties (§5.3).
- **Turnout table vs. CSV:** see §3.

### 6.4 Mail ballots in state data but not in the county's mail list

- **Travis:** Travis's roster ZIP holds mail lists for October 14 and October 19-20, but none for October 15 or 16. The state lists 1,207 Travis mail voters, dated October 19 (437) and October 20 (770) by the state, who are on none of Travis's lists. These are most likely the ballots received October 15-16; Travis should post those days' lists.
- **Elsewhere:** Montgomery 60, Bell 40, Dallas 33, Webb 28 (all dated October 14), Collin 27, Denton 14, Smith 10, Gregg 10, Harris 8. None of these voters appears in the county's files under another voting method.

### 6.5 Precinct differences

On matched voters, the precinct differs between the county and the state for 110 Galveston in-person voters, 55 Harris voters (51 in person), 13 Hunt in-person voters, and 1-6 voters in eight other counties. Williamson's 842 mail-ballot precinct differences from the first review no longer appear in its October 20 file. Denton and Gregg write the precinct with a two-digit split appended ("418603" for 4186); this is formatting, not a difference.

### 6.6 Surnames

Thirty-two VUIDs carry a different surname in the county file than in the state file: Cameron 7, Bexar 4, Galveston 4, Grayson 3, and 1-2 in eleven other counties. This indicates name changes recorded in one system and not the other.

### 6.7 One VUID, two different people

- Within **Harris's** cumulative mail file, one VUID is listed for two different voters; within a **Brazoria** mail file, one VUID is listed for two voters with different first names.
- One VUID is a mail ballot received October 15 on both **Bell's** and **Randall's** mail lists, for two people with different names.
- Besides the Bell/Randall case, three VUIDs name one person in a county's SOS file and a different person on another county's own roster: Bell's roster against the SOS Coryell and Fort Bend files, and Tarrant's roster against the SOS Hidalgo file (the last two were in the first review).
- Within each county, every VUID whose name differs between the county and the state shares at least one name with it: those are name changes or formatting (§6.6), not different people.

### 6.8 One voter on two counties' own rosters

- One VUID is an early in-person voter on October 19 in both **Bexar's** and **Webb's** rosters (same name; reported in the first review).
- One VUID is a returned mail ballot on both **Hidalgo's** and **Cameron's** mail lists (same name; reported in the first review).
- One VUID is a returned mail ballot in **Cameron** (received October 20) and an early in-person voter in **Fort Bend**, under the same name.

### 6.9 Mail ballot and in-person vote by the same voter

Six voters are listed both as a returned mail ballot and as an early in-person voter in the same county: Dallas 2 (mail ballots returned October 9; in person October 19 and October 20), El Paso 2 (mail returned October 13; in person October 19; reported in the first review), Grayson 1 (mail returned October 2; in person October 20) and Tarrant 1 (in person, and a mail ballot recorded as received October 21). A voter whose mail ballot has been accepted may not vote in person; these may be cancelled mail ballots, provisional votes or data errors.

### 6.10 Duplicates within county files

- **Same-day duplicate check-ins:** Grayson 6, Collin 4, Bexar 2, and 1 each in Travis, Denton, Galveston and Webb; Hidalgo's cumulative list repeats 5 voters.
- **Mail ballot received twice:** 9 Bexar voters appear on two days' received-ballot lists; Travis 2, Gregg 2, and 1 each in Harris, Brazoria, Galveston and Brazos.

### 6.11 Identifiers that are not state VUIDs

| County | Records | ID length | Lists |
|---|---:|---|---|
| Tarrant | 245 | 8 digits | mail (218), in-person (27) |
| Denton | 67 | 7 digits | in-person |
| Collin | 62 | 6-7 digits | in-person (61), provisional (1) |
| El Paso | 41 | 7 digits | mail (38), in-person (3) |
| Hidalgo | 32 | 8 digits | mail (25), in-person (7) |
| Grayson | 24 | 6, 8, 9 digits | in-person (20), mail (4) |
| Webb | 18 | 8 digits | mail (16), in-person (2) |
| Bowie | 7 | 9 or 11 digits | mail |
| Gregg | 6 | 6, 7, 11 digits | in-person |
| Smith | 4 | 7 digits | in-person |
| Tom Green | 2 | 9 digits | mail |
| Bexar | 1 | 9 digits | in-person |

These are likely county-internal numbers for voters whose state VUID had not been assigned or synchronized, or (for the 9- and 11-digit cases) a VUID with a digit missing or added. The state files contain none, so these voters cannot be matched by ID between the two sources.

## 7. Other county data quality notes

- **Blank VUIDs** (named voters with no ID): Harris 1,159 mail ballots; Collin 57 (51 provisional); Fort Bend 47; Grayson 23 in-person; Bell 19 mail; Dallas 15 mail; Bexar 15 in-person; Hunt 3. Travis's 139 and Montgomery's 40 Limited Ballot voters have no VUID by design (they are not registered in the county). Tarrant's 307 redacted records also have no VUID (§6.2).
- **No voting date:** Tarrant's, Hidalgo's and Potter's early voting lists, Denton's from October 20, El Paso's day 2 file and McLennan's list carry no date per voter; Smith's absentee lists are cumulative, so the date in their file names is the list date, not the day received.
- **Files replaced in place:** Victoria and Potter replace one file daily, Randall re-posts its two rosters, McLennan posts one list under a new timestamped name, and Bastrop renames its mail CSV each day. Victoria's October 20 file lists only that day's in-person voters, so its October 19 in-person voters are no longer published by the county.
- **Same-day posting:** Collin, Denton and Webb post lists for the current day during that day; their files for a day are not final until the next day's posting.
- **Hard-to-read formats:** Guadalupe's mail lists are scanned images; Wichita's PDFs sit behind a bot challenge; Smith's lists are in a JavaScript document portal; Randall prints its mail roster sideways; Grayson's check-in workbook is a printed report with its column titles stacked on separate rows; Hunt's revised October 19 file switches column order without a header on 14 pages.
- **Name and precinct formats:** several counties write names "FIRST MIDDLE LAST" (Bexar, Denton, Brazoria, Galveston, Randall, Gregg), so a two-word surname cannot be told from a middle name. Precinct formats vary ("P "/"S " prefixes in Bexar and Bastrop, "Precinct 364" in Galveston, district suffixes in Hunt, split digits in Denton and Gregg).

## 8. Recommendations

1. **Download daily** through Election Day: Tarrant, Hidalgo, Denton, El Paso and Potter (no voting date per voter), Victoria and McLennan (files replaced), and Collin, Williamson, Cameron, Dallas, Fort Bend and Harris (latest file only). Save same-named files with the download time.
2. **Prefer county files** for counts and voter lists; use the SOS portal for the 17 counties that post nothing else and as a cross-check. Re-download SOS days after a few days, and do not read SOS mail dates as received dates.
3. **When using SOS county files,** expect a few voters of other counties in each file (§6.1) and check them against neighboring counties before treating them as anomalies.
4. **Refer to the counties:** the six voters with both a mail ballot and an in-person vote (Dallas, El Paso, Grayson, Tarrant); the cross-county VUIDs (Bexar/Webb, Hidalgo/Cameron, Cameron/Fort Bend, Bell/Randall); the shared VUIDs (Harris, Brazoria); Bexar's mail ballots received twice; Travis's missing October 15-16 mail lists; and the precinct differences in Galveston and Harris.
5. **Refer to Tarrant County and the Secretary of State:** the publication by the SOS of voters Tarrant redacts (§6.2), the SOS files' inclusion of other counties' voters (§6.1), the missing October 20 data and ten counties' missing mail data (§6.3), and the turnout-table discrepancies (§3).
6. **Repeat this comparison** after early voting closes and after the canvass.

## Appendix A. Files

Downloaded files are kept under `Voter_Rosters/TX_<County>_County/`:

- county files are saved as `G26_<county file name>`; files a county posts under the same name every day get the download date (`_2026-10-21`), or the download date and time (`_2026-10-21-13-59-44`) for cumulative lists that carry no voting dates (Hidalgo, Potter);
- SOS files are saved as `G26_<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv`, following the portal's own download name.

The files load in Election Explorer (File → Load Voter Roster); the comparisons in this report were produced from the loader's normalized output.

## Appendix B. Caveats

- In-person comparisons cover October 19 fully and October 20 only where noted; the state's October 20 data was incomplete when collected.
- County files were downloaded October 20-21 at different times and reflect different cut-off times; several counties post same-day lists that are later replaced.
- Day-level counts for cumulative, undated county lists (Tarrant, Hidalgo, Denton, El Paso) were recovered from consecutive snapshots and assume each snapshot was complete through the previous day.
- A match between two records means the same VUID; name and precinct differences were checked separately. A match by name, precinct or date without a common VUID (§6.2, §6.11) is an inference, not a confirmed identity.
- This review describes data as published. It does not establish that any voter voted improperly; the records flagged in §6.6-§6.10 have ordinary clerical explanations as well as serious ones, and only the counties can resolve them.
