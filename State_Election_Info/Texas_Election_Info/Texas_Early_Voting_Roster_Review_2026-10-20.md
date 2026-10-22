# Texas Early Voting Rosters: County and State Data Review

**Election:** November 3, 2026 General Election\
**Data through:** Monday, October 19, 2026 (first day of in-person early voting)\
**Prepared:** October 20, 2026\
**Scope:** the 20 most populous Texas counties and the Texas Secretary of State (SOS) roster portal

## 1. Summary

Texas Election Code §87.121 requires each county to post its early voting rosters (in-person and by mail) daily, and requires counties to submit rosters for statewide elections to the Secretary of State, which republishes them. This review downloaded both sources for the 20 largest counties after the first day of early voting and compared them voter by voter (by Voter Unique Identifier, VUID).

Key findings:

1. **Where a county is current with the state, the two sources agree closely.** In eight counties (Travis, Dallas, Denton, Montgomery, Cameron, Brazoria, Bell, Webb) the county's and the state's lists of early in-person voters for October 19 agree on more than 99% of voters (Travis: 40,766 of 40,777 voters on both lists; Dallas 76,191 of 76,243; the widest gap is Denton, 0.6%). No voter is listed as an in-person voter by one source and a mail voter by the other.
2. **The state's data lags badly for some counties.** For October 19 the SOS lists 17 in-person voters for Tarrant (the county lists 50,409), 1 for Fort Bend (31,831), 7,260 for Harris (104,659), and between 7% and 48% of the county's count for El Paso, Collin, Galveston and Williamson. Bexar, Cameron, Brazoria and Lubbock report essentially no mail ballots to the state.
3. **The state's county files contain voters of other counties.** Across the 20 SOS county files, 82 voters appear who are on a *different* county's own roster, with that other county's precinct number (for example, none of the 17 SOS "Tarrant" in-person voters on October 19 is on Tarrant's own roster; 10 of them are on the Collin, Denton or Bell rosters). This accounts for most "state-only" voters in each county comparison.
4. **The state's files name voters that Tarrant County redacts.** Tarrant withholds the name and VUID of 320 protected voters. The SOS Tarrant mail files list 105 mail voters who are not in Tarrant's mail roster, and 99 of them match a redacted Tarrant record by precinct and date (daily counts 35/31/25/14 vs. 35/30/27/21). The state files contain no addresses, so no confidential address is exposed by either source.
5. **Records that warrant review by the counties** (none is proof of wrongdoing; most have clerical explanations):
   - 3 voters listed both as a returned mail ballot and as an early in-person voter (Dallas 1, El Paso 2);
   - 2 VUIDs on two counties' own rosters (one in-person in both Bexar and Webb on 10/19; one mail ballot in both Hidalgo and Cameron);
   - 4 VUIDs used for two different people (within Harris's mail file; within a Brazoria mail file; and across two pairs of counties);
   - 9 Bexar mail ballots listed as received on two different days;
   - 12 same-day duplicate early in-person check-ins across 7 counties;
   - 842 Williamson mail voters whose precinct differs between the county and the state;
   - 12 Cameron voters whose surname differs between the county and the state.
6. **Several counties must be downloaded every day.** Harris's early voting file holds only the latest day and is overwritten at the same address; Tarrant, Hidalgo, Denton and Fort Bend post early voting lists without the date each voter voted, so the date can only be recovered from daily snapshots. Collin, Williamson, Denton and Cameron keep only the latest file on their sites.

Early voting had been open one day when this data was collected, so the in-person comparisons cover a single day; the mail comparisons cover September 21 to October 19. Counts will change as counties and the state update their files.

## 2. Scope and method

**Counties (by population):** Harris, Dallas, Tarrant, Bexar, Travis, Collin, Denton, Fort Bend, Hidalgo, El Paso, Montgomery, Williamson, Cameron, Brazoria, Bell, Galveston, Nueces, Lubbock, Webb, Hays.

**Sources downloaded on October 20, 2026:**

- each county's own roster files for the November 3 election (17 counties; Lubbock, Nueces and Hays post none and link to the SOS portal);
- the SOS portal's per-county early voting CSV for every early voting date from October 14 to October 19 that had data, for all 20 counties.

**Method.** All files were read with Election Explorer's roster loader (which normalizes each county's layout to VUID, precinct, name, voting method and date), cross-checked against independent row counts of the raw files, and compared by VUID:

- *early in-person*: the county's in-person voters vs. the SOS's IN-PERSON voters for October 19;
- *mail*: the county's mail ballots (cumulative or all daily files) vs. the SOS's MAIL-IN voters for October 14-19.

"County-only" and "state-only" voters were then checked against every other county's files. Name and precinct comparisons ignore formatting (case, spacing, leading zeros, "Precinct"/"P"/"S" prefixes). Voter names and VUIDs are deliberately not reproduced in this report.

## 3. The Secretary of State portal

**Where:** "Election Information & Turnout Data", `https://goelect.txelections.civixapps.com/ivis-evr-ui/evr` (an Angular application by Civix). Pick an election and an early voting date, then *Submit*: a table of registered voters, in-person and mail turnout per county, with a **CSV Download** per county under *Voter Details Report* and a *Generate Statewide Report* button.

**How the data is served.** The page fetches JSON whose `upload` field is the file in base64:

| Data | Request |
|---|---|
| Elections and their early voting dates | `/api-ivis-system/api/v1/getFile?type=EVR_ELECTION` |
| Turnout table for one date | `/api-ivis-system/api/v1/getFile?type=EVR_EARLYVOTING&electionId=53815&electionDate=10/19/2026` |
| One county's voter CSV for one date | `/api-ivis-system/api/v1/getFileByFormat?type=EVR_EARLYVOTING&electionId=53815&countyId=<id>&county=<NAME>&electionDate=<MM/DD/YYYY>&format=CSV` |

The November 3, 2026 general election is election 53815; its listed early voting dates start October 14 (mail ballots only until in-person voting began October 19). These requests work from a script; an empty response means the county reported nothing for that day, and the server occasionally returns empty responses or HTTP 502 for a while, so failed downloads should be retried later.

**File contents.** `VOTER_NAME` ("LAST, FIRST MIDDLE"), `ID_VOTER` (VUID), `VOTING_METHOD` (`IN-PERSON` / `MAIL-IN`), `PRECINCT`. There is **no address and no date column**.

**Daily, not cumulative.** A county CSV holds only the voters credited on that date: Travis's October 14, 16 and 19 files share no voter. (The statewide report, by contrast, is cumulative.) Earlier dates are revised after the fact, so a day's file depends on when it is downloaded.

**Internal inconsistency.** The portal's own turnout table and its CSVs disagree for some counties: Dallas's table shows 103,056 in-person voters for October 19 but its CSV lists 76,200; Hidalgo 17,828 vs. 17,316; Williamson 11,737 vs. 12,155; Nueces 1,586 vs. 1,689. (The table and CSVs were fetched about 30 minutes apart, which may explain the small differences but not Dallas's.)

**Name formatting.** The SOS appends name suffixes to the middle name without a space (a middle name of "RAY" with suffix "JR" appears as "RAYJR"), which causes apparent name mismatches against county files.

## 4. How each county posts its rosters

"Cumulative" means each new file contains the earlier ones; "daily" means each file holds only that day. This was verified by comparing consecutive snapshots where two were available (for example, Harris's mail file of October 12 is wholly contained in the October 20 file; Galveston's, Webb's, Bexar's and Montgomery's consecutive daily files share no voters).

| County | Where | Format | Granularity | Voting date per voter | Site keeps history | Download daily? |
|---|---|---|---|---|---|---|
| Harris | harrisvotes.com, Election Rosters | ZIP of CSV | Mail cumulative; **early voting one day, file overwritten daily** (named "Cumulative") | Yes | No | **Yes** (early voting) |
| Dallas | dallascountyvotes.org, election page | XLSX | Mail cumulative; in-person report dated per day | Yes | Latest only | Recommended |
| Tarrant | tarrantcountytx.gov, current election | ZIP of TSV | Cumulative | Mail yes; **early voting no** | No (same file name) | **Yes** (to know the day voted) |
| Bexar | elections.bexar.gov Document Center | PDF (printed from Excel) | Daily (mail by day received; early voting check-ins per day) | Yes | Yes | No |
| Travis | votetravis.gov, current election (ZIP of daily workbooks) | ZIP of XLSX | Daily workbooks, all in one ZIP | Yes | ZIP holds all days | No |
| Collin | collincountytx.gov/Elections/rosters | XLSX | Cumulative ("through October 19") | Yes | Latest only | Recommended |
| Denton | votedenton.gov, rosters page | XLSX | Mail cumulative; in-person "daily" | Mail yes; **early voting no** | Latest only | **Yes** |
| Fort Bend | fortbendcountytx.gov, Early Voting Statistics | XLSX | Mail cumulative; early voting per day | Mail yes; **early voting no** | Latest only | **Yes** |
| Hidalgo | hidalgocounty.us Document Center (updated in place) | CSV / XLSX | Cumulative | **No** (neither list) | No | **Yes** |
| El Paso | epcountyvotestx.gov, Current Election | CSV (mail), XLSX (early voting) | Mail cumulative snapshots; early voting per day | Yes | Yes (links kept) | Recommended until confirmed |
| Montgomery | elections.mctx.org, Early Voting Roster | ZIP of CSV per day | Daily | Yes | Yes | No |
| Williamson | wilcotx.gov Document Center (one document replaced) | XLSX | Cumulative | Yes | No | Recommended (dropped voters only visible in snapshots) |
| Cameron | cameroncountytx.gov, Elections results | XLSX (per jurisdiction) | Cumulative, replaced | Yes | Latest only | Recommended |
| Brazoria | brazoriacountyclerktx.gov, Voter Lists folders | XLSX | Daily | Yes | Yes | No |
| Bell | bellcountytx.com, Daily Early Voting Reports | PDF (printed from Excel) | Daily | Yes | Yes | No |
| Galveston | galvestonvotes.org, Current Elections | CSV (mail), XLSX (check-ins) | Daily | Yes | Yes | No |
| Webb | webbcountytx.gov, Daily Voted List | PDF | Daily | Yes | Yes | No |
| Lubbock, Nueces, Hays | link to the SOS portal | SOS CSV | Daily (SOS) | Yes (file date) | Yes, but revised | Re-download periodically |

Notes on posting:

- **Cameron** posts a roster for every jurisdiction on the ballot (county, school districts, cities); the jurisdiction files are subsets of the countywide one, so only the countywide files should be used.
- **Collin's** mail workbook ("absentee returns") lists every ballot mailed, with `-` in the received-date column for the 10,257 ballots not yet returned; only the 3,883 rows with a received date are returned ballots.
- **Brazoria's** file named "09-28 THRU 10-16" also contains the 276 ballots received October 19, duplicating its October 19 file.
- **Dallas's** precinct-range ZIPs from earlier elections were not posted for this election by October 20.
- **Bexar** switched to PDF for 2026; its October 19 early voting check-in report is a 1,297-page PDF.
- Sites differ in how they block automated downloads: Brazoria requires browser-like request headers; CivicPlus Document Centers (Bexar, Hidalgo, Webb, Williamson) answer HEAD requests with 404 and list documents only through JavaScript.

## 5. County data compared with state data

### 5.1 Early in-person voters, October 19

| County | County | State | On both | County only | State only | State only, voter of another county |
|---|---:|---:|---:|---:|---:|---:|
| Travis | 40,776 | 40,777 | 40,766 | 10 | 11 | 9 |
| Harris | 104,659 | 7,260 | 7,248 | 97,411 | 12 | 7 |
| Dallas | 76,243 | 76,200 | 76,191 | 52 | 9 | 3 |
| Tarrant | 50,305 | 17 | 0 | 50,305 | 17 | 10 |
| Bexar | 42,765 | 30,707 | 30,693 | 12,072 | 14 | 7 |
| Collin | 34,417 | 5,974 | 5,951 | 28,466 | 23 | 13 |
| Denton | 34,738 | 34,536 | 34,527 | 211 | 9 | 4 |
| Fort Bend | 31,788 | 1 | 0 | 31,788 | 1 | 0 |
| Williamson | 25,694 | 12,155 | 12,149 | 13,545 | 6 | 4 |
| Hidalgo | 17,427 | 17,316 | 17,313 | 114 | 3 | 2 |
| El Paso | 17,516 | 1,248 | 1,247 | 16,269 | 1 | 0 |
| Brazoria | 14,321 | 14,312 | 14,311 | 10 | 1 | 0 |
| Galveston | 13,653 | 6,411 | 6,402 | 7,251 | 9 | 2 |
| Montgomery | 13,568 | 13,555 | 13,551 | 17 | 4 | 2 |
| Cameron | 9,424 | 9,427 | 9,411 | 13 | 16 | 4 |
| Bell | 7,111 | 7,094 | 7,093 | 18 | 1 | 0 |
| Webb | 4,943 | 4,921 | 4,920 | 23 | 1 | 0 |
| Hays | — | 10,251 | | | | |
| Lubbock | — | 8,855 | | | | |
| Nueces | — | 1,689 | | | | |

Counts are distinct VUIDs (county voters without a VUID are excluded). "County only" is large where the state has not yet received the county's data (Harris, Tarrant, Bexar, Collin, Fort Bend, Williamson, El Paso, Galveston).

### 5.2 Mail ballots

The SOS mail data begins October 14; county mail lists go back to the first ballots received (September 21-22), so most county mail voters are expected to be "county only".

| County | County mail | State mail (10/14-10/19) | On both | State only |
|---|---:|---:|---:|---:|
| Travis | 5,072 (through 10/14) | 2,492 | 2,041 | 451 (437 dated 10/19, after the county's list) |
| Harris | 23,721 | 6,083 | 6,075 | 8 |
| Dallas | 11,226 | 4,325 | 4,292 | 33 |
| Tarrant | 11,719 | 5,276 | 5,170 | 106 (99 match redacted Tarrant voters; §6.2) |
| Bexar | 14,605 | 1 | 1 | 0 |
| Collin | 3,883 | 726 | 726 | 0 |
| Denton | 3,724 | 2,168 | 2,155 | 13 |
| Fort Bend | 2,330 | 128 | 127 | 1 |
| Hidalgo | 3,130 | 945 | 944 | 1 |
| El Paso | 4,896 | 54 | 54 | 0 |
| Montgomery | 5,387 | 1,624 | 1,575 | 49 |
| Williamson | 3,232 | 944 | 943 | 1 |
| Cameron | 1,053 | 0 | 0 | 0 |
| Brazoria | 2,109 | 0 | 0 | 0 |
| Bell | 1,667 | 1,475 | 1,435 | 40 |
| Galveston | 2,041 | 204 | 202 | 2 |
| Webb | 345 | 207 | 179 | 28 (all dated 10/14) |

## 6. Anomalies between county and state data

### 6.1 State county files include voters of other counties

Each SOS county file contains a handful of voters who appear on a different county's own roster, with that county's precinct number and the same name (82 across the 20 files: Collin 13, Travis 11, Tarrant 11, Harris 7, Bexar 7, Denton 5, and 0-4 elsewhere). In every case checked, the voter appears once in the county where they are registered and again in the SOS file of an unrelated county. The most visible case is Tarrant: none of the 17 voters the SOS lists as Tarrant in-person voters on October 19 is in Tarrant's own roster; 10 of them are on the Collin, Denton or Bell rosters with those counties' precincts, and the other 7 are in none of the 20 counties' files (they may belong to counties outside this review). No voter appeared in two SOS county files, so the misplaced records are not duplicates within the state data; they are assigned to the wrong county's file. Anyone using SOS county files alone will slightly overcount each county and should check unexpected voters against neighboring counties.

### 6.2 State files identify voters Tarrant County redacts

Tarrant replaces the name, VUID and address of 320 protected voters with "--Redacted--" (216 mail, 104 early in-person), keeping only precinct and date; it also hides the addresses (only) of 47 other named voters. No other county among the 20 redacts identities; Harris (653), Collin (32), Denton (32), El Paso (18) and Brazoria (11) hide addresses only.

The SOS Tarrant mail files list 105 Tarrant mail voters who are not in Tarrant's own mail roster (after excluding one voter of another county). For 99 of the 105 there is a redacted Tarrant mail record with the same precinct and the same date, and the daily totals agree closely:

| Date | Tarrant redacted mail records | SOS-only Tarrant mail voters |
|---|---:|---:|
| Oct 14 | 35 | 35 |
| Oct 15 | 30 | 31 |
| Oct 16 | 27 | 25 |
| Oct 17 | 21 | 14 |

This strongly indicates that the state publishes the name, VUID and precinct of mail voters whose identities Tarrant withholds. It cannot be proven record by record, because the redacted rows carry no identifier. The 104 redacted in-person voters could not be checked because the state had almost no Tarrant in-person data. Neither source exposes an address (the SOS files have no address field). Whether the state's publication is consistent with the protection Tarrant applies is a question for Tarrant County Elections and the Secretary of State.

### 6.3 Reporting gaps in the state data

- **In-person, October 19:** Tarrant 17 of 50,409; Fort Bend 1 of 31,831; Harris 7,260 of 104,659; El Paso 1,248 of 17,516; Collin 5,974 of 34,522; Galveston 6,411 of 13,654; Williamson 12,155 of 25,694; Bexar 30,707 of 42,776.
- **Mail:** Bexar (1 voter), Cameron (0), Brazoria (0) and Lubbock (0) report essentially no mail ballots to the state, although the counties list 14,605, 1,053 and 2,109 returned mail ballots (Lubbock posts no county file).
- **Turnout table vs. CSV:** see §3 (Dallas 103,056 vs. 76,200).

### 6.4 Mail ballots in state data but not in the county's mail list

Beyond Tarrant (§6.2) and Travis (whose county mail list ended October 14), the state lists mail voters missing from the county's own cumulative list: Montgomery 49, Bell 40, Dallas 32, Webb 28 (all dated October 14), Denton 12, Harris 8 (after setting aside one Dallas and one Denton voter who belong to other counties). None of these voters appears in the county's files under another voting method. They may be ballots the county accepted after its list was produced, ballots later rejected or cancelled, or reporting errors; the counties' later lists will show which.

### 6.5 Williamson precincts

Of 13,092 Williamson voters on both lists, 842 have a different precinct in the county file than in the state file, and all 842 are mail ballots (none of the 12,149 matched in-person voters differ). The pattern suggests the two sources use different precinct fields for mail voters (for example, a precinct recorded when the ballot was issued vs. the current registration).

### 6.6 Cameron surnames

Twelve Cameron voters carry a different surname in the county file than in the state file under the same VUID. Most are entirely different surnames (for example, a married or former surname in one source and not the other); a few are a second surname present in one source only. This indicates name changes recorded in one system and not the other.

### 6.7 One VUID, two different people

- Within **Harris's** cumulative mail file, one VUID is listed for two different voters (different names and precincts).
- Within a **Brazoria** mail file, one VUID is listed for two voters with different first names and a one-letter surname difference.
- Across counties, one VUID is a mail voter in **Bell's** roster and a different person in the SOS **Fort Bend** file; another is a mail voter in **Tarrant's** roster and a different person in the SOS **Hidalgo** file.
- In **Webb**, the same voter (same surname, first name and precinct; a full middle name in one source and an initial in the other) is listed under one VUID by the county and a different VUID by the state, which suggests a duplicate registration.

### 6.8 One voter on two counties' own rosters

- One VUID is an early in-person voter on October 19 in both **Bexar's** and **Webb's** own rosters (same name; the SOS lists the voter in its Bexar file with the Webb precinct number). The name is very common, so a check-in clerk selecting the wrong registration record in one county is a likely explanation.
- One VUID is a returned mail ballot on both **Hidalgo's** and **Cameron's** mail lists (same name, different precincts).

### 6.9 Mail ballot and in-person vote by the same voter

Three voters are listed both as a returned mail ballot and as an early in-person voter on October 19: one in **Dallas** (ballot returned October 9) and two in **El Paso** (ballots returned October 13). A voter whose mail ballot has been accepted may not vote in person; these records may reflect a cancelled mail ballot, a provisional vote, or a data error, and the counties can resolve them.

### 6.10 Duplicates within county files

- **Same-day duplicate check-ins:** the same voter appears twice in one day's early in-person list in Collin (3), Hidalgo (3), Fort Bend (2), Travis (1), Denton (1), Galveston (1) and Webb (1).
- **Mail ballot received twice:** 9 Bexar voters appear on two different days' received-ballot lists; 1 Galveston voter appears on consecutive days' lists (with the name order reversed); Travis lists 1 mail voter on two days' lists and 3 mail voters twice within a single day's file.

### 6.11 Identifiers that are not state VUIDs

Some counties list voters under a county-internal number instead of the 10-digit state VUID; the state files contain none:

| County | Records | ID length | Lists |
|---|---:|---|---|
| El Paso | 250 | 7 digits | mail (249), in-person (1) |
| Tarrant | 224 | 8 digits | mail (213), in-person (11) |
| Collin | 31 | 6-7 digits | in-person (28), mail (3) |
| Denton | 29 | 7 digits | in-person (27), mail (2) |
| Hidalgo | 27 | 8 digits | mail (24), in-person (3) |
| Webb | 16 | 8 digits | mail |
| Bexar | 1 | 9 digits | in-person |

These are likely voters whose state VUID had not been assigned or synchronized when the county produced its list. In at least two cases (Tarrant, Denton) the state lists the same voter (same name and precinct) under a proper VUID, so these voters cannot be matched by ID between the two sources.

## 7. Other county data quality notes

- **Blank VUIDs** (named voters with no ID): Harris 1,072 mail ballots; Fort Bend 48 (41 in-person, 7 mail); Collin 33 (28 of them provisional); Hidalgo 18 in-person; Bell 18 mail; Dallas 13 mail; Bexar 11 in-person. Tarrant's 320 redacted records also have no VUID (§6.2).
- **No voting date:** Tarrant, Denton and Fort Bend early in-person lists and both Hidalgo lists carry no date for each voter; Harris's official post-election roster lists only "In Person" or "Mail".
- **Name formats:** some counties write names "FIRST MIDDLE LAST" (Bexar's PDF, Denton, Brazoria, Galveston), so a two-word surname cannot be told from a middle name; Denton's list writes some hyphenated surnames as "LAST - LAST". These account for hundreds of apparent name differences that are not real discrepancies (Bexar 628, Denton 447, Brazoria 163).
- **Precinct formats:** Bexar prefixes precincts with "P " or "S " and adds split suffixes ("S 1038.04"); Galveston writes "Precinct 364"; Collin's precincts include spreadsheet rounding artifacts ("34.020000000000003"); Harris uses four-digit zero-padded precincts.
- **File naming:** Galveston's daily file names vary in case and spelling ("Ballots-recevied", "nov-3rd-2025" for a 2026 file, "Oct-13"); Travis's P26 roster ZIP contained a file dated "02.26.2027".
- **Snapshots that drop voters:** Williamson's later snapshot dropped 14 voters listed in its October 10/13 snapshots; Tarrant's mail file dropped 1 voter between October 10 and October 19.

## 8. Recommendations

1. **Download daily** through the end of early voting (October 30) and on Election Day: Harris (early voting), Tarrant, Hidalgo, Denton, Fort Bend; and, because their sites keep only the latest file, Collin, Williamson, Cameron and Dallas. Save same-named files with the download date.
2. **Prefer county files** for counts and voter lists; use the SOS portal for Lubbock, Nueces and Hays (which post nothing else) and as a cross-check. Re-download SOS days after a few days, since the state revises earlier days.
3. **When using SOS county files,** expect a few voters of other counties in each file (§6.1) and check them against neighboring counties before treating them as anomalies.
4. **Refer to the counties:** the voters with both a mail ballot and an in-person vote (Dallas, El Paso), the two cross-county VUIDs (Bexar/Webb, Hidalgo/Cameron), the shared VUIDs (Harris, Brazoria, Bell/Fort Bend, Tarrant/Hidalgo), Bexar's mail ballots received twice, and the same-day duplicate check-ins.
5. **Refer to Tarrant County and the Secretary of State:** the publication by the SOS of voters Tarrant redacts (§6.2), the SOS files' inclusion of other counties' voters (§6.1), and the Dallas turnout-table discrepancy (§3).
6. **Repeat this comparison** after early voting closes and after the canvass, when the state's data should be complete and the official rosters are posted.

## Appendix A. Files

Downloaded files are kept under `Voter_Rosters/TX_<County>_County/`:

- county files are saved as `G26_<county file name>`; files a county posts under the same name every day (Harris, Tarrant) have `_2026-10-20` added; El Paso's undated early voting workbook is saved under its link text (`G26_Early Voting 10-19-2026_...`);
- SOS files are saved as `G26_<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv`, following the portal's own download name.

The files load in Election Explorer (File → Load Voter Roster); the comparisons in this report were produced from the loader's normalized output.

## Appendix B. Caveats

- Early voting had been open for one day; in-person comparisons cover October 19 only.
- County files were downloaded on October 20 at different times and reflect different cut-off times (Travis's mail list ends October 14; Hidalgo's mail list was updated the morning of October 20).
- The SOS turnout table was fetched about 30 minutes before the SOS CSVs.
- A match between two records means the same VUID; name and precinct differences were checked separately. A match by name and precinct without a common VUID (§6.2, §6.11) is an inference, not a confirmed identity.
- This review describes data as published. It does not establish that any voter voted improperly; the records flagged in §6.7-§6.10 have ordinary clerical explanations as well as serious ones, and only the counties can resolve them.
