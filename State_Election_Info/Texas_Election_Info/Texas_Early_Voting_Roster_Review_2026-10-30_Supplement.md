# Texas Early Voting Rosters: Record Locator Supplement

**Supplements:** Texas Early Voting Rosters: County and State Data Review, October 30, 2026\
**Election:** November 3, 2026 General Election (SOS election 53815)\
**Data through:** Thursday, October 29, 2026; county files as posted by 12:30 PM October 30 (Wichita's downloaded by hand that morning), state files as posted on the evening of October 30\
**For:** county election offices and the Secretary of State's Elections Division

This supplement lists the voter records behind the anomalies in sections 6.1 and 6.4-6.11 of the October 30 review, so that county and state staff can find each record in their own files. Section 6.2 (voters whose identities Tarrant County redacts) is not included. It replaces the October 29 supplement: the lists below cover October 14-29, the current versions of each county's files, and the state's files for every day as re-downloaded on the evening of October 30 (the state revised many earlier days, so state line numbers differ from the October 29 supplement).

Each record is identified by its VUID (or, in §6.11, the county's own identifier) and by where it appears: the file name as the county or the Secretary of State posted it, the entry inside a ZIP, and the line, worksheet row or PDF page. Voter names and addresses are not reproduced: the VUID and location identify each record. All identifiers come from rosters published under Texas Election Code §87.121.

The complete list, 22,207 records, is in the companion file **Texas_Early_Voting_Roster_Review_2026-10-30_Supplement_Records.csv**. This document summarizes it and lists the smaller sections in full.

## 1. How to read the locations

- **County files** are named as posted on the county's website. A county that posts a file under the same name every day is shown with the date the copy was downloaded, for example `Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30)`. Travis County's daily workbooks are entries in its ZIP `G26-Voter-Rosters.zip`; Harris, Tarrant and Montgomery files are also ZIP entries. A location is written `file > entry (location)`.
- **State files** are named as the SOS portal's CSV Download names them: `<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv` (Election Information & Turnout Data, early voting date, county Voter Details Report CSV). "..." in the tables below stands for ".2026 NOVEMBER GENERAL ELECTION.EarlyVoting.".
- **Line** numbers count the header as line 1. **Sheet ... row** is the worksheet name (as stored in the file, which may be cut short) and the Excel row number; this includes Jefferson's Excel 97-2003 (.xls) files. **Page** is the PDF page, counting from 1. Where a VUID appears on several lines or pages of one file, all are listed.
- **Dates** are the voting or received date in the county's file, and for state records the date of the state's file (which for mail ballots is when the state recorded the ballot, not when the county received it). Tarrant's, Hidalgo's and Potter's in-person lists are cumulative and carry no voting date; El Paso's, Denton's and Jefferson's lists show the list's date (El Paso's current in-person list is cumulative through October 29, so its records show October 29; the review gives the day each voter first appears in El Paso's daily lists).
- The files were downloaded October 20-30, 2026. Counties replace some files daily (Collin, Williamson, Cameron, Hidalgo, Victoria, Potter, Randall, Tom Green, McLennan, Jefferson's mail list) and the state revises its files, so a later copy of a file may differ in line numbers. Angelina is located in its cumulative October 25 file (October 19-25) and its October 26-29 files. Bexar's October 27 and 28 check-in lists are named `EV_VoterCheckins_10272028.pdf` and `EV_VoterCheckins_10282028.pdf`, and its October 29 lists `Early Vote Ballots - 2026 - 10 - 29 November 3, 2026 General Election.pdf` and `Received Mail Ballots - 2023-10-29 - November 3, General Election.pdf`; their records are dated 2026 here. Travis's ZIP holds two corrections of its October 5 mail list and one of its October 28 mail list; only the latest correction of each is located. Webb's October 28 mail list gives a county certificate number instead of the VUID; its records are not located here (the review matched 47 of its 52 voters to state records by name).

### CSV columns

| Column | Content |
|---|---|
| section, category | The review section (6.1-6.11) and the kind of anomaly |
| county | The county whose file (county or SOS) holds the anomaly |
| vuid_or_id | The VUID (10 digits) or, in 6.11, the county's identifier |
| county_file, county_zip_entry, county_location | Where the record is in the county's own files |
| county_method, county_date, county_precinct | Voting method, date and precinct in the county's file |
| state_file, state_location, state_method, state_date, state_precinct | The same for the SOS county CSV |
| other_county, other_file, other_zip_entry, other_location, other_method, other_date, other_precinct | The second record: another county's roster (6.1, 6.7, 6.8) or the second row in the same county (6.7, 6.9, 6.10) |
| note | Context, e.g. a state mail date after the county's latest list, or an inferred VUID for a 6.11 identifier |

## 2. Records by section and county

| Section | Anomaly | Records by county |
|---|---|---|
| 6.1 | SOS county file lists a voter who is on another county's roster (same first name, that county's precinct) | 2,500: Travis 254, Dallas 243, Denton 214, Bexar 207, Collin 136, Johnson 114, Hays 110, Harris 103, Tarrant 103, Parker 86, Brazos 83, and 1-72 in 39 more SOS county files |
| 6.4 | SOS mail ballot not on the county's own mail list (Tarrant excluded: §6.2) | 3,734: Collin 925, Cameron 357, Hunt 335, Galveston 325, Harris 301, Travis 209, Dallas 194, Bexar 190, Montgomery 165, Bell 150, and 2-119 in 17 more counties |
| 6.5 | Precinct differs between the county's and the state's record of the same VUID | 13,088: Bowie 4,718, Williamson 4,688, Harris 2,619, Tarrant 268, El Paso 210, Galveston 185, Brazos 164, Smith 94, Bexar 70, Montgomery 18, and 1-15 in 12 more counties |
| 6.6 | Surname differs between the county and the state | 833: Collin 122, Bexar 71, Tarrant 71, Denton 68, Williamson 51, Cameron 49, Galveston 41, Brazoria 35, Harris 29, Travis 29, and 3-24 in 24 more counties |
| 6.7 | One VUID, two different people | 68 records, 59 VUIDs (§3) |
| 6.8 | Same voter on two counties' own rosters | 17 (§4) |
| 6.9 | Mail ballot and in-person vote by the same voter | 34 (§5) |
| 6.10 | Same-day duplicate check-in; mail ballot received on two days | 190 + 31 (§6) |
| 6.11 | Identifier that is not a 10-digit state VUID | 1,712: Tarrant 532, Collin 517, Denton 229, Grayson 81, El Paso 78, Guadalupe 69, Hidalgo 66, Smith 40, Webb 34, Jefferson 26, Gregg 19, Bexar 8, Bowie 8, Tom Green 3, Henderson 1, Travis 1 (§7) |

Notes on the larger sections:

- **6.4 timing:** 714 Collin, 247 Cameron, 218 Galveston and 48 Tom Green records are dated after the county's latest mail list (marked in the note column). **6.4 Collin:** besides the 714 dated October 29, 172 are dated October 28, the last date on Collin's list. **6.4 Hunt:** 316 of the 335 are dated October 14-19 by the state. **6.4 Harris:** spread over all days (39 dated October 29, 35 October 25, 33 October 28). **6.4 Travis:** 96 of the 209 are dated October 26 and 56 October 19 by the state. **6.4 Dallas:** 51 are dated October 26, 46 October 25 and 40 October 20. **6.4 Galveston:** besides the 218 dated October 29, 91 are dated October 27, although Galveston's latest list runs through October 28. **6.4 Wichita:** 72 of the 119 are dated October 29 and 41 October 28.
- **6.5 Williamson:** 4,685 of the 4,688 are mail voters in the county's October 29 file (`VoterTurnout_261029_202610292129512765.xlsx`); the county's mail precincts have differed from the state's since its October 22 file, while its October 21 file matched. Three in-person records also differ.
- **6.5 Bowie:** all 4,718 are in the October 20, 21 and 23 Early Vote History reports (1,566, 1,711 and 1,441); the October 22 and 24-29 reports match the state. **6.5 Brazos:** 163 of the 164 are mail voters, 141 of them in `mail-10-19-2026-10-20-2026-accessible.pdf`. **6.5 Bexar:** 65 of the 70 are in `EV_VoterCheckins_10222026.pdf`. **6.5 Harris:** 2,552 of the 2,619 are in-person records. **6.5 Galveston:** 111 of the 185 are in `Checkins_10192026.xlsx`. **6.5 El Paso:** all 210 are in-person records in its cumulative October 29 list.
- **6.6:** in all but 30 records the county and state names share a word, which points to name changes or formatting rather than different people.
- **6.10 Angelina:** 121 of the 154 are in Angelina's cumulative October 25 history file and 11 in its October 26 file (both posted October 27), and 8, 9 and 5 in its October 27, 28 and 29 files; each lists these voters two or three times on the same day with the same precinct.

## 3. One VUID, two different people (§6.7)

| VUID | Case | Record 1 | Record 2 |
|---|---|---|---|
| 3382565201 | Two people under one VUID in Bexar's files | Bexar: voter_check_in_details 10-19-2026.pdf (page 636), in person, 10/19/2026, pct P 3149 | Bexar: EV_VoterCheckins_10232026.pdf (page 76), in person, 10/23/2026, pct S 2019.01 |
| 1019649503 | Two people under one VUID in Bowie's files | Bowie: BBM Received 10.22.2026_1.pdf (page 1), mail, 10/22/2026 | Bowie: BBM Received 10.24.2026.pdf (page 1), mail, 10/24/2026 |
| 1016063262 | Two people under one VUID in Brazoria's files | Brazoria: 2026-10-27-MAIL LIST GENERAL.xlsx (downloaded 2026-10-29) (sheet 10.27.26 MAIL LIST GENERAL row 12; sheet 10.27.26 MAIL LIST GENERAL row 13), mail, 10/27/2026, pct 17 | Brazoria: 2026-10-27-MAIL LIST GENERAL.xlsx (downloaded 2026-10-29) (sheet 10.27.26 MAIL LIST GENERAL row 12; sheet 10.27.26 MAIL LIST GENERAL row 13), mail, 10/27/2026, pct 18 |
| 1220978773 | Two people under one VUID in Dallas's files | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 130341; line 274586), in person, 10/28/2026, pct 3081 | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 130341; line 274586), in person, 10/21/2026, pct 4079 |
| 1033998078 | Two people under one VUID in Galveston's files | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 92; line 213), mail, 10/28/2026, pct Precinct 353 | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 92; line 213), mail, 10/28/2026, pct Precinct 150 |
| 1155831069 | Two people under one VUID in Galveston's files | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 38; line 101), mail, 10/28/2026, pct Precinct 356 | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 38; line 101), mail, 10/28/2026, pct Precinct 218 |
| 2164002074 | Two people under one VUID in Galveston's files | Galveston: Checkins_10272026.xlsx (sheet voter_check_in_details - 2026-1 row 1946; sheet voter_check_in_details - 2026-1 row 2108), in person, 10/27/2026, pct 435 | Galveston: Checkins_10272026.xlsx (sheet voter_check_in_details - 2026-1 row 1946; sheet voter_check_in_details - 2026-1 row 2108), in person, 10/27/2026, pct 206 |
| 2221343697 | Two people under one VUID in Harris's files | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19278), mail, 09/24/2026, pct 0139 | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19278), mail, 10/14/2026, pct 0505 |
| 1015418392 | Two people under one VUID in Hunt's files | Hunt: Vote History 10.27.2026.pdf (page 43), mail, 10/27/2026, pct 431-SCO | Hunt: Vote History 10.28.2026.pdf (page 40), mail, 10/28/2026, pct 325-SGR5-SGSUD |
| 2223366061 | Two counties' rosters, two different people | Bell: 20261021_BBM.pdf (page 9), mail, 10/21/2026, pct 201 | Randall: Mail ballot roster 10.29.26_202610300810337053.pdf (page 34), mail, 10/21/2026 |
| 2223366074 | Two counties' rosters, two different people | Bell: 20261026_BBM.pdf (page 1), mail, 10/26/2026, pct 408 | Wichita: Absentee-10.29.2026.pdf (page 29), mail, 10/29/2026, pct S 309.6-IPCISD-IRR |
| 2223366088 | Two counties' rosters, two different people | Bell: 20261027_BBM.pdf (page 1), mail, 10/27/2026, pct 302 | Randall: Mail ballot roster 10.29.26_202610300810337053.pdf (page 40), mail, 10/27/2026 |
| 2223375713 | Two counties' rosters, two different people | Bell: 20261020_EV.pdf (page 70), in person, 10/20/2026, pct 105 | Fort Bend: EV_1029.xlsx (sheet List of Check-ins 2026-10-30 row 235319), in person, 10/28/2026, pct 4173 |
| 2211542442 | SOS file vs another county's roster | SOS BELL ... 10_22_2026.csv (line 130) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-22-26.csv (line 29341), in person, 10/22/2026, pct 0347 |
| 1075809879 | SOS file vs another county's roster | SOS COLLIN ... 10_23_2026.csv (line 676) | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 95821), in person, 10/23/2026, pct 1059 |
| 2223366026 | SOS file vs another county's roster | SOS CORYELL ... 10_24_2026.csv (line 500) | Bell: 20261024_BBM.pdf (page 4), mail, 10/24/2026, pct 102 |
| 2223366042 | SOS file vs another county's roster | SOS CORYELL ... 10_19_2026.csv (line 1063) | Bell: 20261019_EV.pdf (page 3), in person, 10/19/2026, pct 104 |
| 2223366057 | SOS file vs another county's roster | SOS CORYELL ... 10_26_2026.csv (line 674) | Bell: 20261026_BBM.pdf (page 4), mail, 10/26/2026, pct 201 |
| 2223368590 | SOS file vs another county's roster | SOS DALLAS ... 10_24_2026.csv (line 34342) | Denton: 1126_10-29_Returned_Mail_Roster.xlsx (sheet Sheet1 row 4066), mail, 10/24/2026, pct 3128 |
| 2223368629 | SOS file vs another county's roster | SOS DALLAS ... 10_24_2026.csv (line 18763) | Denton: 1126_10-29_Early-Voting-Roster.xlsx (sheet Sheet1 row 282150), in person, 10/24/2026, pct 103201 |
| 2223381239 | SOS file vs another county's roster | SOS DENTON ... 10_29_2026.csv (line 8053) | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_BBM_ToPost_1126.csv (line 42596), mail, 10/29/2026, pct 0501 |
| 2223375616 | SOS file vs another county's roster | SOS FORT BEND ... 10_19_2026.csv (line 17541) | Bell: 20261019_BBM.pdf (page 30), mail, 10/19/2026, pct 101 |
| 2223375713 | SOS file vs another county's roster | SOS FORT BEND ... 10_20_2026.csv (line 26847) | Bell: 20261020_EV.pdf (page 70), in person, 10/20/2026, pct 105 |
| 1015936692 | SOS file vs another county's roster | SOS HARRIS ... 10_21_2026.csv (line 57297) | El Paso: Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 32299), in person, 10/29/2026, pct 052 |
| 1177988407 | SOS file vs another county's roster | SOS HIDALGO ... 10_27_2026.csv (line 7834) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-27-26.csv (line 75018), in person, 10/27/2026, pct 1120 |
| 2223365544 | SOS file vs another county's roster | SOS HIDALGO ... 10_14_2026.csv (line 9) | Tarrant: absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 15787), mail, 10/14/2026, pct 2636 |
| 2223382143 | SOS file vs another county's roster | SOS HIDALGO ... 10_19_2026.csv (line 16203) | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_BBM_ToPost_1126.csv (line 23889), mail, 10/19/2026, pct 0771 |
| 2223366100 | SOS file vs another county's roster | SOS LUBBOCK ... 10_22_2026.csv (line 3149) | Bell: 20261022_BBM.pdf (page 1), mail, 10/22/2026, pct 311 |
| 2223378210 | SOS file vs another county's roster | SOS MONTGOMERY ... 10_26_2026.csv (line 8528) | McLennan: Website List_202610292045245416.pdf (page 946), , pct 59 |
| 2223378724 | SOS file vs another county's roster | SOS MONTGOMERY ... 10_20_2026.csv (line 11799) | El Paso: Received_Ballots_Voter_List10.30.csv (line 5314), mail, 10/20/2026, pct 029.1 |
| 2223366061 | SOS file vs another county's roster | SOS RANDALL ... 10_21_2026.csv (line 3046) | Bell: 20261021_BBM.pdf (page 9), mail, 10/21/2026, pct 201 |
| 2223366088 | SOS file vs another county's roster | SOS RANDALL ... 10_27_2026.csv (line 582) | Bell: 20261027_BBM.pdf (page 1), mail, 10/27/2026, pct 302 |
| 3382917068 | SOS file vs another county's roster | SOS TARRANT ... 10_26_2026.csv (line 43812) | Denton: 1126_10-29_Early-Voting-Roster.xlsx (sheet Sheet1 row 45572), in person, 10/26/2026, pct 211301 |
| 2196506909 | SOS file vs another county's roster | SOS TOM GREEN ... 10_27_2026.csv (line 1357) | Collin: early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 193241), in person, 10/27/2026, pct 8 |
| 1183615991 | SOS file vs another county's roster | SOS TRAVIS ... 10_28_2026.csv (line 29471) | Williamson: VoterTurnout_261029_202610292129512765.xlsx (sheet Sheet1 row 21572), in person, 10/28/2026, pct 155 |
| 2196434701 | SOS file vs another county's roster | SOS TRAVIS ... 10_29_2026.csv (line 9480) | El Paso: Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 23884), in person, 10/29/2026, pct 154 |
| 2223366035 | SOS file vs another county's roster | SOS VICTORIA ... 10_20_2026.csv (line 1314) | Bell: 20261020_BBM.pdf (page 1), mail, 10/20/2026, pct 317 |
| 2223366074 | SOS file vs another county's roster | SOS WICHITA ... 10_26_2026.csv (line 1036) | Bell: 20261026_BBM.pdf (page 1), mail, 10/26/2026, pct 408 |
| 1031505594 | SOS file vs the same county's roster | SOS BEXAR ... 10_22_2026.csv (line 38629) | Bexar: EV_VoterCheckins_10222026.pdf (page 644), in person, 10/22/2026, pct S 3124.01 |
| 2154123680 | SOS file vs the same county's roster | SOS BEXAR ... 10_24_2026.csv (line 41028) | Bexar: EV_VoterCheckins_10242026.pdf (page 79), in person, 10/24/2026, pct P 4097 |
| 3383579919 | SOS file vs the same county's roster | SOS BEXAR ... 10_22_2026.csv (line 21410) | Bexar: EV_VoterCheckins_10222026.pdf (page 567), in person, 10/22/2026, pct P 3052 |
| 1019649503 | SOS file vs the same county's roster | SOS BOWIE ... 10_24_2026.csv (line 660) | Bowie: BBM Received 10.22.2026_1.pdf (page 1), mail, 10/22/2026 |
| 1017062829 | SOS file vs the same county's roster | SOS BRAZORIA ... 10_20_2026.csv (line 13793) | Brazoria: 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (downloaded 2026-10-29) (sheet BBM ROSTER-9.28-10.16.26 row 1754), mail, 10/02/2026, pct 76 |
| 1145191969 | SOS file vs the same county's roster | SOS BRAZORIA ... 10_26_2026.csv (line 6136) | Brazoria: 2026-10-22-MAIL LIST GENERAL.xlsx (downloaded 2026-10-29) (sheet 10.22.26 MAIL LIST GENERAL row 36), mail, 10/22/2026, pct 12 |
| 1167694139 | SOS file vs the same county's roster | SOS COLLIN ... 10_19_2026.csv (line 15545) | Collin: early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 43047), in person, 10/19/2026, pct 246 |
| 1082025131 | SOS file vs the same county's roster | SOS DALLAS ... 10_29_2026.csv (line 37440) | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 440311), in person, 10/29/2026, pct 3800 |
| 1220978773 | SOS file vs the same county's roster | SOS DALLAS ... 10_21_2026.csv (line 12217) | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 130341; line 274586), in person, 10/28/2026, pct 3081 |
| 2185867363 | SOS file vs the same county's roster | SOS DALLAS ... 10_24_2026.csv (line 10962) | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 429310), in person, 10/24/2026, pct 2608 |
| 3382936850 | SOS file vs the same county's roster | SOS DALLAS ... 10_24_2026.csv (line 12973) | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 242356), in person, 10/24/2026, pct 2048 |
| 2203146614 | SOS file vs the same county's roster | SOS DENTON ... 10_24_2026.csv (line 13661) | Denton: 1126_10-29_Early-Voting-Roster.xlsx (sheet Sheet1 row 97613), in person, 10/24/2026, pct 209701 |
| 1182032143 | SOS file vs the same county's roster | SOS EL PASO ... 10_22_2026.csv (line 13316) | El Paso: Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 58664), in person, 10/29/2026, pct 012 |
| 1033998078 | SOS file vs the same county's roster | SOS GALVESTON ... 10_28_2026.csv (line 7026) | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 92; line 213), mail, 10/28/2026, pct Precinct 353 |
| 1155831069 | SOS file vs the same county's roster | SOS GALVESTON ... 10_28_2026.csv (line 5114) | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 38; line 101), mail, 10/28/2026, pct Precinct 356 |
| 1025420886 | SOS file vs the same county's roster | SOS GRAYSON ... 10_28_2026.csv (line 578) | Grayson: October 28 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1583), in person, 10/28/2026, pct 306 |
| 2000639205 | SOS file vs the same county's roster | SOS GUADALUPE ... 10_27_2026.csv (line 2242) | Guadalupe: Early Voting in Person Roster_10-27-2026.pdf (page 32), in person, 10/27/2026, pct 105 |
| 2141714646 | SOS file vs the same county's roster | SOS GUADALUPE ... 10_27_2026.csv (line 3985) | Guadalupe: Early Voting in Person Roster_10-27-2026.pdf (page 57), in person, 10/27/2026, pct 307 |
| 2209931621 | SOS file vs the same county's roster | SOS GUADALUPE ... 10_27_2026.csv (line 3990) | Guadalupe: Early Voting in Person Roster_10-27-2026.pdf (page 57), in person, 10/27/2026, pct 307 |
| 1143970888 | SOS file vs the same county's roster | SOS HARRIS ... 10_24_2026.csv (line 34739) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 30020), in person, 10/24/2026, pct 0902 |
| 1175275415 | SOS file vs the same county's roster | SOS HARRIS ... 10_24_2026.csv (line 9444) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 67167), in person, 10/24/2026, pct 0006 |
| 2120203917 | SOS file vs the same county's roster | SOS HARRIS ... 10_27_2026.csv (line 12172) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-27-26.csv (line 63314), in person, 10/27/2026, pct 0166 |
| 2170642029 | SOS file vs the same county's roster | SOS HARRIS ... 10_23_2026.csv (line 51096) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-23-26.csv (line 57651), in person, 10/23/2026, pct 0436 |
| 2206845981 | SOS file vs the same county's roster | SOS HARRIS ... 10_22_2026.csv (line 31077) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-22-26.csv (line 75317), in person, 10/22/2026, pct 0672 |
| 1015418392 | SOS file vs the same county's roster | SOS HUNT ... 10_28_2026.csv (line 331) | Hunt: Vote History 10.27.2026.pdf (page 43), mail, 10/27/2026, pct 431-SCO |
| 3383208181 | SOS file vs the same county's roster | SOS MONTGOMERY ... 10_21_2026.csv (line 9373) | Montgomery: GEN110326_10.21.2026.zip > GEN110326_10.21.2026.csv (line 36534; line 36535; line 36536), in person, 10/21/2026, pct 113 |
| 3383218284 | SOS file vs the same county's roster | SOS MONTGOMERY ... 10_27_2026.csv (line 11986) | Montgomery: GEN110326_10.27.2026.zip > GEN110326_10.27.2026.csv (line 43152; line 43153; line 43154; line 43155), in person, 10/27/2026, pct 78 |
| 1045656452 | SOS file vs the same county's roster | SOS TARRANT ... 10_21_2026.csv (line 973) | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-30) > Early_voting_in_person_report.txt (line 440487), in person, pct 4276 |
| 2164441653 | SOS file vs the same county's roster | SOS TARRANT ... 10_27_2026.csv (line 45436) | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-30) > Early_voting_in_person_report.txt (line 7304), in person, pct 1059 |
| 3382951067 | SOS file vs the same county's roster | SOS TRAVIS ... 10_28_2026.csv (line 732) | Travis: G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.28.2026 Early Vote.xlsx (sheet EarlyVote row 920), in person, 10/28/2026, pct 312 |

## 4. Same voter on two counties' own rosters (§6.8)

| VUID | Record 1 | Record 2 |
|---|---|---|
| 1052956069 | Bexar: EV_VoterCheckins_10252026.pdf (page 339), in person, 10/25/2026, pct P 4179 | Hidalgo: EV Roster November 3, 2026 (Cumulative).csv (downloaded 2026-10-30) (line 109737), in person, pct 190 |
| 1053842333 | Bexar: EV_VoterCheckins_10232026.pdf (page 406), in person, 10/23/2026, pct P 2143 | Hidalgo: EV Roster November 3, 2026 (Cumulative).csv (downloaded 2026-10-30) (line 73903), in person, pct 151 |
| 1072354984 | Bexar: voter_check_in_details 10-19-2026.pdf (page 806), in person, 10/19/2026, pct S 1045.01 | Cameron: 01_Cameron_County10-29-2026.xlsx (sheet CC row 45181), in person, 10/25/2026, pct 21 |
| 1095835184 | Bexar: EV_VoterCheckins_10242026.pdf (page 509), in person, 10/24/2026, pct P 4031 | El Paso: Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 101954), in person, 10/29/2026, pct 164 |
| 1173266072 | Bexar: voter_check_in_details 10-19-2026.pdf (page 579), in person, 10/19/2026, pct S 1046.05 | Webb: Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 29), in person, 10/19/2026, pct 266 |
| 2153679438 | Bexar: EV_VoterCheckins_10202026.pdf (page 276), in person, 10/20/2026, pct P 3066 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-30) > Early_voting_in_person_report.txt (line 25146), in person, pct 1170 |
| 3383515497 | Bexar: EV_VoterCheckins_10212026.pdf (page 168), in person, 10/21/2026, pct S 1156.01 | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 301006), in person, 10/29/2026, pct 3616 |
| 2154157208 | Brazoria: 2026-10-29 IN PERSON.xlsx (sheet 2026-10-29 ROSTER row 2239), in person, 10/29/2026, pct 27 | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-22-26.csv (line 68725), in person, 10/22/2026, pct 0699 |
| 1138259335 | Cameron: MAIL-GN26-Cameron-County-CC-2026-10-27.xlsx (sheet Sheet1 row 1689), mail, 10/13/2026, pct 44 | Hidalgo: ABBM LIST November 3, 2026 (Cumulative)_202610300632579549.xlsx (sheet Received Ballots Voter List_Oct row 3898), mail, pct 63.01 |
| 1077658162 | Collin: early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 293311), in person, 10/23/2026, pct 90 | Travis: G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.27.2026 Early Vote.xlsx (sheet EarlyVote row 30170), in person, 10/27/2026, pct 203 |
| 1050774882 | Dallas: In-Person-Early-Voting-Report_10.29.26.csv (line 72474), in person, 10/23/2026, pct 1146 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-30) > Early_voting_in_person_report.txt (line 822), in person, pct 1010 |
| 2153111005 | El Paso: Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 85556), in person, 10/29/2026, pct 180 | Randall: Early Voting Roster 10.29.26_202610300810481362.pdf (page 263), in person, 10/22/2026, pct 332 |
| 3382857971 | El Paso: Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 75858), in person, 10/29/2026, pct 016 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-30) > Early_voting_in_person_report.txt (line 10790), in person, pct 1083 |
| 1102081832 | Guadalupe: Early Voting in Person Roster_10-21-2026.pdf (page 82), in person, 10/21/2026, pct 406 | Smith: 10.19.26 In Person Voter List.pdf (page 67), in person, 10/19/2026, pct 242 |
| 2205290498 | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 54225), in person, 10/24/2026, pct 0451 | Williamson: VoterTurnout_261029_202610292129512765.xlsx (sheet Sheet1 row 161524), in person, 10/29/2026, pct 210 |
| 2208300194 | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_BBM_ToPost_1126.csv (line 475), mail, 09/30/2026, pct 0516 | Travis: G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.23.2026 Early Vote.xlsx (sheet EarlyVote row 9427), in person, 10/23/2026, pct 264 |
| 2215994140 | Smith: 10.22.26 In Person Voter List.pdf (page 73), in person, 10/22/2026, pct 245 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-30) > Early_voting_in_person_report.txt (line 30505), in person, pct 1224 |

## 5. Mail ballot and in-person vote by the same voter (§6.9)

| County | VUID | Mail ballot | In-person vote |
|---|---|---|---|
| Dallas | 1075428602 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 32), 10/24/2026, pct 4079 | In-Person-Early-Voting-Report_10.29.26.csv (line 248122), 10/19/2026 |
| Dallas | 1079098339 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 4388), 10/24/2026, pct 1081 | In-Person-Early-Voting-Report_10.29.26.csv (line 112492), 10/29/2026 |
| Dallas | 1081471506 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 8022), 10/24/2026, pct 1739 | In-Person-Early-Voting-Report_10.29.26.csv (line 326330), 10/28/2026 |
| Dallas | 1081736255 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 10940), 10/09/2026, pct 1748 | In-Person-Early-Voting-Report_10.29.26.csv (line 338768), 10/27/2026 |
| Dallas | 1082858250 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 1832), 10/24/2026, pct 4056 | In-Person-Early-Voting-Report_10.29.26.csv (line 67344), 10/27/2026 |
| Dallas | 1083306122 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 10390), 10/09/2026, pct 2027 | In-Person-Early-Voting-Report_10.29.26.csv (line 175625), 10/22/2026 |
| Dallas | 1083306133 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 10391), 10/09/2026, pct 2027 | In-Person-Early-Voting-Report_10.29.26.csv (line 223209), 10/20/2026 |
| Dallas | 1084339442 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 11531), 10/24/2026, pct 3312 | In-Person-Early-Voting-Report_10.29.26.csv (line 457279), 10/27/2026 |
| Dallas | 1088439372 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 3547), 10/24/2026, pct 2079 | In-Person-Early-Voting-Report_10.29.26.csv (line 130474), 10/28/2026 |
| Dallas | 1091625391 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 19266), 10/24/2026, pct 2020 | In-Person-Early-Voting-Report_10.29.26.csv (line 257354), 10/26/2026 |
| Dallas | 1143777206 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 4517), 10/09/2026, pct 1147 | In-Person-Early-Voting-Report_10.29.26.csv (line 168888), 10/19/2026 |
| Dallas | 1183289224 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 6148), 10/25/2026, pct 2027 | In-Person-Early-Voting-Report_10.29.26.csv (line 131555), 10/28/2026 |
| Dallas | 2155626901 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 17035), 10/25/2026, pct 2410 | In-Person-Early-Voting-Report_10.29.26.csv (line 10644), 10/28/2026 |
| Dallas | 2156785478 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 4574), 10/24/2026, pct 1310 | In-Person-Early-Voting-Report_10.29.26.csv (line 136940), 10/28/2026 |
| Dallas | 2168668700 | Mail-Ballot-Returns-Report_10.29.26.xlsx (sheet Early Voter Roster row 4912), 10/24/2026, pct 4511 | In-Person-Early-Voting-Report_10.29.26.csv (line 378018), 10/29/2026 |
| El Paso | 1095020454 | Received_Ballots_Voter_List10.30.csv (line 4887), 10/20/2026, pct 024 | Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 133791), 10/29/2026 |
| El Paso | 1095047541 | Received_Ballots_Voter_List10.30.csv (line 651), 10/06/2026, pct 150 | Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 128256), 10/29/2026 |
| El Paso | 1096333268 | Received_Ballots_Voter_List10.30.csv (line 5097), 10/20/2026, pct 160 | Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 57202), 10/29/2026 |
| El Paso | 1096656403 | Received_Ballots_Voter_List10.30.csv (line 3529), 10/13/2026, pct 055 | Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 132565), 10/29/2026 |
| El Paso | 1097760377 | Received_Ballots_Voter_List10.30.csv (line 3578), 10/13/2026, pct 115 | Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 106865), 10/29/2026 |
| El Paso | 1098032793 | Received_Ballots_Voter_List10.30.csv (line 3590), 10/13/2026, pct 232 | Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 49552), 10/29/2026 |
| El Paso | 2152866163 | Received_Ballots_Voter_List10.30.csv (line 5356), 10/20/2026, pct 133 | Early Voting 10-29-2026_Voter_List_(Ballot_Address_Day11.xlsx (sheet Sheet1 row 58758), 10/29/2026 |
| Grayson | 1025213073 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1 row 283), 10/05/2026, pct 303.06 | October 27 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 408), 10/27/2026 |
| Grayson | 1025394327 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1 row 65), 10/02/2026, pct 409.01 | October 20 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1491), 10/20/2026 |
| Grayson | 2025562322 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1 row 870), 10/14/2026, pct 404.08 | October 29 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 954), 10/29/2026 |
| Jefferson | 1121803988 | JeffersonCountyMailBallotsReceived01012026to10292026.xls (sheet JeffersonCountyMailBallotsRecei row 695), 10/29/2026 | JeffersonCountyInPersonVoting10202026.xls (sheet JeffersonCountyInPersonVoting10 row 2142), 10/20/2026 |
| Jefferson | 1123862974 | JeffersonCountyMailBallotsReceived01012026to10292026.xls (sheet JeffersonCountyMailBallotsRecei row 2047), 10/29/2026 | JeffersonCountyInPersonVoting10292026.xls (sheet JeffersonCountyInPersonVoting10 row 3954), 10/29/2026 |
| Jefferson | 2209104885 | JeffersonCountyMailBallotsReceived01012026to10292026.xls (sheet JeffersonCountyMailBallotsRecei row 2150), 10/29/2026 | JeffersonCountyInPersonVoting10292026.xls (sheet JeffersonCountyInPersonVoting10 row 4120), 10/29/2026 |
| Tarrant | 1044915588 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 13773), 10/21/2026, pct 2034 | Early_voting_in_person_report.zip (downloaded 2026-10-30) > Early_voting_in_person_report.txt (line 110813) |
| Tarrant | 1050956866 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 6193), 10/22/2026, pct 1277 | Early_voting_in_person_report.zip (downloaded 2026-10-30) > Early_voting_in_person_report.txt (line 38018) |
| Tom Green | 1129510084 | 102826BallotsByMail.pdf (page 26), 09/30/2026 | 102826roster.pdf (page 344), 10/27/2026 |
| Tom Green | 2207275739 | 102826BallotsByMail.pdf (page 29), 10/05/2026 | 102826roster.pdf (page 386), 10/27/2026 |
| Travis | 1133310623 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.07.2026 Ballot by Mail.xlsx (sheet 10.07.2026 Ballot by Mail row 234), 10/07/2026, pct 270 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.21.2026 Early Vote.xlsx (sheet EarlyVote row 8017), 10/21/2026 |
| Travis | 3383337140 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.29.2026 Ballot by Mail.xlsx (sheet 10.29.2026 Ballot by Mail row 647), 10/29/2026, pct 193 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.28.2026 Early Vote.xlsx (sheet EarlyVote row 31360), 10/28/2026 |

## 6. Duplicates within county files (§6.10)

**Checked in twice on the same day**

| County | VUID | File and location | Date |
|---|---|---|---|
| Angelina | 1001827252 | early_voting_history_102526.pdf (page 1) | 10/19/2026 |
| Angelina | 1014266421 | early_voting_history_102526.pdf (page 6) | 10/19/2026 |
| Angelina | 1031582541 | early_voting_history_102526.pdf (page 14) | 10/19/2026 |
| Angelina | 1031615399 | early_voting_history_102526.pdf (page 21) | 10/19/2026 |
| Angelina | 1031625726 | early_voting_history_102526.pdf (page 24) | 10/19/2026 |
| Angelina | 1031642852 | early_voting_history_102526.pdf (page 29) | 10/19/2026 |
| Angelina | 1031669879 | early_voting_history_102526.pdf (page 37) | 10/19/2026 |
| Angelina | 1031677461 | early_voting_history_102526.pdf (page 39) | 10/19/2026 |
| Angelina | 1031709424 | early_voting_history_102526.pdf (page 49) | 10/19/2026 |
| Angelina | 1031717820 | early_voting_history_102526.pdf (page 52) | 10/19/2026 |
| Angelina | 1031756238 | early_voting_history_102526.pdf (page 65) | 10/19/2026 |
| Angelina | 1031760340 | early_voting_history_102526.pdf (page 66) | 10/19/2026 |
| Angelina | 1031784061 | early_voting_history_102526.pdf (page 74) | 10/19/2026 |
| Angelina | 1031800027 | early_voting_history_102526.pdf (page 77) | 10/19/2026 |
| Angelina | 1031843228 | early_voting_history_102526.pdf (page 87) | 10/19/2026 |
| Angelina | 1031843872 | early_voting_history_102526.pdf (page 87) | 10/19/2026 |
| Angelina | 1031882391 | early_voting_history_102526.pdf (page 95) | 10/19/2026 |
| Angelina | 1031894555 | early_voting_history_102526.pdf (page 97) | 10/19/2026 |
| Angelina | 1031981279 | early_voting_history_102526.pdf (page 114) | 10/19/2026 |
| Angelina | 1032008030 | early_voting_history_102526.pdf (page 119) | 10/19/2026 |
| Angelina | 1032050466 | early_voting_history_102526.pdf (page 126) | 10/19/2026 |
| Angelina | 1032058529 | early_voting_history_102526.pdf (page 127) | 10/19/2026 |
| Angelina | 1032062204 | early_voting_history_102526.pdf (page 128) | 10/19/2026 |
| Angelina | 1032098057 | early_voting_history_102526.pdf (page 135) | 10/19/2026 |
| Angelina | 1032114396 | early_voting_history_102526.pdf (page 138) | 10/19/2026 |
| Angelina | 1032124956 | early_voting_history_102526.pdf (page 140) | 10/19/2026 |
| Angelina | 1032128764 | early_voting_history_102526.pdf (page 141) | 10/19/2026 |
| Angelina | 1036104239 | early_voting_history_102526.pdf (page 147) | 10/19/2026 |
| Angelina | 1039798256 | early_voting_history_102526.pdf (page 149) | 10/19/2026 |
| Angelina | 1078620377 | early_voting_history_102526.pdf (page 157) | 10/19/2026 |
| Angelina | 1141815103 | early_voting_history_102526.pdf (page 168) | 10/19/2026 |
| Angelina | 1157671983 | early_voting_history_102526.pdf (page 179) | 10/19/2026 |
| Angelina | 1159148446 | early_voting_history_102526.pdf (page 180) | 10/19/2026 |
| Angelina | 1173268141 | early_voting_history_102526.pdf (page 188) | 10/19/2026 |
| Angelina | 1184887103 | early_voting_history_102526.pdf (page 194) | 10/19/2026 |
| Angelina | 1191129171 | early_voting_history_102526.pdf (page 198) | 10/19/2026 |
| Angelina | 1196321815 | early_voting_history_102526.pdf (page 200) | 10/19/2026 |
| Angelina | 1210445469 | early_voting_history_102526.pdf (page 206) | 10/19/2026 |
| Angelina | 1215586977 | early_voting_history_102526.pdf (page 208) | 10/19/2026 |
| Angelina | 2131094590 | early_voting_history_102526.pdf (page 219) | 10/19/2026 |
| Angelina | 2133877915 | early_voting_history_102526.pdf (page 221) | 10/19/2026 |
| Angelina | 2137420322 | early_voting_history_102526.pdf (page 223) | 10/19/2026 |
| Angelina | 2141488977 | early_voting_history_102526.pdf (page 225) | 10/19/2026 |
| Angelina | 2142735856 | early_voting_history_102526.pdf (page 226) | 10/19/2026 |
| Angelina | 2167429195 | early_voting_history_102526.pdf (page 239) | 10/19/2026 |
| Angelina | 2188989069 | early_voting_history_102526.pdf (page 249) | 10/19/2026 |
| Angelina | 2207499974 | early_voting_history_102526.pdf (page 259) | 10/19/2026 |
| Angelina | 3383475315 | early_voting_history_102526.pdf (page 274) | 10/19/2026 |
| Angelina | 1009673042 | early_voting_history_102526.pdf (page 4) | 10/20/2026 |
| Angelina | 1020067116 | early_voting_history_102526.pdf (page 8) | 10/20/2026 |
| Angelina | 1031607425 | early_voting_history_102526.pdf (page 18) | 10/20/2026 |
| Angelina | 1031644983 | early_voting_history_102526.pdf (page 30) | 10/20/2026 |
| Angelina | 1031650512 | early_voting_history_102526.pdf (page 31) | 10/20/2026 |
| Angelina | 1031672113 | early_voting_history_102526.pdf (page 38) | 10/20/2026 |
| Angelina | 1031697699 | early_voting_history_102526.pdf (page 46) | 10/20/2026 |
| Angelina | 1031717463 | early_voting_history_102526.pdf (page 52) | 10/20/2026 |
| Angelina | 1031729355 | early_voting_history_102526.pdf (page 55) | 10/20/2026 |
| Angelina | 1031752290 | early_voting_history_102526.pdf (page 64) | 10/20/2026 |
| Angelina | 1031754150 | early_voting_history_102526.pdf (page 64) | 10/20/2026 |
| Angelina | 1031761005 | early_voting_history_102526.pdf (page 66) | 10/20/2026 |
| Angelina | 1031797807 | early_voting_history_102526.pdf (page 77) | 10/20/2026 |
| Angelina | 1031897683 | early_voting_history_102526.pdf (page 98) | 10/20/2026 |
| Angelina | 1031940291 | early_voting_history_102526.pdf (page 107) | 10/20/2026 |
| Angelina | 1075972482 | early_voting_history_102526.pdf (page 156; page 157) | 10/20/2026 |
| Angelina | 1159299928 | early_voting_history_102526.pdf (page 180; page 181) | 10/20/2026 |
| Angelina | 1187875267 | early_voting_history_102526.pdf (page 196) | 10/20/2026 |
| Angelina | 1189430529 | early_voting_history_102526.pdf (page 196) | 10/20/2026 |
| Angelina | 2153548860 | early_voting_history_102526.pdf (page 231; page 232) | 10/20/2026 |
| Angelina | 2154049485 | early_voting_history_102526.pdf (page 232) | 10/20/2026 |
| Angelina | 2155925237 | early_voting_history_102526.pdf (page 233) | 10/20/2026 |
| Angelina | 2161923678 | early_voting_history_102526.pdf (page 236) | 10/20/2026 |
| Angelina | 2167486062 | early_voting_history_102526.pdf (page 239) | 10/20/2026 |
| Angelina | 1014318515 | early_voting_history_102526.pdf (page 6) | 10/21/2026 |
| Angelina | 1031683820 | early_voting_history_102526.pdf (page 42) | 10/21/2026 |
| Angelina | 1031684904 | early_voting_history_102526.pdf (page 42) | 10/21/2026 |
| Angelina | 1031699841 | early_voting_history_102526.pdf (page 46) | 10/21/2026 |
| Angelina | 1031774345 | early_voting_history_102526.pdf (page 71) | 10/21/2026 |
| Angelina | 1031805452 | early_voting_history_102526.pdf (page 78) | 10/21/2026 |
| Angelina | 1031807967 | early_voting_history_102526.pdf (page 79) | 10/21/2026 |
| Angelina | 1031890111 | early_voting_history_102526.pdf (page 96) | 10/21/2026 |
| Angelina | 1031990067 | early_voting_history_102526.pdf (page 116) | 10/21/2026 |
| Angelina | 1032003090 | early_voting_history_102526.pdf (page 118) | 10/21/2026 |
| Angelina | 1057298289 | early_voting_history_102526.pdf (page 153) | 10/21/2026 |
| Angelina | 1162692801 | early_voting_history_102526.pdf (page 182) | 10/21/2026 |
| Angelina | 1181456447 | early_voting_history_102526.pdf (page 192) | 10/21/2026 |
| Angelina | 1199592145 | early_voting_history_102526.pdf (page 201; page 202) | 10/21/2026 |
| Angelina | 2130345689 | early_voting_history_102526.pdf (page 219) | 10/21/2026 |
| Angelina | 2136175596 | early_voting_history_102526.pdf (page 222) | 10/21/2026 |
| Angelina | 2144909187 | early_voting_history_102526.pdf (page 227) | 10/21/2026 |
| Angelina | 2152918241 | early_voting_history_102526.pdf (page 231) | 10/21/2026 |
| Angelina | 1028946252 | early_voting_history_102526.pdf (page 12) | 10/22/2026 |
| Angelina | 1030394209 | early_voting_history_102526.pdf (page 13) | 10/22/2026 |
| Angelina | 1031644690 | early_voting_history_102526.pdf (page 30) | 10/22/2026 |
| Angelina | 1031657271 | early_voting_history_102526.pdf (page 33) | 10/22/2026 |
| Angelina | 1031779343 | early_voting_history_102526.pdf (page 73) | 10/22/2026 |
| Angelina | 1031850262 | early_voting_history_102526.pdf (page 88) | 10/22/2026 |
| Angelina | 1032032670 | early_voting_history_102526.pdf (page 123) | 10/22/2026 |
| Angelina | 1035160132 | early_voting_history_102526.pdf (page 147) | 10/22/2026 |
| Angelina | 1119116110 | early_voting_history_102526.pdf (page 164) | 10/22/2026 |
| Angelina | 1181318608 | early_voting_history_102526.pdf (page 192) | 10/22/2026 |
| Angelina | 1190118130 | early_voting_history_102526.pdf (page 197) | 10/22/2026 |
| Angelina | 2128176079 | early_voting_history_102526.pdf (page 218; page 219) | 10/22/2026 |
| Angelina | 3382535413 | early_voting_history_102526.pdf (page 268) | 10/22/2026 |
| Angelina | 1014014964 | early_voting_history_102526.pdf (page 5) | 10/23/2026 |
| Angelina | 1031597417 | early_voting_history_102526.pdf (page 16) | 10/23/2026 |
| Angelina | 1031646779 | early_voting_history_102526.pdf (page 30) | 10/23/2026 |
| Angelina | 1031694438 | early_voting_history_102526.pdf (page 45) | 10/23/2026 |
| Angelina | 1031918353 | early_voting_history_102526.pdf (page 102) | 10/23/2026 |
| Angelina | 1032077694 | early_voting_history_102526.pdf (page 131) | 10/23/2026 |
| Angelina | 1036955539 | early_voting_history_102526.pdf (page 148) | 10/23/2026 |
| Angelina | 1145197898 | early_voting_history_102526.pdf (page 170) | 10/23/2026 |
| Angelina | 1159559929 | early_voting_history_102526.pdf (page 181) | 10/23/2026 |
| Angelina | 1188679596 | early_voting_history_102526.pdf (page 196) | 10/23/2026 |
| Angelina | 2141234291 | early_voting_history_102526.pdf (page 225) | 10/23/2026 |
| Angelina | 2148248884 | early_voting_history_102526.pdf (page 228) | 10/23/2026 |
| Angelina | 1038993640 | early_voting_history_102526.pdf (page 149) | 10/24/2026 |
| Angelina | 2219988766 | early_voting_history_102526.pdf (page 265) | 10/24/2026 |
| Angelina | 3382671237 | early_voting_history_102526.pdf (page 268) | 10/24/2026 |
| Angelina | 1032014852 | early_voting_history_102526.pdf (page 120) | 10/25/2026 |
| Angelina | 2165418075 | early_voting_history_102526.pdf (page 238) | 10/25/2026 |
| Angelina | 2212285012 | early_voting_history_102526.pdf (page 261) | 10/25/2026 |
| Angelina | 1031707285 | early_voting_history_102626.pdf (page 5) | 10/26/2026 |
| Angelina | 1031780829 | early_voting_history_102626.pdf (page 8) | 10/26/2026 |
| Angelina | 1031786352 | early_voting_history_102626.pdf (page 8) | 10/26/2026 |
| Angelina | 1031823381 | early_voting_history_102626.pdf (page 9) | 10/26/2026 |
| Angelina | 1031891887 | early_voting_history_102626.pdf (page 10) | 10/26/2026 |
| Angelina | 1031895345 | early_voting_history_102626.pdf (page 11) | 10/26/2026 |
| Angelina | 1190210863 | early_voting_history_102626.pdf (page 22) | 10/26/2026 |
| Angelina | 2143909991 | early_voting_history_102626.pdf (page 27) | 10/26/2026 |
| Angelina | 2166358487 | early_voting_history_102626.pdf (page 28) | 10/26/2026 |
| Angelina | 2186849786 | early_voting_history_102626.pdf (page 30) | 10/26/2026 |
| Angelina | 2221892960 | early_voting_history_102626.pdf (page 32) | 10/26/2026 |
| Angelina | 1028421736 | early_voting_history_102726.pdf (page 2) | 10/27/2026 |
| Angelina | 1031706377 | early_voting_history_102726.pdf (page 6) | 10/27/2026 |
| Angelina | 1031796469 | early_voting_history_102726.pdf (page 9) | 10/27/2026 |
| Angelina | 1031935401 | early_voting_history_102726.pdf (page 12) | 10/27/2026 |
| Angelina | 1032013132 | early_voting_history_102726.pdf (page 14) | 10/27/2026 |
| Angelina | 1165144220 | early_voting_history_102726.pdf (page 21) | 10/27/2026 |
| Angelina | 1168149506 | early_voting_history_102726.pdf (page 21) | 10/27/2026 |
| Angelina | 2133900120 | early_voting_history_102726.pdf (page 26) | 10/27/2026 |
| Angelina | 1031734773 | early_voting_history_102826.pdf (page 5) | 10/28/2026 |
| Angelina | 1031871072 | early_voting_history_102826.pdf (page 8) | 10/28/2026 |
| Angelina | 1032006885 | early_voting_history_102826.pdf (page 11) | 10/28/2026 |
| Angelina | 1032136426 | early_voting_history_102826.pdf (page 14) | 10/28/2026 |
| Angelina | 1189123982 | early_voting_history_102826.pdf (page 19) | 10/28/2026 |
| Angelina | 1202828841 | early_voting_history_102826.pdf (page 20) | 10/28/2026 |
| Angelina | 1209002226 | early_voting_history_102826.pdf (page 20) | 10/28/2026 |
| Angelina | 2120405573 | early_voting_history_102826.pdf (page 21) | 10/28/2026 |
| Angelina | 2166405716 | early_voting_history_102826.pdf (page 24) | 10/28/2026 |
| Angelina | 1031773316 | early_voting_history_102926.pdf (page 7) | 10/29/2026 |
| Angelina | 1031852809 | early_voting_history_102926.pdf (page 9) | 10/29/2026 |
| Angelina | 1182078022 | early_voting_history_102926.pdf (page 21) | 10/29/2026 |
| Angelina | 2170915501 | early_voting_history_102926.pdf (page 27; page 28) | 10/29/2026 |
| Angelina | 2193691189 | early_voting_history_102926.pdf (page 29) | 10/29/2026 |
| Bexar | 1140208390 | EV_VoterCheckins_10202026.pdf (page 718) | 10/20/2026 |
| Bexar | 2222687508 | EV_VoterCheckins_10202026.pdf (page 152) | 10/20/2026 |
| Bexar | 1069614030 | EV_VoterCheckins_10212026.pdf (page 721) | 10/21/2026 |
| Bexar | 1150529579 | EV_VoterCheckins_10212026.pdf (page 25) | 10/21/2026 |
| Bexar | 1168562626 | EV_VoterCheckins_10212026.pdf (page 71) | 10/21/2026 |
| Bexar | 2120288965 | EV_VoterCheckins_10212026.pdf (page 255) | 10/21/2026 |
| Brazoria | 1016426864 | 2026-10-29 IN PERSON.xlsx (sheet 2026-10-29 ROSTER row 5588; sheet 2026-10-29 ROSTER row 5589) | 10/29/2026 |
| Collin | 1076603030 | early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 85727; sheet Detailed Check-in Report - Orde row 85728) | 10/19/2026 |
| Collin | 1201013964 | early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 115570; sheet Detailed Check-in Report - Orde row 115571) | 10/19/2026 |
| Collin | 1175567020 | early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 66344; sheet Detailed Check-in Report - Orde row 66345) | 10/20/2026 |
| Collin | 2190293340 | early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 111291; sheet Detailed Check-in Report - Orde row 111292) | 10/27/2026 |
| Dallas | 3383295764 | In-Person-Early-Voting-Report_10.29.26.csv (line 330295; line 346300) | 10/19/2026 |
| Dallas | 2170886201 | In-Person-Early-Voting-Report_10.29.26.csv (line 126148; line 193893) | 10/22/2026 |
| Dallas | 2170970526 | In-Person-Early-Voting-Report_10.29.26.csv (line 333737; line 360200) | 10/23/2026 |
| Galveston | 1185946285 | Checkins_10192026.xlsx (sheet Checkins_10192026 row 13286; sheet Checkins_10192026 row 13288) | 10/19/2026 |
| Galveston | 2164002074 | Checkins_10272026.xlsx (sheet voter_check_in_details - 2026-1 row 1946; sheet voter_check_in_details - 2026-1 row 2108) | 10/27/2026 |
| Grayson | 1024897712 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3415; sheet Detailed Check-in Report - Orde row 3422) | 10/19/2026 |
| Grayson | 1024922211 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3575; sheet Detailed Check-in Report - Orde row 3591) | 10/19/2026 |
| Grayson | 1042130958 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3480; sheet Detailed Check-in Report - Orde row 3491) | 10/19/2026 |
| Grayson | 1076365904 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3327; sheet Detailed Check-in Report - Orde row 3336) | 10/19/2026 |
| Grayson | 1090251506 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3310; sheet Detailed Check-in Report - Orde row 3316) | 10/19/2026 |
| Grayson | 1025261772 | October 20 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 4517; sheet Detailed Check-in Report - Orde row 4522) | 10/20/2026 |
| Grayson | 1025207956 | October 22 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 656; sheet Detailed Check-in Report - Orde row 667) | 10/22/2026 |
| Grayson | 1089592991 | October 22 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 483; sheet Detailed Check-in Report - Orde row 487) | 10/22/2026 |
| Guadalupe | 1101950568 | Early Voting in Person Roster_10-19-2026.pdf (page 58) | 10/19/2026 |
| Guadalupe | 1221702819 | Early Voting in Person Roster_10-19-2026.pdf (page 89) | 10/19/2026 |
| Guadalupe | 1101632395 | Early Voting in Person Roster_10-20-2026.pdf (page 50) | 10/20/2026 |
| Guadalupe | 2197563557 | Early Voting in Person Roster_10-22-2026.pdf (page 26) | 10/22/2026 |
| Jefferson | 2183895285 | JeffersonCountyInPersonVoting10192026.xls (sheet ALL row 2671; sheet ALL row 2672) | 10/19/2026 |
| Jefferson | 1123342941 | JeffersonCountyInPersonVoting10212026.xls (sheet JeffersonCountyInPersonVoting10 row 1189; sheet JeffersonCountyInPersonVoting10 row 1190) | 10/21/2026 |
| Jefferson | 1185749681 | JeffersonCountyInPersonVoting10232026.xls (sheet JeffersonCountyInPersonVoting10 row 3386; sheet JeffersonCountyInPersonVoting10 row 3387) | 10/23/2026 |
| Jefferson | 1123165994 | JeffersonCountyInPersonVoting10292026.xls (sheet JeffersonCountyInPersonVoting10 row 2086; sheet JeffersonCountyInPersonVoting10 row 2087) | 10/29/2026 |
| Tom Green | 1052505413 | 102826roster.pdf (page 303) | 10/20/2026 |
| Tom Green | 1048893092 | 102826roster.pdf (page 313) | 10/27/2026 |
| Travis | 1130790938 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.19.2026 Early Vote.xlsx (sheet EarlyVote row 27271; sheet EarlyVote row 27273) | 10/19/2026 |
| Webb | 2223144396 | Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 49) | 10/19/2026 |

**Mail ballot listed as received on two days**

| County | VUID | First listing | Second listing |
|---|---|---|---|
| Bexar | 1064449288 | ABBM_Recvd_10052026.pdf (page 4), 10/05/2026 | ABBM_Recvd_10102026_202610152202246278.pdf (page 6), 10/10/2026 |
| Bexar | 1065766865 | ABBM_Recvd_10072026.pdf (page 38), 10/07/2026 | ABBM_Recvd_10192026.pdf (page 27), 10/19/2026 |
| Bexar | 2153842163 | ABBM_Recvd_10082026.pdf (page 11), 10/08/2026 | ABBM_Recvd_10152026.pdf (page 12), 10/15/2026 |
| Bexar | 1067081664 | ABBM_Recvd_10102026_202610152202246278.pdf (page 22), 10/10/2026 | ABBM_Recvd_10132026.pdf (page 14), 10/13/2026 |
| Bexar | 1071067645 | ABBM_Recvd_10102026_202610152202246278.pdf (page 34), 10/10/2026 | ABBM_Recvd_10132026.pdf (page 21), 10/13/2026 |
| Bexar | 1147096356 | ABBM_Recvd_10102026_202610152202246278.pdf (page 9), 10/10/2026 | ABBM_Recvd_10132026.pdf (page 6), 10/13/2026 |
| Bexar | 1065220238 | ABBM_Recvd_10152026.pdf (page 2), 10/15/2026 | ABBM_Recvd_10162026.pdf (page 1), 10/16/2026 |
| Bexar | 1069344089 | ABBM_Recvd_10152026.pdf (page 1), 10/15/2026 | ABBM_Recvd_10162026.pdf (page 1), 10/16/2026 |
| Bexar | 1070856222 | ABBM_Recvd_10152026.pdf (page 1), 10/15/2026 | ABBM_Recvd_10162026.pdf (page 1), 10/16/2026 |
| Bexar | 1067011360 | ABBM_Recvd_10192026.pdf (page 13), 10/19/2026 | ABBM_Recvd_10212026.pdf (page 9), 10/21/2026 |
| Bexar | 1067521180 | ABBM_Recvd_10242026.pdf (page 9), 10/24/2026 | ABBM_Recvd_10262026.pdf (page 10), 10/26/2026 |
| Bexar | 1065541222 | ABBM_Recvd_10282026.pdf (page 13), 10/28/2026 | Received Mail Ballots - 2023-10-29 - November 3, General Election.pdf (page 28), 10/29/2026 |
| Bexar | 1070002959 | ABBM_Recvd_10282026.pdf (page 10), 10/28/2026 | Received Mail Ballots - 2023-10-29 - November 3, General Election.pdf (page 19), 10/29/2026 |
| Bexar | 1188735488 | ABBM_Recvd_10282026.pdf (page 13), 10/28/2026 | Received Mail Ballots - 2023-10-29 - November 3, General Election.pdf (page 27), 10/29/2026 |
| Bexar | 2216759498 | ABBM_Recvd_10282026.pdf (page 6), 10/28/2026 | Received Mail Ballots - 2023-10-29 - November 3, General Election.pdf (page 12), 10/29/2026 |
| Bowie | 1019649503 | BBM Received 10.22.2026_1.pdf (page 1), 10/22/2026 | BBM Received 10.24.2026.pdf (page 1), 10/24/2026 |
| Brazos | 2180077579 | mail-10-09-2026-accessible.pdf (page 1), 10/09/2026 | mail-10-13-2026-accessible.pdf (page 4), 10/13/2026 |
| Galveston | 1032377676 | Nov-3-2026-Ballots-Received-Oct-13.csv (line 450), 10/13/2026 | Nov-3rd-2026-Ballots-Received-10.14.26.csv (line 28), 10/14/2026 |
| Gregg | 1034151785 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 17), 10/16/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 2), 10/21/2026 |
| Gregg | 2132128292 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 14), 10/19/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 3), 10/20/2026 |
| Harris | 2221343697 | Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19278), 09/24/2026 | Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-30) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19278), 10/14/2026 |
| Hunt | 1015418392 | Vote History 10.27.2026.pdf (page 43), 10/27/2026 | Vote History 10.28.2026.pdf (page 40), 10/28/2026 |
| Montgomery | 1100132244 | GEN110326_10.02.2026.zip > GEN110326_10.02.2026.csv (line 184; line 185), 10/02/2026 | GEN110326_10.22.2026.zip > GEN110326_10.22.2026.csv (line 40502; line 40503), 10/22/2026 |
| Montgomery | 1126476176 | GEN110326_10.05.2026.zip > GEN110326_10.05.2026.csv (line 498; line 499; line 500), 10/05/2026 | GEN110326_10.24.2026.zip > GEN110326_10.24.2026.csv (line 44163; line 44164; line 44165), 10/24/2026 |
| Montgomery | 1119403485 | GEN110326_10.08.2026.zip > GEN110326_10.08.2026.csv (line 176; line 177; line 178; line 179), 10/08/2026 | GEN110326_10.26.2026.zip > GEN110326_10.26.2026.csv (line 49917; line 49918; line 49919; line 49920), 10/26/2026 |
| Travis | 1132888023 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 09.30.2026 Ballot by Mail.xlsx (sheet 10.01.2026 Ballot by Mail row 16), 09/30/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 999), 10/22/2026 |
| Travis | 1140151036 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.05.2026 Ballot by Mail_UPDATED_2.xlsx (sheet 10.05.2026 Ballot by Mail row 107), 10/05/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.19.2026 Ballot by Mail.xlsx (sheet 10.19.2026 Ballot by Mail row 2110), 10/19/2026 |
| Travis | 3383285286 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.07.2026 Ballot by Mail.xlsx (sheet 10.07.2026 Ballot by Mail row 906), 10/07/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 1085), 10/22/2026 |
| Travis | 1139039837 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.12.2026 Ballot by Mail.xlsx (sheet 10.12.2026 Ballot by Mail row 12), 10/12/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.13.2026 Ballot by Mail.xlsx (sheet 10.13.2026 Ballot by Mail row 376), 10/13/2026 |
| Travis | 1140972395 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.16.2026 Ballot by Mail.xlsx (sheet 10.16.2026 Ballot by Mail row 7), 10/16/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 1114), 10/22/2026 |
| Travis | 2129048624 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.26.2026 Ballot by Mail_UPDATED.xlsx (sheet 10.26.2026 Ballot by Mail row 29), 10/26/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-30) > 10.27.2026 Ballot by Mail.xlsx (sheet 10.27.2026 Ballot by Mail row 19), 10/27/2026 |

## 7. Identifiers that are not state VUIDs (§6.11)

All 1712 records are in the CSV. For 26 of them, the state's file for the same county lists one voter with the same last name, first name and precinct under a 10-digit VUID; these are inferences to be confirmed, not matches by ID:

| County | County identifier | County file and location | Possible state VUID |
|---|---|---|---|
| Bexar | 221423660 | EV_VoterCheckins_10212026.pdf (page 195), in person 10/21/2026, pct S 1105.01 | 2214238660 |
| Bexar | 383323270 | EV_VoterCheckins_10242026.pdf (page 625), in person 10/24/2026, pct S 2028.01 | 3383323270 |
| Collin | 4168640 | early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 33465), in person 10/22/2026, pct 94 | 1076312029 |
| Collin | 4203312 | early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 179348), in person 10/29/2026, pct 22 | 3382649972 |
| Collin | 472987 | early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 87628), in person 10/24/2026, pct 69 | 1075716466 |
| Collin | 585248 | early-voters-through-october-29-2026.xlsx (sheet Detailed Check-in Report - Orde row 249298), in person 10/27/2026, pct 138 | 3383598994 |
| El Paso | 1482939 | Received_Ballots_Voter_List10.30.csv (line 5939), mail 10/22/2026, pct 008.1 | 1095318907 |
| El Paso | 1491908 | Received_Ballots_Voter_List10.30.csv (line 7632), mail 10/29/2026, pct 228.1 | 2211907132 |
| Grayson | 116404233 | October 20 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 299), in person 10/20/2026, pct 109 | 1090375242 |
| Grayson | 168949 | October 25 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 706), in person 10/25/2026, pct 302 | 1133330857 |
| Grayson | 169043 | October 24 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 976), in person 10/24/2026, pct 406 | 2146465425 |
| Grayson | 169077 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1904), in person 10/19/2026, pct 108 | 2156111295 |
| Grayson | 169246 | October 22 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1825), in person 10/22/2026, pct 408 | 2121250168 |
| Grayson | 170955 | October 21 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 4015), in person 10/21/2026, pct 412 | 1090100033 |
| Hidalgo | 10451891 | EV Roster November 3, 2026 (Cumulative).csv (downloaded 2026-10-30) (line 45290), in person, pct 2 | 2151999073 |
| Hidalgo | 61100048 | ABBM LIST November 3, 2026 (Cumulative)_202610300632579549.xlsx (sheet Received Ballots Voter List_Oct row 3430), mail, pct 98.01 | 1154845862 |
| Hidalgo | 61108735 | ABBM LIST November 3, 2026 (Cumulative)_202610300632579549.xlsx (sheet Received Ballots Voter List_Oct row 203), mail, pct 135.01 | 2141425996 |
| Smith | 2436166 | 10.25.26 In Person Voter List.pdf (page 53), in person 10/25/2026, pct 412 | 1027280545 |
| Smith | 2439110 | 10.19.26 In Person Voter List.pdf (page 123), in person 10/19/2026, pct 451 | 1129017692 |
| Tarrant | 41737473 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 16381), mail 10/29/2026, pct 2423 | 2222245072 |
| Tarrant | 41741013 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 16412), mail 10/22/2026, pct 4053 | 3383117753 |
| Tarrant | 41741720 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 16435), mail 10/20/2026, pct 4116 | 2202167573 |
| Tarrant | 41748593 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 16577), mail 10/24/2026, pct 2235 | 1175768732 |
| Tarrant | 41765735 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 16685), mail 10/20/2026, pct 3035 | 1082322527 |
| Tarrant | 41768481 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 16723), mail 10/16/2026, pct 3176 | 3383497931 |
| Tarrant | 41772205 | absentee_returned_voter_report.zip (downloaded 2026-10-30) > absentee_returned_voter_report.txt (line 16768), mail 10/23/2026, pct 2055 | 3383532539 |
