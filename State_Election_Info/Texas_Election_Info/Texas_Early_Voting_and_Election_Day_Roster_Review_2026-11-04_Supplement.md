# Texas Early Voting and Election Day Rosters: Record Locator Supplement

**Supplements:** Texas Early Voting and Election Day Rosters: County and State Data Review, November 4, 2026\
**Election:** November 3, 2026 General Election (SOS election 53815)\
**Data:** early voting October 19-30, mail ballots through Election Day, and Election Day; county files as posted by 3:30 PM November 4 (Travis's ZIP and Wichita's Election Day check-ins downloaded by hand that afternoon), state files as posted on the afternoon of November 4\
**For:** county election offices and the Secretary of State's Elections Division

This supplement lists the voter records behind the anomalies in sections 6.1 and 6.4-6.12 of the November 4 review, so that county and state staff can find each record in their own files. Section 6.2 (voters whose identities Tarrant County redacts) is not included. It replaces the November 2 supplement: the lists below cover October 14 - November 3, the current versions of each county's files including the Election Day rosters posted by November 4, and the state's early voting files for every day as re-downloaded on November 4 and its Election Day files (the state revised many earlier days, so state line numbers differ from the November 2 supplement). The state's Election Day files were partial for most large counties on November 4; records that involve them may change when those files are complete.

Each record is identified by its VUID (or, in §6.11, the county's own identifier) and by where it appears: the file name as the county or the Secretary of State posted it, the entry inside a ZIP, and the line, worksheet row or PDF page. Voter names and addresses are not reproduced: the VUID and location identify each record. All identifiers come from rosters published under Texas Election Code §87.121.

The complete list, 25,149 records, is in the companion file **Texas_Early_Voting_and_Election_Day_Roster_Review_2026-11-04_Supplement_Records.csv**. This document summarizes it and lists the smaller sections in full.

## 1. How to read the locations

- **County files** are named as posted on the county's website. A county that posts a file under the same name every day is shown with the date the copy was downloaded, for example `Cumulative_BBM_ToPost_1126.zip (downloaded 2026-11-04)`. Travis County's daily workbooks, including "11.03.2026 Election Day.xlsx", are entries in its ZIP `G26-Voter-Rosters.zip`; Harris, Tarrant and Montgomery files are also ZIP entries (Harris's Election Day roster is split into four CSV parts). A location is written `file > entry (location)`.
- **State files** are named as the SOS portal's downloads name them: early voting `<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv` ("..." in the tables stands for ".2026 NOVEMBER GENERAL ELECTION.EarlyVoting."), and Election Day `<COUNTY>.2026 NOVEMBER GENERAL ELECTION.ElectionDay.11_03_2026.zip` ("...ElectionDay..." in the tables), whose voters are in the entry `<id>_<COUNTY>_VOTER_INFO.csv`.
- **Line** numbers count the header as line 1. **Sheet ... row** is the worksheet name (as stored in the file, which may be cut short) and the Excel row number; this includes Jefferson's Excel 97-2003 (.xls) files. **Page** is the PDF page, counting from 1. Where a VUID appears on several lines or pages of one file, all are listed.
- **Dates** are the voting or received date in the county's file, and for state records the date of the state's file (which for mail ballots is when the state recorded the ballot, not when the county received it; 11/03/2026 for the state's Election Day files). Tarrant's, Hidalgo's and Potter's in-person lists are cumulative and carry no voting date, nor do Harris's, Tarrant's, Hidalgo's and Potter's Election Day rosters; El Paso's, Denton's and Jefferson's lists show the list's date.
- The files were downloaded October 20 - November 4, 2026. Counties replace some files daily and the state revises its files, so a later copy of a file may differ in line numbers. Angelina is located in its cumulative October 25 file (October 19-25) and its October 26-30 files. Bexar's October 27, 28 and 30 check-in lists are named `EV_VoterCheckins_10272028.pdf`, `EV_VoterCheckins_10282028.pdf` and `EV_VoterCheckins_10302028.pdf`; their records are dated 2026 here. Collin is located in its separate early voting (`early-voting-through-october-30-2026.xlsx`) and Election Day (`election-day-voters-november-3-2026.xlsx`) lists, not its combined list. Webb's October 28 mail list gives a county certificate number instead of the VUID; its records are not located here. Mail ballots that Bastrop and Randall list as received November 4 are outside this supplement.

### CSV columns

| Column | Content |
|---|---|
| section, category | The review section (6.1-6.12) and the kind of anomaly |
| county | The county whose file (county or SOS) holds the anomaly |
| vuid_or_id | The VUID (10 digits) or, in 6.11, the county's identifier |
| county_file, county_zip_entry, county_location | Where the record is in the county's own files |
| county_method, county_date, county_precinct | Voting method, date and precinct in the county's file |
| state_file, state_location, state_method, state_date, state_precinct | The same for the SOS county file (early voting CSV or Election Day ZIP entry) |
| other_county, other_file, other_zip_entry, other_location, other_method, other_date, other_precinct | The second record: another county's roster (6.1, 6.7, 6.8), the second row in the same county (6.7, 6.9, 6.10), or the earlier vote (6.12) |
| note | Context, e.g. a state mail date after the county's latest list, an inferred VUID for a 6.11 identifier, or what the state lists for a 6.12 voter |

## 2. Records by section and county

| Section | Anomaly | Records by county |
|---|---|---|
| 6.1 | SOS county file lists a voter who is on another county's roster (same first name, that county's precinct) | 3,138 (226 in Election Day files): Travis 339, Dallas 290, Denton 260, Bexar 252, Harris 159, Collin 156, Johnson 139, Hays 138, Tarrant 117, Brazos 114, Parker 104, and 1-96 in 39 more SOS county files |
| 6.4 | SOS mail ballot not on the county's own mail list (Tarrant excluded: §6.2) | 3,888: Travis 579, Montgomery 527, Harris 373, Hunt 344, Galveston 318, Dallas 277, Gregg 199, Bexar 194, Jefferson 190, Bell 171, and 2-169 in 18 more counties |
| 6.5 | Precinct differs between the county's and the state's record of the same VUID | 14,158: Williamson 4,722, Bowie 4,717, Harris 3,509, Tarrant 316, El Paso 253, Galveston 206, Brazos 164, Smith 116, Bexar 71, Denton 17, and 1-17 in 12 more counties |
| 6.6 | Surname differs between the county and the state | 1,197: Collin 186, Tarrant 90, Denton 89, Bexar 84, Williamson 76, Galveston 68, Brazoria 62, Cameron 53, Dallas 45, Travis 43, and 1-40 in 24 more counties |
| 6.7 | One VUID, two different people | 104 records, 80 VUIDs (§3) |
| 6.8 | Same voter on two counties' own rosters | 31, 11 of them new and involving Election Day (§4) |
| 6.9 | Mail ballot and early in-person vote by the same voter | 43 (§5) |
| 6.10 | Same-day duplicate check-in (early voting or Election Day); mail ballot received on two days | 228 + 39 (§6) |
| 6.11 | Identifier that is not a 10-digit state VUID | 2,306: Collin 776, Tarrant 669, Denton 264, Grayson 108, El Paso 91, Hidalgo 91, Smith 76, Guadalupe 69, Jefferson 69, Webb 36, Gregg 28, Bowie 11, Bexar 9, Tom Green 4, Henderson 3, Travis 2 (§7) |
| 6.12 | Election Day vote and an early in-person vote or mail ballot in the same county's own files | 17: Dallas 5, Bexar 3, Grayson 3, Webb 2, and one each in Brazos, Galveston, Tarrant and Travis (§8) |

Notes on the larger sections:

- **6.4 timing:** 775 records are dated after the county's latest mail list (marked in the note column), most of them mail ballots in the state's Election Day files for counties whose mail lists stop on October 29 or 30 (Montgomery 355, Jefferson 184, Gregg 114, Fort Bend 56, Wichita 50). **6.4 Travis:** 350 of the 579 are in the state's Election Day file, although Travis's mail lists run through November 3. **6.4 Galveston:** 216 are dated October 29 and 91 October 27, days for which Galveston has posted no mail list. **6.4 Hunt:** 319 of the 344 are dated October 14-19 by the state.
- **6.5 Williamson:** 4,718 of the 4,722 are mail voters in the county's latest file (`VoterTurnout_261103_202611040153278106.xlsx`); the county's mail precincts have differed from the state's since its October 22 file. **6.5 Bowie:** all 4,717 are in the October 20, 21 and 23 Early Vote History reports. **6.5 Harris:** 3,428 of the 3,509 are early in-person records.
- **6.6:** in all but 45 records the county and state names share a word, which points to name changes or formatting rather than different people.
- **6.7 Galveston:** ten VUIDs on rows 7038-7047 of `Checkins_10302026.xlsx` are each repeated further down the same list for a different person; they are listed under 6.7 only, not also as same-day duplicates in 6.10. The state's revised Galveston files now list those ten voters, which adds ten "SOS file vs the same county's roster" records.
- **6.10 Angelina:** 166 records in its early voting history files, which list voters two or three times on the same day with the same precinct.

## 3. One VUID, two different people (§6.7)

| VUID | Case | Record 1 | Record 2 |
|---|---|---|---|
| 3382565201 | Two people under one VUID in Bexar's files | Bexar: voter_check_in_details 10-19-2026.pdf (page 636), in person, 10/19/2026, pct P 3149 | Bexar: EV_VoterCheckins_10232026.pdf (page 76), in person, 10/23/2026, pct S 2019.01 |
| 3383566298 | Two people under one VUID in Bexar's files | Bexar: Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 609; page 2693), Election Day, 11/03/2026, pct P 1071 | Bexar: Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 609; page 2693), Election Day, 11/03/2026, pct P 3039.01 |
| 1019649503 | Two people under one VUID in Bowie's files | Bowie: BBM Received 10.22.2026_1.pdf (page 1), mail, 10/22/2026 | Bowie: BBM Received 10.24.2026.pdf (page 1), mail, 10/24/2026 |
| 1016063262 | Two people under one VUID in Brazoria's files | Brazoria: 2026-10-27-MAIL LIST GENERAL.xlsx (downloaded 2026-10-31) (sheet 10.27.26 MAIL LIST GENERAL row 12; sheet 10.27.26 MAIL LIST GENERAL row 13), mail, 10/27/2026, pct 17 | Brazoria: 2026-10-27-MAIL LIST GENERAL.xlsx (downloaded 2026-10-31) (sheet 10.27.26 MAIL LIST GENERAL row 12; sheet 10.27.26 MAIL LIST GENERAL row 13), mail, 10/27/2026, pct 18 |
| 1220978773 | Two people under one VUID in Dallas's files | Dallas: In-Person-Early-Voting-Details-Report_10.30.26.csv (line 171479; line 315725), in person, 10/28/2026, pct 3081 | Dallas: In-Person-Early-Voting-Details-Report_10.30.26.csv (line 171479; line 315725), in person, 10/21/2026, pct 4079 |
| 1033536850 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7046; sheet voter_check_in_details - 2026-1 row 11236), in person, 10/30/2026, pct 401 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7046; sheet voter_check_in_details - 2026-1 row 11236), in person, 10/30/2026, pct 191 |
| 1033998078 | Two people under one VUID in Galveston's files | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 92; line 213), mail, 10/28/2026, pct Precinct 353 | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 92; line 213), mail, 10/28/2026, pct Precinct 150 |
| 1048631693 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7044; sheet voter_check_in_details - 2026-1 row 10343), in person, 10/30/2026, pct 230 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7044; sheet voter_check_in_details - 2026-1 row 10343), in person, 10/30/2026, pct 480 |
| 1069072158 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7041; sheet voter_check_in_details - 2026-1 row 8824), in person, 10/30/2026, pct 191 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7041; sheet voter_check_in_details - 2026-1 row 8824), in person, 10/30/2026, pct 214 |
| 1155831069 | Two people under one VUID in Galveston's files | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 38; line 101), mail, 10/28/2026, pct Precinct 356 | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 38; line 101), mail, 10/28/2026, pct Precinct 218 |
| 1178314385 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7045; sheet voter_check_in_details - 2026-1 row 10776), in person, 10/30/2026, pct 384 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7045; sheet voter_check_in_details - 2026-1 row 10776), in person, 10/30/2026, pct 216 |
| 1180740179 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7038; sheet voter_check_in_details - 2026-1 row 7440), in person, 10/30/2026, pct 356 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7038; sheet voter_check_in_details - 2026-1 row 7440), in person, 10/30/2026, pct 232.3 |
| 1195153193 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7040; sheet voter_check_in_details - 2026-1 row 8113), in person, 10/30/2026, pct 150 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7040; sheet voter_check_in_details - 2026-1 row 8113), in person, 10/30/2026, pct 203 |
| 1221470536 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7043; sheet voter_check_in_details - 2026-1 row 10344), in person, 10/30/2026, pct 278 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7043; sheet voter_check_in_details - 2026-1 row 10344), in person, 10/30/2026, pct 480 |
| 2164002074 | Two people under one VUID in Galveston's files | Galveston: Checkins_10272026.xlsx (sheet voter_check_in_details - 2026-1 row 1946; sheet voter_check_in_details - 2026-1 row 2108), in person, 10/27/2026, pct 435 | Galveston: Checkins_10272026.xlsx (sheet voter_check_in_details - 2026-1 row 1946; sheet voter_check_in_details - 2026-1 row 2108), in person, 10/27/2026, pct 206 |
| 3383495514 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7047; sheet voter_check_in_details - 2026-1 row 12014), in person, 10/30/2026, pct 460 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7047; sheet voter_check_in_details - 2026-1 row 12014), in person, 10/30/2026, pct 206 |
| 3383501025 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7042; sheet voter_check_in_details - 2026-1 row 9080), in person, 10/30/2026, pct 438 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7042; sheet voter_check_in_details - 2026-1 row 9080), in person, 10/30/2026, pct 276 |
| 3383603916 | Two people under one VUID in Galveston's files | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7039; sheet voter_check_in_details - 2026-1 row 7959), in person, 10/30/2026, pct 428 | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7039; sheet voter_check_in_details - 2026-1 row 7959), in person, 10/30/2026, pct 354 |
| 2221343697 | Two people under one VUID in Harris's files | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-11-04) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19283), mail, 09/24/2026, pct 0139 | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-11-04) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19283), mail, 10/14/2026, pct 0505 |
| 1015418392 | Two people under one VUID in Hunt's files | Hunt: Vote History 10.27.2026.pdf (page 43), mail, 10/27/2026, pct 431-SCO | Hunt: Vote History 10.28.2026.pdf (page 40), mail, 10/28/2026, pct 325-SGR5-SGSUD |
| 2223366061 | Two counties' rosters, two different people | Bell: 20261021_BBM.pdf (page 9), mail, 10/21/2026, pct 201 | Randall: Mail Ballot Roster 11.4.26_202611041313068348.pdf (page 34), mail, 10/21/2026 |
| 2223366074 | Two counties' rosters, two different people | Bell: 20261026_BBM.pdf (page 1), mail, 10/26/2026, pct 408 | Wichita: Absentee-10.29.2026.pdf (page 29), mail, 10/29/2026, pct S 309.6-IPCISD-IRR |
| 2223366088 | Two counties' rosters, two different people | Bell: 20261027_BBM.pdf (page 1), mail, 10/27/2026, pct 302 | Randall: Mail Ballot Roster 11.4.26_202611041313068348.pdf (page 41), mail, 10/27/2026 |
| 2223367198 | Two counties' rosters, two different people | Bell: 20261019_BBM.pdf (page 1), mail, 10/15/2026, pct 307 | Randall: Early Voting Roster 10.30.26_202610310002151652.pdf (page 715), in person, 10/30/2026, pct 216 |
| 2223375637 | Two counties' rosters, two different people | Bell: 20261026_BBM.pdf (page 2), mail, 10/26/2026, pct 308 | Fort Bend: EV_1030_1.xlsx (sheet List of Check-ins 2026-10-31 row 243430), in person, 10/30/2026, pct 4113 |
| 2223375713 | Two counties' rosters, two different people | Bell: 20261020_EV.pdf (page 70), in person, 10/20/2026, pct 105 | Fort Bend: EV_1030_1.xlsx (sheet List of Check-ins 2026-10-31 row 266883), in person, 10/28/2026, pct 4173 |
| 3383289734 | Two counties' rosters, two different people | Bexar: voter_check_in_details 10-19-2026.pdf (page 311), in person, 10/19/2026, pct S 4020.01 | Brazoria: 2026-10-29-MAIL LIST GENERAL.xlsx (downloaded 2026-10-31) (sheet 10.29.26 MAIL LIST GENERAL row 166), mail, 10/29/2026, pct 78 |
| 2161903259 | Two counties' rosters, two different people | Collin: election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 58094), Election Day, 11/03/2026, pct 18 | Dallas: Election-Day-Website-Roster.csv (line 64320), Election Day, 11/03/2026, pct 2520 |
| 1015936692 | Two counties' rosters, two different people | El Paso: Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 36676), in person, 10/30/2026, pct 052 | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-30-26.csv (line 80439), in person, 10/30/2026, pct 0851 |
| 2211542442 | SOS file vs another county's roster | SOS BELL ... 10_22_2026.csv (line 130) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-22-26.csv (line 29341), in person, 10/22/2026, pct 0347 |
| 3383289734 | SOS file vs another county's roster | SOS BEXAR ... 10_19_2026.csv (line 31760) | Brazoria: 2026-10-29-MAIL LIST GENERAL.xlsx (downloaded 2026-10-31) (sheet 10.29.26 MAIL LIST GENERAL row 166), mail, 10/29/2026, pct 78 |
| 1075809879 | SOS file vs another county's roster | SOS COLLIN ... 10_23_2026.csv (line 676) | Dallas: In-Person-Early-Voting-Details-Report_10.30.26.csv (line 126194), in person, 10/23/2026, pct 1059 |
| 2223366026 | SOS file vs another county's roster | SOS CORYELL ... 10_24_2026.csv (line 500) | Bell: 20261024_BBM.pdf (page 4), mail, 10/24/2026, pct 102 |
| 2223366042 | SOS file vs another county's roster | SOS CORYELL ... 10_19_2026.csv (line 1060) | Bell: 20261019_EV.pdf (page 3), in person, 10/19/2026, pct 104 |
| 2223366057 | SOS file vs another county's roster | SOS CORYELL ... 10_26_2026.csv (line 650) | Bell: 20261026_BBM.pdf (page 4), mail, 10/26/2026, pct 201 |
| 2161903259 | SOS file vs another county's roster | SOS DALLAS ...ElectionDay... 11_03_2026.zip > 57_DALLAS_VOTER_INFO.csv (line 95) | Collin: election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 58094), Election Day, 11/03/2026, pct 18 |
| 2223368590 | SOS file vs another county's roster | SOS DALLAS ... 10_24_2026.csv (line 34339) | Denton: 1126_11-03_Returned_Mail_Roster.xlsx (sheet Sheet1 row 4681), mail, 10/24/2026, pct 3128 |
| 2223368629 | SOS file vs another county's roster | SOS DALLAS ... 10_24_2026.csv (line 18762) | Denton: 1126_10-30_Early-Voting-Roster.xlsx (sheet Sheet1 row 318145), in person, 10/24/2026, pct 103201 |
| 2223381239 | SOS file vs another county's roster | SOS DENTON ... 10_29_2026.csv (line 8053) | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-11-04) > Cumulative_BBM_ToPost_1126.csv (line 42585), mail, 10/29/2026, pct 0501 |
| 2223375616 | SOS file vs another county's roster | SOS FORT BEND ... 10_19_2026.csv (line 17572) | Bell: 20261019_BBM.pdf (page 30), mail, 10/19/2026, pct 101 |
| 2223375637 | SOS file vs another county's roster | SOS FORT BEND ... 10_30_2026.csv (line 21334) | Bell: 20261026_BBM.pdf (page 2), mail, 10/26/2026, pct 308 |
| 2223375713 | SOS file vs another county's roster | SOS FORT BEND ... 10_20_2026.csv (line 26862) | Bell: 20261020_EV.pdf (page 70), in person, 10/20/2026, pct 105 |
| 1015936692 | SOS file vs another county's roster | SOS HARRIS ... 10_21_2026.csv (line 57358) | El Paso: Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 36676), in person, 10/30/2026, pct 052 |
| 1177988407 | SOS file vs another county's roster | SOS HIDALGO ... 10_27_2026.csv (line 7920) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-27-26.csv (line 75018), in person, 10/27/2026, pct 1120 |
| 2223365544 | SOS file vs another county's roster | SOS HIDALGO ... 10_14_2026.csv (line 9) | Tarrant: absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 17855), mail, 10/14/2026, pct 2636 |
| 2223382143 | SOS file vs another county's roster | SOS HIDALGO ... 10_19_2026.csv (line 16142) | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-11-04) > Cumulative_BBM_ToPost_1126.csv (line 23877), mail, 10/19/2026, pct 0771 |
| 2223366100 | SOS file vs another county's roster | SOS LUBBOCK ... 10_22_2026.csv (line 3122) | Bell: 20261022_BBM.pdf (page 1), mail, 10/22/2026, pct 311 |
| 2223378210 | SOS file vs another county's roster | SOS MONTGOMERY ... 10_26_2026.csv (line 8526) | McLennan: Website List_202610302226078575.pdf (page 1063), , pct 59 |
| 2223378724 | SOS file vs another county's roster | SOS MONTGOMERY ... 10_20_2026.csv (line 11799) | El Paso: Received_Ballots_Voter_List_20261104113257.csv (line 5314), mail, 10/20/2026, pct 29.1 |
| 2223366061 | SOS file vs another county's roster | SOS RANDALL ... 10_21_2026.csv (line 3044) | Bell: 20261021_BBM.pdf (page 9), mail, 10/21/2026, pct 201 |
| 2223366088 | SOS file vs another county's roster | SOS RANDALL ... 10_27_2026.csv (line 570) | Bell: 20261027_BBM.pdf (page 1), mail, 10/27/2026, pct 302 |
| 2223367198 | SOS file vs another county's roster | SOS RANDALL ... 10_30_2026.csv (line 2438) | Bell: 20261019_BBM.pdf (page 1), mail, 10/15/2026, pct 307 |
| 3382917068 | SOS file vs another county's roster | SOS TARRANT ... 10_26_2026.csv (line 43810) | Denton: 1126_10-30_Early-Voting-Roster.xlsx (sheet Sheet1 row 51250), in person, 10/26/2026, pct 211301 |
| 2196506909 | SOS file vs another county's roster | SOS TOM GREEN ... 10_27_2026.csv (line 1357) | Collin: early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 219589), in person, 10/27/2026, pct 8 |
| 1183615991 | SOS file vs another county's roster | SOS TRAVIS ... 10_28_2026.csv (line 29472) | Williamson: VoterTurnout_261103_202611040153278106.xlsx (sheet Sheet1 row 70582), in person, 10/28/2026, pct 155 |
| 2196434701 | SOS file vs another county's roster | SOS TRAVIS ... 10_29_2026.csv (line 9482) | El Paso: Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 27042), in person, 10/30/2026, pct 154 |
| 2223366035 | SOS file vs another county's roster | SOS VICTORIA ... 10_20_2026.csv (line 1315) | Bell: 20261020_BBM.pdf (page 1), mail, 10/20/2026, pct 317 |
| 2223366074 | SOS file vs another county's roster | SOS WICHITA ... 10_26_2026.csv (line 1036) | Bell: 20261026_BBM.pdf (page 1), mail, 10/26/2026, pct 408 |
| 1031505594 | SOS file vs the same county's roster | SOS BEXAR ... 10_22_2026.csv (line 38574) | Bexar: EV_VoterCheckins_10222026.pdf (page 644), in person, 10/22/2026, pct S 3124.01 |
| 2154123680 | SOS file vs the same county's roster | SOS BEXAR ... 10_24_2026.csv (line 40995) | Bexar: EV_VoterCheckins_10242026.pdf (page 79), in person, 10/24/2026, pct P 4097 |
| 3383579919 | SOS file vs the same county's roster | SOS BEXAR ... 10_22_2026.csv (line 21369) | Bexar: EV_VoterCheckins_10222026.pdf (page 567), in person, 10/22/2026, pct P 3052 |
| 1019649503 | SOS file vs the same county's roster | SOS BOWIE ... 10_24_2026.csv (line 660) | Bowie: BBM Received 10.22.2026_1.pdf (page 1), mail, 10/22/2026 |
| 1017062829 | SOS file vs the same county's roster | SOS BRAZORIA ... 10_20_2026.csv (line 13869) | Brazoria: 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (downloaded 2026-10-29) (sheet BBM ROSTER-9.28-10.16.26 row 1754), mail, 10/02/2026, pct 76 |
| 1145191969 | SOS file vs the same county's roster | SOS BRAZORIA ... 10_26_2026.csv (line 6164) | Brazoria: 2026-10-22-MAIL LIST GENERAL.xlsx (downloaded 2026-10-29) (sheet 10.22.26 MAIL LIST GENERAL row 36), mail, 10/22/2026, pct 12 |
| 1167694139 | SOS file vs the same county's roster | SOS COLLIN ... 10_19_2026.csv (line 15505) | Collin: early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 49035), in person, 10/19/2026, pct 246 |
| 2174117604 | SOS file vs the same county's roster | SOS COLLIN ... 10_26_2026.csv (line 867) | Collin: early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 214969), in person, 10/26/2026, pct 40 |
| 1082025131 | SOS file vs the same county's roster | SOS DALLAS ... 10_29_2026.csv (line 37414) | Dallas: In-Person-Early-Voting-Details-Report_10.30.26.csv (line 501648), in person, 10/29/2026, pct 3800 |
| 3382936850 | SOS file vs the same county's roster | SOS DALLAS ... 10_24_2026.csv (line 12989) | Dallas: In-Person-Early-Voting-Details-Report_10.30.26.csv (line 283521), in person, 10/24/2026, pct 2048 |
| 2203146614 | SOS file vs the same county's roster | SOS DENTON ... 10_24_2026.csv (line 13648) | Denton: 1126_10-30_Early-Voting-Roster.xlsx (sheet Sheet1 row 109822), in person, 10/24/2026, pct 209701 |
| 1182032143 | SOS file vs the same county's roster | SOS EL PASO ... 10_22_2026.csv (line 13316) | El Paso: Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 66526), in person, 10/30/2026, pct 012 |
| 1033536850 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 11744) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7046; sheet voter_check_in_details - 2026-1 row 11236), in person, 10/30/2026, pct 401 |
| 1033998078 | SOS file vs the same county's roster | SOS GALVESTON ... 10_28_2026.csv (line 7026) | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 92; line 213), mail, 10/28/2026, pct Precinct 353 |
| 1048631693 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 10458) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7044; sheet voter_check_in_details - 2026-1 row 10343), in person, 10/30/2026, pct 230 |
| 1069072158 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 8920) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7041; sheet voter_check_in_details - 2026-1 row 8824), in person, 10/30/2026, pct 191 |
| 1155831069 | SOS file vs the same county's roster | SOS GALVESTON ... 10_28_2026.csv (line 5114) | Galveston: NOV-3-2026-BALLOTS-RECEIVED-10.28.26.csv (line 38; line 101), mail, 10/28/2026, pct Precinct 356 |
| 1178314385 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 10894) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7045; sheet voter_check_in_details - 2026-1 row 10776), in person, 10/30/2026, pct 384 |
| 1180740179 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 7523) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7038; sheet voter_check_in_details - 2026-1 row 7440), in person, 10/30/2026, pct 356 |
| 1195153193 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 8201) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7040; sheet voter_check_in_details - 2026-1 row 8113), in person, 10/30/2026, pct 150 |
| 1221470536 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 10459) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7043; sheet voter_check_in_details - 2026-1 row 10344), in person, 10/30/2026, pct 278 |
| 3383495514 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 12144) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7047; sheet voter_check_in_details - 2026-1 row 12014), in person, 10/30/2026, pct 460 |
| 3383501025 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 9173) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7042; sheet voter_check_in_details - 2026-1 row 9080), in person, 10/30/2026, pct 438 |
| 3383603916 | SOS file vs the same county's roster | SOS GALVESTON ... 10_30_2026.csv (line 8043) | Galveston: Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 7039; sheet voter_check_in_details - 2026-1 row 7959), in person, 10/30/2026, pct 428 |
| 1025420886 | SOS file vs the same county's roster | SOS GRAYSON ... 10_28_2026.csv (line 578) | Grayson: October 28 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1583), in person, 10/28/2026, pct 306 |
| 3383626540 | SOS file vs the same county's roster | SOS GREGG ...ElectionDay... 11_03_2026.zip > 92_GREGG_VOTER_INFO.csv (line 7704) | Gregg: 2026-11-03-Voter-Turnout.pdf (page 116), Election Day, 11/03/2026, pct 11 |
| 2000639205 | SOS file vs the same county's roster | SOS GUADALUPE ... 10_27_2026.csv (line 2242) | Guadalupe: Early Voting in Person Roster_10-27-2026.pdf (page 32), in person, 10/27/2026, pct 105 |
| 2141714646 | SOS file vs the same county's roster | SOS GUADALUPE ... 10_27_2026.csv (line 3985) | Guadalupe: Early Voting in Person Roster_10-27-2026.pdf (page 57), in person, 10/27/2026, pct 307 |
| 2209931621 | SOS file vs the same county's roster | SOS GUADALUPE ... 10_27_2026.csv (line 3986) | Guadalupe: Early Voting in Person Roster_10-27-2026.pdf (page 57), in person, 10/27/2026, pct 307 |
| 1143970888 | SOS file vs the same county's roster | SOS HARRIS ... 10_24_2026.csv (line 34758) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 30020), in person, 10/24/2026, pct 0902 |
| 1175275415 | SOS file vs the same county's roster | SOS HARRIS ... 10_24_2026.csv (line 9445) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 67167), in person, 10/24/2026, pct 0006 |
| 2120203917 | SOS file vs the same county's roster | SOS HARRIS ... 10_27_2026.csv (line 12173) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-27-26.csv (line 63314), in person, 10/27/2026, pct 0166 |
| 2170642029 | SOS file vs the same county's roster | SOS HARRIS ... 10_23_2026.csv (line 51133) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-23-26.csv (line 57651), in person, 10/23/2026, pct 0436 |
| 2206845981 | SOS file vs the same county's roster | SOS HARRIS ... 10_22_2026.csv (line 31019) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-22-26.csv (line 75317), in person, 10/22/2026, pct 0672 |
| 3382471503 | SOS file vs the same county's roster | SOS HARRIS ... 10_30_2026.csv (line 30618) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-30-26.csv (line 104272), in person, 10/30/2026, pct 0994 |
| 1015418392 | SOS file vs the same county's roster | SOS HUNT ... 10_28_2026.csv (line 331) | Hunt: Vote History 10.27.2026.pdf (page 43), mail, 10/27/2026, pct 431-SCO |
| 2001538580 | SOS file vs the same county's roster | SOS MCLENNAN ... 10_30_2026.csv (line 4698) | McLennan: Website List_202610302226078575.pdf (page 278), , pct 42 |
| 3383208181 | SOS file vs the same county's roster | SOS MONTGOMERY ... 10_21_2026.csv (line 9373) | Montgomery: GEN110326_10.21.2026.zip > GEN110326_10.21.2026.csv (line 36534; line 36535; line 36536), in person, 10/21/2026, pct 113 |
| 3383218284 | SOS file vs the same county's roster | SOS MONTGOMERY ... 10_27_2026.csv (line 11986) | Montgomery: GEN110326_10.27.2026.zip > GEN110326_10.27.2026.csv (line 43152; line 43153; line 43154; line 43155), in person, 10/27/2026, pct 78 |
| 2139770499 | SOS file vs the same county's roster | SOS POTTER ... 10_24_2026.csv (line 730) | Potter: Early Voting Roster.pdf (downloaded 2026-10-31) (page 155), in person, pct 326 |
| 1045656452 | SOS file vs the same county's roster | SOS TARRANT ... 10_21_2026.csv (line 985) | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 493957), in person, pct 4276 |
| 2164441653 | SOS file vs the same county's roster | SOS TARRANT ... 10_27_2026.csv (line 45436) | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 8182), in person, pct 1059 |
| 3382951067 | SOS file vs the same county's roster | SOS TRAVIS ... 10_28_2026.csv (line 732) | Travis: G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.28.2026 Early Vote.xlsx (sheet EarlyVote row 920), in person, 10/28/2026, pct 312 |
| 1000217084 | SOS file vs the same county's roster | SOS VICTORIA ... 10_30_2026.csv (line 1246) | Victoria: 10-30-2026 mail and in person.xlsx (sheet Sheet1 row 15928), in person, 10/30/2026, pct 5 |
| 1141320567 | SOS file vs the same county's roster | SOS WEBB ...ElectionDay... 11_03_2026.zip > 240_WEBB_VOTER_INFO.csv (line 5168) | Webb: Check-in Report - Ordered by Check-in Time 11-03-2026.pdf (page 197), Election Day, 11/03/2026, pct 343 |

## 4. Same voter on two counties' own rosters (§6.8)

| VUID | Record 1 | Record 2 |
|---|---|---|
| 1050584877 | Bexar: Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 1870), Election Day, 11/03/2026, pct S 2115.01 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 120235), in person, pct 2026 |
| 1052956069 | Bexar: EV_VoterCheckins_10252026.pdf (page 339), in person, 10/25/2026, pct P 4179 | Hidalgo: EV Roster November 3, 2026 (Cumulative).csv (downloaded 2026-10-31) (line 122930), in person, pct 190 |
| 1053842333 | Bexar: EV_VoterCheckins_10232026.pdf (page 406), in person, 10/23/2026, pct P 2143 | Hidalgo: EV Roster November 3, 2026 (Cumulative).csv (downloaded 2026-10-31) (line 82740), in person, pct 151 |
| 1068776945 | Bexar: Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 695), Election Day, 11/03/2026, pct P 1118 | Travis: G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.21.2026 Early Vote.xlsx (sheet EarlyVote row 21920), in person, 10/21/2026, pct 180 |
| 1072354984 | Bexar: voter_check_in_details 10-19-2026.pdf (page 806), in person, 10/19/2026, pct S 1045.01 | Cameron: 01_Cameron_County10-30-2026.xlsx (sheet CC row 45181), in person, 10/25/2026, pct 21 |
| 1095835184 | Bexar: EV_VoterCheckins_10242026.pdf (page 509), in person, 10/24/2026, pct P 4031 | El Paso: Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 115578), in person, 10/30/2026, pct 164 |
| 1173266072 | Bexar: voter_check_in_details 10-19-2026.pdf (page 579), in person, 10/19/2026, pct S 1046.05 | Webb: Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 29), in person, 10/19/2026, pct 266 |
| 2121723707 | Bexar: EV_VoterCheckins_10232026.pdf (page 745), in person, 10/23/2026, pct S 3072.01 | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-30-26.csv (line 23828), in person, 10/30/2026, pct 0163 |
| 2145693600 | Bexar: Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 1135), Election Day, 11/03/2026, pct S 3084.02 | Harris: VoterRoster - ELECTION DAY ONLY_ToPost_1126.zip (downloaded 2026-11-04) > VoterRoster - ELECTION DAY ONLY_ToPost_1126_2.csv (line 89553), Election Day, pct 0611 |
| 2153679438 | Bexar: EV_VoterCheckins_10202026.pdf (page 276), in person, 10/20/2026, pct P 3066 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 28453), in person, pct 1170 |
| 2154635510 | Bexar: EV_VoterCheckins_10232026.pdf (page 464), in person, 10/23/2026, pct S 1075.01 | Dallas: Election-Day-Website-Roster.csv (line 114078), Election Day, 11/03/2026, pct 3032 |
| 2175993859 | Bexar: EV_VoterCheckins_10302028.pdf (page 854), in person, 10/30/2026, pct P 3043 | Travis: G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.23.2026 Early Vote.xlsx (sheet EarlyVote row 39615), in person, 10/23/2026, pct 376 |
| 3383515497 | Bexar: EV_VoterCheckins_10212026.pdf (page 168), in person, 10/21/2026, pct S 1156.01 | Dallas: In-Person-Early-Voting-Details-Report_10.30.26.csv (line 343632), in person, 10/29/2026, pct 3616 |
| 2154157208 | Brazoria: 2026-10-29 IN PERSON.xlsx (sheet 2026-10-29 ROSTER row 2239), in person, 10/29/2026, pct 27 | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-22-26.csv (line 68725), in person, 10/22/2026, pct 0699 |
| 2180196523 | Brazos: in-person-10-23-2026-accessible.pdf (page 69), in person, 10/23/2026, pct 21 | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-30-26.csv (line 73639), in person, 10/30/2026, pct 0380 |
| 1138259335 | Cameron: MAIL-GN26-Cameron-County-CC-2026-11-03.xlsx (sheet Sheet1 row 2304), mail, 10/13/2026, pct 44 | Hidalgo: ABBM LIST November 3, 2026 (Cumulative)_202611040820489518.xlsx (sheet Received Ballots Voter List_Nov row 4514), mail, pct 63.01 |
| 1077658162 | Collin: early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 333297), in person, 10/23/2026, pct 90 | Travis: G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.27.2026 Early Vote.xlsx (sheet EarlyVote row 30170), in person, 10/27/2026, pct 203 |
| 1201776639 | Collin: election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 47363), provisional, 11/03/2026, pct 9 | Dallas: Election-Day-Website-Roster.csv (line 85386), Election Day, 11/03/2026, pct 3969 |
| 2174127580 | Collin: election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 12613), Election Day, 11/03/2026, pct 143 | Dallas: Election-Day-Website-Roster.csv (line 77455), Election Day, 11/03/2026, pct 2519 |
| 1050774882 | Dallas: In-Person-Early-Voting-Details-Report_10.30.26.csv (line 102804), in person, 10/23/2026, pct 1146 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 968), in person, pct 1010 |
| 2176537545 | Dallas: Election-Day-Website-Roster.csv (line 94106), Election Day, 11/03/2026, pct 3940 | Travis: G26-Voter-Rosters.zip (downloaded 2026-11-04) > 11.03.2026 Election Day.xlsx (sheet ElectionDay row 39451), Election Day, 11/03/2026, pct 226 |
| 1011777953 | Denton: 1126_10-30_Early-Voting-Roster.xlsx (sheet Sheet1 row 66910), in person, 10/20/2026, pct 420302 | Smith: 11.03.26 In Person Voter List.pdf (page 176), Election Day, 11/03/2026, pct 215 |
| 2153111005 | El Paso: Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 96921), in person, 10/30/2026, pct 180 | Randall: Early Voting Roster 10.30.26_202610310002151652.pdf (page 263), in person, 10/22/2026, pct 332 |
| 3382836445 | El Paso: Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 130528), in person, 10/30/2026, pct 207 | Galveston: Checkins_11032026.xlsx (sheet voter_check_in_details - 2026-1 row 18981), Election Day, 11/03/2026, pct 223 |
| 3382857971 | El Paso: Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 85942), in person, 10/30/2026, pct 016 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 12369), in person, pct 1083 |
| 1102081832 | Guadalupe: Early Voting in Person Roster_10-21-2026.pdf (page 82), in person, 10/21/2026, pct 406 | Smith: 10.19.26 In Person Voter List.pdf (page 67), in person, 10/19/2026, pct 242 |
| 2205290498 | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 54225), in person, 10/24/2026, pct 0451 | Williamson: VoterTurnout_261103_202611040153278106.xlsx (sheet Sheet1 row 227500), in person, 10/29/2026, pct 210 |
| 2208300194 | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-11-04) > Cumulative_BBM_ToPost_1126.csv (line 475), mail, 09/30/2026, pct 0516 | Travis: G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.23.2026 Early Vote.xlsx (sheet EarlyVote row 9427), in person, 10/23/2026, pct 264 |
| 3383169103 | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-31) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 45999), in person, 10/24/2026, pct 0072 | Travis: G26-Voter-Rosters.zip (downloaded 2026-11-04) > 11.03.2026 Election Day.xlsx (sheet ElectionDay row 14839), Election Day, 11/03/2026, pct 315 |
| 2161167605 | Hidalgo: EV Roster November 3, 2026 (Cumulative).csv (downloaded 2026-10-31) (line 82521), in person, pct 67 | Travis: G26-Voter-Rosters.zip (downloaded 2026-11-04) > 11.03.2026 Election Day.xlsx (sheet ElectionDay row 53578), Election Day, 11/03/2026, pct 370 |
| 2215994140 | Smith: 10.22.26 In Person Voter List.pdf (page 73), in person, 10/22/2026, pct 245 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 34334), in person, pct 1224 |

## 5. Mail ballot and in-person vote by the same voter (§6.9)

| County | VUID | Mail ballot | In-person vote |
|---|---|---|---|
| Bexar | 1003348982 | ABBM_Recvd_10312026.pdf (page 9), 10/31/2026, pct P 3020 | EV_VoterCheckins_10202026.pdf (page 548), 10/20/2026 |
| Brazoria | 1016987682 | 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (downloaded 2026-10-29) (sheet BBM ROSTER-9.28-10.16.26 row 517), 09/29/2026, pct 25 | 2026-10-30 IN PERSON.xlsx (sheet 2026-10-30 ROSTER row 3332), 10/30/2026 |
| Dallas | 1075428602 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 596), 10/24/2026, pct 4079 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 289276), 10/19/2026 |
| Dallas | 1079098339 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 5038), 10/24/2026, pct 1081 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 142876), 10/29/2026 |
| Dallas | 1081471506 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 8415), 10/24/2026, pct 1739 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 372878), 10/28/2026 |
| Dallas | 1081736255 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 10818), 10/09/2026, pct 1748 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 389562), 10/27/2026 |
| Dallas | 1082858250 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 1656), 10/24/2026, pct 4056 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 97673), 10/27/2026 |
| Dallas | 1083306122 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 11408), 10/09/2026, pct 2027 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 216765), 10/22/2026 |
| Dallas | 1083306133 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 11409), 10/09/2026, pct 2027 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 264336), 10/20/2026 |
| Dallas | 1084339442 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 12075), 10/24/2026, pct 3312 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 522683), 10/27/2026 |
| Dallas | 1084810226 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 3549), 10/27/2026, pct 3802 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 498608), 10/30/2026 |
| Dallas | 1088439372 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 4410), 10/24/2026, pct 2079 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 171627), 10/28/2026 |
| Dallas | 1091625391 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 21930), 10/24/2026, pct 2020 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 298507), 10/26/2026 |
| Dallas | 1143777206 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 4779), 10/09/2026, pct 1147 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 210020), 10/19/2026 |
| Dallas | 1183289224 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 6480), 10/25/2026, pct 2027 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 172703), 10/28/2026 |
| Dallas | 2155626901 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 18049), 10/25/2026, pct 2410 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 13043), 10/28/2026 |
| Dallas | 2156785478 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 4838), 10/24/2026, pct 1310 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 178079), 10/28/2026 |
| Dallas | 2168668700 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 4494), 10/24/2026, pct 4511 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 431375), 10/29/2026 |
| El Paso | 1094732728 | Received_Ballots_Voter_List_20261104113257.csv (line 3630), 10/13/2026, pct 20 | Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 86354), 10/30/2026 |
| El Paso | 1095020454 | Received_Ballots_Voter_List_20261104113257.csv (line 4887), 10/20/2026, pct 24 | Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 151703), 10/30/2026 |
| El Paso | 1095047541 | Received_Ballots_Voter_List_20261104113257.csv (line 651), 10/06/2026, pct 150 | Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 145386), 10/30/2026 |
| El Paso | 1096333268 | Received_Ballots_Voter_List_20261104113257.csv (line 5097), 10/20/2026, pct 160 | Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 64898), 10/30/2026 |
| El Paso | 1096656403 | Received_Ballots_Voter_List_20261104113257.csv (line 3529), 10/13/2026, pct 55 | Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 150318), 10/30/2026 |
| El Paso | 1097760377 | Received_Ballots_Voter_List_20261104113257.csv (line 3578), 10/13/2026, pct 115 | Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 121121), 10/30/2026 |
| El Paso | 1098032793 | Received_Ballots_Voter_List_20261104113257.csv (line 3590), 10/13/2026, pct 232 | Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 56200), 10/30/2026 |
| El Paso | 2152866163 | Received_Ballots_Voter_List_20261104113257.csv (line 5356), 10/20/2026, pct 133 | Early Voting 10-30-2026_Voter_List_(Ballot_Address_Day12.xlsx (sheet Sheet1 row 66630), 10/30/2026 |
| Galveston | 1198506989 | November-3rd-2026-Ballots-received-10.30.26-1.csv (line 107), 10/30/2026, pct Precinct 159 | Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 10063), 10/30/2026 |
| Grayson | 1025213073 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1 row 283), 10/05/2026, pct 303.06 | October 27 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 408), 10/27/2026 |
| Grayson | 1025394327 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1 row 65), 10/02/2026, pct 409.01 | October 20 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1491), 10/20/2026 |
| Grayson | 2025562322 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1 row 870), 10/14/2026, pct 404.08 | October 29 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 954), 10/29/2026 |
| Jefferson | 1121803988 | JeffersonCountyMailBallotsReceived01012026to10302026.xls (sheet FPCA-ABBM-ACTIVITY-REPORT_13233 row 716), 10/30/2026 | JeffersonCountyInPersonVoting10202026.xls (sheet JeffersonCountyInPersonVoting10 row 2142), 10/20/2026 |
| Jefferson | 1123165075 | JeffersonCountyMailBallotsReceived01012026to10302026.xls (sheet FPCA-ABBM-ACTIVITY-REPORT_13233 row 1884), 10/30/2026 | JeffersonCountyInPerson10302026.xls (sheet JeffersonCountyInPerson10302026 row 5586), 10/30/2026 |
| Jefferson | 1123862974 | JeffersonCountyMailBallotsReceived01012026to10302026.xls (sheet FPCA-ABBM-ACTIVITY-REPORT_13233 row 2099), 10/30/2026 | JeffersonCountyInPersonVoting10292026.xls (sheet JeffersonCountyInPersonVoting10 row 3954), 10/29/2026 |
| Jefferson | 2209104885 | JeffersonCountyMailBallotsReceived01012026to10302026.xls (sheet FPCA-ABBM-ACTIVITY-REPORT_13233 row 2203), 10/30/2026 | JeffersonCountyInPersonVoting10292026.xls (sheet JeffersonCountyInPersonVoting10 row 4120), 10/29/2026 |
| Tarrant | 1044613243 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 19438), 11/02/2026, pct 4508 | Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 523426) |
| Tarrant | 1044915588 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 15358), 10/21/2026, pct 2034 | Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 124964) |
| Tarrant | 1045645239 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 22885), 11/02/2026, pct 4261 | Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 492748) |
| Tarrant | 1047338883 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 3297), 10/31/2026, pct 3214 | Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 305052) |
| Tarrant | 1050956866 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 6791), 10/22/2026, pct 1277 | Early_voting_in_person_report.zip (downloaded 2026-10-31) > Early_voting_in_person_report.txt (line 42938) |
| Tom Green | 1129510084 | 110226BallotsByMail.pdf (page 27), 09/30/2026 | 103026roster.pdf (page 410), 10/27/2026 |
| Tom Green | 2207275739 | 110226BallotsByMail.pdf (page 30), 10/05/2026 | 103026roster.pdf (page 461), 10/27/2026 |
| Travis | 1133310623 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.07.2026 Ballot by Mail.xlsx (sheet 10.07.2026 Ballot by Mail row 234), 10/07/2026, pct 270 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.21.2026 Early Vote.xlsx (sheet EarlyVote row 8017), 10/21/2026 |
| Travis | 3383337140 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.29.2026 Ballot by Mail.xlsx (sheet 10.29.2026 Ballot by Mail row 647), 10/29/2026, pct 193 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.28.2026 Early Vote.xlsx (sheet EarlyVote row 31360), 10/28/2026 |

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
| Angelina | 1029469206 | early_voting_history_103026.pdf (page 3) | 10/30/2026 |
| Angelina | 1031777800 | early_voting_history_103026.pdf (page 9) | 10/30/2026 |
| Angelina | 1031899219 | early_voting_history_103026.pdf (page 13) | 10/30/2026 |
| Angelina | 1174976155 | early_voting_history_103026.pdf (page 29) | 10/30/2026 |
| Angelina | 1186526934 | early_voting_history_103026.pdf (page 31) | 10/30/2026 |
| Angelina | 1196737811 | early_voting_history_103026.pdf (page 32) | 10/30/2026 |
| Angelina | 2133832611 | early_voting_history_103026.pdf (page 38) | 10/30/2026 |
| Angelina | 2138292730 | early_voting_history_103026.pdf (page 39) | 10/30/2026 |
| Angelina | 2138639824 | early_voting_history_103026.pdf (page 39) | 10/30/2026 |
| Angelina | 2171739605 | early_voting_history_103026.pdf (page 43) | 10/30/2026 |
| Angelina | 2176563156 | early_voting_history_103026.pdf (page 43) | 10/30/2026 |
| Angelina | 2191868578 | early_voting_history_103026.pdf (page 45) | 10/30/2026 |
| Bexar | 1140208390 | EV_VoterCheckins_10202026.pdf (page 718) | 10/20/2026 |
| Bexar | 2222687508 | EV_VoterCheckins_10202026.pdf (page 152) | 10/20/2026 |
| Bexar | 1069614030 | EV_VoterCheckins_10212026.pdf (page 721) | 10/21/2026 |
| Bexar | 1150529579 | EV_VoterCheckins_10212026.pdf (page 25) | 10/21/2026 |
| Bexar | 1168562626 | EV_VoterCheckins_10212026.pdf (page 71) | 10/21/2026 |
| Bexar | 2120288965 | EV_VoterCheckins_10212026.pdf (page 255) | 10/21/2026 |
| Bexar | 1064001864 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 2292) | 11/03/2026 |
| Bexar | 1065348794 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 2446) | 11/03/2026 |
| Bexar | 1070297501 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 939) | 11/03/2026 |
| Bexar | 1071611824 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 2625) | 11/03/2026 |
| Bexar | 2109936278 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 127) | 11/03/2026 |
| Bexar | 2134547980 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 1901; page 1902) | 11/03/2026 |
| Bexar | 2145231758 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 2808) | 11/03/2026 |
| Bexar | 2177456929 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 1189) | 11/03/2026 |
| Bexar | 2183653978 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 246) | 11/03/2026 |
| Bexar | 3383102399 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 2600) | 11/03/2026 |
| Brazoria | 1016426864 | 2026-10-29 IN PERSON.xlsx (sheet 2026-10-29 ROSTER row 5588; sheet 2026-10-29 ROSTER row 5589) | 10/29/2026 |
| Brazoria | 1106252870 | 2026-10-30 IN PERSON.xlsx (sheet 2026-10-30 ROSTER row 7613; sheet 2026-10-30 ROSTER row 7614) | 10/30/2026 |
| Collin | 1076603030 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 97319; sheet Detailed Check-in Report - Orde row 97320) | 10/19/2026 |
| Collin | 1201013964 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 131334; sheet Detailed Check-in Report - Orde row 131335) | 10/19/2026 |
| Collin | 1175567020 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 75405; sheet Detailed Check-in Report - Orde row 75406) | 10/20/2026 |
| Collin | 2190293340 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 126436; sheet Detailed Check-in Report - Orde row 126437) | 10/27/2026 |
| Collin | 1076954370 | election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 8240; sheet Detailed Check-in Report - Orde row 8241) | 11/03/2026 |
| Collin | 1174426272 | election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 46767; sheet Detailed Check-in Report - Orde row 46768) | 11/03/2026 |
| Collin | 2148450548 | election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 63151; sheet Detailed Check-in Report - Orde row 63152) | 11/03/2026 |
| Collin | 2190834567 | election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 73608; sheet Detailed Check-in Report - Orde row 73609) | 11/03/2026 |
| Dallas | 3383295764 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 379060; line 397091) | 10/19/2026 |
| Dallas | 2170886201 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 167299; line 235033) | 10/22/2026 |
| Dallas | 2170970526 | In-Person-Early-Voting-Details-Report_10.30.26.csv (line 382501; line 410991) | 10/23/2026 |
| Galveston | 1185946285 | Checkins_10192026.xlsx (sheet Checkins_10192026 row 13286; sheet Checkins_10192026 row 13288) | 10/19/2026 |
| Galveston | 1218946561 | Checkins_10302026.xlsx (sheet voter_check_in_details - 2026-1 row 599; sheet voter_check_in_details - 2026-1 row 600) | 10/30/2026 |
| Grayson | 1024897712 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3415; sheet Detailed Check-in Report - Orde row 3422) | 10/19/2026 |
| Grayson | 1024922211 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3575; sheet Detailed Check-in Report - Orde row 3591) | 10/19/2026 |
| Grayson | 1042130958 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3480; sheet Detailed Check-in Report - Orde row 3491) | 10/19/2026 |
| Grayson | 1076365904 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3327; sheet Detailed Check-in Report - Orde row 3336) | 10/19/2026 |
| Grayson | 1090251506 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3310; sheet Detailed Check-in Report - Orde row 3316) | 10/19/2026 |
| Grayson | 1025261772 | October 20 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 4517; sheet Detailed Check-in Report - Orde row 4522) | 10/20/2026 |
| Grayson | 1025207956 | October 22 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 656; sheet Detailed Check-in Report - Orde row 667) | 10/22/2026 |
| Grayson | 1089592991 | October 22 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 483; sheet Detailed Check-in Report - Orde row 487) | 10/22/2026 |
| Grayson | 1086311908 | November 3 2026 ED Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3906; sheet Detailed Check-in Report - Orde row 4316) | 11/03/2026 |
| Grayson | 1150101461 | November 3 2026 ED Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 2836; sheet Detailed Check-in Report - Orde row 2862) | 11/03/2026 |
| Guadalupe | 1101950568 | Early Voting in Person Roster_10-19-2026.pdf (page 58) | 10/19/2026 |
| Guadalupe | 1221702819 | Early Voting in Person Roster_10-19-2026.pdf (page 89) | 10/19/2026 |
| Guadalupe | 1101632395 | Early Voting in Person Roster_10-20-2026.pdf (page 50) | 10/20/2026 |
| Guadalupe | 2197563557 | Early Voting in Person Roster_10-22-2026.pdf (page 26) | 10/22/2026 |
| Henderson | 1007113440 | Early Voting Roster October 30, 2026.pdf (page 12) | 10/30/2026 |
| Jefferson | 2183895285 | JeffersonCountyInPersonVoting10192026.xls (sheet ALL row 2671; sheet ALL row 2672) | 10/19/2026 |
| Jefferson | 1123342941 | JeffersonCountyInPersonVoting10212026.xls (sheet JeffersonCountyInPersonVoting10 row 1189; sheet JeffersonCountyInPersonVoting10 row 1190) | 10/21/2026 |
| Jefferson | 1185749681 | JeffersonCountyInPersonVoting10232026.xls (sheet JeffersonCountyInPersonVoting10 row 3386; sheet JeffersonCountyInPersonVoting10 row 3387) | 10/23/2026 |
| Jefferson | 1123165994 | JeffersonCountyInPersonVoting10292026.xls (sheet JeffersonCountyInPersonVoting10 row 2086; sheet JeffersonCountyInPersonVoting10 row 2087) | 10/29/2026 |
| Jefferson | 1121849035 | JeffersonCountyInPersonVoting11032026.xls (sheet JeffersonCountyInPersonVoting11 row 10852; sheet JeffersonCountyInPersonVoting11 row 10853) | 11/03/2026 |
| Jefferson | 1122687987 | JeffersonCountyInPersonVoting11032026.xls (sheet JeffersonCountyInPersonVoting11 row 1561; sheet JeffersonCountyInPersonVoting11 row 1562) | 11/03/2026 |
| Jefferson | 1122775890 | JeffersonCountyInPersonVoting11032026.xls (sheet JeffersonCountyInPersonVoting11 row 1197; sheet JeffersonCountyInPersonVoting11 row 1198) | 11/03/2026 |
| Jefferson | 1122815755 | JeffersonCountyInPersonVoting11032026.xls (sheet JeffersonCountyInPersonVoting11 row 1191; sheet JeffersonCountyInPersonVoting11 row 1192) | 11/03/2026 |
| Jefferson | 1200585804 | JeffersonCountyInPersonVoting11032026.xls (sheet JeffersonCountyInPersonVoting11 row 2693; sheet JeffersonCountyInPersonVoting11 row 2694) | 11/03/2026 |
| Jefferson | 2149422456 | JeffersonCountyInPersonVoting11032026.xls (sheet JeffersonCountyInPersonVoting11 row 10433; sheet JeffersonCountyInPersonVoting11 row 10434) | 11/03/2026 |
| Jefferson | 2196053011 | JeffersonCountyInPersonVoting11032026.xls (sheet JeffersonCountyInPersonVoting11 row 313; sheet JeffersonCountyInPersonVoting11 row 314) | 11/03/2026 |
| Potter | 1182794631 | Election Day Roster.pdf (downloaded 2026-11-04) (page 56) |  |
| Tom Green | 1052505413 | 103026roster.pdf (page 362) | 10/20/2026 |
| Tom Green | 1048893092 | 103026roster.pdf (page 375) | 10/27/2026 |
| Travis | 1130790938 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.19.2026 Early Vote.xlsx (sheet EarlyVote row 27271; sheet EarlyVote row 27273) | 10/19/2026 |
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
| Brazoria | 2188722197 | 2026-11-03--MAIL LIST GENERAL.xlsx (sheet 11.03.26 MAIL LIST GENERAL row 44), 10/01/2026 | 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (downloaded 2026-10-29) (sheet BBM ROSTER-9.28-10.16.26 row 753), 10/05/2026 |
| Brazoria | 1150959145 | 2026-11-03--MAIL LIST GENERAL.xlsx (sheet 11.03.26 MAIL LIST GENERAL row 58), 10/30/2026 | 2026-11-02--MAIL LIST GENERAL.xlsx (sheet 11.02.26 MAIL LIST GENERAL row 120), 11/02/2026 |
| Brazoria | 1185869412 | 2026-10-30-MAIL LIST GENERAL.xlsx (downloaded 2026-11-04) (sheet 10.30.26 MAIL LIST GENERAL row 102), 10/30/2026 | 2026-11-02--MAIL LIST GENERAL.xlsx (sheet 11.02.26 MAIL LIST GENERAL row 145), 11/02/2026 |
| Brazos | 2180077579 | mail-10-09-2026-accessible.pdf (page 1), 10/09/2026 | mail-10-13-2026-accessible.pdf (page 4), 10/13/2026 |
| Galveston | 1032377676 | Nov-3-2026-Ballots-Received-Oct-13.csv (line 450), 10/13/2026 | Nov-3rd-2026-Ballots-Received-10.14.26.csv (line 28), 10/14/2026 |
| Grayson | 1025070714 | October 29 2026 Absentee Voter List.xlsx (sheet Sheet1 row 5), 10/29/2026 | October 30 2026 Absentee Voter List.xlsx (sheet Sheet1 row 16), 10/30/2026 |
| Grayson | 1078849825 | October 29 2026 Absentee Voter List.xlsx (sheet Sheet1 row 9), 10/29/2026 | October 30 2026 Absentee Voter List.xlsx (sheet Sheet1 row 27), 10/30/2026 |
| Gregg | 1034151785 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 17), 10/16/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 2), 10/21/2026 |
| Gregg | 2132128292 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 14), 10/19/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 3), 10/20/2026 |
| Harris | 2221343697 | Cumulative_BBM_ToPost_1126.zip (downloaded 2026-11-04) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19283), 09/24/2026 | Cumulative_BBM_ToPost_1126.zip (downloaded 2026-11-04) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19283), 10/14/2026 |
| Hunt | 1022716555 | Vote History 10.24.2026.pdf (page 45), 10/24/2026 | Vote History 10.30.2026.pdf (page 65), 10/30/2026 |
| Hunt | 1015418392 | Vote History 10.27.2026.pdf (page 43), 10/27/2026 | Vote History 10.28.2026.pdf (page 40), 10/28/2026 |
| Montgomery | 1100132244 | GEN110326_10.02.2026.zip > GEN110326_10.02.2026.csv (line 184; line 185), 10/02/2026 | GEN110326_10.22.2026.zip > GEN110326_10.22.2026.csv (line 40502; line 40503), 10/22/2026 |
| Montgomery | 1126476176 | GEN110326_10.05.2026.zip > GEN110326_10.05.2026.csv (line 498; line 499; line 500), 10/05/2026 | GEN110326_10.24.2026.zip > GEN110326_10.24.2026.csv (line 44163; line 44164; line 44165), 10/24/2026 |
| Montgomery | 1119403485 | GEN110326_10.08.2026.zip > GEN110326_10.08.2026.csv (line 176; line 177; line 178; line 179), 10/08/2026 | GEN110326_10.26.2026.zip > GEN110326_10.26.2026.csv (line 49917; line 49918; line 49919; line 49920), 10/26/2026 |
| Tom Green | 2208208124 | 110226BallotsByMail.pdf (page 5), 10/26/2026 | 110226BallotsByMail.pdf (page 5), 11/02/2026 |
| Tom Green | 1130084141 | 110226BallotsByMail.pdf (page 12), 10/30/2026 | 110226BallotsByMail.pdf (page 12), 11/02/2026 |
| Travis | 1132888023 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 09.30.2026 Ballot by Mail.xlsx (sheet 10.01.2026 Ballot by Mail row 16), 09/30/2026 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 999), 10/22/2026 |
| Travis | 1140151036 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.05.2026 Ballot by Mail_UPDATED_2.xlsx (sheet 10.05.2026 Ballot by Mail row 107), 10/05/2026 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.19.2026 Ballot by Mail.xlsx (sheet 10.19.2026 Ballot by Mail row 2110), 10/19/2026 |
| Travis | 3383285286 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.07.2026 Ballot by Mail.xlsx (sheet 10.07.2026 Ballot by Mail row 906), 10/07/2026 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 1085), 10/22/2026 |
| Travis | 1139039837 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.12.2026 Ballot by Mail.xlsx (sheet 10.12.2026 Ballot by Mail row 12), 10/12/2026 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.13.2026 Ballot by Mail.xlsx (sheet 10.13.2026 Ballot by Mail row 376), 10/13/2026 |
| Travis | 1140972395 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.16.2026 Ballot by Mail.xlsx (sheet 10.16.2026 Ballot by Mail row 7), 10/16/2026 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 1114), 10/22/2026 |
| Travis | 2129048624 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.26.2026 Ballot by Mail_UPDATED.xlsx (sheet 10.26.2026 Ballot by Mail row 29), 10/26/2026 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.27.2026 Ballot by Mail.xlsx (sheet 10.27.2026 Ballot by Mail row 19), 10/27/2026 |

## 7. Identifiers that are not state VUIDs (§6.11)

All 2306 records are in the CSV. For 34 of them, the state's file for the same county lists one voter with the same last name, first name and precinct under a 10-digit VUID; these are inferences to be confirmed, not matches by ID:

| County | County identifier | County file and location | Possible state VUID |
|---|---|---|---|
| Bexar | 216104818 | EV_VoterCheckins_10232026.pdf (page 49), in person 10/23/2026, pct S 4056.01 | 2161048181 |
| Bexar | 221423660 | EV_VoterCheckins_10212026.pdf (page 195), in person 10/21/2026, pct S 1105.01 | 2214238660 |
| Bexar | 383323270 | EV_VoterCheckins_10242026.pdf (page 625), in person 10/24/2026, pct S 2028.01 | 3383323270 |
| Collin | 1683613 | election-day-voters-november-3-2026.xlsx (sheet Detailed Check-in Report - Orde row 41794), provisional 11/03/2026, pct 22 | 1139260270 |
| Collin | 330806 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 168276), in person 10/23/2026, pct 75 | 2153628740 |
| Collin | 4157442 | absentee-returns-through-november-3-2026.xlsx (sheet Received Ballots Voter List_110 row 7518), mail 10/27/2026, pct 25.02 | 2148708758 |
| Collin | 4168640 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 38217), in person 10/22/2026, pct 94 | 1076312029 |
| Collin | 4203312 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 203832), in person 10/29/2026, pct 22 | 3382649972 |
| Collin | 461558 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 47737), provisional 10/30/2026, pct 30 | 3383588682 |
| Collin | 472987 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 99490), in person 10/24/2026, pct 69 | 1075716466 |
| Collin | 585248 | early-voting-through-october-30-2026.xlsx (sheet Detailed Check-in Report - Orde row 283324), in person 10/27/2026, pct 138 | 3383598994 |
| El Paso | 1482939 | Received_Ballots_Voter_List_20261104113257.csv (line 5939), mail 10/22/2026, pct 8.1 | 1095318907 |
| Grayson | 116404233 | October 20 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 299), in person 10/20/2026, pct 109 | 1090375242 |
| Grayson | 168949 | October 25 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 706), in person 10/25/2026, pct 302 | 1133330857 |
| Grayson | 169043 | October 24 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 976), in person 10/24/2026, pct 406 | 2146465425 |
| Grayson | 169077 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1904), in person 10/19/2026, pct 108 | 2156111295 |
| Grayson | 169246 | October 22 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1825), in person 10/22/2026, pct 408 | 2121250168 |
| Grayson | 170955 | October 21 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 4015), in person 10/21/2026, pct 412 | 1090100033 |
| Gregg | 2017474 | 2026-10-28-Voter-Turnout.pdf (page 30), in person 10/28/2026, pct 02 | 1012858255 |
| Henderson | 10229402149 | Early Voting Roster October 30, 2026.pdf (page 53), in person 10/30/2026, pct 4LP | 1029402149 |
| Hidalgo | 10451891 | EV Roster November 3, 2026 (Cumulative).csv (downloaded 2026-10-31) (line 50759), in person, pct 2 | 2151999073 |
| Hidalgo | 61100048 | ABBM LIST November 3, 2026 (Cumulative)_202611040820489518.xlsx (sheet Received Ballots Voter List_Nov row 3970), mail, pct 98.01 | 1154845862 |
| Hidalgo | 61108735 | ABBM LIST November 3, 2026 (Cumulative)_202611040820489518.xlsx (sheet Received Ballots Voter List_Nov row 242), mail, pct 135.01 | 2141425996 |
| Smith | 2436166 | 10.25.26 In Person Voter List.pdf (page 53), in person 10/25/2026, pct 412 | 1027280545 |
| Smith | 2439110 | 10.19.26 In Person Voter List.pdf (page 123), in person 10/19/2026, pct 451 | 1129017692 |
| Tarrant | 41737473 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 18599), mail 10/29/2026, pct 2423 | 2222245072 |
| Tarrant | 41741013 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 18637), mail 10/22/2026, pct 4053 | 3383117753 |
| Tarrant | 41741720 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 18665), mail 10/20/2026, pct 4116 | 2202167573 |
| Tarrant | 41748593 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 18840), mail 10/24/2026, pct 2235 | 1175768732 |
| Tarrant | 41765735 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 18983), mail 10/20/2026, pct 3035 | 1082322527 |
| Tarrant | 41768481 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 19033), mail 10/16/2026, pct 3176 | 3383497931 |
| Tarrant | 41772205 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 19096), mail 10/23/2026, pct 2055 | 3383532539 |
| Travis | 220260776 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.29.2026 Ballot by Mail.xlsx (sheet 10.29.2026 Ballot by Mail row 226), mail 10/29/2026, pct 385 | 2202607776 |
| Webb | 91021177 | Check-in Report - Ordered by Check-in Time 10-25-2026.pdf (page 21), in person 10/25/2026, pct 458 | 2222897948 |

## 8. Election Day vote and an earlier vote in the same county (§6.12)

| County | VUID | Election Day record | Earlier record | State's files |
|---|---|---|---|---|
| Bexar | 1065979485 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 690), pct S 4121.04 | EV_VoterCheckins_10282028.pdf (page 463), in person, 10/28/2026 | early in person (SOS BEXAR ... 10_28_2026.csv (line 28683)) |
| Bexar | 1069292502 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 282), pct S 2049.01 | Early Vote Ballots - 2026 - 10 - 29 November 3, 2026 General Election.pdf (page 121), in person, 10/29/2026 | early in person (SOS BEXAR ... 10_29_2026.csv (line 41129)) |
| Bexar | 2174481639 | Election Day Voters - 2026-11-03 - November 3, 2026 General Election.pdf (page 1413), pct P 2065 | EV_VoterCheckins_10252026.pdf (page 80), in person, 10/25/2026 | early in person (SOS BEXAR ... 10_25_2026.csv (line 4353)) |
| Galveston | 3383489998 | Checkins_11032026.xlsx (sheet voter_check_in_details - 2026-1 row 22185), pct 216 | Checkins_10192026.xlsx (sheet Checkins_10192026 row 12692), in person, 10/19/2026 | early in person (SOS GALVESTON ... 10_19_2026.csv (line 12729)) |
| Grayson | 1025016563 | November 3 2026 ED Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 3632), pct 102 | October 29 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 830), in person, 10/29/2026 | early in person (SOS GRAYSON ... 10_29_2026.csv (line 1585)) |
| Grayson | 1141965270 | November 3 2026 ED Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 10966), pct 304 | October 23 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1322), in person, 10/23/2026 | early in person (SOS GRAYSON ... 10_23_2026.csv (line 2484)) |
| Grayson | 2161115606 | November 3 2026 ED Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 4351), pct 206 | October 26 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 2638), in person, 10/26/2026 | early in person (SOS GRAYSON ... 10_26_2026.csv (line 1072)) |
| Brazos | 2168283236 | in-person-11-03-2026-accessible.pdf (page 292), pct 27 | mail-10-09-2026-accessible.pdf (page 1), mail, 10/09/2026 | Election Day (SOS BRAZOS ...ElectionDay... 11_03_2026.zip > 21_BRAZOS_VOTER_INFO.csv (line 12388)) |
| Dallas | 1079979970 | Election-Day-Website-Roster.csv (line 39837), pct 3807 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 21611), mail, 11/03/2026 | neither |
| Dallas | 1083052703 | Election-Day-Website-Roster.csv (line 133030), pct 2509 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 7665), mail, 10/24/2026 | neither |
| Dallas | 1180270766 | Election-Day-Website-Roster.csv (line 5049), pct 3982 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 6534), mail, 10/25/2026 | neither |
| Dallas | 2126016598 | Election-Day-Website-Roster.csv (line 73684), pct 3805 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 5987), mail, 10/24/2026 | neither |
| Dallas | 2213568232 | Election-Day-Website-Roster.csv (line 29743), pct 3966 | Mail-Ballot-Returns-Report_11.03.26.xlsx (sheet Early Voter Roster row 21610), mail, 10/30/2026 | mail (SOS DALLAS ... 10_30_2026.csv (line 35748)) |
| Tarrant | 1174356419 | Election_Day_in_person_report.zip (downloaded 2026-11-04) > Election_day_in_person_report_Redacted.txt (line 13488), pct 1302 | absentee_returned_voter_report.zip (downloaded 2026-11-04) > absentee_returned_voter_report.txt (line 9986), mail, 11/02/2026 | neither |
| Travis | 2167201303 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 11.03.2026 Election Day.xlsx (sheet ElectionDay row 90150), pct 126 | G26-Voter-Rosters.zip (downloaded 2026-11-04) > 10.19.2026 Ballot by Mail.xlsx (sheet 10.19.2026 Ballot by Mail row 2228), mail, 10/19/2026 | neither |
| Webb | 1134050575 | Check-in Report - Ordered by Check-in Time 11-03-2026.pdf (page 262), pct 218 | 10-27-2026 - Mail Ballot Received (PDF).pdf (page 1), mail, 10/27/2026 | Election Day (SOS WEBB ...ElectionDay... 11_03_2026.zip > 240_WEBB_VOTER_INFO.csv (line 301)) |
| Webb | 1151030708 | Check-in Report - Ordered by Check-in Time 11-03-2026.pdf (page 219), pct 308 | 10-15-2026 - Mail Ballot Received (PDF).pdf (page 3), mail, 10/15/2026 | Election Day (SOS WEBB ...ElectionDay... 11_03_2026.zip > 240_WEBB_VOTER_INFO.csv (line 5553)) |
