# Texas Early Voting Rosters: Record Locator Supplement

**Supplements:** Texas Early Voting Rosters: County and State Data Review, October 27, 2026\
**Election:** November 3, 2026 General Election (SOS election 53815)\
**Data through:** Monday, October 26, 2026; county files as posted by 3:09 PM October 27 (Guadalupe's and Wichita's by 9:39 PM), state files as posted by 11:16 PM October 27\
**For:** county election offices and the Secretary of State's Elections Division

This supplement lists the voter records behind the anomalies in sections 6.1 and 6.4-6.11 of the October 27 review, so that county and state staff can find each record in their own files. Section 6.2 (voters whose identities Tarrant County redacts) is not included. It replaces the October 26 supplement: the lists below cover October 14-26, the current versions of each county's files, and the state's files for every day as re-downloaded on the evening of October 27 (the state revised many earlier days, so state line numbers differ from the October 26 supplement). Guadalupe's in-person rosters are included for the first time.

Each record is identified by its VUID (or, in §6.11, the county's own identifier) and by where it appears: the file name as the county or the Secretary of State posted it, the entry inside a ZIP, and the line, worksheet row or PDF page. Voter names and addresses are not reproduced: the VUID and location identify each record. All identifiers come from rosters published under Texas Election Code §87.121.

The complete list, 18,819 records, is in the companion file **Texas_Early_Voting_Roster_Review_2026-10-27_Supplement_Records.csv**. This document summarizes it and lists the smaller sections in full.

## 1. How to read the locations

- **County files** are named as posted on the county's website. A county that posts a file under the same name every day is shown with the date the copy was downloaded, for example `Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-27)`. Travis County's daily workbooks are entries in its ZIP `G26-Voter-Rosters.zip`; Harris, Tarrant and Montgomery files are also ZIP entries. A location is written `file > entry (location)`.
- **State files** are named as the SOS portal's CSV Download names them: `<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv` (Election Information & Turnout Data, early voting date, county Voter Details Report CSV). "..." in the tables below stands for ".2026 NOVEMBER GENERAL ELECTION.EarlyVoting.".
- **Line** numbers count the header as line 1. **Sheet ... row** is the worksheet name (as stored in the file, which may be cut short) and the Excel row number; this includes Jefferson's Excel 97-2003 (.xls) files. **Page** is the PDF page, counting from 1. Where a VUID appears on several lines or pages of one file, all are listed.
- **Dates** are the voting or received date in the county's file, and for state records the date of the state's file (which for mail ballots is when the state recorded the ballot, not when the county received it). Tarrant's, Hidalgo's and Potter's in-person lists are cumulative and carry no voting date; El Paso's, Denton's and Jefferson's lists show the list's date.
- The files were downloaded October 20-27, 2026. Counties replace some files daily (Collin, Williamson, Cameron, Hidalgo, Victoria, Potter, Randall, Tom Green, McLennan, Jefferson's mail list) and the state revises its files, so a later copy of a file may differ in line numbers. Angelina is located in its cumulative October 25 file (October 19-25) and its October 26 file.

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
| 6.1 | SOS county file lists a voter who is on another county's roster (same first name, that county's precinct) | 1,778: Travis 191, Dallas 167, Denton 163, Bexar 157, Collin 99, Hays 84, Johnson 75, Tarrant 67, Harris 66, Brazos 64, Parker 57, and 1-45 in 38 more SOS county files |
| 6.4 | SOS mail ballot not on the county's own mail list (Tarrant excluded: §6.2) | 3,695: Dallas 1,172, Collin 800, Hunt 323, Harris 200, Travis 191, Wichita 153, Bexar 140, Montgomery 126, Bell 123, Denton 114, and 1-62 in 17 more counties |
| 6.5 | Precinct differs between the county's and the state's record of the same VUID | 11,317: Bowie 4,718, Williamson 4,690, Harris 1,274, Tarrant 188, Galveston 163, El Paso 103, Bexar 68, Smith 61, Hunt 14, Montgomery 13, and 1-6 in 8 more counties |
| 6.6 | Surname differs between the county and the state | 528: Collin 72, Tarrant 48, Bexar 44, Denton 42, Cameron 36, Dallas 34, Williamson 31, Brazoria 24, Galveston 20, Hidalgo 16, and 1-16 in 23 more counties |
| 6.7 | One VUID, two different people | 41 records, 39 VUIDs (§3) |
| 6.8 | Same voter on two counties' own rosters | 9 (§4) |
| 6.9 | Mail ballot and in-person vote by the same voter | 18 (§5) |
| 6.10 | Same-day duplicate check-in; mail ballot received on two days | 163 + 31 (§6) |
| 6.11 | Identifier that is not a 10-digit state VUID | 1,239: Tarrant 420, Collin 311, Denton 159, El Paso 69, Grayson 62, Guadalupe 59, Hidalgo 53, Smith 31, Webb 31, Gregg 14, Jefferson 10, Bexar 8, Bowie 8, Tom Green 3, Henderson 1 (§7) |

Notes on the larger sections:

- **6.4 Dallas:** 927 of the 1,172 are dated October 26 by the state; Dallas's October 26 mail list holds 119 ballots received that day. **6.4 Collin:** 710 are dated October 26 and 71 October 25 by the state; the county's list "through October 26" ends with ballots received October 25. **6.4 Hunt:** most of the 323 are dated October 14-19 by the state. **6.4 Travis:** 97 of the 191 are dated October 26, 56 October 19 and 24 October 22 by the state. **6.4 timing:** 710 Collin, 55 Victoria and 35 Tom Green records are dated after the county's latest mail ballot (marked in the note column).
- **6.5 Williamson:** 4,689 of the 4,690 are mail voters in the county's October 26 file (`VoterTurnout_261026_202610262051148753.xlsx`); the county's mail precincts have differed from the state's since its October 22 file, while its October 21 file matched. One in-person record also differs.
- **6.5 Bowie:** all 4,718 are in the October 20, 21 and 23 Early Vote History reports (1,566, 1,711 and 1,441); the October 22 and 24-26 reports match the state. **6.5 Bexar:** 65 of the 68 are in `EV_VoterCheckins_10222026.pdf`. **6.5 Harris:** 1,231 of the 1,274 are in-person records. **6.5 Galveston:** 111 of the 163 are in `Checkins_10192026.xlsx`.
- **6.6:** in all but 18 records the county and state names share a word, which points to name changes or formatting rather than different people.
- **6.10 Angelina:** all 132 are in Angelina's early voting history files posted October 27, which list these voters two or three times on the same day with the same precinct.

## 3. One VUID, two different people (§6.7)

| VUID | Case | Record 1 | Record 2 |
|---|---|---|---|
| 3382565201 | Two people under one VUID in Bexar's files | Bexar: voter_check_in_details 10-19-2026.pdf (page 636), in person, 10/19/2026, pct P 3149 | Bexar: EV_VoterCheckins_10232026.pdf (page 76), in person, 10/23/2026, pct S 2019.01 |
| 1019649503 | Two people under one VUID in Bowie's files | Bowie: BBM Received 10.22.2026_1.pdf (page 1), mail, 10/22/2026 | Bowie: BBM Received 10.24.2026.pdf (page 1), mail, 10/24/2026 |
| 2221343697 | Two people under one VUID in Harris's files | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19275), mail, 09/24/2026, pct 0139 | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19275), mail, 10/14/2026, pct 0505 |
| 2223366061 | Two counties' rosters, two different people | Bell: 20261021_BBM.pdf (page 9), mail, 10/21/2026, pct 201 | Randall: Mail Ballot Roster 10.26.26_202610270751076226.pdf (page 34), mail, 10/21/2026 |
| 2211542442 | SOS file vs another county's roster | SOS BELL ... 10_22_2026.csv (line 130) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_EV_ToPost_1126_10-22-26.csv (line 29341), in person, 10/22/2026, pct 0347 |
| 1075809879 | SOS file vs another county's roster | SOS COLLIN ... 10_23_2026.csv (line 676) | Dallas: In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 11005), in person, 10/23/2026, pct 1059 |
| 2223366026 | SOS file vs another county's roster | SOS CORYELL ... 10_24_2026.csv (line 500) | Bell: 20261024_BBM.pdf (page 4), mail, 10/24/2026, pct 102 |
| 2223366042 | SOS file vs another county's roster | SOS CORYELL ... 10_19_2026.csv (line 1063) | Bell: 20261019_EV.pdf (page 3), in person, 10/19/2026, pct 104 |
| 2223366057 | SOS file vs another county's roster | SOS CORYELL ... 10_26_2026.csv (line 672) | Bell: 20261026_BBM.pdf (page 4), mail, 10/26/2026, pct 201 |
| 2223368590 | SOS file vs another county's roster | SOS DALLAS ... 10_24_2026.csv (line 34340) | Denton: 1126_10-26_Returned_Mail_Roster.xlsx (sheet Sheet1 row 3507), mail, 10/24/2026, pct 3128 |
| 2223368629 | SOS file vs another county's roster | SOS DALLAS ... 10_24_2026.csv (line 18760) | Denton: 1126_10-26_Early-Voting-Roster.xlsx (sheet Sheet1 row 216105), in person, 10/24/2026, pct 103201 |
| 2223375616 | SOS file vs another county's roster | SOS FORT BEND ... 10_19_2026.csv (line 17560) | Bell: 20261019_BBM.pdf (page 30), mail, 10/19/2026, pct 101 |
| 2223375713 | SOS file vs another county's roster | SOS FORT BEND ... 10_20_2026.csv (line 26845) | Bell: 20261020_EV.pdf (page 70), in person, 10/20/2026, pct 105 |
| 1015936692 | SOS file vs another county's roster | SOS HARRIS ... 10_21_2026.csv (line 57295) | El Paso: Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 23350), in person, 10/26/2026, pct 052 |
| 2223365544 | SOS file vs another county's roster | SOS HIDALGO ... 10_14_2026.csv (line 9) | Tarrant: absentee_returned_voter_report.zip (downloaded 2026-10-27) > absentee_returned_voter_report.txt (line 14421), mail, 10/14/2026, pct 2636 |
| 2223382143 | SOS file vs another county's roster | SOS HIDALGO ... 10_19_2026.csv (line 16203) | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_BBM_ToPost_1126.csv (line 23889), mail, 10/19/2026, pct 0771 |
| 2223366100 | SOS file vs another county's roster | SOS LUBBOCK ... 10_22_2026.csv (line 3147) | Bell: 20261022_BBM.pdf (page 1), mail, 10/22/2026, pct 311 |
| 2223378210 | SOS file vs another county's roster | SOS MONTGOMERY ... 10_26_2026.csv (line 8527) | McLennan: Website List_202610262022325993.pdf (page 696), , pct 59 |
| 2223378724 | SOS file vs another county's roster | SOS MONTGOMERY ... 10_20_2026.csv (line 11800) | El Paso: Received_Ballots_Voter_List_10.27.csv (line 5314), mail, 10/20/2026, pct 29.1 |
| 2223366061 | SOS file vs another county's roster | SOS RANDALL ... 10_21_2026.csv (line 3047) | Bell: 20261021_BBM.pdf (page 9), mail, 10/21/2026, pct 201 |
| 3382917068 | SOS file vs another county's roster | SOS TARRANT ... 10_26_2026.csv (line 43817) | Denton: 1126_10-26_Early-Voting-Roster.xlsx (sheet Sheet1 row 34938), in person, 10/26/2026, pct 211301 |
| 2223366035 | SOS file vs another county's roster | SOS VICTORIA ... 10_20_2026.csv (line 1313) | Bell: 20261020_BBM.pdf (page 1), mail, 10/20/2026, pct 317 |
| 2223366074 | SOS file vs another county's roster | SOS WICHITA ... 10_26_2026.csv (line 1076) | Bell: 20261026_BBM.pdf (page 1), mail, 10/26/2026, pct 408 |
| 1031505594 | SOS file vs the same county's roster | SOS BEXAR ... 10_22_2026.csv (line 38629) | Bexar: EV_VoterCheckins_10222026.pdf (page 644), in person, 10/22/2026, pct S 3124.01 |
| 2154123680 | SOS file vs the same county's roster | SOS BEXAR ... 10_24_2026.csv (line 41010) | Bexar: EV_VoterCheckins_10242026.pdf (page 79), in person, 10/24/2026, pct P 4097 |
| 3383579919 | SOS file vs the same county's roster | SOS BEXAR ... 10_22_2026.csv (line 21410) | Bexar: EV_VoterCheckins_10222026.pdf (page 567), in person, 10/22/2026, pct P 3052 |
| 1019649503 | SOS file vs the same county's roster | SOS BOWIE ... 10_24_2026.csv (line 660) | Bowie: BBM Received 10.22.2026_1.pdf (page 1), mail, 10/22/2026 |
| 1017062829 | SOS file vs the same county's roster | SOS BRAZORIA ... 10_20_2026.csv (line 13150) | Brazoria: 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (downloaded 2026-10-27) (sheet BBM ROSTER-9.28-10.16.26 row 1754), mail, 10/02/2026, pct 76 |
| 1145191969 | SOS file vs the same county's roster | SOS BRAZORIA ... 10_22_2026.csv (line 7082) | Brazoria: 2026-10-22-MAIL LIST GENERAL.xlsx (downloaded 2026-10-27) (sheet 10.22.26 MAIL LIST GENERAL row 36), mail, 10/22/2026, pct 12 |
| 1167694139 | SOS file vs the same county's roster | SOS COLLIN ... 10_19_2026.csv (line 15545) | Collin: early-voters-through-october-26-2026.xlsx (sheet Detailed Check-in Report - Orde row 31724), in person, 10/19/2026, pct 246 |
| 2185867363 | SOS file vs the same county's roster | SOS DALLAS ... 10_24_2026.csv (line 10962) | Dallas: In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 348490), in person, 10/24/2026, pct 2608 |
| 2200965088 | SOS file vs the same county's roster | SOS DALLAS ... 10_22_2026.csv (line 30808) | Dallas: In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 332905), in person, 10/22/2026, pct 1141 |
| 3382936850 | SOS file vs the same county's roster | SOS DALLAS ... 10_24_2026.csv (line 13005) | Dallas: In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 390839), in person, 10/24/2026, pct 2048 |
| 2203146614 | SOS file vs the same county's roster | SOS DENTON ... 10_24_2026.csv (line 13668) | Denton: 1126_10-26_Early-Voting-Roster.xlsx (sheet Sheet1 row 74715), in person, 10/24/2026, pct 209701 |
| 1182032143 | SOS file vs the same county's roster | SOS EL PASO ... 10_22_2026.csv (line 13316) | El Paso: Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 42645), in person, 10/26/2026, pct 012 |
| 1143970888 | SOS file vs the same county's roster | SOS HARRIS ... 10_24_2026.csv (line 34714) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 30020), in person, 10/24/2026, pct 0902 |
| 1175275415 | SOS file vs the same county's roster | SOS HARRIS ... 10_24_2026.csv (line 9443) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_EV_ToPost_1126_10-24-26.csv (line 67166), in person, 10/24/2026, pct 0006 |
| 2170642029 | SOS file vs the same county's roster | SOS HARRIS ... 10_23_2026.csv (line 51114) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_EV_ToPost_1126_10-23-26.csv (line 57651), in person, 10/23/2026, pct 0436 |
| 2206845981 | SOS file vs the same county's roster | SOS HARRIS ... 10_22_2026.csv (line 31017) | Harris: Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_EV_ToPost_1126_10-22-26.csv (line 75317), in person, 10/22/2026, pct 0672 |
| 3383208181 | SOS file vs the same county's roster | SOS MONTGOMERY ... 10_21_2026.csv (line 9374) | Montgomery: GEN110326_10.21.2026.zip > GEN110326_10.21.2026.csv (line 36534; line 36535; line 36536), in person, 10/21/2026, pct 113 |
| 1045656452 | SOS file vs the same county's roster | SOS TARRANT ... 10_21_2026.csv (line 988) | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-27) > Early_voting_in_person_report.txt (line 362851), in person, pct 4276 |

## 4. Same voter on two counties' own rosters (§6.8)

| VUID | Record 1 | Record 2 |
|---|---|---|
| 1053842333 | Bexar: EV_VoterCheckins_10232026.pdf (page 406), in person, 10/23/2026, pct P 2143 | Hidalgo: EV Roster November 3, 2026 (Cumulative).csv (downloaded 2026-10-27) (line 56021), in person, pct 151 |
| 1072354984 | Bexar: voter_check_in_details 10-19-2026.pdf (page 806), in person, 10/19/2026, pct S 1045.01 | Cameron: 01_Cameron_County10-26-2026.xlsx (sheet CC row 45181), in person, 10/25/2026, pct 21 |
| 1095835184 | Bexar: EV_VoterCheckins_10242026.pdf (page 509), in person, 10/24/2026, pct P 4031 | El Paso: Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 74150), in person, 10/26/2026, pct 164 |
| 1173266072 | Bexar: voter_check_in_details 10-19-2026.pdf (page 579), in person, 10/19/2026, pct S 1046.05 | Webb: Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 29), in person, 10/19/2026, pct 266 |
| 1138259335 | Cameron: MAIL-GN26-Cameron-County-CC-2026-10-26.xlsx (sheet Sheet1 row 1670), mail, 10/13/2026, pct 44 | Hidalgo: ABBM LIST November 3, 2026 (Cumulative)_202610270734494921.xlsx (sheet Received Ballots Voter List_Oct row 3713), mail, pct 63.01 |
| 1050774882 | Dallas: In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 183119), in person, 10/23/2026, pct 1146 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-27) > Early_voting_in_person_report.txt (line 758), in person, pct 1010 |
| 1102081832 | Guadalupe: Early Voting in Person Roster_10-21-2026.pdf (page 82), in person, 10/21/2026, pct 406 | Smith: 10.19.26 In Person Voter List.pdf (page 67), in person, 10/19/2026, pct 242 |
| 2208300194 | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_BBM_ToPost_1126.csv (line 475), mail, 09/30/2026, pct 0516 | Travis: G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.23.2026 Early Vote.xlsx (sheet EarlyVote row 9427), in person, 10/23/2026, pct 264 |
| 2215994140 | Smith: 10.22.26 In Person Voter List.pdf (page 73), in person, 10/22/2026, pct 245 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-27) > Early_voting_in_person_report.txt (line 25165), in person, pct 1224 |

## 5. Mail ballot and in-person vote by the same voter (§6.9)

| County | VUID | Mail ballot | In-person vote |
|---|---|---|---|
| Bastrop | 1155981619 | Mail-Ballots-18.csv (line 169), 10/16/2026, pct S 4022.1 | Thursday-October-22.csv (line 395), 10/22/2026 |
| Dallas | 1075428602 | Mail-Ballot-Returns-Report_10.26.26.xlsx (sheet Early Voter Roster row 20), 10/24/2026, pct 4079 | In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 337531), 10/19/2026 |
| Dallas | 1083306122 | Mail-Ballot-Returns-Report_10.26.26.xlsx (sheet Early Voter Roster row 9507), 10/09/2026, pct 2027 | In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 29572), 10/22/2026 |
| Dallas | 1083306133 | Mail-Ballot-Returns-Report_10.26.26.xlsx (sheet Early Voter Roster row 9508), 10/09/2026, pct 2027 | In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 29571), 10/20/2026 |
| Dallas | 1091625391 | Mail-Ballot-Returns-Report_10.26.26.xlsx (sheet Early Voter Roster row 16397), 10/24/2026, pct 2020 | In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 281129), 10/26/2026 |
| Dallas | 1143777206 | Mail-Ballot-Returns-Report_10.26.26.xlsx (sheet Early Voter Roster row 4134), 10/09/2026, pct 1147 | In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 143101), 10/19/2026 |
| Denton | 1091223859 | 1126_10-26_Returned_Mail_Roster.xlsx (sheet Sheet1 row 3655), 10/23/2026, pct 3131 | 1126_10-26_Early-Voting-Roster.xlsx (sheet Sheet1 row 173250), 10/26/2026 |
| El Paso | 1095047541 | Received_Ballots_Voter_List_10.27.csv (line 651), 10/06/2026, pct 150 | Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 93403), 10/26/2026 |
| El Paso | 1096333268 | Received_Ballots_Voter_List_10.27.csv (line 5097), 10/20/2026, pct 160 | Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 41569), 10/26/2026 |
| El Paso | 1096656403 | Received_Ballots_Voter_List_10.27.csv (line 3529), 10/13/2026, pct 55 | Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 96521), 10/26/2026 |
| El Paso | 1097760377 | Received_Ballots_Voter_List_10.27.csv (line 3578), 10/13/2026, pct 115 | Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 77679), 10/26/2026 |
| El Paso | 1098032793 | Received_Ballots_Voter_List_10.27.csv (line 3590), 10/13/2026, pct 232 | Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 35971), 10/26/2026 |
| El Paso | 2152866163 | Received_Ballots_Voter_List_10.27.csv (line 5356), 10/20/2026, pct 133 | Early Voting 10-26-2026_Voter_List_(Ballot_Address_Day8.xlsx (sheet Sheet1 row 42707), 10/26/2026 |
| Grayson | 1025394327 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1 row 65), 10/02/2026, pct 409.01 | October 20 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1491), 10/20/2026 |
| Jefferson | 1121803988 | JeffersonCountyMailBallotsReceived11032026-01012026to10262026.xls (sheet JeffersonCountyMailBallotsRecei row 633), 10/26/2026 | JeffersonCountyInPersonVoting10202026.xls (sheet JeffersonCountyInPersonVoting10 row 2142), 10/20/2026 |
| Tarrant | 1044915588 | absentee_returned_voter_report.zip (downloaded 2026-10-27) > absentee_returned_voter_report.txt (line 12630), 10/21/2026, pct 2034 | Early_voting_in_person_report.zip (downloaded 2026-10-27) > Early_voting_in_person_report.txt (line 91400) |
| Tarrant | 1050956866 | absentee_returned_voter_report.zip (downloaded 2026-10-27) > absentee_returned_voter_report.txt (line 5690), 10/22/2026, pct 1277 | Early_voting_in_person_report.zip (downloaded 2026-10-27) > Early_voting_in_person_report.txt (line 31948) |
| Travis | 1133310623 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.07.2026 Ballot by Mail.xlsx (sheet 10.07.2026 Ballot by Mail row 234), 10/07/2026, pct 270 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.21.2026 Early Vote.xlsx (sheet EarlyVote row 8017), 10/21/2026 |

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
| Bexar | 1140208390 | EV_VoterCheckins_10202026.pdf (page 718) | 10/20/2026 |
| Bexar | 2222687508 | EV_VoterCheckins_10202026.pdf (page 152) | 10/20/2026 |
| Bexar | 1069614030 | EV_VoterCheckins_10212026.pdf (page 721) | 10/21/2026 |
| Bexar | 1150529579 | EV_VoterCheckins_10212026.pdf (page 25) | 10/21/2026 |
| Bexar | 1168562626 | EV_VoterCheckins_10212026.pdf (page 71) | 10/21/2026 |
| Bexar | 2120288965 | EV_VoterCheckins_10212026.pdf (page 255) | 10/21/2026 |
| Collin | 1076603030 | early-voters-through-october-26-2026.xlsx (sheet Detailed Check-in Report - Orde row 63581; sheet Detailed Check-in Report - Orde row 63582) | 10/19/2026 |
| Collin | 1201013964 | early-voters-through-october-26-2026.xlsx (sheet Detailed Check-in Report - Orde row 85842; sheet Detailed Check-in Report - Orde row 85843) | 10/19/2026 |
| Collin | 1175567020 | early-voters-through-october-26-2026.xlsx (sheet Detailed Check-in Report - Orde row 49123; sheet Detailed Check-in Report - Orde row 49124) | 10/20/2026 |
| Dallas | 3383295764 | In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 393660; sheet Early Voting Turnout Details row 397099) | 10/19/2026 |
| Dallas | 2170970526 | In-Person-Early-Voting-Turnout-Details-Report_10.26.2026.xlsx (sheet Early Voting Turnout Details row 301673; sheet Early Voting Turnout Details row 401904) | 10/23/2026 |
| Denton | 2205229587 | 1126_10-26_Early-Voting-Roster.xlsx (sheet Sheet1 row 160721; sheet Sheet1 row 160724) | 10/19/2026 |
| Galveston | 1185946285 | Checkins_10192026.xlsx (sheet Checkins_10192026 row 13286; sheet Checkins_10192026 row 13288) | 10/19/2026 |
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
| Tom Green | 1052505413 | 102526Roster.pdf (page 63; page 65) | 10/20/2026 |
| Travis | 1130790938 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.19.2026 Early Vote.xlsx (sheet EarlyVote row 27271; sheet EarlyVote row 27273) | 10/19/2026 |
| Webb | 2223144396 | Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 49) | 10/19/2026 |

**Mail ballot listed as received on two days**

| County | VUID | First listing | Second listing |
|---|---|---|---|
| Bastrop | 1043348399 | Mail-Ballots-18.csv (line 697; line 698), 10/13/2026 | Mail-Ballots-18.csv (line 697; line 698), 10/23/2026 |
| Bastrop | 1043189497 | Mail-Ballots-18.csv (line 567; line 568), 10/15/2026 | Mail-Ballots-18.csv (line 567; line 568), 10/23/2026 |
| Bastrop | 1043215320 | Mail-Ballots-18.csv (line 784; line 785), 10/16/2026 | Mail-Ballots-18.csv (line 784; line 785), 10/23/2026 |
| Bastrop | 1043219916 | Mail-Ballots-18.csv (line 149; line 150), 10/19/2026 | Mail-Ballots-18.csv (line 149; line 150), 10/26/2026 |
| Bastrop | 1140681719 | Mail-Ballots-18.csv (line 32; line 33), 10/20/2026 | Mail-Ballots-18.csv (line 32; line 33), 10/26/2026 |
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
| Bowie | 1019649503 | BBM Received 10.22.2026_1.pdf (page 1), 10/22/2026 | BBM Received 10.24.2026.pdf (page 1), 10/24/2026 |
| Brazoria | 2177416650 | 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (downloaded 2026-10-27) (sheet BBM ROSTER-9.28-10.16.26 row 426), 10/05/2026 | 2026-10-21 MAIL LIST GENERAL.xlsx (sheet 10.21.26 MAIL LIST GENERAL row 27), 10/21/2026 |
| Brazos | 2180077579 | mail-10-09-2026-accessible.pdf (page 1), 10/09/2026 | mail-10-13-2026-accessible.pdf (page 4), 10/13/2026 |
| Galveston | 1032377676 | Nov-3-2026-Ballots-Received-Oct-13.csv (line 450), 10/13/2026 | Nov-3rd-2026-Ballots-Received-10.14.26.csv (line 28), 10/14/2026 |
| Gregg | 1034151785 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 17), 10/16/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 2), 10/21/2026 |
| Gregg | 2132128292 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 14), 10/19/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 3), 10/20/2026 |
| Harris | 2221343697 | Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19275), 09/24/2026 | Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-27) > Cumulative_BBM_ToPost_1126.csv (line 11; line 19275), 10/14/2026 |
| Montgomery | 1100132244 | GEN110326_10.02.2026.zip > GEN110326_10.02.2026.csv (line 184; line 185), 10/02/2026 | GEN110326_10.22.2026.zip > GEN110326_10.22.2026.csv (line 40502; line 40503), 10/22/2026 |
| Montgomery | 1126476176 | GEN110326_10.05.2026.zip > GEN110326_10.05.2026.csv (line 498; line 499; line 500), 10/05/2026 | GEN110326_10.24.2026.zip > GEN110326_10.24.2026.csv (line 44163; line 44164; line 44165), 10/24/2026 |
| Montgomery | 1119403485 | GEN110326_10.08.2026.zip > GEN110326_10.08.2026.csv (line 176; line 177; line 178; line 179), 10/08/2026 | GEN110326_10.26.2026.zip > GEN110326_10.26.2026.csv (line 49917; line 49918; line 49919; line 49920), 10/26/2026 |
| Travis | 1132888023 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 09.30.2026 Ballot by Mail.xlsx (sheet 10.01.2026 Ballot by Mail row 16), 09/30/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 999), 10/22/2026 |
| Travis | 1140151036 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.05.2026 Ballot by Mail_UPDATED.xlsx (sheet 10.05.2026 Ballot by Mail row 108), 10/05/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.19.2026 Ballot by Mail.xlsx (sheet 10.19.2026 Ballot by Mail row 2110), 10/19/2026 |
| Travis | 3383285286 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.07.2026 Ballot by Mail.xlsx (sheet 10.07.2026 Ballot by Mail row 906), 10/07/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 1085), 10/22/2026 |
| Travis | 1139039837 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.12.2026 Ballot by Mail.xlsx (sheet 10.12.2026 Ballot by Mail row 12), 10/12/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.13.2026 Ballot by Mail.xlsx (sheet 10.13.2026 Ballot by Mail row 376), 10/13/2026 |
| Travis | 1140972395 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.16.2026 Ballot by Mail.xlsx (sheet 10.16.2026 Ballot by Mail row 7), 10/16/2026 | G26-Voter-Rosters.zip (downloaded 2026-10-27) > 10.22.2026 Ballot by Mail.xlsx (sheet 10.22.2026 Ballot by Mail row 1114), 10/22/2026 |

## 7. Identifiers that are not state VUIDs (§6.11)

All 1239 records are in the CSV. For 18 of them, the state's file for the same county lists one voter with the same last name, first name and precinct under a 10-digit VUID; these are inferences to be confirmed, not matches by ID:

| County | County identifier | County file and location | Possible state VUID |
|---|---|---|---|
| Bexar | 221423660 | EV_VoterCheckins_10212026.pdf (page 195), in person 10/21/2026, pct S 1105.01 | 2214238660 |
| Collin | 4168640 | early-voters-through-october-26-2026.xlsx (sheet Detailed Check-in Report - Orde row 24650), in person 10/22/2026, pct 94 | 1076312029 |
| Collin | 472987 | early-voters-through-october-26-2026.xlsx (sheet Detailed Check-in Report - Orde row 64975), in person 10/24/2026, pct 69 | 1075716466 |
| El Paso | 1482939 | Received_Ballots_Voter_List_10.27.csv (line 5939), mail 10/22/2026, pct 8.1 | 1095318907 |
| Grayson | 116404233 | October 20 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 299), in person 10/20/2026, pct 109 | 1090375242 |
| Grayson | 168949 | October 25 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 706), in person 10/25/2026, pct 302 | 1133330857 |
| Grayson | 169043 | October 24 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 976), in person 10/24/2026, pct 406 | 2146465425 |
| Grayson | 169077 | October 19 2026  EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1904), in person 10/19/2026, pct 108 | 2156111295 |
| Grayson | 169246 | October 22 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 1825), in person 10/22/2026, pct 408 | 2121250168 |
| Grayson | 170955 | October 21 2026 EV Check-in Report.xlsx (sheet Detailed Check-in Report - Orde row 4015), in person 10/21/2026, pct 412 | 1090100033 |
| Hidalgo | 61100048 | ABBM LIST November 3, 2026 (Cumulative)_202610270734494921.xlsx (sheet Received Ballots Voter List_Oct row 3276), mail, pct 98.01 | 1154845862 |
| Hidalgo | 61108735 | ABBM LIST November 3, 2026 (Cumulative)_202610270734494921.xlsx (sheet Received Ballots Voter List_Oct row 194), mail, pct 135.01 | 2141425996 |
| Smith | 2436166 | 10.25.26 In Person Voter List.pdf (page 53), in person 10/25/2026, pct 412 | 1027280545 |
| Tarrant | 41741013 | absentee_returned_voter_report.zip (downloaded 2026-10-27) > absentee_returned_voter_report.txt (line 14988), mail 10/22/2026, pct 4053 | 3383117753 |
| Tarrant | 41741720 | absentee_returned_voter_report.zip (downloaded 2026-10-27) > absentee_returned_voter_report.txt (line 15007), mail 10/20/2026, pct 4116 | 2202167573 |
| Tarrant | 41765735 | absentee_returned_voter_report.zip (downloaded 2026-10-27) > absentee_returned_voter_report.txt (line 15225), mail 10/20/2026, pct 3035 | 1082322527 |
| Tarrant | 41768481 | absentee_returned_voter_report.zip (downloaded 2026-10-27) > absentee_returned_voter_report.txt (line 15255), mail 10/16/2026, pct 3176 | 3383497931 |
| Tarrant | 41772205 | absentee_returned_voter_report.zip (downloaded 2026-10-27) > absentee_returned_voter_report.txt (line 15289), mail 10/23/2026, pct 2055 | 3383532539 |
