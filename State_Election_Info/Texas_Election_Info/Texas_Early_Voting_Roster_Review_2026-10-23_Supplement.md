# Texas Early Voting Rosters: Record Locator Supplement

**Supplements:** Texas Early Voting Rosters: County and State Data Review, October 23, 2026\
**Election:** November 3, 2026 General Election (SOS election 53815)\
**Data through:** Thursday, October 22, 2026; files as posted by 8:20 PM October 23\
**For:** county election offices and the Secretary of State's Elections Division

This supplement lists the voter records behind the anomalies in sections 6.1 and 6.4-6.11 of the October 23 review, so that county and state staff can find each record in their own files. Section 6.2 (voters whose identities Tarrant County redacts) is not included. It replaces the October 22 supplement: the lists below cover October 14-22 and the current versions of each county's files.

Each record is identified by its VUID (or, in §6.11, the county's own identifier) and by where it appears: the file name as the county or the Secretary of State posted it, the entry inside a ZIP, and the line, worksheet row or PDF page. Voter names and addresses are not reproduced: the VUID and location identify each record. All identifiers come from rosters published under Texas Election Code §87.121.

The complete list, 11,659 records, is in the companion file **Texas_Early_Voting_Roster_Review_2026-10-23_Supplement_Records.csv**. This document summarizes it and lists the smaller sections in full.

## 1. How to read the locations

- **County files** are named as posted on the county's website. A county that posts a file under the same name every day is shown with the date the copy was downloaded, for example `Cumulative_EV_ToPost_1126.zip (downloaded 2026-10-23)`. Travis County's daily workbooks are entries in its ZIP `G26-Voter-Rosters.zip`; Harris, Tarrant and Montgomery files are also ZIP entries. A location is written `file > entry (location)`.
- **State files** are named as the SOS portal's CSV Download names them: `<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv` (Election Information & Turnout Data, early voting date, county Voter Details Report CSV). "..." in the tables below stands for ".2026 NOVEMBER GENERAL ELECTION.EarlyVoting.".
- **Line** numbers count the header as line 1. **Sheet ... row** is the worksheet name and the Excel row number. **Page** is the PDF page, counting from 1. Where a VUID appears on several lines or pages of one file, all are listed.
- **Dates** are the voting or received date in the county's file, and for state records the date of the state's file (which for mail ballots is when the state recorded the ballot, not when the county received it). Tarrant's, El Paso's and Hidalgo's in-person lists are cumulative and carry no voting date; the CSV shows the list date for El Paso and none for the others.
- The files were downloaded October 20-23, 2026. Counties replace some files daily (Collin, Williamson, Cameron, Hidalgo, Victoria, Potter, Randall, Tom Green, McLennan) and the state revises its files, so a later copy of a file may differ in line numbers.

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
| 6.1 | SOS county file lists a voter who is on another county's roster (same first name, that county's precinct) | 815: Dallas 83, Denton 81, Travis 69, Collin 59, Bexar 47, Tarrant 35, Hays 34, Parker 33, Harris 32, Johnson 30, and 1-25 in 37 more SOS county files |
| 6.4 | SOS mail ballot not on the county's own mail list (Tarrant excluded: §6.2) | 2,743: Travis 1,505, Collin 806, Montgomery 89, Harris 77, Dallas 75, Bell 69, Webb 28, Fort Bend 21, Smith 16, Denton 13, Gregg 11, El Paso 10, Tom Green 9, Williamson 4, Randall 3, Galveston 2, Grayson 2, Hidalgo 2, Victoria 1 |
| 6.5 | Precinct differs between the county's and the state's record of the same VUID | 7,127: Bowie 3,277, Williamson 2,929, Harris 527, Galveston 124, Tarrant 89, Bexar 65, El Paso 52, Smith 31, Hunt 14, Montgomery 8, Denton 4, Travis 4, Bastrop 1, Dallas 1, Henderson 1 |
| 6.6 | Surname differs between the county and the state | 213: Collin 26, Tarrant 23, Williamson 20, Cameron 19, Bexar 18, Denton 17, Hidalgo 9, Webb 8, and 1-7 in 20 more counties |
| 6.7 | One VUID, two different people | 18 records, 17 VUIDs (§3) |
| 6.8 | Same voter on two counties' own rosters | 3 (§4) |
| 6.9 | Mail ballot and in-person vote by the same voter | 13 (§5) |
| 6.10 | Same-day duplicate check-in; mail ballot received on two days | 22 + 19 (§6) |
| 6.11 | Identifier that is not a 10-digit state VUID | 686: Tarrant 289, Collin 123, Denton 79, El Paso 59, Grayson 40, Hidalgo 39, Webb 22, Smith 12, Bowie 7, Bexar 6, Gregg 6, Tom Green 3, Henderson 1 (§7) |

Notes on the larger sections:

- **6.4 Travis:** 1,208 of the 1,505 are dated October 19-20 by the state; Travis's ZIP has no mail lists for October 15 or 16, which most likely hold these ballots. **6.4 timing:** 772 Collin and 27 Travis records are dated October 22 by the state, after the county's latest mail list (marked in the note column).
- **6.5 Williamson:** all 2,929 are mail voters in the county's October 22 file (`VoterTurnout_261022b_202610230849476200.xlsx`), whose mail precincts changed from the October 21 file for 4,053 voters. Comparing with the October 21 file (`VoterTurnout_261021b_202610220755307110.xlsx`) shows the change.
- **6.5 Bowie:** all 3,277 are in the October 20 and 21 Early Vote History reports (1,566 and 1,711); the October 22 report matches the state. **6.5 Bexar:** 64 of the 65 are in `EV_VoterCheckins_10222026.pdf`.
- **6.6:** in all but 4 records the county and state names share a word, which points to name changes or formatting rather than different people.

## 3. One VUID, two different people (§6.7)

| VUID | Case | Record 1 | Record 2 |
|---|---|---|---|
| 1017062829 | Two voters under one VUID in one Brazoria file | Brazoria: 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (sheet BBM ROSTER-9.28-10.16.26, rows 1754 and 1755), mail, 10/02/2026, pct 76 | same file, the other of the two rows |
| 2221343697 | Two voters under one VUID in one Harris file | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-23) > Cumulative_BBM_ToPost_1126.csv (line 11), mail, 09/24/2026, pct 0139 | same file (line 19271), mail, 10/14/2026, pct 0505 |
| 2223366061 | Two counties' rosters, two different people | Bell: 20261021_BBM.pdf (page 9), mail, 10/21/2026, pct 201 | Randall: Mail Ballot Roster 10.22.26_202610230851281153.pdf (page 34), mail, 10/21/2026; also SOS RANDALL ... 10_21_2026.csv (line 3032) |
| 2223367198 | SOS file vs another county's roster (reported October 22 on Randall's roster; no longer there) | SOS RANDALL ... 10_15_2026.csv (line 2) | Bell: 20261019_BBM.pdf (page 1), mail, 10/15/2026, pct 307 |
| 2223366042 | SOS file vs another county's roster | SOS CORYELL ... 10_19_2026.csv (line 1060) | Bell: 20261019_EV.pdf (page 3), in person, 10/19/2026, pct 104 |
| 2223375616 | SOS file vs another county's roster | SOS FORT BEND ... 10_19_2026.csv (line 17537) | Bell: 20261019_BBM.pdf (page 30), mail, 10/19/2026, pct 101 |
| 2223375713 | SOS file vs another county's roster | SOS FORT BEND ... 10_20_2026.csv (line 26840) | Bell: 20261020_EV.pdf (page 70), in person, 10/20/2026, pct 105 |
| 2223366100 | SOS file vs another county's roster (new) | SOS LUBBOCK ... 10_22_2026.csv (line 3122) | Bell: 20261022_BBM.pdf (page 1), mail, 10/22/2026, pct 311 |
| 2223366035 | SOS file vs another county's roster | SOS VICTORIA ... 10_20_2026.csv (line 1294) | Bell: 20261020_BBM.pdf (page 1), mail, 10/20/2026, pct 317 |
| 1015936692 | SOS file vs another county's roster | SOS HARRIS ... 10_21_2026.csv (line 57291) | El Paso: Early Voting 10-22-2026_Voter_List_(Ballot_Address_Day4.xlsx (sheet Sheet1, row 14457), in person, pct 052 |
| 2223378724 | SOS file vs another county's roster | SOS MONTGOMERY ... 10_20_2026.csv (line 11800) | El Paso: Received_Ballots_Voter_List_20261023085122.csv (line 5314), mail, 10/20/2026, pct 29.1 |
| 2223382143 | SOS file vs another county's roster | SOS HIDALGO ... 10_19_2026.csv (line 16253) | Harris: Cumulative_BBM_ToPost_1126.zip (downloaded 2026-10-23) > Cumulative_BBM_ToPost_1126.csv (line 23889), mail, 10/19/2026, pct 0771 |
| 2223365544 | SOS file vs another county's roster | SOS HIDALGO ... 10_14_2026.csv (line 9) | Tarrant: absentee_returned_voter_report.zip (downloaded 2026-10-23) > absentee_returned_voter_report.txt (line 12591), mail, 10/14/2026, pct 2636 |
| 3383579919 | SOS file vs the same county's roster (new) | SOS BEXAR ... 10_22_2026.csv (line 20780) | Bexar: EV_VoterCheckins_10222026.pdf (page 567), in person, 10/22/2026, pct P 3052 |
| 1182032143 | SOS file vs the same county's roster (new) | SOS EL PASO ... 10_22_2026.csv (line 13305) | El Paso: Early Voting 10-22-2026_Voter_List_(Ballot_Address_Day4.xlsx (sheet Sheet1, row 26555), in person, pct 012 |
| 3383208181 | SOS file vs the same county's roster | SOS MONTGOMERY ... 10_21_2026.csv (line 9374) | Montgomery: GEN110326_10.21.2026.zip > GEN110326_10.21.2026.csv (lines 36534-36536), in person, 10/21/2026, pct 113 |
| 1045656452 | SOS file vs the same county's roster | SOS TARRANT ... 10_21_2026.csv (line 976) | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-23) > Early_voting_in_person_report.txt (line 172230), in person, pct 4276 |

Ten of these VUIDs (2223365544 through 2223382143) are recent registrations in one numeric range. Seven of them are on Bell County's roster under different names than the state gives them, so the Bell County Elections office and the SOS may want to compare them first.

## 4. Same voter on two counties' own rosters (§6.8)

| VUID | Record 1 | Record 2 |
|---|---|---|
| 2215994140 | Smith: 10.22.26 In Person Voter List.pdf (page 73), in person, 10/22/2026, pct 245 | Tarrant: Early_voting_in_person_report.zip (downloaded 2026-10-23) > Early_voting_in_person_report.txt (line 12464), in person (in the list from October 20), pct 1224; also SOS SMITH ... 10_20_2026.csv, in person, pct 1224 |
| 1173266072 | Bexar: voter_check_in_details 10-19-2026.pdf (page 579), in person, 10/19/2026, pct S 1046.05 | Webb: Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 29), in person, 10/19/2026, pct 266 |
| 1138259335 | Cameron: MAIL-GN26-Cameron-County-CC-2026-10-22.xlsx (sheet Sheet1, row 1087), mail, 10/13/2026, pct 44 | Hidalgo: ABBM LIST November 3, 2026 (Cumulative)_202610230629166413.xlsx (sheet Received Ballots Voter List_Oct, row 3373), mail, pct 63.01 |

The Smith/Tarrant voter has the same first and last name in both counties' rosters and in the state's file.

## 5. Mail ballot and in-person vote by the same voter (§6.9)

| County | VUID | Mail ballot | In-person vote |
|---|---|---|---|
| Bastrop | 1155981619 | Mail-Ballots-16.csv (line 140), 10/16/2026, pct S 4022.1 | Thursday-October-22.csv (line 395), 10/22/2026 |
| Dallas | 1083306122 | Mail-Ballot-Returns-Report_10.22.26.xlsx (sheet Early Voter Roster, row 8514), 10/09/2026, pct 2027 | In-Person-Early-Voting-Details-Report_10.22.26.xlsx (sheet Early Voting Turnout Details, row 60419), 10/22/2026 |
| Dallas | 1083306133 | same workbook, row 8515, 10/09/2026, pct 2027 | same in-person workbook, row 105052, 10/20/2026 |
| Dallas | 1143777206 | same workbook, row 3720, 10/09/2026, pct 1147 | same in-person workbook, row 54790, 10/19/2026 |
| El Paso | 1095047541 | Received_Ballots_Voter_List_20261023085122.csv (line 651), 10/06/2026, pct 150 | Early Voting 10-22-2026_Voter_List_(Ballot_Address_Day4.xlsx (sheet Sheet1, row 58208) |
| El Paso | 1096656403 | same CSV (line 3529), 10/13/2026, pct 55 | same workbook, row 60180 |
| El Paso | 1097760377 | same CSV (line 3578), 10/13/2026, pct 115 | same workbook, row 48372 |
| El Paso | 1098032793 | same CSV (line 3590), 10/13/2026, pct 232 | same workbook, row 22375 |
| El Paso | 2152866163 | same CSV (line 5356), 10/20/2026, pct 133 | same workbook, row 26594 |
| Grayson | 1025394327 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1, row 65), 10/02/2026, pct 409.01 | October 20 2026 EV Check-in Report.xlsx (row 1491), 10/20/2026 |
| Tarrant | 1044915588 | absentee_returned_voter_report.zip (downloaded 2026-10-23) > absentee_returned_voter_report.txt (line 11134), 10/21/2026, pct 2034 | Early_voting_in_person_report.zip (downloaded 2026-10-23) > Early_voting_in_person_report.txt (line 43543); in the list from October 19 |
| Tarrant | 1050956866 | same absentee file (line 5022), 10/22/2026, pct 1277 | same in-person file (line 15559); in the list from October 20 |
| Travis | 1133310623 | G26-Voter-Rosters.zip > 10.07.2026 Ballot by Mail.xlsx (row 234), 10/07/2026, pct 270 | G26-Voter-Rosters.zip > 10.21.2026 Early Vote.xlsx (sheet EarlyVote, row 8017), 10/21/2026 |

El Paso's early voting workbook is cumulative and undated. The first two El Paso voters were in its list by October 19, the next two by October 21 and the last by October 22.

## 6. Duplicates within county files (§6.10)

**Checked in twice on the same day**

| County | VUID | File and location | Date |
|---|---|---|---|
| Bexar | 1140208390 | EV_VoterCheckins_10202026.pdf (page 718) | 10/20/2026 |
| Bexar | 2222687508 | EV_VoterCheckins_10202026.pdf (page 152) | 10/20/2026 |
| Bexar | 1069614030 | EV_VoterCheckins_10212026.pdf (page 721) | 10/21/2026 |
| Bexar | 1150529579 | EV_VoterCheckins_10212026.pdf (page 25) | 10/21/2026 |
| Bexar | 1168562626 | EV_VoterCheckins_10212026.pdf (page 71) | 10/21/2026 |
| Bexar | 2120288965 | EV_VoterCheckins_10212026.pdf (page 255) | 10/21/2026 |
| Collin | 1076603030 | early-voters-through-october-22-2026.xlsx (rows 33950, 33951) | 10/19/2026 |
| Collin | 1201013964 | early-voters-through-october-22-2026.xlsx (rows 45778, 45779) | 10/19/2026 |
| Collin | 1175567020 | early-voters-through-october-22-2026.xlsx (rows 26060, 26061) | 10/20/2026 |
| Denton | 2205229587 | 1126_10-22_Early-Voting-Roster-1.xlsx (rows 89562, 89571) | 10/19/2026 |
| Galveston | 1185946285 | Checkins_10192026.xlsx (rows 13286, 13288) | 10/19/2026 |
| Grayson | 1024897712 | October 19 2026 EV Check-in Report.xlsx (rows 3415, 3422) | 10/19/2026 |
| Grayson | 1024922211 | October 19 2026 EV Check-in Report.xlsx (rows 3575, 3591) | 10/19/2026 |
| Grayson | 1042130958 | October 19 2026 EV Check-in Report.xlsx (rows 3480, 3491) | 10/19/2026 |
| Grayson | 1076365904 | October 19 2026 EV Check-in Report.xlsx (rows 3327, 3336) | 10/19/2026 |
| Grayson | 1090251506 | October 19 2026 EV Check-in Report.xlsx (rows 3310, 3316) | 10/19/2026 |
| Grayson | 1025261772 | October 20 2026 EV Check-in Report.xlsx (rows 4517, 4522) | 10/20/2026 |
| Grayson | 1025207956 | October 22 2026 EV Check-in Report.xlsx (rows 656, 667) | 10/22/2026 |
| Grayson | 1089592991 | October 22 2026 EV Check-in Report.xlsx (rows 483, 487) | 10/22/2026 |
| Tom Green | 1052505413 | 102226Roster.pdf (page 147) | 10/20/2026 |
| Travis | 1130790938 | G26-Voter-Rosters.zip > 10.19.2026 Early Vote.xlsx (sheet EarlyVote, rows 27271, 27273) | 10/19/2026 |
| Webb | 2223144396 | Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 49) | 10/19/2026 |

The Collin voter 1079622635 reported on October 22 no longer appears twice in Collin's current file.

**Mail ballot listed as received on two days**

| County | VUID | First listing | Second listing |
|---|---|---|---|
| Bexar | 1064449288 | ABBM_Recvd_10052026.pdf (page 4) | ABBM_Recvd_10102026_202610152202246278.pdf (page 6) |
| Bexar | 1065766865 | ABBM_Recvd_10072026.pdf (page 38) | ABBM_Recvd_10192026.pdf (page 27) |
| Bexar | 2153842163 | ABBM_Recvd_10082026.pdf (page 11) | ABBM_Recvd_10152026.pdf (page 12) |
| Bexar | 1067081664 | ABBM_Recvd_10102026_202610152202246278.pdf (page 22) | ABBM_Recvd_10132026.pdf (page 14) |
| Bexar | 1071067645 | ABBM_Recvd_10102026_202610152202246278.pdf (page 34) | ABBM_Recvd_10132026.pdf (page 21) |
| Bexar | 1147096356 | ABBM_Recvd_10102026_202610152202246278.pdf (page 9) | ABBM_Recvd_10132026.pdf (page 6) |
| Bexar | 1065220238 | ABBM_Recvd_10152026.pdf (page 2) | ABBM_Recvd_10162026.pdf (page 1) |
| Bexar | 1069344089 | ABBM_Recvd_10152026.pdf (page 1) | ABBM_Recvd_10162026.pdf (page 1) |
| Bexar | 1070856222 | ABBM_Recvd_10152026.pdf (page 1) | ABBM_Recvd_10162026.pdf (page 1) |
| Bexar | 1067011360 | ABBM_Recvd_10192026.pdf (page 13) | ABBM_Recvd_10212026.pdf (page 9) |
| Brazoria | 2177416650 | 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (row 426), 10/05/2026 | 2026-10-21 MAIL LIST GENERAL.xlsx (row 27), 10/21/2026 |
| Brazos | 2180077579 | mail-10-09-2026-accessible.pdf (page 1) | mail-10-13-2026-accessible.pdf (page 4) |
| Galveston | 1032377676 | Nov-3-2026-Ballots-Received-Oct-13.csv (line 450) | Nov-3rd-2026-Ballots-Received-10.14.26.csv (line 28) |
| Gregg | 1034151785 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 17), received 10/16/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 2), received 10/21/2026 |
| Gregg | 2132128292 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 14), received 10/19/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 3), received 10/20/2026 |
| Harris | 2221343697 | Cumulative_BBM_ToPost_1126.csv (line 11), 09/24/2026 | same file (line 19271), 10/14/2026 (two different voters: §3) |
| Montgomery | 1100132244 | GEN110326_10.02.2026.zip > GEN110326_10.02.2026.csv (lines 184-185), 10/02/2026 | GEN110326_10.22.2026.zip > GEN110326_10.22.2026.csv (lines 40502-40503), 10/22/2026 (new) |
| Travis | 1140151036 | G26-Voter-Rosters.zip > 10.05.2026 Ballot by Mail_UPDATED.xlsx (row 108) | G26-Voter-Rosters.zip > 10.19.2026 Ballot by Mail.xlsx (row 2110) |
| Travis | 1139039837 | G26-Voter-Rosters.zip > 10.12.2026 Ballot by Mail.xlsx (row 12) | G26-Voter-Rosters.zip > 10.13.2026 Ballot by Mail.xlsx (row 376) |

The Randall voter reported on October 22 as received twice is no longer listed twice in Randall's October 22 roster.

## 7. Identifiers that are not state VUIDs (§6.11)

All 686 records are in the CSV. For eleven of them, the state's file for the same county lists one voter with the same last name, first name and precinct under a 10-digit VUID; these are inferences to be confirmed, not matches by ID:

| County | County identifier | County file and location | Possible state VUID |
|---|---|---|---|
| Bexar | 221423660 | EV_VoterCheckins_10212026.pdf (page 195), in person 10/21/2026, pct S 1105.01 | 2214238660 (one digit missing in the county's file) |
| Collin | 4168640 | early-voters-through-october-22-2026.xlsx (row 12750), in person 10/22/2026, pct 94 | 1076312029 |
| Grayson | 116404233 | October 20 2026 EV Check-in Report.xlsx (row 299), in person 10/20/2026, pct 109 | 1090375242 |
| Grayson | 169077 | October 19 2026 EV Check-in Report.xlsx (row 1904), in person 10/19/2026, pct 108 | 2156111295 |
| Grayson | 170955 | October 21 2026 EV Check-in Report.xlsx (row 4015), in person 10/21/2026, pct 412 | 1090100033 |
| Grayson | 169246 | October 22 2026 EV Check-in Report.xlsx (row 1825), in person 10/22/2026, pct 408 | 2121250168 |
| Hidalgo | 61100048 | ABBM LIST November 3, 2026 (Cumulative)_202610230629166413.xlsx (sheet Received Ballots Voter List_Oct, row 2960), mail, pct 98.01 | 1154845862 |
| Tarrant | 41741720 | absentee_returned_voter_report.zip (downloaded 2026-10-23) > absentee_returned_voter_report.txt (line 13079), mail 10/20/2026, pct 4116 | 2202167573 |
| Tarrant | 41765735 | same file (line 13259), mail 10/20/2026, pct 3035 | 1082322527 |
| Tarrant | 41768481 | same file (line 13279), mail 10/16/2026, pct 3176 | 3383497931 |
| Tarrant | 41741013 | same file (line 13063), mail 10/22/2026, pct 4053 | 3383117753 |

## 8. Caveats

- The records are as published by the counties and the Secretary of State. A listing here is a discrepancy to check, not a finding that any voter voted improperly; most have clerical explanations.
- Records in §6.1 and §6.7 rely on a VUID match plus a first-name comparison; §7's possible VUIDs rely on a name and precinct match only.
- Section 6.2 of the review (state files identifying voters Tarrant County redacts) is deliberately not itemized.
