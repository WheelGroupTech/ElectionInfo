# Texas Early Voting Rosters: Record Locator Supplement

**Supplements:** Texas Early Voting Rosters: County and State Data Review, October 22, 2026\
**Election:** November 3, 2026 General Election (SOS election 53815)\
**Data through:** Wednesday, October 21, 2026; files as posted by 7:50 PM October 22\
**For:** county election offices and the Secretary of State's Elections Division

This supplement lists the voter records behind the anomalies in sections 6.1 and 6.4-6.11 of the October 22 review, so that county and state staff can find each record in their own files. Section 6.2 (voters whose identities Tarrant County redacts) is not included.

Each record is identified by its VUID (or, in §6.11, the county's own identifier) and by where it appears: the file name as the county or the Secretary of State posted it, the entry inside a ZIP, and the line, worksheet row or PDF page. Voter names and addresses are not reproduced: the VUID and location identify each record. All identifiers come from rosters published under Texas Election Code §87.121.

The complete list, 7,974 records, is in the companion file **Texas_Early_Voting_Roster_Review_2026-10-22_Supplement_Records.csv**. This document summarizes it and lists the smaller sections in full.

## 1. How to read the locations

- **County files** are named as posted on the county's website. Travis County's daily workbooks are entries in its ZIP `G26-Voter-Rosters.zip` (as posted October 22); Harris, Tarrant and Montgomery files are also ZIP entries. A location is written `file > entry (location)`.
- **State files** are named as the SOS portal's CSV Download names them: `<COUNTY>.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.<MM_DD_YYYY>.csv` (Election Information & Turnout Data, early voting date, county Voter Details Report CSV).
- **Line** numbers count the header as line 1. **Sheet ... row** is the worksheet name and the Excel row number. **Page** is the PDF page, counting from 1. Where a VUID appears on several lines or pages of one file, all are listed.
- **Dates** are the voting or received date in the county's file, and for state records the date of the state's file (which for mail ballots is when the state recorded the ballot, not when the county received it).
- The files were downloaded October 20-22, 2026. Counties replace some files daily (Collin, Williamson, Cameron, Hidalgo, Victoria, Potter, Randall, McLennan) and the state revises its files, so a later copy of a file may differ in line numbers.

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
| 6.1 | SOS county file lists a voter who is on another county's roster (same first name, that county's precinct) | 631: Dallas 67, Denton 62, Travis 62, Collin 45, Bexar 33, Tarrant 29, Parker 27, Hays 25, Harris 23, Johnson 21, and 1-16 in 30 more SOS county files |
| 6.4 | SOS mail ballot not on the county's own mail list (Tarrant excluded: §6.2) | 2,623: Travis 1,479, Collin 732, Montgomery 77, Dallas 75, Smith 69, Bell 53, Harris 46, Webb 28, Tom Green 14, Denton 13, Gregg 11, El Paso 9, Fort Bend 6, Randall 3, Galveston 2, Grayson 2, Hidalgo 2, Victoria 1, Williamson 1 |
| 6.5 | Precinct differs between the county's and the state's record of the same VUID | 3,901: Bowie 3,277, Harris 345, Galveston 120, Tarrant 67, El Paso 42, Smith 20, Hunt 14, Montgomery 7, Travis 3, Denton 2, Bastrop 1, Bexar 1, Dallas 1, Henderson 1 |
| 6.6 | Surname differs between the county and the state | 139: Tarrant 20, Collin 17, Cameron 15, Williamson 12, Bexar 11, Denton 11, Galveston 7, Hidalgo 7, and 1-5 in 17 more counties |
| 6.7 | One VUID, two different people | 15 records, 14 VUIDs (§3) |
| 6.8 | Same voter on two counties' own rosters | 2 (§4) |
| 6.9 | Mail ballot and in-person vote by the same voter | 9 (§5) |
| 6.10 | Same-day duplicate check-in; mail ballot received on two days | 19 + 19 (§6) |
| 6.11 | Identifier that is not a 10-digit state VUID | 616: Tarrant 261, Collin 100, Denton 79, El Paso 56, Hidalgo 35, Grayson 34, Webb 20, Smith 9, Bowie 7, Bexar 6, Gregg 6, Tom Green 2, Henderson 1 (§7) |

Notes on the larger sections:

- **6.4 Travis:** 1,209 of the 1,479 are dated October 19-20 by the state; Travis's ZIP has no mail lists for October 15 or 16, which most likely hold these ballots. **6.4 Collin and Tom Green:** 715 Collin and 9 Tom Green records are dated after the county's latest mail list used here (timing, marked in the note column).
- **6.5 Bowie:** the county's October 20 and 21 Early Vote History reports give a precinct that matches the state's for only 395 of 1,961 and 206 of 1,917 voters.
- **6.6:** in most of these records the county and state names share a word, which points to name changes or formatting rather than different people.

## 3. One VUID, two different people (§6.7)

| VUID | Case | Record 1 | Record 2 |
|---|---|---|---|
| 1017062829 | Two voters under one VUID in one Brazoria file | Brazoria: 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (sheet BBM ROSTER-9.28-10.16.26, rows 1754 and 1755), mail, 10/02/2026, pct 76 | same file, the other of the two rows |
| 2221343697 | Two voters under one VUID in one Harris file | Harris: Cumulative_BBM_1126.zip (posted Oct 22) > Cumulative_BBM_1126.csv (line 11), mail, 09/24/2026, pct 0139 | same file (line 19270), mail, 10/14/2026, pct 0505 |
| 2223367198 | Two counties' rosters, two different people | Bell: 20261019_BBM.pdf (page 1), mail, 10/15/2026, pct 307 | Randall: Mail Ballot Roster 10.21.26 (posted Oct 22, page 23), mail, 10/15/2026; also SOS RANDALL ... 10_15_2026.csv (line 2) |
| 2223366042 | SOS file vs another county's roster | SOS CORYELL.2026 NOVEMBER GENERAL ELECTION.EarlyVoting.10_19_2026.csv (line 1060) | Bell: 20261019_EV.pdf (page 3), in person, 10/19/2026, pct 104 |
| 2223375616 | SOS file vs another county's roster | SOS FORT BEND ... 10_19_2026.csv (line 17503) | Bell: 20261019_BBM.pdf (page 30), mail, 10/19/2026, pct 101 |
| 2223375713 | SOS file vs another county's roster | SOS FORT BEND ... 10_20_2026.csv (line 8957) | Bell: 20261020_EV.pdf (page 70), in person, 10/20/2026, pct 105 |
| 2223366061 | SOS file vs another county's roster | SOS RANDALL ... 10_21_2026.csv (line 3033) | Bell: 20261021_BBM.pdf (page 9), mail, 10/21/2026, pct 201 |
| 2223366035 | SOS file vs another county's roster | SOS VICTORIA ... 10_20_2026.csv (line 1294) | Bell: 20261020_BBM.pdf (page 1), mail, 10/20/2026, pct 317 |
| 1015936692 | SOS file vs another county's roster | SOS HARRIS ... 10_21_2026.csv (line 15709) | El Paso: Early Voting 10-21-2026_Voter_List_(Ballot_Address_Day3-2.xlsx (sheet Sheet1, row 11215), in person, 10/21/2026, pct 052 |
| 2223378724 | SOS file vs another county's roster | SOS MONTGOMERY ... 10_20_2026.csv (line 11800) | El Paso: Received_Ballots_Voter_List_20261022084250.csv (line 5314), mail, 10/20/2026, pct 29.1 |
| 2223382143 | SOS file vs another county's roster | SOS HIDALGO ... 10_19_2026.csv (line 16253) | Harris: Cumulative_BBM_1126.zip (posted Oct 22) > Cumulative_BBM_1126.csv (line 23889), mail, 10/19/2026, pct 0771 |
| 2223365544 | SOS file vs another county's roster | SOS HIDALGO ... 10_14_2026.csv (line 9) | Tarrant: absentee_returned_voter_report.zip (posted Oct 21) > absentee_returned_voter_report.txt (line 10623), mail, 10/14/2026, pct 2636 |
| 3383208181 | SOS file vs the same county's roster | SOS MONTGOMERY ... 10_21_2026.csv (line 9370) | Montgomery: GEN110326_10.21.2026.zip > GEN110326_10.21.2026.csv (lines 36534-36536), in person, 10/21/2026, pct 113 |
| 1045656452 | SOS file vs the same county's roster | SOS TARRANT ... 10_21_2026.csv (line 968) | Tarrant: Early_voting_in_person_report.zip (posted Oct 22) > Early_voting_in_person_report.txt (line 131607), in person, pct 4276 |

"..." stands for ".2026 NOVEMBER GENERAL ELECTION.EarlyVoting.". Six of these VUIDs (2223365544 through 2223382143) are new registrations in one numeric range; five of them are on Bell County's roster under different names than the state gives them, which the Bell County Elections office and the SOS may want to compare first.

## 4. Same voter on two counties' own rosters (§6.8)

| VUID | Record 1 | Record 2 |
|---|---|---|
| 1173266072 | Bexar: voter_check_in_details 10-19-2026.pdf (page 579), in person, 10/19/2026, pct S 1046.05 | Webb: Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 29), in person, 10/19/2026, pct 266 |
| 1138259335 | Cameron: MAIL-GN26-Cameron-County-CC-2026-10-20.xlsx (sheet Sheet1, row 1099), mail, 10/13/2026, pct 44 | Hidalgo: ABBM LIST November 3, 2026 (Cumulative) (posted Oct 22; sheet Received Ballots Voter List_Oct, row 3044), mail, pct 63.01 |

## 5. Mail ballot and in-person vote by the same voter (§6.9)

| County | VUID | Mail ballot | In-person vote |
|---|---|---|---|
| Dallas | 1083306133 | Mail-Ballot-Returns-Report_10.21.26.xlsx (sheet Early Voter Roster, row 7937), 10/09/2026, pct 2027 | In_Person-Early-Voting-Details-Report_10.21.26.csv (line 77730), 10/20/2026 |
| Dallas | 1143777206 | Mail-Ballot-Returns-Report_10.21.26.xlsx (sheet Early Voter Roster, row 3558), 10/09/2026, pct 1147 | In_Person-Early-Voting-Details-Report_10.21.26.csv (line 50322), 10/19/2026 |
| El Paso | 1095047541 | Received_Ballots_Voter_List_20261022084250.csv (line 651), 10/06/2026, pct 150 | Early Voting 10-21-2026_Voter_List_(Ballot_Address_Day3-2.xlsx (sheet Sheet1, row 45528) |
| El Paso | 1096656403 | Received_Ballots_Voter_List_20261022084250.csv (line 3529), 10/13/2026, pct 55 | same workbook, row 47134 |
| El Paso | 1097760377 | Received_Ballots_Voter_List_20261022084250.csv (line 3578), 10/13/2026, pct 115 | same workbook, row 37813 |
| El Paso | 1098032793 | Received_Ballots_Voter_List_20261022084250.csv (line 3590), 10/13/2026, pct 232 | same workbook, row 17433 |
| Grayson | 1025394327 | October 19 2026 Absentee Voter List.xlsx (sheet Sheet1, row 65), 10/02/2026, pct 409.01 | October 20 2026 EV Check-in Report.xlsx (row 1491), 10/20/2026 |
| Tarrant | 1044915588 | absentee_returned_voter_report.zip (posted Oct 21) > absentee_returned_voter_report.txt (line 9435), received 10/21/2026, pct 2034 | Early_voting_in_person_report.zip (posted Oct 22) > Early_voting_in_person_report.txt (line 34001) |
| Travis | 1133310623 | G26-Voter-Rosters.zip > 10.07.2026 Ballot by Mail.xlsx (row 234), 10/07/2026, pct 270 | G26-Voter-Rosters.zip > 10.21.2026 Early Vote.xlsx (sheet EarlyVote, row 8017), 10/21/2026 |

El Paso's early voting workbook is cumulative and undated, so its in-person date is not given; the first two El Paso voters were reported in the October 20 review as in-person voters on October 19.

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
| Collin | 1076603030 | early-voters-through-october-21-2026.xlsx (rows 26179, 26180) | 10/19/2026 |
| Collin | 1079622635 | early-voters-through-october-21-2026.xlsx (rows 5571, 5572) | 10/19/2026 |
| Collin | 1201013964 | early-voters-through-october-21-2026.xlsx (rows 35265, 35266) | 10/19/2026 |
| Collin | 1175567020 | early-voters-through-october-21-2026.xlsx (rows 20143, 20144) | 10/20/2026 |
| Galveston | 1185946285 | Checkins_10192026.xlsx (rows 13286, 13288) | 10/19/2026 |
| Grayson | 1024897712 | October 19 2026 EV Check-in Report.xlsx (rows 3415, 3422) | 10/19/2026 |
| Grayson | 1024922211 | October 19 2026 EV Check-in Report.xlsx (rows 3575, 3591) | 10/19/2026 |
| Grayson | 1042130958 | October 19 2026 EV Check-in Report.xlsx (rows 3480, 3491) | 10/19/2026 |
| Grayson | 1076365904 | October 19 2026 EV Check-in Report.xlsx (rows 3327, 3336) | 10/19/2026 |
| Grayson | 1090251506 | October 19 2026 EV Check-in Report.xlsx (rows 3310, 3316) | 10/19/2026 |
| Grayson | 1025261772 | October 20 2026 EV Check-in Report.xlsx (rows 4517, 4522) | 10/20/2026 |
| Travis | 1130790938 | G26-Voter-Rosters.zip > 10.19.2026 Early Vote.xlsx (sheet EarlyVote, rows 27271, 27273) | 10/19/2026 |
| Webb | 2223144396 | Check-in Report - Ordered by Check-in Time 10-19-2026.pdf (page 49) | 10/19/2026 |

**Mail ballot listed as received on two days**

| County | VUID | First listing | Second listing |
|---|---|---|---|
| Bexar | 1064449288 | ABBM_Recvd_10052026.pdf (page 4) | ABBM_Recvd_10102026.pdf (page 6) |
| Bexar | 1065766865 | ABBM_Recvd_10072026.pdf (page 38) | ABBM_Recvd_10192026.pdf (page 27) |
| Bexar | 2153842163 | ABBM_Recvd_10082026.pdf (page 11) | ABBM_Recvd_10152026.pdf (page 12) |
| Bexar | 1067081664 | ABBM_Recvd_10102026.pdf (page 22) | ABBM_Recvd_10132026.pdf (page 14) |
| Bexar | 1071067645 | ABBM_Recvd_10102026.pdf (page 34) | ABBM_Recvd_10132026.pdf (page 21) |
| Bexar | 1147096356 | ABBM_Recvd_10102026.pdf (page 9) | ABBM_Recvd_10132026.pdf (page 6) |
| Bexar | 1065220238 | ABBM_Recvd_10152026.pdf (page 2) | ABBM_Recvd_10162026.pdf (page 1) |
| Bexar | 1069344089 | ABBM_Recvd_10152026.pdf (page 1) | ABBM_Recvd_10162026.pdf (page 1) |
| Bexar | 1070856222 | ABBM_Recvd_10152026.pdf (page 1) | ABBM_Recvd_10162026.pdf (page 1) |
| Bexar | 1067011360 | ABBM_Recvd_10192026.pdf (page 13) | ABBM_Recvd_10212026.pdf (page 9) |
| Brazoria | 2177416650 | 09-28-THRU 10-16.26 MAIL LIST GENERAL ELECTION.xlsx (row 426), 10/05/2026 | 2026-10-21 MAIL LIST GENERAL.xlsx (row 27), 10/21/2026 |
| Brazos | 2180077579 | mail-10-09-2026-accessible.pdf (page 1) | mail-10-13-2026-accessible.pdf (page 4) |
| Galveston | 1032377676 | Nov-3-2026-Ballots-Received-Oct-13.csv (line 450) | Nov-3rd-2026-Ballots-Received-10.14.26.csv (line 28) |
| Gregg | 1034151785 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 17), received 10/16/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 2), received 10/21/2026 |
| Gregg | 2132128292 | 2026-10-20-2_00PM-BBM-Voters.pdf (page 14), received 10/19/2026 | 2026-10-20-to-10-21-BBM-Voters.pdf (page 3), received 10/20/2026 |
| Harris | 2221343697 | Cumulative_BBM_1126.csv (line 11), 09/24/2026 | same file (line 19270), 10/14/2026 (two different voters: §3) |
| Randall | 1025881037 | Mail Ballot Roster 10.21.26 (page 24), received 10/16/2026 | same file (page 32), received 10/21/2026 |
| Travis | 1140151036 | G26-Voter-Rosters.zip > 10.05.2026 Ballot by Mail_UPDATED.xlsx (row 108) | G26-Voter-Rosters.zip > 10.19.2026 Ballot by Mail.xlsx (row 2110) |
| Travis | 1139039837 | G26-Voter-Rosters.zip > 10.12.2026 Ballot by Mail.xlsx (row 12) | G26-Voter-Rosters.zip > 10.13.2026 Ballot by Mail.xlsx (row 376) |

Bexar's October 10 file was posted as `ABBM_Recvd_10102026.pdf`.

## 7. Identifiers that are not state VUIDs (§6.11)

All 616 records are in the CSV. For seven of them, the state's file for the same county lists one voter with the same last name, first name and precinct under a 10-digit VUID; these are inferences to be confirmed, not matches by ID:

| County | County identifier | County file and location | Possible state VUID |
|---|---|---|---|
| Bexar | 221423660 | EV_VoterCheckins_10212026.pdf (page 195), in person 10/21/2026, pct S 1105.01 | 2214238660 (one digit missing in the county's file) |
| Grayson | 116404233 | October 20 2026 EV Check-in Report.xlsx (row 299), in person 10/20/2026, pct 109 | 1090375242 |
| Grayson | 169077 | October 19 2026 EV Check-in Report.xlsx (row 1904), in person 10/19/2026, pct 108 | 2156111295 |
| Grayson | 170955 | October 21 2026 EV Check-in Report.xlsx (row 4015), in person 10/21/2026, pct 412 | 1090100033 |
| Tarrant | 41741720 | absentee_returned_voter_report.zip (posted Oct 21) > absentee_returned_voter_report.txt (line 11021), mail 10/20/2026, pct 4116 | 2202167573 |
| Tarrant | 41765735 | same file (line 11179), mail 10/20/2026, pct 3035 | 1082322527 |
| Tarrant | 41768481 | same file (line 11194), mail 10/16/2026, pct 3176 | 3383497931 |

## 8. Caveats

- The records are as published by the counties and the Secretary of State. A listing here is a discrepancy to check, not a finding that any voter voted improperly; most have clerical explanations.
- Records in §6.1 and §6.7 rely on a VUID match plus a first-name comparison; §7's possible VUIDs rely on a name and precinct match only.
- Section 6.2 of the review (state files identifying voters Tarrant County redacts) is deliberately not itemized.
