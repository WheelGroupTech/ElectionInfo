# Travis County November 2026 General Election: Voter Participation and Data Review

**Election:** November 3, 2026 General Election (G26), compared with the March 3, 2026 primaries (P26), the November 5, 2024 general election (G24) and the March 5, 2024 primaries (P24)\
**County:** Travis County, Texas\
**Data:**

- Travis County's own voter rosters and the Texas Secretary of State's (SOS) roster files for all four elections. The SOS G26 Election Day files were downloaded again on November 5, 2026.
- Travis County voter registration lists from March 2024 to October 27, 2026:
  - the county's published lists (`Registered_Voter_List`, `Registered_Voters_With_History`);
  - Official Lists of Registered Voters (OLRV) from the state's Texas Election Administration Management (TEAM) system, which Travis County provided under an Open Records Request.
  - Sources are in section 3.
- Ballot counts from the Central Count reconciliation records, the tabulation audit logs and the official results (section 2.1).

**Date:** November 5, 2026

This review uses public rosters (Texas Election Code §87.121) and county voter registration lists. Records are identified only by VUID (state voter ID). No voter names, home addresses or dates of birth are reproduced. Ages and mailing-address states appear only as counts. Section 5 describes properties by TCAD property type, ZIP code and precinct, not by street address. The records behind the anomalies in section 9 and the commercial-address registrations in section 5.3 are listed by VUID in the companion file **Travis_County_G26_Voter_Participation_Review_2026-11-05_Records.csv**.

The companion file has one row per record, with the columns `section`, `category`, `vuid` and `detail`. Its records are in sections A1 through A10:

| Section | Records | What it lists | Report section |
|------|------:|------------------------------------------------------|--------|
| A1 | 4,938 | G26 voters on no 2026 Travis registration list | 9.1 |
| A2 | 558 | Registration date after the October 5 deadline, and on no earlier Travis list | 9.2 |
| A3 | 6 | G26 voters on the Elly deceased-voter list of May 1, 2026 | 9.3 |
| A4 | 4 | Age 105 or older on Election Day (one is listed as 118) | 9.5 |
| A5 | 3 | Two voting methods on the county roster | 9.5 |
| A6 | 7 | Also on another county's own G26 roster | 9.5 |
| A7 | 81 | Also in another county's G26 SOS file, but not on that county's roster | 9.5 |
| A8 | 9 | Repeated roster entries: 6 mail ballots listed as received on two days, 3 voters listed twice on the same day | 9.5 |
| A9 | 99 | On the September 16 OLRV but on no October list | 9.4 |
| A10 | 350 | No vote in P24, G24 or P26, and registered at a property with a commercial or industrial improvement (TCAD property ID in `detail`) | 5.3 |
| **Total** | **6,055** | | |

## 1. Summary

**Earlier participation of the G26 voters.** The county's G26 roster has 550,572 records. Of these, 547,076 are distinct voters with a VUID. The other 3,496 are:

- 3,482 limited-ballot voters, whom the county lists by name only;
- two mail rows with 9-digit IDs, both apparently VUIDs with one digit dropped (section 9.5);
- 12 repeated rows.

Of the 547,076 voters:

| Earlier election | Voted | Share of G26 voters |
|---|---:|---:|
| P26 (March 2026 primary), Democratic | 203,166 | 37.1% |
| P26, Republican | 52,925 | 9.7% |
| P26, either party | 256,091 | 46.8% |
| G24 (November 2024 general) | 433,942 | 79.3% |
| P24 (March 2024 primary), Democratic | 87,721 | 16.0% |
| P24, Republican | 43,793 | 8.0% |
| P24, either party | 131,514 | 24.0% |
| None of P24, G24, P26 | 92,537 | 16.9% |

**92,537 G26 voters (16.9%) did not vote in P24, G24 or P26.** Section 5 looks at them in the registration lists:

- **Timing:** 60% registered in Travis after the November 2024 election, and 38% after the March 2026 primary, so most could not have voted in those elections in Travis.
- **Who they are:** 43% are under 30, and a third have first-time Texas VUIDs.
- **Where they live:** they are concentrated around the University of Texas campus and in east and southeast Travis County, but nearly every precinct has some.
- **Addresses:** 0.4% (350) are registered at commercial or industrial properties. The three largest, looked up in TCAD, are two apartment buildings and a church used as a homeless registration address.

**Official ballots cast and the rosters** (section 2.1):

| Election | Ballots counted | County roster records | Difference | Main reasons |
|----------|------:|------:|-----:|----------------------------------|
| P24 (March 2024 primaries) | 155,143 | 152,217 | 2,926 | 2,455 Election Day voters missing from the county's roster; 256 provisional ballots |
| G24 (November 2024 general) | 587,090 | 583,202 | 3,888 | 2,603 limited ballots and 960 provisional ballots not on the roster; 615 mail ballots without a roster record |
| P26 (March 2026 primaries) | 274,443 | 274,183 | 260 | 485 provisional ballots, less 184 mail voters on the roster whose ballots were rejected |
| G26 (November 2026 general, unofficial) | 552,560 | 550,572 | 1,988 | 1,238 early voting provisional ballots; 833 mail ballots without a roster record |

- **The audit logs match:** replaying the tabulation audit logs gives exactly the ballots tabulated in P24, G24 and P26, for every vote type. The CVRs match for G24 and P26.
- **P24 Republican mail ballots were hand counted:** the Travis County Republican Party hand counted all 1,553 of its P24 mail ballots, outside the county's tabulation system. They are in the 155,143 ballots counted but not in the results' 153,590 ballots cast, the audit log or the Republican CVR.
- **The county's P24 Election Day roster is incomplete:** it lists 74,763 voters, while the SOS files have 77,219 entries, the same as the county's reconciliation. This review therefore counts P24 voters from the county roster and the SOS files together (section 2.1).

**Primary voters who did not vote in the next general election** (on neither the county's nor the state's Travis roster for that general election):

| Primary voters | Voted in the primary | Did not vote in the next general |
|---|---:|---:|
| P26 Republicans → G26 | 56,729 | 3,766 (6.6%) |
| P26 Democrats → G26 | 217,010 | 13,689 (6.3%) |
| P24 Republicans → G24 | 52,704 | 2,492 (4.7%) |
| P24 Democrats → G24 | 101,989 | 4,445 (4.4%) |

**Finding these voters in the registration lists** (section 7):

| | P26 Rep. | P26 Dem. | P24 Rep. | P24 Dem. |
|---|---:|---:|---:|---:|
| Did not vote in the general | 3,766 | 13,689 | 2,492 | 4,445 |
| Voted in the general in another tracked county (moved) | 672 | 3,129 | 692 | 1,117 |
| Still registered in Travis at the general (active or suspense) | 2,649 | 9,295 | 1,433 | 2,745 |
| Removed from the Travis lists before the general | 445 | 1,265 | 367 | 582 |
| On the Elly deceased-voter list | 0 | 0 | 0 | 1 |

**Voters who voted by mail in the primary and have a mailing address outside Texas** (section 8):

| | P26 Rep. | P26 Dem. | P24 Rep. | P24 Dem. |
|---|---:|---:|---:|---:|
| Voted by mail in the primary | 154 | 525 | 115 | 394 |
| Outside Texas on the list at the time (P26 list / Oct 2024 list) | 0 | 0 | 3 | 4 |
| Outside Texas on the October 27 OLRV | 5 | 27 | 9 | 6 |

Of all 17,854 G26 mail voters, 1,244 have a registration mailing address outside Texas:

- 928 in a foreign country (70 countries);
- 242 in another state;
- 74 at a military (APO/FPO) address.

**Anomalies in the G26 data** (section 9):

- 4,938 G26 voters are on no 2026 Travis registration list, including the county's own Voter Lists through October 20. Earlier elections had the same gap: 8,804 G24 voters and 1,503 P26 voters were missing from the list at the time. Nearly all the P26 voters appeared on the next list (section 9.1).
- 558 have a registration date after the October 5 deadline and are on no earlier Travis list. The date may be when the record was entered in TEAM, not when the application was received.
- 99 are on the September 16 OLRV but on no October list. 72 of them voted by mail.
- 6 voters were flagged as deceased (obituary findings) in a May 1, 2026 review of the Travis voter list by Elly (theelectorlist.com).
- 7 voters are also on another county's own G26 roster.
- 3 voters are on the roster under two voting methods.
- 6 mail ballots are listed as received on two different days.
- One woman is listed as 118 years old and voted early in person in G24 and G26. There is no record of her on LongeviQuest's list of validated supercentenarians, so either her birth date is wrong or someone else voted as her.
- No two registrations with the same full name and date of birth both voted.

## 2. Data and method

**Rosters (who voted):**

| Election | County roster (distinct VUIDs) | SOS files (distinct VUIDs) | On both | County only | SOS only |
|---|---:|---:|---:|---:|---:|
| P24 | 152,217 | 152,931 | 150,455 | 1,762 | 2,476 |
| G24 | 583,188 | 579,124 | 577,701 | 5,487 | 1,423 |
| P26 | 273,739 | 272,188 | 271,238 | 2,501 | 950 |
| G26 | 547,076 | 544,120 | 542,926 | 4,150 | 1,194 |

- **County rosters:** Travis County's posted roster ZIPs:
  - `P24_Voter_Rosters_2024-03-14.zip`, `G24_Voter_Rosters_2024-11-10.zip` and `P26_Voter_Rosters_2026-03-11.zip`;
  - for G26, `G26-Voter-Rosters_2026-11-04.zip` (early voting, mail and Election Day).
- **SOS files:** the per-day early voting files and the Election Day file.
  - **2024:** from the SOS's older portal, earlyvoting.texas-election.com.
  - **2026:** from the current portal.
- **The SOS G26 Election Day file is now complete:**
  - **Count:** 101,867 voters, against the county's 101,814. On November 4 it held 45,857.
  - **Mail ballots:** most of the remaining "county only" voters are mail ballots (3,969). The county's mail lists run past the state's.
  - **State only:** the state's file lists 1,194 voters the county roster does not (589 mail, 513 early in person, 92 Election Day).
- **Who counts as having voted:**
  - **Voted:** a voter counts as voting in an election when on the county roster. For P24, a voter on the county roster or the SOS files counts, because the county's P24 Election Day roster is incomplete (section 2.1).
  - **Did not vote:** a voter counts as not voting only when on neither the county roster nor the SOS file.
- **Party in the primaries:**
  - **County rosters:** from the party worksheets.
  - **SOS files:** from the election name.
- **P24 Election Day roster:**
  - **A misnamed file is handled:** the county's `03.05.2024 Early Vote.xlsx` holds the same 74,763 voters as its Election Day roster and was read as Election Day.
  - **Missing voters:** both files lack 2,455 Election Day voters that the SOS files list, so P24 voters are taken from both sources (section 2.1).
- **Other counties:** voters were looked up in the other 49 of the 50 largest Texas counties.
  - **G26:** those counties' own rosters and their SOS files, with the Election Day files downloaded November 5.
  - **G24:** the SOS's G24 files for those counties.
  - **Not covered:** voters who moved to the other 204 counties cannot be seen.
- **Mailing address:**
  - **Meaning:** the registration record's mailing address. A mail ballot can be sent to another address named on the application, so this measures the registration, not where each ballot went.
  - **Foreign addresses:** the OLRV leaves the state (`MAIL_PROVINCE`) and ZIP code blank for a foreign mailing address. A blank state **with** a mailing address is counted as foreign, and the country is taken from the county Voter List's `Mail Country Code`.
  - **Counted as Texas:** a voter with no separate mailing address (blank state and no mailing address).
  - **Masked addresses:** `*****` (protected voters) are counted separately.

### 2.1 Official ballots cast compared with the rosters

The rosters list voters; the official counts are of ballots. They do not match exactly, and the differences can be traced by vote type.

**Sources for the ballot counts:**

- **Central Count reconciliation records.** The ballot counts by vote type were recorded at Travis County Central Count by the Republican judge: the presiding judge of the Republican primary in P24 and P26, and the alternate judge in G24 and G26.
  - **P24:** the signed *Election Reconciliation - Official Totals* (SOS form 12-2), `20240305 P24-Reconciliation.pdf`.
  - **G24:** `Polling Locations and Ballot Counts.xlsx`.
  - **P26:** `P26 Ballot Canvas.xlsx`, the official reconciliation in total and by party.
  - **G26:** `G26 Ballot Canvas.xlsx`, unofficial: the canvass has not happened yet.
- **Tabulation audit logs (P24, G24, P26).** The ES&S Electionware *Election Audit Events* reports in `Audit_Logs/TX_Travis_County` were replayed with Election Explorer.
  - **What the replay counts:** every ballot batch committed in the election session, less the batches removed by media clears.
  - **Test sessions are left out:** the logic and accuracy test decks loaded before each count are excluded.
- **Official results and CVRs:**
  - **Results:** ballots cast from the county's results exports (Clarity `*_detailxml.zip`).
  - **CVRs:** the cast vote records in `Election_CVRs/TX_Travis_County`, one record per ballot.

**Ballots counted by vote type:**

| Vote type | P24 | G24 | P26 | G26 (unofficial) |
|---|---:|---:|---:|---:|
| Early voting in person | 69,248 | 460,326 | 164,973 | 427,382 |
| Limited ballots | (in early voting) | 2,603 | 447 | 3,477 |
| Election Day | 77,198 | 97,925 | 101,066 | 101,766 |
| Mail | 8,441 | 25,276 | 7,472 | 18,697 |
| Provisional | 256 (74 early voting, 182 Election Day) | 960 (710 early voting, 250 Election Day) | 485 (299 early voting, 186 Election Day) | 1,238 early voting (batches of 370 and 868); Election Day not yet counted |
| **Total ballots counted** | **155,143** | **587,090** | **274,443** | **552,560** |
| Audit log replay (tabulated ballots) | 153,590 | 587,090 | 274,443 | (log not yet available) |
| Results: ballots cast | 153,590 | 587,090 | 274,443 | 552,560 |
| CVR records | Republican only: 52,183 | 587,090 | 274,443 | (not yet released) |

- **The audit log matches every vote type:** limited ballots are counted as early voting ballots. With that, the audit log's early voting, Election Day, mail and provisional totals equal the reconciliation's for all three elections.
- **The P24 difference is the Republican mail hand count:** the 1,553 ballots are counted in the reconciliation but were never tabulated (below).
- **The G24 log shows batches reloaded at the canvass:**
  - Four early voting provisional batches (697 ballots), first loaded November 2 and 5, were cleared and loaded again with the same counts on November 13.
  - A 12-ballot batch loaded as early voting provisional was cleared and loaded again as Election Day provisional.
  - The final totals are unchanged.

**Ballots counted minus county roster records, by vote type:**

| Election | Early voting in person | Limited | Election Day | Mail | Provisional (not on the rosters) | Total |
|---|---:|---:|---:|---:|---:|---:|
| P24 | +37 (limited included) | | **+2,435** | +198 | +256 | **+2,926** |
| G24 | −201 | +2,603 (not on the roster) | −89 | +615 | +960 | **+3,888** |
| P26 | −21 | +4 | −24 | −184 | +485 | **+260** |
| G26 (unofficial) | −30 | −5 | −48 | +833 | +1,238 | **+1,988** |

The roster records are:

- **P24:** early voting 69,211; Election Day 74,763; mail 8,243.
- **G24:** early voting 460,527; Election Day 98,014; mail 24,661.
- **P26:** early voting 164,994; limited 443; Election Day 101,090; mail 7,656.
- **G26:** early voting 427,412; limited 3,482; Election Day 101,814; mail 17,864.

What explains the differences:

- **Provisional ballots:** the rosters do not list provisional voters.
  - **P24:** 461 provisional ballots were submitted; 205 were rejected and 256 counted.
  - **P26:** 1,094 were submitted; 609 were rejected and 485 counted.
  - **G26:** early voting provisional ballots are already in the unofficial count. Election Day provisional ballots will be added at the canvass.
- **Limited ballots:** the G24 roster has no limited-ballot records. The P26 and G26 rosters list these voters by name only.
- **Voters who checked in but whose ballot was not counted:** the small negative in-person differences are these voters.
  - **P24:** the reconciliation gives the reasons for its 33-voter difference as "confidential voters, fleeing voters, empty carrier envelope voters".
  - **G24:** the early voting sheet records 203 more check-ins than ballots counted.
- **Mail voters on the roster:**
  - **P26:** the county's mail roster lists 184 more voters than mail ballots counted. The reconciliation shows 189 mail ballots rejected, so the mail roster includes voters whose ballots were rejected.
  - **G24 and G26:** the opposite happens. 615 (G24) and 833 (G26, unofficial) counted mail ballots have no record on the county's mail roster. The G26 roster may still be completed.
- **P24 Election Day roster:** the county's posted roster is missing 2,455 voters (below).

**P24: the county roster and the SOS files compared** (distinct VUIDs):

| P24 vote type | County roster | SOS files | On both | County only | SOS only |
|---|---:|---:|---:|---:|---:|
| Early voting in person | 69,211 | 69,211 | 69,211 | 0 | 0 |
| Election Day | 74,763 | 77,217 | 74,762 | 1 | **2,455** |
| Mail | 8,243 | 6,503 | 6,482 | **1,761** | 21 |
| **Total** | **152,217** | **152,931** | **150,455** | **1,762** | **2,476** |

- **Early voting:** the two sources are identical.
- **Where both list a voter, they agree:** every such voter has the same voting method in both. All but one have the same party; that one voter is in both parties' SOS Election Day files.
- **Election Day: the county roster is incomplete.**
  - **The county files:** the county's `03.05.2024 Election Day.xlsx` and `03.05.2024 Early Vote.xlsx` hold the same 74,763 voters.
  - **The SOS files:** they have 77,219 entries, the same as the 77,219 Election Day voters in the county's reconciliation (for 77,198 ballots). They are 77,217 distinct VUIDs, one voter listed in both parties' files, and one entry without a valid VUID.
  - **The missing voters are not in one place:** the 2,455 voters on the SOS files but not on the county roster (1,702 Democratic, 753 Republican) are spread over 93 of the 148 vote centers and 271 precincts. At no vote center is more than 14% of its voters missing.
- **Mail: the SOS files are incomplete.**
  - **Lists after early voting are missing:** the SOS early voting files end with March 1, the last day of early voting. The county's mail lists continue after it, and their voters are not in the SOS files: 1,129 on the March 4 list, 100 on March 6 and 11 on March 7.
  - **Earlier mail lists:** another 521 voters on the county's earlier mail lists are not in the SOS files either.
  - **The other way:** 21 SOS mail voters are not on the county roster.
  - **Against the ballots:** the county's mail roster (8,243) is closer to the 8,441 mail ballots counted, which include the Republican hand count.
- **How this review counts P24 voters:** a P24 voter counts as having voted when on the county roster or the SOS files. The 2,476 voters found only on the SOS files take their party and method from those files.
  - **The totals:** this gives 154,693 P24 voters, 101,989 Democratic and 52,704 Republican.
  - **G26 voters:** 2,060 more of them count as P24 voters than on the county roster alone (131,514 instead of 129,454).
  - **The group with no earlier vote:** 22 of its members are on the SOS files only, so it is 92,537.
  - **Non-returning primary voters:** the P24 groups in section 6 grow by 37 Republicans and 90 Democrats.

**P24 Republican mail ballots were counted by hand:**

- **The results:** all 38 Republican contests in the official P24 results report zero mail votes.
- **The cause:** the Travis County Republican Party hand counted all of its P24 mail ballots. None went through the county's tabulation system.
  - **The final count:** the party's final results (`P24 REP Final BBM Results.pdf`, March 13, 2024) total 1,553 ballots for President (candidates plus 20 undervotes).
  - **The reconciliation includes them:** its 8,441 mail ballots counted are the 6,888 tabulated mail ballots plus these 1,553.
  - **The tabulation records leave them out:** the results' ballots cast, the audit log and the Republican CVR do not include them. The Republican CVR's 52,183 ballots are early voting, Election Day and provisional ballots only.
- **Ballots by party:**

  | P24 party | Ballots counted | County roster records | Difference | County + SOS roster voters |
  |---|---:|---:|---:|---:|
  | Democratic | 101,407 | 100,277 | 1,130 | 101,990 |
  | Republican | 53,736 (52,183 tabulated + 1,553 hand counted) | 51,940 | 1,796 | 52,704 |

**P26 by party** (official reconciliation):

| P26 party | Early voting | Election Day | Mail | Provisional | Ballots counted | County roster records |
|---|---:|---:|---:|---:|---:|---:|
| Democratic | 131,865 | 79,579 | 5,801 | 365 | 217,610 | 217,010 |
| Republican | 33,555 | 21,487 | 1,671 | 120 | 56,833 | 56,730 |

- **Limited ballots:** the county roster's 443 limited-ballot records carry no party.
- **The CVRs agree:** the P26 CVRs give 217,609 Democratic ballots, 56,833 Republican and one blank.

**For the overlap counts in this review:** the voter-level questions use the roster records, so voters whose ballots have no roster record cannot be included. These are mainly:

- provisional and limited-ballot voters;
- some mail voters.

## 3. Registration lists and the TEAM problems

**Sources.** The review uses two kinds of registration list.

- **OLRV, the Official List of Registered Voters.** These are exports from TEAM in the SOS's standard layout.
  - Travis County provides them on request under the Texas Public Information Act (Open Records Request).
  - They include each voter's date of birth and mailing address.
  - In this review the date of birth is used only to compute ages and to match two registrations of the same person; it is never reproduced.
  - The OLRV files are the September 16 OLRV, the October 27 initial OLRV, the October 27 OLRV and the P26 primary list (`p26_olvr_primary_voter_data_rep.csv`).
- **County voter registration lists.** The `Registered_Voter_List` and `Registered_Voters_With_History` files are published by the Travis County Tax Office (voter registration).
  - They were downloaded from its data page, <https://voter-registration-maps-traviscountytx.hub.arcgis.com/pages/data-files-and-reference>, on the dates in their file names.
  - They carry registration and effective dates, status and mailing address, and the history files carry each voter's past elections. They have no dates of birth.

| List (file name prefix) | Date | Registered VUIDs | Used for |
|---|---|---:|---|
| `2026-10-27_G26_journal_gen_voter.csv` (October 27 OLRV: the current OLRV from TEAM) | Oct 27, 2026 | 950,042 | status (active / suspense), mailing state, age on Nov 3 |
| `2026-10-27_G26_Olrv INITIAL .csv` (ePollbook L&A) | Oct 27, 2026 | 940,396 | presence |
| `2026-10-20_Registered_Voter_List.csv` (county Voter List) | Oct 20, 2026 | 950,016 | status, registration date, effective date (EDR) |
| `2026-10-05_Registered_Voter_List.csv` | Oct 5, 2026 (registration deadline) | 944,288 | presence |
| `2026-09-16 OFFICIAL-LIST-OF-REGISTERED-VOTERS_...csv` (September OLRV) | Sept 16, 2026 | 933,778 | presence, mailing state |
| `2026-07-06_Registered_Voters_With_History.csv` | July 6, 2026 | 929,662 | presence; vote history (section 10) |
| `p26_olvr_primary_voter_data_rep.csv` | P26 | 920,114 | mailing state at P26 |
| `2026-02-10 Registered_Voters_With_History.csv` | Feb 10, 2026 | 894,170 | presence |
| `2024-10-24 Registered_Voters_With_History.csv` | Oct 24, 2024 (G24) | 922,960 | status and mailing state at G24 |
| `TRAVIS-OLRV-Homeless Voters.csv` (voters registered as homeless, in OLRV layout) | 2026 | 1,497 | homeless registration addresses (section 5.3) |
| `TRAVIS-VR 5_1_26 Deceased Voters.csv` (generated by Elly, <https://theelectorlist.com/>; not provided by Travis County or the State) and `TRAVIS-OLRV-Deceased Voters.csv` (the Elly list compared with the OLRV in Election Explorer: the 555 of its 556 voters found on the OLRV, in OLRV layout) | May 1, 2026 / Oct 2026 | 556 / 555 | deceased-voter review |

**What Travis County said about the October 27 files.** The OLRV files come from the state's Texas Election Administration Management (TEAM) system, which is fairly new and has had problems.

- **October 27 initial OLRV** (`Olrv INITIAL`), pulled to complete the ePollbook logic and accuracy test:
  - it was missing the voters of two precinct splits;
  - it included voters with incorrect splits, whom the county removed;
  - P99999 voters were not pulled in;
  - about 4,000 voters with active EDRs on the county's Voter List were not included.
- **October 27 OLRV** (`journal_gen_voter`), the current version of the OLRV:
  - it includes the missing splits;
  - it still showed about 200 voters with the incorrect split, whom the county was updating by hand;
  - it first included P99999 voters, which should not be on the OLRV and were deleted.

**Note:** we do not know what "P99999" refers to. It may be a precinct designation, such as a placeholder precinct for voters not yet assigned, or some other value. No precinct or other field with that value appears in the lists reviewed, so these voters cannot be identified.

**What the lists show:**

**Gaps in the OLRV files:**

- **Sept 16 OLRV:** 12,515 voters on the October 20 Voter List with an EDR on or before September 16 are not on the September 16 OLRV, and 11,207 of them are active. All but 2 of them are on the October 27 OLRV, and 5,642 of them voted in G26.
- **Oct 27 initial OLRV:** 12,176 active voters with an EDR on the October 20 Voter List are not on the October 27 initial OLRV. 11,897 of them are on the October 27 OLRV, 279 are on neither, and 101 of those 279 voted in G26.

**Voters dropped between files:**

- 4,380 voters on the October 27 initial OLRV are not on the October 27 OLRV (the removed incorrect splits and deletions).
- 9,463 voters on the September 16 OLRV are on no October list (section 9.4).

**G26 voters found on each list:**

| List | G26 voters found |
|---|---:|
| September 16 OLRV | 521,414 |
| October 27 initial OLRV | 532,992 |
| October 20 Voter List | 541,432 |
| October 27 OLRV | 541,711 |
| Any of these | 541,964 of 547,076 |

Because the OLRV files had these gaps, this review treats a voter as registered when found on **any** 2026 list. A record's registration date may be the date it was entered in TEAM rather than the date the application was received.

## 4. G26 voters' participation in P26, G24 and P24

| P24 | G24 | P26 | G26 voters |
|---|---|---|---:|
| – | voted | – | 174,399 |
| – | voted | DEM | 110,611 |
| – | – | – | 92,537 |
| DEM | voted | DEM | 69,679 |
| REP | voted | REP | 26,939 |
| – | voted | REP | 18,861 |
| – | – | DEM | 16,944 |
| DEM | voted | – | 12,309 |
| REP | voted | – | 10,997 |
| REP | voted | DEM | 5,403 |
| DEM | voted | REP | 4,744 |
| – | – | REP | 2,210 |
| DEM | – | DEM | 487 |
| DEM | – | – | 482 |
| REP | – | – | 261 |
| REP | – | REP | 151 |
| REP | – | DEM | 42 |
| DEM | – | REP | 20 |
| **Total** | | | **547,076** |

- **Republican 2024, Democratic 2026:** 5,445 G26 voters voted in the Republican primary in 2024 and the Democratic primary in 2026.
- **Democratic 2024, Republican 2026:** 4,764 did the reverse.
- **None of the three:** 92,537 G26 voters (16.9%) voted in none of the three earlier elections.
  - Most are new registrants or voters new to Travis (section 5).
  - Among all G26 voters, 34,966 have VUIDs from the newest numbering range (33xxxxxxxx).
- **Counting the county or SOS files** (548,270 G26 voters) changes these counts by under 1%:
  - P26: Democratic 203,986, Republican 53,102.
  - G24: 434,992.
  - P24: Democratic 87,806, Republican 43,831.

## 5. The 92,537 G26 voters who did not vote in P24, G24 or P26

92,537 G26 voters (16.9%) are on none of the rosters for the March 2024 primaries (county or SOS, section 2.1), the November 2024 general election or the March 2026 primaries. This section looks at them in the registration lists.

**How the comparison was made:**

- **The comparison group:** the group is compared with the 454,539 "returning" G26 voters who voted in at least one of the three.
- **Addresses:** residence addresses come from the October 20 Voter List.
- **Property records:** each address was matched to the Travis Central Appraisal District (TCAD) 2026 property records.
  - The property list is `travis_properties_2026.csv`, derived from the TCAD appraisal exports.
  - The matching uses the rules of `Scripts/Travis_County_Voter_Registration/process_registered_voters_with_property_data.py`, the same address normalization and the same residential / non-residential test.
- **Privacy:** voter names and street addresses are not reproduced. Properties are described by TCAD property type, ZIP code and precinct.

### 5.1 Why they had no earlier vote in Travis

Most of them could not have voted in those elections in Travis, because their registration took effect later.

The tables compare the group with **returning voters**: the 454,539 G26 voters who voted in at least one of P24, G24 and P26. Both groups are on the county's G26 roster and together make up its 547,076 voters with a VUID. Percentages are of each group.

| Effective registration date (October 20 Voter List) | Did not vote in P24, G24, P26 | Returning voters |
|---|---:|---:|
| After the P26 primary (March 3, 2026): could not vote in any of the three | 34,808 (37.6%) | 1,639 (0.4%) |
| Between G24 and P26: could vote only in P26 | 20,849 (22.5%) | 18,074 (4.0%) |
| Between P24 and G24: could vote in G24 and P26 | 3,941 (4.3%) | 37,753 (8.3%) |
| By the P24 primary (March 5, 2024): could vote in all three | 27,635 (29.9%) | 396,733 (87.3%) |
| Not on the October 20 Voter List (section 9.1) | 5,304 (5.7%) | 340 (0.1%) |

- **Effective date:** the effective date is the date of the voter's current Travis registration. A voter who moved from another Texas county may have voted there; those votes are not in the Travis rosters.
- **No earlier vote despite being eligible:** 27,635 voters (29.9% of the group) were registered in Travis before March 2024 and still voted in none of the three earlier elections. These are infrequent voters who turned out in G26.

| Profile | Did not vote in P24, G24, P26 | Returning voters |
|---|---:|---:|
| VUID from the newest range (33xxxxxxxx: first registered in Texas recently) | 33.3% | 0.9% |
| Age 18-21 on Election Day | 17.3% | 2.9% |
| Age 22-29 | 26.0% | 10.7% |
| Age 65 or older | 7.1% | 22.3% |
| Voted on Election Day | 31.7% | 15.9% |
| Voted early in person | 65.2% | 80.8% |
| Voted by mail | 3.1% | 3.3% |
| In suspense on the October 27 OLRV | 4.6% | 2.8% |
| Not on the October 27 OLRV | 5.4% | 0.1% |

- **Young and new:** the group is young and new to Texas voter rolls.
- **How they voted:** like returning voters, most of the group voted early in person (65.2%).
  - **Election Day:** the group's share voting on Election Day is twice the returning voters' share: 31.7% against 15.9%.
  - **Early voting:** its share voting early in person is lower: 65.2% against 80.8%.
  - **What "more likely" means:** a voter in the group was about twice as likely as a returning voter to vote on Election Day. It does not mean more of the group voted on Election Day than early.

### 5.2 Where they live

**Spread:** the group is spread across the whole county.

- 296 of the 298 precincts and 64 of the 68 ZIP codes with G26 voters have some. The exceptions have only 9 and 30 G26 voters in all.
- **By precinct:** in precincts with at least 300 G26 voters, their share ranges from 5.7% to 64.0%, against 16.1% county-wide.
- **Top 10 precincts:** the 10 precincts with the most hold 11.0% of the group, against 5.5% of all G26 voters.

**Concentrations:**

- **The University of Texas campus area.**
  - ZIP 78705 (West Campus and the UT residence halls) has 7,530 of the group: 39.8% of its G26 voters.
  - Precincts 312 (64.0%), 315 (48.3%), 311 (41.0%), 313 (35.1%) and 314 (34.3%) cover the campus and West Campus.
- **East and southeast Travis County:**

  | ZIP | Share of G26 voters in the group |
  |---|---:|
  | 78741 | 24.5% |
  | 78724 | 24.6% |
  | 78744 | 22.3% |
  | 78617 | 25.9% |
  | 78653 | 21.5% |
  | 78610 | 26.9% |
  | 78621 | 24.4% |

- **Downtown:** 78701, 25.6%.
- **Fewest:** the western ZIPs, 78733, 78737, 78739 and 78620, at 8.4-9.5%.

ZIP codes with the most voters in the group:

| ZIP | Did not vote in P24, G24, P26 | All G26 voters | Share |
|---|---:|---:|---:|
| 78705 | 7,530 | 18,912 | 39.8% |
| 78660 | 7,094 | 43,174 | 16.4% |
| 78704 | 3,719 | 22,303 | 16.7% |
| 78745 | 3,703 | 26,293 | 14.1% |
| 78744 | 3,521 | 15,804 | 22.3% |
| 78748 | 3,399 | 24,383 | 13.9% |
| 78653 | 3,022 | 14,061 | 21.5% |
| 78758 | 2,764 | 13,546 | 20.4% |
| 78741 | 2,741 | 11,174 | 24.5% |
| 78759 | 2,557 | 19,903 | 12.8% |

### 5.3 What kind of address they are registered at

| Residence address matched to TCAD | Did not vote in P24, G24, P26 | Returning voters |
|---|---:|---:|
| Residential property | 77,003 (83.2%) | 429,287 (94.4%) |
| Not matched to a TCAD property | 6,727 (7.3%) | 18,470 (4.1%) |
| Non-residential: parcel with no improvement recorded | 3,134 (3.4%) | 5,020 (1.1%) |
| Non-residential: commercial or industrial improvement | 350 (0.4%) | 784 (0.2%) |
| Confidential (protected address) | 19 | 638 |
| Not on the October 20 Voter List | 5,304 (5.7%) | 340 (0.1%) |

**Multi-unit housing:** more of the group live in large multi-unit buildings.

- **Big buildings:** 26.0% of them are at a building where 25 or more of the group are registered, against 16.1% of returning voters at buildings where 25 or more returning voters are registered.
- **Building types:** among residential matches, the group makes up a larger share of G26 voters in some types.

  | Building type | Share of G26 voters in the group |
  |---|---:|
  | Luxury high-rise apartments (100+ units) | 37.1% |
  | Apartments with 100+ units | 33.5% |
  | Mobile homes | 32-36% |
  | Single-family homes | 11.6% |

**Unmatched and "no improvement" addresses:**

- **What they are:** most are student housing and large apartment complexes, not commercial sites.
  - The largest are University of Texas residence halls (one campus address alone has 1,047 of the group).
  - The others are private West Campus dormitories, state-exempt parcels, and new apartment complexes that the 2026 export still records as land.
- **Why they show as non-residential:** the matching script classifies a parcel with no improvement record as non-residential. Campus housing on state land is exempt and has no TCAD improvement record, so it either matches such a parcel or does not match at all.

**Commercial or industrial properties.** 350 voters in the group (0.4%) are registered at a property whose TCAD improvement is commercial or industrial. That is about twice the rate of returning voters (0.2%). They are listed by VUID and TCAD property ID in the records file (A10). Three properties account for 154 of them. Each has hundreds of registered voters, so they were looked up in the TCAD 2026 certified appraisal export:

| TCAD property (type) | ZIP | Did not vote in P24, G24, P26 | All G26 voters there | All registered voters there | TCAD record |
|---|---|---:|---:|---:|---|
| 283712 (small store, under 10,000 sq ft) | 78741 | 73 | 189 | 530 | Owner: 1620 East Riverside Drive LLC, in care of AMLI Residential. An apartment property: the 530 registrations use 299 different unit numbers. The parcel carries the retail improvement; the same owner holds a second parcel (283714). |
| 937607 (industrial, 20,000+ sq ft) | 78745 | 47 | 154 | 256 | Owner: MTP-St Elmo Market LLC. The "market unit" of the St. Elmo Market Place Master Condominium, a mixed-use development. The same address has an "APARTMENT 100+" parcel (937606). The 256 registrations use 204 unit numbers, so these voters live in the apartments. |
| 171435 (medical office, 10,000-35,000 sq ft) | 78759 | 34 | 49 | 677 | Owner: Mosaic Church Austin. 662 of the 677 registrations have no unit number. 601 of them are on the Travis homeless-voters list (`TRAVIS-OLRV-Homeless Voters.csv`), the most common registration address on that list. |

- **Apartments:** two of the three are apartment buildings. The voters' street address matched a commercial parcel at the same address (an address-matching limitation).
- **Homeless registration address:** the church is used as a registration address by voters experiencing homelessness. The registrations at this address go back to 2019; 49 of these voters cast a G26 ballot, and 34 of them are in the group.
- **Context:** shelters, retirement homes, condominiums and similar residences are legitimate registration addresses. A voter experiencing homelessness may register at the place where they regularly stay or receive services.
- **Next step:** the other commercial-coded addresses in A10 are smaller (fewer than 25 of the group each) and can be reviewed the same way.

### 5.4 Patterns

- **Explained by timing:** 60% of the group registered in Travis after G24, and 38% after the March 2026 primary. That alone explains most of the "no earlier vote".
- **Who they are:** the group is young, with 43% under 30. A third have first-time Texas VUIDs.
- **Where they live:** they are concentrated around the University of Texas and in east and southeast Travis County, mostly in apartments, dormitories and mobile-home communities.
- **Concentrated, but still widespread:** nearly every precinct has some.
- **No sign of mass registration at one address:** the commercial-address share is small (0.4%). The only large concentrations are student housing.
- **The three largest commercial-coded addresses:** on review in TCAD, two are apartment buildings and one is a church used as a homeless registration address (section 5.3).
- **Needing review:** the 5,304 voters not on the October 20 Voter List (section 9.1).

## 6. Primary voters who did not vote in the following general election

| Group | Primary voters (county roster; P24 also the SOS files) | Not on the county's general-election roster | Not on the county roster or the SOS file |
|---|---:|---:|---:|
| P26 Republican → G26 | 56,729 | 3,804 | **3,766 (6.6%)** |
| P26 Democratic → G26 | 217,010 | 13,844 | **13,689 (6.3%)** |
| P24 Republican → G24 | 52,704 | 2,535 | **2,492 (4.7%)** |
| P24 Democratic → G24 | 101,989 | 4,598 | **4,445 (4.4%)** |

How they voted in the primary:

| Group | Early in person | Election Day | Mail |
|---|---:|---:|---:|
| P26 Republican, not G26 | 1,841 | 1,771 | 154 |
| P26 Democratic, not G26 | 7,022 | 6,142 | 525 |
| P24 Republican, not G24 | 957 | 1,420 | 115 |
| P24 Democratic, not G24 | 1,660 | 2,391 | 394 |

Age on November 3, 2026 (from the October 27 OLRV; "no record" = not on that list):

| Group | 18-29 | 30-44 | 45-64 | 65-79 | 80+ | no record |
|---|---:|---:|---:|---:|---:|---:|
| P26 Republican, not G26 | 530 | 777 | 963 | 689 | 305 | 502 |
| P26 Democratic, not G26 | 4,366 | 3,607 | 2,131 | 1,229 | 461 | 1,895 |
| P24 Republican, not G24 | 239 | 271 | 362 | 293 | 116 | 1,211 |
| P24 Democratic, not G24 | 600 | 910 | 512 | 420 | 164 | 1,839 |

## 7. Finding these voters in the registration lists

**P26 primary voters not on a Travis G26 roster** (first category that applies):

| Where found | Republican | Democratic |
|---|---:|---:|
| Voted in G26 in another tracked county (moved) | 672 | 3,129 |
| On the October 27 OLRV, active | 2,565 | 8,628 |
| On the October 27 OLRV, in suspense | 84 | 667 |
| On a fall 2026 list (Sept 16, Oct 5 or Oct 20) but not the October 27 OLRV | 177 | 449 |
| On the July 2026 list only (removed before the fall) | 112 | 406 |
| On no 2026 list after the primary | 156 | 410 |
| **Total** | **3,766** | **13,689** |

**P24 primary voters not on a Travis G24 roster** (first category that applies):

| Where found | Republican | Democratic |
|---|---:|---:|
| Voted in G24 in another tracked county (moved) | 692 | 1,117 |
| On the October 24, 2024 list, active | 1,378 | 2,643 |
| On the October 24, 2024 list, in suspense (returned mail, SOS data or jury non-residence) | 55 | 102 |
| Not on the October 24, 2024 list | 367 | 582 |
| On the Elly deceased-voter list | 0 | 1 |
| **Total** | **2,492** | **4,445** |
| Still registered in Travis on the October 27 OLRV | 1,258 | 2,571 |

- **Moved:** a sizeable share of the non-returning primary voters voted in another county in the general election:

  | Group | Share that voted in another county |
  |---|---:|
  | P26 Republicans | 18% |
  | P26 Democrats | 23% |
  | P24 Republicans | 28% |
  | P24 Democrats | 25% |
- **The rest:** most were still registered in Travis and did not vote. A smaller number had been removed from the list or put in suspense.

## 8. Mail voting and mailing addresses outside Texas

**The non-returning primary voters who voted by mail in the primary:**

| Group | Voted by mail in the primary | Outside Texas, list at the time (P26 list / Oct 2024 list) | Outside Texas, October 27 OLRV | Not on the Oct 2026 lists |
|---|---:|---:|---:|---:|
| P26 Republican, not G26 | 154 | 0 | 5 (AZ, MD; military AE; abroad 2: Thailand, United Kingdom) | 41 |
| P26 Democratic, not G26 | 525 | 0 | 27 (FL, HI, KY, NV, OK, WA; military 4; abroad or U.S. territory 17: United Kingdom 5, Fiji 2, France 2, Guam 2, Spain 2, and one each in Germany, Indonesia, the Netherlands, Portugal) | 112 |
| P24 Republican, not G24 | 115 | 3 (CA, NC, OK) | 9 (CA, NC, NM 2, OK, VA; military AP 2; abroad 1: Germany) | 60 |
| P24 Democratic, not G24 | 394 | 4 (GA, MA, NY, WI) | 6 (NY, WI; abroad 4: Australia, Portugal, 2 with no country code) | 203 |

**All G26 mail voters (17,854)**, by registration mailing address:

| Mailing address | Voters |
|---|---:|
| Texas, or no separate mailing address | 15,909 |
| Foreign country | 928 |
| Another U.S. state | 242 |
| Military (APO/FPO: AE 59, AP 10, AA 5) | 74 |
| Masked (protected address) | 8 |
| Not on the fall 2026 lists (see section 9.1) | 693 |
| **Total** | **17,854** |
| **Outside Texas (foreign, another state or military)** | **1,244** |

- **Foreign countries:** 70 countries. The most common are the United Kingdom 106, Mexico 94, Spain 79, France 73, Germany 63, Canada 57 and Portugal 50, then Italy 30, the Netherlands 28, Australia 26, Japan 24, Switzerland 24 and New Zealand 23. 20 have no country code on the Voter List.
- **Other states:** the most common are Virginia 36, California 33, Maryland 18, Colorado 16, New York 13, the District of Columbia 12 and North Carolina 11, then 29 other states with 1-9 each.
- **Lawful reasons:** mail voting from outside Texas is lawful for voters who are temporarily away, such as students, military personnel and their families, or travelers (Election Code §82.001).
- **Residence:** an out-of-state mailing address does not show that the voter has given up Texas residence.

**All registered voters** on the October 27 OLRV with a mailing address outside Texas:

| Group | Registered | Voted in G26 | By mail | Early in person | Election Day |
|---|---:|---:|---:|---:|---:|
| Foreign country | 2,200 | 1,460 | 923 | 445 | 92 |
| Another U.S. state | 4,248 | 680 | 239 | 317 | 124 |
| Military (APO/FPO) | 422 | 97 | 73 | 20 | 4 |

## 9. Anomalies in the G26 data

### 9.1 Voters on no 2026 registration list (4,938; records A1)

4,938 G26 voters with a VUID are on none of the 2026 Travis lists in section 3. That includes the county's own Voter Lists of October 5 and 20, as well as the TEAM OLRV files of September 16 and October 27. These voters are:

- 0.9% of all G26 voters;
- 2,228 early in person, 2,147 on Election Day and 563 by mail;
- in the state's files too, for 4,793 of them (145 are on the county roster only).

Pointers to the cause:

- **New VUIDs:** 2,996 have VUIDs from the newest range (33xxxxxxxx), against 6% of all G26 voters.
- **Late in the voting period:** 3,106 voted on October 30, the last day of early voting, or on Election Day.
- **New to Travis:** only 124 had voted in Travis in P24, G24 or P26, and 140 are on a 2024-25 Travis list.
- **Not one of the OLRV gaps the county described:** these voters are not on the county's Voter List either.

The pattern fits registrations or transfers from other counties that were processed or entered in TEAM after October 20. It does not show ineligible voting. It should be checked against the county's post-election registration list: a G26 voter still on no Travis list after the canvass would need an explanation.

**The same gap in earlier elections.** No registration list on file has a record for every G26 voter. To see whether that is unusual, the G24 and P26 rosters (county or SOS) were compared in the same way with the list in use at the time. Missing voters were then looked up on every later Travis list through October 27, 2026: 24 lists in all, from March 2024 on.

| | G24 | P26 |
|---|---:|---:|
| Voters on the roster (VUIDs) | 584,611 | 274,689 |
| List(s) at the time | the county's "2024-10-24" lists | the county's February 10, 2026 list and the P26 OLRV |
| **Missing from the list at the time** | **8,804 (1.5%)** | **1,503 (0.5%)** |
| Found on a later list | 4,682 | 1,433 |
| On no later list | 4,122 | 70 |
| The county's later vote history credits the vote | 4,056 | 1,397 |

**Limited ballots are outside these counts.** Limited-ballot voters moved from another Texas county and are not on the Travis list, so none of the three comparisons includes them:

- **G24:** 2,603 limited ballots were counted, but the G24 roster has no limited-ballot records.
- **P26 and G26:** the rosters list limited-ballot voters by name only, without a VUID: 443 in P26 and 3,482 in G26.

**P26:** the gap closed within weeks.

- **Most reappeared on the next list:** 1,425 of the 1,503 are on the P26 runoff OLRV of May 14, 2026.
- **The list at the time was the official one:** the P26 OLRV has 920,114 records, exactly the official registered-voter count for P26.
- **Who they are:** mostly early (940) and Election Day (495) voters, many with new VUIDs (33xxxxxxxx).
- **Registration timing:** among the 1,433 found later, 1,271 had applied by the February 2 registration deadline and 147 after it but before Election Day.

**G24:** two different groups.

- **4,682 found on a later list:**
  - **New registrations (3,563):** mostly early and Election Day voters; 2,927 had applied by the October 7 deadline.
  - **Absent from the October list only (1,119):** on the March 2024 list and on later lists.
    - 1,045 of these are mail voters listed only in the SOS file, not on the county's roster.
    - The county's own later vote history credits 1,077 of the 1,119 with a G24 vote.
  - **When they reappear:** most first reappear on the October 25, 2025 list, because no list between October 2024 and October 2025 is on file.
- **4,081 on no Travis list at all:** from March 2024 through October 27, 2026.
  - **Who they are:** 4,012 are mail voters, and 4,054 have VUIDs beginning with 22.
  - **When their ballots arrived:** they make up almost all of the county's earliest mail lists, before regular mail ballots went out: 87 of 88 voters on September 30, all 216 on October 7 and all 41 on October 8. They are most of the lists after Election Day: 110 of 135 on November 7 and 46 of 53 on November 8. 156 of their mail ballots were logged after Election Day.
  - **G26:** 4,009 of them did not vote in G26.
  - **Likely explanation (not confirmed by the county):** this fits overseas and military voters registered by Federal Post Card Application, whom the county's published lists appear not to include.
- **The G24 list was older than its name:**
  - **Its real date:** the files named "2024-10-24" carry a file date of October 14, 2024, and their G24 vote history ends with mail ballots received October 11.
  - **What that means:** they show the list about three weeks before Election Day, a week after the registration deadline, so registrations processed after that are missing.

**What this means for G26:** the 4,938 G26 voters (0.9%) look like the P26 group and the G24 new registrations, not the G24 mail group.

- **Who they are:** mostly early and Election Day voters with new VUIDs.
- **Comparison with G24:** without the 4,081 never-listed mail voters, G24's gap is 4,723 voters (0.8%), close to G26's.
- **What to expect:** if G26 follows P26, most of the 4,938 should appear on the first Travis list after the election.

**History files not refreshed:** the county's `Registered_Voters_With_History` downloads dated October 25, 2025, January 17, 2026 and February 10, 2026 are byte-for-byte the same file. The county did not refresh it for four months.

### 9.2 Registration dated after the deadline (558; records A2)

558 G26 voters are on the October 20 Voter List with a registration date after October 5, 2026 (the registration deadline), and on no earlier Travis list.

- **How they voted:** 419 early in person, 131 on Election Day and 8 by mail.
- **Effective date:** 149 of them have an EDR after November 3.
- **VUIDs:** 139 have new-range VUIDs. Most of the others have older VUIDs, so they were probably registered elsewhere in Texas before.

**Caution:** with TEAM's problems, this registration date may be the date the record was entered, not the date the application was received or postmarked. An application received by October 5 but entered later would look like this. These records need the county's application dates before any conclusion.

### 9.3 Elly deceased-voter list (6; records A3)

Six G26 voters are on `TRAVIS-VR 5_1_26 Deceased Voters.csv`:

- **The list:** generated on May 1, 2026 by Elly (<https://theelectorlist.com/>), a third-party review of the Travis voter list. It was not provided by Travis County or the State, and it is not a county or state finding. For these six it gives a finding of "Deceased" from an obituary, dates of death from 2015 to 2021, and registration status Active.
- **The match:** the name and date of birth on that list match the voter's Travis registration.
- **The votes:** one by mail (October 19), four early in person (October 20-26) and one on Election Day.

An obituary match can be wrong: a person with the same name and birth date, or a wrong obituary. These six need verification by the county before any conclusion.

### 9.4 On the September OLRV but on no October list (99; records A9)

99 G26 voters are on the September 16 OLRV but on none of the October lists (October 5, October 20, and the October 27 initial OLRV and the October 27 OLRV).

- **How they voted:** 72 by mail, 23 early in person and 4 on Election Day.
- **Status on Sept 16:** 22 were in suspense.
- **Interpretation:** their registrations appear to have been cancelled or moved after September 16, yet a ballot was counted. The removal may have been one of the TEAM corrections the county describes. The county should confirm each voter's registration status on the day the ballot was cast.

### 9.5 Other findings

| Finding | Voters | Records |
|---|---:|---|
| Also on another county's own G26 roster (Bexar 2, Collin, Dallas, Harris 2, Hidalgo) | 7 | A6 |
| Also in another county's G26 SOS file, but not on that county's roster | 81 | A7 |
| Two voting methods on the county roster (early in person + mail 2, Election Day + mail 1) | 3 | A5 |
| Mail ballot listed as received on two different days | 6 | A8 |
| Listed twice on the same day | 3 | A8 |
| Age 105 or older on Election Day (registration birth date) | 4 | A4 |
| Under 18 on Election Day | 0 | |

- **Another county's own roster:** for the 7 voters on another county's own roster, both counties' records should be compared, starting with the dates and voting methods in the records file.
- **Another county's SOS file only:** the 81 SOS-file-only cases are more likely SOS file errors, since the state's file lists a voter the county did not.
- **Ages:** ages of 105 and over are plausible, but birth dates entered as placeholders show up this way.
- **One woman is listed as 118 years old** (VUID 1167992321, records A4):
  - **What the records show:** the same birth date appears on all four registration lists that carry one (the September 16 OLRV, the October 27 initial OLRV, the October 27 OLRV and the P26 primary list).
    - The registration is active and not in suspense.
    - No other Travis registration has the same first and last name.
  - **How she voted:** early in person, on October 28, 2024 (G24) and again on October 28, 2026 (G26). Both the county roster and the SOS files list both votes.
  - **No supercentenarian record:** LongeviQuest (longeviquest.com) tracks validated supercentenarians, people aged 110 or older. A search there found no record of her.
    - Only a handful of people have ever been documented as reaching 118.
  - **Two possibilities:** either the registration's birth date is wrong, or someone else voted in person as her.
  - **Recommendation:** the county should verify the birth date on the registration. It should also review the G24 and G26 early voting check-in records for this VUID: the polling place, the form of identification shown and the signature.

**Patterns that are not anomalies by themselves:**

- **Duplicate registrations:** 225 people are registered twice on the October 27 OLRV, under two VUIDs (450 VUIDs) with the same first, middle and last name and date of birth.
  - In 165 of these, one of the two VUIDs voted in G26; in none did both.
  - This is list maintenance, not a double vote.
- **Suspense voters:** 17,099 G26 voters (3.1%) were in suspense on the October 27 OLRV. A suspense voter may vote after confirming the address on a Statement of Residence.
- **Mail voters under 65:** 2,953 of the 17,155 mail voters with a known age were under 65 on Election Day.
  - They must qualify by disability, absence from the county, or confinement (Election Code §§82.001-82.008).
  - The roster does not say which.
- **Limited ballots:** the G26 roster lists 3,482 limited-ballot voters (3,477 limited ballots counted, unofficial). The county lists these voters by name only, so they cannot be checked here.
- **9-digit IDs:** two mail rows on the county roster carry 9-digit IDs. Both look like clerical errors: each is a VUID with one digit dropped.
  - **ID 220260776 (October 29 mail list):** restoring a "7" gives VUID 2202607776, a registration with the same name in the same precinct (385).
    - The SOS file lists 2202607776 as a G26 mail voter; the county roster does not, because of the typo.
    - This was reported to Travis County Elections.
  - **ID 113332465 (October 31 mail list):** adding a final "1" gives VUID 1133324651, a registration with the same name in the same precinct (232).
    - Neither the county roster nor the SOS files list 1133324651.
  - **Effect on this review:** neither row is counted among the 547,076 G26 voters with a VUID.

## 10. Checks

- **Vote history:** the county rosters agree with the county's own vote history in the July 6, 2026 list.

  | Election | Agree | History entry, no roster record | Roster record, no history entry | Party differs |
  |---|---:|---:|---:|---:|
  | P26 | 273,293 (same party) | 1,232 | 22 | 7 |
  | G24 | 556,039 | 4,514 | 1,359 | |
  | P24 | 145,511 (same party) | | | 1 |

- **County vs state:** the state's files list 98.8% (P24), 99.1% (G24), 99.1% (P26) and 99.2% (G26) of the voters on the county's rosters.
- **Mail voters outside Texas:** an independent comparison in Election Explorer matched the G26 mail roster with the October 27 OLRV records whose mailing address is not in Texas. It found 1,232 voters.
  - For the same voters, the October 27 OLRV has 1,243 records without a Texas mailing address: 312 in another state or military, 923 foreign and 8 masked.
  - The 1,244 in section 8 is the 1,235 non-masked OLRV records plus 9 voters who are not on the October 27 OLRV, taken from earlier lists.
  - The remaining gap of 11 likely reflects how the two methods pair records. The roster lists 8 VUIDs twice.
- **Ballot counts:** the replayed tabulation audit logs equal the ballots tabulated in P24 (153,590), G24 (587,090) and P26 (274,443), by vote type. The Central Count reconciliation records agree, once the 1,553 hand-counted P24 Republican mail ballots are added (section 2.1).
- **Same-person rule:** two registrations are treated as the same person only when the first, middle and last name and the date of birth all match (Election Explorer's rule). Twins can differ only in the middle name.

## 11. Limitations

- **Registration lists:** the newest lists (the October 27 initial OLRV and the October 27 OLRV) were provided October 27, 2026. TEAM problems are documented for them (section 3). Registrations processed or entered after that date look missing (section 9.1).
- **Registration dates:** a registration date may be a TEAM entry date (section 9.2).
- **P24 rosters:** the county's P24 Election Day roster lacks 2,455 voters, and the SOS P24 files lack 1,761 mail voters. P24 voters are therefore taken from both (section 2.1).
- **Other counties:** only the 50 largest Texas counties were checked for other-county votes.
- **Property matching:** matches use the TCAD 2026 property list and the county's address rules.
  - A parcel with no improvement record is classified non-residential, even when it is campus or new apartment housing.
  - 7.3% of the group's addresses could not be matched.
  - A property's type does not by itself show whether a voter lives there.
- **Mailing address:** this is the registration mailing address, not necessarily the address on the mail ballot application.
- **G26 rosters are not final:** they will still change with late mail ballots, provisional ballots and corrections.
- **This review reports patterns that need checking, not findings of wrongdoing.**
