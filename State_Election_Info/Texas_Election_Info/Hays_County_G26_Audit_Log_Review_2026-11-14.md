# Hays County November 2026 General Election: Audit Log Compared with the Official Results

**Election:** November 3, 2026 General Election (G26), Verity election ID 2308\
**County:** Hays County, Texas (Hart InterCivic Verity)\
**Data:**

- **The VerityCount audit log:** `G26_Verity_AuditLogReport - Audit Log Report 20261103 General.pdf`, run November 9, 2026 at 5:39 PM. It is 178 pages and 3,018 rows.
- **The official canvass results** (run November 9, 2026): the Cumulative Results Report (5:35 PM) and the Precinct Results Report (5:37 PM). These are the same reports as in the [CVR review](Hays_County_G26_CVR_Review_2026-11-10.md).
- **The county's cast vote record export,** `G26 CVR.zip`. Its batch numbers tie the central-count reads to the ballots they carried.

**Date:** November 14, 2026

This review compares counts and event records only. The log holds no voter identity, and none is reported here.

## 1. Summary

**Every ballot in the official results is accounted for by the audit log.** VerityCount read 63 vDrives into the official task, holding 120,880 ballots. That is the official ballots cast.

| Source of the ballots | vDrives | Read | Ballots |
|---|---:|---|---:|
| Early voting sites (14) | 21 | 11/2, 12:34–12:45 PM | 95,087 |
| Election Day polling places (34) | 39 | 11/3, 8:30–11:11 PM | 21,280 |
| Central count (VerityCentral) | 3 | 11/2 12:33 PM; 11/3 7:40 PM; 11/9 4:16 PM | 4,513 |
| **Total** | **63** | | **120,880** |
| **Official results: ballots cast** | | | **120,880** |

- **The registered voters match too:** the 160 precinct splits loaded on 10/26 add up to 204,327, the official registered voters.
- **The central-count reads match the CVR batch for batch.** CVR batches 1–74 hold 3,254 ballots, 75–107 hold 819, and 108–130 hold 440: the three central reads exactly (section 3.2). Batch numbers 33, 34 and 120 are not in the CVR (a question below).
- **The write-in decisions in the log match the CVR, contest by contest** (section 4):
  - **Polling-place ballots:** the log records 1,131 write-in decisions on Election Night, one per write-in mark. These are exactly those ballots' marks: U.S. Senator 174, District 27 24, Governor 91, Lieutenant Governor 37, Woodcreek 805.
  - **Central-count ballots:** their 90 marks carry no decisions in this log. Central-count write-ins are resolved in VerityCentral, whose log was not provided.
- **No results were output early.** None was printed, saved or exported before the polls closed while ballots were loaded.
  - **10/21:** the one output in the official task before Election Day was a zero report. Nothing had been read.
  - **Early count:** early voting and the first mail ballots were read on 11/2 at 12:33–12:45 PM. Nothing more happened in the program until 7:04 PM on 11/3.
  - **First report:** printed at 7:05 PM on 11/3, after the polls closed.
- **No errors were logged.** No vDrive was rejected, no manual vote record was made, and no clock was off.
- **Logic and accuracy (L&A) tests ran before the early count and after each counting day** (section 5). The log shows no test between the 11/2 count and the Election Night count. It shows none on 11/9 before the canvass read either.

**Vote types.** The polling-place vDrives come 176 short of the official early-voting column, and 75 short of the Election Day column. The central-count vDrives carry 251 more ballots than the official mail column (4,513 against 4,262). So the central count also scanned 251 ballots reported as in-person votes: 176 early voting and 75 Election Day. These are most likely accepted provisional ballots, which are scanned centrally (section 3.3).

**Questions for the county:**

1. **Provisional ballots:** do the 251 central-count ballots in the in-person columns (176 early voting, 75 Election Day) equal the provisional ballots accepted? Or do they include other ballots scanned centrally, such as ballots duplicated by the ballot board?
2. **Missing batch numbers:** VerityCentral batch numbers 33, 34 and 120 are not in the CVR. Were these batches started and then discarded, and were their ballots rescanned in other batches? The VerityCentral audit log would show this.
3. **Testing:** the log shows no test task read between the 11/2 early count and the 11/3 Election Night count, or before the 11/9 canvass read. Was a test run some other way before those counts, outside VerityCount?
4. **Other logs:** please provide the VerityCentral audit log, and if possible the VerityData and Verity Build logs. VerityCentral's log would confirm the vDrive transfers, the write-in decisions on central-count ballots and the missing batch numbers. The Verity Build log would show any ballots printed after Election Day; Hays's G24 log had them.

## 2. Data and method

**The audit log.** The file is VerityCount's report for election 2308. Its rows run from 1 to 3,018 with none missing:

| Log | Run | Pages | Rows | Earliest event | Newest event |
|---|---|---:|---:|---|---|
| VerityCount 2.7.5 | 11/9/2026 5:39 PM | 178 | 3,018 | 9/15/2026 11:47 AM | 11/9/2026 5:38 PM |

- **Coverage:** the log runs to the official canvass exports at 5:36–5:38 PM on 11/9, just before the report was run. It covers the whole election.
- **One workstation:** every event is from VerityCount workstation D2500350903.
- **Two user accounts:** `cliff` (544 events) and `jeff` (2,474 events). Two login attempts failed (9/15 and 11/3), and each was followed by a successful login.
- **Times:** the log gives each time with its zone (CDT through 11/1, CST after). Times here are local.
- **The file was printed again from a web browser.** Its PDF was made on 11/13 by Firefox, so it is not the Reporting Services PDF that Verity writes. The XML signature Hays posted with its earlier logs did not come with it.
  - **What the reprint changed:** each letter is drawn on its own, and spaces are not drawn.
  - **What it kept:** the page layout is the same as Verity's own report.

**How the log was read:**

- **Election Explorer:** the release version (1.6.0.0) could not read the file, because of the browser reprint. It was fixed on November 14, 2026 (section 8).
- **A second reader** (PyMuPDF) read the same PDF column by column. All 3,018 rows agree with Election Explorer's in every field, word spaces included.
- **The ballot accounting:** VerityCount's "vDrive read" events (State Read), by the task each was read into. These are the rules that gave the official totals in the Hart elections checked before (Parker and Burnet).
- **The CVR:**
  - **Ballots by batch:** a separate script counted the CVR's ballots (first sheets) and write-in marks by batch number.
  - **Polling-place ballots:** these records carry no batch number. They number 116,367, the early-voting and Election Day vDrives' total.

**Not provided:** the VerityCentral (central scanner), VerityData and Verity Build logs.

## 3. Ballot accounting

### 3.1 Tasks

A VerityCount task is a results container. The county used one official task and five test tasks:

| Task | Type | Created | vDrives read | Ballots |
|---|---|---|---:|---:|
| L&A Pre-Test 15 Sep 2026 | Test | 9/15 11:47 AM | 3 | 300 |
| L&A Main Test 16 Sep 2026 | Test | 9/15 11:48 AM | 5 | 300 |
| **November 3, 2026 General Election** | **Official** | **10/21 11:45 AM** | **63** | **120,880** |
| L&A Test #2 2 Nov 2026 | Test | 11/2 12:02 PM | 5 | 300 |
| L&A Test #3 3 Nov 2026 | Test | 11/3 11:33 PM | 5 | 300 |
| L&A Test #4 9 Nov 2026 | Test | 11/9 3:59 PM | 5 | 300 |

Every test read was marked "Media type: Test", and every official read "Media type: Official". No vDrive was read into the official task twice.

### 3.2 The official task, read by read

**November 2, 12:33–12:45 PM: 98,341 ballots.**

- **Central count:** one vDrive of 3,254 ballots.
- **Early voting:** 21 vDrives from 14 sites, 95,087 ballots. This includes the limited-ballot vDrive (1,132, matching the official limited ballots).
- **Two vDrives each** at seven sites:

| Early voting site | vDrives | Ballots |
|---|---|---:|
| Buda City Hall and Library | 6,812 + 6,488 | 13,300 |
| Hays County Election Center | 6,268 + 6,501 | 12,769 |
| Patriots' Hall of Dripping Springs | 1,160 + 9,905 | 11,065 |
| Kyle City Hall | 9,296 + 1,177 | 10,473 |
| Precinct 4 Office Hays County | 7,878 + 898 | 8,776 |
| Wimberley Community Center | 2,937 + 2,984 | 5,921 |
| Sunfield Station | 4,024 + 927 | 4,951 |

**November 3, 7:40–11:11 PM: 22,099 ballots.**

- **Central count:** one vDrive of 819 ballots at 7:40 PM.
- **Election Day:** 39 vDrives from 34 polling places, 21,280 ballots, from 8:30 PM to 11:11 PM.
- **Two vDrives each** at five polling places: Hays County Election Center, Buda City Hall and Library, Wimberley Community Center, Gemstone Palace, and Dripping Springs Ranch Park and Event Center.
- **Buda's two:** both hold 870 ballots. They are different vDrives (each has its own ID), so both count.

**November 9, 4:16 PM: 440 ballots.** One central-count vDrive, read at the canvass.

**The central-count reads and the CVR.** VerityCentral numbers its batches in scanning order. Taken in that order, the CVR's batches fall exactly into the three central reads:

| Central read | Ballots read | CVR batches | CVR ballots | Federal overseas ballots | Write-in marks |
|---|---:|---|---:|---:|---:|
| 11/2 12:33 PM | 3,254 | 1–74 (72 batches; no 33, 34) | 3,254 | 67 | 65 |
| 11/3 7:40 PM | 819 | 75–107 (33 batches) | 819 | 17 | 8 |
| 11/9 4:16 PM | 440 | 108–130 (22 batches; no 120) | 440 | 6 | 17 |
| **Total** | **4,513** | **127 batches** | **4,513** | **90** | **90** |

- **Federal overseas:** the 90 ballots match the official "Federal Overseas" unit.
- **Missing numbers:** batches 33, 34 and 120 have no CVR records. With the totals exact, they held no counted ballots. Only VerityCentral's log can say what they were (question 2).

### 3.3 Vote types

VerityCount reports each ballot as absentee (mail), early voting or Election Day. A central-count ballot takes its vote type from its batch, so central-count ballots can land in the in-person columns. These are the official ballots cast by vote type, from the U.S. Senator contest (votes, undervotes and overvotes):

| Vote type | Official ballots | Audit log | Difference |
|---|---:|---|---:|
| Absentee (mail) | 4,262 | central-count vDrives 4,513 | central count +251 |
| Early voting | 95,263 | early voting vDrives 95,087 | official +176 |
| Election Day | 21,355 | Election Day vDrives 21,280 | official +75 |
| **Total** | **120,880** | **120,880** | **0** |

The 251 central-count ballots in the in-person columns are most likely provisional ballots the ballot board accepted (question 1). The CVR records no vote type, so it cannot tell.

## 4. Write-ins

**The log records each write-in decision.** Each mark the county resolved in VerityCount shows as "Write-in assigned" (to a candidate) or "Write-in rejected", with its own write-in ID.

- **When:** all 1,131 were made on Election Night, 11/3, from 7:45 PM to 11:27 PM. They were posted at 8:38 PM ("1943 changes posted") and 11:27 PM ("407 changes posted").
- **One decision per mark:** no ID was decided twice, and the same written name recurs across many IDs.
- **Which ballots:** the decisions match the write-in marks on the polling-place ballots (the CVR records with no batch number), contest by contest. The central-count marks are the rest:

| Contest | Decisions in the log (assigned + rejected) | CVR marks, polling-place ballots | CVR marks, central count | Official marks (CVR review) |
|---|---:|---:|---:|---:|
| U.S. Senator | 174 (60 + 114) | 174 | 13 | 187 |
| U.S. Representative, District 27 | 24 (2 + 22) | 24 | 3 | 27 |
| Governor | 91 (43 + 48) | 91 | 7 | 98 |
| Lieutenant Governor | 37 (3 + 34) | 37 | 2 | 39 |
| City of Woodcreek, City Council at-Large | 805 (758 + 47) | 805 | 65 | 870 |
| **Total** | **1,131** | **1,131** | **90** | **1,221** |

- **Central-count marks:** they carry no decisions here, because VerityCentral resolves them. The 11/9 "Write-ins posted: 17 changes posted" comes right after the canvass read. Its 17 matches the 17 marks in that read's batches.
- **Totals:** each certified write-in candidate's official total is at least the log's assignments to that candidate. Each official undervote count is likewise at least the log's rejections. The differences come only from the central-count marks.
  - **One example:** Lieutenant Governor write-in Arturo Espinosa has 2 official votes and none in the log, so both came from central-count ballots.
- **Not checked:** whether each decision read the written name correctly. That would need the images.

## 5. Logic and accuracy testing

Each test task read the same test deck: 300 ballots on 3 to 5 vDrives. Each was printed before and after its write-ins were posted.

| Test | Run | Counting it is next to |
|---|---|---|
| L&A Pre-Test 15 Sep 2026 | 9/15 3:24–3:35 PM; printed again 9/23 | (before the election) |
| L&A Main Test 16 Sep 2026 | 9/16 11:33–11:57 AM | (before the election) |
| L&A Test #2 2 Nov 2026 | 11/2 12:03–12:17 PM | **before** the 11/2 count (12:33 PM) |
| L&A Test #3 3 Nov 2026 | 11/3 11:33–11:45 PM | **after** the Election Night count (last read 11:11 PM) |
| L&A Test #4 9 Nov 2026 | 11/9 4:24–4:31 PM | **after** the canvass read (4:16 PM) |

**Counting with no test run just before it:**

- **The Election Night count:** it began at 7:40 PM on 11/3. The last test before it was L&A Test #2, on 11/2 before the early count. No test task was read between the two counts.
- **The canvass read:** it came at 4:16 PM on 11/9. The task L&A Test #3 was opened at 1:21 PM that day, but nothing was read into it. The last test read before the canvass was Test #3's, at 11:34 PM on 11/3.

This review reports what the log shows; it does not judge compliance. A test of the scanners or of VerityCentral would not appear in this log (question 3).

## 6. Results output

- **Before Election Day:** all results output came from test tasks, apart from one zero report.
  - **Test tasks:** 10 Cumulative Results prints, 9/15–11/2.
  - **10/21 11:45 AM:** a "Detailed Vote Totals" export in the official task, a minute after it was created and before anything was read.
- **11/2 after the early count:** nothing was printed, exported or even opened until 7:04 PM on 11/3.
- **Election Night:**
  - **7:05 PM:** the first Cumulative Results print, after the polls closed. It was printed again at 7:07, 7:10, 7:11, 7:23 and 7:23 PM, and exported to PDF at 7:11 PM. These carried the 98,341 early and mail ballots read on 11/2.
  - **8:30–11:11 PM:** as the Election Day vDrives were read, Cumulative Results PDFs were exported at 8:36, 9:09, 9:21, 9:55 and 10:08 PM, and printed at 9:10 PM. The 11:29 PM export came after the last read.
  - **11:29–11:32 PM:** seven Cumulative Results prints.
- **11/3 11:54 PM – 11/4 12:22 AM:** Precinct, Canvass and Cumulative Results exports (PDF and CSV), and a Detailed Vote Totals export.
- **11/6 12:15–2:48 PM:** custom filtered reports were created and printed:
  - **Mail:** U.S. Senator, Chief Justice of the Supreme Court and Criminal District Attorney, mail ballots of precincts 128, 216 and 332.
  - **Election Day:** Gemstone Palace, Child Nutrition HCISD Support Services, Dunbar Center.
  - **Early voting:** Kyle City Hall, Patriots' Hall of Dripping Springs, Wimberley Community Center.

  These look like the counts for the partial manual count of selected precincts and polling places (TEC §127.201); the county can confirm.
- **11/9:**
  - **4:18 PM:** "Label Reports As Official" was turned on, after the canvass read.
  - **4:19–4:22 PM:** Cumulative Results were printed three times, then Cumulative and Precinct PDFs were exported.
  - **5:36–5:38 PM:** the official Cumulative, Canvass and Precinct reports were exported as PDF and CSV. These are the reports this review compares with.
- **The audit log itself** was printed seven times: on 11/3 at 7:16, 7:17, 11:49 and 11:51 PM, and on 11/9 at 4:34, 4:39 and 4:41 PM.

## 7. Other events worth noting

- **Reporting status set by hand (11/3, 11:29 PM):** "Federal Overseas-US 27" and "Federal Overseas-US 21" were changed from NotReporting to Reporting.
  - **Ballots at the time:** these units have no polling place, and 84 of their 90 ballots had been read by then.
  - **Effect:** reporting status changes no count. It is why the reports show 90 of 90 precincts reporting.
- **Registered voters (10/26, 5:52 PM):** all 160 precinct splits were set from 0 to their counts, 204,327 in all, the official registered voters. They were not changed again.
- **Write-in candidates:** the official task's 15 certified write-in candidates were added on 10/21. Each test task had 14 or 15 added.
- **vDrive records:** each of the 86 "vDrive read" events has its "vDrive saved", with the same "Time read", logged at most a few seconds later. Unlike Parker P26, none was logged an hour off.
- **Not seen:** errors, rejected vDrives, manual vote records, deleted tasks, and results cleared from a task.

## 8. Notes on Election Explorer

**The reprinted log could not be read by Election Explorer 1.6.0.0.** It said the file "is not a Hart Verity audit log report." Firefox's PDF output differs from the Reporting Services original in two ways:

- **Letters:** it shows each letter as its own piece of text and draws no spaces.
- **Header:** it joins "Component:" and its value ("VerityCount 2.7.5") into one piece.

**The fix (November 14, 2026; not yet committed):**

- **Rebuilding the words:** on a page where most text pieces are single letters, the Hart reader joins the letters of a line back into words, using the glyph widths. A gap wider than 0.15 em is a space; 0.6 em or more starts a new piece.
- **The header:** it accepts "Component:" with or without its value attached.
- **Original reports are unaffected:** every other audit log read before (Travis, Dallas, Parker, Burnet and Hays, 35 sets) loads exactly as before, event for event.
- **The check:** this log's 3,018 rows equal an independent PyMuPDF reading in every field.

**Not shown by Election Explorer:** its reports cover the VerityCount log alone. The CVR batch mapping (section 3.2) and the write-in comparison (section 4) were done by separate scripts.
