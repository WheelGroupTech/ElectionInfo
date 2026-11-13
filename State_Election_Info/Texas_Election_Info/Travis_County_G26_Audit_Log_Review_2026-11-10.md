# Travis County November 2026 General Election: Audit Logs Compared with the Final Results

**Election:** November 3, 2026 Joint General and Special Elections (G26)\
**County:** Travis County, Texas (ES&S Electionware)\
**Data:**

- **Electionware audit reports:** the printed copies of the **Election Audit Events Report** (6 copies) and the **Admin Audit Events Report** (7 copies).
  - Both reports: 10/31 10 AM, 10/31 8:35 PM, 11/2, 11/3 ("All EV"), 11/10 7:17 PM, and the final copies printed 11/12 at 11:19–11:20 AM (named "11.11").
  - Admin report only: one more copy, 11/10 10:04 PM.
- **The final canvass results, printed twice on November 10:**
  - `Results Final 11.10.2026.pdf` (Election Summary, 6:57 PM) and `Results Final Precinct Report 11.10.2026.pdf` (Precinct Summary, 7:03 PM);
  - `Results Final 11.10.2026 9PM.pdf` (9:22 PM) and `Results Final Precinct Report 11.10.2026 9PM.pdf` (9:27 PM);
  - the official results posted on Clarity, whose snapshot is timestamped 9:22:40 PM.

**Date:** November 10, 2026; updated November 12, 2026 with the final audit logs.

## 1. Summary

**Every ballot in the official results is accounted for by the audit logs.**

| Vote type | Audit log: ballots counted | Official results (9:22 PM): ballots cast | Difference |
|---|---:|---:|---:|
| Early voting in person | 430,859 | 430,859 | 0 |
| Election Day | 101,766 | 101,766 | 0 |
| Mail | 19,373 | 19,373 | 0 |
| Provisional (EV 1,735 + ED 496) | 2,231 | 2,231 | 0 |
| **Total** | **554,229** | **554,229** | **0** |

- **The 367 mail ballots this review first reported missing were rescanned and counted.** The first version of this review compared the 7:17 PM logs with the 6:57 PM "Results Final" summary, and found 367 mail ballots loaded but not in the results. The final logs show what happened next:
  - **5:17–5:19 PM:** the batches (F0021–F0025) were loaded and then deleted at the scanner.
  - **8:29–9:00 PM:** attempts to load the old media again were refused.
  - **9:03 PM:** the two boxes were rescanned and loaded as batches F0028–F0032, with the same five ballot counts.
  - **9:22 PM:** the results were printed again with all 554,229 ballots (section 3).
- **How the deletion happened** (from Central Count; the logs agree at every step, section 3.2):
  - On the DS950 mail scanner, the batches of the first two boxes (117 and 250 ballots) were exported to two thumb drives, but not cleared from the scanner.
  - After the next box (225 ballots) was scanned, those 367 ballots' batches were deleted on the scanner, and the 225 were exported to a third drive.
  - That drive also carried the deletions, so loading it took the 367 ballots out of the EMS.
- **Everything else reconciles exactly:**
  - **Vote types:** every other vote type matches.
  - **Results reports:** both printings of the precinct report add up to their summaries. The 9:22 PM summary differs from the 6:57 PM one only in the mail ballots (section 5).
  - **Log copies:** every copy holds every event of the copies before it. The 11/10 copy printed one event with a line of its text lost at a page break; that's a printing artifact, not a missing event (section 2).
- **No results were output early.**
  - No results report was printed, saved or exported before Election Day while ballots were loaded.
  - On Election Day, mail, EV provisional and limited ballots were loaded from 2:15 PM to 3:55 PM. The first report was printed at 8:32 PM, after the polls closed.
  - Per Central Count, no results were released until the last voter in the county had voted (section 4).
- **Every counting period was bracketed by tests.** This includes the 9:03 PM rescan: a logic and accuracy (L&A) test copy was restored at 9:52 PM (section 6).

**Question for the county** (section 3.4): the six entities' canvass reports were saved twice, at 7:22–7:36 PM without the 367 ballots and at 10:16–10:30 PM with them. Please confirm that the entities received the later versions. The entities are Austin ISD, City of Jonestown, ACC, Leander ISD, Kennedy Hill MUD and ESD 10.

**Two points that are most likely explained** (section 3.4, to confirm):

- **The posted results:** they came from the export saved as "Final results 11.10.2026" at 9:23 PM, after the rescans.
- **The final audit log copies:** they were exported from the EMS with an end date of 11/10.

## 2. Data and method

**Audit logs.** Electionware prints each log newest event first. Each later printing contains the earlier ones plus new events:

| Log | Copies | Final copy | Events in the final copy | Earliest event | Newest event |
|---|---:|---|---:|---|---|
| Election | 6 | 7,014 pages, printed 11/12 11:20 AM | 150,221 | 09/04/2026 9:33 AM | 11/10/2026 10:30 PM |
| Admin | 7 | 75 pages, printed 11/12 11:19 AM | 1,532 | 09/04/2026 8:49 AM | 11/10/2026 10:05 PM |

**How the logs were read:**

- **Election Explorer read all thirteen PDFs** and merged the copies of each log. The version used includes the fixes of November 12, 2026, which were made after this election's logs showed two gaps in it (section 9).
- **It replayed the ballot accounting:** every "Committing new Batch … Number of Ballots n" event, oldest first, with each full clear of results starting a new session. A "Committing Machine Deleted Batch" removes the batch of that name. This replay equalled the official ballots cast in each of the nine earlier Travis elections checked (P24 to PR26).
- **A second, independent parser** read the PDFs of the first version of this review. It gave the same event count for the 11/10 Election log (149,666).

**Copies check.** Every copy holds every event of the copies printed before it.
- **One page-break artifact:** the 11/10 copy breaks the 11/3 6:51:07 PM "Ballot Review filter applied" event across pages 696–697. The PDF leaves out one line of its text ("Precincts[ALL] Polls[ALL]"). Every other copy prints the event whole, so it is the same event.
- **The 11/12 printings** add 555 Election log events and 3 Admin log events to the 11/10 copies.

**Results.** Election Explorer read each pair of results PDFs, the summary with its precinct report (section 5). The first version of this review also had a second, independent reader add up the 6:57 PM precinct report contest by contest. The 9:27 PM precinct report was checked with Election Explorer only.

## 3. The 367 mail ballots: deleted at the scanner, then rescanned

### 3.1 What the log records

All times are November 10, 2026. The loads are on WORKSTATION-11, in the canvass-day sessions that came after the post-Election-Day test copy was opened (section 6).

| Time | Event |
|---|---|
| 5:17:45 PM | Load of results from E:\ — "4 batch(es) found" |
| 5:17:49–5:18:03 PM | Committing new Batches **F0024** (55), **F0025** (78), **F0022** (70), **F0023** (47) — Vote By Mail, media 6: **250** ballots |
| 5:18:36 PM | Load of results from E:\ — "1 batch(es) found" |
| 5:18:37 PM | Committing new Batch **F0021** — Vote By Mail, media 6: **117** ballots |
| 5:19:16 PM | Load of results from E:\ — "6 batch(es) found" |
| 5:19:20–5:19:42 PM | Five times: "Reloading media with images, Poll ID 2,844 **Media ID 6 Machine SN 9522090625**", then "Committing **Machine Deleted** Batch" F0021, F0024, F0025, F0022 and F0023 (each 0 ballots, under new batch IDs 777–782) |
| 5:19:36 PM | Committing new Batch F0026 (225 ballots), in the same load |
| 5:33:48 PM | Committing new Batch F0027 (84 ballots) |
| 6:17–7:10 PM | Final ENR export (6:19 PM); "Results Final" summary (6:57 PM) and precinct report (7:03 PM): **553,862** ballots |
| 7:17 PM | Both audit logs printed (the copies the first version of this review used) |
| 7:22–7:36 PM | Canvass and cumulative reports saved for six entities (question 1) |
| 8:29:08 PM, 8:29:29 PM | Loads of E:\ ("1 batch(es) found", then "4 batch(es) found"): "Results not loaded. A more recent version of this batch was previously loaded." |
| 9:00:31–9:00:35 PM | The same message three more times; the load is stopped |
| 9:03:10 PM | Load of results from E:\ — "4 batch(es) found" |
| 9:03:14–9:03:31 PM | Committing new Batches **F0028** (70), **F0029** (78), **F0030** (47), **F0031** (55): **250** ballots |
| 9:03:43 PM | Load of results from E:\ — "1 batch(es) found" |
| 9:03:44 PM | Committing new Batch **F0032**: **117** ballots |
| 9:15:18–9:17:10 PM | Commit of adjudicated results, including ballots in batches F0027 (ID 783), F0028 (785) and F0029 (786) |
| 9:20–9:31 PM | Election Summary (9:22 PM, saved as "Results Final 11.10.2026 9PM" and printed), Precinct Summary (9:27 PM): **554,229** ballots |
| 9:31–10:03 PM | Backup; test copy "11.10 CCS 3rd Tab Test 9PM" restored (9:52 PM) and opened (section 6) |
| 10:16–10:30 PM | The six entities' canvass and cumulative reports saved again |

### 3.2 How the log matches the account from Central Count

- **Two boxes, two drives:**
  - The first load (5:17 PM) holds four batches of 250 ballots in all.
  - The second load (5:18 PM) holds one batch of 117.
  - These are the two boxes, exported on two thumb drives.
- **The deletion came on the third drive:**
  - The 5:19 PM load found six batches: the five deletions, plus F0026 with 225 ballots.
  - Each deletion follows a "Reloading media with images" event that names the same scanner (machine SN 9522090625) and media (6).
  - So the batches were deleted on the scanner after their media had been exported, and the next media carried the deletion.
- **The rescans match batch for batch.** Each rescanned batch has the ballot count of one deleted batch:

  | Deleted batch | Ballots | Rescanned as | Ballots |
  |---|---:|---|---:|
  | F0021 | 117 | F0032 | 117 |
  | F0022 | 70 | F0028 | 70 |
  | F0025 | 78 | F0029 | 78 |
  | F0023 | 47 | F0030 | 47 |
  | F0024 | 55 | F0031 | 55 |

- **Nothing was counted twice:**
  - Electionware refused the old drives at 8:29 and 9:00 PM.
  - The rescans came in as new batches.
  - No full clear, media clear or batch removal appears in the log.
- **Canvass-day mail:** the mail ballots counted on November 10 are F0026 (225), F0027 (84) and the rescans (367): **676**, the canvass-day mail batches recorded at Central Count (117, 250, 225 and 84).

### 3.3 Earlier deletions in this election

Eight other batches were committed as "Machine Deleted" with 0 ballots. None had been loaded before, so none changed a total:

- **11/2:** early voting B0022–B0025 and B0027 (media 3), and Z0007 (media 4);
- **11/3:** mail E0030 and E0031 (media 4).

**Other ES&S logs.** Among the 14 ES&S elections whose logs Election Explorer has read (Travis P24 to G26, PR20 and Dallas P24, L24, G24), only one other deletion removed ballots. In Travis PR20 (July 2020), a 100-ballot vote center batch was deleted at the scanner three minutes after it was loaded. The Dallas tabulator logs have no deletions; its central-count scanner log shows "Clear Results" after its exports.

### 3.4 Questions for the county

**Open question: the six entities' canvass reports.**
- The canvass and cumulative reports for Austin ISD, City of Jonestown, Austin Community College District, Leander ISD, Kennedy Hill MUD and Travis County ESD No. 10 were saved at 7:22–7:36 PM. That was after the deletion and before the rescans, so they lack the 367 ballots.
- They were saved again at 10:16–10:30 PM.
- The later versions are expected to be the ones sent. Please confirm that each entity received them.

**Most likely explained (please confirm):**

- **The ENR export.** The "Final ENR Results 11.10.26" and "ENR Results" exports (6:19 PM) came before the rescans. The file saved as "Final results 11.10.2026" at 9:23 PM is most likely the export the posted results came from:
  - it came after the rescans and the 9:22 PM Election Summary;
  - the Clarity results carry 9:22:40 PM and all 554,229 ballots.
- **The final audit log copies.** Both "11.11" copies were printed on 11/12 at 11:19–11:20 AM. Yet the Election log ends at 11/10 10:30 PM, and the Admin log at 10:05 PM. Every earlier printing shows its own login and "…Report was started" events; these two don't. So they were most likely exported from the EMS with an end date of 11/10.

## 4. Ballot accounting by day

**The election's counting session** begins with a full clear of results at 10:33:24 AM on October 31, after the morning's in-database L&A test (20 test-deck batches, 4,562 ballots). The session ends with the last event of the final log.

| Day | Early voting | Election Day | Mail | Provisional | Batches |
|---|---:|---:|---:|---:|---:|
| Sat 10/31 | 320,981 | | | | 37 |
| Mon 11/2 | 106,294 | | | | 258 |
| Tue 11/3 | 3,584 | 87,123 | 17,782 | 1,238 | 426 |
| Wed 11/4 (to 2:52 AM) | | 14,643 | 915 | | 31 |
| Tue 11/10, 5:17–5:33 PM | | | 676 committed, 367 deleted at the scanner: 309 | 993 | 11 committed, 5 deleted |
| Tue 11/10, 9:03 PM | | | 367 (rescans) | | 5 |
| **Counted** | **430,859** | **101,766** | **19,373** | **2,231** | **763** |
| Official results | 430,859 | 101,766 | 19,373 | 2,231 | |

**Notes on the counting:**

- **Ballots loaded on Election Day before the polls closed:** Central Count loaded mail ballots, EV provisional ballots and limited ballots (counted as early voting). Between 2:15 PM and 3:55 PM it loaded 11,148 ballots:
  - 118 mail batches;
  - 6 EV provisional batches;
  - 20 early voting batches: W0001 (107 emergency-slot ballots from the Ben Hur Shrine Center, below) and W0002–W0020 (783 limited ballots).
- **The 107 emergency-slot ballots (W0001):** during early voting at the Ben Hur Shrine Center (EV9), the DS200 scanner malfunctioned. As its instructions direct, voters put 107 ballots in the scanner's emergency slot. Central Count scanned these separately and loaded them as W0001. They are regular early voting ballots: the 427,275 ballots on the early voting scanners plus these 107 make the 427,382 scanned in early voting.
- **Loading stopped at 3:56 PM and resumed at 10:30 PM.** The election stayed open meanwhile for ballot review (adjudication).
- **The other limited ballots** were loaded at 10:30–10:33 PM: W0021–W0029 (490), Q0009–Q0012 (690), B0001–B0008 (1,060) and A0001–A0004 (454).
- **Limited ballots in all:** 3,477. The early voting total is the 427,382 ballots scanned in early voting (including the 107 emergency-slot ballots) plus these 3,477: 430,859.
- **Is this allowed?** A county of 100,000 or more may begin tabulating early ballots before Election Day, within the period the Election Code allows. What it may not do is release results before the polls close.
  - **No results were output before the polls closed** (section 7).
  - **When results were released:** according to Central Count, the initial results were available after the polls closed at 7 PM. Travis County released no results until the last voter in the county had voted.
- **No media clears and no replaced batch IDs** in the counting session.
- **Refused loads:** 6 loads in the counting session were refused as already loaded, and nothing was counted twice:
  - "These Results were previously loaded. No files have changed." (Vote Center 31, 11:51 PM on 11/3);
  - the old mail drives on 11/10, five times (section 3.1).
  - A seventh refusal came during testing, on 9/16.
- **Three mail batches of 0 ballots**, all on 11/3: Z0047 and Z0052 (media 3), E0020 (media 4).

## 5. The results PDFs

**Statistics (Election Summary), the two final printings:**

| | Total | Mail | Early voting | Election Day | EV provisional | ED provisional |
|---|---:|---:|---:|---:|---:|---:|
| Ballots cast, 6:57 PM | 553,862 | 19,006 | 430,859 | 101,766 | 1,735 | 496 |
| Ballots cast, 9:22 PM | **554,229** | **19,373** | 430,859 | 101,766 | 1,735 | 496 |
| Blank ballots, 9:22 PM | 3,359 | 482 | 2,873 | 4 | 0 | 0 |

Registered voters: 955,362; turnout 58.01% at 9:22 PM (57.97% at 6:57 PM). The 6:57 PM printing had 3,342 blank ballots, 465 of them mail.

**The 9:22 PM summary against the 6:57 PM summary:**
- All 446 choice totals are present in both printings. Of these, 252 changed, in 139 contests.
- Every change is in the mail column, as 367 more mail ballots would make it. The largest is 312 votes, for one candidate for U.S. Senator.

**Precinct report against the summary.** Each pair comes from the same database, a few minutes apart, and agrees:

- **Ballots:** the 308 precincts' ballots add up to the summary's total in each printing, 553,862 and 554,229, and in every vote-type column.
  - 10 precincts have no ballots in either printing: 128, 139, 147, 159, 179, 228, 239, 249, 257 and 259.
  - No precinct has more ballots than registered voters (checked at 9:22 PM in the Clarity precinct data: 554,229 ballots, 955,362 registered).
- **Votes:** every contest of the precinct report adds up to the summary, in both printings. The one exception, the same in both, is the City of Austin propositions below.
- **City of Austin Propositions C–J appear twice:**
  - **In the summary:** two separate sets of contests with the same titles, on pages 20–21 and 22–23. The second set carries about 9,000 votes per proposition.
  - **In the precinct report:** 50 precincts carry both sets. Five precincts (164, 170, 193, 294, 490) carry only the second set; only precinct 193 has votes in it.
  - **Matching:** a reader that matches contests by title alone puts precinct 193's votes in the first set. The two sets then seem to differ from the summary by 3 to 6 votes. Matched by their place in each precinct's contest list, both sets agree with the summary exactly (checked on the 6:57 PM printing).
- **File names:**
  - The 6:57 PM summary's copy saved to E:\ is named "Results Final 10.10.2026" (11/10 6:57 PM); the C:\ copy is named 11.10.2026.
  - At 9:22 PM a save as "Results Final 11.10.2026_9:10PM.pdf" failed, since a colon can't be used in a file name. It was then saved as "Results Final 11.10.2026 9PM".
  - These are only file-name slips.

## 6. Logic and accuracy testing

Travis County runs an L&A test before and after each counting day. As in earlier elections, it does this with copies of the election database named "… 3rd Tab Test":

- **Opened before counting:** the Admin log shows the last test copy opened before counting begins.
- **Restored after counting:** after counting, the Admin log shows a backup of the election, then a new test copy restored from WORKSTATION-11.

| Counting period | Loads | Test before | Test after |
|---|---|---|---|
| 10/31 3:45–5:21 PM (320,981 ballots) | 37 batches | In-database test session, 10:23–10:33 AM, then full clear | `G26 EV CC 3rd Tab Test` restored 8:21 PM |
| 11/2 10:59–11:23 AM (106,294) | 258 batches | `G26 EV CC 3rd Tab Test` opened 10:13 AM | `G26 EV CC 3rd Tab Test 11.2` restored 6:26 PM |
| 11/3 2:15 PM – 11/4 2:52 AM (125,285) | 457 batches (no loading 3:56–10:30 PM; ballot review went on) | `G26 EV CC 3rd Tab Test 11.2` opened 11:50 AM | `G26 ED CC 3rd Tab Test 11.3` restored 11/4 4:32 AM |
| 11/10 5:17–5:33 PM (1,669 loaded; 1,302 after the deletions) | 11 batches, plus 5 deletions | `G26 ED CC 3rd Tab Test 11.3` opened 10:25 AM | `11.10 CCS 3rd Tab Test` restored 6:43 PM, opened 6:47–6:53 PM |
| 11/10 9:03 PM (367, the rescans) | 5 batches | `11.10 CCS 3rd Tab Test` (6:47 PM) | `11.10 CCS 3rd Tab Test 9PM` restored 9:52 PM, opened 9:56–10:03 PM |

**Before the election:**

- **Test sessions** in the election database: 51 in the Election log, from September 14 to October 7. Each loaded test decks only and ended with a full clear of results. A 52nd ran on the morning of October 31, just before the counting session's full clear.
- **Results output during testing:** 41 events, from September 14 to October 31. Each came with only test decks loaded, or nothing (the October 31 zero report).

**Each test's own records** are in the test copies' Election logs. Those logs are not in these PDFs.

## 7. Results output

- **Before Election Day:** none with ballots loaded.
  - The Election Summary printed at 10:35 AM on October 31 came two minutes after the full clear, with nothing loaded: a zero report.
  - The other outputs before Election Day were during L&A testing.
- **Election Day:** the first report was printed at 8:32 PM, after the polls closed, from the ballots loaded before 3:56 PM.
  - **Before Election Day ballots were loaded:** Election Summaries were printed at 9:41 and 9:51 PM. Custom table reports were saved at 9:56–9:59 PM, files "REPORT" (10:01 PM) and "REPORT2" (10:21 PM), and an Election Summary at 10:25 PM ("Updated Election Results"). Loading of Election Day ballots began at 10:30 PM.
  - **Release:** per Central Count, no results were released until the last voter in the county had voted.
  - **After that:** Election Summaries followed through 3:46 AM on November 4.
- **November 5:** 71 canvass and cumulative reports were saved for the cities, school districts, MUDs and other districts on the ballot.
- **November 9, 1:58 PM:** a cast vote record export ran over 566,806 records and wrote 5,848. This was a filtered export, not the full CVR.
- **November 10, before the rescans (without the 367 ballots):**
  - the "Final ENR Results" export and a plain-text precinct export (6:19 PM);
  - an Election Summary (6:18 PM);
  - the "Results Final" summary (6:57 PM) and precinct report (7:03 PM);
  - the six entities' canvass and cumulative reports (7:22–7:36 PM);
  - two more Election Summaries, created and not saved (7:42 and 7:55 PM).
- **November 10, after the rescans (all 554,229 ballots):**
  - the "Results Final … 9PM" summary (9:22 PM), saved and printed;
  - the precinct report (9:27 PM);
  - "Final results 11.10.2026" (9:23 PM);
  - a plain-text precinct export, created but with no save logged (9:31 PM; the 6:19 PM one was saved);
  - the six entities' canvass and cumulative reports, saved again (10:16–10:30 PM).

## 8. Other events worth noting

- **Ballot sheet load errors:** 4,356 "An error occurred while loading Ballot Sheet ID n" events, on 11/2 to 11/10.
  - **They are repeats:** they name only 311 distinct sheet IDs, each up to 34 times, and they recur at the start of later loads.
  - **Same as Dallas County:** its G24 log shows the same pattern.
  - **Effect on the count:** none that the totals show. Every vote type reconciles exactly.
- **Media validation failures:** 6 media failed validation on 10/31 at 4:03 PM and later, each followed by "Error loading results". There was also one results validation error at 4:10 PM ("Election is not closed or missing Poll Place collection file"). None of these failed loads committed a batch. The 4:03 and 4:07 PM loads were retried and committed within a minute.
- **Failed restore:** two "Unable to decrypt restore package" messages at 8:14 PM on 10/31, then a successful restore of the test copy at 8:21 PM.
- **User accounts** (Admin log):
  - **Created:** HernandezA on 10/2, and AndresS at 3:54 PM on 10/31, while early-voting media were being loaded. AndresS loaded media from 4:02 PM on WORKSTATION-13.
  - **Updated:** seven accounts were updated: on 9/4 (during setup), 10/2, 10/31 and 11/3.
- **Adjudication:** 915 ballots approved in Ballot Review, and 5,385 write-in names updated (County Clerk 3,765; U.S. Senator 958; Governor 392).
- **Clock anomalies:** 5, all from September during ballot setup. None during counting.

## 9. Notes on Election Explorer

This election's logs showed two gaps in Election Explorer 1.6.0.0. Both were fixed on November 12, 2026 (commit 7592215):

- **Machine-deleted batches.** The replay did not read "Committing Machine Deleted Batch".
  - **Before the fix:** it reported 554,229 ballots for the 7:17 PM logs, which hid the gap; once the rescans were loaded it would have reported 554,596.
  - **Now:** it removes each deleted batch. The Ballot Accounting report has a Deleted Batches tab with the scanner, the load the deletion came in, and the batch it removed. It also shows a later batch of the same poll place and ballot count, which may be a rescan.
- **A description line lost at a page break.** The copies check had counted the 6:51:07 PM event as missing from the 11/10 and 11/12 copies. It now matches the shortened printing to the whole one and reports it as "Printed Incomplete".

One gap remains:

- **Repeated contest titles in a precinct report.** Contests are matched by title alone. This gives the false 3–6 vote differences in the Austin propositions (section 5).
