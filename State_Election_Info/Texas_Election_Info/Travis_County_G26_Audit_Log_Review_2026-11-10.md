# Travis County November 2026 General Election: Audit Logs Compared with the Final Results

**Election:** November 3, 2026 Joint General and Special Elections (G26)\
**County:** Travis County, Texas (ES&S Electionware)\
**Data:**

- Five printed copies of the Electionware **Election Audit Events Report** and five of the **Admin Audit Events Report**, printed 10/31 10 AM, 10/31 8:35 PM, 11/2, 11/3 ("All EV") and 11/10 7:17 PM.
- The final canvass results:
  - `Results Final 11.10.2026.pdf`, the Election Summary, printed 11/10 6:57 PM;
  - `Results Final  Precinct Report 11.10.2026.pdf`, the Precinct Summary, printed 11/10 7:03 PM.

**Date:** November 10, 2026

## 1. Summary

**367 mail ballots loaded into the election management system (EMS) are not in the final results.** On November 10 the EMS committed 554,229 ballots. The final results report 553,862. The difference is entirely mail ballots: five batches from one mail scanner, loaded on canvass day and then marked deleted by that scanner.

| Vote type | Audit log: ballots committed | Final results: ballots cast | Difference |
|---|---:|---:|---:|
| Early voting in person | 430,859 | 430,859 | 0 |
| Election Day | 101,766 | 101,766 | 0 |
| Mail | **19,373** | **19,006** | **367** |
| Provisional (EV 1,735 + ED 496) | 2,231 | 2,231 | 0 |
| **Total** | **554,229** | **553,862** | **367** |

- **The 367 ballots** are batches F0021–F0025 from mail scanner media 6 (machine serial number 9522090625):
  - 5:17–5:18 PM, November 10: committed with 55 + 78 + 70 + 47 + 117 = 367 ballots;
  - 5:19 PM: the same media reloaded, with the five batches as "Machine Deleted" batches of 0 ballots;
  - no later event in the log refers to those batches again (section 3).
- **Everything else reconciles exactly:**
  - every other vote type matches;
  - the precinct report adds up to the summary in all 223 contests and every vote-type column;
  - all five printed copies of each log are complete (no events dropped between printings).
- **No results were output early.** No results report was printed, saved or exported before Election Day while ballots were loaded. On Election Day the first report was printed at 8:32 PM.
- **Every counting day was bracketed by tests:** a logic and accuracy (L&A) test copy of the election was opened before each counting day and restored after it (section 6).

**Questions for the county** (section 3.4): why the five batches were deleted at the scanner, where those 367 paper ballots are now, and whether they were counted in some other batch.

## 2. Data and method

**Audit logs.** Electionware prints each log newest event first. Each later printing contains the earlier ones plus new events:

| Log | Copies | Final copy | Events in the final copy | Earliest event |
|---|---:|---|---:|---|
| Election | 5 | 6,988 pages, printed 11/10 7:17 PM | 149,666 | 09/04/2026 9:33 AM |
| Admin | 5 | 72 pages, printed 11/10 7:17 PM | 1,475 | 09/04/2026 8:49 AM |

**How the logs were read:**

- **Election Explorer** (the audit-log engine of version 1.6) read all ten PDFs and merged the copies of each log.
- **It replayed the ballot accounting:** every "Committing new Batch … Number of Ballots n" event, oldest first, with each full clear of results starting a new session. This replay equalled the official ballots cast in each of the nine earlier Travis elections checked (P24 to PR26).
- **A second, independent parser** read the PDFs again. It gives the same event count for the final Election log (149,666).

**Results.** Election Explorer read both results PDFs. A second, independent reader then added up the precinct report contest by contest (section 5).

## 3. The 367 mail ballots

### 3.1 What the log records

All times are November 10, 2026. Every event is logged on WORKSTATION-11 under the same user, in the canvass-day load that came after the post-Election-Day test copy was opened (section 6).

| Time | Event |
|---|---|
| 5:17:45 PM | Load of results from E:\ — "4 batch(es) found" |
| 5:17:49 PM | Committing new Batch **F0024**-Vote By Mail - 6, ID 772, **55** ballots |
| 5:17:54 PM | Committing new Batch **F0025**-Vote By Mail - 6, ID 773, **78** ballots |
| 5:17:59 PM | Committing new Batch **F0022**-Vote By Mail - 6, ID 774, **70** ballots |
| 5:18:03 PM | Committing new Batch **F0023**-Vote By Mail - 6, ID 775, **47** ballots |
| 5:18:36 PM | Load of results from E:\ — "1 batch(es) found" |
| 5:18:37 PM | Committing new Batch **F0021**-Vote By Mail - 6, ID 776, **117** ballots |
| 5:19:16 PM | Load of results from E:\ — "6 batch(es) found" |
| 5:19:20 PM | Reloading media with images, Poll ID 2,844 **Media ID 6 Machine SN 9522090625** |
| 5:19:20 PM | Committing **Machine Deleted** Batch F0021-Vote By Mail - 6, ID 777, 0 ballots |
| 5:19:24 PM | Committing **Machine Deleted** Batch F0024-Vote By Mail - 6, ID 778, 0 ballots |
| 5:19:28 PM | Committing **Machine Deleted** Batch F0025-Vote By Mail - 6, ID 779, 0 ballots |
| 5:19:33 PM | Committing **Machine Deleted** Batch F0022-Vote By Mail - 6, ID 780, 0 ballots |
| 5:19:36 PM | Committing new Batch F0026-Vote By Mail - 6, ID 781, 225 ballots |
| 5:19:42 PM | Committing **Machine Deleted** Batch F0023-Vote By Mail - 6, ID 782, 0 ballots |
| 5:20:12 PM | Stop Load |
| 5:33:48 PM | Committing new Batch F0027-Vote By Mail - 6, ID 783, 84 ballots |

**Before and after:**

- Before the reload, the EMS held the five batches with 367 ballots.
- After it, the same five batch names stand as deleted batches of 0 ballots, under new batch IDs.
- The deletion is recorded as done on the scanner ("Machine Deleted"), not in Electionware: the log has no clear of results, no media clear and no removal of a batch.

### 3.2 The results follow the deletion

- **The mail total:** the final results' mail ballots cast (19,006) equal the audit log's mail commits less these five batches (19,373 − 367).
- **Canvass-day mail:** of the 676 mail ballots committed on November 10, the results hold only F0026 and F0027 (225 + 84 = 309).

### 3.3 What the log does not show

- **No one-for-one rescan:** F0026 (225) and F0027 (84) total 309, not 367, and no combination of the deleted batches' counts gives 225.
  - If F0026 and F0027 were a rescan of the deleted ballots, at least 58 of the 367 were never counted again.
  - If they were other ballots, none of the 367 were counted.
- **No later handling:** no Ballot Review, adjudication or reload event after 5:19 PM names F0021–F0025 or batch IDs 772–776.
- **No reason** for the deletion is logged. A deletion on the scanner is made at the scanner, and the EMS sees only its result.
- **Timing:** only 36 seconds passed between the first load of F0021 (5:18:40 PM) and the reload that found it deleted (5:19:16 PM). That is too short to delete five batches at the scanner and scan 225 new ballots. So the two loads were probably two different copies of media 6: one written before the deletion, one after.

### 3.4 Questions for the county

1. Why were batches F0021–F0025 (367 ballots) deleted on scanner SN 9522090625 after they were scanned?
2. Were those ballots rescanned? If so, in which batches? Batches F0026 and F0027 hold 309 ballots, not 367.
3. Where are the 367 paper ballots, and does the canvass's ballot reconciliation (Form 15-1 or the county's equivalent) account for them?
4. Was the count of accepted mail ballots for the canvass 19,006, or 19,373?

### 3.5 Other "Machine Deleted" batches (no effect on the count)

Eight other batches were committed as "Machine Deleted" with 0 ballots. None had first been committed with ballots, so none changes a total:

- **11/2:** early voting B0022–B0025 and B0027 (media 3), and Z0007 (media 4);
- **11/3:** mail E0030 and E0031 (media 4).

## 4. Ballot accounting by day

**The election's counting session** begins with a full clear of results at 10:33:24 AM on October 31, after the morning's in-database L&A test (20 test-deck batches, 4,562 ballots). The session ends with the last event of the final log.

| Day | Early voting | Election Day | Mail | Provisional | Batches |
|---|---:|---:|---:|---:|---:|
| Sat 10/31 | 320,981 | | | | 37 |
| Mon 11/2 | 106,294 | | | | 258 |
| Tue 11/3 | 3,584 | 87,123 | 17,782 | 1,238 | 426 |
| Wed 11/4 (to 2:52 AM) | | 14,643 | 915 | | 31 |
| Tue 11/10 | | | 676 | 993 | 11 |
| **Committed** | **430,859** | **101,766** | **19,373** | **2,231** | **763** |
| Final results | 430,859 | 101,766 | 19,006 | 2,231 | |

**Notes on the counting:**

- **Mail and early ballots before polls closed:** on Election Day, 11,148 ballots (118 mail batches, 20 early voting batches, 6 EV provisional batches) were loaded between 2:15 PM and 3:55 PM, before the polls closed. Then the election was closed until 10:30 PM.
  - **Is this allowed?** A county of 100,000 or more may begin tabulating early ballots before Election Day, within the period the Election Code allows. What it may not do is release results.
  - **No results were output** during this time (section 7).
- **No media clears and no replaced batch IDs** in the counting session. One load was refused as already loaded ("These Results were previously loaded. No files have changed.", Vote Center 31 at 11:51 PM on 11/3), and nothing was counted twice.
- **Three mail batches of 0 ballots**, all on 11/3: Z0047 and Z0052 (media 3), E0020 (media 4).

## 5. The results PDFs

**Statistics (Election Summary):**

| | Total | Mail | Early voting | Election Day | EV provisional | ED provisional |
|---|---:|---:|---:|---:|---:|---:|
| Ballots cast | 553,862 | 19,006 | 430,859 | 101,766 | 1,735 | 496 |
| Blank ballots | 3,342 | 465 | 2,873 | 4 | 0 | 0 |

Registered voters: 955,362; turnout 57.97%.

**Precinct report against the summary.** These two reports come from the same database, six minutes apart, and agree exactly:

- **Ballots:** the 308 precincts' ballots cast add up to 553,862. Each vote type adds up to its summary figure, and each precinct's vote types add up to its total.
  - 10 precincts have no ballots: 128, 139, 147, 159, 179, 228, 239, 249, 257 and 259.
  - No precinct has more ballots than registered voters.
- **Votes:** all 715 summary rows (223 contests, every choice, write-ins, over- and undervotes) equal the precinct sums in all six columns.
- **City of Austin Propositions C–J appear twice:**
  - **In the summary:** two separate sets of contests with the same titles, on pages 20–21 and 22–23. The second set carries about 9,000 votes per proposition.
  - **In the precinct report:** 50 precincts carry both sets. Five precincts (164, 170, 193, 294, 490) carry only the second set; only precinct 193 has votes in it.
  - **Matching:** a reader that matches contests by title alone puts precinct 193's votes in the first set, and the two sets then seem to differ from the summary by 3 to 6 votes. Matched by their place in each precinct's contest list, both sets agree with the summary exactly.
- **File name:** the summary's copy saved to E:\ is named "Results Final 10.10.2026" (11/10 6:57 PM). The C:\ copy is named 11.10.2026. This is only a typo in a file name.

## 6. Logic and accuracy testing

Travis County runs an L&A test before and after each counting day. As in earlier elections, it does this with copies of the election database named "… 3rd Tab Test":

- **Opened before counting:** the Admin log shows the last test copy opened before counting begins.
- **Restored after counting:** after counting, the Admin log shows a backup of the election, then a new test copy restored from WORKSTATION-11.

| Counting period | Loads | Test before | Test after |
|---|---|---|---|
| 10/31 3:45–5:21 PM (320,981 ballots) | 37 batches | In-database test session, 10:23–10:33 AM, then full clear | `G26 EV CC 3rd Tab Test` restored 8:21 PM |
| 11/2 10:59–11:23 AM (106,294) | 258 batches | `G26 EV CC 3rd Tab Test` opened 10:13 AM | `G26 EV CC 3rd Tab Test 11.2` restored 6:26 PM |
| 11/3 2:15 PM – 11/4 2:52 AM (125,285) | 457 batches (paused 3:55–10:30 PM, election closed) | `G26 EV CC 3rd Tab Test 11.2` opened 11:50 AM | `G26 ED CC 3rd Tab Test 11.3` restored 11/4 4:32 AM |
| 11/10 5:17–5:33 PM (1,669, including the 367) | 11 batches | `G26 ED CC 3rd Tab Test 11.3` opened 10:25 AM | `11.10 CCS 3rd Tab Test` restored 6:43 PM |

**Before the election:**

- **Test sessions** in the election database: 51 in the Election log, from September 14 to October 7. Each loaded test decks only and ended with a full clear of results. A 52nd ran on the morning of October 31, just before the counting session's full clear.
- **Results output during testing:** 41 events, from September 14 to October 31. Each came with only test decks loaded, or nothing (the October 31 zero report).

**Each test's own records** are in the test copies' Election logs. Those logs are not in these PDFs.

## 7. Results output

- **Before Election Day:** none with ballots loaded.
  - The Election Summary printed at 10:35 AM on October 31 came two minutes after the full clear, with nothing loaded: a zero report.
  - The other outputs before Election Day were during L&A testing.
- **Election Day:** the first report was printed at 8:32 PM, after the polls closed. Election Summaries followed through 3:46 AM on November 4.
- **November 5:** 71 canvass and cumulative reports were saved for the cities, school districts, MUDs and other districts on the ballot.
- **November 9, 1:58 PM:** a cast vote record export ran over 566,806 records and wrote 5,848. This was a filtered export, not the full CVR.
- **November 10:** the final ENR export (6:19 PM), the final Election Summary (6:57 PM) and the Precinct Report (7:10 PM). All came after the 11/10 loads, with the 367 ballots already deleted.

## 8. Other events worth noting

- **Ballot sheet load errors:** 4,049 "An error occurred while loading Ballot Sheet ID n" events, on 11/2 to 11/10.
  - **They are repeats:** they name only 311 distinct sheet IDs, each up to 33 times, and they recur at the start of later loads.
  - **Same as Dallas County:** its G24 log shows the same pattern.
  - **Effect on the count:** none that the totals show. Every vote type except mail reconciles exactly.
- **Media validation failures:** 6 media failed validation on 10/31 at 4:03 PM and later, each followed by "Error loading results". There was also one results validation error at 4:10 PM ("Election is not closed or missing Poll Place collection file"). None of these failed loads committed a batch. The 4:03 and 4:07 PM loads were retried and committed within a minute.
- **Failed restore:** two "Unable to decrypt restore package" messages at 8:14 PM on 10/31, then a successful restore of the test copy at 8:21 PM.
- **User accounts** (Admin log):
  - **Created:** HernandezA on 10/2, and AndresS at 3:54 PM on 10/31, while early-voting media were being loaded. AndresS loaded media from 4:02 PM on WORKSTATION-13.
  - **Updated:** six accounts were updated on 10/2, 10/31 and 11/3.
- **Adjudication:** 906 ballots approved in Ballot Review, and about 5,400 write-in names updated (County Clerk 3,762; U.S. Senator 956).
- **Clock anomalies:** 5, all from September during ballot setup. None during counting.
- **A line dropped from one printed event.** One long Ballot Review filter event (11/3 6:51:07 PM) is cut across pages 696–697 of the final Election log, and the PDF leaves out one line of its text ("Precincts[ALL] Polls[ALL]"). This is how the report was printed: the event itself is in every copy.

## 9. Notes on Election Explorer

This review found three things Election Explorer 1.6 does not yet handle:

- **Machine-deleted batches.** The replay counts "Committing new Batch" but does not read "Committing Machine Deleted Batch". So it reports 554,229 ballots with no explanation of the difference. The audit window should name the five batches as loaded and then deleted at the scanner.
- **Repeated contest titles in a precinct report.** Contests are matched by title alone, which gives the false 3–6 vote differences in the Austin propositions (section 5).
- **A description line lost at a page break.** The copies check counts the 6:51:07 PM event as missing from the final copy.
