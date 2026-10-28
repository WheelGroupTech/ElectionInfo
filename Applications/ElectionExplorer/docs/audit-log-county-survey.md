# EMS audit logs — survey of the 50 largest Texas counties (2026-10-28)

**Request:** check the election sites of the 50 largest Texas counties for audit logs and save them in
`Audit_Logs/` (gitignored), as was done for election results and voter rosters.

**Answer:** only **Dallas, Parker and Hays** post audit logs. Travis's logs were supplied by the user.
Every other county's election site has no audit log online, although the logs are election records
(1 TAC §81.62) and can be requested under the Public Information Act.

## What was saved

| County | System | Files | Elections | Source |
|---|---|---|---|---|
| Dallas | ES&S Electionware | 14 audit event reports (Election + Admin) | P24, L24 (new), G24 | Public directory listing `dallascounty.org/elections/Election Specific Record/2024/<election>/6. Canvass/Logs and CVRs/Audit Logs/` |
| Dallas | ES&S Electionware | 22 machine log reports, 286 MB, under `TX_Dallas_County/Machine_Logs/` | P24 (6), L24 (1), G24 (15) | Same tree, `…/Logs and CVRs/Machine Logs/` |
| Parker | Hart Verity (VerityCount) | 14 Audit Log Reports | G22, PR22, S23_0207, L23, G23, P24, L24, PR24, G24, L25, P26, S26_0331, L26, PR26 | "Audit Log" links on `parkercountytx.gov/441/Past-Election-Results` |
| Hays | Hart Verity | 1 ZIP: Audit Log Report PDF + XML signature | PR24 | Document 1607. The archive page (`Archive.aspx?ADID=200`) links its "Primary Run-off Audit Log Report June 3, 2024 (ZIP)" to the wrong document (1605). |
| Travis | ES&S Electionware | 56 (user-supplied, untouched) | P24, G24, GR24, L25, G25, GR25, P26, L26, PR26 | — |

**Naming:** `<code>_<server file name>`, the convention of `Election_Results/`.
- The code is P / PR / L / LR / G / GR plus the year, or `S<yy>_<mmdd>` for other dates.
- It is dated from the report's own election date. All 84 audit logs on disk were re-checked and
  agree with their prefixes.
- The machine logs are dated by their election folder.

## Formats found

- **ES&S Electionware "Election / Admin Audit Events Report"** (JasperReports, iText): Dallas,
  Travis. This is the format of `audit-log-design.md`.
- **ES&S Electionware "Machine Log Report"** (Dallas):
  - the same JasperReports family;
  - columns Log Entry Text | Date and Time | Machine Serial Number | …;
  - DS200 / DS850 event logs per tabulator, printed by poll-place range ("EV 1-10", "ED 151-300",
    "Central Count");
  - up to 16,306 pages per file.
  - Some G24 files carry the customer name "Electionware County" instead of Dallas.
- **Hart VerityCount "Audit Log Report"** (Microsoft Reporting Services): Parker, Hays. Two
  layouts:
  - **VerityCount 2.7 (2026):** a page header with the run time and date, the county, the election
    date, the election name and ID, and "Component: VerityCount 2.7.1". The columns are Row | Log
    Date & Time | Device ID | User | Event | Event Data.
  - **VerityCount 2.5 and earlier (2022–2025):** no header. Rows start at 1, with an ISO 8601
    timestamp and offset (`2024-09-17T12:12:32-05:00`), device ID, user and event. The election is
    named by the first "Election Open" event (`The election '11-5-24 Parker A v2.5' (86857) was
    opened.`).
  - Some reports cover a date range rather than one election ("1.1.2015-3.21.2024",
    "9.16.24 to 11.21.24").
  - Hays's export adds an XML-DSig RSA-SHA256 signature of the PDF, which an app could verify.

## How the counties were checked

- **Crawl:** a polite crawler (one request per second per site, plain requests) walked each county's
  election pages from the `Election_Results/` downloader's page list.
  - It went three links deep, up to 300 pages per county; 8,676 pages in all.
  - It looked for links whose text or address mentions audit logs, event logs, tabulation logs,
    Verity or Electionware.
  - What it found was almost all post-election hand-count audits, county financial audits,
    L&A notices and certifications, and reconciliation forms.
- **Web searches:**
  - site searches for "audit log" per county;
  - searches for the two report titles ("Election Audit Events Report", "Audit Log Report" with
    VerityCount).
  - They found Parker's and Dallas's files, and Electionware and Verity logs from outside Texas
    (Pinal County AZ, Butte-Silver Bow MT, Jasper County IL). The crawl had missed Parker's page.
- **Built-in browser:** for sites that refuse automated requests or that the crawl reached only
  partly. Brazoria, Johnson, Wichita (Cloudflare, which loaded normally in the browser), Brazos,
  Nueces, McLennan, Cameron, Hays, Williamson and Montgomery.
  - Hays's site search (`/Search?searchPhrase=audit log`) found the mislinked PR24 log.
  - Probing Hays documents 1560–1700 found no other audit log.
  - None of the other nine has audit logs online.
  - Montgomery's public-information site needs an account and was not entered.

| Result | Counties |
|---|---|
| Audit logs online | Dallas (ES&S), Parker (Hart), Hays (Hart, one election) |
| None found; crawled | Angelina, Bastrop, Bell, Bexar, Bowie, Collin, Comal, Coryell, Denton, Ector, El Paso, Ellis, Fort Bend, Galveston, Grayson, Gregg, Guadalupe, Harris, Henderson, Hidalgo, Hunt, Jefferson, Kaufman, Liberty, Lubbock, Midland, Orange, Potter, Randall, Rockwall, Smith, Tarrant, Taylor, Tom Green, Travis (county site), Victoria, Walker, Webb |
| None found; checked in the browser (refuse or rate-limit automated requests) | Brazoria, Brazos, Cameron, Johnson, McLennan, Montgomery, Nueces, Wichita, Williamson |

**Limits:**
- A crawl capped at 300 pages, with link-text matching, can miss a file posted under an unhelpful
  name. Parker's page was found by search, not by the crawl.
- A county could post logs inside results ZIPs. The Hays results ZIPs in `Election_Results/` were
  opened and hold none.

## Related records seen (not downloaded)

These records are useful for the ballot accounting in `audit-log-design.md`:

- **Reconciliation reports:**
  - Tarrant posts, per election, `…/elections/<year>/<mmyy>/audit/`: an Early Voting / Election
    Day "Reconciliation of Total Votes Cast at Each Polling Location" and the SOS preliminary and
    official reconciliation reports.
  - Johnson, Hays and Montgomery post the SOS Election Reconciliation forms (Form 12-1 / 12-2:
    ballots counted versus voters).
- **Dallas L&A test summaries:** `2. Election setup/Public Test/<date>_L&A Tests (Summary).pdf`,
  6–67 MB each.
- **Hays CVR exports** per election (already a CVR source).

## Getting logs from other counties

The SOS canvass checklist puts "an audit log from the Central Count Computer before and after the
election" in the canvass materials. 1 TAC §81.62 makes the central accumulator's audit log an
election record kept for the preservation period. So the logs exist at every county and can be
requested under the Public Information Act. Collin County answered such a request for its
2020–2021 Electionware logs (American Oversight archive).

## Refreshing

```
python -I Audit_Logs/_download_tools/audit_download.py [--only Dallas,Parker,Hays] [--dry-run]
```

- `audit_download.py` (gitignored, next to `audit_state.json`) re-walks the Dallas directory tree,
  the Parker results pages and the Hays archive.
- It saves only new files: a file already in the county folder is reported HAVE.
- It reuses the results downloader's date-to-code rules, and dates each report from its first page
  (PyMuPDF; `pdftotext` scrambles the Electionware header).
- Hays rate-limits bursts (403). Run it again later rather than retrying at once.
