# Section-by-Section Analysis

**Bill:** relating to a bipartisan pre-canvass reconciliation of election records and a bounded,
non-destructive integrity examination of voting system equipment (90th Legislature, R.S., 2027;
draft).

**Companion files:** `1_Bill_Text.md`, `3_Fiscal_Note.md`, `4_Sponsor_Summary.md`.

**Sources:** `State_Election_Info/Co-Judge_Reconciliation_Concept.md` (the "concept memo"),
`RMF_for_EMS_Control_Catalog.md` (the "control catalog"), `Analysis_Software_Endorsement_Path.md`
(the "software memo"), and the Texas county dataset in `State_Election_Info/Texas_Election_Info/`.

---

## 1. Background and approach

Texas already has the parts this bill needs. The commissioners court canvasses county elections
(Elec. Code §67.002(a)(1)) inside a fixed window (§67.003). The presiding judge of the central
counting station attests a reconciliation of votes and voters (§127.131(f)). Each county runs a
partial manual count (§127.201) and, since S.B. 827 (2025), every county takes part in the
risk-limiting audit (§127.302). Counties of 250,000 or more keep electronic activity logs at the
central counting station and send them to the Secretary of State (§127.009). Counties of 100,000 or
more livestream the areas holding voted ballots through the canvass (§127.1232). Equipment is
hash-validated during logic and accuracy testing (§129.023(c-1)).

What Texas lacks is a single place where all of those records are reconciled against one another,
before the canvass, with both major parties watching and signing. The bill supplies that by
adding one subchapter to Chapter 127, which already houses the partial manual count, the
risk-limiting audit, and the Secretary of State's randomized audits. It does not create a
standalone "forensic audit" chapter. It amends the canvass statute (§§67.003 and 67.004) so the
results feed the canvass without delaying it.

Three design rules from the concept memo run through every section:

1. **No custody transfer.** The general custodian keeps custody of everything, and every examiner
   works as the custodian's agent on the custodian's hardware (concept memo §§1–2; bill
   §127.403).
2. **Supervision, not control.** Co-judges observe, question, and sign, but do not run equipment or
   direct staff (bill §127.408).
3. **No new veto.** Every disagreement ends in a decision made by a rule fixed in advance, within a
   fixed time, with dissent preserved (concept memo §4; bill §§127.413–127.418).

---

## 2. Section-by-section

### SECTION 1. Findings and purpose (uncodified)

States the reasons for the bill: public confidence through a full accounting; the existing Texas
party-balanced bodies that break deadlock without a veto (§§32.002, 32.071, 127.005; Chapter 87
EVBB/SVC; Chapter 65 hand counts; §213.006 recounts); the federal retention duty in 52 U.S.C.
§20701 and DOJ guidance that officials keep "ultimate management authority"; the chain-of-custody
problems when originals leave official custody; why timing before the canvass matters; why paper
is the ground truth; and why a structure without a tiebreaker would be a stall risk. The findings
are written as legislative history for a court reading the subchapter. They are uncodified, as is
usual for Texas findings.

### SECTION 2. Election Code §67.003(d): canvass timing

When an examination is held, the presiding officer of the commissioners court must set the
canvass no earlier than the day after the examination closing date. The existing outer limit
(the 11th day after election day, or the 14th day for elections under §65.051(a-1)) does not
change. The subsection says outright that the examination never extends the canvass period and
that an unfinished examination never prevents the canvass. In practice, the canvass may move later
*within* the window it already has, but certification never moves past the current statutory
deadline.

### SECTION 3. Election Code §67.004(h)–(j): canvass record

- **(h)** The custodian hands the commissioners court the assurance statement, the master
  discrepancy log, and any referred items together with the sealed precinct returns.
- **(i)** The court considers them when it compares returns and tally lists under existing
  §67.004(d), may order a correction only where existing law already allows one, and puts the
  documents in the canvass record.
- **(j)** The court may not refuse or delay the canvass because of anything in those documents, a
  dissent, an unresolved item, or an incomplete examination. This codifies the "hard deadline
  fallback" (concept memo Pattern 9) at the point where a stall would otherwise happen.

### SECTION 4. New Subchapter K, Chapter 127

**§127.401 Definitions.** Defines the working terms. Key choices:

- *Eligible political party* uses the gubernatorial-vote test that Texas already uses to pair
  presiding and alternate judges (§32.002) and to pick the EVBB presiding judge (§87.002). The
  concept memo's "two major parties" therefore means the two parties that already supply the
  county's presiding and alternate judges.
- *Presiding co-judge* comes from the party whose nominee for governor carried the county, the
  same rule as §87.002 and the SVC chair under §87.027.
- *Factual discrepancy* and *interpretive dispute* are defined separately, because the bill
  resolves them differently (re-performance for facts, a designated decider for interpretation).
- *Trusted build* ties integrity checks to the version approved under Chapter 122 and furnished by
  the Secretary of State, which is the same source the Secretary already uses for L&A hash
  validation (SOS Election Advisory 2022-30).
- *Unit* covers both ES&S and Hart equipment classes (EMS, central and precinct tabulators, BMDs,
  DREs) without naming a vendor.

**§127.402 Applicability.** Covers elections canvassed by the commissioners court and run by the
county officer who is general custodian. That includes the November general election, special
elections ordered by the governor, and county elections. It excludes party primaries, which the
parties run themselves under Title 10, and elections of other political subdivisions. In a joint
election, the examination reaches only the county custodian's records and equipment.

**§127.403 Custody.** This is the core of the bill's legal design. The custodian keeps custody and
"ultimate management authority" (tracking DOJ's phrasing on 52 U.S.C. §20701). Every step is done
by the custodian, its staff, Secretary of State staff, or a contract examiner acting as the
custodian's agent, in a custodian facility, on custodian or state hardware. No party, co-judge,
candidate, or watcher may take or keep originals or copies. Subsection (d) says plainly that
nothing here authorizes a party to commission its own examination. That forecloses the
private-firm model that caused the custody problems elsewhere.

**§127.404 Request.** Either eligible party's county chair, acting alone or at the direction of
the county executive committee, may request an examination. The request is due by the 60th day
before election day, which puts the co-judges in place before the public L&A test (no later than
the 48th day before election day under §129.023(b)), so they can witness the pre-election
hash-validation baseline. A late request, up to 7 days before election day, gets the records
reconciliation but not the integrity examination unless the custodian finds it workable. A
request by either chair triggers one joint examination with co-judges from both parties. A chair
may make one request that covers every covered election in a calendar year. The custodian may also
run an examination on its own initiative.

**§127.405 Appointment.** Each chair appoints a co-judge and an alternate. If a chair does not
appoint, the custodian asks once and then may appoint a consenting person from that party
(identified by primary participation), echoing the vacancy process in §32.002. Most important for
deadlock prevention, subsection (d) provides that a party's absence never stops the examination.
Work continues under staff dual control, recorded on video, and open to later review by the absent
co-judge. That closes the "boycott" route to a stall.

**§127.406 Eligibility.** Co-judges must be county voters who meet the general election-judge
eligibility rules in Chapter 32, Subchapter C (§32.051 and following), complete Secretary of State
training, and sign an oath and confidentiality agreement. Candidates, county chairs, state
executive committee officers, custodian staff, and current or recent (two-year) voting-system
vendor employees are excluded.

**§127.407 Oath, bond, training, compensation.** Co-judges are sworn and covered by a $10,000
surety bond paid by the county (blanket bonds allowed). The Secretary of State provides free
training, which may be online, like watcher training under §33.008. Pay matches the central
counting station presiding judge under §127.005.

**§127.408 Authority.** Lists what co-judges may do (observe everything, inspect records, ask
questions and get written answers, require re-performance, make anomaly designations, sign or
dissent, and observe the §127.131(f) reconciliation, §127.201 count, the risk-limiting audit, and
any equipment test in the period). It also lists what they may not do (handle originals except
under dual control, operate equipment, bring recording devices, or direct staff). Access is joint,
never solo (control catalog AC-1, two-person integrity). Co-judges may see confidential voter data
but may not copy it. Watchers who may serve at the central counting station may also watch the
examination, except for sensitive security information.

**§127.409 Examination plan.** By the 30th day before election day, the custodian issues a
written plan in a Secretary of State form: schedule, location, hardware, copy and imaging method,
video and logging, the random-draw procedure, and the names of staff and examiners. Co-judges may
comment, and the custodian decides. Fixing the plan in advance follows the EVBB practice of setting
the decision rule before ballots are qualified (concept memo Pattern 2).

**§127.410 Records reconciliation (Part 1).** Requires every count-to-count loop in the concept
memo §5, by precinct, by polling location, and countywide:

| Loop | Compares | Existing Texas record it uses |
|---|---|---|
| Poll list/pollbook | voters accepted ↔ ballots cast and tabulated | pollbook and poll list; §127.131(f) reconciliation |
| Mail ballots | applications ↔ ballots provided ↔ returned ↔ accepted/rejected/cured ↔ counted | early voting clerk, SVC and EVBB records (Ch. 87) |
| Provisional ballots | cast ↔ accepted ↔ rejected ↔ counted | EVBB and registrar records |
| Tabulation | tabulator ↔ EMS ↔ CVRs ↔ ballot images ↔ manual count ↔ RLA | §127.201 count; Subchapter I RLA |
| Ballot accounting | ordered/printed ↔ delivered ↔ issued ↔ spoiled ↔ voted ↔ provisional ↔ duplicated ↔ unused | ballot register; §127.126 duplication records |
| Chain of custody | seals, transfers, access and media logs | §127.1232 video; §127.009 device logs; Subchapter D and §129.023 test and hash records |

Subsection (c) treats a missing record as a discrepancy, so silence is never read as agreement.
Subsection (d) handles timing. The §127.201 manual count may by law run to the 21st day, and the
risk-limiting audit runs on dates the Secretary of State sets. The bill asks the custodian, and on
request the Secretary, to finish those before the examination closes where practicable. Anything
not finished is completed in a supplemental report by the 45th day. Subsection (e) is the only
override of existing law. Notwithstanding §66.058, which generally bars opening voted-ballot
containers during the preservation period, a container may be opened only for a count the code
already authorizes (Chapter 65, §127.201, or the RLA). Containers of unvoted or spoiled ballots may
be opened for ballot accounting, under dual control and with resealing logged.

**§127.411 Read-only copies; controlled hardware.** Work is done on read-only copies and
forensic images, exported through the certified system's own export functions (or a method the
Secretary of State approves), moved by two-person transfer with hash logging (control catalog
MP-4), and analyzed on custodian or state hardware that is not part of the voting system and is
offline. Hashes of originals are taken before and after copying.

**§127.412 Reports.** There are three outputs: a per-precinct and per-location reconciliation
report, a master discrepancy and resolution log (with fields for each re-performance,
classification, reasons, segregation, dissent, and referral), and an assurance statement that
becomes part of the canvass record. Subsection (d) makes a report complete on the custodian's
signature, so a co-judge's refusal to sign is noted but cannot block it.

**§127.413 Factual discrepancies: re-perform until agreement (tier a).** Modeled on the hand-count
rule that disagreeing tally lists are resolved by recounting (§65.005) and that no counter is
replaced until discrepancies are cleared (§65.006). The step is redone from the same or a better
source until two consecutive passes agree. After three passes without agreement, the item becomes
an interpretive dispute, so re-performance cannot loop forever. Re-performance establishes how big
the discrepancy is, not why it happened.

**§127.414 Interpretive disputes: designated decider (tier b).** The bill splits authority three
ways (concept memo Pattern 8, facts versus law):

- The **presiding co-judge** classifies each discrepancy as explained, clerical or procedural, or
  unexplained, and decides other disputes about what the examination found, with written reasons.
  This follows the presiding-judge model (§§32.002, 32.071), the EVBB presiding judge, and the
  recount committee chair who has "the same authority as a presiding election judge" and must give
  written reasons (§213.006).
- The **general custodian** decides custody, security, scope, and equipment questions, because the
  custodian is the legally responsible official. It may not use that power to deny a co-judge's
  statutory rights.
- **Questions of law** (ballot validity, statutory interpretation) are not decided in the
  examination at all. They are logged and referred.

Disputed physical items are sealed separately and disputed electronic items are preserved as
hashed copies (Pattern 5, segregation as in §213.006).

**§127.415 Presumptions (tier c, statutory defaults).** Official returns are presumed correct. The
examination changes no vote total and no ballot decision. Totals change only through existing
procedures (§67.004(d), §127.131, Title 13 recount, Title 14 contest). EVBB and SVC acceptance
decisions are not reopened, which mirrors the one-way rule of §87.027(j). A chain-of-custody gap
does not by itself invalidate a ballot. This is the "default outcome" pattern of the SVC, where a
split leaves the ballot accepted.

**§127.416 Dissent (tier d).** Either co-judge may dissent in writing on any report, log entry, or
the assurance statement. Signing with a dissent counts as signing. A dissent never delays anything
and is preserved for a recount or contest. This is the role the alternate judge and watchers play
today.

**§127.417 Referral and judicial review (tier e, one-way escalation).** Only three kinds of items
go up: unexplained discrepancies, dissented items, and questions of law. The commissioners court
may order a correction already authorized by law, refer the item to the Secretary of State or a
prosecutor, and must note it in the record. It may not reopen agreed items or take up items that
were not referred (Pattern 4, the §87.027(j) model). Courts may compel the custodian by mandamus
(§273.061) to perform duties such as granting access, but may not push the canvass past its
statutory deadline. Challenges to results go only through the existing recount and contest
statutes.

**§127.418 Deadline (tier f, hard fallback).** The examination closes on the earlier of completion
or the second day before the last permissible canvass day. At 5 p.m. that day, open items are
recorded as unresolved and travel with the canvass record. Unfinished comparisons and integrity
work go into a supplemental report by the 45th day, which does not affect the canvass.

*Example: Tuesday, November 7, 2028 general election.*

| Date | Event | Source |
|---|---|---|
| Fri Sep 8 (day −60) | Request deadline | §127.404(b) |
| Mon Sep 18 (day −50) | Co-judges appointed | §127.405(a) |
| by Wed Sep 20 (day −48) | Public L&A and hash validation, witnessed by co-judges | §129.023(b), (c-1) |
| Sun Oct 8 (day −30) | Examination plan issued | §127.409 |
| Nov 7 to 9 | Tabulation; §127.131(f) reconciliation; public random draw by day +2 | §§127.131, 127.420 |
| by Fri Nov 10 (day +3) | Partial manual count must begin (72 hours) | §127.201 |
| Nov 8 to 19 | Records reconciliation; integrity examination of drawn units | §§127.410, 127.419 |
| Sun Nov 19 (day +12) | Latest examination closing date | §127.418(a)(2) |
| Mon Nov 20 or Tue Nov 21 | Canvass (latest is day +14) | §67.003(c), (d) |
| by 3rd day after canvass | Reports posted | §127.425 |
| Fri Dec 22 (day +45) | Supplemental report deadline | §127.418(d) |

If the 2nd-day-before date falls on a weekend, staff may need to work that day. The Council should
also confirm how §1.006 (weekend and holiday extensions) interacts with the new closing date.

**§127.419 Integrity examination (Part 2).** By default the examination is limited to four
non-destructive procedures from concept memo §6 and control catalog SI-1, AU-2/AU-3, MP-3/MP-4:
(1) full-manifest hash verification against the trusted build, which goes further than the
sample hash done during L&A; (2) audit, event, and system log export and review; (3) forensic
imaging of removable media; and (4) air-gap verification. Opening a unit or imaging internal
storage requires an objective anomaly trigger, written approval from the Secretary of State, and
withdrawal of the unit until re-acceptance. Prohibited: installing software, network connection,
source-code review (left to the Secretary of State under other law), penetration testing of
production units (catalog RA-1 and CA-3 limit those to test units), and anything that alters
ballots, data, or logs. A unit cannot be examined until its tabulation is finished and preserved.

**§127.420 Selection.** There are three routes into the sample:

- **Random draw (public, by day +2):** every EMS server or workstation and every central
  tabulator (usually a small number); the greater of 2 or 3% of precinct tabulators; and the greater
  of 2 or 1% of BMDs and DREs. The Secretary of State may cap the count for very large counties if
  the cap still supports the detection-probability statement.
- **Co-judge designations:** each co-judge may designate up to two more units with a written
  basis. This needs no one's approval, which keeps the minority party from being outvoted on where
  to look without letting it expand the examination without limit.
- **Mandatory anomaly triggers:** an unexplained discrepancy over a threshold set by Secretary of
  State rule, a hash mismatch, a log showing unexpected media, a post-L&A configuration change or
  network activity, or a seal problem. Only these units may get the invasive procedures.

The custodian schedules invasive work on units that are not needed again before re-acceptance
(concept memo §6, limit 1).

**§127.421 Qualified examiners.** Examiners must hold a digital-forensics certification the
Secretary of State recognizes or the Texas Forensic Science Commission's digital and multimedia
evidence analyst license (the software memo notes that digital evidence is otherwise exempt from
lab accreditation), complete system-specific training, pass a criminal history check, and have no
recent compensation from candidates, parties, PACs, or the county's voting-system vendor. Contract
examiners act only as the custodian's agents under a written agreement that bars removal or
retention of data and requires bonding and confidentiality. The Secretary of State must maintain
examiner capacity at no charge to counties, the "state or regional shared service" the control
catalog (§7) says counties cannot supply alone. Vendor staff may operate approved vendor tools
under supervision but may not be the examiner.

**§127.422 Limits; required statement.** Every integrity report must carry a plain-language
statement of what the examination can and cannot show (concept memo §6, limit 5, and §8). It must
also state the detection probability for the random sample. For example, if 25 of 500 precinct
tabulators (5%) were affected, a random sample of 15 would include at least one affected unit
about 54% of the time. This keeps the report from claiming more than it can show.

**§127.423 Re-acceptance.** An opened or imaged unit returns to service only after it is verified
against or reinstalled from the trusted build, passes hash validation (§129.023) and the applicable
tabulating-equipment test (Chapter 127, Subchapter D), and is resealed in front of co-judges or the
testing board, plus anything else the Secretary of State requires to keep it within its Chapter 122
approval. A seal broken for a non-invasive step is replaced and logged. The Secretary decides by
rule which of those steps also require full re-acceptance. Subsection (c) says that an examination
done in compliance does not itself affect the system's certification.

**§127.424 Safeguards.** Requires dual control for every step, a continuous log with hashes and
seal numbers, video where feasible (livestreamed in §127.1232 counties, with cameras positioned
away from sensitive data), tamper-evident seals, a clean/dirty media scan (control catalog MP-2),
and a pre-planned evidence-preservation procedure for suspected tampering (catalog IR-1).

**§127.425 Public reports; sensitive security information.** All reports, logs, dissents, and
integrity summaries are posted within three days after the canvass, matching S.B. 827's
three-day posting rule for audits. Redactions are limited to defined sensitive security
information (credentials, exploitable configuration details, confidential trusted-build
references, and categories set by rule) and information confidential under other law, with each
redaction's category disclosed. The information is confidential and tied to Gov't Code §552.139.
Subsection (e) requires that a discrepancy's existence and resolution always be disclosed even when
details are redacted.

**§127.426 Analysis software.** Puts the software memo's two design rules into statute. Read-only
analysis software that runs on separate, offline custodian hardware, never writes to official
records, logs its inputs and hashes, and whose findings are advisory is not a "voting system"
under Chapter 122. That lets a reconciliation tool support the examination without a full
certification exam. The Secretary of State may set functional acceptance criteria (correctness,
seeded-error detection, determinism, read-only operation, logging, published release hashes) and
publish a list of tools that meet them, and may prescribe data formats, preferring the NIST common
formats.

**§127.427 Preservation; use in recount and contest.** Every examination record is an election
record kept for the precinct-records period (22 months for federal elections under §66.058). Recount
committees and contest courts may use them. Forensic images go only to the Secretary of State, a
court, or as law requires.

**§127.428 Offenses.** Creates three offenses:

- Knowingly disclosing sensitive security information without authorization is a Class A
  misdemeanor, or a state jail felony if done to facilitate unauthorized access or alteration.
  This echoes Colorado's felony for publishing system passwords, at a lower default tier.
- Knowingly removing or retaining originals, copies, images, media, or units without
  authorization is a state jail felony.
- Knowingly connecting a device to a unit or examination hardware without authorization is a
  state jail felony.

Other offenses, such as tampering with a governmental record (Penal Code §37.10), remain
available.

**§127.429 Rules and forms.** The Secretary of State adopts forms (machine-readable), trusted-build
verification and log-export guidance for each approved system (ES&S and Hart today), the random
draw and probability methods, anomaly thresholds, training, sensitive-information categories, and
re-acceptance procedures. Subsection (b) lets the Secretary require vendors, as a condition of
approval, to supply the manifests, hashes, and export tools that verification depends on. That is
how the "Affecting" controls in the control catalog get built into certified baselines rather than
bolted on afterward (catalog §6).

**§127.430 Report.** The Secretary of State reports to state leadership every odd-numbered year on
volume, costs, findings, unresolved items, and recommendations.

**§127.431 Construction.** The subchapter adds to watchers' rights, recounts, contests, the manual
count, the RLA, and the Secretary of State's Subchapter J audits, and limits none of them.

### SECTIONS 5–9. Transition and boilerplate

- **5:** Secretary of State rules by December 1, 2027; examiner capacity by September 1, 2028
  (before the November 2028 general); first report January 15, 2029.
- **6:** Applies to elections held on or after January 1, 2028, which skips the November 2027
  constitutional amendment election so rules can be in place first.
- **7:** Severability. The Code Construction Act (Gov't Code §311.032) already provides this; the
  clause is included because the request asked for it.
- **8:** Standard prevailing-over-codification clause.
- **9:** Effective September 1, 2027. No emergency clause is needed.

**Repealer and savings.** No existing provision is repealed. The only override of existing law is
the narrow "notwithstanding §66.058" clause in §127.410(e). The savings rule is in Section 6.
Texas bills do not use a general repealer.

---

## 3. How the tiered deadlock rule is codified

| Concept memo tier | Bill section | Texas precedent |
|---|---|---|
| (a) Factual discrepancies: re-perform until agreement | §127.413 | §§65.005, 65.006 |
| (b) Interpretive disputes: designated decider, written reasons, segregation | §127.414 | §§32.002, 32.071, 87.002, 213.006 |
| Statutory defaults / presumption of acceptance | §127.415 | §87.027(i)–(j) |
| (c) Dissent recorded, not a veto | §127.416 | alternate judge and watchers |
| (d) One-way, bounded escalation | §127.417; §67.004(i)–(j) | §87.027(j); §67.002 |
| (e) Hard deadline fallback | §127.418; §67.003(d) | Tex. Const. art. III, §28 (pattern) |
| Facts versus law split | §127.414(c)–(d) | appraisal-umpire model (*State Farm Lloyds v. Johnson*) |
| Avoid equal-vote, no-tiebreak | presiding co-judge; absence never stops work (§127.405(d)) | Ethics Commission cautionary model |

---

## 4. Defaults chosen for open policy questions

The concept memo left these points open. Each is a reasonable default that the sponsor can change
without affecting the rest of the structure.

| Issue | Default in draft | Alternatives |
|---|---|---|
| Who decides interpretive disputes | Presiding co-judge (majority-party, §87.002 model) on findings; custodian on custody, security, and scope | Custodian decides everything; or a neutral umpire picked by both co-judges (appraisal model) |
| Co-judge qualifications | County voter, Ch. 32 judge eligibility, training; excludes candidates, chairs, custodian staff, vendor staff (2 years) | Add a background check; allow county chairs to serve |
| Bond | $10,000 surety, county-paid, blanket bond allowed | No bond (EVBB members are not bonded); a higher amount |
| Compensation and funding | County pays co-judges at the central-counting-station presiding-judge rate; state pays for examiners | State reimburses counties; requesting party contributes |
| Request deadline | 60 days before election day (before L&A); late request to 7 days before, records only | 30 days; post-election requests |
| Random sample size | All EMS and central tabulators; greater of 2 or 3% of precinct tabulators; greater of 2 or 1% of BMDs/DREs; SOS may cap | Fixed counts; a risk-based sample size set by rule |
| Co-judge designations | 2 units each, no approval needed | 1 to 5; require a threshold |
| Invasive examination | Only on objective anomaly, with SOS written approval and withdrawal from service | Prohibit entirely; allow on co-judge designation |
| Fit inside §67.003 window | Close 2 days before the last canvass day; canvass the next day or later; supplemental report by day 45 | Close 1 day before; amend §67.003 to add days (rejected, because it delays certification) |
| Confidentiality | Defined sensitive security information, confidential under Gov't Code §552.139; redaction categories disclosed | Rely on SOS rule only; broader exemption |
| Penalties | Class A misdemeanor for disclosure (state jail felony if intended to facilitate tampering); state jail felony for removal or unauthorized connection | All Class A; Colorado-style felony for password disclosure |
| Scope and pilot | Statewide for county-run, commissioners-court-canvassed elections from Jan 1, 2028; primaries and other subdivisions excluded; biennial report | Pilot in a set of counties (for example, those of 250,000 or more, matching §127.009); include primaries |
| Who may request | Either eligible party's county chair; custodian on own initiative | Require both chairs; let candidates request |

---

## 5. What the examination can and cannot prove (for legislative history)

- **Can:** show that every count-to-count loop closes or document why it does not; detect software
  or firmware that differs from the approved build on the examined units; surface logged events
  that indicate tampering or error; and confirm that the examined units have no active network
  capability.
- **Cannot:** prove the absence of a sophisticated, self-erasing implant; speak to units that were
  not examined; or replace the paper count. The bill says this in the required statement
  (§127.422) and in the presumptions (§127.415).

---

## 6. Verification notes

Code text was checked against the 2025 edition of the Election Code (after the 89th Legislature)
through secondary sources, because the Legislature's statutes site could not be reached from the
drafting environment. Each point below should be re-checked against the official text before
filing.

- **Confirmed as described:** §67.002(a)(1); §67.003(b)–(c); §67.004(a)–(g) (amended by S.B. 2753,
  2025); §32.002(c) (presiding and alternate judges from different parties); §32.071; §87.002
  (EVBB presiding judge from the party carrying the county for governor); §87.027(i)–(j);
  §65.005, §65.006, §65.009; §127.005; §127.009 (250,000+ counties, logs to SOS within 5 days);
  §127.131(f); §127.201 and §127.302 as amended by S.B. 827 (2025; all counties take part in the
  RLA, dates set by SOS, results posted within 3 days); §127.351; §127.1232; §129.023(b), (c-1),
  (f); §66.058 (22-month preservation; voted-ballot containers not opened except as authorized);
  §213.006; §213.013; Chapter 127 subchapters A through J; Chapter 122 Subchapter B headings;
  Gov't Code §552.139 and Elec. Code §273.061 and §32.051 exist with the subjects described.
- **Not verified:** the text of §127.093 (post-count equipment testing), because the source page
  was rate-limited and could not be read; the exact subsection lettering inside §66.058 (avoided by
  using a "notwithstanding" clause); Chapter 122 reexamination provisions (cited generally); and
  how §1.006 interacts with the new closing date.
- **Correction to the concept memo:** §127.005 requires an alternate presiding judge at the central
  counting station only in elections with judges appointed under §32.002. It does not itself
  require party pairing in all elections. The findings were drafted to match.

## 7. Data points from the Texas county dataset

From `texas_county_election_info.csv` (254 counties): 141 counties use ES&S and 113 use Hart.
Nearly all tabulate paper ballots by optical scan. 183 counties post reconciliation forms online
and 149 post hash or L&A certifications, but only 1 posts EMS audit logs, 11 post cast vote
records, 3 post ballot images, and 1 posts ballot transfer logs. Most of the inputs Part 1 needs
already exist, but few are published or cross-checked, which is the gap this bill fills.
