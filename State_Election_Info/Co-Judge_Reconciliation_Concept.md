# Bipartisan Co-Judge Pre-Canvass Reconciliation & Bounded Forensic Examination

*A concept for a full examination and reconciliation of county election records — and a
bounded, non-destructive forensic examination of a subset of equipment — conducted before
final canvass, under the election administrator's authority, supervised by two co-judges
appointed by the two major political parties.*

**Status:** Concept / analysis memo. Not legal advice. Specifics depend on each state's
election code (see `state_election_codes.json`) and voting-system certification rules.
**Date:** 2026-09-13 (updated 2026-10-04: added Section 4, deadlock resolution).

---

## 1. The legal insight this design rests on

Federal record-retention law (52 U.S.C. § 20701, the 22-month retention duty) and U.S.
Department of Justice guidance require election officers to retain **ultimate management
authority over the retention and security** of election records. The chain-of-custody
problem with the post-2020 "forensic audits" was never that examination happened — it was
that records and equipment **left the administrator's control and were handed to private
parties**.

This concept satisfies the constraint **by construction**: every record and machine stays on
systems under the election administrator's authority, and the examiners act as **agents of
the administrator**, not as outside custodians. The two co-judges add bipartisan legitimacy
and a mutual check **without transferring custody** and without making anyone but the
administrator legally responsible.

## 2. Design principles

1. **No custody transfer.** All records and equipment remain under the administrator; all
   processing happens on administrator-controlled hardware.
2. **Bipartisan supervision, not bipartisan control.** Co-judges observe, question, and
   co-sign; the administrator remains the responsible official.
3. **Before certification, not after.** The examination *feeds* the canvass/certification
   decision rather than second-guessing a completed one — more useful and less legally
   fraught.
4. **Build on existing structures.** Most states already have **bipartisan boards of
   canvassers** and run poll-book reconciliation and logic-and-accuracy (L&A) testing under
   bipartisan observation. Anchor the program in that statute rather than inventing new
   officers.
5. **Work on copies where possible.** Reconcile against read-only / write-protected copies
   and forensic images so originals stay pristine.

## 3. Governance: the two co-judges

- **Appointment.** One judge nominated by each of the two major parties (mirroring existing
  canvass-board composition), sworn, and bonded.
- **Authority.** Defined in statute or administrative rule: the right to observe all steps,
  inspect records and logs, pose questions, and **co-sign** the reconciliation and assurance
  reports. Access is joint (two-person rule), never solo.
- **Dispute resolution (critical).** Define in advance what happens when the co-judges
  disagree. **Without a tie-break, a single partisan co-judge could stall certification** —
  which is its own attack surface. The program must not create a new veto. Section 4 sets
  out a tiered rule modeled on existing Texas law.
- **Transparency vs. sensitive information.** Reports are public, but sensitive security
  details (passwords, exploitable configuration specifics) are withheld — echoing North
  Carolina's escrow-access limits and Colorado's felony for publishing system passwords.

## 4. Deadlock resolution: Texas precedents

Texas law relies heavily on party-balanced election bodies, but it almost never gives two
co-equal officials a mutual veto. Instead it breaks deadlock with a small set of recurring
patterns. Texas is used here as a worked example; other states have their own analogues.

| # | Pattern | Texas example | How deadlock is broken |
|---|---|---|---|
| 1 | Designated lead from the majority party; the other party is a check, not a veto | Precinct presiding judge and alternate from the two leading gubernatorial parties — presiding judge from the party that carried the precinct (Elec. Code §32.002); presiding judge "in charge of and responsible for" the polling place (§32.071); same pairing at the central counting station (§127.005) | The presiding judge decides; the alternate's leverage is presence, observation, and the record |
| 2 | Majority vote with a presiding-officer backstop | Early Voting Ballot Board (Elec. Code ch. 87; SoS *EVBB Handbook*, rev. 7/10/2026) | If a majority cannot decide, the presiding judge makes the final determination; the board fixes its majority standard (and any tiebreaker) **before** qualifying ballots |
| 3 | Default outcome + burden of proof | Signature Verification Committee: rejection only by majority vote (§87.027(i)); to reject, a member must show the signatures differ (SoS *SVC Handbook*, rev. 9/22/2025); chair breaks subcommittee ties | A split defaults to **acceptance** — the tie resolves itself |
| 4 | One-directional appeal | EVBB may overturn an SVC rejection by majority vote but may not override an SVC acceptance (§87.027(j)) | Disputes flow upward in one direction only; a second body cannot manufacture new rejections |
| 5 | Single decider + written reasons + segregation | Recount committee chair has "the same authority as a presiding election judge" to decide counting questions, must give a written statement of specific reasons for not counting a ballot, and keeps uncounted ballots separate (§213.006); watchers may photocopy challenged ballots (§213.013) | Decided quickly, but nothing is lost — the record survives for an election contest |
| 6 | Facts resolved by re-performance, not by vote | Hand counts: if tally lists disagree, the ballots are recounted and the lists corrected (§65.005(b)); no team member replaced until discrepancies are cleared (§65.006); voter-intent questions go to the presiding judge (§65.009; SoS Advisory 2025-18) | Factual disputes are redone until they agree; interpretive disputes go to a designated decider |
| 7 | Bipartisan supermajority | Texas Ethics Commission: eight commissioners balanced between the parties; rules, reviews, and decisions on complaints require at least six votes (Gov't Code ch. 571) | Deadlock = **no action** — acceptable for an enforcement body, unacceptable for a canvass with statutory deadlines (cautionary) |
| 8 | Two party-appointed members + neutral umpire | Insurance appraisal clause: each side picks an appraiser, the two pick an impartial umpire, and any two of the three decide; appraisal decides the **amount of loss**, while liability/coverage stays with the courts (*State Farm Lloyds v. Johnson*, 290 S.W.3d 886 (Tex. 2009)) | A neutral third member breaks ties; scope is limited to facts, with legal questions reserved to courts |
| 9 | Deadline-triggered backstop | If the Legislature fails to redistrict in the first regular session after the census, the Legislative Redistricting Board must adopt a plan on a fixed schedule (Tex. Const. art. III, §28) | Failure to act by a deadline transfers the decision to a pre-designated fallback |

**Why the SVC/EVBB pair is the strongest precedent.** It already combines most of what this
concept proposes: party-balanced panels, a majority rule fixed in advance, a presiding-officer
backstop, a default outcome with a burden of proof, a one-directional appeal, and (per the SoS
handbook) continuous livestreaming of the work in larger counties. Citing it shows that the
co-judge structure extends an existing Texas mechanism rather than inventing a new one.

### Recommended tiered rule for the co-judges

1. **Factual discrepancies** (counts, reconciliation totals): the co-judges jointly
   re-perform the step until the results agree, logging each pass. *(Pattern 6)*
2. **Interpretive disputes:** a designated decider rules — the election administrator, or a
   presiding co-judge designated by the majority-party rule of §32.002 — with **written
   reasons**, and the disputed items are **segregated and preserved**. *(Patterns 1, 5)*
3. **Statutory defaults apply** where they already exist (e.g., a presumption of acceptance
   unless rejection is shown). *(Pattern 3)*
4. **Dissent is recorded, not a veto.** A disagreeing co-judge notes the dissent on the
   co-signed report, preserving the issue for an election contest — the role the alternate
   judge and watchers play today.
5. **Escalation is one-directional and bounded:** only the flagged items go to the canvassing
   authority (for county elections, the commissioners court, Elec. Code §67.002) and then to a
   court — not the whole canvass. *(Pattern 4)*
6. **Hard deadline fallback:** the canvass proceeds on its statutory schedule; unresolved
   items are transmitted with the record rather than holding up certification. *(Pattern 9)*
7. **Scope split:** the co-judges determine the *facts* of the reconciliation; questions of
   *law* go to the canvassing authority or a court. *(Pattern 8)*
8. **Avoid an equal-vote, no-tiebreak structure.** It reproduces "deadlock = inaction"
   *(Pattern 7)* — exactly the certification-stall risk this program must not create.

## 5. Part 1 — Records examination & reconciliation (high value, largely lawful today)

Essentially a rigorous, comprehensive, bipartisan pre-canvass audit that closes every
count-to-count loop.

| Reconciliation | Compares |
|---|---|
| Poll-book reconciliation | Voters checked in ↔ ballots cast (per precinct / vote center) |
| Mail-ballot accounting | Ballots issued ↔ returned ↔ accepted ↔ rejected ↔ counted |
| Tabulator ↔ paper | Machine totals ↔ ballot images ↔ hand count of a sample (the risk-limiting audit) |
| Ballot accounting | Ordered/printed ↔ issued ↔ spoiled ↔ provisional ↔ duplicated |
| Chain-of-custody / seals | Seal numbers, transfer logs, access logs — continuous and unbroken |

**Workflow.** Run on the administrator's hardware against read-only/write-protected copies
of EMS data and ballot images; co-judges observe and co-sign a **per-precinct reconciliation
report** plus a discrepancy log with documented resolutions.

**Timing.** Must complete inside the statutory canvass window (often ~1–2 weeks). Build it
into the canvass schedule, or obtain an explicit statutory allowance, so it does not collide
with the certification deadline.

**Outputs.** Signed reconciliation reports, a discrepancy/resolution log, and an assurance
statement that feeds the certification decision.

**Legal fit.** Most states already authorize the bipartisan pieces (canvass boards,
poll-book reconciliation, L&A). The novel element is doing it **comprehensively, before
certification, with formal co-judge sign-off, on the administrator's systems** — which is
consistent with DOJ chain-of-custody guidance.

## 6. Part 2 — Bounded, non-destructive forensic examination of a subset

The harder half. Be precise about what "determine whether the system was modified" can
actually deliver.

**Achievable non-destructively, under the administrator's authority:**
- **Full software/firmware integrity verification** — hash the *entire* installed
  file set / manifest against the certified **trusted build** (vendor/escrowed build or
  EAC/NIST trusted-build hashes). This is genuinely more than the single L&A hash, yet
  non-invasive.
- **Audit-log & event-log forensics** — tabulator/EMS logs, configuration-change
  timestamps, removable-media insertion history, access and seal logs. This is where real
  tampering signatures appear.
- **Removable-media imaging** — forensic images of USB/CF media, verified against expected
  contents.
- **Air-gap verification** — confirm no network interface is enabled/active.

**Hard limits (state them plainly):**
1. **Certification & seals.** Opening or imaging a unit breaks seals and, in many states,
   forces **re-certification / re-L&A before reuse**. Schedule forensic exams on units not
   needed again this cycle, and plan a re-seal / re-L&A path. This is exactly why Colorado
   and Pennsylvania moved to *restrict* machine access.
2. **Who performs it.** Co-judges *witness*; a qualified examiner does the work. If that
   examiner is an outside specialist, they must be engaged **as an agent of the
   administrator** — bonded, under NDA, working only on administrator-controlled systems,
   with bipartisan witnesses and dual-control throughout — to stay inside the chain of
   custody.
3. **Vendor IP.** Comparing installed binaries to trusted-build hashes needs no source code;
   true **source-level** review requires escrowed source and usually State-Board or court
   authorization (the North Carolina / New York model).
4. **Subset selection.** A subset only speaks to the units examined. Make it meaningful via
   either **(a)** a bipartisan **random draw** with a statistical framing, or **(b)**
   **anomaly-triggered** selection driven by the Part 1 reconciliation. The two halves
   reinforce each other: reconciliation tells you where to look.
5. **What it can and cannot prove.** It detects unexpected files, hash mismatches, and log
   anomalies — *known* modification signatures. It **cannot prove the absence** of a
   sophisticated, self-erasing implant. It is assurance and deterrence, not proof of a clean
   machine.

## 7. Chain-of-custody & dual-control safeguards

- Two-person (dual-control) access for all steps; nothing done solo.
- Continuous logging; video where feasible; tamper-evident seals re-applied and re-logged.
- Work on forensic **copies/images**, keeping originals pristine.
- A documented, pre-planned procedure so an anomaly triggers a chain-of-custody-preserving
  response rather than an improvised one.

## 8. Where the paper fits (the ground truth)

The **paper record remains the source of outcome truth.** The risk-limiting hand count in
Part 1 is what actually assures the *result*. The software forensics in Part 2 assure
*process integrity* and provide deterrence — valuable defense-in-depth, but they must not be
presented as the thing that certifies the count.

## 9. Statutory hooks for implementation

- **Amend the canvass-board statute** to add the reconciliation duty and the co-judge roles,
  rather than passing a standalone "forensic audit" law.
- **Deadlines.** Ensure the canvass/certification clock accommodates the work, or extend it.
- **Re-certification path** for any equipment opened during a forensic exam.
- **Transparency with carve-outs** for sensitive security information.
- **Tie-break / dispute-resolution** mechanism so the co-judge structure cannot deadlock
  certification — codify the tiered rule in Section 4.

## 10. Relationship to tooling (ElectionExplorer)

Part 1 maps naturally to software: a reconciliation tool that runs **on the administrator's
own hardware**, ingests read-only copies of EMS/poll-book/ballot data, performs the
count-to-count reconciliations, and produces the co-signed reports and discrepancy logs. The
integrity-verification and log-analysis elements of Part 2 are likewise concrete features.

## 11. Honest assessment

- **Part 1 is implementable now** in most states with modest statutory/administrative
  changes and is strongly consistent with DOJ chain-of-custody guidance. Lead with it.
- **Part 2 is implementable in a bounded, non-destructive form** under the administrator's
  authority — and the in-house-custody design is the right way to do it — but "prove the
  system was never modified via third-party forensics" hits certification, vendor-IP,
  expertise, and provability ceilings that no governance structure fully removes.

## 12. References

- 52 U.S.C. § 20701 (federal record retention); U.S. DOJ, *Federal Law Constraints on
  Post-Election "Audits."*
- North Carolina (source-code escrow / independent review) and New York (escrow + expert
  review under court supervision) — the closest existing statutory analogues.
- Colorado SB22-153 (Election Security Act) and Pennsylvania decertification policy — the
  restrict-third-party-access precedents.
- Texas deadlock-resolution precedents (Section 4): Tex. Elec. Code §§32.002, 32.071,
  65.005–65.006, 65.009, 67.002, 87.027(i)–(j), 127.005, 213.006, 213.013; Texas Secretary
  of State, *Early Voting Ballot Board Handbook* (rev. 7/10/2026), *Handbook for Signature
  Verification Committee* (rev. 9/22/2025), and Election Advisory No. 2025-18 (hand-counting);
  Tex. Gov't Code ch. 571 (Texas Ethics Commission); *State Farm Lloyds v. Johnson*,
  290 S.W.3d 886 (Tex. 2009); Tex. Const. art. III, §28 (Legislative Redistricting Board).
- Companion memo: `RMF_for_EMS_Control_Catalog.md`.
- State statutory election codes and chief-election-authority sites:
  `state_election_codes.json`.
