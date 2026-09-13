# Bipartisan Co-Judge Pre-Canvass Reconciliation & Bounded Forensic Examination

*A concept for a full examination and reconciliation of county election records — and a
bounded, non-destructive forensic examination of a subset of equipment — conducted before
final canvass, under the election administrator's authority, supervised by two co-judges
appointed by the two major political parties.*

**Status:** Concept / analysis memo. Not legal advice. Specifics depend on each state's
election code (see `state_election_codes.json`) and voting-system certification rules.
**Date:** 2026-09-13.

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
- **Dispute resolution (critical).** Define what happens when the co-judges disagree:
  escalate to the full county board, then to a court, on a fixed clock. **Without a
  tie-break, a single partisan co-judge could stall certification** — which is its own
  attack surface. The program must not create a new veto.
- **Transparency vs. sensitive information.** Reports are public, but sensitive security
  details (passwords, exploitable configuration specifics) are withheld — echoing North
  Carolina's escrow-access limits and Colorado's felony for publishing system passwords.

## 4. Part 1 — Records examination & reconciliation (high value, largely lawful today)

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

## 5. Part 2 — Bounded, non-destructive forensic examination of a subset

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

## 6. Chain-of-custody & dual-control safeguards

- Two-person (dual-control) access for all steps; nothing done solo.
- Continuous logging; video where feasible; tamper-evident seals re-applied and re-logged.
- Work on forensic **copies/images**, keeping originals pristine.
- A documented, pre-planned procedure so an anomaly triggers a chain-of-custody-preserving
  response rather than an improvised one.

## 7. Where the paper fits (the ground truth)

The **paper record remains the source of outcome truth.** The risk-limiting hand count in
Part 1 is what actually assures the *result*. The software forensics in Part 2 assure
*process integrity* and provide deterrence — valuable defense-in-depth, but they must not be
presented as the thing that certifies the count.

## 8. Statutory hooks for implementation

- **Amend the canvass-board statute** to add the reconciliation duty and the co-judge roles,
  rather than passing a standalone "forensic audit" law.
- **Deadlines.** Ensure the canvass/certification clock accommodates the work, or extend it.
- **Re-certification path** for any equipment opened during a forensic exam.
- **Transparency with carve-outs** for sensitive security information.
- **Tie-break / dispute-resolution** mechanism so the co-judge structure cannot deadlock
  certification.

## 9. Relationship to tooling (ElectionExplorer)

Part 1 maps naturally to software: a reconciliation tool that runs **on the administrator's
own hardware**, ingests read-only copies of EMS/poll-book/ballot data, performs the
count-to-count reconciliations, and produces the co-signed reports and discrepancy logs. The
integrity-verification and log-analysis elements of Part 2 are likewise concrete features.

## 10. Honest assessment

- **Part 1 is implementable now** in most states with modest statutory/administrative
  changes and is strongly consistent with DOJ chain-of-custody guidance. Lead with it.
- **Part 2 is implementable in a bounded, non-destructive form** under the administrator's
  authority — and the in-house-custody design is the right way to do it — but "prove the
  system was never modified via third-party forensics" hits certification, vendor-IP,
  expertise, and provability ceilings that no governance structure fully removes.

## 11. References

- 52 U.S.C. § 20701 (federal record retention); U.S. DOJ, *Federal Law Constraints on
  Post-Election "Audits."*
- North Carolina (source-code escrow / independent review) and New York (escrow + expert
  review under court supervision) — the closest existing statutory analogues.
- Colorado SB22-153 (Election Security Act) and Pennsylvania decertification policy — the
  restrict-third-party-access precedents.
- Companion memo: `RMF_for_EMS_Control_Catalog.md`.
- State statutory election codes and chief-election-authority sites:
  `state_election_codes.json`.
