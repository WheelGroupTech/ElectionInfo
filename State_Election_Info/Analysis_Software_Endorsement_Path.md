# Earning Official Acceptance for Election Data-Analysis Software

*How Texas and other states authorize, certify, or endorse election software — and
comparably complex government software (property tax, GIS, forensics, gaming) — and a
concrete plan for getting a pre-canvass analysis tool accepted in large counties
(500,000+ ballots cast).*

**Status:** Concept / analysis memo. Not legal advice. Whether a given tool falls under a
state's certification statutes should be confirmed with that state's election authority.
**Date:** 2026-10-05.

---

## 1. The problem

Analyzing the full record set of a large county — cast vote records (CVRs), ballot images,
poll books, mail-ballot accounting, chain-of-custody logs — needs specialized software to
finish in the window between election day and the final canvass. In Texas that window is
short: the local canvass must occur **no earlier than** the later of the third day after
election day, the day provisional ballots are verified and counted, or the day all timely
received ballots are counted — and **no later than** the 11th day (14th for the November
general election) (Tex. Elec. Code §67.003). After the last ballots are counted, a few days
may remain.

Speed is not the hard part: a desktop computer can process half a million CVR rows in
minutes. The hard part is **trust** — getting election officials, co-judges, and courts to
accept the tool's findings, inside that window, from software no statute specifically
contemplates. This memo surveys how governments establish that trust for software and
proposes a path for an analysis tool.

## 2. Key finding

**No state certifies election data-analysis software as such.** Certification law attaches
to software that **casts or counts votes, or determines who may vote** — voting systems and
electronic pollbooks. Audit and analysis tools, and comparably complex government software
in other fields, gain official standing through other mechanisms: independent testing,
university oversight, open source with security review, independence requirements, data
standards, security authorization, or accreditation of the people doing the work.

## 3. Ten models for establishing trust in software

### Election software

| # | Model | Example | What confers trust |
|---|---|---|---|
| 1 | **Formal certification panel** | Texas voting systems: four SoS examiners (two technical, two election-law) and two Attorney General examiners (Elec. Code §122.035); procedures in 1 TAC §81.60 | Statutory approval by a mixed technical/legal panel. Applies to voting systems only. |
| 2 | **Lab exam + state functional exam + annual renewal** | Texas electronic pollbooks (Elec. Code §31.014): accredited-lab technical exam against the Texas Technical Testing Matrix, then an in-person SoS functional exam against the Functional Testing Matrix; certification renewed **every year** | The strongest Texas precedent for software that doesn't count votes. |
| 3 | **Federal voluntary certification for election supporting technology** | EAC Election Supporting Technology Evaluation Program (ESTEP, 2023): e-pollbooks first (first certification announced Feb. 2025); framework also covers election-night reporting, voter registration, and electronic ballot delivery | Voluntary federal requirements and testing. Audit/analysis tools are not yet a category. |
| 4 | **University-run independent oversight** | Indiana VSTOP at Ball State examines voting systems and e-pollbooks for the state (IC 3-11-18.1-12). Connecticut's UConn VoTeR Center built AuditStation; electronic audits are allowed only when the SoS, *in consultation with UConn*, authorizes them, and UConn analyzes audit reports (CGS §9-320f, as amended by PA 21-2 JSS) | Standing, expert, neutral technical reviewer. |
| 5 | **State-adopted open-source tool + independent security testing** | ColoradoRLA (commissioned by the Colorado SoS, open source); Arlo (VotingWorks, CISA-supported, used for statewide RLAs including Texas; independently penetration-tested and security-reviewed) | Public code, state adoption by rule or practice, third-party security review — no formal certification. |
| 6 | **Independent vendor working "blind"** | Maryland: Clear Ballot ClearAudit re-tabulates every ballot image with software independent of the voting system and reports totals without seeing official results (COMAR 33.08.05.09) | Independence from the system being checked. |

### Data standards instead of software certification

| # | Model | Example | What confers trust |
|---|---|---|---|
| 7 | **Prescribed data formats** | **Elections:** VVSG 2.0 Principle 4 requires voting systems to import/export NIST common data formats — election results (SP 1500-100) and cast vote records (SP 1500-103), the latter explicitly for audit and adjudication systems. **Property tax:** the Texas Comptroller's Electronic Appraisal Roll Submission (EARS) prescribes a CSV layout, a signed media form, and certified totals; the Comptroller validates the data, not the appraisal district's software. **GIS:** Texas's Commission on State Emergency Communications NG911 GIS Data Standard (2021), aligned to the NENA NG9-1-1 GIS Data Model; data is validated against the model regardless of the GIS tool used | Anyone can independently verify the data; the producing software doesn't need certification. |

### Non-election analogues

| # | Model | Example | What confers trust |
|---|---|---|---|
| 8 | **Security authorization of software** | TX-RAMP (Gov't Code §2054.0593): state agencies and universities may buy only TX-RAMP-certified cloud services (Level 1: 117 controls; Level 2: 223). Counties are **not** covered | A standard control baseline, assessed. |
| 9 | **Accrediting the lab or analyst, not the tool** | Texas Forensic Science Commission: forensic analysis must come from an accredited lab to be admissible — but **digital evidence is exempt by statute** (CCP art. 38.35); the Commission offers voluntary digital/multimedia analyst licensing | The competence of the people and lab doing the work. |
| 10 | **Regulated test labs + field hash verification** | Nevada Gaming Control Board Regulation 14: registered independent testing laboratories certify gaming software; the Board relies on their results, may field test, and verifies installed software by hash signature | The highest-assurance model here; the closest match to control SI-1 (integrity manifest) in `RMF_for_EMS_Control_Catalog.md`. |

**Implication for forensic examination (see `Co-Judge_Reconciliation_Concept.md`):** because
Texas exempts digital evidence from forensic-lab accreditation, no Texas rule currently
requires that a digital forensic examination of election equipment be performed by an
accredited lab or licensed analyst. A program could *choose* to require the voluntary
digital/multimedia analyst license as a qualification standard.

## 4. Where an analysis tool fits under Texas law

Read-only software that analyzes **exported** election records does not cast or count votes,
so it appears to fall outside the Chapter 122 voting-system certification requirement. That
reading changes if the tool's output is used to **alter official results** or **decide
whether a ballot is accepted** — at that point it starts to function as part of the voting
system or the ballot-qualification process. Two design rules keep the tool clearly outside
certified territory:

1. **Never install it on the EMS or any certified component.** Installing software on a
   certified system changes its certified configuration (see the RMF memo, Section 6). The
   tool runs on **separate** administrator-controlled hardware, and data reaches it through
   the removable-media regime (RMF memo, Section 4).
2. **Its findings are advisory.** Discrepancies go to the co-judges and the canvassing
   authority, which decide what to do under existing law. The tool never writes back to
   official records.

This reading should be confirmed with the Texas Secretary of State's Elections Division
(for example, by requesting written guidance) before relying on it.

## 5. The endorsement path

No single mechanism above is sufficient on its own; stacked, they give a tool credibility
comparable to certified election equipment. Each step names its precedent.

### Step 1 — Read standard, published formats *(Model 7)*

- Import the NIST CVR format (SP 1500-103) and election-results format (SP 1500-100).
- Many deployed systems were certified to earlier guidelines and may not export the NIST
  formats, so vendor-specific importers will also be needed. Document **every** importer's
  field mapping publicly, the way the Comptroller publishes the EARS record layout.
- Publish the tool's own output formats (reports, discrepancy logs) so third parties can
  re-check its conclusions.

### Step 2 — Make the build verifiable *(Models 5, 10)*

- Open-source the code, or escrow it with the state under the North Carolina / New York
  escrow model.
- Use reproducible builds and publish the SHA-256 hash of every release, so an
  administrator can confirm the installed binary matches the reviewed code — the field
  verification Nevada requires and the RMF memo's SI-1 control.
- Code-sign releases. Store-distributed packages are signed by the store, but air-gapped
  county machines need an offline, signed package whose hash is published independently.

### Step 3 — Get independent security testing *(Model 5)*

- Commission a third-party penetration test and source review, as Arlo did, and publish a
  summary of findings and fixes.
- Repeat for major releases.

### Step 4 — Get independent validation from a university program *(Model 4)*

- Ask a university computer-science or election-research group to validate the tool against
  the acceptance matrix in Step 5, following the Indiana VSTOP and Connecticut UConn models.
  A Texas university would give the review the most standing with Texas officials.
- The university's role is to test and report, not to certify; its report is what officials
  and courts can rely on.

### Step 5 — Pass a published functional acceptance test matrix *(Model 2)*

Modeled on the Texas e-pollbook Functional Testing Matrix. Illustrative categories:

| Category | Test | Pass criterion |
|---|---|---|
| Correctness | Reproduce the official canvass of a past election from its CVR and results data | Every contest total matches the official canvass exactly |
| Discrepancy detection | Run against a dataset with deliberately seeded errors (missing batches, duplicated CVRs, count mismatches) | Every seeded error is reported; no false positives on the clean dataset |
| Determinism | Run the same input twice | Byte-identical reports (report hashes match) |
| Data handling | Inspect file access during a run | Inputs opened read-only; no network access; source files unchanged (hashes before and after) |
| Performance | Synthetic dataset sized for a 500,000+ ballot county, on documented reference hardware | Full reconciliation report within a published target time that fits comfortably in the canvass window |
| Audit logging | Review the tool's own log | Every input file (with hash), parameter, and output recorded |
| Usability | Non-technical co-judges run the core workflow from the documentation | They can complete it and interpret the discrepancy report |

**Re-validate annually and on every release**, mirroring the annual renewal in Elec. Code
§31.014(b).

### Step 6 — Operate under the administrator's authority *(co-judge concept)*

- Run on administrator-owned, air-gapped hardware; inputs arrive through the clean/dirty media
  workflow (RMF memo, MP-1 to MP-5).
- Co-judges witness the run and co-sign the reconciliation report; disagreements follow the
  tiered rule in the co-judge memo (Section 4).
- Findings are advisory to the canvassing authority (for county elections, the commissioners
  court, Elec. Code §67.002).

## 6. Formal routes to official recognition

Ranked from least to most effort:

1. **Written guidance from the Secretary of State** confirming that a read-only analysis tool
   on separate hardware is not a voting system, and describing acceptable use.
2. **Adoption by rule or practice**, the way states adopted ColoradoRLA and Arlo — a county or
   the SoS names the tool in its audit or canvass procedures.
3. **Propose an EAC ESTEP category** for audit and analysis software, so a federal voluntary
   standard exists.
4. **Legislation** creating an approval pathway for "election audit and analysis software,"
   modeled on the e-pollbook statute (§31.014): accredited-lab technical exam, SoS functional
   exam against a published matrix, annual renewal — plus the read-only and advisory-only
   design rules in Section 4.

TX-RAMP applies only if a state agency or university buys the tool as a cloud service;
county use of desktop software is outside it.

## 7. Honest assessment

- **Endorsement is not legal authority.** Even a fully validated tool produces findings that
  officials *may* rely on; it does not change who decides. That is a feature: it keeps the
  tool out of certification and keeps responsibility with the administrator and canvassing
  authority.
- **The paper record remains the ground truth.** Analysis of electronic records finds
  discrepancies and directs attention; the risk-limiting audit of paper ballots is what
  confirms the outcome.
- **Vendor formats are the practical bottleneck.** Until deployed systems export the NIST
  formats, much of the effort goes into documented, tested importers for each vendor.
- **The strongest near-term step is Step 5.** A published acceptance matrix, passed on real
  past-election data, is concrete evidence that officials can evaluate quickly — and it is
  the core of every other route.

## 8. References

- Tex. Elec. Code §§31.014 (electronic pollbook certification), 67.002 (canvassing
  authority), 67.003 (time for local canvass), 122.035 (voting-system examiners); 1 TAC
  §81.60; Texas SoS, *Texas Certification Procedures for Electronic Pollbooks*.
- U.S. EAC, Election Supporting Technology Evaluation Program (ESTEP) and Voluntary
  Electronic Poll Book Certification Requirements 1.0.
- Indiana Code §3-11-18.1-12 (VSTOP e-pollbook examination); Connecticut General Statutes
  §9-320f, as amended by PA 21-2 (June Special Session) (UConn-authorized electronic audits).
- ColoradoRLA (Colorado Department of State, open source); Arlo (VotingWorks, open source).
- Maryland State Board of Elections, automated ballot-image audit plan; COMAR 33.08.05.09.
- U.S. EAC, VVSG 2.0, Principle 4 (interoperability); NIST SP 1500-100 (election results
  common data format) and SP 1500-103 (cast vote records common data format).
- Texas Comptroller of Public Accounts, Electronic Appraisal Roll Submission (EARS) record
  layout and instructions.
- Texas Commission on State Emergency Communications, NG911 GIS Data Standard; NENA NG9-1-1
  GIS Data Model (NENA-STA-006).
- Tex. Gov't Code §2054.0593 (TX-RAMP).
- Tex. Code Crim. Proc. arts. 38.01, 38.35; Texas Forensic Science Commission accreditation
  and digital/multimedia evidence analyst licensing.
- Nevada Gaming Control Board, Regulation 14 (independent testing laboratories).
- Companion memos: `Co-Judge_Reconciliation_Concept.md`, `RMF_for_EMS_Control_Catalog.md`.
