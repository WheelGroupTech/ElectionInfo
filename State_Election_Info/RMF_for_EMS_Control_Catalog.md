# RMF-for-EMS: A Control Catalog for Auditing Election Management Systems

*Adapting U.S. Department of Defense / Intelligence Community assurance practices for
Election Management Systems (EMS).*

**Status:** Concept / analysis memo. Not legal advice and not an official standard.
Implementation must be validated against each state's certification rules and the U.S.
Election Assistance Commission (EAC) Voluntary Voting System Guidelines (VVSG); some
controls below **change a certified configuration and cannot be applied unilaterally**
(see the *Certification impact* column).
**Date:** 2026-09-13.

---

## 1. Why the military model fits

Classified/national-security systems and EMS share the defining trait that ordinary IT
security assumes away: **you cannot rely on the machine being patched, current, or clean.**
Most EMS run older, not-fully-patched Windows builds, and their principal security control
is an air gap — which is porous, because the EMS ingests and emits data on **USB / removable
media**.

The DoD faced precisely this after the 2008 *agent.btz* worm crossed into classified
networks on a USB stick (prompting Operation Buckshot Yankee and the modern DoD
removable-media regime). Stuxnet later showed the same air-gap-via-USB / vendor-laptop
vector against isolated industrial control systems. The military response was **not** "patch
everything." It was to move assurance off the patch cycle and onto four load-bearing
controls:

1. **Deny-by-default execution** (application whitelisting) — so *unpatched* ≠ *exploitable
   at will*.
2. **A disciplined removable-media regime** — because the air gap's only real hole is what
   you carry across it.
3. **Continuous, off-box audit logging** that is actually reviewed, independently.
4. **Cryptographic baseline / integrity verification** before and after every use.

Patching is desirable but secondary; those four are the walls that hold the roof up.

## 2. How to read this catalog

Each control lists its DoD/NIST source, the election adaptation, and two tags:

**Certification impact**
- **Safe** — does not alter the certified voting-system software or configuration. These are
  process, media-handling, external-assessment, personnel, and log-review controls. A
  jurisdiction can generally adopt them without re-certification.
- **Affecting** — changes software or configuration on the certified system (installs an
  agent, changes the OS baseline, adds device-control drivers). These **must be adopted into
  the certified baseline together with the EAC, the state certification authority, and the
  vendor** — applying them unilaterally can *decertify* the equipment.

**Capacity tier** — who can realistically own/perform it:
- **County (C)** — a local election office can do it with training.
- **State (S)** — needs a state-level team, tooling, or a shared regional service.
- **Federal/Vendor (F)** — depends on the EAC, a federally accredited lab (VSTL), or the
  voting-system vendor (build signing, SBOM, escrow).

## 3. The control catalog

Grouped by NIST SP 800-53 control family. "DoD analog" names the practice the adaptation is
drawn from.

### Access Control & Personnel (AC / PS / PM)

| # | DoD analog | Election adaptation | Cert. impact | Tier |
|---|---|---|---|---|
| AC-1 | Two-Person Integrity (TPI) / No-Lone-Zone | Two-person rule for **all** EMS access; no single operator alone with the EMS | Safe | C |
| AC-2 | Least privilege / role separation | Distinct, individually attributable EMS accounts (no shared logins); roles split (config vs. tabulate vs. report) | Affecting (account config) | C/S |
| PS-1 | Personnel Reliability Program (PRP) / clearances | Background vetting of EMS operators; documented reliability standard | Safe | C/S |
| PM-1 | Insider-threat program / User Activity Monitoring (UAM) | Logged, reviewable operator activity; bipartisan review of access logs | Safe | C/S |

### Configuration Management & System Integrity (CM / SI)

| # | DoD analog | Election adaptation | Cert. impact | Tier |
|---|---|---|---|---|
| CM-1 | **Application whitelisting** (deny-by-default execution) | Only signed, approved EMS binaries may execute — **highest-value control for unpatched EMS**; dropped payloads won't run | **Affecting** | F (build) / S |
| CM-2 | STIG hardening baselines | Published EMS hardening baseline (services, ports, accounts, autorun off) | **Affecting** | F/S |
| CM-3 | SCAP automated config compliance | Automated scan confirms the running config matches the hardened baseline before L&A | Safe (read-only scan) | S |
| SI-1 | File-integrity monitoring / **trusted build** | Cryptographic manifest of the **entire** installed image vs. the certified/escrowed build, verified pre- and post-election ("more than one hash") | Safe if verify-only against external manifest; Affecting if an agent is installed | S / F |
| SI-2 | Golden-image rebuild | Re-image from a verified golden build between cycles rather than trusting persistence | Affecting | F/S |

### Media Protection (MP) — the air-gap / USB regime

| # | DoD analog | Election adaptation | Cert. impact | Tier |
|---|---|---|---|---|
| MP-1 | Device Control (HBSS/ESS DCM): USB whitelisting by hardware ID | Only enrolled, serial-numbered election media are recognized; all other devices inert; autorun disabled | **Affecting** | F/S |
| MP-2 | Clean/dirty scanning kiosk | Any media that touched an outside system passes through a standalone scanning station before nearing the EMS | Safe | C/S |
| MP-3 | Media custodian & inventory | Numbered media inventory, tamper-evident storage, check-in/out logs — treat media like COMSEC keying material | Safe | C |
| MP-4 | Trusted download / two-person media review | Data crossing the gap is verified (file set + hashes) by two people and logged | Safe | C |
| MP-5 | Physical port control | Disable or physically block unused USB controllers and ports | Affecting | F/S |

### System & Communications Protection (SC)

| # | DoD analog | Election adaptation | Cert. impact | Tier |
|---|---|---|---|---|
| SC-1 | Data diode / one-way transfer | Results leave the EMS only via a one-way, verified transfer — never a bidirectional link | Safe–Affecting (depends on method) | S/F |
| SC-2 | Cross-Domain Solution boundary discipline | Formal boundary between the air-gapped EMS enclave and any networked reporting system | Safe | C/S |

### Risk Assessment & Vulnerability Management (RA)

| # | DoD analog | Election adaptation | Cert. impact | Tier |
|---|---|---|---|---|
| RA-1 | ACAS / Nessus vulnerability scanning | Scan a **non-production twin** (never the sealed production unit) to enumerate the CVE list the authorization must address | Safe (twin only) | S |
| RA-2 | Known-vuln acceptance register | Documented list of unpatched CVEs with the compensating control for each | Safe | S |

### Assessment & Authorization (CA)

| # | DoD analog | Election adaptation | Cert. impact | Tier |
|---|---|---|---|---|
| CA-1 | **RMF Authorization to Operate (ATO)** | A named authorizing official (administrator, co-signed by the bipartisan co-judges) formally accepts residual risk **each cycle**, with a Plan of Action & Milestones (POA&M) | Safe | C/S |
| CA-2 | Independent Security Control Assessor (SCA) | An independent security-control assessment, under the administrator's authority | Safe | S |
| CA-3 | Red Team / penetration test | Adversarial test **on a test unit only** — never the sealed production system | Safe (twin only) | S/F |

### Audit & Accountability (AU)

| # | DoD analog | Election adaptation | Cert. impact | Tier |
|---|---|---|---|---|
| AU-1 | Comprehensive audit logging (AU family) | EMS configured to log who/what/when: config changes, media insertions, access events | Affecting (log config) | F/S |
| AU-2 | Off-box log aggregation | Export logs to write-once media / a separate collector so logs can't be edited on the box | Safe | C/S |
| AU-3 | **Continuous Monitoring (ConMon) + independent review** | Logs reviewed post-election by the **bipartisan co-judges** as part of reconciliation (see companion memo) | Safe | C |

### Supply Chain & Maintenance (SR / MA / IR)

| # | DoD analog | Election adaptation | Cert. impact | Tier |
|---|---|---|---|---|
| SR-1 | SBOM / build provenance | Require a Software Bill of Materials and build provenance as a certification condition | Affecting (certification req.) | F |
| SR-2 | Source/build escrow | Certified build + source held in escrow, reviewable by the state/independent expert (NC/NY model) | Safe (procedural) | S/F |
| MA-1 | Controlled maintenance devices | Vendor maintenance uses a dedicated, baselined, whitelisted device — never the vendor's own laptop (the Stuxnet vector) | Affecting | F/S |
| IR-1 | Forensic readiness / incident response | Pre-planned, chain-of-custody-preserving procedure to image and examine a unit if an anomaly is found | Safe | S |

## 4. The USB / removable-media playbook (detail)

The single most transferable piece of DoD doctrine. Treat every piece of media as hostile
until proven otherwise:

- **Deny by default at the port.** Device-control recognizes only enrolled, serial-numbered
  election media (by hardware ID); autorun/autoplay disabled system-wide. A random USB does
  nothing.
- **Clean/dirty workflow.** Media that has touched any outside system (ballot layouts,
  outbound results, vendor updates) transits a standalone **scanning kiosk** running current
  detection tooling before it goes near the EMS. Media never moves machine-to-machine
  without transiting the kiosk.
- **One-directional, verified transfers.** Data crossing the gap moves under a two-person
  **trusted download**: both co-signers verify the file set and hashes, logged.
- **Media custody.** A media custodian, numbered inventory, tamper-evident storage, and
  check-in/out logs — media handled like COMSEC keying material, not office supplies.
- **Kill the deeper vectors.** BadUSB-class firmware attacks defeat antivirus, so device
  whitelisting + disabling unused USB controllers + physical port control matter more than
  scanning alone. Vendor maintenance (a classic Stuxnet-style vector) uses a dedicated
  baselined device, never a vendor-owned laptop.

## 5. Audit cadence

- **Pre-election:** verify the integrity manifest against the certified build → run the
  hardened-config (SCAP) scan → confirm whitelisting active → seal, with co-judges
  witnessing and co-signing.
- **During:** tamper-evident seals + logs under two-person control; media only via the
  clean/dirty workflow.
- **Post-election, pre-canvass:** export and **independently review the audit logs**
  (device-insertion history, config changes, access events), re-verify the integrity
  manifest, reconcile. The co-judges sign the assurance report that feeds certification.
  This is the bounded, non-destructive forensic examination described in the companion memo,
  now backed by military-grade log and integrity artifacts.

## 6. The certification problem (the real blocker)

Every **Affecting** control above alters a federally/state-certified configuration and can
*decertify* the equipment if bolted on after the fact — the same trap that led Colorado
(SB22-153) and Pennsylvania to *restrict* third-party machine access. The path forward is
not local bolt-ons but **baking these controls into the certified baseline** in cooperation
with the EAC, the state certification authority, and the vendor. Application whitelisting,
device control, hardened baselines, and comprehensive logging are all things a vendor can
ship *inside* a certified build; the policy work is requiring them there.

## 7. Capacity tiering summary

| Tier | Realistic scope | Examples |
|---|---|---|
| **County (C)** | Process, custody, two-person rules, media handling, log export, ATO sign-off | MP-2/3/4, AC-1, AU-2/3, CA-1 |
| **State (S)** | Shared assessment team, scanning, SCA, baseline authorship | CM-3, RA-1/2, CA-2/3, SR-2, AU config support |
| **Federal/Vendor (F)** | Anything inside the certified build | CM-1/2, MP-1/5, SI-2, SR-1, MA-1, AU-1 |

The county cannot carry this alone — mirroring how DoD centralizes Security Control
Assessors and tooling. Realistically this is a **state or regional shared service**. The
existing on-ramps are **CISA** (voluntary scanning/assessments for election infrastructure)
and the **EI-ISAC**; the gap is that their help is voluntary and inconsistent rather than a
mandated RMF-style program with a formal authorization decision.

## 8. Highest-leverage priorities

1. **Application whitelisting** (CM-1) — neutralizes most unpatched-OS risk.
2. **Removable-media regime** (MP-1..5) — closes the actual air-gap hole.
3. **Full-image integrity verification against a trusted build** (SI-1) — pre/post election.
4. **Independent audit-log review by bipartisan co-judges** (AU-3) — ties to the companion
   reconciliation concept.

## 9. References

- NIST SP 800-37 (Risk Management Framework), SP 800-53 / 800-53A (security & privacy
  controls and assessment).
- DISA STIGs; SCAP; DoD Endpoint Security Solution (ESS, formerly HBSS) Device Control.
- U.S. EAC Voluntary Voting System Guidelines (VVSG 2.0).
- CISA — Election Security / election infrastructure services; Elections Infrastructure ISAC
  (EI-ISAC).
- Companion memo: `Co-Judge_Reconciliation_Concept.md`.
- State statutory election codes and chief-election-authority sites:
  `state_election_codes.json`.
