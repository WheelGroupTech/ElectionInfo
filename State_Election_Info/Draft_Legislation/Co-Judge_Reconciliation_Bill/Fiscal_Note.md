# Fiscal Note and Impact Statement (Drafter's Estimate)

**90th Legislature, Regular Session (2027)**
**Bill:** relating to a bipartisan pre-canvass reconciliation of election records and a bounded,
non-destructive integrity examination of voting system equipment; creating criminal offenses.

> This is a sponsor-side estimate prepared to support drafting. The official fiscal note will come
> from the Legislative Budget Board, based on agency responses from the Secretary of State and
> county surveys. Every figure below rests on the stated assumptions, which are estimates and not
> survey data.

---

## Estimated two-year net impact to General Revenue

**Negative impact of about $2.32 million** through the biennium ending August 31, 2029, almost all
of it for the Secretary of State. Federal Help America Vote Act election-security funds may cover
part of the state cost, subject to federal grant terms.

## Five-year impact to the State (General Revenue)

| Fiscal year | State cost | Main driver |
|---|---|---|
| 2028 | ($890,000) | Rules, forms, and training build; examiner team hired mid-year; forensic equipment |
| 2029 | ($1,431,000) | November 2028 general election (about 126 county examinations assumed) |
| 2030 | ($1,063,000) | November 2029 constitutional amendment election (about 63 examinations) |
| 2031 | ($1,431,000) | November 2030 gubernatorial general election |
| 2032 | ($1,063,000) | November 2031 constitutional amendment election |

No change in state revenue is expected. Criminal penalties are unlikely to produce a significant
number of prosecutions or a significant correctional cost.

## Fiscal analysis

The bill lets the county chair of either of the two leading political parties request a
bipartisan reconciliation of all county election records before the local canvass, plus a
non-destructive integrity examination of a sample of voting equipment. The work is done by or for
the county's general custodian of election records. The Secretary of State (SOS) must adopt rules
and forms, train co-judges and examiners, maintain a pool of qualified examiners available to
counties at no charge, and report every two years.

## Methodology and assumptions

### State (Secretary of State)

| Item | Annual | One-time | Basis |
|---|---|---|---|
| Elections Division: 1 attorney and 1 program/training specialist | $250,000 | n/a | Rules, forms, training, help desk, biennial report |
| State examination team: 5 digital-forensics examiners | $700,000 | n/a | About $140,000 per examiner, salary plus benefits; hired about March 2028 (half year in FY 2028) |
| Forensic software licenses and maintenance | $50,000 | n/a | Imaging, hashing, and log-analysis tools |
| Forensic field kits (6 kits: write-blockers, offline workstations, write-once media, seals) | n/a | $90,000 | About $15,000 per kit |
| Online training and machine-readable forms | n/a | $150,000 | Development in FY 2028 |
| Contract examiner surge in general-election years | $305,000 | n/a | Examiner hours above in-house peak capacity, at $200 per hour |
| Travel for state examiners | $63,000 to $126,000 | n/a | About $1,000 per county examination |

**Examiner hours per county examination:** 16 (small), 32 (medium), 80 (large). In-house peak
capacity is about 1,500 hours across the 6-week post-election period. At 50% uptake in a general
election, demand is about 3,000 hours. The difference is met by contract examiners acting as the
county custodian's agents, as the bill requires.

### Counties

The bill does not require any county to spend money unless a party chair requests an examination,
or the custodian starts one on its own. When an examination is held, the county pays co-judge
compensation (at the central counting station presiding-judge rate), the co-judges' bonds, staff
time, and supplies. The state supplies examiners.

| County size (estimated number of counties) | Recurring cost per examination | One-time setup | What drives it |
|---|---|---|---|
| Small, under about 50,000 population (about 189) | about $4,600 | about $1,500 | 2 co-judges × 40 hours; 60 staff hours; seals and media; bonds. Setup: video where none exists |
| Medium, about 50,000 to 250,000 (about 45) | about $13,100 | about $3,000 | 2 co-judges × 80 hours; 200 staff hours. Setup: offline analysis workstation |
| Large, about 250,000 and over (about 20) | about $47,400 | about $10,000 | 2 co-judges × 120 hours; 800 staff hours. Setup: analysis hardware, extra secure workspace |

The county counts by size are approximate and need confirmation against current census estimates.

**Statewide county cost per general election, by share of counties requesting:**

| Uptake | Examinations | Recurring county cost | First-time setup |
|---|---|---|---|
| 25% | about 63 | about $0.60 million | about $0.15 million |
| 50% (assumed) | about 126 | about $1.19 million | about $0.31 million |
| 100% | 254 | about $2.41 million | about $0.62 million |

Costs are lower in counties that already meet related requirements. Counties of 100,000 or more
already livestream areas holding voted ballots (Elec. Code §127.1232), and counties of 250,000 or
more already keep electronic activity logs at the central counting station (§127.009). Every
county already prepares the §127.131(f) reconciliation, runs the §127.201 manual count, and takes
part in the risk-limiting audit (§127.302, as amended in 2025). The bill reconciles those existing
outputs rather than duplicating them.

### Equipment and vendors

The default examination is non-invasive, uses the voting system's own export functions, and does
not change the certified configuration. No re-certification cost is expected for those steps.
Invasive work (opening a unit) happens only on an objective anomaly with SOS approval. It is
scheduled on units not needed until re-acceptance, which costs a logic-and-accuracy test and
resealing (staff time only). Requiring vendors to supply trusted-build manifests and export tools
as a condition of certification (§127.429(b)) may raise future certification-application costs
for vendors. That cost is not borne by the state.

## Technology

The SOS will need secure storage and distribution for trusted-build reference files, building on
its existing practice of furnishing hash values for logic-and-accuracy testing. A reconciliation
software tool that meets §127.426 can be used without voting-system certification, but the bill
does not require the state to buy one.

## Local government impact

The impact is permissive, triggered by a party request, and varies with county size and uptake.
It could be significant for a large county that holds an examination in a general election (about
$47,000 per election). It is likely modest for small counties (about $5,000 per election). The SOS
examiner pool removes what would otherwise be the largest county cost: specialized forensic labor.

## Sensitivity

- If every county requests an examination in every general election, state costs rise by about
  $0.7 million in general-election years (more contract surge and travel), and county costs double
  from the 50% case.
- If co-judge pay is set higher than the assumed $20 to $25 an hour, each $5 an hour adds about
  $70,000 statewide at 50% uptake.
- Phasing the bill in (for example, the integrity examination only in counties of 250,000 or more)
  would largely eliminate the contract surge and travel, saving about $0.4 million in
  general-election years.

**Source agencies (for LBB):** 307 Secretary of State; 304 Comptroller of Public Accounts;
Texas Association of Counties; county election officials.
