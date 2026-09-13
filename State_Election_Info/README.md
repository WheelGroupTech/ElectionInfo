# State_Election_Info

Reference dataset of official state-government election resources for all 50 states.
For each state it records the **statutory election code** (citation and official source
URL), the state's **chief election-administration agency** and its official website, and
the public **voter portal**.

Unlike most folders in this repository, this one contains **data, not scripts**. The same
records are provided in three formats so they can be read by people or consumed by the app.

## Files

| File | Overview |
|------|----------|
| `state_election_codes.json` | Canonical source. Top-level `title`, `description`, `generated` date, `notes`, and a `states` array of 50 records. Intended for programmatic use. |
| `state_election_codes.csv` | Flat, one row per state (same fields as the JSON records) for spreadsheets and imports. |
| `State_Election_Codes.md` | Human-readable table of all 50 states. |
| `generate.py` | Regenerates the CSV and Markdown from the JSON (see below). |

The three data files are kept in sync: the CSV and Markdown are generated from the JSON by
`generate.py`.

## Record schema

Each entry in the JSON `states` array (and each CSV row) has these fields:

| Field | Meaning |
|-------|---------|
| `state` | State name |
| `abbr` | Two-letter USPS abbreviation |
| `code_citation` | Statutory location of the election code (e.g., title/chapter) |
| `code_url` | Official source for the code (legislature / legislative-service site, or the state's designated official code publisher) |
| `code_url_secondary` | Alternate official source, or `null` — a compiled PDF, or the LexisNexis public-access portal for states whose official code is published only through LexisNexis (AR, GA, MS, TN) |
| `admin_agency` | The state's chief election authority (see note below) |
| `admin_url` | Official website of that agency |
| `voter_portal_url` | Public voter portal — register / check registration / find polling place / view ballot (empty for Wyoming) |

## Notes on the data

- **The chief election authority is not always the Secretary of State.** 17 states run
  elections through a Board or Commission, a Department of Elections, or the Lieutenant
  Governor. The `admin_agency` field names the actual body (e.g., Wisconsin Elections
  Commission, North Carolina State Board of Elections, Utah Lieutenant Governor's Office,
  Virginia Department of Elections).
- **A few states' official code is published only through LexisNexis** under contract with
  the legislature (Arkansas, Georgia, Mississippi, Tennessee). For those, the free
  public-access portal is the `code_url_secondary`, with the legislature/SOS landing page
  as the primary `code_url`.
- **Wyoming has no statewide voter portal** — it has no online voter registration, and
  records are held by county clerks — so its `voter_portal_url` is empty.
- **Deep links can shift** when codes are recompiled or portals are updated. The
  `code_citation` lets you re-navigate a state's statute browser if a link breaks.
- Verified via web research on **2026-09-13** (also recorded in the JSON `generated` field).

## Regenerating the CSV and Markdown

`state_election_codes.json` is the source of truth. If you edit it, regenerate the other
two files with `generate.py` rather than hand-editing them, so all three stay consistent:

```
python generate.py
```

The script uses only the Python standard library and resolves paths relative to itself, so
it can be run from any directory. It rebuilds `state_election_codes.csv` and
`State_Election_Codes.md` in place; the verification date shown in the Markdown comes from
the JSON `generated` field.
