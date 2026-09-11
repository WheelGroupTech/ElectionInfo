# Sample data generators

Scripts that produce the **synthetic** voter lists used for Microsoft Store
screenshots and demos. Every name, address, ID, and date is fabricated with a
seeded RNG — no real voter-registration data is used. Output uses the same
Travis-County export schema the app parses, so it derives Voter ID / Precinct /
Name / Address automatically.

The generated CSVs live one level up in `docs/`:

| CSV | Made by | Screenshots it supports |
|-----|---------|-------------------------|
| `sample_voters_large.csv` | `gen_sample.py` | main grid, filtering, sorting, Precinct/Address reports |
| `sample_voters_large_v2.csv` | `gen_compare.py` | Compare + Show Differences (against the file above) |
| `sample_voters_dups.csv` | `gen_dups.py` | duplicate detection (has a `DOB` column) |

## Regenerate

Run from the `ElectionExplorer/` directory (needs Python 3):

```bash
python docs/sample-data/gen_sample.py  docs/sample_voters_large.csv 1500
python docs/sample-data/gen_compare.py docs/sample_voters_large.csv docs/sample_voters_large_v2.csv
python docs/sample-data/gen_dups.py    docs/sample_voters_dups.csv 1500
```

- `gen_sample.py <out> [rows]` — realistic base list.
- `gen_compare.py <before.csv> <after.csv>` — reads the base list and writes an
  edited copy (name/address minor+major, precinct-only, adds, removals) that
  populates every Compare bucket. Matched by Voter ID.
- `gen_dups.py <out> [rows]` — base list plus planted duplicate Voter IDs and
  duplicate Name+DOB records.

Each script is seeded, so reruns are deterministic. Edit the `random.seed(...)`
value or the counts near the top to vary the data.
