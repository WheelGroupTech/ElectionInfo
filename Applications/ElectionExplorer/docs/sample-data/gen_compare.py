#!/usr/bin/env python3
"""Produce an edited 'after' copy of the synthetic voter list for Compare demos.

Reads sample_voters_large.csv (the 'before' file) and writes an 'after' file that,
when compared by Voter ID, exercises every Compare bucket:
  name minor / name major, address minor / address major, precinct-changed
  (address unchanged), only-in-A (removed), only-in-B (added), and identical.

Fully synthetic; matching key is VUIDNO (kept for changed rows, new for adds).
"""
import csv
import random
import sys

random.seed(4242)

src = sys.argv[1] if len(sys.argv) > 1 else "docs/sample_voters_large.csv"
dst = sys.argv[2] if len(sys.argv) > 2 else "docs/sample_voters_large_v2.csv"

with open(src, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    header = reader.fieldnames
    rows = list(reader)

n = len(rows)
idx = list(range(n))
random.shuffle(idx)

def take(k):
    out = idx[:k]
    del idx[:k]
    return out

sel = {
    "name_minor": take(14),
    "name_major": take(7),
    "addr_minor": take(16),
    "addr_major": take(9),
    "pct_changed": take(11),
    "removed": take(12),
}

STREETS = ["Guadalupe", "Lamar", "Congress", "Burnet", "Riverside", "Slaughter",
           "Parmer", "Braker", "Metric", "Menchaca", "Brodie", "Escarpment",
           "Balcones", "Far West", "Pecan", "Elm", "Oak", "Maple", "Cedar",
           "Magnolia", "Sycamore", "Creekside", "Lakeview", "Ridgeline"]
STR_TYPES = ["ST", "AVE", "DR", "BLVD", "LN", "RD", "CT", "WAY", "TRL"]
MARRIED_NAMES = ["Nguyen", "Patel", "OBrien", "Whitfield", "Castillo", "Hargrove",
                 "Sinclair", "Delgado", "Fairbanks", "Underwood", "Vance", "Yamamoto"]

def flip_one_char(s):
    if len(s) < 3:
        return s + "e"
    i = random.randint(1, len(s) - 2)
    c = s[i]
    repl = random.choice([ch for ch in "abcdefghijklmnopqrstuvwxyz" if ch != c.lower()])
    return s[:i] + repl + s[i + 1:]

name_minor = set(sel["name_minor"])
name_major = set(sel["name_major"])
addr_minor = set(sel["addr_minor"])
addr_major = set(sel["addr_major"])
pct_changed = set(sel["pct_changed"])
removed = set(sel["removed"])

for i, r in enumerate(rows):
    if i in name_minor:
        # one-character typo in the first name (data-entry style)
        r["FSTNAM"] = flip_one_char(r["FSTNAM"])
    elif i in name_major:
        # surname change (e.g., marriage) -> large edit distance
        r["LSTNAM"] = random.choice([m for m in MARRIED_NAMES if m != r["LSTNAM"]])
    if i in addr_minor:
        # small change: unit added/changed, or block number nudged by 2
        if r["UNITNO"]:
            base = "".join(ch for ch in r["UNITNO"] if ch.isdigit()) or "1"
            r["UNITNO"] = str(int(base) + 1)
        else:
            try:
                r["BLKNUM"] = str(int(r["BLKNUM"]) + 2)
            except ValueError:
                r["UNITYP"], r["UNITNO"] = "APT", str(random.randint(1, 40))
    elif i in addr_major:
        # a move: different street (and drop any unit)
        r["BLKNUM"] = str(random.randint(100, 9900))
        r["STRPRE"] = random.choice(["", "", "N", "S", "E", "W"])
        r["STRNAM"] = random.choice([s for s in STREETS if s != r["STRNAM"]])
        r["STRTYP"] = random.choice(STR_TYPES)
        r["UNITYP"], r["UNITNO"] = "", ""
    if i in pct_changed:
        # re-precincting: change precinct only, address unchanged
        old = int(r["PCTCOD"])
        r["PCTCOD"] = str(old + 1 if old < 460 else old - 1)

out_rows = [r for i, r in enumerate(rows) if i not in removed]

# Additions: new synthetic voters (new VUIDs) -> only-in-B.
existing_ids = {r["VUIDNO"] for r in rows}
FIRST = ["Jordan", "Taylor", "Morgan", "Avery", "Riley", "Cameron", "Quinn",
         "Reese", "Rowan", "Sage", "Dylan", "Harper"]
LAST = ["Ellison", "Marsh", "Calloway", "Bishop", "Frost", "Aguilar", "Beckett",
        "Contreras", "Hollis", "Pruitt", "Salazar", "Winters"]
CITIES = [("Austin", "78704"), ("Austin", "78745"), ("Round Rock", "78664"),
          ("Pflugerville", "78660"), ("Cedar Park", "78613"), ("Leander", "78641")]
added = 0
for _ in range(12):
    while True:
        vuid = str(random.randint(1000000000, 1099999999))
        if vuid not in existing_ids:
            existing_ids.add(vuid)
            break
    city, zc = random.choice(CITIES)
    row = {h: "" for h in header}
    row.update({
        "VUIDNO": vuid, "PCTCOD": str(random.randint(101, 460)),
        "PCTSPT": random.choice(["", "A", "B", "C"]),
        "EDRDAT": "2026{:02d}{:02d}".format(random.randint(1, 8), random.randint(1, 28)),
        "GENDER": random.choice(["M", "F"]),
        "LSTNAM": random.choice(LAST), "FSTNAM": random.choice(FIRST),
        "MIDNAM": random.choice(["", "A", "L", "M", "J"]),
        "BLKNUM": str(random.randint(100, 9900)),
        "STRNAM": random.choice(STREETS), "STRTYP": random.choice(STR_TYPES),
        "RSCITY": city, "RZIPCD": zc,
    })
    out_rows.append(row)
    added += 1

out_rows.sort(key=lambda r: (int(r["PCTCOD"]), r["LSTNAM"], r["FSTNAM"]))

with open(dst, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=header)
    w.writeheader()
    w.writerows(out_rows)

print("before rows: {}".format(n))
print("after rows : {}".format(len(out_rows)))
print("edits applied (matched by Voter ID):")
print("  name minor      : {}".format(len(name_minor)))
print("  name major      : {}".format(len(name_major)))
print("  address minor   : {}".format(len(addr_minor)))
print("  address major   : {}".format(len(addr_major)))
print("  precinct changed: {}".format(len(pct_changed)))
print("  removed (only in A): {}".format(len(removed)))
print("  added   (only in B): {}".format(added))
