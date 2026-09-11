#!/usr/bin/env python3
"""Generate a realistic SYNTHETIC voter list with planted duplicates for the
duplicate-detection screenshots. Includes a DOB column so both scans work:
  * duplicate Voter IDs (same VUIDNO on 2+ rows)
  * duplicate voters by Name + DOB (same name and birthdate, different VUIDNO)

All data is fabricated (seeded RNG). No real voter data.
"""
import csv
import random
import sys

random.seed(90210)

N = int(sys.argv[2]) if len(sys.argv) > 2 else 1500
out_path = sys.argv[1] if len(sys.argv) > 1 else "sample_voters_dups.csv"

SURNAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson",
    "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez",
    "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis",
    "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres",
    "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall",
    "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Patel", "Reyes",
    "Cruz", "Gomez", "Murphy", "Bailey", "Cooper", "Richardson", "Cox", "Howard",
    "Ward", "Peterson", "Gray", "Ramos", "Watson", "Brooks", "Kelly", "Sanders",
    "Price", "Bennett", "Wood", "Barnes", "Ross", "Henderson", "Coleman", "Chen"]
FIRST_M = ["James", "John", "Robert", "Michael", "David", "William", "Richard",
    "Joseph", "Thomas", "Charles", "Christopher", "Daniel", "Matthew", "Anthony",
    "Mark", "Steven", "Paul", "Andrew", "Joshua", "Carlos", "Juan", "Luis",
    "Miguel", "Jose", "Kevin", "Brian", "Eric", "Jacob", "Nathan", "Aaron"]
FIRST_F = ["Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara",
    "Susan", "Jessica", "Sarah", "Karen", "Nancy", "Lisa", "Margaret", "Sandra",
    "Ashley", "Maria", "Ana", "Sofia", "Isabella", "Emily", "Amanda", "Melissa",
    "Stephanie", "Rebecca", "Laura", "Michelle", "Amy", "Angela", "Grace", "Olivia"]
MIDDLE = ["", "", "", "A", "L", "M", "J", "R", "E", "Marie", "Ann", "Lee", "Rose"]
STREETS = ["Guadalupe", "Lamar", "Congress", "Burnet", "Riverside", "Slaughter",
    "Parmer", "Braker", "Anderson", "Duval", "Menchaca", "Brodie", "Escarpment",
    "Balcones", "Far West", "Pecan", "Elm", "Oak", "Maple", "Cedar", "Live Oak",
    "Magnolia", "Sycamore", "Creekside", "Lakeview", "Meadow", "Hillside"]
STR_TYPES = ["ST", "AVE", "DR", "BLVD", "LN", "RD", "CT", "WAY", "TRL", "CIR", "PL"]
STR_PRE = ["", "", "", "", "N", "S", "E", "W"]
UNIT_TYPES = ["APT", "UNIT", "STE", "#"]
CITIES = [("Austin", ["78701", "78702", "78703", "78704", "78705", "78723",
    "78741", "78745", "78748", "78751", "78753", "78758", "78759"]),
    ("Round Rock", ["78664", "78665", "78681"]), ("Pflugerville", ["78660"]),
    ("Cedar Park", ["78613"]), ("Leander", ["78641"]), ("Del Valle", ["78617"])]

header = ["VUIDNO", "PCTCOD", "PCTSPT", "EDRDAT", "DOB", "GENDER", "LSTNAM",
          "FSTNAM", "MIDNAM", "BLKNUM", "STRPRE", "STRNAM", "STRTYP", "UNITYP",
          "UNITNO", "RSCITY", "RZIPCD", "SUSIND", "G22VOTED", "G24VOTED", "P26VOTED"]

used_ids = set()

def new_id():
    while True:
        v = random.randint(1000000000, 1099999999)
        if v not in used_ids:
            used_ids.add(v)
            return v

def voted():
    return "Y" if random.random() < 0.55 else ""

def rand_dob():
    return "{:04d}{:02d}{:02d}".format(random.randint(1935, 2007),
                                       random.randint(1, 12), random.randint(1, 28))

def make_row(vuid=None, gender=None, last=None, first=None, mid=None, dob=None):
    gender = gender or random.choice(["M", "F"])
    first = first or (random.choice(FIRST_M) if gender == "M" else random.choice(FIRST_F))
    last = last if last is not None else random.choice(SURNAMES)
    mid = mid if mid is not None else random.choice(MIDDLE)
    blk = random.choice([random.randint(100, 9999), random.randint(1, 99) * 100])
    if random.random() < 0.28:
        ut = random.choice(UNIT_TYPES)
        un = str(random.randint(1, 400))
    else:
        ut, un = "", ""
    city, zips = random.choice(CITIES)
    return {
        "VUIDNO": vuid if vuid is not None else new_id(),
        "PCTCOD": random.randint(101, 460), "PCTSPT": random.choice(["", "A", "B", "C"]),
        "EDRDAT": "{:04d}{:02d}{:02d}".format(random.randint(2008, 2025),
                                              random.randint(1, 12), random.randint(1, 28)),
        "DOB": dob or rand_dob(), "GENDER": gender, "LSTNAM": last, "FSTNAM": first,
        "MIDNAM": mid, "BLKNUM": blk, "STRPRE": random.choice(STR_PRE),
        "STRNAM": random.choice(STREETS), "STRTYP": random.choice(STR_TYPES),
        "UNITYP": ut, "UNITNO": un, "RSCITY": city, "RZIPCD": random.choice(zips),
        "SUSIND": "S" if random.random() < 0.04 else "",
        "G22VOTED": voted(), "G24VOTED": voted(), "P26VOTED": voted(),
    }

rows = [make_row() for _ in range(N)]

# --- Plant duplicate VOTER IDs: copy an existing row's VUIDNO onto a new record
# (same person entered twice), varying address/precinct so it looks like a real
# double entry. A couple of 3-member groups.
dup_id_groups = 0
dup_id_rows = 0
for k in range(8):
    base = random.choice(rows)
    copies = 2 if k >= 2 else 3  # first two groups have 3 members
    for _ in range(copies):
        r = make_row(vuid=base["VUIDNO"], gender=base["GENDER"],
                     last=base["LSTNAM"], first=base["FSTNAM"], mid=base["MIDNAM"],
                     dob=base["DOB"])
        rows.append(r)
        dup_id_rows += 1
    dup_id_groups += 1

# --- Plant duplicate NAME + DOB: same name and birthdate, DIFFERENT VUIDNO
# (registered twice under different IDs), different address.
dup_namedob_groups = 0
dup_namedob_rows = 0
for _ in range(8):
    base = random.choice(rows[:N])
    r = make_row(gender=base["GENDER"], last=base["LSTNAM"], first=base["FSTNAM"],
                 mid=base["MIDNAM"], dob=base["DOB"])  # new VUIDNO, same name+DOB
    rows.append(r)
    dup_namedob_rows += 1
    dup_namedob_groups += 1

rows.sort(key=lambda r: (int(r["PCTCOD"]), r["LSTNAM"], r["FSTNAM"]))

with open(out_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=header)
    w.writeheader()
    w.writerows(rows)

print("wrote {} rows to {}".format(len(rows), out_path))
print("planted duplicate Voter ID groups : {} ({} extra rows, incl. two 3-member groups)"
      .format(dup_id_groups, dup_id_rows))
print("planted duplicate Name+DOB pairs   : {} ({} extra rows)"
      .format(dup_namedob_groups, dup_namedob_rows))
