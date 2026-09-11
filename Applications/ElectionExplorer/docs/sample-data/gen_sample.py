#!/usr/bin/env python3
"""Generate realistic-looking SYNTHETIC voter data (Travis-County export schema).

All names/addresses are fabricated (seeded RNG). No real voter data is used.
Output columns match ElectionExplorer's test/sample_voters.csv so the app derives
Voter ID / Precinct / Name / Address from them.
"""
import csv
import random
import sys

random.seed(20260911)

N = int(sys.argv[2]) if len(sys.argv) > 2 else 1500
out_path = sys.argv[1] if len(sys.argv) > 1 else "sample_voters_large.csv"

SURNAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Patel", "Reyes", "Cruz", "Gomez", "Murphy", "Bailey",
    "Cooper", "Richardson", "Cox", "Howard", "Ward", "Peterson", "Gray", "Ramos",
    "Watson", "Brooks", "Kelly", "Sanders", "Price", "Bennett", "Wood", "Barnes",
    "Ross", "Henderson", "Coleman", "Jenkins", "Powell", "Long", "Patterson", "Hughes",
    "Washington", "Butler", "Simmons", "Foster", "Bryant", "Alexander", "Russell",
    "Griffin", "Diaz", "Hayes", "Chen", "Kim", "Nguyen", "Tran", "Okafor", "Abbott",
]
FIRST_M = [
    "James", "John", "Robert", "Michael", "David", "William", "Richard", "Joseph",
    "Thomas", "Charles", "Christopher", "Daniel", "Matthew", "Anthony", "Mark",
    "Donald", "Steven", "Paul", "Andrew", "Joshua", "Carlos", "Juan", "Luis",
    "Miguel", "Jose", "Kevin", "Brian", "Eric", "Jacob", "Nathan", "Aaron",
    "Adam", "Henry", "Samuel", "Raymond", "Patrick", "Sean", "Ethan", "Wei", "Omar",
]
FIRST_F = [
    "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan",
    "Jessica", "Sarah", "Karen", "Nancy", "Lisa", "Margaret", "Betty", "Sandra",
    "Ashley", "Maria", "Ana", "Sofia", "Isabella", "Emily", "Amanda", "Melissa",
    "Deborah", "Stephanie", "Rebecca", "Laura", "Michelle", "Amy", "Angela",
    "Cynthia", "Kathleen", "Grace", "Olivia", "Hannah", "Priya", "Fatima", "Mei",
    "Rosa", "Carmen",
]
MIDDLE = ["", "", "", "A", "L", "M", "J", "R", "E", "Marie", "Ann", "Lee", "Ray",
          "Grace", "Dean", "Rose", "Kay", "Paul", "Jean"]

STREETS = [
    "Guadalupe", "Lamar", "Congress", "Burnet", "Cesar Chavez", "Manor", "Airport",
    "Riverside", "Slaughter", "Parmer", "Braker", "Anderson", "Koenig", "Cameron",
    "Duval", "Speedway", "Red River", "San Jacinto", "Nueces", "Rio Grande",
    "Barton Springs", "Oltorf", "William Cannon", "Ben White", "Metric", "Howard",
    "Dessau", "Wells Branch", "Palmer", "Menchaca", "Brodie", "Convict Hill",
    "Escarpment", "Davis", "Bee Cave", "Spicewood Springs", "Balcones", "Steck",
    "Far West", "Jollyville", "McNeil", "Gattis School", "University", "Sunset Valley",
    "Pecan", "Elm", "Oak", "Maple", "Cedar", "Live Oak", "Pinewood", "Meadow",
    "Hillside", "Lakeview", "Ridgeline", "Creekside", "Sycamore", "Magnolia",
]
STR_TYPES = ["ST", "AVE", "DR", "BLVD", "LN", "RD", "CT", "WAY", "TRL", "CIR", "PL", "PKWY"]
STR_PRE = ["", "", "", "", "N", "S", "E", "W"]
UNIT_TYPES = ["APT", "UNIT", "STE", "#"]

# (city, [zips]) with plausible Central-Texas ZIPs.
CITIES = [
    ("Austin", ["78701", "78702", "78703", "78704", "78705", "78717", "78721",
                "78722", "78723", "78727", "78731", "78741", "78745", "78748",
                "78749", "78751", "78753", "78756", "78758", "78759"]),
    ("Round Rock", ["78664", "78665", "78681"]),
    ("Pflugerville", ["78660"]),
    ("Cedar Park", ["78613"]),
    ("Leander", ["78641", "78645"]),
    ("Del Valle", ["78617"]),
    ("Manor", ["78653"]),
    ("Buda", ["78610"]),
]

PARTIES = ["Dem", "Rep", "Lib", "Grn", ""]

def voted():
    return "Y" if random.random() < 0.55 else ""

def party_for(v):
    return random.choice(["Dem", "Rep", "Rep", "Dem", "Lib", ""]) if v == "Y" else ""

header = ["VUIDNO", "PCTCOD", "PCTSPT", "EDRDAT", "GENDER", "LSTNAM", "FSTNAM",
          "MIDNAM", "BLKNUM", "STRPRE", "STRNAM", "STRTYP", "UNITYP", "UNITNO",
          "RSCITY", "RZIPCD", "SUSIND", "G20VOTED", "P22VOTED", "G22VOTED",
          "P24VOTED", "P24PARTY", "G24VOTED", "P26VOTED", "P26PARTY"]

used_ids = set()
rows = []
for _ in range(N):
    while True:
        vuid = random.randint(1000000000, 1099999999)
        if vuid not in used_ids:
            used_ids.add(vuid)
            break
    pct = random.randint(101, 460)
    spt = random.choice(["", "A", "B", "C", "D"])
    edr = "{:04d}{:02d}{:02d}".format(random.randint(2008, 2025),
                                      random.randint(1, 12), random.randint(1, 28))
    gender = random.choice(["M", "F"])
    if gender == "M":
        first = random.choice(FIRST_M)
    else:
        first = random.choice(FIRST_F)
    last = random.choice(SURNAMES)
    mid = random.choice(MIDDLE)

    blk = random.choice([random.randint(100, 9999), random.randint(1, 99) * 100])
    pre = random.choice(STR_PRE)
    stn = random.choice(STREETS)
    stt = random.choice(STR_TYPES)
    if random.random() < 0.28:
        ut = random.choice(UNIT_TYPES)
        un = random.choice([str(random.randint(1, 480)),
                            str(random.randint(1, 40)) + random.choice("AB"),
                            random.choice("ABCDE") + str(random.randint(1, 12))])
    else:
        ut, un = "", ""
    city, zips = random.choice(CITIES)
    zc = random.choice(zips)
    sus = "S" if random.random() < 0.04 else ""

    g20 = voted(); p22 = voted(); g22 = voted()
    p24 = voted(); g24 = voted(); p26 = voted()
    rows.append([vuid, pct, spt, edr, gender, last, first, mid, blk, pre, stn, stt,
                 ut, un, city, zc, sus, g20, p22, g22, p24, party_for(p24), g24,
                 p26, party_for(p26)])

rows.sort(key=lambda r: (r[1], r[5], r[6]))  # by precinct, last, first

with open(out_path, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(header)
    w.writerows(rows)

print("wrote {} rows to {}".format(len(rows), out_path))
