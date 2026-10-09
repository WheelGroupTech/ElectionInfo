#-----------------------------------------------------------------------------
# analyze_roster_redactions.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Tests whether Tarrant County's roster redactions track ballot secrecy:
# under countywide polling, a voter whose precinct ballot style is the only
# one (or one of two) cast at a polling place -- or, for mail, in the
# precinct -- can have their vote read from the public CVRs.  For each
# precinct and voting method this compares the number of such "small-cell"
# CVR ballots with the number of redacted entries in the March roster
# reports, and checks whether the later full ("Redacted") roster names the
# voters the March reports redacted.
#
# Usage: python -I analyze_roster_redactions.py <work_dir> <roster_dir>
#   writes redaction_by_precinct.csv and prints a summary
#-----------------------------------------------------------------------------
"""analyze_roster_redactions.py"""
import collections
import csv
import json
import os
import sys

REPORTS = {
    ('rep', 'ev'): 'Early_voting_in_person_report_Rep.txt',
    ('dem', 'ev'): 'Early_voting_in_person_report_Dem.txt',
    ('rep', 'ed'): 'Election_day_in_person_report_Redacted_Rep.txt',
    ('dem', 'ed'): 'Election_day_in_person_report_Redacted_Dem.txt',
    ('rep', 'abs'): 'absentee_returned_voter_report_Rep.txt',
    ('dem', 'abs'): 'absentee_returned_voter_report_Dem.txt',
}
FULL = {'rep': 'REP_Election Roster_Redacted.txt', 'dem': 'DEM_Election Roster_Redacted.txt'}
FULL_TYPES = {'ev': ('E',), 'ed': ('Y',), 'abs': ('A', 'F')}
SMALL = 2


def load(path):
    """Loads a tab-delimited roster"""
    with open(path, encoding='latin-1', newline='') as f:
        return list(csv.DictReader(f, delimiter='\t'))


def redacted(row):
    """True when the roster row's name is redacted"""
    return 'edact' in (row.get('Name') or '') + (row.get('Voter_Name') or '')


def main():
    """Main entry point"""
    work, roster_dir = sys.argv[1], sys.argv[2]
    out = []
    print('party method  CVR-ballots-in-cells<=2  March-redacted  precincts: '
          'cells<=2 / with-0-redactions / where-full-roster-names-voters')
    for (p, v), report in REPORTS.items():
        place = {}
        if v != 'abs':
            with open(os.path.join(work, f'pdf_{p}_{v}.jsonl'), encoding='utf-8') as f:
                for line in f:
                    r = json.loads(line)
                    place[r['Cvr Id']] = r.get('Polling Place', '')
        cells = collections.Counter()
        with open(os.path.join(work, f'zip_{p}_{v}.jsonl'), encoding='utf-8') as f:
            for line in f:
                r = json.loads(line)
                if r['sheet'] != '1':
                    continue
                where = place.get(r['guid'], 'MAIL')
                if 'PROV' in where.upper():
                    continue          # provisional voters are not in the rosters
                cells[(r['split'].split('-')[0], where)] += 1
        small = collections.Counter()
        for (prec, _where), n in cells.items():
            if n <= SMALL:
                small[prec] += n
        rows = load(os.path.join(roster_dir, report))
        red = collections.Counter(r['Precinct'][:4] for r in rows if redacted(r))
        named = collections.Counter(r['Precinct'][:4] for r in rows if not redacted(r))
        full = load(os.path.join(roster_dir, FULL[p]))
        full_named = collections.Counter(r['Precinct'] for r in full
                                         if r['Vote_Type'] in FULL_TYPES[v] and not redacted(r))
        precs = set(small) | set(red)
        no_red = exposed_full = 0
        for prec in sorted(precs):
            s, rd = small.get(prec, 0), red.get(prec, 0)
            if s and not rd:
                no_red += 1
            if s and full_named.get(prec, 0) and rd:
                exposed_full += 1
            out.append([p.upper(), v.upper(), prec, s, rd, named.get(prec, 0),
                        full_named.get(prec, 0)])
        print(f'{p.upper():5} {v.upper():4}  {sum(small.values()):8}  {sum(red.values()):8}   '
              f'{len(small):5} / {no_red:5} / {exposed_full:5}')
    with open(os.path.join(work, 'redaction_by_precinct.csv'), 'w', newline='',
              encoding='utf-8-sig') as f:
        w = csv.writer(f)
        w.writerow(['Party', 'Method', 'Precinct', f'CVR ballots in cells <= {SMALL}',
                    'March report redacted', 'March report named',
                    'Full roster named (same method)'])
        w.writerows(out)


if __name__ == '__main__':
    main()
