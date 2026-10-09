#-----------------------------------------------------------------------------
# compare_cvr_to_official.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Tabulates the Hart CVR exports (JSON lines from extract_hart_cvr_zip.py)
# and compares every contest/choice/vote type/precinct, plus undervotes and
# overvotes, against the official Clarity detail (parse_clarity_detail.py).
#
# Usage: python -I compare_cvr_to_official.py <work_dir>
#   <work_dir> holds zip_{rep,dem}_{abs,ev,ed}.jsonl and clarity.json; writes
#   cvr_vs_official_contests.csv and cvr_vs_official_precincts.csv
#-----------------------------------------------------------------------------
"""compare_cvr_to_official.py"""
import collections
import csv
import json
import os
import sys

VOTE_TYPES = {'abs': 'Absentee', 'ev': 'Early Voting', 'ed': 'Election'}
PARTIES = {'rep': 'REP', 'dem': 'DEM'}


def norm(name):
    """Normalizes a candidate/contest name for matching"""
    return ' '.join(name.replace('’', "'").replace('‘', "'")
                    .replace('“', '"').replace('”', '"').split()).lower()


def precinct_of(split):
    """Maps a CVR precinct split name to the Clarity precinct name"""
    if split.endswith('-F99'):
        return split[:-4] + ' - F99'
    return split


def main():
    """Main entry point"""
    work = sys.argv[1]
    official = json.load(open(os.path.join(work, 'clarity.json'), encoding='utf-8'))
    # cvr[(party, contest)][choice][votetype][precinct] = votes; choice may
    # be the pseudo choices __under__ / __over__
    cvr = collections.defaultdict(lambda: collections.defaultdict(
        lambda: collections.defaultdict(collections.Counter)))
    for p in PARTIES:
        for v in VOTE_TYPES:
            with open(os.path.join(work, f'zip_{p}_{v}.jsonl'), encoding='utf-8') as f:
                for line in f:
                    r = json.loads(line)
                    prec = precinct_of(r['split'])
                    for cname, opts, under, over in r['contests']:
                        key = (PARTIES[p], norm(cname))
                        if over:
                            cvr[key]['__over__'][VOTE_TYPES[v]][prec] += 1
                            continue
                        for oname, value, _wi in opts:
                            cvr[key][norm(oname)][VOTE_TYPES[v]][prec] += int(value or 1)
                        if under:
                            cvr[key]['__under__'][VOTE_TYPES[v]][prec] += int(under)

    contest_rows, precinct_rows = [], []
    seen = set()
    for c in official['contests']:
        key = (c['party'], norm(c['name']))
        seen.add(key)
        cc = cvr.get(key, {})
        choices = [(ch, norm(ch), vts) for ch, vts in c['choices'].items()]
        for label, nkey, vts in choices:
            for vt in VOTE_TYPES.values():
                off = vts.get(vt, {})
                got = cc.get(nkey, {}).get(vt, collections.Counter())
                o_tot, c_tot = sum(off.values()), sum(got.values())
                contest_rows.append([c['party'], c['name'], label, vt, o_tot, c_tot, c_tot - o_tot])
                for prec in set(off) | set(got):
                    if off.get(prec, 0) != got.get(prec, 0):
                        precinct_rows.append([c['party'], c['name'], label, vt, prec,
                                              off.get(prec, 0), got.get(prec, 0)])
        for pseudo, field in (('__under__', 'under'), ('__over__', 'over')):
            off = c[field]
            got = collections.Counter()
            for vt_counts in cc.get(pseudo, {}).values():
                got.update(vt_counts)
            o_tot, c_tot = sum(off.values()), sum(got.values())
            contest_rows.append([c['party'], c['name'], pseudo.strip('_'), 'All', o_tot, c_tot, c_tot - o_tot])
            for prec in set(off) | set(got):
                if off.get(prec, 0) != got.get(prec, 0):
                    precinct_rows.append([c['party'], c['name'], pseudo.strip('_'), 'All', prec,
                                          off.get(prec, 0), got.get(prec, 0)])
        extra = set(cc) - {n for _l, n, _v in choices} - {'__under__', '__over__'}
        for nkey in extra:
            for vt, counts in cc[nkey].items():
                contest_rows.append([c['party'], c['name'], nkey, vt, 0, sum(counts.values()), sum(counts.values())])
    for key in set(cvr) - seen:
        contest_rows.append([key[0], key[1], '(contest not in official results)', '', 0, 0, 0])

    with open(os.path.join(work, 'cvr_vs_official_contests.csv'), 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f)
        w.writerow(['Party', 'Contest', 'Choice', 'Vote Type', 'Official', 'CVR', 'Difference'])
        w.writerows(contest_rows)
    with open(os.path.join(work, 'cvr_vs_official_precincts.csv'), 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f)
        w.writerow(['Party', 'Contest', 'Choice', 'Vote Type', 'Precinct', 'Official', 'CVR'])
        w.writerows(precinct_rows)
    diffs = [r for r in contest_rows if r[6] != 0]
    print(f'{len(contest_rows)} contest/choice/vote-type rows compared, {len(diffs)} differ')
    for r in diffs[:60]:
        print(r)
    print(f'{len(precinct_rows)} precinct-level differences')
    for r in precinct_rows[:30]:
        print(r)


if __name__ == '__main__':
    main()
