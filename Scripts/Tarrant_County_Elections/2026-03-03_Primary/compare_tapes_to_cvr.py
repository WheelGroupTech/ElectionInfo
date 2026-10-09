#-----------------------------------------------------------------------------
# compare_tapes_to_cvr.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Compares every parsed Hart Verity Scan results tape (parse_hart_tapes.py)
# with the cast vote records produced by the same scanner.  CVR records are
# joined to their scanner serial / polling place through the CVR Report PDF
# (extract_hart_cvr_pdf.py); contest selections come from the XML export
# (extract_hart_cvr_zip.py), which matches the official canvass exactly.
#
# For each tape the CVR-expected (label, count) sequence -- contests in
# official order, choices in official order, then Undervotes and Overvotes --
# is aligned to the OCR'd tape lines and each aligned count compared.
#
# Usage: python -I compare_tapes_to_cvr.py <work_dir>
#   writes tapes_vs_cvr_summary.csv, tapes_vs_cvr_mismatches.csv and
#   scanners_without_tapes.csv
#-----------------------------------------------------------------------------
"""compare_tapes_to_cvr.py"""
import collections
import csv
import difflib
import json
import os
import re
import sys

FILES = [(p, v) for p in ('rep', 'dem') for v in ('ev', 'ed', 'abs')]


def norm(text):
    """Normalizes a label for matching"""
    text = text.replace('’', "'").replace('�', "'")
    return re.sub(r'[^a-z0-9]', '', text.lower())


def vt_code(text):
    """'EV' or 'ED' from a Hart voting type or a tape ZIP name"""
    return 'EV' if 'Early' in (text or '') or 'EV' in (text or '') else 'ED'


def load_cvr(work):
    """Returns tallies and ballot counts per (serial, EV/ED) and per site.

    A scanner can carry both early-voting ballots and Election Day rescans,
    so everything is keyed by serial AND voting method; site totals cover
    ballots rescanned on other units (the original tape then shows the site)."""
    def counter3():
        return collections.defaultdict(collections.Counter)
    tally = collections.defaultdict(counter3)        # (ser, vt) -> (party, contest) -> choice
    site_tally = collections.defaultdict(counter3)   # (place, vt) -> (party, contest) -> choice
    ballots = collections.defaultdict(collections.Counter)       # (ser, vt) -> party -> sheet-1
    site_ballots = collections.defaultdict(collections.Counter)  # (place, vt) -> party -> sheet-1
    sheets = collections.Counter()                                # (ser, vt) -> all sheets
    where = collections.defaultdict(collections.Counter)         # serial -> (place, vtype)
    for p, v in FILES:
        info = {}
        with open(os.path.join(work, f'pdf_{p}_{v}.jsonl'), encoding='utf-8') as f:
            for line in f:
                r = json.loads(line)
                info[r['Cvr Id']] = (r.get('Device Serial') or 'CENTRAL',
                                     r.get('Polling Place') or '', r.get('Voting Type'))
        with open(os.path.join(work, f'zip_{p}_{v}.jsonl'), encoding='utf-8') as f:
            for line in f:
                r = json.loads(line)
                ser, place, vtype = info[r['guid']]
                party = p.upper()
                key, skey = (ser, vt_code(vtype)), (place, vt_code(vtype))
                sheets[key] += 1
                where[ser][(place, vtype)] += 1
                if r['sheet'] == '1':
                    ballots[key][party] += 1
                    site_ballots[skey][party] += 1
                for cname, opts, under, over in r['contests']:
                    for t in (tally[key][(party, cname)], site_tally[skey][(party, cname)]):
                        if over:
                            t['Overvotes'] += 1
                            continue
                        for oname, val, _wi in opts:
                            t[oname] += int(val or 1)
                        if under:
                            t['Undervotes'] += int(under)
    return tally, site_tally, ballots, site_ballots, sheets, where


def expected_sequence(official, tally, parties):
    """CVR-expected (label, count, contest) items in official order"""
    seq = []
    for c in official['contests']:
        if c['party'] not in parties:
            continue
        t = tally.get((c['party'], c['name']), collections.Counter())
        for choice in c['choices']:
            seq.append((choice, t.get(choice, 0), c['party'], c['name']))
        seq.append(('Undervotes', t.get('Undervotes', 0), c['party'], c['name']))
        seq.append(('Overvotes', t.get('Overvotes', 0), c['party'], c['name']))
    return seq


def token(label, vocab):
    """Maps an OCR'd label to the closest expected label"""
    n = norm(label)
    if n.startswith('undervote'):
        return 'undervotes'
    if n.startswith('overvote'):
        return 'overvotes'
    if n in vocab:
        return n
    best = difflib.get_close_matches(n, vocab, n=1, cutoff=0.72)
    if best:
        return best[0]
    # a wrapped name may carry the contest title in front of it
    for cand in vocab:
        if len(cand) > 5 and n.endswith(cand):
            return cand
    return '?' + n


# Sites the CVR names only generically; identified by matching ballot counts
# against the Election Day cross-check report (both parties agree).
CVR_ALIASES = {'edlocation201': 'agapeunitedchristianfellowship',
               'edlocation202': 'alphainternationalseventhdayadventistchurch',
               'edlocation203': 'houseofprayerpraisechurch',
               'edlocation204': 'jamesstarrettelementaryschool'}


def place_key(text):
    """Normalizes a polling-place name (drops EV/REP/DEM prefixes)"""
    text = re.sub(r'^(EEC#\d+\s*)', '', text or '')
    text = re.sub(r'\.tif$', '', text, flags=re.I)
    text = re.sub(r'\s+2$', '', text)
    text = re.sub(r'^(EV\s*-\s*|EV\s+(REP|DEM)-\s*|REP\s+|DEM\s+)', '', text)
    key = re.sub(r'[^a-z0-9]', '', text.lower().replace('limted', 'limited'))
    return CVR_ALIASES.get(key, key)


def same_place(a, b):
    """True when two normalized place names refer to the same site"""
    if not a or not b:
        return False
    if a == b or (min(len(a), len(b)) >= 10 and (a.startswith(b) or b.startswith(a))):
        return True
    return difflib.SequenceMatcher(None, a, b).ratio() >= 0.85


def hamming(a, b):
    """Digit differences between two serials (99 when not comparable)"""
    if not a or not b or len(a) != len(b):
        return 99
    return sum(x != y for x, y in zip(a, b))


def tape_party(tp):
    """Party of an Election Day tape: its own header first, then the ZIP name"""
    m = re.search(r'\b(REP|DEM) ', tp.get('location', ''))
    if m:
        return m.group(1)
    return 'REP' if '-REP' in tp['zip'] else 'DEM' if '-DEM' in tp['zip'] else None


def match_tape(tp, where, ballots, sheets):
    """Picks the CVR scanner serial a tape belongs to.

    OCR can drop or misread serial digits, so candidates are the scanners
    used at the tape's polling place (from the tape header and the file
    name) for the tape's party; the OCR'd serial and the ballot counter
    then choose among them."""
    party = tape_party(tp)
    keys = {place_key(tp['file']), place_key(tp.get('location', ''))} - {''}
    cands = []
    for ser, places in where.items():
        if ser == 'CENTRAL':
            continue
        for (place, _vt) in places:
            if party and not place.startswith(party):
                continue
            if any(same_place(place_key(place), k) for k in keys):
                cands.append(ser)
                break
    counter = tp.get('ballot_counter')
    totals = tp.get('totals', {})

    vt = vt_code(tp.get('voting_type') or tp['zip'])

    def score(ser):
        key = (ser, vt)
        counts = {sheets.get(key), sum(ballots.get(key, {}).values())}
        hits = int(counter in counts) + sum(
            int(totals.get(p) == ballots.get(key, {}).get(p)) for p in ('REP', 'DEM')
            if totals.get(p) is not None)
        return (hamming(tp.get('serial'), ser), -hits)
    if cands:
        best = min(cands, key=score)
        if score(best)[0] <= 3 or score(best)[1] < 0 or len(cands) == 1:
            return best
    serial = tp.get('serial')
    if serial in where:
        return serial
    close = [k for k in where if hamming(k, serial) <= 1]
    return close[0] if len(close) == 1 else None


def main():
    """Main entry point"""
    work = sys.argv[1]
    official = json.load(open(os.path.join(work, 'clarity.json'), encoding='utf-8'))
    tapes = json.load(open(os.path.join(work, 'tapes_parsed.json'), encoding='utf-8'))
    tally, site_tally, ballots, site_ballots, sheets, where = load_cvr(work)

    summary, mismatches = [], []
    taped = set()
    for tp in tapes:
        ser = match_tape(tp, where, ballots, sheets)
        vt = vt_code(tp.get('voting_type') or tp['zip'])
        key = (ser, vt)
        place, basis = '', 'scanner'
        counter = tp.get('ballot_counter')
        totals = tp.get('totals', {})
        cvr_b = ballots.get(key, {})
        exp_tally = tally.get(key, {})
        if ser:
            taped.add(key)
            places = [(pl, n) for (pl, t), n in where[ser].most_common() if vt_code(t) == vt]
            place = '; '.join(f'{pl} x{n}' for pl, n in places)
            site = (places[0][0], vt) if places else None
            n_ser = sum(cvr_b.values())
            n_site = sum(site_ballots.get(site, {}).values()) if site else None
            if counter == 0 and not any(totals.get(p) for p in ('REP', 'DEM')):
                basis = 'empty tape'
            elif counter is not None and counter != n_ser and counter == n_site:
                # ballots rescanned on other units: the original tape shows the site
                basis = 'site (rescanned)'
                exp_tally, cvr_b = site_tally[site], site_ballots[site]
        parties = [p for p in ('REP', 'DEM')
                   if any(c['party'] == p for c in tp.get('contests', []))]
        items = [(ch[0], ch[1], ch[4], ch[3]) for c in tp.get('contests', []) for ch in c['choices']]
        exp = expected_sequence(official, exp_tally, parties) \
            if ser and basis != 'empty tape' else []
        vocab = {norm(e[0]) for e in exp}
        tok_t = [token(it[0], vocab) for it in items]
        tok_e = [norm(e[0]) for e in exp]
        sm = difflib.SequenceMatcher(None, tok_t, tok_e, autojunk=False)
        aligned = agree = 0
        for blk in sm.get_matching_blocks():
            for k in range(blk.size):
                (_lab, val, y0, y1), e = items[blk.a + k], exp[blk.b + k]
                aligned += 1
                if val == e[1]:
                    agree += 1
                else:
                    mismatches.append([tp['zip'], tp['file'], ser, e[2], e[3], e[0], val, e[1], y0, y1])
        summary.append([
            tp['zip'], tp['file'], tp.get('serial'), ser, vt, basis, tp.get('location', '')[:60],
            place, counter, sheets.get(key, 0),
            totals.get('REP'), cvr_b.get('REP', 0), totals.get('DEM'), cvr_b.get('DEM', 0),
            len(exp), aligned, agree, aligned - agree, tp.get('error', '')])

    no_tape = []
    for (ser, vt) in sorted(set(ballots) - taped):
        for (place, vtype), n in where[ser].most_common():
            if vt_code(vtype) == vt:
                no_tape.append([ser, place, vtype, n, ballots[(ser, vt)].get('REP', 0),
                                ballots[(ser, vt)].get('DEM', 0)])

    def write(name, header, rows):
        with open(os.path.join(work, name), 'w', newline='', encoding='utf-8-sig') as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)
    write('tapes_vs_cvr_summary.csv',
          ['Zip', 'Tape File', 'Tape S/N (OCR)', 'Matched CVR S/N', 'Method', 'Compared To',
           'Tape Location',
           'CVR Polling Place(s)', 'Tape Ballot Counter', 'CVR Sheets',
           'Tape REP Ballots', 'CVR REP Ballots', 'Tape DEM Ballots', 'CVR DEM Ballots',
           'Expected Lines', 'Aligned Lines', 'Agree', 'Disagree', 'Error'], summary)
    write('tapes_vs_cvr_mismatches.csv',
          ['Zip', 'Tape File', 'CVR S/N', 'Party', 'Contest', 'Choice', 'Tape (OCR)', 'CVR',
           'Y0', 'Y1'], mismatches)
    write('scanners_without_tapes.csv',
          ['CVR S/N', 'Polling Place', 'Voting Type', 'Sheets', 'REP Ballots', 'DEM Ballots'], no_tape)
    print(len(summary), 'tape reports;', sum(r[15] for r in summary), 'lines aligned;',
          sum(r[17] for r in summary), 'disagree;', len(no_tape), 'scanner/place rows without tape')


if __name__ == '__main__':
    main()
