#-----------------------------------------------------------------------------
# parse_hart_tapes.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Parses the OCR line lists written by ocr_hart_results_tapes.py into Hart
# Verity Scan close-polls reports: scanner serial, polling place, voting
# type, ballot counter, print time, per-party "Total Ballots Cast", and the
# Tally Summary Report By Contest (party -> contest -> choice counts with
# Undervotes / Overvotes).
#
# Usage: python -I parse_hart_tapes.py <ocr_dir> <out.json>
#-----------------------------------------------------------------------------
"""parse_hart_tapes.py"""
import glob
import json
import os
import re
import sys

PARTY_RE = re.compile(r'^(Republican|Democratic|Nonpartisan) Part', re.I)


def value(line):
    """Returns the best count for a line (first pass, else digits re-read)"""
    if line['num'] is not None:
        return line['num']
    return line.get('alt')


def serial(text):
    """Normalizes an OCR'd scanner serial such as 'S/N: $1902851109'"""
    digits = re.sub(r'\D', '', text.split(':', 1)[-1])
    return 'S' + digits if digits else None


def is_tally_start(text):
    """True for the line that starts the Tally Summary Report By Contest"""
    return ('Tally Summary' in text or 'Report By Contest' in text
            or re.match(r'^[|\]\s]*Prec\w*/Sp\w*\s*In', text, re.I) is not None)


def parse_report(lines, start, end):
    """Parses one close-polls report spanning lines[start:end]"""
    rep = {'serial': None, 'location': [], 'voting_type': None,
           'ballot_counter': None, 'printed': None, 'totals': {},
           'contests': [], 'review': []}
    i = start
    # --- header up to the tally summary
    while i < end and not is_tally_start(lines[i]['text']):
        t = lines[i]['text']
        if re.match(r'^S.?N\b', t) or t.startswith(('S/N', 'SIN', 'S/IN')):
            rep['serial'] = serial(t)
        elif t.startswith('Ballot Counter') or t.startswith('Ballot Count'):
            rep['ballot_counter'] = value(lines[i])
        elif re.search(r'(Early Voting|Election Day Voting|Absentee)', t):
            rep['voting_type'] = t
        elif re.match(r'^\d\d/\d\d/\d{4}', t) and rep['ballot_counter'] is not None:
            rep['printed'] = t
        elif rep['voting_type'] is None and not t.startswith(
                ('Primary', 'Election', 'Tarrant', 'Verity', 'Version')):
            rep['location'].append(t)
        i += 1
    rep['location'] = ' '.join(rep['location'])
    # --- tally summary
    party = None
    header = []
    contest = None
    pending = []
    in_totals = False
    total_party = 'ALL'
    while i < end:
        line = lines[i]
        t = line['text'].strip().lstrip('|]._-~ ').strip()
        v = value(line)
        i += 1
        if t.startswith('Ballot Count by') or t.startswith('Ballot Count By'):
            in_totals = True
            total_party = 'ALL'
            continue
        m = PARTY_RE.match(t)
        if m:
            name = m.group(1).upper()[:3]
            if in_totals:
                total_party = name
            else:
                party = name
            continue
        if in_totals:
            if 'Total Ballots Cast' in t and v is not None:
                rep['totals'][total_party] = v
            continue
        if not t and v is None:
            continue
        is_uo = re.match(r'^(Under|Over)\s*vote', t, re.I) is not None
        if v is None and not is_uo:
            if contest is not None and not contest['closed']:
                pending.append(t)           # wrapped choice name
            else:
                header.append(t)
            continue
        if re.match(r'^Prec\w*/Sp\w*', t, re.I):
            continue
        if contest is None or contest['closed']:
            contest = {'party': party, 'header': ' '.join(header), 'choices': [],
                       'closed': False}
            rep['contests'].append(contest)
            header = []
        prev = lines[i - 2]
        # second line of a wrapped name whose centred count was already read
        # on the first line: the digits re-read just sees that count again
        if line['num'] is None and not line.get('wrapped') and contest['choices'] and not is_uo \
                and prev['num'] is not None and not pending \
                and line['y'] - prev.get('y1', line['y']) < 20 \
                and not t.lower().startswith(('undervote', 'overvote')):
            contest['choices'][-1][0] += ' ' + t
            continue
        label = ' '.join(pending + [t]).strip()
        pending = []
        if line['num'] is not None and line.get('alt') is not None \
                and line['alt'] != line['num']:
            rep['review'].append([label, line['num'], line['alt'], line['y']])
        if v is None:
            rep['review'].append([label, None, None, line['y']])
        contest['choices'].append([label, v, line['y'], line.get('y1', line['y'] + 30),
                                   (lines[i - 2]['y'] if line.get('wrapped') else line['y'])])
        if re.match(r'^over\s*vote', label, re.I):
            contest['closed'] = True
    for c in rep['contests']:
        del c['closed']
    return rep


def parse_file(path):
    """Splits an OCR'd tape file into its close-polls reports"""
    data = json.load(open(path, encoding='utf-8'))
    reports = []
    for frame in data['frames']:
        lines = frame['lines']
        heads = [k for k, l in enumerate(lines) if 'Election Header' in l['text']]
        # 'Tally Summary' banner, else (OCR missed it) the splits line below it
        tallies = []
        for k, l in enumerate(lines):
            if is_tally_start(l['text']) and not (tallies and k - tallies[-1] < 5):
                tallies.append(k)
        for n, tally in enumerate(tallies):
            start = max([h for h in heads if h < tally], default=0)
            end = tallies[n + 1] if n + 1 < len(tallies) else len(lines)
            end = min([h for h in heads if h > tally] + [end])
            rep = parse_report(lines, start, end)
            rep['zip'] = data['zip']
            rep['file'] = data['member']
            reports.append(rep)
    if not reports:
        reports.append({'zip': data['zip'], 'file': data['member'], 'serial': None,
                        'error': 'no Tally Summary found', 'contests': []})
    return reports


def main():
    """Main entry point"""
    out = []
    for path in sorted(glob.glob(os.path.join(sys.argv[1], '*.json'))):
        out.extend(parse_file(path))
    with open(sys.argv[2], 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=0)
    print(len(out), 'reports')


if __name__ == '__main__':
    main()
