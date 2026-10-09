#-----------------------------------------------------------------------------
# parse_clarity_detail.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Parses a Clarity Elections detail.xml into a JSON file:
#   {"contests": [{"key","name","party","voteFor",
#                  "choices": {choice: {votetype: {precinct: votes}}},
#                  "under": {precinct: n}, "over": {precinct: n}}],
#    "precincts": {precinct: {"registered", "ballots"}}}
#
# Usage: python -I parse_clarity_detail.py <detail.xml> <out.json>
#-----------------------------------------------------------------------------
"""parse_clarity_detail.py"""
import collections
import json
import sys
import xml.etree.ElementTree as ET


def precinct_votes(elem):
    """Returns {precinct: votes} for the Precinct children of elem"""
    return {p.get('name'): int(p.get('votes')) for p in elem.findall('Precinct')}


def main():
    """Main entry point"""
    root = ET.parse(sys.argv[1]).getroot()
    out = {'contests': [], 'precincts': {}}
    for p in root.find('VoterTurnout').find('Precincts').findall('Precinct'):
        out['precincts'][p.get('name')] = {
            'registered': int(p.get('totalVoters')),
            'ballots': int(p.get('ballotsCast'))}
    for c in root.findall('Contest'):
        rec = {'key': c.get('key'), 'name': c.get('text'),
               'voteFor': int(c.get('voteFor')), 'choices': {},
               'under': {}, 'over': {}}
        parties = collections.Counter()
        for vt in c.findall('VoteType'):
            if vt.get('name') == 'Undervotes':
                rec['under'] = precinct_votes(vt)
            elif vt.get('name') == 'Overvotes':
                rec['over'] = precinct_votes(vt)
        for ch in c.findall('Choice'):
            parties[ch.get('party')] += 1
            rec['choices'][ch.get('text')] = {
                vt.get('name'): precinct_votes(vt) for vt in ch.findall('VoteType')}
        rec['party'] = parties.most_common(1)[0][0] if parties else None
        out['contests'].append(rec)
    with open(sys.argv[2], 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)
    print(len(out['contests']), 'contests', len(out['precincts']), 'precincts')


if __name__ == '__main__':
    main()
