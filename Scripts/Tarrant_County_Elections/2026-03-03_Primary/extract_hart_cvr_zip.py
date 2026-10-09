#-----------------------------------------------------------------------------
# extract_hart_cvr_zip.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Extracts a Hart Verity CVR export ZIP (one XML file per ballot sheet) into
# a JSON-lines file: CvrGuid, sheet number, batch sequence, precinct split,
# party, IsBlank, and every contest with its marked options.
#
# Usage: python -I extract_hart_cvr_zip.py <CVRExport.zip> <out.jsonl>
#-----------------------------------------------------------------------------
"""extract_hart_cvr_zip.py"""
import concurrent.futures
import json
import re
import sys
import zipfile

RE_CONTEST = re.compile(r'<Contest>(.*?)</Contest>', re.S)
RE_OPTION = re.compile(r'<Option>(.*?)</Option>', re.S)


def tag(text, name):
    """Returns the first <name>value</name> in text (non-nested)"""
    m = re.search(r'<%s>(.*?)</%s>' % (name, name), text, re.S)
    return m.group(1) if m else None


def parse_xml(member, text):
    """Parses one Hart CVR XML document"""
    contests_end = text.find('</Contests>')
    body = text[:contests_end] if contests_end >= 0 else ''
    tail = text[contests_end:] if contests_end >= 0 else text
    contests = []
    for cm in RE_CONTEST.finditer(body):
        ctext = cm.group(1)
        cname = tag(ctext.split('<Options')[0], 'Name')
        opts = []
        for om in RE_OPTION.finditer(ctext):
            otext = om.group(1)
            opts.append([tag(otext, 'Name'), tag(otext, 'Value'),
                         'WriteInData' in otext])
        undervotes = tag(ctext, 'Undervotes')
        overvotes = '<Overvoted' in ctext
        contests.append([cname, opts, undervotes, overvotes])
    split = re.search(r'<PrecinctSplit><Name>(.*?)</Name>', tail)
    party = re.search(r'<Party><Name>(.*?)</Name>', tail)
    return {
        'member': member,
        'guid': (tag(tail, 'CvrGuid') or '').lower(),
        'sheet': tag(tail, 'SheetNumber'),
        'batch_seq': tag(tail, 'BatchSequence'),
        'batch': tag(tail, 'BatchNumber'),
        'split': split.group(1) if split else None,
        'party': party.group(1) if party else None,
        'blank': tag(tail, 'IsBlank'),
        'contests': contests,
    }


def work(args):
    """Parses a list of ZIP members"""
    path, names = args
    out = []
    with zipfile.ZipFile(path) as z:
        for name in names:
            text = z.read(name).decode('utf-8-sig')
            out.append(parse_xml(name, text))
    return out


def main():
    """Main entry point"""
    path, out_path = sys.argv[1], sys.argv[2]
    with zipfile.ZipFile(path) as z:
        names = [n for n in z.namelist() if n.lower().endswith('.xml')]
    step = 5000
    chunks = [(path, names[s:s + step]) for s in range(0, len(names), step)]
    count = 0
    with open(out_path, 'w', encoding='utf-8') as f, \
         concurrent.futures.ProcessPoolExecutor(max_workers=14) as pool:
        for result in pool.map(work, chunks):
            for rec in result:
                f.write(json.dumps(rec, ensure_ascii=False) + '\n')
                count += 1
    print(f'{path}: {len(names)} members, {count} records')


if __name__ == '__main__':
    main()
