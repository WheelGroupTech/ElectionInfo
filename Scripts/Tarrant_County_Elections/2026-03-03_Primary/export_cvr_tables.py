#-----------------------------------------------------------------------------
# export_cvr_tables.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Merges the Hart CVR export (zip_*.jsonl from extract_hart_cvr_zip.py) with
# the CVR Report PDF fields (pdf_*.jsonl from extract_hart_cvr_pdf.py) and
# writes reusable tables, one record per ballot sheet:
#
#   cvr_sheet_index.csv        every sheet: id, party, method, sheet, precinct,
#                              polling place, scanner serial, batch, blank flag
#   <party>_<method>_records.jsonl.gz
#                              the same plus every contest with its marked
#                              options, undervotes and overvote flag
#   <party>_cvr_wide.csv       key columns + one column per contest (the
#                              selection, "undervote", "overvote" or blank
#                              when the contest is not on that sheet);
#                              loads in ElectionExplorer's CVR window
#
# Usage: python -I export_cvr_tables.py <work_dir> <out_dir>
#-----------------------------------------------------------------------------
"""export_cvr_tables.py"""
import csv
import gzip
import json
import os
import sys

PARTIES = {'rep': 'Republican', 'dem': 'Democratic'}
METHODS = {'abs': 'Mail', 'ev': 'Early Voting', 'ed': 'Election Day'}
KEYS = ['Cvr Id', 'Party', 'Method', 'Sheet', 'Precinct Split', 'Polling Place',
        'Device Serial', 'Device Type', 'Batch', 'Batch Sequence', 'Blank']


def main():
    """Main entry point"""
    work, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    official = json.load(open(os.path.join(work, 'clarity.json'), encoding='utf-8'))
    order = {p: [c['name'] for c in official['contests'] if c['party'] == p.upper()]
             for p in PARTIES}
    with open(os.path.join(out, 'cvr_sheet_index.csv'), 'w', newline='',
              encoding='utf-8-sig') as fidx:
        idx = csv.writer(fidx)
        idx.writerow(KEYS)
        for p, pname in PARTIES.items():
            contests = order[p]
            with open(os.path.join(out, f'{p}_cvr_wide.csv'), 'w', newline='',
                      encoding='utf-8-sig') as fwide:
                wide = csv.writer(fwide)
                wide.writerow(KEYS + contests)
                for m, mname in METHODS.items():
                    pdf = {}
                    with open(os.path.join(work, f'pdf_{p}_{m}.jsonl'), encoding='utf-8') as f:
                        for line in f:
                            r = json.loads(line)
                            pdf[r['Cvr Id']] = r
                    path = os.path.join(out, f'{p}_{m}_records.jsonl.gz')
                    n = 0
                    with gzip.open(path, 'wt', encoding='utf-8') as fgz, \
                            open(os.path.join(work, f'zip_{p}_{m}.jsonl'), encoding='utf-8') as f:
                        for line in f:
                            z = json.loads(line)
                            d = pdf.get(z['guid'], {})
                            key = [z['guid'], pname, mname, z['sheet'], z['split'],
                                   d.get('Polling Place', ''), d.get('Device Serial', ''),
                                   d.get('Device Type', ''), z.get('batch') or '',
                                   z.get('batch_seq') or '', z['blank']]
                            idx.writerow(key)
                            rec = dict(zip(KEYS, key))
                            rec['Contests'] = [{'Contest': c, 'Selections': [o[0] for o in opts],
                                                'Write-in': any(o[2] for o in opts),
                                                'Undervotes': int(u) if u else 0,
                                                'Overvoted': bool(ov)}
                                               for c, opts, u, ov in z['contests']]
                            fgz.write(json.dumps(rec, ensure_ascii=False) + '\n')
                            cells = {}
                            for c, opts, u, ov in z['contests']:
                                if ov:
                                    cells[c] = 'overvote'
                                elif opts:
                                    cells[c] = '; '.join(o[0] for o in opts)
                                else:
                                    cells[c] = 'undervote'
                            wide.writerow(key + [cells.get(c, '') for c in contests])
                            n += 1
                    print(f'{p} {m}: {n} sheets', flush=True)


if __name__ == '__main__':
    main()
