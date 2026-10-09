#-----------------------------------------------------------------------------
# extract_hart_cvr_pdf.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Extracts Hart Verity "CVR Report" PDFs (one record per page, a record may
# span several pages that repeat its Cvr Id) into a JSON-lines file with the
# header fields (precinct, party, polling place, voting type, device) and
# the contest/option selections of each cast vote record.
#
# Usage: python -I extract_hart_cvr_pdf.py <cvr_report.pdf> <out.jsonl>
#-----------------------------------------------------------------------------
"""extract_hart_cvr_pdf.py"""
import concurrent.futures
import json
import sys

import fitz

HEADER_KEYS = ('Precinct', 'Party', 'Polling Place', 'Voting Type',
               'Device Type', 'Device Serial', 'Device Data Id',
               'Cvr Id', 'Central Batch Id')


def parse_page(page):
    """Returns (header dict, [(contest, option), ...]) for one page"""
    header = {}
    rows = {}
    for block in page.get_text('dict')['blocks']:
        for line in block.get('lines', []):
            text = ''.join(s['text'] for s in line['spans']).strip()
            x0, y0 = line['bbox'][0], line['bbox'][1]
            key, sep, value = text.partition(':')
            if sep and key in HEADER_KEYS:
                header[key] = value.strip()
                continue
            # the header block shrinks when a page omits Precinct / Device
            # Type, so contest rows start below the 'Contest Title' caption
            if text in ('Contest Title', 'Option') or y0 < 215:
                continue
            slot = rows.setdefault(round(y0), ['', ''])
            if x0 < 200:
                slot[0] = (slot[0] + ' ' + text).strip()
            else:
                slot[1] = (slot[1] + ' ' + text).strip()
    selections = [tuple(rows[y]) for y in sorted(rows)]
    return header, selections


def work(args):
    """Parses a page range of the PDF"""
    path, start, stop = args
    doc = fitz.open(path)
    out = []
    for pno in range(start, stop):
        out.append(parse_page(doc[pno]))
    doc.close()
    return out


def main():
    """Main entry point"""
    path, out_path = sys.argv[1], sys.argv[2]
    pages = fitz.open(path).page_count
    step = 2000
    chunks = [(path, s, min(s + step, pages)) for s in range(0, pages, step)]
    records = {}
    order = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=14) as pool:
        for n, result in enumerate(pool.map(work, chunks)):
            for header, selections in result:
                cid = header.get('Cvr Id', '').lower()
                rec = records.get(cid)
                if rec is None:
                    rec = dict(header)
                    rec['Cvr Id'] = cid
                    rec['selections'] = []
                    rec['pages'] = 0
                    records[cid] = rec
                    order.append(cid)
                rec['pages'] += 1
                rec['selections'].extend(selections)
            print(f'{path}: {min((n + 1) * step, pages)}/{pages}', file=sys.stderr, flush=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        for cid in order:
            f.write(json.dumps(records[cid], ensure_ascii=False) + '\n')
    print(f'{path}: {pages} pages, {len(order)} records')


if __name__ == '__main__':
    main()
