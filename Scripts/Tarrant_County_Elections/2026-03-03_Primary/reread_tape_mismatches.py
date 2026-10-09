#-----------------------------------------------------------------------------
# reread_tape_mismatches.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Re-reads every results-tape count that disagreed with the CVR in
# tapes_vs_cvr_mismatches.csv (compare_tapes_to_cvr.py).  Each count is
# cropped from the tape image, enlarged 3x and read with a digits-only
# Tesseract model under several thresholds and page modes.  A line whose
# re-reads include the CVR value is marked "resolved"; the rest are written
# out as labelled crop images for a person to read.
#
# Usage: python -I reread_tape_mismatches.py <work_dir> <results_tapes_dir>
#   writes tapes_vs_cvr_reread.csv and review/<n>.png
#-----------------------------------------------------------------------------
"""reread_tape_mismatches.py"""
import collections
import concurrent.futures
import csv
import io
import os
import shutil
import subprocess
import sys
import zipfile

import numpy as np
import pytesseract
from PIL import Image, ImageDraw, ImageOps

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
Image.MAX_IMAGE_PIXELS = None
NUM_X = 330


def read_member(zip_path, member):
    """Reads a ZIP member, falling back to unzip for Deflate64"""
    try:
        with zipfile.ZipFile(zip_path) as z:
            return z.read(member)
    except NotImplementedError:
        unzip = shutil.which('unzip') or r'C:\Program Files\Git\usr\bin\unzip.exe'
        return subprocess.run([unzip, '-p', zip_path, member], check=True,
                              capture_output=True).stdout


def readings(crop):
    """Digits-only readings of a count crop under several treatments"""
    out = set()
    big = crop.resize((crop.width * 3, crop.height * 3), Image.LANCZOS)
    for thr in (None, 110, 150, 190):
        img = big if thr is None else big.point(lambda p, t=thr: 255 if p > t else 0)
        img = ImageOps.expand(img, 30, fill=255)
        for psm in (7, 8, 13):
            txt = pytesseract.image_to_string(
                img, config=f'--psm {psm} -c tessedit_char_whitelist=0123456789').strip()
            if txt.isdigit():
                out.add(int(txt))
    return out


def work(args):
    """Re-reads all mismatched lines of one tape"""
    zip_path, member, rows, review_dir, cache = args
    if all((member, r['Y0']) in cache for r in rows):
        img = None
    else:
        img = load_image(zip_path, member)
    results = []
    for row in rows:
        y0, y1 = int(float(row['Y0'])), int(float(row['Y1']))
        if (member, row['Y0']) in cache:
            got = cache[(member, row['Y0'])]
        else:
            got = readings(img.crop((NUM_X, max(0, y0 - 10), img.width - 4,
                                     min(img.height, y1 + 12))))
        cvr = int(row['CVR'])
        status = 'resolved' if cvr in got else 'review'
        image = ''
        if status == 'review':
            if img is None:
                img = load_image(zip_path, member)
            wide = img.crop((0, max(0, y0 - 60), img.width, min(img.height, y1 + 60)))
            sheet = Image.new('L', (wide.width, wide.height + 40), 255)
            sheet.paste(wide, (0, 40))
            ImageDraw.Draw(sheet).text((4, 4), f"CVR={cvr} | {row['Choice'][:40]}", fill=0)
            image = os.path.join(review_dir, f"{abs(hash((member, y0))) % 10**10}.png")
            sheet.save(image)
        results.append(dict(row, Rereads=' '.join(map(str, sorted(got))), Status=status,
                            Image=image))
    return results


def load_image(zip_path, member):
    """Loads a tape image as 8-bit grayscale"""
    raw = read_member(zip_path, member)
    try:
        img = Image.open(io.BytesIO(raw)).convert('L')
    except Exception:     # pylint: disable=broad-except
        # TIFF written without its IFD: 8-bit pixels, 490 wide, after the header
        width = 490
        height = (len(raw) - 8) // width
        img = Image.fromarray(np.frombuffer(raw[8:8 + width * height], dtype=np.uint8)
                              .reshape(height, width))
    return img


def main():
    """Main entry point"""
    work_dir, tapes_dir = sys.argv[1], sys.argv[2]
    review_dir = os.path.join(work_dir, 'review')
    os.makedirs(review_dir, exist_ok=True)
    rows = list(csv.DictReader(open(os.path.join(work_dir, 'tapes_vs_cvr_mismatches.csv'),
                                    encoding='utf-8-sig')))
    groups = collections.defaultdict(list)
    for r in rows:
        if r['Y0'] not in ('', 'None'):
            groups[(r['Zip'], r['Tape File'])].append(r)
    zips = {os.path.basename(z): os.path.join(tapes_dir, z) for z in os.listdir(tapes_dir)}
    # re-use earlier readings of the same tape line (keyed by file and position)
    cache = {}
    cache_path = os.path.join(work_dir, 'tapes_vs_cvr_reread.csv')
    if os.path.exists(cache_path):
        for r in csv.DictReader(open(cache_path, encoding='utf-8-sig')):
            cache[(r['Tape File'], r['Y0'])] = {int(x) for x in r['Rereads'].split()}
    jobs = [(zips[z], m, g, review_dir,
             {k: v for k, v in cache.items() if k[0] == m}) for (z, m), g in groups.items()]
    out = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=14) as pool:
        for n, res in enumerate(pool.map(work, jobs), 1):
            out.extend(res)
            if n % 20 == 0:
                print(f'{n}/{len(jobs)} tapes', flush=True)
    with open(os.path.join(work_dir, 'tapes_vs_cvr_reread.csv'), 'w', newline='',
              encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)
    c = collections.Counter(r['Status'] for r in out)
    print(dict(c))


if __name__ == '__main__':
    main()
