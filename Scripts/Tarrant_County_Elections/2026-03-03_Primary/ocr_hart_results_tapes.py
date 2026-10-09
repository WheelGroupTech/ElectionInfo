#-----------------------------------------------------------------------------
# ocr_hart_results_tapes.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# OCRs Hart Verity Scan results tapes (TIFF images, read straight out of the
# ZIP files the county publishes) into JSON line lists for later parsing by
# analyze_p26_tarrant.py.  Each output line is
#   {"y": top, "text": left-hand text, "num": count or null, "conf": ...}
# A count that the full-text pass did not read as a confident number is
# re-read from its own crop with a digits-only whitelist.
#
# Usage: python -I ocr_hart_results_tapes.py <out_dir> <tapes.zip> [...]
#-----------------------------------------------------------------------------
"""ocr_hart_results_tapes.py"""
import concurrent.futures
import io
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile

import numpy as np
import pytesseract
from PIL import Image

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
Image.MAX_IMAGE_PIXELS = None

CHUNK = 5000        # target rows per OCR call (Tesseract limit is 32767)
NUM_X = 360         # counts are right-aligned to the right of this column


def cut_points(gray):
    """Returns row offsets to split the tape at blank rows near CHUNK"""
    h = gray.shape[0]
    blank = (gray[:, 60:gray.shape[1] - 10] < 128).sum(axis=1) == 0
    cuts = [0]
    while h - cuts[-1] > CHUNK + 1000:
        target = cuts[-1] + CHUNK
        best = None
        for d in range(0, 800):
            for y in (target - d, target + d):
                if cuts[-1] < y < h and blank[y]:
                    best = y
                    break
            if best:
                break
        cuts.append(best or target)
    cuts.append(h)
    return cuts


def read_digits(img, box):
    """Re-reads a count from its crop with a digit whitelist"""
    crop = img.crop(box)
    crop = Image.fromarray(np.pad(np.asarray(crop), 12, constant_values=255))
    txt = pytesseract.image_to_string(
        crop, config='--psm 7 -c tessedit_char_whitelist=0123456789').strip()
    return int(txt) if txt.isdigit() else None


def ocr_image(img):
    """OCRs one tape image, returns line records"""
    gray = np.asarray(img.convert('L'))
    img = Image.fromarray(gray)
    width = gray.shape[1]
    cuts = cut_points(gray)
    lines = []
    for top, bottom in zip(cuts, cuts[1:]):
        part = img.crop((0, top, width, bottom))
        data = pytesseract.image_to_data(part, config='--psm 6',
                                         output_type=pytesseract.Output.DICT)
        groups = {}
        for i, word in enumerate(data['text']):
            if not word.strip():
                continue
            key = (data['block_num'][i], data['par_num'][i], data['line_num'][i])
            groups.setdefault(key, []).append(
                (data['left'][i], data['top'][i] + top, data['width'][i],
                 data['height'][i], word, float(data['conf'][i])))
        for words in groups.values():
            words.sort()
            y0 = min(w[1] for w in words)
            y1 = max(w[1] + w[3] for w in words)
            last = words[-1]
            num, conf = None, None
            label_words = words
            if last[0] >= NUM_X and re.fullmatch(r'\d+', last[4]):
                num, conf = int(last[4]), last[5]
                label_words = words[:-1]
            elif last[0] >= NUM_X:
                label_words = words[:-1]
            right_of_label = max([w[0] + w[2] for w in label_words], default=0)
            x0 = max(NUM_X - 20, right_of_label + 4)
            # alt = digits-only re-read; differs from num → needs review
            alt = None
            if (num is None or conf < 90) and x0 < width - 10:
                region = gray[max(0, y0 - 4):y1 + 4, x0:width - 4]
                if region.size and (region < 128).sum() > 15:
                    alt = read_digits(img, (x0, max(0, y0 - 4), width - 4, y1 + 4))
            lines.append({'y': y0, 'y1': y1, 'x0': x0,
                          'text': ' '.join(w[4] for w in label_words),
                          'num': num, 'conf': conf, 'alt': alt})
    lines.sort(key=lambda r: r['y'])
    # A wrapped choice name prints its count centred between its two lines,
    # so re-read the count from the union of both lines' crops.
    for a, b in zip(lines, lines[1:]):
        if a['num'] is None and b['num'] is None and a['alt'] is not None \
                and b['y'] - a['y1'] < 20:
            x0 = max(a['x0'], b['x0'])
            b['alt'] = read_digits(img, (x0, max(0, a['y'] - 4), width - 4, b['y1'] + 4))
            b['wrapped'] = True
            a['alt'] = None
    return lines, gray.shape


def work(args):
    """OCRs one TIFF member of a ZIP"""
    zip_path, member, out_dir = args
    out_path = os.path.join(out_dir, re.sub(r'[^\w.#-]+', '_',
                            os.path.basename(zip_path) + '__' + member) + '.json')
    if os.path.exists(out_path):
        return out_path
    try:
        with zipfile.ZipFile(zip_path) as z:
            raw = z.read(member)
    except NotImplementedError:
        # Deflate64 members: let Info-ZIP unzip decompress them
        unzip = shutil.which('unzip') or r'C:\Program Files\Git\usr\bin\unzip.exe'
        raw = subprocess.run([unzip, '-p', zip_path, member], check=True,
                             capture_output=True).stdout
    record = {'zip': os.path.basename(zip_path), 'member': member,
              'bytes': len(raw), 'frames': []}
    try:
        img = Image.open(io.BytesIO(raw))
        for f in range(getattr(img, 'n_frames', 1)):
            img.seek(f)
            lines, shape = ocr_image(img.copy())
            record['frames'].append({'shape': shape, 'lines': lines})
    except Exception as exc:     # pylint: disable=broad-except
        record['error'] = f'{type(exc).__name__}: {exc}'
        record['head'] = raw[:16].hex()
    with open(out_path, 'w', encoding='utf-8') as fp:
        json.dump(record, fp, ensure_ascii=False)
    return out_path


def main():
    """Main entry point"""
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    jobs = []
    for zip_path in sys.argv[2:]:
        with zipfile.ZipFile(zip_path) as z:
            jobs += [(zip_path, m, out_dir) for m in z.namelist()
                     if m.lower().endswith(('.tif', '.tiff'))]
    done = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=14) as pool:
        for path in pool.map(work, jobs):
            done += 1
            print(f'{done}/{len(jobs)} {path}', flush=True)


if __name__ == '__main__':
    main()
