#-----------------------------------------------------------------------------
# verify_results_tape.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to verify an ES&S DS200 results tape against the cast vote
# records (CVRs) of the ballots it reports.  For one polling location it:
#
#   1. OCRs the results tape TIFF (header fields and every vote line) in two
#      passes: a full-line pass and a digits-only re-read of each count.
#   2. Selects the CVRs the tape reports.  Candidate batches come from the
#      ES&S "Ballot Review" export and are chosen by ballot-style overlap with
#      the location's check-in roster, then split into (batch, scanner serial)
#      groups.  Groups are added only while they bring the selection's
#      ballot-style mix closer to the roster's, which finds ballots that were
#      rescanned at central count or scanned on a replacement unit.
#   3. Compares every vote line on the tape (candidates, zero lines, and
#      proposition Yes/No pairs in ballot order) with the CVR tally.  A count
#      is accepted only when one of its readings equals the CVR count.  Lines
#      the OCR readings do not confirm get a third reading by template
#      matching: the tape is printed in a fixed font, so digit templates are
#      harvested from lines whose OCR already agrees with the CVRs and used to
#      read the disputed counts.  Every line still unconfirmed is written to a
#      review sheet of image crops for a person to read, because machine
#      reading alone cannot certify a count.
#
# Example (Dallas County, 2024 Joint Primary, election day location V3100):
#
#   python verify_results_tape.py
#       --tape "...\5. Election Day\Results tapes\V3100_Voting Results Tape.tif"
#       --images "...\6. Canvass\Ballot Images\202403 Redacted Ballot Images"
#       --ballot-review "...\Cast Vote Records\20240305_Ballot Review"
#       --roster "...\Election-Day-Website-Roster.csv"
#       --election-date "March 05, 2024"
#
# Add --batch "<batch name>" (repeatable) to skip automatic selection once the
# batch is known, or --serial to restrict the scanners.  OCR and the Ballot
# Review index are cached in --cache, so re-runs are fast.
#
# Outputs (in --out): <site>_report.txt, <site>_lines.csv, and
# <site>_review_N.png sheets for lines that need a visual check.  The report
# also flags reprinted tapes (printed after election day) and lines the tape
# image repeats because of scan-stitch overlap.
#-----------------------------------------------------------------------------
"""verify_results_tape.py""" # for pylint
# pylint: disable=line-too-long,too-many-locals,too-many-branches,too-many-statements
# pylint: disable=too-many-arguments,broad-exception-caught

# Standard imports
import argparse
import collections
import concurrent.futures
import csv
import difflib
import glob
import json
import os
import re

# 3rd Party imports
import fitz
import numpy as np
import openpyxl
import pytesseract
from PIL import Image, ImageDraw, ImageOps

# We need to specify the location of the Tesseract-OCR binary for pytesseract
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# We will disable Pillow maximum image size since results tapes TIF files can be very large
Image.MAX_IMAGE_PIXELS = None

# Columns of the ES&S "Ballot Review" report
BALLOT_REVIEW_COLUMNS = ['CVR', 'Batch', 'OriginalException', 'BallotStatus', 'RemainingException',
                         'WriteInType', 'ResultsReport', 'BallotStyle', 'ReportingGroup', 'TabulatorCVR']

# Characters OCR commonly produces in place of digits on DS200 tapes
DIGIT_FIXES = str.maketrans({'O': '0', 'o': '0', 'S': '5', 'I': '1', 'l': '1', '|': '1',
                             'i': '1', 'g': '9', ')': '1'})

# Lines that are contest headers, report fields, or headings rather than vote lines
NON_VOTE_LINE = re.compile(r'[/?]|Number to|Vote For|Count|Total|Sheets|Serial|Opened|Closed|'
                           r'Date:|Time:|REPORT|Primary|Vote Center|Dallas County|March|bang|Precin|'
                           r'Election Judge|Signature|CERTIF', re.IGNORECASE)

# Minimum name similarity for matching a tape line to a CVR selection
MATCH_RATIO = 0.72

# Template matching: a glyph is read as a digit only when its correlation with
# that digit's template is at least TEMPLATE_MIN and beats the next-best digit
# by at least TEMPLATE_MARGIN.
TEMPLATE_MIN = 0.75
TEMPLATE_MARGIN = 0.08

# Fields read from the top of each CVR PDF
CVR_FIELDS = ['Cast Vote Record', 'Poll Place', 'Precinct', 'Ballot Style', 'Party',
              'Tabulator CVR', 'Machine Serial', 'Blank Ballot', 'Reporting Group']


#-----------------------------------------------------------------------------
# normalize_style()
#
# Normalizes a ballot style such as "REP 1325R" or "1325 R" to "1325R".
#-----------------------------------------------------------------------------
def normalize_style(style):
    """Normalizes a ballot style string"""

    style = re.sub(r'^(DEM|REP)\s*', '', str(style).strip(), flags=re.IGNORECASE)
    return re.sub(r'[^0-9A-Z]', '', style.upper())


#-----------------------------------------------------------------------------
# letters()
#-----------------------------------------------------------------------------
def letters(text):
    """Returns only the lowercase letters of a string (for fuzzy name matching)"""

    return re.sub(r'[^a-z]', '', text.lower())


#-----------------------------------------------------------------------------
# file_signature()
#
# Returns a string that changes when any of the files change, used to key the
# caches.
#-----------------------------------------------------------------------------
def file_signature(paths):
    """Returns a cache signature for a list of files"""

    return '|'.join(f"{os.path.basename(p)}:{os.path.getsize(p)}:{int(os.path.getmtime(p))}" for p in sorted(paths))


#-----------------------------------------------------------------------------
# load_ballot_review()
#
# Loads the ES&S Ballot Review export (one row per ballot) and caches it as
# JSON.  The workbooks declare their dimensions as "A1", so openpyxl's
# read-only mode returns a single row; they must be fully loaded.  Rows are
# de-duplicated by CVR number in case the export set overlaps.
#-----------------------------------------------------------------------------
def load_ballot_review(review_dir, review_glob, cache_dir):
    """Loads the Ballot Review export into a list of ballot dictionaries"""

    files = sorted(glob.glob(os.path.join(review_dir, review_glob)))
    if not files:
        raise FileNotFoundError(f"No Ballot Review files matching {review_glob} in {review_dir}")

    cache_path = os.path.join(cache_dir, 'ballot_review_index.json')
    signature = file_signature(files)
    if os.path.exists(cache_path):
        with open(cache_path, encoding='utf-8') as cache_file:
            cached = json.load(cache_file)
        if cached.get('signature') == signature:
            return cached['ballots']

    ballots = {}
    for pathname in files:
        print(f"  reading {os.path.basename(pathname)}")
        workbook = openpyxl.load_workbook(pathname, data_only=True)
        for row in workbook.worksheets[0].iter_rows(values_only=True):
            if not row or row[0] is None:
                continue
            first = row[0]
            if isinstance(first, (int, float)) and not isinstance(first, bool):
                cvr = int(first)
            elif str(first).replace(',', '').strip().isdigit():
                cvr = int(str(first).replace(',', '').strip())
            else:
                continue
            record = dict(zip(BALLOT_REVIEW_COLUMNS, row))
            if cvr not in ballots:
                ballots[cvr] = {'CVR': cvr, 'Batch': str(record['Batch']),
                                'Style': normalize_style(record['BallotStyle']),
                                'Group': str(record['ReportingGroup'])}
        workbook.close()

    result = list(ballots.values())
    os.makedirs(cache_dir, exist_ok=True)
    with open(cache_path, 'w', encoding='utf-8') as cache_file:
        json.dump({'signature': signature, 'ballots': result}, cache_file)
    return result


#-----------------------------------------------------------------------------
# load_roster_styles()
#
# Returns a Counter of ballot styles for the voters who checked in at the
# specified site.  The roster CSV may begin with a totals row, so the header
# row is located by its "SITE ID" and "BALLOTSTYLE" columns.
#-----------------------------------------------------------------------------
def load_roster_styles(roster_csv, site):
    """Loads the ballot styles of the voters who checked in at a site"""

    with open(roster_csv, encoding='utf-8-sig', errors='replace', newline='') as roster_file:
        rows = list(csv.reader(roster_file))

    header_index = next((i for i, row in enumerate(rows[:20]) if 'SITE ID' in row and 'BALLOTSTYLE' in row), None)
    if header_index is None:
        raise ValueError(f"{roster_csv} has no SITE ID / BALLOTSTYLE header row")

    columns = {name: i for i, name in enumerate(rows[header_index])}
    return collections.Counter(normalize_style(row[columns['BALLOTSTYLE']])
                               for row in rows[header_index + 1:]
                               if len(row) > columns['BALLOTSTYLE'] and row[columns['SITE ID']] == site)


#-----------------------------------------------------------------------------
# tape_segments()
#
# Splits a tall tape image into segments of about 3000 rows, cutting only on
# blank rows so that no printed line is split between segments.
#-----------------------------------------------------------------------------
def tape_segments(pixels):
    """Returns (start, end) row ranges for OCR segments"""

    blank = (pixels < 128).sum(axis=1) < 3
    height = pixels.shape[0]
    segments = []
    start = 0
    while start < height:
        end = min(start + 3000, height)
        if end < height:
            row = end
            while row > start + 1500 and not blank[row]:
                row -= 1
            end = row if row > start + 1500 else end
        segments.append((start, end))
        start = end
    return segments


#-----------------------------------------------------------------------------
# ocr_tape()
#
# OCRs a results tape into lines.  Each line records its text and vertical
# position; lines ending in a count in the right-hand column also record the
# count token and a digits-only re-read of that token at 3x magnification.
# Results are cached because OCR of a full tape takes minutes.
#-----------------------------------------------------------------------------
def ocr_tape(tape_path, cache_dir):
    """OCRs a results tape and returns its lines"""

    cache_path = os.path.join(cache_dir, re.sub(r'[^\w.-]', '_', os.path.basename(tape_path)) + '.ocr.json')
    signature = file_signature([tape_path])
    if os.path.exists(cache_path):
        with open(cache_path, encoding='utf-8') as cache_file:
            cached = json.load(cache_file)
        if cached.get('signature') == signature:
            return cached['lines']

    image = Image.open(tape_path).convert('L')
    pixels = np.asarray(image)
    lines = []
    for start, end in tape_segments(pixels):
        segment = image.crop((0, start, image.width, end))
        data = pytesseract.image_to_data(segment, config='--psm 6', output_type=pytesseract.Output.DICT)

        grouped = {}
        for i, word in enumerate(data['text']):
            if word.strip():
                key = (data['block_num'][i], data['par_num'][i], data['line_num'][i])
                grouped.setdefault(key, []).append((data['left'][i], data['top'][i] + start,
                                                    data['width'][i], data['height'][i], word))

        for words in sorted(grouped.values(), key=lambda ws: min(w[1] for w in ws)):
            words.sort()
            line = {'text': ' '.join(w[4] for w in words),
                    'y0': min(w[1] for w in words), 'y1': max(w[1] + w[3] for w in words)}

            # A count is a short final token in the right-hand column of the tape
            last = words[-1]
            if last[0] > image.width * 0.6 and len(last[4]) <= 5:
                box = (max(0, last[0] - 25), max(0, last[1] - 12),
                       min(image.width, last[0] + last[2] + 25), last[1] + last[3] + 12)
                crop = image.crop(box).resize(((box[2] - box[0]) * 3, (box[3] - box[1]) * 3), Image.LANCZOS)
                crop = ImageOps.expand(crop, border=30, fill=255)
                line['name'] = ' '.join(w[4] for w in words[:-1])
                line['raw'] = last[4]
                line['digits'] = pytesseract.image_to_string(
                    crop, config='--psm 7 -c tessedit_char_whitelist=0123456789').strip()
            lines.append(line)

    os.makedirs(cache_dir, exist_ok=True)
    with open(cache_path, 'w', encoding='utf-8') as cache_file:
        json.dump({'signature': signature, 'lines': lines}, cache_file)
    return lines


#-----------------------------------------------------------------------------
# readings()
#
# Returns the set of integer values a tape count could be, from the full-line
# OCR token and the digits-only re-read.
#-----------------------------------------------------------------------------
def readings(line):
    """Returns the possible integer readings of a tape line's count"""

    values = set()
    for token in (line.get('raw'), line.get('digits')):
        if token:
            digits = re.sub(r'[^0-9]', '', token.translate(DIGIT_FIXES))
            if digits:
                values.add(int(digits))
    return values


#-----------------------------------------------------------------------------
# count_glyphs()
#
# Returns normalized glyph images for the count printed at the right of a
# tape line.  Glyphs are separated by blank pixel columns; the count is the
# rightmost cluster of closely spaced glyphs.
#-----------------------------------------------------------------------------
def count_glyphs(image, y0, y1, mode='median'):
    """Returns normalized vectors for the count glyphs of a tape line"""

    # Faded thermal tapes print in grey, so the ink threshold is normally set
    # per line, halfway between the darkest ink and the paper background:
    #   'median'  ink at the 0.1 percentile, paper at the median (a lone digit
    #             covers well under 1% of the region)
    #   'p99'     ink at the 1st percentile, paper at the 99th
    #   'fixed'   a fixed threshold of 128 (crisp, dark print)
    region = np.asarray(image.crop((int(image.width * 0.6), max(0, y0 - 6), image.width, y1 + 6)), dtype=float)
    if mode == 'fixed':
        ink = region < 128
    else:
        if mode == 'median':
            darkest, paper = np.percentile(region, 0.1), np.median(region)
        else:
            darkest, paper = np.percentile(region, 1), np.percentile(region, 99)
        if paper - darkest < 40:
            return []
        ink = region < (darkest + paper) / 2
    columns = ink.any(axis=0)

    runs = []
    x = 0
    while x < len(columns):
        if columns[x]:
            start = x
            while x < len(columns) and columns[x]:
                x += 1
            if x - start >= 3:
                runs.append((start, x))
        else:
            x += 1
    if not runs:
        return []

    cluster = [runs[-1]]
    for run in reversed(runs[:-1]):
        if cluster[0][0] - run[1] < 14:
            cluster.insert(0, run)
        else:
            break

    vectors = []
    for start, end in cluster:
        glyph = ink[:, start:end]
        rows = np.where(glyph.any(axis=1))[0]
        glyph = glyph[rows[0]:rows[-1] + 1]
        resized = Image.fromarray((glyph * 255).astype(np.uint8)).resize((16, 24), Image.BILINEAR)
        vector = np.asarray(resized, dtype=float).ravel()
        vector -= vector.mean()
        norm = np.linalg.norm(vector)
        vectors.append(vector / norm if norm else vector)
    return vectors


#-----------------------------------------------------------------------------
# apply_template_reads()
#
# Gives every unconfirmed line a third reading by template matching.  For
# each ink-threshold mode, digit templates are averaged from the glyphs of
# lines whose OCR already equals the CVR count (never from template-confirmed
# lines), so they are calibrated to this tape.  A line is confirmed only if,
# in some mode, every glyph is read unambiguously and the number equals the
# CVR count; lines whose tape position is unknown or out of step are skipped.
#-----------------------------------------------------------------------------
def apply_template_reads(tape_path, rows):
    """Confirms unconfirmed lines by digit template matching; returns template digits and count"""

    image = Image.open(tape_path).convert('L')
    for row in rows:
        row['Method'] = 'OCR' if row['Status'] == 'MATCH' else ''
    ocr_confirmed = [row for row in rows if row['Method'] == 'OCR' and row['y0'] is not None]

    digits_seen = set()
    confirmed = 0
    for mode in ('median', 'p99', 'fixed'):
        samples = collections.defaultdict(list)
        for row in ocr_confirmed:
            text = str(row['CVR'])
            vectors = count_glyphs(image, row['y0'], row['y1'], mode)
            if len(vectors) == len(text):
                for digit, vector in zip(text, vectors):
                    samples[digit].append(vector)

        templates = {}
        for digit, vectors in samples.items():
            mean = np.mean(vectors, axis=0)
            norm = np.linalg.norm(mean)
            if norm:
                templates[digit] = mean / norm
        digits_seen.update(templates)

        for row in rows:
            if row['Status'] != 'REVIEW' or row['y0'] is None:
                continue
            digits = []
            for vector in count_glyphs(image, row['y0'], row['y1'], mode):
                scores = sorted(((float(np.dot(vector, t)), d) for d, t in templates.items()), reverse=True)
                if not scores or scores[0][0] < TEMPLATE_MIN or (len(scores) > 1 and scores[0][0] - scores[1][0] < TEMPLATE_MARGIN):
                    digits = []
                    break
                digits.append(scores[0][1])
            value = int(''.join(digits)) if digits else None
            if value is not None:
                row['Template'] = value
            if value is not None and value == row['CVR']:
                row['Status'] = 'MATCH'
                row['Method'] = f'template ({mode})'
                confirmed += 1
    return digits_seen, confirmed


#-----------------------------------------------------------------------------
# parse_tape_header()
#
# Extracts the unit serial number, public count, and poll times from the
# lines at the top of the tape.
#-----------------------------------------------------------------------------
def parse_tape_header(lines):
    """Parses header fields from the top of a results tape"""

    text = '\n'.join(line['text'] for line in lines[:60])
    header = {}
    numeric = {'UnitSerial': r'Serial Number:\s*([0-9OoSIl|]+)',
               'PublicCount': r'Public Count:\s*([0-9OoSIl|]+)',
               'ExpressVoteCards': r'ExpressVote Cards:\s*([0-9OoSIl|]+)',
               'SheetsProcessed': r'Sheets Processed:\s*([0-9OoSIl|]+)'}
    textual = {'Printed': r'(\d{1,2}:\d{2}\s*[AP]M\s+[A-Za-z]+\s+\d{1,2},\s*\d{4})',
               'ElectionDate': r'Election Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})',
               'PollOpened': r'Opened Time:\s*([0-9:]+\s*[AP]M)',
               'PollClosed': r'Closed Time:\s*([0-9:]+\s*[AP]M)'}
    for field, pattern in list(numeric.items()) + list(textual.items()):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            header[field] = match.group(1).translate(DIGIT_FIXES) if field in numeric else match.group(1)
    return header


#-----------------------------------------------------------------------------
# reprint_note()
#
# A tape printed on a later day than the election is a reprint, typically
# made from the results media on another unit (so its unit serial differs
# from the scanner that read the ballots).
#-----------------------------------------------------------------------------
def reprint_note(header, election_date=None):
    """Returns a note if the tape was printed after election day, else ''"""

    # OCR can garble the month on faded tapes, so compare only the day and year
    election_text = header.get('ElectionDate') or election_date or ''
    printed = re.search(r'(\d{1,2}),\s*(\d{4})$', header.get('Printed', ''))
    election = re.search(r'(\d{1,2}),\s*(\d{4})$', election_text)
    if printed and election and (int(printed.group(1)), printed.group(2)) != (int(election.group(1)), election.group(2)):
        return (f"  NOTE: tape printed {header['Printed']}, after the election date ({election_text}): "
                "a reprint, possibly on a different unit than the one that scanned the ballots")
    return ''


#-----------------------------------------------------------------------------
# find_cvr_pdf()
#
# Finds the CVR PDF for a ballot.  Ballot image exports are organized as
# <style>\<party>\<cvr>c.pdf (CVR) and <cvr>i.pdf (image).
#-----------------------------------------------------------------------------
def find_cvr_pdf(images_dir, style, cvr):
    """Returns the pathname of a ballot's CVR PDF, or None"""

    hits = (glob.glob(os.path.join(images_dir, style, '*', f'{cvr}c.pdf')) or
            glob.glob(os.path.join(images_dir, style, f'{cvr}c.pdf')))
    return hits[0] if hits else None


#-----------------------------------------------------------------------------
# read_cvr()
#
# Reads the header fields and the counted selections from a CVR PDF.  A
# selection whose name contains quotation marks can wrap its "(id)" onto the
# next line, so lone "(id)" lines are joined to the line above.
#-----------------------------------------------------------------------------
def read_cvr(pathname):
    """Reads a CVR PDF into a dictionary of header fields and selections"""

    text = '\n'.join(page.get_text() for page in fitz.open(pathname))
    cvr = {'Path': pathname}
    for field in CVR_FIELDS:
        match = re.search(rf'^{re.escape(field)}:\s*(.*)$', text, flags=re.MULTILINE)
        cvr[field] = match.group(1).strip() if match else None

    lines = []
    for line in (l.strip() for l in text.splitlines()):
        if not line:
            continue
        if re.fullmatch(r'\(\d+\)', line) and lines:
            lines[-1] = f"{lines[-1]} {line}"
        else:
            lines.append(line)

    selections = []
    contest = None
    for i, line in enumerate(lines):
        if i + 1 < len(lines) and lines[i + 1].startswith('Vote For:'):
            contest = re.sub(r'\s*\(\d+\)$', '', line)
        elif contest and re.search(r'\(\d+\)$', line) and i + 1 < len(lines) and lines[i + 1] == 'Counted':
            selections.append((contest, re.sub(r'\s*\(\d+\)$', '', line)))
    cvr['Selections'] = selections
    return cvr


#-----------------------------------------------------------------------------
# style_distance()
#-----------------------------------------------------------------------------
def style_distance(selected, roster):
    """Returns the L1 distance between two ballot-style Counters"""

    return sum(abs(selected[s] - roster[s]) for s in set(selected) | set(roster))


#-----------------------------------------------------------------------------
# select_ballots()
#
# Selects the CVRs that the tape reports.  Candidate batches are those with
# meaningful ballot-style overlap with the roster; their CVRs are read and
# grouped by (batch, scanner serial); groups are then added greedily while
# each addition reduces the style distance to the roster.  Overrides:
# batches restricts the candidate batches; serials keeps only those scanners.
#-----------------------------------------------------------------------------
def select_ballots(index, group, roster, images_dir, batches=None, serials=None, workers=16):
    """Selects and reads the CVRs reported by a results tape"""

    ballots = [b for b in index if b['Group'] == group]
    by_batch = collections.defaultdict(list)
    for ballot in ballots:
        by_batch[ballot['Batch']].append(ballot)

    if batches:
        candidates = [b for b in batches if b in by_batch]
    else:
        roster_size = sum(roster.values())
        candidates = []
        for batch, members in by_batch.items():
            in_set = sum(1 for m in members if m['Style'] in roster)
            if in_set >= 2 and in_set / len(members) >= 0.2 and len(members) <= 3 * roster_size + 50:
                candidates.append(batch)

    to_read = [m for batch in candidates for m in by_batch[batch]]

    def read(member):
        pathname = find_cvr_pdf(images_dir, member['Style'], member['CVR'])
        cvr = read_cvr(pathname) if pathname else {'Path': None, 'Machine Serial': None, 'Selections': []}
        cvr.update(member)
        return cvr

    with concurrent.futures.ThreadPoolExecutor(workers) as executor:
        cvrs = list(executor.map(read, to_read))

    groups = collections.defaultdict(list)
    for cvr in cvrs:
        groups[(cvr['Batch'], cvr.get('Machine Serial'))].append(cvr)
    if serials:
        groups = {k: v for k, v in groups.items() if any(s in str(k[1]) for s in serials)}

    selected_keys = []
    selected_styles = collections.Counter()
    if batches or serials or not roster:
        selected_keys = list(groups)
        for key in selected_keys:
            selected_styles.update(c['Style'] for c in groups[key])
    else:
        remaining = set(groups)
        distance = style_distance(selected_styles, roster)
        while remaining:
            best_key, best_distance = None, distance
            for key in remaining:
                trial = selected_styles + collections.Counter(c['Style'] for c in groups[key])
                trial_distance = style_distance(trial, roster)
                if trial_distance < best_distance:
                    best_key, best_distance = key, trial_distance
            if best_key is None:
                break
            selected_keys.append(best_key)
            selected_styles.update(c['Style'] for c in groups[best_key])
            remaining.discard(best_key)
            distance = best_distance

    diagnostics = []
    for key, members in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        in_set = sum(1 for m in members if m['Style'] in roster)
        diagnostics.append({'Batch': key[0], 'Serial': key[1], 'Ballots': len(members),
                            'InRosterStyles': in_set, 'Selected': key in selected_keys})

    selected = [c for key in selected_keys for c in groups[key]]
    return selected, diagnostics, selected_styles


#-----------------------------------------------------------------------------
# proposition_order()
#
# Orders proposition contests as they appear on a joint primary tape:
# Republican contests, then Democratic, each by proposition number.
#-----------------------------------------------------------------------------
def proposition_order(contest):
    """Sort key for proposition contests"""

    number = re.search(r'(\d+)\s*$', contest)
    return (0 if contest.upper().startswith('REP') else 1, int(number.group(1)) if number else 0)


#-----------------------------------------------------------------------------
# compare_tape()
#
# Compares every vote line on the tape with the CVR tally and returns one row
# per comparison with a status of MATCH or REVIEW.
#-----------------------------------------------------------------------------
def compare_tape(lines, cvrs):
    """Compares results tape lines with the CVR tally"""

    candidates = collections.Counter()
    propositions = collections.defaultdict(collections.Counter)
    first_seen = {}
    for cvr in cvrs:
        for contest, choice in cvr['Selections']:
            first_seen.setdefault((contest, choice), len(first_seen))
            if choice in ('Yes', 'No'):
                propositions[contest][choice] += 1
            else:
                candidates[(contest, choice)] += 1

    # Vote lines include lines whose count token OCR dropped, so a candidate's name can still
    # be found and its count read by template matching.
    vote_lines = []
    for line in lines:
        name = line.get('name', line['text'])
        if sum(ch.isalpha() for ch in name) >= 3 and not re.match(r'^(Yes|No)\b', line['text']):
            vote_lines.append(dict(line, name=name))
    yes_no_lines = [line for line in lines if re.match(r'^(Yes|No)\b', line['text'])]
    rows = []

    # Candidate selections: assign tape lines one-to-one, highest similarity first.  Ties
    # (the same name in both parties' contests) go to the line closest in ballot order.
    order = sorted(candidates, key=lambda key: (0 if key[0].upper().startswith('REP') else 1, first_seen[key]))
    rank = {key: i / max(1, len(order) - 1) for i, key in enumerate(order)}
    line_rank = {id(line): i / max(1, len(vote_lines) - 1) for i, line in enumerate(vote_lines)}
    pairs = []
    for key in order:
        for line in vote_lines:
            ratio = difflib.SequenceMatcher(None, letters(key[1]), letters(line['name'])).ratio()
            if ratio >= MATCH_RATIO:
                pairs.append((-ratio, abs(rank[key] - line_rank[id(line)]), key, line))
    pairs.sort(key=lambda p: (p[0], p[1]))
    assigned_keys, assigned_lines = {}, set()
    for _, _, key, line in pairs:
        if key not in assigned_keys and id(line) not in assigned_lines:
            assigned_keys[key] = line
            assigned_lines.add(id(line))

    for key in order:
        expected = candidates[key]
        line = assigned_keys.get(key)
        if line is None:
            rows.append({'Kind': 'candidate', 'Contest': key[0], 'Choice': key[1], 'CVR': expected,
                         'TapeText': '', 'Readings': '', 'Status': 'REVIEW (not found on tape)', 'y0': None, 'y1': None})
            continue
        found = readings(line)
        rows.append({'Kind': 'candidate', 'Contest': key[0], 'Choice': key[1], 'CVR': expected,
                     'TapeText': line['text'], 'Readings': sorted(found),
                     'Status': 'MATCH' if expected in found else 'REVIEW', 'y0': line['y0'], 'y1': line['y1']})

    # Tape lines no CVR selection claimed must be zero.  Skip headings: lines matching
    # NON_VOTE_LINE, garbled "Number to Vote For" lines, lines whose only possible readings
    # exceed the number of ballots (district numbers in contest headers), and lines with
    # no count token that do not look like a name.  A line repeating a matched candidate's
    # name and count is a scan-stitch overlap in the tape image and is reported as such.
    matched = [(letters(line['name']), readings(line)) for line in vote_lines if id(line) in assigned_lines]
    for line in vote_lines:
        if id(line) in assigned_lines or NON_VOTE_LINE.search(line['text']):
            continue
        if difflib.SequenceMatcher(None, letters(line['text']), 'numbertovotefor').ratio() >= 0.5:
            continue
        # Candidate names print in mixed case; all-capital sentences are the certification block
        if not re.search(r'[a-z]', line['text']) and len(line['text'].split()) >= 2:
            continue
        found = readings(line)
        if found and min(found) > len(cvrs):
            continue
        if 'raw' not in line and not re.fullmatch(r"[A-Z][\w.'\"“”-]*(\s+[A-Z\"“(][\w.'\"“”()-]*)+", line['name']):
            continue
        if found and any(difflib.SequenceMatcher(None, letters(line['name']), name).ratio() >= 0.85 and found & values
                         for name, values in matched):
            rows.append({'Kind': 'duplicate', 'Contest': '', 'Choice': line['name'], 'CVR': '',
                         'TapeText': line['text'], 'Readings': sorted(found),
                         'Status': 'DUPLICATE (scan overlap)', 'y0': line['y0'], 'y1': line['y1']})
            continue
        rows.append({'Kind': 'zero', 'Contest': '', 'Choice': line['name'], 'CVR': 0,
                     'TapeText': line['text'], 'Readings': sorted(found),
                     'Status': 'MATCH' if 0 in found else 'REVIEW', 'y0': line['y0'], 'y1': line['y1']})

    # Propositions: Yes/No lines in tape order against contests in ballot order.  When OCR
    # dropped a line the labels fall out of step; the expected value is then marked REVIEW
    # and the tape line is kept for the next expected value.
    expected_yes_no = [(contest, choice, propositions[contest][choice])
                       for contest in sorted(propositions, key=proposition_order) for choice in ('Yes', 'No')]
    position = 0
    for contest, choice, expected in expected_yes_no:
        line = yes_no_lines[position] if position < len(yes_no_lines) else None
        if line is None or not line['text'].startswith(choice):
            rows.append({'Kind': 'proposition', 'Contest': contest, 'Choice': choice, 'CVR': expected,
                         'TapeText': line['text'] if line else '', 'Readings': '',
                         'Status': 'REVIEW (tape line out of step)', 'y0': line['y0'] if line else None,
                         'y1': line['y1'] if line else None})
            continue
        found = readings(line)
        rows.append({'Kind': 'proposition', 'Contest': contest, 'Choice': choice, 'CVR': expected,
                     'TapeText': line['text'], 'Readings': sorted(found),
                     'Status': 'MATCH' if expected in found else 'REVIEW', 'y0': line['y0'], 'y1': line['y1']})
        position += 1

    return rows


#-----------------------------------------------------------------------------
# write_review_sheets()
#
# Writes PNG sheets of tape-image crops for every row that needs a visual
# check, each labeled with the expected CVR count.  A row with no tape
# position (not found / out of step) is listed by name only.
#-----------------------------------------------------------------------------
def write_review_sheets(tape_path, rows, out_dir, site, per_sheet=24):
    """Writes review sheets and returns their pathnames"""

    review = [r for r in rows if r['Status'].startswith('REVIEW')]
    if not review:
        return []

    image = Image.open(tape_path).convert('L')
    sheets = []
    for start in range(0, len(review), per_sheet):
        crops = []
        for row in review[start:start + per_sheet]:
            label = f"{row['Contest'][:34]} | {row['Choice'][:28]} | CVR={row['CVR']}"
            if row['y0'] is None:
                crops.append((label + ' | NOT ON TAPE OCR', Image.new('L', (image.width - 150, 40), 255)))
            else:
                crops.append((label, image.crop((150, max(0, row['y0'] - 12), image.width, row['y1'] + 12))))
        width = max(c.width for _, c in crops) + 520
        height = sum(c.height + 8 for _, c in crops)
        sheet = Image.new('L', (width, height), 255)
        draw = ImageDraw.Draw(sheet)
        y = 0
        for label, crop in crops:
            draw.text((6, y + min(crop.height // 2, 18)), label, fill=0)
            sheet.paste(crop, (520, y))
            y += crop.height + 8
        pathname = os.path.join(out_dir, f"{site}_review_{start // per_sheet + 1}.png")
        sheet.save(pathname)
        sheets.append(pathname)
    return sheets


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    parser = argparse.ArgumentParser(description='Verify an ES&S DS200 results tape against CVRs.')
    parser.add_argument('--tape', required=True, help='results tape TIFF')
    parser.add_argument('--images', required=True, help='ballot images root (<style>\\<party>\\<cvr>c.pdf)')
    parser.add_argument('--ballot-review', required=True, help='directory of ES&S Ballot Review .xlsx exports')
    parser.add_argument('--review-glob', default='Ballot Review *.xlsx', help='Ballot Review file pattern')
    parser.add_argument('--roster', help='check-in roster CSV with SITE ID and BALLOTSTYLE columns')
    parser.add_argument('--site', help='site ID (default: tape file name prefix, e.g. V3100)')
    parser.add_argument('--group', default='Election Day', help='Ballot Review reporting group')
    parser.add_argument('--election-date', help='election date, e.g. "March 05, 2024" (used when OCR cannot read it from the tape)')
    parser.add_argument('--batch', action='append', help='use only this batch (repeatable)')
    parser.add_argument('--serial', action='append', help='use only this scanner serial (repeatable)')
    parser.add_argument('--out', default='.', help='output directory')
    parser.add_argument('--cache', default='verify_cache', help='cache directory')
    args = parser.parse_args()

    site = args.site or re.split(r'[_\s]', os.path.basename(args.tape))[0]
    os.makedirs(args.out, exist_ok=True)

    print(f"OCR results tape {os.path.basename(args.tape)} ...")
    lines = ocr_tape(args.tape, args.cache)
    header = parse_tape_header(lines)

    print("Loading Ballot Review index ...")
    index = load_ballot_review(args.ballot_review, args.review_glob, args.cache)
    roster = load_roster_styles(args.roster, site) if args.roster else collections.Counter()
    if not roster and not (args.batch or args.serial):
        tape_serial = header.get('UnitSerial')
        if not tape_serial:
            raise SystemExit("No roster for the site and no unit serial read from the tape; use --batch or --serial.")
        args.serial = [tape_serial]

    print("Selecting and reading CVRs ...")
    cvrs, diagnostics, styles = select_ballots(index, args.group, roster, args.images, args.batch, args.serial)
    rows = compare_tape(lines, cvrs)
    templates, confirmed = apply_template_reads(args.tape, rows)

    # Write the line-by-line comparison
    csv_path = os.path.join(args.out, f"{site}_lines.csv")
    with open(csv_path, 'w', encoding='utf-8', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=['Kind', 'Contest', 'Choice', 'CVR', 'TapeText', 'Readings',
                                                      'Template', 'Method', 'Status', 'y0', 'y1'], extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    sheets = write_review_sheets(args.tape, rows, args.out, site)

    # The tape's ballot total: the value at least two of the three header totals agree on
    totals = collections.Counter(header.get(k) for k in ('PublicCount', 'ExpressVoteCards', 'SheetsProcessed') if header.get(k))
    tape_total = totals.most_common(1)[0][0] if totals and totals.most_common(1)[0][1] >= 2 else header.get('PublicCount', '?')

    # Build the report
    serials = collections.Counter(c.get('Machine Serial') for c in cvrs)
    report = [f"Results tape verification: {site}",
              f"  Tape: {args.tape}",
              f"  Tape header: {header}",
              f"  Roster check-ins at {site}: {sum(roster.values())}",
              "  Tape totals (public count / ExpressVote cards / sheets processed): "
              f"{header.get('PublicCount', '?')} / {header.get('ExpressVoteCards', '?')} / {header.get('SheetsProcessed', '?')}"
              + ('' if len({header.get('PublicCount'), header.get('ExpressVoteCards'), header.get('SheetsProcessed')}) == 1
                 else '  (disagree: OCR error or multi-sheet ballots; check the tape header)'),
              f"  CVRs selected: {len(cvrs)}  (tape ballot total: {tape_total})",
              reprint_note(header, args.election_date),
              f"  Scanners of selected CVRs: {dict(serials)}",
              "  Candidate (batch, serial) groups:"]
    for d in diagnostics:
        mark = '*' if d['Selected'] else ' '
        report.append(f"    {mark} {d['Batch']:32} {str(d['Serial']):30} ballots={d['Ballots']:>5}  in roster styles={d['InRosterStyles']:>5}")
    if roster:
        differences = {s: styles[s] - roster[s] for s in set(styles) | set(roster) if styles[s] != roster[s]}
        report.append(f"  Ballot styles, selected CVRs minus roster: {differences or 'none (exact match)'}")
    status = collections.Counter(r['Status'].split(' ')[0] for r in rows)
    methods = collections.Counter(r['Method'].split(' ')[0] for r in rows if r['Status'] == 'MATCH')
    report.append(f"  Vote lines compared: {len(rows) - status['DUPLICATE']}  MATCH={status['MATCH']} (OCR={methods['OCR']}, "
                  f"template={methods['template']})  REVIEW={status['REVIEW']}  [digit templates: {''.join(sorted(templates))}]")
    for r in rows:
        if r['Kind'] == 'duplicate':
            report.append(f"    DUPLICATE (scan overlap)     tape={r['TapeText'][:40]!r} at y={r['y0']}: the tape image repeats this line")
    for r in rows:
        if r['Status'].startswith('REVIEW'):
            report.append(f"    {r['Status']:28} {r['Contest'][:40]:40} {r['Choice'][:30]:30} CVR={r['CVR']:<5} "
                          f"tape={r['TapeText'][:40]!r} readings={r['Readings']} template={r.get('Template')}")
    report.append(f"  Line comparison: {csv_path}")
    for sheet in sheets:
        report.append(f"  Review sheet: {sheet}")

    report = [line for line in report if line]
    report_path = os.path.join(args.out, f"{site}_report.txt")
    with open(report_path, 'w', encoding='utf-8') as report_file:
        report_file.write('\n'.join(report) + '\n')
    print('\n'.join(report))


main()
