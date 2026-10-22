#-----------------------------------------------------------------------------
# compare_listing_to_sample_ballots.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to compare the Ballot Detail Listing (the CSV written by
# parse_ballot_detail_listing.py) with the sample ballots exported by the
# ES&S EVS 6.3 for the November 3, 2026 Joint General and Special Elections.
#
# The sample ballots may be either the single PDF containing every ballot,
# or a folder of PDFs (e.g. one per ballot style, as written by
# process_g26_sample_bbm_ballots.py or as distributed by Travis County).
#
# Every page of a sample ballot PDF is a side of a card (sheet).  The front
# of each card has the ballot style (e.g. "100A") in its header; the back is
# the following page of the same PDF.
#
# The timing marks along the top and left edges form the grid used by the
# listing: columns are numbered from 0 on the left and rows from 0 at the
# top.  The grid is measured on each page, relative to the outermost timing
# marks (the PDFs distributed by the county are cropped, which shifts the
# page coordinates), and each selection oval is located on it.
#
# The front of each card also has a column of code marks just inside the
# left timing marks, read by the scanner to identify the card:
#   row  1      always marked
#   rows 2, 3   ballot type: row 2 = federal offices only (FED),
#               row 3 = limited ballot (LB), neither = full ballot
#   row  6      always marked
#   row  7      second card (sheet) of the ballot
#   rows 15-24  four marks and rows 25-28 one mark, which together encode
#               the card style number from the listing (1, 2, 3, ...)
#
# Checks:
#   - every card in the listing is in the PDFs, and every card in the PDFs is
#     in the listing (only for the ballot types found in the PDFs, so a
#     folder of Full ballots is not reported as missing the FED and LB cards)
#   - no card appears more than once, and a file named for a ballot style
#     contains that style
#   - the card style number in the code marks matches the listing
#   - every oval on each side is at a Row/Col in the listing for that side,
#     and every listing selection has an oval
#   - the text to the right of each oval matches the listing candidate
#   - the "Sheet X of Y, Page N of M" footer (when present) agrees with the
#     number of cards in the listing
#-----------------------------------------------------------------------------
"""compare_listing_to_sample_ballots.py"""
# pylint: disable=line-too-long,too-many-locals,too-many-branches,too-many-statements

import collections
import csv
import itertools
import os
import re
import sys
import unicodedata

# 3rd Party imports
import pymupdf


# Ballot style on the line following the election title / Spanish title
STYLE_PATTERN = re.compile(r"Travis County Joint General and Special Elections\s*\n[^\n]*\n\s*(\d+[A-Z]+)\s*\n")

# Footer present on the full and federal only ballots
PAGE_PATTERN = re.compile(r"Sheet\s+(\d+)\s+of\s+(\d+),\s*Page\s+(\d+)\s+of\s+(\d+)")

# Card style number encoding: the Nth combination of 4 of the 10 rows 15-24
# (in order), times 4, plus which of rows 25-28 is marked
STYLE_COMBINATIONS = list(itertools.combinations(range(15, 25), 4))

# Number of timing marks across the top and down the left edge
GRID_COLUMNS = 26
GRID_ROWS = 93

# Ballot style in a file name, e.g. "Sample-100A.pdf" or "100A.pdf"
FILENAME_STYLE_PATTERN = re.compile(r"^(?:Sample-)?(\d+[A-Z]+)\.pdf$", re.IGNORECASE)


#-----------------------------------------------------------------------------
# read_listing()
#
# Returns {(ballot type, style, page): card} where card has the card style
# number and {side: {(row, col): (contest, candidate)}}.
#-----------------------------------------------------------------------------
def read_listing(pathname):
    """Reads the CSV written by parse_ballot_detail_listing.py"""

    cards = {}
    with open(pathname, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            key = (row["Ballot Type"], row["Ballot Style ID"], int(row["Page"]))
            card = cards.setdefault(key, {"number": int(row["Card Style Number"]), "sides": {1: {}, 2: {}}})
            card["sides"][int(row["Side"])][(int(row["Row"]), int(row["Col"]))] = (row["Contest"], row["Candidate"])
    return cards


#-----------------------------------------------------------------------------
# measure_grid()
#
# Returns the centers of the column timing marks (along the top) and of the
# row timing marks (down the left edge), and the x position of the left
# timing marks.  The timing marks are the small filled rectangles; the top
# row of them and the leftmost column of them are the tracks.
#-----------------------------------------------------------------------------
def measure_grid(page):
    """Locates the timing marks on a page"""

    marks = []
    for drawing in page.get_drawings():
        rect = drawing["rect"]
        if drawing.get("fill") is None or rect.width > 15 or rect.height > 15:
            continue
        if any(item[0] == "c" for item in drawing["items"]):
            continue   # ovals
        marks.append(rect)
    if not marks:
        return [], [], None

    top = min(rect.y0 for rect in marks)
    left = min(rect.x0 for rect in marks)
    columns = {round((rect.x0 + rect.x1) / 2, 1) for rect in marks if abs(rect.y0 - top) < 3}
    rows = {round((rect.y0 + rect.y1) / 2, 1) for rect in marks if abs(rect.x0 - left) < 3}
    return sorted(columns), sorted(rows), left


#-----------------------------------------------------------------------------
# nearest()
#-----------------------------------------------------------------------------
def nearest(centers, value, tolerance=6):
    """Returns the index of the nearest center, or None if not close"""

    index = min(range(len(centers)), key=lambda i: abs(centers[i] - value))
    return index if abs(centers[index] - value) <= tolerance else None


#-----------------------------------------------------------------------------
# find_ovals()
#
# Returns {(row, col): label} for each selection oval on the page, where the
# label is the first word printed to the right of the oval.
#-----------------------------------------------------------------------------
def find_ovals(page, columns, rows):
    """Locates the selection ovals on a page"""

    words = page.get_text("words")
    ovals = {}
    for drawing in page.get_drawings():
        rect = drawing["rect"]
        if drawing.get("fill") is None or rect.width < 7 or rect.width > 10 or rect.height < 5 or rect.height > 7:
            continue
        if not any(item[0] == "c" for item in drawing["items"]):
            continue

        center_y = (rect.y0 + rect.y1) / 2
        col = nearest(columns, (rect.x0 + rect.x1) / 2)
        row = nearest(rows, center_y)
        if col is None or row is None:
            continue

        right = sorted((w for w in words if rect.x1 < w[0] < rect.x1 + 40 and w[1] - 3 < center_y < w[3] + 3), key=lambda w: w[0])
        label = right[0][4] if right else ""

        # Skip the example oval in the voting instructions: "fill the oval ( )"
        if label.startswith(")"):
            continue
        ovals[(row, col)] = label
    return ovals


#-----------------------------------------------------------------------------
# decode_card_code()
#
# Returns (ballot type, page, card style number) from the code marks on the
# front of a card, or raises ValueError if the marks cannot be decoded.
#-----------------------------------------------------------------------------
def decode_card_code(page, rows, left):
    """Decodes the code marks just inside the left timing marks"""

    # The code marks are about 6 points to the right of the left timing marks
    marked = set()
    for drawing in page.get_drawings():
        rect = drawing["rect"]
        if drawing.get("fill") is not None and left + 3 <= rect.x0 <= left + 9 and rect.width < 10 and rect.height < 12:
            row = nearest(rows, (rect.y0 + rect.y1) / 2)
            if row is not None:
                marked.add(row)

    if 1 not in marked or 6 not in marked:
        raise ValueError(f"code marks {sorted(marked)} missing row 1 or 6")
    ballot_type = {(): "Full", (2,): "FED", (3,): "LB"}.get(tuple(sorted(marked & {2, 3})))
    if ballot_type is None:
        raise ValueError(f"code marks {sorted(marked)} have both rows 2 and 3")
    card_page = 2 if 7 in marked else 1

    combination = tuple(sorted(r for r in marked if 15 <= r <= 24))
    suffix = [r for r in marked if 25 <= r <= 28]
    if combination not in STYLE_COMBINATIONS or len(suffix) != 1:
        raise ValueError(f"code marks {sorted(marked)} do not encode a card style number")
    number = STYLE_COMBINATIONS.index(combination) * 4 + (suffix[0] - 25) + 1

    return ballot_type, card_page, number


#-----------------------------------------------------------------------------
# label_matches()
#-----------------------------------------------------------------------------
def label_matches(label, candidate):
    """Checks the word printed next to an oval against the listing candidate"""

    def norm(text):
        return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower().rstrip(",")

    label = norm(label)
    candidate = norm(candidate)
    if candidate.startswith("write-in"):
        # The write-in oval has the write-in line to its right
        return label in ("", "write-in")
    if candidate in ("for", "against"):
        # "FOR / A FAVOR", "AGAINST / EN CONTRA"
        return label == candidate
    return bool(label) and candidate.split()[0].startswith(label)


#-----------------------------------------------------------------------------
# compare_pdf()
#
# Compares the cards in one sample ballot PDF with the listing, adding to
# `results`: "found" maps each card already seen (in this or an earlier PDF)
# to where it was seen, plus the "problems" and "stats".  `prefix` is
# prepended to page references ("" for a single PDF, or the file name when
# comparing a folder).
#-----------------------------------------------------------------------------
def compare_pdf(listing, pathname, prefix, results):
    """Compares the cards in one sample ballot PDF with the listing"""

    found, problems, stats = results["found"], results["problems"], results["stats"]
    doc = pymupdf.open(pathname)
    cards_per_style = collections.Counter((ballot_type, style) for ballot_type, style, page in listing)
    filename_match = FILENAME_STYLE_PATTERN.match(os.path.basename(pathname)) if prefix else None

    page_texts = [doc[i].get_text() for i in range(doc.page_count)]
    if not any(STYLE_PATTERN.search(text) for text in page_texts):
        problems["cards"].append(f"{prefix or pathname}: no ballot style found")

    for i in range(doc.page_count):
        style_match = STYLE_PATTERN.search(page_texts[i])
        if not style_match:
            continue
        style = style_match.group(1)
        where = f"{prefix}page {i+1}"
        front = doc[i]
        side_pages = [i]
        if i + 1 < doc.page_count and not STYLE_PATTERN.search(page_texts[i + 1]):
            side_pages.append(i + 1)

        if filename_match and filename_match.group(1).upper() != style:
            problems["cards"].append(f"{where}: file is named for style {filename_match.group(1)}, but the page is style {style}")

        columns, rows, left = measure_grid(front)
        if len(columns) != GRID_COLUMNS or len(rows) != GRID_ROWS:
            problems["grid"].append(f"{where}: found {len(columns)} column and {len(rows)} row timing marks")
            continue

        try:
            ballot_type, card_page, number = decode_card_code(front, rows, left)
        except ValueError as e:
            problems["code"].append(f"{where} ({style}): {e}")
            continue

        key = (ballot_type, style, card_page)
        name = f"{ballot_type} {style} card {card_page}"
        stats["types"].add(ballot_type)
        if key in found:
            problems["cards"].append(f"{where}: {name} also on {found[key]}")
            continue
        found[key] = where
        stats["counts"]["cards"] += 1

        card = listing.get(key)
        if card is None:
            problems["cards"].append(f"{where}: {name} is not in the listing")
            continue
        if number != card["number"]:
            problems["code"].append(f"{where}: {name} code marks encode card style {number}, listing has {card['number']}")

        for side in (1, 2):
            expected = card["sides"][side]
            if side > len(side_pages):
                if expected:
                    problems["positions"].append(f"{where}: {name} side {side}: page missing, listing has {len(expected)} selections")
                continue
            page_index = side_pages[side - 1]
            page = doc[page_index]
            side_where = f"{prefix}page {page_index+1}"
            stats["counts"]["sides"] += 1

            side_columns, side_rows, _ = measure_grid(page) if side == 2 else (columns, rows, left)
            if len(side_columns) != GRID_COLUMNS or len(side_rows) != GRID_ROWS:
                problems["grid"].append(f"{side_where}: found {len(side_columns)} column and {len(side_rows)} row timing marks")
                continue
            ovals = find_ovals(page, side_columns, side_rows)

            for position in sorted(set(expected) - set(ovals)):
                problems["positions"].append(f"{side_where}: {name} side {side}: no oval at row {position[0]} col {position[1]} for {expected[position][0]} / {expected[position][1]}")
            for position in sorted(set(ovals) - set(expected)):
                problems["positions"].append(f"{side_where}: {name} side {side}: oval at row {position[0]} col {position[1]} ('{ovals[position]}') is not in the listing")

            for position in sorted(set(ovals) & set(expected)):
                stats["counts"]["ovals"] += 1
                contest, candidate = expected[position]
                if not label_matches(ovals[position], candidate):
                    problems["labels"].append(f"{side_where}: {name} side {side} row {position[0]} col {position[1]}: printed '{ovals[position]}', listing has {contest} / {candidate}")

            # Footer, when present
            footer = PAGE_PATTERN.search(page_texts[page_index])
            if footer is None:
                stats["counts"][f"sides without footer ({ballot_type})"] += 1
                continue
            cards_in_style = cards_per_style[(ballot_type, style)]
            expected_footer = (card_page, cards_in_style, 2 * (card_page - 1) + side, 2 * cards_in_style)
            actual_footer = tuple(int(g) for g in footer.groups())
            if actual_footer != expected_footer:
                problems["footers"].append(f"{side_where}: {name} side {side}: footer '{footer.group(0)}', listing has {cards_in_style} card(s), expected 'Sheet {expected_footer[0]} of {expected_footer[1]}, Page {expected_footer[2]} of {expected_footer[3]}'")

    doc.close()


#-----------------------------------------------------------------------------
# compare()
#
# Compares the sample ballots (a PDF file, or a folder of PDF files) with the
# listing and returns the problems and statistics.
#-----------------------------------------------------------------------------
def compare(listing, sample_pathname):
    """Compares the sample ballots with the listing; returns the problems"""

    results = {"found": {}, "problems": collections.defaultdict(list),
               "stats": {"counts": collections.Counter(), "types": set(), "files": 0}}
    found, problems, stats = results["found"], results["problems"], results["stats"]

    if os.path.isdir(sample_pathname):
        pathnames = sorted(os.path.join(sample_pathname, f) for f in os.listdir(sample_pathname) if f.lower().endswith(".pdf"))
        for pathname in pathnames:
            compare_pdf(listing, pathname, f"{os.path.basename(pathname)} ", results)
    else:
        pathnames = [sample_pathname]
        compare_pdf(listing, sample_pathname, "", results)
    stats["files"] = len(pathnames)

    # Only report missing cards for the ballot types in the sample ballots
    for key in sorted(set(listing) - set(found)):
        if key[0] in stats["types"]:
            problems["cards"].append(f"{key[0]} {key[1]} card {key[2]} is in the listing but not in the sample ballots")

    return problems, stats


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    if len(sys.argv) not in (3, 4):
        print("Usage: python compare_listing_to_sample_ballots.py <ballot_detail_listing.csv> <sample_ballots.pdf or folder> [report.txt]")
        return False
    listing = read_listing(sys.argv[1])
    problems, stats = compare(listing, sys.argv[2])

    listing_types = sorted({ballot_type for ballot_type, style, page in listing})
    lines = [f"Listing: {len(listing)} cards, {sum(len(s) for c in listing.values() for s in c['sides'].values())} selections"]
    lines.append(f"Sample ballots: {stats['files']} PDF file(s), ballot types {', '.join(sorted(stats['types'])) or 'none'}")
    lines += [f"Sample ballots: {count} {what}" for what, count in sorted(stats["counts"].items())]
    unchecked = [t for t in listing_types if t not in stats["types"]]
    if unchecked:
        lines.append(f"Not checked (no sample ballots of this type): {', '.join(unchecked)}")
    lines.append(f"Problems: {sum(len(v) for v in problems.values())}")
    for category in ("cards", "code", "grid", "positions", "labels", "footers"):
        if problems.get(category):
            lines.append("")
            lines.append(f"{category.upper()} ({len(problems[category])})")
            lines += [f"  {problem}" for problem in problems[category]]

    print("\n".join(lines))
    if len(sys.argv) == 4:
        with open(sys.argv[3], "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print(f"Wrote {sys.argv[3]}")

    return True

main()
