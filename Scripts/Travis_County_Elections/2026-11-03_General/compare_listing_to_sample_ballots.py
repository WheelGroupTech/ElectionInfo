#-----------------------------------------------------------------------------
# compare_listing_to_sample_ballots.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to compare the Ballot Detail Listing (the CSV written by
# parse_ballot_detail_listing.py) with the sample ballot PDF exported by the
# ES&S EVS 6.3 for the November 3, 2026 Joint General and Special Elections.
#
# Every page of the sample ballot PDF is a side of a card (sheet).  The front
# of each card has the ballot style (e.g. "100A") in its header; the back is
# the following page.
#
# The timing marks along the top and left edges form the grid used by the
# listing: columns are numbered from 0 on the left and rows from 0 at the
# top.  The grid is measured on each page, and each selection oval is
# located on it.
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
#   - every card in the listing is in the PDF, and every card in the PDF is
#     in the listing
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
# row timing marks (down the left edge), and the left edge x position.
#-----------------------------------------------------------------------------
def measure_grid(page):
    """Locates the timing marks on a page"""

    columns = set()
    rows = set()
    for drawing in page.get_drawings():
        rect = drawing["rect"]
        if drawing.get("fill") is None or rect.width > 15 or rect.height > 15:
            continue
        if rect.y0 < 55:
            columns.add(round((rect.x0 + rect.x1) / 2, 1))
        if rect.x0 < 32:
            rows.add(round((rect.y0 + rect.y1) / 2, 1))
    return sorted(columns), sorted(rows)


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
def decode_card_code(page, rows):
    """Decodes the code marks just inside the left timing marks"""

    marked = set()
    for drawing in page.get_drawings():
        rect = drawing["rect"]
        if drawing.get("fill") is not None and 33 <= rect.x0 <= 38 and rect.width < 10 and rect.height < 12:
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
# compare()
#-----------------------------------------------------------------------------
def compare(listing, pdf_pathname):
    """Compares the sample ballot PDF with the listing; returns the problems"""

    doc = pymupdf.open(pdf_pathname)
    problems = collections.defaultdict(list)
    found = {}
    stats = collections.Counter()

    cards_per_style = collections.Counter((ballot_type, style) for ballot_type, style, page in listing)

    page_texts = [doc[i].get_text() for i in range(doc.page_count)]
    for i in range(doc.page_count):
        style_match = STYLE_PATTERN.search(page_texts[i])
        if not style_match:
            continue
        style = style_match.group(1)
        front = doc[i]
        side_pages = [i]
        if i + 1 < doc.page_count and not STYLE_PATTERN.search(page_texts[i + 1]):
            side_pages.append(i + 1)

        columns, rows = measure_grid(front)
        if len(columns) != GRID_COLUMNS or len(rows) != GRID_ROWS:
            problems["grid"].append(f"page {i+1}: found {len(columns)} column and {len(rows)} row timing marks")
            continue

        try:
            ballot_type, card_page, number = decode_card_code(front, rows)
        except ValueError as e:
            problems["code"].append(f"page {i+1} ({style}): {e}")
            continue

        key = (ballot_type, style, card_page)
        name = f"{ballot_type} {style} card {card_page}"
        if key in found:
            problems["cards"].append(f"page {i+1}: {name} also on page {found[key]}")
            continue
        found[key] = i + 1
        stats["cards"] += 1

        card = listing.get(key)
        if card is None:
            problems["cards"].append(f"page {i+1}: {name} is not in the listing")
            continue
        if number != card["number"]:
            problems["code"].append(f"page {i+1}: {name} code marks encode card style {number}, listing has {card['number']}")

        for side in (1, 2):
            expected = card["sides"][side]
            if side > len(side_pages):
                if expected:
                    problems["positions"].append(f"{name} side {side}: page missing, listing has {len(expected)} selections")
                continue
            page_index = side_pages[side - 1]
            page = doc[page_index]
            stats["sides"] += 1

            side_columns, side_rows = measure_grid(page) if side == 2 else (columns, rows)
            if len(side_columns) != GRID_COLUMNS or len(side_rows) != GRID_ROWS:
                problems["grid"].append(f"page {page_index+1}: found {len(side_columns)} column and {len(side_rows)} row timing marks")
                continue
            ovals = find_ovals(page, side_columns, side_rows)

            for position in sorted(set(expected) - set(ovals)):
                problems["positions"].append(f"page {page_index+1}: {name} side {side}: no oval at row {position[0]} col {position[1]} for {expected[position][0]} / {expected[position][1]}")
            for position in sorted(set(ovals) - set(expected)):
                problems["positions"].append(f"page {page_index+1}: {name} side {side}: oval at row {position[0]} col {position[1]} ('{ovals[position]}') is not in the listing")

            for position in sorted(set(ovals) & set(expected)):
                stats["ovals"] += 1
                contest, candidate = expected[position]
                if not label_matches(ovals[position], candidate):
                    problems["labels"].append(f"page {page_index+1}: {name} side {side} row {position[0]} col {position[1]}: printed '{ovals[position]}', listing has {contest} / {candidate}")

            # Footer, when present
            footer = PAGE_PATTERN.search(page_texts[page_index])
            if footer is None:
                stats[f"sides without footer ({ballot_type})"] += 1
                continue
            cards_in_style = cards_per_style[(ballot_type, style)]
            expected_footer = (card_page, cards_in_style, 2 * (card_page - 1) + side, 2 * cards_in_style)
            actual_footer = tuple(int(g) for g in footer.groups())
            if actual_footer != expected_footer:
                problems["footers"].append(f"page {page_index+1}: {name} side {side}: footer '{footer.group(0)}', listing has {cards_in_style} card(s), expected 'Sheet {expected_footer[0]} of {expected_footer[1]}, Page {expected_footer[2]} of {expected_footer[3]}'")

    for key in sorted(set(listing) - set(found)):
        problems["cards"].append(f"{key[0]} {key[1]} card {key[2]} is in the listing but not in the PDF")

    doc.close()
    return problems, stats


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    if len(sys.argv) not in (3, 4):
        print("Usage: python compare_listing_to_sample_ballots.py <ballot_detail_listing.csv> <sample_ballots.pdf> [report.txt]")
        return False
    listing = read_listing(sys.argv[1])
    problems, stats = compare(listing, sys.argv[2])

    lines = [f"Listing: {len(listing)} cards, {sum(len(s) for c in listing.values() for s in c['sides'].values())} selections"]
    lines += [f"PDF: {count} {what}" for what, count in sorted(stats.items())]
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
