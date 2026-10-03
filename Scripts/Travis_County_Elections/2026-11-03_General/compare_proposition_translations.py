#-----------------------------------------------------------------------------
# compare_proposition_translations.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to extract the English and Spanish text of every ballot
# proposition from the sample ballot PDF exported by the ES&S EVS 6.3 for
# the November 3, 2026 Joint General and Special Elections, and to check the
# translations for problems that can be detected automatically.
#
# The ballots print English in an upright font and Spanish in italics, with
# the headers in bold:
#   City of Austin Proposition A          (Arial,Bold)
#   THIS IS A TAX INCREASE; ...           (Arial)
#   Ciudad de Austin Propuesta A          (Arial,BoldItalic)
#   ESTE ES UN AUMENTO DE IMPUESTO; ...   (Arial,Italic)
#   FOR / A FAVOR
#   AGAINST / EN CONTRA
#
# The PDF contains the full set of ballot styles three times (Full, FED, LB),
# as described in process_g26_sample_bbm_ballots.py.
#
# Automated checks:
#   - the Spanish header is not translated ("Proposition" instead of
#     "Propuesta"), or has a different proposition letter
#   - numbers (amounts, rates, section and ordinance numbers) that appear in
#     only one language
#   - a "TAX INCREASE" statement in only one language
#   - the same proposition worded differently on different ballot styles
#   - unexpected choice labels
#   - a proposition on a style's Full ballot but not its LB ballot, or the
#     reverse
#
# These checks cannot tell whether a translation is accurate; the CSV output
# lists each distinct English/Spanish pair once for side-by-side review.
#
# Output:
#   <prefix>.csv          one row per distinct proposition wording
#   <prefix>_report.txt   the automated check results
#-----------------------------------------------------------------------------
"""compare_proposition_translations.py"""
# pylint: disable=line-too-long,too-many-locals,too-many-branches,too-many-statements
# pylint: disable=unsubscriptable-object

import collections
import csv
import difflib
import re
import sys

# 3rd Party imports
import pymupdf


# Ballot style on the line following the election title / Spanish title
STYLE_PATTERN = re.compile(r"Travis County Joint General and Special Elections\s*\n[^\n]*\n\s*(\d+[A-Z]+)\s*\n")

# Ballot type for each successive occurrence of the ballot styles
BALLOT_TYPES = ["Full", "FED", "LB"]

# English and Spanish proposition headers
EN_HEADER_PATTERN = re.compile(r"^(.*?\bProposition\s+[A-Z0-9]+)\b")
ES_HEADER_PATTERN = re.compile(r"\b(?:Propuesta|Proposici[oó]n|Proposition)\s+([A-Z0-9]+)\b")

# Choice lines, e.g. "FOR / A FAVOR"
CHOICE_PATTERN = re.compile(r"^[^/a-z]+ / [^/a-z]+$")
EXPECTED_CHOICES = {"FOR / A FAVOR", "AGAINST / EN CONTRA"}

# Body text size; smaller text is oval labels and timing mark labels, larger
# is the page header and the "Sample Ballot" watermark
MIN_TEXT_SIZE = 8
MAX_TEXT_SIZE = 11


#-----------------------------------------------------------------------------
# reading_order()
#
# Sorts the text blocks of a page down each of the three ballot columns.
#-----------------------------------------------------------------------------
def reading_order(page):
    """Returns the page's text blocks in reading order"""

    width = page.rect.width
    def column(block):
        x = block["bbox"][0]
        return 0 if x < width * 0.23 else 1 if x < width * 0.51 else 2
    blocks = [b for b in page.get_text("dict")["blocks"] if b.get("lines")]
    return sorted(blocks, key=lambda b: (column(b), b["bbox"][1]))


#-----------------------------------------------------------------------------
# classify_line()
#
# Returns the line's text and the (bold, italic, text) of each body-size span.
#-----------------------------------------------------------------------------
def classify_line(line):
    """Classifies the spans of a text line"""

    spans = []
    for span in line["spans"]:
        # Keep the spaces, which are often spans of their own
        if MIN_TEXT_SIZE <= span["size"] <= MAX_TEXT_SIZE:
            bold = "Bold" in span["font"]
            italic = "Italic" in span["font"] or bool(span["flags"] & 2)
            spans.append((bold, italic, span["text"]))
    return re.sub(r"\s+", " ", "".join(t for b, i, t in spans)).strip(), spans


#-----------------------------------------------------------------------------
# is_english_header()
#-----------------------------------------------------------------------------
def is_english_header(spans):
    """Checks whether a line's text is all bold, upright (English header)"""

    printed = [(bold, italic) for bold, italic, text in spans if text.strip()]
    return bool(printed) and all(bold and not italic for bold, italic in printed)


#-----------------------------------------------------------------------------
# extract_propositions()
#-----------------------------------------------------------------------------
def extract_propositions(pathname):
    """Extracts every proposition from the sample ballot PDF"""

    doc = pymupdf.open(pathname)
    propositions = []
    occurrences = collections.Counter()
    style = None
    ballot_type = None

    def new_proposition(page_number):
        return {"style": style, "type": ballot_type, "page": page_number,
                "en_header": [], "en_body": [], "es_header": [], "es_body": [], "choices": []}

    def finish(proposition):
        for part in ("en_header", "en_body", "es_header", "es_body"):
            proposition[part] = re.sub(r"\s+", " ", " ".join(proposition[part])).strip()
        match = EN_HEADER_PATTERN.match(proposition["en_header"])
        proposition["id"] = match.group(1) if match else proposition["en_header"]
        propositions.append(proposition)

    for i in range(doc.page_count):
        page = doc[i]
        style_match = STYLE_PATTERN.search(page.get_text())
        if style_match and style_match.group(1) != style:
            style = style_match.group(1)
            ballot_type = BALLOT_TYPES[min(occurrences[style], len(BALLOT_TYPES) - 1)]
            occurrences[style] += 1

        current = None
        for block in reading_order(page):
            lines = [classify_line(line) for line in block["lines"]]
            for k, (text, spans) in enumerate(lines):
                if not text:
                    continue

                if current is not None and CHOICE_PATTERN.match(text):
                    current["choices"].append(text)
                    continue
                if current is not None and current["choices"]:
                    finish(current)
                    current = None

                if current is None:
                    # A proposition starts at a bold English line containing
                    # "Proposition", together with the bold English lines
                    # just above it in the same block (e.g. "City of Manor")
                    if not (is_english_header(spans) and re.search(r"\bProposition\b", text)):
                        continue
                    current = new_proposition(i + 1)
                    start = k
                    while start > 0 and is_english_header(lines[start - 1][1]):
                        start -= 1
                    for previous_text, _ in lines[start:k]:
                        current["en_header"].append(previous_text)

                # Consecutive spans of the same style are joined as printed;
                # the parts of different lines are joined with a space
                segments = []
                for bold, italic, span_text in spans:
                    part = ("es_" if italic else "en_") + ("header" if bold else "body")
                    if segments and segments[-1][0] == part:
                        segments[-1][1] += span_text
                    else:
                        segments.append([part, span_text])
                for part, segment_text in segments:
                    current[part].append(segment_text)

        if current is not None:
            if not current["choices"]:
                print(f"WARNING: page {i+1}: proposition '{' '.join(current['en_header'])}' has no choices on the page")
            finish(current)

    doc.close()
    return propositions


#-----------------------------------------------------------------------------
# numbers()
#
# Returns the set of numbers in the text, as digit strings with the
# thousands and decimal separators removed (so "$1.20" and "1,20 $" match).
#-----------------------------------------------------------------------------
def numbers(text):
    """Extracts the numbers from text"""

    return {re.sub(r"[.,]", "", n) for n in re.findall(r"\d[\d.,]*\d|\d", text)}


#-----------------------------------------------------------------------------
# check_wording()
#
# Returns the list of automated check results for one English/Spanish pair.
#-----------------------------------------------------------------------------
def check_wording(proposition):
    """Checks one proposition's English and Spanish text"""

    flags = []
    en_letter = re.search(r"\bProposition\s+([A-Z0-9]+)\b", proposition["en_header"])
    es_letter = ES_HEADER_PATTERN.search(proposition["es_header"])

    if not proposition["es_header"] and not proposition["es_body"]:
        flags.append("no Spanish text")
    if re.search(r"\bProposition\b", proposition["es_header"]):
        flags.append(f"Spanish header not translated: '{proposition['es_header']}'")
    if en_letter and es_letter and en_letter.group(1) != es_letter.group(1):
        flags.append(f"Spanish header is proposition {es_letter.group(1)}, English is {en_letter.group(1)}")
    if en_letter and not es_letter:
        flags.append(f"Spanish header has no proposition letter: '{proposition['es_header']}'")

    en_numbers = numbers(proposition["en_body"])
    es_numbers = numbers(proposition["es_body"])
    if en_numbers - es_numbers:
        flags.append(f"numbers only in English: {sorted(en_numbers - es_numbers)}")
    if es_numbers - en_numbers:
        flags.append(f"numbers only in Spanish: {sorted(es_numbers - en_numbers)}")

    en_tax = "TAX INCREASE" in proposition["en_body"].upper()
    es_tax = "AUMENTO" in proposition["es_body"].upper() and "IMPUESTO" in proposition["es_body"].upper()
    if en_tax != es_tax:
        flags.append("tax increase statement only in " + ("English" if en_tax else "Spanish"))

    unexpected = [c for c in proposition["choices"] if c not in EXPECTED_CHOICES]
    if unexpected:
        flags.append(f"unexpected choices: {unexpected}")
    if not proposition["choices"]:
        flags.append("no choices")

    return flags


#-----------------------------------------------------------------------------
# describe_difference()
#-----------------------------------------------------------------------------
def describe_difference(a, b):
    """Summarizes the word differences between two texts"""

    a_words = a.split()
    b_words = b.split()
    changes = []
    for op, a1, a2, b1, b2 in difflib.SequenceMatcher(None, a_words, b_words).get_opcodes():
        if op != "equal":
            changes.append(f"'{' '.join(a_words[a1:a2])}' -> '{' '.join(b_words[b1:b2])}'")
    summary = "; ".join(changes)
    return summary if len(summary) <= 400 else summary[:400] + " ..."


#-----------------------------------------------------------------------------
# style_list()
#-----------------------------------------------------------------------------
def style_list(styles, limit=None):
    """Formats a set of styles"""

    styles = sorted(styles)
    if limit and len(styles) > limit:
        return ", ".join(styles[:limit]) + f", ... ({len(styles)} styles)"
    return ", ".join(styles)


#-----------------------------------------------------------------------------
# write_outputs()
#-----------------------------------------------------------------------------
def write_outputs(propositions, prefix):
    """Groups the propositions, runs the checks, and writes the outputs"""

    # Distinct wordings of each proposition
    variants = collections.defaultdict(list)
    for proposition in propositions:
        wording = (proposition["en_header"], proposition["en_body"], proposition["es_header"], proposition["es_body"], tuple(proposition["choices"]))
        variants[(proposition["id"], wording)].append(proposition)
    by_id = collections.defaultdict(list)
    for (prop_id, wording), instances in variants.items():
        by_id[prop_id].append((wording, instances))

    report = [f"{len(propositions)} proposition instances, {len(by_id)} propositions, {len(variants)} distinct wordings", ""]

    # Per-wording checks and CSV
    with open(f"{prefix}.csv", "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Proposition", "Variant", "Ballot Types", "Style Count", "Styles",
                         "English Header", "English", "Spanish Header", "Spanish", "Choices", "Flags"])
        report.append("WORDING CHECKS")
        flagged = 0
        for prop_id in sorted(by_id):
            for number, (wording, instances) in enumerate(by_id[prop_id], start=1):
                styles = {p["style"] for p in instances}
                types = sorted({p["type"] for p in instances})
                flags = check_wording(instances[0])
                writer.writerow([prop_id, number, " ".join(types), len(styles), style_list(styles),
                                 wording[0], wording[1], wording[2], wording[3], " | ".join(wording[4]), " | ".join(flags)])
                if flags:
                    flagged += 1
                    variant = f" (variant {number})" if len(by_id[prop_id]) > 1 else ""
                    report.append(f"  {prop_id}{variant} - styles {style_list(styles, 12)}")
                    report += [f"      {flag}" for flag in flags]
        if not flagged:
            report.append("  none")

    # Propositions worded differently on different styles
    report += ["", "PROPOSITIONS WITH MORE THAN ONE WORDING"]
    multiple = [prop_id for prop_id in sorted(by_id) if len(by_id[prop_id]) > 1]
    for prop_id in multiple:
        report.append(f"  {prop_id}")
        first_wording, _ = by_id[prop_id][0]
        for number, (wording, instances) in enumerate(by_id[prop_id], start=1):
            report.append(f"    variant {number}: styles {style_list({p['style'] for p in instances}, 12)}")
            if number > 1:
                for label, index in (("English", 1), ("Spanish", 3)):
                    if wording[index] != first_wording[index]:
                        report.append(f"      {label} differs from variant 1: {describe_difference(first_wording[index], wording[index])}")
    if not multiple:
        report.append("  none")

    # Propositions on the Full ballot but not the LB ballot, or the reverse
    report += ["", "PROPOSITIONS ON ONLY ONE OF A STYLE'S FULL AND LB BALLOTS"]
    present = collections.defaultdict(set)
    for proposition in propositions:
        present[(proposition["style"], proposition["type"])].add(proposition["id"])
    missing = collections.defaultdict(set)
    for style in sorted({p["style"] for p in propositions}):
        full = present.get((style, "Full"), set())
        limited = present.get((style, "LB"), set())
        for prop_id in full - limited:
            missing[(prop_id, "LB")].add(style)
        for prop_id in limited - full:
            missing[(prop_id, "Full")].add(style)
    for (prop_id, absent_from), styles in sorted(missing.items()):
        report.append(f"  {prop_id}: missing from the {absent_from} ballot of styles {style_list(styles)}")
    if not missing:
        report.append("  none")

    with open(f"{prefix}_report.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(report) + "\n")
    print("\n".join(report))
    print(f"\nWrote {prefix}.csv and {prefix}_report.txt")


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    if len(sys.argv) not in (2, 3):
        print("Usage: python compare_proposition_translations.py <sample_ballots.pdf> [output_prefix]")
        return False
    prefix = sys.argv[2] if len(sys.argv) == 3 else "proposition_translations"

    propositions = extract_propositions(sys.argv[1])
    write_outputs(propositions, prefix)

    return True

main()
