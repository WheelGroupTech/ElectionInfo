#-----------------------------------------------------------------------------
# build_election_data_formats_pdf.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Python script to build ELECTION-DATA-FORMATS.pdf from
# ELECTION-DATA-FORMATS.md at the top of the repository.
#
# Steps:
#   - convert the Markdown to HTML (the `markdown` package), with a print
#     style sheet: Letter pages, navy headings and table headers
#   - replace the Mermaid flowchart, which a PDF cannot run, with the same
#     diagram drawn as HTML boxes
#   - point links to the repository's own files at their GitHub pages (a PDF
#     has no repository beside it); links within the document still work
#   - print the HTML to PDF with headless Microsoft Edge
#   - add a "Page N of M" footer to every page after the first, and the
#     document title, with PyMuPDF
#
# Rerun it whenever the Markdown changes.  Needs Microsoft Edge and:
#   python -m pip install markdown pymupdf
#
# Usage:
#   python -I build_election_data_formats_pdf.py [input.md [output.pdf]]
#-----------------------------------------------------------------------------
"""build_election_data_formats_pdf.py"""

import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# 3rd Party imports
import markdown
import pymupdf


REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPOSITORY / "ELECTION-DATA-FORMATS.md"
DEFAULT_OUTPUT = REPOSITORY / "ELECTION-DATA-FORMATS.pdf"
GITHUB = "https://github.com/WheelGroupTech/ElectionInfo"
TITLE = "Election Data Formats"

EDGE_LOCATIONS = [
    Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
]

# Section 10's Mermaid flowchart, drawn with HTML boxes
DIAGRAM = """
<div class="flow">
  <div class="stage"><div class="stage-title">Sources</div>
    <div class="box">Vendors<br><small>ES&amp;S, Hart, Dominion</small></div>
    <div class="box">Websites<br><small>Clarity, Enhanced Voting, county sites</small></div>
    <div class="box">Texas SOS</div></div>
  <div class="arrow">&#8594;</div>
  <div class="stage"><div class="stage-title">Containers</div>
    <div class="box">ZIP</div><div class="box">XLSX / XLS</div><div class="box">CSV / TSV / TXT</div>
    <div class="box">XML / JSON</div><div class="box">PDF text</div><div class="box">PDF image &#8594; OCR</div></div>
  <div class="arrow">&#8594;</div>
  <div class="stage"><div class="stage-title">Common table models</div>
    <div class="box">Voter list</div><div class="box">Voter roster</div><div class="box">CVR</div>
    <div class="box">Results</div><div class="box">Audit log</div></div>
  <div class="arrow">&#8594;</div>
  <div class="stage"><div class="stage-title">&nbsp;</div>
    <div class="box strong">Compare and reconcile</div></div>
</div>
"""

CSS = """
@page { size: Letter; margin: 0.75in 0.7in 0.8in 0.7in; }
body { font-family: "Segoe UI", Arial, sans-serif; font-size: 10pt; line-height: 1.42;
       color: #1a1a1a; }
h1 { font-size: 24pt; margin: 0 0 6pt; color: #12355b; }
h2 { font-size: 15pt; color: #12355b; border-bottom: 1.5pt solid #12355b; padding-bottom: 2pt;
     margin-top: 18pt; break-after: avoid; }
h3 { font-size: 11.5pt; color: #12355b; margin-top: 14pt; break-after: avoid; }
h2.newpage { break-before: page; }
p, li { orphans: 3; widows: 3; }
ul, ol { padding-left: 18pt; }
li { margin: 2pt 0; }
hr { display: none; }
a { color: #1d5fa8; text-decoration: none; }
code { font-family: Consolas, "Courier New", monospace; font-size: 8.8pt; background: #f1f3f5;
       padding: 0 2pt; border-radius: 2pt; }
blockquote { margin: 8pt 0; padding: 6pt 10pt; background: #eef4fb; border-left: 3pt solid #1d5fa8; }
blockquote p { margin: 0; }
table { border-collapse: collapse; width: 100%; margin: 8pt 0 10pt; font-size: 8.6pt;
        line-height: 1.3; }
thead { display: table-header-group; }
[id="11-further-reading"] + table { break-inside: avoid; }
tr { break-inside: avoid; }
th { background: #12355b; color: white; text-align: left; padding: 3pt 5pt; font-weight: 600; }
td { border-bottom: 0.5pt solid #c8ced6; padding: 3pt 5pt; vertical-align: top; }
tr:nth-child(even) td { background: #f7f9fb; }
th code, td code { font-size: 8pt; }
.flow { display: flex; align-items: stretch; gap: 6pt; margin: 10pt 0 12pt; break-inside: avoid; }
.stage { flex: 1; display: flex; flex-direction: column; gap: 4pt; }
.stage-title { font-weight: 600; color: #12355b; font-size: 9pt; text-align: center; }
.box { border: 1pt solid #1d5fa8; border-radius: 4pt; padding: 4pt; text-align: center;
       font-size: 8.6pt; background: #eef4fb; }
.box.strong { background: #12355b; color: white; margin-top: auto; margin-bottom: auto; }
.box small { color: #4a5a6a; font-size: 7.5pt; }
.arrow { align-self: center; font-size: 18pt; color: #1d5fa8; }
"""


#-----------------------------------------------------------------------------
# github_link()
#
# Points a repository-relative link at its GitHub page.
#-----------------------------------------------------------------------------
def github_link(match):
    """Rewrites one href attribute"""

    href = match.group(1)
    if href.startswith(("http:", "https:", "#", "mailto:")):
        return match.group(0)
    kind = "tree" if href.endswith("/") else "blob"
    return f'href="{GITHUB}/{kind}/main/{href}"'


#-----------------------------------------------------------------------------
# markdown_to_html()
#-----------------------------------------------------------------------------
def markdown_to_html(input_pathname):
    """Converts the Markdown document to a printable HTML page"""

    text = Path(input_pathname).read_text(encoding="utf-8")
    text, count = re.subn(r"```mermaid\n.*?```\n", "<!--DIAGRAM-->\n", text, flags=re.S)
    if count != 1:
        print(f"Warning: expected one Mermaid diagram, found {count}")

    body = markdown.markdown(text, extensions=["tables", "toc", "sane_lists", "fenced_code"])
    body = body.replace("<!--DIAGRAM-->", DIAGRAM)
    body = re.sub(r'href="([^"]+)"', github_link, body)

    # The detailed sections start on a new page after the summary and contents
    body = body.replace('<h2 id="1-the-data-collection"',
                        '<h2 class="newpage" id="1-the-data-collection"')

    return (f'<!doctype html><html><head><meta charset="utf-8"><title>{TITLE}</title>'
            f"<style>{CSS}</style></head><body>{body}</body></html>")


#-----------------------------------------------------------------------------
# print_to_pdf()
#
# Prints the HTML page with headless Edge.  msedge.exe can hand the job to
# another process and return before the PDF is written, so wait for it.
#-----------------------------------------------------------------------------
def print_to_pdf(html_pathname, pdf_pathname, work_folder):
    """Prints an HTML file to PDF with Microsoft Edge"""

    edge = next((path for path in EDGE_LOCATIONS if path.exists()), None)
    if edge is None:
        print("Microsoft Edge was not found")
        return False

    subprocess.run([str(edge), "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    f"--user-data-dir={work_folder / 'edge-profile'}",
                    f"--print-to-pdf={pdf_pathname}", html_pathname.as_uri()],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    for _ in range(120):
        if pdf_pathname.exists() and pdf_pathname.stat().st_size > 0:
            time.sleep(1)
            return True
        time.sleep(0.5)

    print("Edge did not write the PDF")
    return False


#-----------------------------------------------------------------------------
# finish_pdf()
#
# Adds the footer and the document information, and saves the final PDF.
#-----------------------------------------------------------------------------
def finish_pdf(printed_pathname, output_pathname):
    """Adds page footers and metadata to the printed PDF"""

    doc = pymupdf.open(printed_pathname)
    total = doc.page_count
    grey = (0.35, 0.4, 0.45)
    for number, page in enumerate(doc, start=1):
        if number == 1:
            continue
        bottom = page.rect.height - 30
        page.insert_text((54, bottom), TITLE, fontsize=8, fontname="helv", color=grey)
        label = f"Page {number} of {total}"
        width = pymupdf.get_text_length(label, fontname="helv", fontsize=8)
        page.insert_text((page.rect.width - 54 - width, bottom), label, fontsize=8,
                         fontname="helv", color=grey)
    doc.set_metadata({"title": TITLE,
                      "subject": "How U.S. election records are stored and published",
                      "author": "WheelGroupTech ElectionInfo",
                      "creator": "build_election_data_formats_pdf.py"})
    doc.save(output_pathname, garbage=3, deflate=True)
    doc.close()
    return total


#-----------------------------------------------------------------------------
# main()
#-----------------------------------------------------------------------------
def main():
    """Main function"""

    if len(sys.argv) > 3:
        print("Usage: python build_election_data_formats_pdf.py [input.md [output.pdf]]")
        return False
    input_pathname = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_INPUT
    output_pathname = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUTPUT

    # Edge can still hold its profile folder open when the folder is removed
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as folder:
        work_folder = Path(folder)
        html_pathname = work_folder / "document.html"
        printed_pathname = work_folder / "printed.pdf"
        html_pathname.write_text(markdown_to_html(input_pathname), encoding="utf-8")
        if not print_to_pdf(html_pathname, printed_pathname, work_folder):
            return False
        pages = finish_pdf(printed_pathname, output_pathname)

    print(f"Wrote {output_pathname} ({pages} pages)")
    return True

main()
