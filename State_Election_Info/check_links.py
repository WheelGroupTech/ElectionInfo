#!/usr/bin/env python3
"""Check every URL in state_election_codes.json and report broken or suspicious links.

A plain HTTP status check is not enough for this dataset: several state sites are
JavaScript applications that return "200 OK" with the same HTML shell for *every* path,
so a moved or deleted page still looks healthy. For each link this script:

  * fetches it (following redirects) with a browser-like User-Agent;
  * flags HTTP errors, timeouts, and TLS problems;
  * requests a random nonexistent path on the same host and compares the two responses -
    if they match, the host is serving an app shell / catch-all page and the link's
    content cannot be verified without a real browser;
  * flags "soft 404" pages (200 OK with a "not found" title), redirects that land on an
    error page, deep links that redirect to the site's home page, .pdf links that do not
    return a PDF, and TLS certificates that don't match the host name;
  * notes redirects to a different host (the link works but the canonical URL has moved).

Usage (from any directory; paths resolve relative to this file):

    python check_links.py                  # report problems only
    python check_links.py -v               # also list links that are OK
    python check_links.py --state TX       # limit to one or more states (repeatable)
    python check_links.py --field code_url # limit to one or more fields (repeatable)
    python check_links.py --report out.json  # write full results as JSON
    python check_links.py --strict         # exit non-zero on WARN as well as FAIL

Levels: FAIL (broken), WARN (suspicious - check in a browser), BLOCKED (the site refused a
scripted request, e.g. bot protection or a bot-challenge redirect; the link may be fine but
can't be verified by script), NOTE (works, but worth knowing), OK.
Exit status is 1 if any FAIL (or any WARN with --strict), else 0. BLOCKED never fails the run.
Uses only the Python standard library.
"""

import argparse
import concurrent.futures as cf
import hashlib
import json
import re
import secrets
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
JSON_PATH = HERE / "state_election_codes.json"

URL_FIELDS = ["code_url", "code_url_secondary", "admin_url", "voter_portal_url"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Read at most this much of each response; enough to fingerprint pages and sniff PDFs.
MAX_BYTES = 1_000_000

OK, NOTE, BLOCKED, WARN, FAIL = "OK", "NOTE", "BLOCKED", "WARN", "FAIL"
LEVEL_ORDER = {FAIL: 0, WARN: 1, BLOCKED: 2, NOTE: 3, OK: 4}

# Links already known to work only in a browser (documented in the JSON notes).
# They are reported as NOTE instead of WARN so they don't drown out new problems.
KNOWN_BROWSER_ONLY = {
    ("TX", "code_url"): "statutes.capitol.texas.gov is a JavaScript app (see JSON notes)",
}

# HTTP statuses that usually mean the site refused a scripted request rather than
# that the page is gone; a browser often loads these fine.
BLOCKED_STATUSES = {401, 403, 406, 429, 503}

# Redirect targets that are bot-protection challenges rather than real pages.
BOT_CHALLENGE = re.compile(r"(perfdrive\.com|captcha|/challenge|cf_chl|incapsula)", re.I)

# Redirects whose final path is an error page (catches pages with no usable <title>).
ERROR_PATH = re.compile(r"/(not-?found|page-?not-?found|404|error)(\.[a-z]+)?/?$", re.I)

SOFT_404_TITLE = re.compile(
    rb"(not\s+found|page\s+cannot\s+be\s+found|page\s+not\s+available|\b404\b|error\s+404)",
    re.I,
)


def fetch(url, timeout, verify=True):
    """GET a URL and return status, final URL, content type, and the first MAX_BYTES."""
    result = {"status": None, "final_url": url, "content_type": "", "body": b"", "error": None}
    req = urllib.request.Request(url, headers=HEADERS)
    context = None if verify else ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            result["status"] = resp.status
            result["final_url"] = resp.geturl()
            result["content_type"] = resp.headers.get("Content-Type", "")
            result["body"] = resp.read(MAX_BYTES)
    except urllib.error.HTTPError as e:
        result["status"] = e.code
        result["final_url"] = e.geturl() or url
        result["content_type"] = e.headers.get("Content-Type", "") if e.headers else ""
        try:
            result["body"] = e.read(MAX_BYTES)
        except Exception:
            pass
    except urllib.error.URLError as e:
        result["error"] = e.reason
    except Exception as e:  # timeouts, connection resets, malformed responses
        result["error"] = e
    return result


def is_cert_error(err):
    return isinstance(err, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(err)


def check_url(url, timeout):
    """Fetch with one retry for transient errors; fall back to an unverified TLS fetch on
    certificate errors so the report can still say whether the page itself loads."""
    res = fetch(url, timeout)
    if res["error"] is not None and not is_cert_error(res["error"]):
        res = fetch(url, timeout * 2)
    res["cert_error"] = False
    if res["error"] is not None and is_cert_error(res["error"]):
        detail = str(getattr(res["error"], "verify_message", "") or res["error"])
        res = fetch(url, timeout, verify=False)
        res["cert_error"] = True
        res["cert_detail"] = detail
    return res


def title_of(body):
    m = re.search(rb"<title[^>]*>(.*?)</title>", body, re.I | re.S)
    return re.sub(rb"\s+", b" ", m.group(1)).strip() if m else b""


def is_pdf(res):
    return res["body"][:5] == b"%PDF-" or "pdf" in res["content_type"].lower()


def host_of(url):
    return urllib.parse.urlsplit(url).netloc.lower()


def bare_host(url):
    h = host_of(url)
    return h[4:] if h.startswith("www.") else h


def is_root(url):
    parts = urllib.parse.urlsplit(url)
    return parts.path in ("", "/") and not parts.query


def short(url, limit=100):
    return url if len(url) <= limit else url[: limit - 3] + "..."


def same_page(a, b):
    """True if two 200 responses look like the same page (identical, or same title and
    nearly the same size - allowing for per-request tokens embedded in the HTML)."""
    if a["status"] != 200 or b["status"] != 200 or not a["body"] or not b["body"]:
        return False
    if hashlib.sha256(a["body"]).digest() == hashlib.sha256(b["body"]).digest():
        return True
    ta, tb = title_of(a["body"]), title_of(b["body"])
    la, lb = len(a["body"]), len(b["body"])
    return bool(ta) and ta == tb and abs(la - lb) <= 0.02 * max(la, lb)


def probe_url(url):
    parts = urllib.parse.urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}/check-links-probe-{secrets.token_hex(6)}"


def classify(abbr, field, url, res, probe):
    """Return (level, message) for one link."""
    known = KNOWN_BROWSER_ONLY.get((abbr, field))
    status, err = res["status"], res["error"]

    if err is not None:
        if isinstance(err, TimeoutError) or "timed out" in str(err).lower():
            return FAIL, ("timed out - no response (site down, or dropping traffic from this "
                          "network); recheck later or from another network before changing the link")
        return FAIL, f"connection error: {err}"
    if BOT_CHALLENGE.search(res["final_url"]):
        return BLOCKED, f"redirected to a bot-protection challenge ({host_of(res['final_url'])}) - check in a browser"
    if status in BLOCKED_STATUSES:
        return BLOCKED, f"HTTP {status} to a scripted request (bot protection?) - check in a browser"
    if status is None or status >= 400:
        return FAIL, f"HTTP {status}"

    detail = res.get("cert_detail", "").lower()
    if res.get("cert_error") and ("mismatch" in detail or "not valid for" in detail):
        return FAIL, ("TLS certificate does not match the host name - browsers show a security "
                      f"error instead of the page ({res.get('cert_detail')})")
    cert = " (TLS certificate failed Python verification - possibly an incomplete chain)"
    cert = cert if res.get("cert_error") else ""

    final_path = urllib.parse.urlsplit(res["final_url"]).path
    if ERROR_PATH.search(final_path) and not ERROR_PATH.search(urllib.parse.urlsplit(url).path):
        return FAIL, f"redirected to an error page ({short(res['final_url'])})"

    if url.lower().split("?")[0].endswith(".pdf") and not is_pdf(res):
        level = NOTE if known else WARN
        return level, f"expected a PDF but got '{res['content_type'] or 'unknown'}'{cert}"

    if not is_pdf(res):
        t = title_of(res["body"])
        if SOFT_404_TITLE.search(t):
            return FAIL, f"page title looks like an error page: {t[:80].decode('utf-8', 'replace')!r}"
        if probe is not None and same_page(res, probe):
            if known:
                return NOTE, f"browser-only: {known}"
            return WARN, (f"{host_of(res['final_url'])} returns the same page for a nonexistent "
                          "path (JavaScript app or catch-all) - content can't be verified "
                          f"without a browser{cert}")
        same_host = bare_host(res["final_url"]) == bare_host(url)
        if same_host and not is_root(url) and is_root(res["final_url"]):
            return WARN, f"deep link redirects to the site home page ({short(res['final_url'])}) - page may have moved{cert}"

    if bare_host(res["final_url"]) != bare_host(url):
        return NOTE, f"redirects to another host: {short(res['final_url'])}{cert}"
    if cert:
        return WARN, cert.strip(" ()")
    return OK, "OK"


def main():
    ap = argparse.ArgumentParser(description="Check the URLs in state_election_codes.json.")
    ap.add_argument("-v", "--verbose", action="store_true", help="also list links that are OK")
    ap.add_argument("--state", action="append", metavar="ABBR", help="limit to these states (repeatable)")
    ap.add_argument("--field", action="append", choices=URL_FIELDS, help="limit to these fields (repeatable)")
    ap.add_argument("--timeout", type=float, default=20, help="per-request timeout in seconds (default 20)")
    ap.add_argument("--workers", type=int, default=8, help="concurrent requests (default 8)")
    ap.add_argument("--report", metavar="PATH", help="write full results to this JSON file")
    ap.add_argument("--strict", action="store_true", help="exit non-zero on WARN as well as FAIL")
    args = ap.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")

    with open(JSON_PATH, encoding="utf-8") as f:
        states = json.load(f)["states"]
    wanted_states = {s.upper() for s in args.state} if args.state else None
    fields = args.field or URL_FIELDS

    links = [
        (s["abbr"], field, s[field])
        for s in states
        if wanted_states is None or s["abbr"] in wanted_states
        for field in fields
        if s.get(field)
    ]
    unique_urls = sorted({url for _, _, url in links})
    print(f"Checking {len(links)} links ({len(unique_urls)} unique URLs)...", flush=True)

    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        fetched = dict(zip(unique_urls, pool.map(lambda u: check_url(u, args.timeout), unique_urls)))

        # One nonexistent-path probe per host that served a non-root, non-PDF 200 page.
        probe_hosts = {}
        for url, res in fetched.items():
            if res["status"] == 200 and not is_pdf(res) and not is_root(url):
                probe_hosts.setdefault(host_of(res["final_url"]), probe_url(res["final_url"]))
        probe_results = dict(zip(probe_hosts, pool.map(lambda u: check_url(u, args.timeout),
                                                        probe_hosts.values())))

    results = []
    for abbr, field, url in links:
        res = fetched[url]
        probe = probe_results.get(host_of(res["final_url"]))
        level, message = classify(abbr, field, url, res, probe)
        results.append({
            "abbr": abbr, "field": field, "url": url, "level": level, "message": message,
            "status": res["status"], "final_url": res["final_url"],
            "content_type": res["content_type"],
        })
    results.sort(key=lambda r: (LEVEL_ORDER[r["level"]], r["abbr"], URL_FIELDS.index(r["field"])))

    for r in results:
        if r["level"] == OK and not args.verbose:
            continue
        print(f"{r['level']:<7}  {r['abbr']}  {r['field']:<18}  {r['url']}")
        if r["level"] != OK:
            print(f"         {r['message']}")

    counts = {lvl: sum(r["level"] == lvl for r in results) for lvl in (FAIL, WARN, BLOCKED, NOTE, OK)}
    print("\nSummary: " + ", ".join(f"{n} {lvl}" for lvl, n in counts.items()))

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
            f.write("\n")
        print(f"Wrote {len(results)} results to {args.report}")

    failing = counts[FAIL] + (counts[WARN] if args.strict else 0)
    return 1 if failing else 0


if __name__ == "__main__":
    sys.exit(main())
