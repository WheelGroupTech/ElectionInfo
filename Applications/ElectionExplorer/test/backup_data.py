"""Back up election records that Election Explorer reads to another folder (OneDrive).

Checks every voter roster, CVR, election results file and audit log in the source tree
(the repository's data folders) and in the backup tree with Election Explorer's own
loaders (test/data_check.exe), plans which source files to copy, cross-checks the
elections that have CVRs, results and audit logs, and writes a report. Voter lists are
never copied (some carry dates of birth); the backup's own voter lists are only
load-checked.

Nothing is copied unless --copy is given. A backup file whose content differs from the
source is a conflict for the user to decide, except files matching backup_data.json
"replace_patterns" (state files that are revised after download), which a newer source
copy that loads replaces.

Usage (from any directory, after test/build_data_check.ps1):
    python -I backup_data.py                 # check + plan + report (dry run)
    python -I backup_data.py --copy          # also copy the planned files
"""
# pylint: disable=too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
APP = os.path.dirname(HERE)
REPO = os.path.normpath(os.path.join(APP, "..", ".."))
CHECKER = os.path.normpath(
    os.path.join(APP, "..", "Build", "win-x64-release", "ElectionExplorer", "data_check", "data_check.exe"))
STATE = os.path.normpath(os.path.join(APP, "..", "Build", "backup_data"))
DEFAULT_DST = os.path.join(os.path.expanduser("~"), "OneDrive", "Documents", "Elections", "ElectionExplorerData")

CATEGORIES = {"Voter_Rosters": "roster", "Election_CVRs": "cvr", "Election_Results": "results",
              "Audit_Logs": "audit"}
VOTER_LISTS = "Voter_Lists"
JUNK = re.compile(r"^(desktop\.ini|thumbs\.db|~\$.*|.*\.partial|.*\.tmp)$", re.I)
CODE = re.compile(r"^([A-Z]{1,3}\d{2})(?=[ _-])")
BIG = 1 << 30  # files over 1 GB are loaded one or two at a time

LOCK = threading.Lock()


def log(msg):
    """Print a progress line."""
    with LOCK:
        print(time.strftime("%H:%M:%S ") + msg, flush=True)


def load_json(path, default):
    """Read a JSON file, or return @default when it does not exist."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def save_json(path, data):
    """Write a JSON file atomically."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, ensure_ascii=False)
    os.replace(tmp, path)


# ---------------------------------------------------------------- inventory

def inventory(root, cfg, with_lists):
    """Files of the data categories under @root: {relative path: (size, mtime)}."""
    out = {}
    skipped_dirs = set()
    tops = list(CATEGORIES) + ([VOTER_LISTS] if with_lists else [])
    for top in tops:
        base = os.path.join(root, top)
        if not os.path.isdir(base):
            continue
        for dp, dn, fn in os.walk(base):
            rel_dir = os.path.relpath(dp, root)
            keep = []
            for d in dn:
                rel = os.path.join(rel_dir, d)
                if d.startswith("_") and rel not in cfg.get("include_dirs", []):
                    skipped_dirs.add(rel)
                elif rel in cfg.get("skip_dirs", []):
                    skipped_dirs.add(rel)
                else:
                    keep.append(d)
            dn[:] = keep
            for f in fn:
                if JUNK.match(f):
                    continue
                if top == VOTER_LISTS and dp == base:
                    continue
                p = os.path.join(dp, f)
                st = os.stat(p)
                out[os.path.relpath(p, root)] = (st.st_size, st.st_mtime)
    return out, sorted(skipped_dirs)


def sha256(path):
    """SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(8 << 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def hash_all(paths, cache, jobs):
    """Hash files {abs path: (size, mtime)}, reusing @cache entries with the same size/mtime."""
    todo = [p for p, (sz, mt) in paths.items()
            if not (p in cache and cache[p][0] == sz and abs(cache[p][1] - mt) < 1)]
    total = sum(paths[p][0] for p in todo)
    log(f"hashing {len(todo)} files ({total / 1e9:.1f} GB); {len(paths) - len(todo)} cached")
    with concurrent.futures.ThreadPoolExecutor(jobs) as ex:
        for p, h in zip(todo, ex.map(sha256, todo)):
            cache[p] = [paths[p][0], paths[p][1], h]
    return {p: cache[p][2] for p in paths}


# ---------------------------------------------------------------- load checks

# Bump when data_check's load output (roster/cvr/results/audit/voters JSON) changes.
CHECK_FORMAT = 1


def checker_signature():
    """Identifies the loaders' source (not the GUI); load results are redone when it changes."""
    h = hashlib.sha256(str(CHECK_FORMAT).encode())
    src = os.path.join(APP, "src")
    for dp, _, fn in sorted(os.walk(src)):
        for f in sorted(fn):
            if f.lower().endswith((".c", ".h")) and f.lower() not in ("main.c", "main.h", "resource.h"):
                with open(os.path.join(dp, f), "rb") as fh:
                    h.update(f.encode() + b"\0" + fh.read())
    return h.hexdigest()[:16]


def run_checker(mode, files, timeout):
    """Run data_check; returns its JSON (or a failure record)."""
    t0 = time.time()
    try:
        cp = subprocess.run([CHECKER, mode] + list(files), capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        return {"mode": mode, "ok": False, "error": f"timed out after {timeout} s"}
    out = cp.stdout.decode("utf-8", "replace").strip().splitlines()
    try:
        res = json.loads(out[-1]) if out else None
    except json.JSONDecodeError:
        res = None
    if res is None:
        return {"mode": mode, "ok": False,
                "error": f"checker exit code {cp.returncode:#x} after {time.time() - t0:.0f} s (crash?)"}
    return res


def hart_sources(res):
    """TRUE when a results load read a Hart report."""
    return any("Hart" in s.get("format", "") for g in res.get("groups", []) for s in g.get("sources", []))


def misread(g):
    """A results group whose contest titles mostly repeat: a precinct-by-precinct report read as
    totals (each contest once per precinct). A two-party primary repeats at most half."""
    return g.get("contests", 0) >= 10 and 2 * g.get("repeated_titles", 0) > g["contests"]


def verdict(res):
    """'ok', 'empty' (read, but no data), 'misread' (read wrongly) or 'failed'."""
    if not res.get("ok"):
        return "failed"
    mode = res["mode"]
    if mode in ("roster", "cvr", "voters"):
        return "ok" if res.get("rows", 0) > 0 else "empty"
    if mode == "results":
        if not any(g.get("contests", 0) > 0 for g in res.get("groups", [])):
            return "empty"
        return "misread" if hart_sources(res) and any(misread(g) for g in res["groups"]) else "ok"
    if mode == "audit":
        return "ok" if any(lg.get("events", 0) > 0 for e in res.get("elections", [])
                           for lg in e.get("logs", [])) else "empty"
    return "failed"


def check_all(items, cache, sig, jobs, timeout):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Load-check {(mode, sha): abs path}; results cached by content hash and checker build."""
    def stale(k):
        c = cache.get(f"{k[0]}:{k[1]}", {})
        if c.get("sig") != sig:
            return True
        # Hart results checked before the repeated-titles count existed
        r = c["res"]
        return (k[0] == "results" and r.get("ok") and hart_sources(r)
                and any("repeated_titles" not in g for g in r["groups"]))

    todo = [(k, p) for k, p in items.items() if stale(k)]
    log(f"load-checking {len(todo)} files; {len(items) - len(todo)} cached")
    big = threading.Semaphore(2)
    done = [0]

    def one(kp):
        (mode, h), p = kp
        try:
            size = os.path.getsize(p)
        except OSError:
            # moved or deleted since the inventory (a download, the user): checked again next run
            with LOCK:
                cache[f"{mode}:{h}"] = {"sig": "vanished", "res": {
                    "mode": mode, "ok": False, "error": "the file was moved or deleted during the check; run again"}}
            log(f"  vanished {p}")
            return
        if size > BIG:
            with big:
                res = run_checker(mode, [p], timeout)
        else:
            res = run_checker(mode, [p], timeout)
        with LOCK:
            cache[f"{mode}:{h}"] = {"sig": sig, "res": res}
            done[0] += 1
        if verdict(res) == "failed" or size > BIG or done[0] % 250 == 0:
            log(f"  [{done[0]}/{len(todo)}] {verdict(res):6} {os.path.basename(p)}")

    # largest first so the long loads overlap the many small ones
    todo.sort(key=lambda kp: -(os.path.getsize(kp[1]) if os.path.exists(kp[1]) else 0))
    with concurrent.futures.ThreadPoolExecutor(jobs) as ex:
        list(ex.map(one, todo))


# ---------------------------------------------------------------- planning

def plan_copies(src, dst, hs, hd, verd, cfg):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Decide per source file: copy, replace, present, duplicate, conflict, skipped_variant, not_loaded,
    misread or no_data."""
    dst_by_dir = {}
    for rel in dst:
        dst_by_dir.setdefault(os.path.dirname(rel).lower(), {})[hd[rel]] = rel
    variants = [re.compile(x, re.I) for x in cfg.get("skip_patterns", [])]
    skip_files = {x.lower() for x in cfg.get("skip_files", [])}
    replace = [re.compile(x, re.I) for x in cfg.get("replace_patterns", [])]
    plan = []
    for rel in sorted(src):
        if rel.split(os.sep)[0] == VOTER_LISTS:
            continue
        h = hs[rel]
        v = verd[rel]
        item = {"rel": rel, "size": src[rel][0], "verdict": v}
        same_dir = dst_by_dir.get(os.path.dirname(rel).lower(), {})
        if rel in dst:
            if hd[rel] == h:
                item["action"] = "present"
            elif v == "ok" and src[rel][1] > dst[rel][1] and any(x.search(rel) for x in replace):
                item["action"] = "replace"
            else:
                item["action"] = "conflict"
                item["dst_size"] = dst[rel][0]
                item["dst_mtime"] = dst[rel][1]
                item["src_mtime"] = src[rel][1]
        elif h in same_dir:
            item["action"] = "duplicate"
            item["of"] = same_dir[h]
        elif rel.lower() in skip_files or any(x.search(os.path.basename(rel)) for x in variants):
            item["action"] = "skipped_variant"
        elif v in ("failed", "misread", "empty"):
            # only files read properly, with data, are backed up (user, 2026-10-30)
            item["action"] = {"failed": "not_loaded", "misread": "misread", "empty": "no_data"}[v]
        else:
            item["action"] = "copy"
        plan.append(item)
    return plan


def do_copy(src_root, dst_root, plan):
    """Copy the planned files, verifying each copy's hash."""
    n = 0
    for item in plan:
        if item["action"] not in ("copy", "replace"):
            continue
        s = os.path.join(src_root, item["rel"])
        d = os.path.join(dst_root, item["rel"])
        os.makedirs(os.path.dirname(d), exist_ok=True)
        tmp = d + ".partial"
        shutil.copy2(s, tmp)
        if sha256(tmp) != sha256(s):
            os.remove(tmp)
            item["action"] = "copy_failed"
            log(f"COPY FAILED (hash) {item['rel']}")
            continue
        os.replace(tmp, d)
        item["action"] = "copied" if item["action"] == "copy" else "replaced"
        n += 1
        if n % 200 == 0:
            log(f"  copied {n}")
    log(f"copied {n} files")


# ---------------------------------------------------------------- cross-checks

MONTHS = {m: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
     "november", "december"])}


def norm_date(s):
    """A results/audit date as YYYY-MM-DD when it can be read."""
    if s is None:
        return None
    s = str(s).strip()
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", s)
    if m:
        return f"{m[1]}-{m[2]}-{m[3]}"
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return f"{m[1]}-{int(m[2]):02d}-{int(m[3]):02d}"
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if m:
        y = int(m[3])
        y += 2000 if y < 100 else 0
        return f"{y}-{int(m[1]):02d}-{int(m[2]):02d}"
    m = re.search(r"([A-Za-z]+)\.? (\d{1,2}),? (\d{4})", s)
    if m and m[1].lower() in MONTHS:
        return f"{m[3]}-{MONTHS[m[1].lower()]:02d}-{int(m[2]):02d}"
    return None


PARTY = re.compile(r"(?:^|[ _-])(dem|democrat|democratic|rep|republican)(?=[ _.-]|$)", re.I)


def party_of(path):
    """'DEM' / 'REP' when a file name names a primary party, else ''."""
    m = PARTY.search(os.path.basename(path))
    return "" if m is None else ("DEM" if m[1].lower().startswith("dem") else "REP")


def cvr_sets(files, cfg):
    """The CVR files to tabulate for one election, as sets loaded apart (one per party when
    the file names give parties): originals of one format, no converted copies."""
    variants = [re.compile(x, re.I) for x in cfg.get("skip_patterns", []) + cfg.get("cvr_compare_skip", [])]
    files = [f for f in files if not any(x.search(os.path.basename(f)) for x in variants)]
    for ext in (".zip", ".xlsx", ".pdf", ".csv", ".txt", ".tsv"):
        pick = sorted(f for f in files if f.lower().endswith(ext))
        if pick:
            sets = {}
            for f in pick:
                sets.setdefault(party_of(f), []).append(f)
            return [sets[k] for k in sorted(sets)]
    return []


def crosscheck(files, cache_get, cfg, timeout, jobs):
    """Cross-check CVRs, results and audit logs of each election (county + election code)."""
    by = {}  # (county, code) -> {mode: [abs paths]}
    for rel, (absp, mode) in files.items():
        parts = rel.split(os.sep)
        m = CODE.match(parts[-1])
        if len(parts) < 3 or m is None:
            continue
        by.setdefault((parts[1], m[1]), {}).setdefault(mode, []).append(absp)
    # an election's date is the date most of its code's results files give
    code_date = {}
    for key, mm in by.items():
        dates = {}
        for p in mm.get("results", []):
            for g in cache_get("results", p).get("groups", [])[:1]:
                d = norm_date(g.get("date"))
                if d:
                    dates[d] = dates.get(d, 0) + 1
        if dates:
            code_date[key] = max(dates, key=lambda d, ds=dates: ds[d])
    audits = {}
    for (county, code), mm in by.items():
        for p in mm.get("audit", []):
            for e in cache_get("audit", p).get("elections", []):
                d = norm_date(e.get("date"))
                if d:
                    audits.setdefault((county, d), set()).add(p)

    elections = []
    for (county, code), mm in sorted(by.items()):
        d = code_date.get((county, code))
        if "cvr" not in mm and (d is None or (county, d) not in audits):
            continue
        e = {"county": county, "code": code, "date": d,
             "results_files": sorted(p for p in mm.get("results", [])
                                     if norm_date((cache_get("results", p).get("groups") or [{}])[0].get("date")) == d),
             "cvr_sets": cvr_sets(mm.get("cvr", []), cfg),
             "audit_files": sorted(audits.get((county, d), []))}
        if not e["results_files"]:
            e["note"] = "no results files of this election to compare with"
        e["results_load"] = {os.path.basename(p): cache_get("results", p) for p in e["results_files"]}
        elections.append(e)

    def one(e):
        if e["cvr_sets"] and e["results_files"]:
            args = []
            for i, s in enumerate(e["cvr_sets"]):
                args += (["+"] if i else []) + s
            for p in e["results_files"]:
                args += ["--", p]
            e["compare"] = run_checker("compare", args, timeout)
        if e["audit_files"]:
            e["audit"] = run_checker("audit", e["audit_files"], timeout)
        judge(e)
        log(f"  {e['county']} {e['code']} {e.get('date')}: {e['verdict']}")
        return e

    log(f"cross-checking {len(elections)} elections")
    with concurrent.futures.ThreadPoolExecutor(max(1, jobs // 2)) as ex:
        list(ex.map(one, elections))
    return elections


def ballot_candidates(e):
    """Ballot totals the results files give: each group's, each file's sum, and the sum of
    one file per primary party."""
    cands = set()
    per_party = {}
    for name, r in e["results_load"].items():
        vals = [g.get("ballots", -1) for g in r.get("groups", []) if g.get("ballots", -1) > 0]
        cands.update(vals)
        if vals:
            cands.add(sum(vals))
            pk = party_of(name)
            if pk:
                per_party[pk] = max(per_party.get(pk, 0), *vals)
    if len(per_party) > 1:
        cands.add(sum(per_party.values()))
    return cands


def judge_compare(e, findings):
    """CVR tabulation vs each results file's contest totals."""
    cmp_ = e.get("compare")
    if cmp_ is None:
        return
    if not cmp_.get("ok"):
        findings.append(("red", "CVR does not load for the compare: " + str(cmp_.get("error"))[:200]))
        return
    good, bad = [], []
    # a CVR of one primary party only: the other party's results contests are expected to be missing
    parties = {party_of(f) for s in e.get("cvr_sets", []) for f in s}
    one_party = len(parties) == 1 and "" not in parties
    if one_party:
        findings.append(("info", f"the CVR covers the {parties.pop()} primary only"))
    for r in cmp_.get("results", []):
        if not r.get("ok") or not r.get("groups"):
            continue
        best = max(r["groups"], key=lambda g: g.get("match", 0) - g.get("differs", 0) - g.get("only_results", 0))
        if best.get("match", 0) > 0 and best.get("differs", 0) == 0 and (one_party or best.get("only_results", 0) == 0):
            good.append((r["file"], best))
        else:
            bad.append((r["file"], best))
    if good and not bad:
        findings.append(("ok", f"CVR totals match all {len(good)} results file(s) "
                               f"({good[0][1]['match']} contest totals)"))
    for f, g in bad:
        ex = "; ".join(f"{x['contest']} / {x['choice']}: results {x['results']:,} vs CVR {x['cvr']:,}"
                       if x["status"] == "differs" else f"{x['contest']} / {x['choice']}: {x['status']}"
                       for x in g.get("examples", [])[:2])
        findings.append(("review" if good else "red",
                         f"CVR vs {f}: {g.get('differs', 0)} totals differ, {g.get('only_results', 0)} only in "
                         f"results, {g.get('match', 0)} match" + (f" (e.g. {ex})" if ex else "")))
    if good and bad:
        findings.append(("info", "CVR matches " + ", ".join(f for f, _ in good)))
    cb = cmp_.get("cvr_ballots", 0)
    cands = ballot_candidates(e)
    if cb > 0 and cands and cb not in cands:
        findings.append(("info", f"ballots: CVR {cb:,}; results give {', '.join(f'{c:,}' for c in sorted(cands))}"))


def judge(e):
    """Summarize one election's cross-check as a verdict, findings and audit flags."""
    findings = []
    flags = []
    judge_compare(e, findings)
    au = e.get("audit")
    if au is not None:
        if not au.get("ok"):
            findings.append(("red", "audit logs do not load together: " + str(au.get("error"))[:200]))
        else:
            cands = ballot_candidates(e)
            for el in au.get("elections", []):
                ob = el.get("official_ballots")
                if ob is None:
                    findings.append(("review", "audit log: no official counting session found"))
                elif cands:
                    findings.append(("ok" if ob in cands else "review",
                                     f"audit log official ballots {ob:,}"
                                     + ("" if ob in cands else
                                        f"; results give {', '.join(f'{c:,}' for c in sorted(cands))}")))
                for lg in el.get("logs", []):
                    for c in lg.get("copies", []):
                        if c.get("missing_rows"):
                            findings.append(("review", f"audit log {c['file']}: {c['missing_rows']} rows missing"))
                for name, n in el.get("notes", {}).items():
                    if n.get("severity", 0) >= 1:
                        flags.append(f"{'RED ' if n['severity'] >= 2 else ''}{name} ({n['count']})")
    if any("precanvas" in re.sub(r"[ _-]", "", os.path.basename(f).lower())
           for s in e.get("cvr_sets", []) for f in s):
        findings.append(("info", "the CVR is pre-canvass: differences from the final results are expected"))
    sev = [s for s, _ in findings]
    e["findings"] = findings
    e["flags"] = flags
    e["verdict"] = "red" if "red" in sev else "review" if "review" in sev else "ok" if findings else "-"


# ---------------------------------------------------------------- report

def fmt_size(n):
    """Bytes as a short size."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def short_error(res):
    """One line saying why a load failed (or was read wrongly)."""
    if res.get("ok") and res.get("mode") == "results" and any(misread(g) for g in res.get("groups", [])):
        g = max(res["groups"], key=lambda g: g.get("repeated_titles", 0))
        return f"read as {g['contests']:,} contests, {g['repeated_titles']:,} of them repeated (precinct report)"
    if res.get("ok"):
        return "loads, but holds no data"
    return str(res.get("error", "")).replace("\r", " ").replace("\n", " ")[:220]


def write_report(path, ctx):
    """Write the Markdown report."""
    plan, dst_checks, elections = ctx["plan"], ctx["dst_checks"], ctx["elections"]
    lines = []
    lines.append(f"# Election data backup report — {ctx['when']}")
    lines.append("")
    lines.append(f"- Source: `{ctx['src']}`")
    lines.append(f"- Backup: `{ctx['dst']}`")
    lines.append(f"- Mode: {'copy' if ctx['copy'] else 'dry run (nothing copied)'}")
    lines.append("")
    acts = {}
    for it in plan:
        a = acts.setdefault(it["action"], [0, 0])
        a[0] += 1
        a[1] += it["size"]
    lines.append("## Source files")
    lines.append("")
    lines.append("| Action | Files | Size |")
    lines.append("|---|---:|---:|")
    for a in sorted(acts):
        lines.append(f"| {a} | {acts[a][0]:,} | {fmt_size(acts[a][1])} |")
    lines.append("")
    lines.append("Actions: *copy/copied* = loads and is not in the backup; *replace/replaced* = a newer revision of a "
             "state file replaces the backup's; *present* = identical file in the "
             "backup; *duplicate* = identical content in the backup folder under another name; *conflict* = "
             "the backup has a different file of this name (not overwritten); *skipped_variant* = a "
             "converted copy matched by a skip pattern; *not_loaded* = Election Explorer cannot read it; "
             "*misread* = Election Explorer reads it wrongly (a Hart precinct-by-precinct report read as totals); "
             "*no_data* = it loads but holds no voters / votes (turnout summaries).")
    lines.append("")
    conf = [it for it in plan if it["action"] == "conflict"]
    if conf:
        lines.append("## Conflicts (decision needed)")
        lines.append("")
        lines.append("| File | Source | Backup |")
        lines.append("|---|---|---|")
        for it in conf:
            s = time.strftime("%Y-%m-%d %H:%M", time.localtime(it["src_mtime"]))
            d = time.strftime("%Y-%m-%d %H:%M", time.localtime(it["dst_mtime"]))
            lines.append(f"| {it['rel']} | {fmt_size(it['size'])}, {s}, {it['verdict']} | "
                     f"{fmt_size(it['dst_size'])}, {d}, {it.get('dst_verdict', '?')} |")
        lines.append("")
    bad_dst = [x for x in dst_checks if x["verdict"] != "ok"]
    lines.append("## Backup files that do not load")
    lines.append("")
    if not bad_dst:
        lines.append(f"All {len(dst_checks):,} backup files load.")
    else:
        lines.append(f"{len(bad_dst)} of {len(dst_checks):,} backup files:")
        lines.append("")
        for x in bad_dst:
            lines.append(f"- `{x['rel']}` — {x['verdict']}: {short_error(x['res'])}")
    lines.append("")
    mr = [it for it in plan if it["action"] == "misread"]
    if mr:
        lines.append(f"## Source files Election Explorer reads wrongly ({len(mr)}, not copied)")
        lines.append("")
        lines.append("Hart precinct-by-precinct reports read as totals: most contest titles repeat "
                     "(once per precinct).")
        lines.append("")
        for it in mr:
            g = max(it["res"]["groups"], key=lambda g: g.get("repeated_titles", 0))
            lines.append(f"- `{it['rel']}` — {g['contests']:,} contests, {g['repeated_titles']:,} repeated")
        lines.append("")
    nl = [it for it in plan if it["action"] == "not_loaded"]
    lines.append(f"## Source files Election Explorer does not read ({len(nl)})")
    lines.append("")
    by_dir = {}
    for it in nl:
        by_dir.setdefault(os.path.dirname(it["rel"]), []).append(it)
    for d in sorted(by_dir):
        lines.append(f"- `{d}`: {len(by_dir[d])}")
        for it in by_dir[d][:5]:
            lines.append(f"  - {os.path.basename(it['rel'])} — {short_error(it['res'])}")
        if len(by_dir[d]) > 5:
            lines.append(f"  - … {len(by_dir[d]) - 5} more (see plan.json)")
    lines.append("")
    emp = [it for it in plan if it["verdict"] == "empty"]
    if emp:
        lines.append(f"## Files that load with no data ({len(emp)})")
        lines.append("")
        for it in emp[:60]:
            lines.append(f"- `{it['rel']}` ({it['action']})")
        lines.append("")
    if ctx["skipped_dirs"]:
        lines.append("## Folders not backed up")
        lines.append("")
        for d in ctx["skipped_dirs"]:
            lines.append(f"- `{d}`")
        lines.append("")
    lines.append("## Cross-checks (CVRs, results, audit logs)")
    lines.append("")
    lines.append("Each results file of the election is compared with the CVR tabulation (contest totals), and "
                 "the audit log's official ballots with the results' ballots. *Audit log notes* are Election "
                 "Explorer's own review / red-flag notes from the audit log (not backup problems).")
    lines.append("")
    lines.append("| County | Election | Date | Verdict | Findings | Audit log notes |")
    lines.append("|---|---|---|---|---|---|")
    for e in elections:
        f = "<br>".join(f"{s}: {t}" for s, t in e.get("findings", [])) or e.get("note", "")
        fl = "<br>".join(e.get("flags", []))
        lines.append(f"| {e['county']} | {e['code']} | {e.get('date') or '?'} | {e.get('verdict', '-')} | "
                     f"{f.replace('|', '/')} | {fl} |")
    lines.append("")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------- main

def main():
    """Run the backup check (and copy with --copy)."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default=REPO)
    ap.add_argument("--dst", default=DEFAULT_DST)
    ap.add_argument("--config", default=os.path.join(HERE, "backup_data.json"))
    ap.add_argument("--copy", action="store_true", help="copy the planned files")
    ap.add_argument("--jobs", type=int, default=6)
    ap.add_argument("--timeout", type=int, default=3600, help="seconds per load")
    ap.add_argument("--no-crosscheck", action="store_true")
    ap.add_argument("--no-voter-lists", action="store_true", help="skip load-checking the backup's voter lists")
    a = ap.parse_args()
    if not os.path.exists(CHECKER):
        sys.exit(f"build the checker first: {os.path.join(HERE, 'build_data_check.ps1')}")
    cfg = load_json(a.config, {})
    os.makedirs(STATE, exist_ok=True)
    when = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    out_dir = os.path.join(STATE, "reports", when)
    os.makedirs(out_dir, exist_ok=True)
    hcache_p = os.path.join(STATE, "hash_cache.json")
    lcache_p = os.path.join(STATE, "load_cache.json")
    hcache = load_json(hcache_p, {})
    lcache = load_json(lcache_p, {})

    src, src_skipped = inventory(a.src, cfg, False)
    dst, dst_skipped = inventory(a.dst, cfg, not a.no_voter_lists)
    log(f"source {len(src)} files, backup {len(dst)} files")
    absmap = {os.path.join(a.src, r): v for r, v in src.items()}
    absmap.update({os.path.join(a.dst, r): v for r, v in dst.items()})
    hashes = hash_all(absmap, hcache, a.jobs)
    save_json(hcache_p, hcache)
    hs = {r: hashes[os.path.join(a.src, r)] for r in src}
    hd = {r: hashes[os.path.join(a.dst, r)] for r in dst}

    def mode_of(rel):
        top = rel.split(os.sep)[0]
        return "voters" if top == VOTER_LISTS else CATEGORIES[top]

    items = {}
    for r in src:
        items.setdefault((mode_of(r), hs[r]), os.path.join(a.src, r))
    for r in dst:
        items.setdefault((mode_of(r), hd[r]), os.path.join(a.dst, r))
    sig = checker_signature()
    try:
        check_all(items, lcache, sig, a.jobs, a.timeout)
    finally:
        save_json(lcache_p, lcache)

    def res_of(mode, h):
        return lcache[f"{mode}:{h}"]["res"]

    verd = {r: verdict(res_of(mode_of(r), hs[r])) for r in src}
    plan = plan_copies(src, dst, hs, hd, verd, cfg)
    for it in plan:
        it["res"] = res_of(mode_of(it["rel"]), hs[it["rel"]]) if it["verdict"] != "ok" else None
        if it["action"] == "conflict":
            it["dst_verdict"] = verdict(res_of(mode_of(it["rel"]), hd[it["rel"]]))
    dst_checks = [{"rel": r, "verdict": verdict(res_of(mode_of(r), hd[r])), "res": res_of(mode_of(r), hd[r])}
                  for r in sorted(dst)]
    if a.copy:
        do_copy(a.src, a.dst, plan)

    elections = []
    if not a.no_crosscheck:
        files = {}
        for r in dst:
            if mode_of(r) != "voters" and verdict(res_of(mode_of(r), hd[r])) == "ok":
                files[r] = (os.path.join(a.dst, r), mode_of(r))
        for it in plan:
            if it["verdict"] == "ok" and it["action"] in ("copy", "copied", "replace", "replaced", "present"):
                files.setdefault(it["rel"], (os.path.join(a.src, it["rel"]), mode_of(it["rel"])))
        hash_of = {os.path.join(a.src, r): h for r, h in hs.items()}
        hash_of.update({os.path.join(a.dst, r): h for r, h in hd.items()})

        def cache_get(mode, p):
            return res_of(mode, hash_of[p])

        elections = crosscheck(files, cache_get, cfg, a.timeout, a.jobs)

    ctx = {"when": when.replace("_", " "), "src": a.src, "dst": a.dst, "copy": a.copy, "plan": plan,
           "dst_checks": dst_checks, "elections": elections,
           "skipped_dirs": sorted(set(src_skipped) | set(dst_skipped))}
    save_json(os.path.join(out_dir, "plan.json"), plan)
    save_json(os.path.join(out_dir, "backup_checks.json"), dst_checks)
    save_json(os.path.join(out_dir, "crosscheck.json"), elections)
    write_report(os.path.join(out_dir, "report.md"), ctx)
    log(f"report: {os.path.join(out_dir, 'report.md')}")


if __name__ == "__main__":
    main()
