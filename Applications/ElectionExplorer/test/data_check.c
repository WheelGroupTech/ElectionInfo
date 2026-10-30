/* data_check: load election data files with Election Explorer's own loaders and print one
 * JSON object describing what loaded. Used by test/backup_data.py to decide which files
 * are backed up and to cross-check CVRs, results and audit logs of one election.
 *
 *   data_check roster  files...           voter rosters (EeRoster_LoadFiles)
 *   data_check cvr     files...           cast vote records (vendor chosen as the CVR window does)
 *   data_check results files...           election results (EeResults_LoadFiles)
 *   data_check audit   files...           audit logs (EeAudit_LoadFiles)
 *   data_check voters  file               a voter list (EeVoterTable_LoadFromFile)
 *   data_check compare cvr... [+ cvr...] -- results [-- results ...]
 *       contest totals of each results file vs the CVR tabulation (CVR sets after '+' are
 *       loaded apart, e.g. a primary's parties, and tabulated together)
 *
 * Exit code 0 when the load succeeded (the JSON then has "ok": true), 1 when it failed.
 * No voter names or addresses are printed: rosters and voter lists report counts only. */
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "voter_table.h"
#include "voter_roster.h"
#include "ee_cvr.h"
#include "ee_results.h"
#include "ee_audit.h"

/* ---- JSON output ---- */

static void j_str(const wchar_t *s)
{
    putchar('"');
    for (; s != NULL && *s; s++)
    {
        wchar_t c = *s;
        if (c == L'"' || c == L'\\')
        {
            printf("\\%c", (char)c);
        }
        else if (c < 0x20)
        {
            printf("\\u%04x", (unsigned)c);
        }
        else if (c < 0x80)
        {
            putchar((char)c);
        }
        else
        {
            char u[8];
            int n;
            if (c >= 0xD800 && c <= 0xDBFF && s[1] >= 0xDC00 && s[1] <= 0xDFFF)
            {
                n = WideCharToMultiByte(CP_UTF8, 0, s, 2, u, sizeof u, NULL, NULL);
                s++;
            }
            else
            {
                n = WideCharToMultiByte(CP_UTF8, 0, s, 1, u, sizeof u, NULL, NULL);
            }
            fwrite(u, 1, (size_t)(n > 0 ? n : 0), stdout);
        }
    }
    putchar('"');
}

static void j_astr(const char *s)
{
    wchar_t w[1024];
    if (s == NULL)
    {
        printf("null");
        return;
    }
    if (MultiByteToWideChar(CP_UTF8, 0, s, -1, w, ARRAYSIZE(w)) == 0)
    {
        w[0] = L'\0';
    }
    j_str(w);
}

/* A note can be long; keep its first @p max characters. */
static void j_strn(const wchar_t *s, size_t max)
{
    wchar_t buf[1024];
    if (s == NULL)
    {
        printf("null");
        return;
    }
    if (max >= ARRAYSIZE(buf))
    {
        max = ARRAYSIZE(buf) - 1;
    }
    wcsncpy_s(buf, ARRAYSIZE(buf), s, _TRUNCATE);
    if (wcslen(buf) > max)
    {
        buf[max] = L'\0';
    }
    j_str(buf);
}

static const wchar_t *base_name(const wchar_t *p)
{
    const wchar_t *b = wcsrchr(p, L'\\');
    return b != NULL ? b + 1 : p;
}

static int fail(const wchar_t *mode, EeLoadStatus st, const wchar_t *err, DWORD t0)
{
    printf("{\"mode\":");
    j_str(mode);
    printf(",\"ok\":false,\"status\":%d,\"ms\":%lu,\"error\":", (int)st, GetTickCount() - t0);
    j_strn(err, 600);
    printf("}\n");
    return 1;
}

/* ---- modes ---- */

static int do_roster(const wchar_t *const *paths, int n)
{
    EeVoterTable t;
    EeRosterLoadInfo info;
    wchar_t err[1024] = L"";
    DWORD t0 = GetTickCount();
    EeLoadStatus st;
    int m;
    EeVoterTable_Init(&t);
    ZeroMemory(&info, sizeof info);
    st = EeRoster_LoadFiles(paths, n, &t, &info, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (st != EeLoadStatus_Ok)
    {
        EeRoster_FreeInfo(&info);
        return fail(L"roster", st, err, t0);
    }
    printf("{\"mode\":\"roster\",\"ok\":true,\"ms\":%lu,\"rows\":%u,\"cols\":%u,\"files_read\":%u,"
           "\"sheets_read\":%u,\"unsupported_files\":%u,\"reference_files\":%u,\"skipped_sheets\":%u,"
           "\"recovered_sheets\":%u,\"rows_no_date\":%u,\"vuids_malformed\":%u,\"vuids_missing\":%u,"
           "\"rows_redacted\":%u,\"method_rows\":[",
           GetTickCount() - t0, t.row_count, t.column_count, info.files_read, info.sheets_read,
           info.unsupported_files, info.reference_files, info.skipped_sheets, info.recovered_sheets,
           info.rows_no_date, info.vuids_malformed, info.vuids_missing, info.rows_redacted);
    for (m = 0; m < EE_VM_COUNT; m++)
    {
        printf("%s%u", m ? "," : "", info.method_rows[m]);
    }
    printf("],\"note\":");
    j_strn(info.note, 900);
    printf("}\n");
    EeRoster_FreeInfo(&info);
    EeVoterTable_Clear(&t);
    return 0;
}

static BOOL ends_with(const wchar_t *p, const wchar_t *ext)
{
    size_t n = wcslen(p), k = wcslen(ext);
    return n >= k && _wcsicmp(p + n - k, ext) == 0;
}

/* Load CVR files choosing the vendor as the CVR window does (CvrLoadThreadProc). */
static EeLoadStatus load_cvr(const wchar_t *const *paths, int n, EeCvrTable *t, wchar_t *err, size_t cch)
{
    int i, nhart = 0, ndom = 0;
    for (i = 0; i < n; i++)
    {
        if (ends_with(paths[i], L".zip") && EeCvr_IsDominionZip(paths[i]))
        {
            ndom++;
        }
        else if (ends_with(paths[i], L".zip") || ends_with(paths[i], L".pdf"))
        {
            nhart++;
        }
    }
    if ((ndom > 0 && ndom < n) || (nhart > 0 && nhart < n))
    {
        wcscpy_s(err, cch, L"files from more than one vendor");
        return EeLoadStatus_Error;
    }
    if (ndom > 0)
    {
        return EeCvr_LoadFromDominionZips(paths, n, t, NULL, NULL, NULL, err, cch);
    }
    if (nhart > 0)
    {
        return EeCvr_LoadFromHartFiles(paths, n, t, NULL, NULL, NULL, err, cch);
    }
    return EeCvr_LoadFromFiles(paths, n, t, NULL, NULL, NULL, err, cch);
}

static int do_cvr(const wchar_t *const *paths, int n)
{
    EeCvrTable t;
    EeCvrCardCounts cc;
    wchar_t err[1024] = L"";
    DWORD t0 = GetTickCount();
    EeLoadStatus st;
    uint32_t pcol = 0;
    EeCvr_Init(&t);
    st = load_cvr(paths, n, &t, err, ARRAYSIZE(err));
    if (st != EeLoadStatus_Ok)
    {
        EeCvr_Clear(&t);
        return fail(L"cvr", st, err, t0);
    }
    ZeroMemory(&cc, sizeof cc);
    EeCvr_CardCounts(&t, NULL, 0, &cc);
    printf("{\"mode\":\"cvr\",\"ok\":true,\"ms\":%lu,\"rows\":%u,\"cols\":%u,\"ballots\":%u,\"cards\":%u,"
           "\"multi_card\":%s,\"has_precinct\":%s,\"note\":",
           GetTickCount() - t0, t.nrows, t.ncols, cc.ballots, cc.rows, EeCvr_HasMultiCard(&t) ? "true" : "false",
           EeCvr_FindColumnByTitle(&t, L"Precinct", &pcol) ? "true" : "false");
    j_strn(t.load_note, 600);
    printf("}\n");
    EeCvr_Clear(&t);
    return 0;
}

/* Contests whose title repeats an earlier contest's. A precinct-by-precinct report read as
 * totals repeats each contest once per precinct; a two-party primary without party
 * prefixes repeats at most half its titles. */
static uint32_t repeated_titles(const EeResults *r)
{
    uint32_t i, j, n = 0;
    for (i = 1; i < r->ncontests; i++)
    {
        for (j = 0; j < i; j++)
        {
            if (r->contests[i].title != NULL && r->contests[j].title != NULL &&
                wcscmp(r->contests[i].title, r->contests[j].title) == 0)
            {
                n++;
                break;
            }
        }
    }
    return n;
}

static void print_results(const EeResults *r)
{
    uint32_t i;
    printf("{\"election\":");
    j_str(r->election_name);
    printf(",\"date\":");
    j_str(r->election_date);
    printf(",\"region\":");
    j_str(r->region);
    printf(",\"timestamp\":");
    j_str(r->timestamp);
    printf(",\"contests\":%u,\"repeated_titles\":%u,\"choices\":%u,\"precincts\":%u,\"detail\":%s,\"registered\":%lld,"
           "\"ballots\":%lld,\"vote_types\":[",
           r->ncontests, repeated_titles(r), r->nchoices, r->nprecincts,
           EeResults_HasPrecinctDetail(r) ? "true" : "false", (long long)r->registered_total,
           (long long)r->ballots_total);
    for (i = 0; i < r->nvt; i++)
    {
        printf("%s{\"name\":", i ? "," : "");
        j_str(r->vote_types[i]);
        printf(",\"ballots\":%lld}", r->ballots_vt ? (long long)r->ballots_vt[i] : -1LL);
    }
    printf("],\"sources\":[");
    for (i = 0; i < r->nsources; i++)
    {
        printf("%s{\"file\":", i ? "," : "");
        j_str(base_name(r->sources[i].path));
        printf(",\"member\":");
        j_str(r->sources[i].member);
        printf(",\"format\":");
        j_str(EeResults_FormatLabel(r->sources[i].format));
        printf(",\"contests\":%u,\"precincts\":%u}", r->sources[i].ncontests, r->sources[i].nprecincts);
    }
    printf("],\"nissues\":%u,\"issues\":[", r->nissues);
    for (i = 0; i < r->nissues && i < 12; i++)
    {
        printf("%s", i ? "," : "");
        j_strn(r->issues[i].text, 300);
    }
    printf("]");
    if (GetEnvironmentVariableW(L"DATA_CHECK_TOTALS", NULL, 0) > 0)
    {
        /* DATA_CHECK_TOTALS=1: every choice's total, to compare two reports of one election */
        uint32_t c, k, first = 1;
        printf(",\"totals\":[");
        for (c = 0; c < r->ncontests; c++)
        {
            const EeResultsContest *ct = &r->contests[c];
            for (k = 0; k < ct->nchoices; k++)
            {
                printf("%s[", first ? "" : ",");
                first = 0;
                j_str(ct->title);
                printf(",");
                j_str(r->choices[ct->first_choice + k].name);
                printf(",%lld]", (long long)r->choices[ct->first_choice + k].total);
            }
        }
        printf("]");
    }
    printf("}");
}

static int do_results(const wchar_t *const *paths, int n)
{
    EeResults *res = NULL;
    uint32_t nres = 0, g;
    wchar_t err[1024] = L"";
    DWORD t0 = GetTickCount();
    EeLoadStatus st = EeResults_LoadFiles(paths, (uint32_t)n, &res, &nres, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (st != EeLoadStatus_Ok)
    {
        return fail(L"results", st, err, t0);
    }
    printf("{\"mode\":\"results\",\"ok\":true,\"ms\":%lu,\"groups\":[", GetTickCount() - t0);
    for (g = 0; g < nres; g++)
    {
        printf("%s", g ? "," : "");
        print_results(&res[g]);
    }
    printf("]}\n");
    EeResults_FreeArray(res, nres);
    return 0;
}

static int do_audit(const wchar_t *const *paths, int n)
{
    EeAuditElection *els = NULL;
    uint32_t nels = 0, e, i;
    wchar_t err[1024] = L"";
    DWORD t0 = GetTickCount();
    EeLoadStatus st = EeAudit_LoadFiles(paths, (uint32_t)n, &els, &nels, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (st != EeLoadStatus_Ok)
    {
        return fail(L"audit", st, err, t0);
    }
    printf("{\"mode\":\"audit\",\"ok\":true,\"ms\":%lu,\"elections\":[", GetTickCount() - t0);
    for (e = 0; e < nels; e++)
    {
        EeAuditElection *el = &els[e];
        EeAuditAccounting a;
        EeAuditNotes notes;
        int l, first = 1;
        printf("%s{\"customer\":", e ? "," : "");
        j_astr(el->customer);
        printf(",\"election\":");
        j_astr(el->election);
        printf(",\"date\":%d,\"hart\":%s,\"logs\":[", el->election_date, EeAudit_IsHart(el) ? "true" : "false");
        for (l = 0; l < EE_AUDIT_NLOGS; l++)
        {
            const EeAuditLog *log = &el->logs[l];
            if (log->ncopies == 0)
            {
                continue;
            }
            printf("%s{\"log\":", first ? "" : ",");
            first = 0;
            j_str(EeAudit_LogName(log->kind));
            printf(",\"events\":%u,\"copies\":[", log->nevents);
            for (i = 0; i < log->ncopies; i++)
            {
                const EeAuditCopy *c = &log->copies[i];
                char b[48];
                EeAudit_FormatTime(c->printed, b, sizeof b);
                printf("%s{\"file\":", i ? "," : "");
                j_str(base_name(c->path));
                printf(",\"pages\":%u,\"events\":%u,\"missing_rows\":%u,\"printed\":", c->pages, c->nevents,
                       c->missing_rows);
                j_astr(b);
                if (c->nevents > 0)
                {
                    EeAudit_FormatTime(c->events[0].ts, b, sizeof b);
                    printf(",\"newest\":");
                    j_astr(b);
                }
                printf("}");
            }
            printf("]}");
        }
        printf("]");
        if (EeAudit_ElectionAccounting(el, &a))
        {
            printf(",\"batches\":%u,\"sessions\":%u", a.nbatches, a.nsessions);
            if (a.election_session >= 0 && (uint32_t)a.election_session < a.nsessions)
            {
                const EeAuditSession *s = &a.sessions[a.election_session];
                printf(",\"official_ballots\":%lld,\"official_batches\":%u,\"official_task\":", (long long)s->ballots,
                       s->counted);
                j_astr(s->name);
            }
            printf(",\"transfers\":%u,\"transfers_not_matched\":", a.ntransfers);
            {
                uint32_t bad = 0;
                for (i = 0; i < a.ntransfers; i++)
                {
                    bad += a.transfers[i].status != EE_AUDIT_XFER_MATCHED;
                }
                printf("%u", bad);
            }
            EeAudit_FreeAccounting(&a);
        }
        if (EeAudit_Notable(el, &notes))
        {
            int c, sep = 0;
            printf(",\"notes\":{");
            for (c = 0; c < EE_AUDIT_N_COUNT; c++)
            {
                if (notes.per_category[c] > 0)
                {
                    printf("%s", sep ? "," : "");
                    sep = 1;
                    j_str(EeAudit_NoteName(c));
                    printf(":{\"count\":%u,\"severity\":%d}", notes.per_category[c], EeAudit_NoteSeverity(c));
                }
            }
            printf("}");
            EeAudit_FreeNotes(&notes);
        }
        printf("}");
    }
    printf("]}\n");
    EeAudit_FreeElections(els, nels);
    return 0;
}

static int do_voters(const wchar_t *path)
{
    EeVoterTable t;
    wchar_t err[1024] = L"";
    DWORD t0 = GetTickCount();
    EeLoadStatus st;
    EeVoterTable_Init(&t);
    st = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (st != EeLoadStatus_Ok)
    {
        EeVoterTable_Clear(&t);
        return fail(L"voters", st, err, t0);
    }
    printf("{\"mode\":\"voters\",\"ok\":true,\"ms\":%lu,\"rows\":%u,\"cols\":%u}\n", GetTickCount() - t0, t.row_count,
           t.column_count);
    EeVoterTable_Clear(&t);
    return 0;
}

/* The CVR side of a compare: contest totals of one or more CVR tables (a primary's parties
 * may come as separate CVRs with different layouts), tabulated as the CVR window does. */
typedef struct CvrSide
{
    EeResults res;
    int64_t ballots;
    uint32_t rows;
} CvrSide;

static BOOL cvr_side(EeCvrTable *tabs, int ntabs, CvrSide *out)
{
    EeResultsTallyItem *items = NULL;
    EeCvrTally **tals = (EeCvrTally **)calloc((size_t)ntabs, sizeof(EeCvrTally *));
    uint32_t *ntals = (uint32_t *)calloc((size_t)ntabs, sizeof(uint32_t));
    size_t nitems = 0, total = 0;
    int i;
    uint32_t k;
    BOOL ok;
    ZeroMemory(out, sizeof *out);
    for (i = 0; i < ntabs; i++)
    {
        EeCvrCardCounts cc;
        EeCvr_Tabulate(&tabs[i], TRUE, &tals[i], &ntals[i]);
        total += ntals[i];
        ZeroMemory(&cc, sizeof cc);
        if (EeCvr_CardCounts(&tabs[i], NULL, 0, &cc))
        {
            out->ballots += cc.ballots;
        }
        out->rows += tabs[i].nrows;
    }
    items = (EeResultsTallyItem *)calloc(total + 1, sizeof(EeResultsTallyItem));
    for (i = 0; i < ntabs; i++)
    {
        for (k = 0; k < ntals[i]; k++)
        {
            /* contest keys are column indexes: keep the tables' contests apart */
            items[nitems].contest_key = tals[i][k].group + (uint32_t)i * 1000000u;
            items[nitems].contest = tals[i][k].contest;
            items[nitems].choice = tals[i][k].selection;
            items[nitems].votes = tals[i][k].count;
            nitems++;
        }
    }
    ok = EeResults_FromTally(items, (uint32_t)nitems, out->ballots, &out->res);
    free(items);
    for (i = 0; i < ntabs; i++)
    {
        EeCvr_FreeTally(tals[i], ntals[i]);
    }
    free(tals);
    free(ntals);
    return ok;
}

static const char *diff_name(uint8_t s)
{
    static const char *k[] = {"match", "differs", "only_results", "only_cvr", "not_reported"};
    return s < 5 ? k[s] : "?";
}

/* Compare one results group with the CVR totals: counts of the contest-total rows (no
 * precinct, no vote type) by status, and the first rows that do not match. */
static void compare_group(const EeResults *r, const CvrSide *cvr)
{
    EeResultsDiff *d = NULL;
    uint32_t nd = 0, k, shown = 0, counts[5] = {0};
    printf("{\"election\":");
    j_str(r->election_name);
    printf(",\"date\":");
    j_str(r->election_date);
    printf(",\"ballots\":%lld,\"contests\":%u", (long long)r->ballots_total, r->ncontests);
    if (!EeResults_Compare(r, NULL, NULL, &cvr->res, NULL, NULL, &d, &nd))
    {
        printf(",\"error\":\"compare failed\"}");
        return;
    }
    for (k = 0; k < nd; k++)
    {
        /* the Ballots Cast row (no contest) is left to the caller's ballot check */
        if (d[k].precinct == NULL && d[k].vote_type == NULL && d[k].contest != NULL && d[k].contest[0] != L'\0')
        {
            counts[d[k].status < 5 ? d[k].status : 0]++;
        }
    }
    printf(",\"match\":%u,\"differs\":%u,\"only_results\":%u,\"only_cvr\":%u,\"not_reported\":%u,\"examples\":[",
           counts[0], counts[1], counts[2], counts[3], counts[4]);
    /* DATA_CHECK_ALL=1 lists every contest-total row (to see how names paired) */
    BOOL all = GetEnvironmentVariableW(L"DATA_CHECK_ALL", NULL, 0) > 0;
    for (k = 0; k < nd && (all || shown < 8); k++)
    {
        if (d[k].precinct != NULL || d[k].vote_type != NULL || d[k].contest == NULL || d[k].contest[0] == L'\0' ||
            (!all && (d[k].status == EE_DIFF_MATCH || d[k].status == EE_DIFF_ONLY_B ||
                      d[k].status == EE_DIFF_NOT_REPORTED)))
        {
            continue;
        }
        printf("%s{\"status\":\"%s\",\"contest\":", shown ? "," : "", diff_name(d[k].status));
        j_str(d[k].contest);
        printf(",\"choice\":");
        j_str(d[k].choice);
        printf(",\"results\":%lld,\"cvr\":%lld}", (long long)d[k].a, (long long)d[k].b);
        shown++;
    }
    printf("]}");
    EeResults_FreeDiff(d, nd);
}

/* compare cvr-set [+ cvr-set ...] -- results-file [-- results-file ...]: the CVR sets are
 * loaded once, then each results file is loaded alone and each of its groups compared. */
static int do_compare(const wchar_t *const *args, int n)
{
    EeCvrTable tabs[8];
    CvrSide cvr;
    int ntabs = 0, i = 0, first = 1;
    wchar_t err[1024] = L"";
    DWORD t0 = GetTickCount();
    while (i < n && wcscmp(args[i], L"--") != 0 && ntabs < 8)
    {
        int j = i;
        EeLoadStatus st;
        while (j < n && wcscmp(args[j], L"--") != 0 && wcscmp(args[j], L"+") != 0)
        {
            j++;
        }
        EeCvr_Init(&tabs[ntabs]);
        st = load_cvr(args + i, j - i, &tabs[ntabs], err, ARRAYSIZE(err));
        if (st != EeLoadStatus_Ok)
        {
            return fail(L"compare", st, err, t0);
        }
        ntabs++;
        i = (j < n && wcscmp(args[j], L"+") == 0) ? j + 1 : j;
    }
    if (ntabs == 0 || !cvr_side(tabs, ntabs, &cvr))
    {
        return fail(L"compare", EeLoadStatus_Error, L"no CVR loaded or tabulation failed", t0);
    }
    printf("{\"mode\":\"compare\",\"ok\":true,\"cvr_sets\":%d,\"cvr_ballots\":%lld,\"cvr_rows\":%u,\"results\":[",
           ntabs, (long long)cvr.ballots, cvr.rows);
    while (i < n)
    {
        EeResults *res = NULL;
        uint32_t nres = 0, g;
        EeLoadStatus st;
        i++; /* the "--" */
        if (i >= n)
        {
            break;
        }
        printf("%s{\"file\":", first ? "" : ",");
        first = 0;
        j_str(base_name(args[i]));
        st = EeResults_LoadFiles(args + i, 1, &res, &nres, NULL, NULL, NULL, err, ARRAYSIZE(err));
        if (st != EeLoadStatus_Ok)
        {
            printf(",\"ok\":false,\"error\":");
            j_strn(err, 300);
        }
        else
        {
            printf(",\"ok\":true,\"groups\":[");
            for (g = 0; g < nres; g++)
            {
                printf("%s", g ? "," : "");
                compare_group(&res[g], &cvr);
            }
            printf("]");
            EeResults_FreeArray(res, nres);
        }
        printf("}");
        i++;
    }
    printf("],\"ms\":%lu}\n", GetTickCount() - t0);
    EeResults_Clear(&cvr.res);
    for (i = 0; i < ntabs; i++)
    {
        EeCvr_Clear(&tabs[i]);
    }
    return 0;
}

int wmain(int argc, wchar_t **argv)
{
    const wchar_t *const *files = (const wchar_t *const *)(argv + 2);
    int n = argc - 2;
    if (argc < 3)
    {
        fwprintf(stderr, L"usage: data_check roster|cvr|results|audit|voters|compare files...\n");
        return 2;
    }
    if (wcscmp(argv[1], L"roster") == 0)
    {
        return do_roster(files, n);
    }
    if (wcscmp(argv[1], L"cvr") == 0)
    {
        return do_cvr(files, n);
    }
    if (wcscmp(argv[1], L"results") == 0)
    {
        return do_results(files, n);
    }
    if (wcscmp(argv[1], L"audit") == 0)
    {
        return do_audit(files, n);
    }
    if (wcscmp(argv[1], L"voters") == 0)
    {
        return do_voters(argv[2]);
    }
    if (wcscmp(argv[1], L"compare") == 0)
    {
        return do_compare(files, n);
    }
    fwprintf(stderr, L"usage: data_check roster|cvr|results|audit|voters|compare files...\n");
    return 2;
}
