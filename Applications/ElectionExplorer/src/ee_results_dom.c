/**
 * @file ee_results_dom.c
 * @brief Dominion Democracy Suite results reports (Election Summary Report as XML, XLSX or
 *        PDF; Statement of Votes Cast PDF) read into EeRpdfDoc. See ee_results_dom.h.
 */

#include "ee_results_dom.h"
#include "xlsx.h"

#include "third_party/miniz/miniz.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <wctype.h>
#include <strsafe.h>

#define RD_MAX_VT 32 /* counting groups kept */

/* -------------------------------------------------------------------------- */
/* Building the document                                                      */
/* -------------------------------------------------------------------------- */

typedef struct RdCx
{
    EeRpdfDoc *d;
    BOOL oom;
    int64_t bvt[RD_MAX_VT]; /* summary: ballots cast by vote type (EE_RPDF_UNKNOWN if absent) */
    int64_t *votes;         /* per contest: its "Total Votes", for working out Vote For */
    uint32_t cap_votes;
    uint32_t pre_hint;
    uint64_t protected_cells; /* "****" counts no Total row gives */
    uint64_t withheld_cells;  /* precinct counts whose vote types are "****": in "Withheld" */
    uint32_t writeins;        /* qualified write-in names, counted in Write-in */
    uint32_t party_lines;     /* contest titles whose second line (the party) was dropped */
} RdCx;

static void rd_set_err(wchar_t *err, size_t cch, const wchar_t *msg)
{
    if (err != NULL && cch > 0)
    {
        StringCchCopyW(err, cch, msg);
    }
}

static BOOL rd_grow(void **arr, uint32_t *cap, uint32_t need, size_t elem)
{
    if (need > *cap)
    {
        uint32_t nc = *cap != 0 ? *cap : 16;
        void *p;
        while (nc < need)
        {
            nc *= 2;
        }
        p = realloc(*arr, (size_t)nc * elem);
        if (p == NULL)
        {
            return FALSE;
        }
        *arr = p;
        *cap = nc;
    }
    return TRUE;
}

static BOOL rd_growz(void **arr, size_t *cap, size_t need, size_t elem)
{
    if (need > *cap)
    {
        size_t nc = *cap != 0 ? *cap : 256;
        void *p;
        while (nc < need)
        {
            nc *= 2;
        }
        p = realloc(*arr, nc * elem);
        if (p == NULL)
        {
            return FALSE;
        }
        *arr = p;
        *cap = nc;
    }
    return TRUE;
}

/* Collapse whitespace (line breaks, tabs, no-break spaces) to single spaces and trim. */
static void rd_norm(wchar_t *s)
{
    wchar_t *w = s;
    const wchar_t *r = s;
    BOOL space = FALSE;
    while (*r != L'\0')
    {
        wchar_t c = *r++;
        if (c == L' ' || c == L'\t' || c == L'\r' || c == L'\n' || c == 0xA0)
        {
            space = w != s;
            continue;
        }
        if (space)
        {
            *w++ = L' ';
            space = FALSE;
        }
        *w++ = c;
    }
    *w = L'\0';
}

/* @p n bytes of UTF-8 as a malloc'd, normalized wide string (NULL out of memory). */
static wchar_t *rd_u8w(const char *s, size_t n)
{
    int wn = n > 0 ? MultiByteToWideChar(CP_UTF8, 0, s, (int)n, NULL, 0) : 0;
    wchar_t *w = (wchar_t *)malloc(((size_t)wn + 1) * sizeof(wchar_t));
    if (w == NULL)
    {
        return NULL;
    }
    if (wn > 0)
    {
        MultiByteToWideChar(CP_UTF8, 0, s, (int)n, w, wn);
    }
    w[wn] = L'\0';
    rd_norm(w);
    return w;
}

static wchar_t *rd_wdup(const wchar_t *s)
{
    size_t n = wcslen(s) + 1;
    wchar_t *w = (wchar_t *)malloc(n * sizeof(wchar_t));
    if (w != NULL)
    {
        memcpy(w, s, n * sizeof(wchar_t));
    }
    return w;
}

static void rd_note(RdCx *cx, const wchar_t *text)
{
    EeRpdfDoc *d = cx->d;
    if (!rd_grow((void **)&d->notes, &d->cap_notes, d->nnotes + 1, sizeof(wchar_t *)) ||
        (d->notes[d->nnotes] = rd_wdup(text)) == NULL)
    {
        cx->oom = TRUE;
        return;
    }
    d->nnotes++;
}

static uint32_t rd_vt(RdCx *cx, const wchar_t *name)
{
    EeRpdfDoc *d = cx->d;
    uint32_t i;
    for (i = 0; i < d->nvt; i++)
    {
        if (_wcsicmp(d->vote_types[i], name) == 0)
        {
            return i;
        }
    }
    if (d->nvt >= RD_MAX_VT ||
        !rd_grow((void **)&d->vote_types, &d->cap_vt, d->nvt + 1, sizeof(wchar_t *)) ||
        (d->vote_types[d->nvt] = rd_wdup(name)) == NULL)
    {
        cx->oom = cx->oom || d->nvt < RD_MAX_VT;
        return UINT32_MAX;
    }
    cx->bvt[d->nvt] = EE_RPDF_UNKNOWN;
    return d->nvt++;
}

static uint32_t rd_pre(RdCx *cx, const wchar_t *name)
{
    EeRpdfDoc *d = cx->d;
    uint32_t i;
    if (cx->pre_hint < d->nprecincts && wcscmp(d->precincts[cx->pre_hint].name, name) == 0)
    {
        return cx->pre_hint;
    }
    for (i = 0; i < d->nprecincts; i++)
    {
        if (wcscmp(d->precincts[i].name, name) == 0)
        {
            return cx->pre_hint = i;
        }
    }
    if (!rd_grow((void **)&d->precincts, &d->cap_pre, d->nprecincts + 1, sizeof(EeRpdfPrecinct)) ||
        (d->precincts[d->nprecincts].name = rd_wdup(name)) == NULL)
    {
        cx->oom = TRUE;
        return UINT32_MAX;
    }
    d->precincts[d->nprecincts].registered = EE_RPDF_UNKNOWN;
    d->precincts[d->nprecincts].ballots = EE_RPDF_UNKNOWN;
    return cx->pre_hint = d->nprecincts++;
}

static uint32_t rd_contest(RdCx *cx, const wchar_t *title, uint32_t vote_for)
{
    EeRpdfDoc *d = cx->d;
    EeRpdfContest *c;
    if (!rd_grow((void **)&d->contests, &d->cap_contests, d->ncontests + 1, sizeof(EeRpdfContest)) ||
        !rd_grow((void **)&cx->votes, &cx->cap_votes, d->ncontests + 1, sizeof(int64_t)))
    {
        cx->oom = TRUE;
        return UINT32_MAX;
    }
    c = &d->contests[d->ncontests];
    ZeroMemory(c, sizeof(*c));
    c->title = rd_wdup(title);
    if (c->title == NULL)
    {
        cx->oom = TRUE;
        return UINT32_MAX;
    }
    c->vote_for = vote_for;
    c->over = EE_RPDF_UNKNOWN;
    c->under = EE_RPDF_UNKNOWN;
    c->ballots = EE_RPDF_UNKNOWN;
    cx->votes[d->ncontests] = EE_RPDF_UNKNOWN;
    return d->ncontests++;
}

/* The choice @p name of contest @p ci, added when new. */
static uint32_t rd_choice(RdCx *cx, uint32_t ci, const wchar_t *name, const wchar_t *party, int64_t total)
{
    EeRpdfContest *c = &cx->d->contests[ci];
    EeRpdfChoice *ch;
    uint32_t k;
    for (k = 0; k < c->nchoices; k++)
    {
        if (wcscmp(c->choices[k].name, name) == 0)
        {
            if (total != EE_RPDF_UNKNOWN)
            {
                c->choices[k].total = total;
            }
            return k;
        }
    }
    if (!rd_grow((void **)&c->choices, &c->cap_choices, c->nchoices + 1, sizeof(EeRpdfChoice)))
    {
        cx->oom = TRUE;
        return UINT32_MAX;
    }
    ch = &c->choices[c->nchoices];
    ch->name = rd_wdup(name);
    ch->party = party != NULL && party[0] != L'\0' ? rd_wdup(party) : NULL;
    ch->total = total;
    if (ch->name == NULL)
    {
        cx->oom = TRUE;
        return UINT32_MAX;
    }
    return c->nchoices++;
}

static void rd_count(RdCx *cx, uint32_t ci, uint32_t choice, uint32_t pre, uint32_t vt, int64_t votes)
{
    EeRpdfDoc *d = cx->d;
    EeRpdfCount *e;
    if (!rd_growz((void **)&d->counts, &d->cap_counts, d->ncounts + 1, sizeof(EeRpdfCount)))
    {
        cx->oom = TRUE;
        return;
    }
    e = &d->counts[d->ncounts++];
    e->contest = ci;
    e->choice = choice;
    e->pre = pre;
    e->vt = vt;
    e->votes = votes;
    if (votes == EE_RPDF_PROTECTED)
    {
        cx->protected_cells++;
    }
}

static void rd_turnout(RdCx *cx, uint32_t pre, uint32_t vt, int64_t ballots)
{
    EeRpdfDoc *d = cx->d;
    EeRpdfCount *e;
    if (!rd_growz((void **)&d->turnout, &d->cap_turnout, d->nturnout + 1, sizeof(EeRpdfCount)))
    {
        cx->oom = TRUE;
        return;
    }
    e = &d->turnout[d->nturnout++];
    e->contest = 0;
    e->choice = 0;
    e->pre = pre;
    e->vt = vt;
    e->votes = ballots;
}

/* "1,234" (leading and trailing spaces allowed; "1,658 / 18,047" gives 1,658); "****" is a
 * withheld count. FALSE for anything else, a percentage included. */
static BOOL rd_num_w(const wchar_t *s, int64_t *out)
{
    int64_t v = 0;
    int digits = 0;
    while (*s == L' ')
    {
        s++;
    }
    if (wcsncmp(s, L"****", 4) == 0)
    {
        *out = EE_RPDF_PROTECTED;
        return TRUE;
    }
    for (; (*s >= L'0' && *s <= L'9') || *s == L','; s++)
    {
        if (*s != L',')
        {
            v = v * 10 + (*s - L'0');
            if (++digits > 15)
            {
                return FALSE;
            }
        }
    }
    while (*s == L' ')
    {
        s++;
    }
    if (digits == 0 || (*s != L'\0' && *s != L'/'))
    {
        return FALSE;
    }
    *out = v;
    return TRUE;
}

static BOOL rd_num_a(const char *s, int64_t *out)
{
    wchar_t w[64];
    if (MultiByteToWideChar(CP_UTF8, 0, s, -1, w, ARRAYSIZE(w)) <= 0)
    {
        return FALSE;
    }
    return rd_num_w(w, out);
}

/* A long date anywhere in @p s ("November 5, 2024"): writes "11/5/2024" to @p out, and the
 * span it takes to *at / *len. FALSE when there is none. */
static BOOL rd_long_date(const wchar_t *s, wchar_t *out, size_t cch, size_t *at, size_t *len)
{
    static const wchar_t *const months[] = {L"January", L"February", L"March",     L"April",   L"May",      L"June",
                                            L"July",    L"August",   L"September", L"October", L"November", L"December"};
    int m;
    for (m = 0; m < 12; m++)
    {
        const wchar_t *p = s;
        size_t k = wcslen(months[m]);
        while ((p = wcsstr(p, months[m])) != NULL)
        {
            const wchar_t *q = p + k;
            int d = 0, y = 0, nd = 0, ny = 0;
            if (*q == L' ')
            {
                q++;
                while (*q >= L'0' && *q <= L'9' && nd < 2)
                {
                    d = d * 10 + (*q++ - L'0');
                    nd++;
                }
                if (nd > 0 && *q == L',' && q[1] == L' ')
                {
                    q += 2;
                    while (*q >= L'0' && *q <= L'9' && ny < 4)
                    {
                        y = y * 10 + (*q++ - L'0');
                        ny++;
                    }
                    if (ny == 4 && d >= 1 && d <= 31 && !(*q >= L'0' && *q <= L'9'))
                    {
                        StringCchPrintfW(out, cch, L"%d/%d/%d", m + 1, d, y);
                        *at = (size_t)(p - s);
                        *len = (size_t)(q - p);
                        return TRUE;
                    }
                }
            }
            p += k;
        }
    }
    return FALSE;
}

/* The report's title lines ("City and County of San Francisco", "November 5, 2024 Consolidated
 * General Election"; or "General Election", "DeKalb", "March 10, 2026"): the election name,
 * date and region. Report names and status lines are skipped. */
static void rd_title_line(RdCx *cx, const wchar_t *line)
{
    EeRpdfDoc *d = cx->d;
    wchar_t date[32], rest[512];
    size_t at = 0, len = 0, n = wcslen(line);
    if (n == 0 || _wcsicmp(line, L"Election Summary Report") == 0 || _wcsicmp(line, L"Statement of Votes Cast") == 0 ||
        _wcsnicmp(line, L"Summary for:", 12) == 0 || _wcsnicmp(line, L"SOVC for:", 9) == 0 ||
        _wcsnicmp(line, L"Official", 8) == 0 || _wcsnicmp(line, L"Unofficial", 10) == 0 ||
        (n > 7 && _wcsicmp(line + n - 6, L"Report") == 0))
    {
        return;
    }
    if (rd_long_date(line, date, ARRAYSIZE(date), &at, &len))
    {
        if (d->election_date == NULL)
        {
            d->election_date = rd_wdup(date);
        }
        StringCchCopyNW(rest, ARRAYSIZE(rest), line, at);
        StringCchCatW(rest, ARRAYSIZE(rest), line + at + len);
        rd_norm(rest);
        while (rest[0] == L',' || rest[0] == L' ')
        {
            memmove(rest, rest + 1, wcslen(rest) * sizeof(wchar_t));
        }
        if (wcslen(rest) > 3)
        {
            /* "November 5, 2024 Consolidated General Election": the name, date included; a
             * line before it ("City and County of San Francisco") is the region */
            if (d->election_name != NULL && d->region == NULL)
            {
                d->region = d->election_name;
            }
            else
            {
                free(d->election_name);
            }
            d->election_name = rd_wdup(line);
        }
        return;
    }
    if (d->election_name == NULL)
    {
        d->election_name = rd_wdup(line);
    }
    else if (d->region == NULL)
    {
        d->region = rd_wdup(line);
    }
}

/* A contest title of the XML or workbook: cut at a line break (the party follows it in a
 * primary: "DEM CCC DISTRICT 17\r\nDEM"). */
static void rd_contest_title(RdCx *cx, wchar_t *t)
{
    wchar_t *br = wcspbrk(t, L"\r\n");
    if (br != NULL)
    {
        *br = L'\0';
        cx->party_lines++;
    }
    rd_norm(t);
}

/* Vote For from a contest's own figures: every ballot gives Vote For votes, undervotes or
 * overvotes, so (votes + undervotes + overvotes) / ballots is a whole number. */
static void rd_vote_for(RdCx *cx)
{
    EeRpdfDoc *d = cx->d;
    uint32_t c, k;
    for (c = 0; c < d->ncontests; c++)
    {
        EeRpdfContest *ct = &d->contests[c];
        int64_t votes = cx->votes[c];
        if (ct->vote_for != 0 || ct->ballots == EE_RPDF_UNKNOWN || ct->ballots <= 0 || ct->under == EE_RPDF_UNKNOWN ||
            ct->over == EE_RPDF_UNKNOWN)
        {
            continue;
        }
        if (votes == EE_RPDF_UNKNOWN)
        {
            votes = 0;
            for (k = 0; k < ct->nchoices; k++)
            {
                votes += ct->choices[k].total != EE_RPDF_UNKNOWN ? ct->choices[k].total : 0;
            }
        }
        if ((votes + ct->under + ct->over) % ct->ballots == 0)
        {
            ct->vote_for = (uint32_t)((votes + ct->under + ct->over) / ct->ballots);
        }
    }
}

/* Qualified write-in candidates are listed by name (party "WRITE-IN"), as the CVR names them;
 * the contest's "Write-in" row counts them too, so it keeps only the other write-ins (and
 * goes when there are none). */
static void rd_writeins(RdCx *cx)
{
    EeRpdfDoc *d = cx->d;
    uint32_t c, k;
    for (c = 0; c < d->ncontests; c++)
    {
        EeRpdfContest *ct = &d->contests[c];
        uint32_t wi = UINT32_MAX;
        int64_t named = 0;
        for (k = 0; k < ct->nchoices; k++)
        {
            const EeRpdfChoice *ch = &ct->choices[k];
            if (ch->party != NULL && wcscmp(ch->party, L"WRITE-IN") == 0)
            {
                named += ch->total != EE_RPDF_UNKNOWN ? ch->total : 0;
            }
            else if (_wcsicmp(ch->name, L"Write-in") == 0)
            {
                wi = k;
            }
        }
        if (wi == UINT32_MAX || named == 0 || ct->choices[wi].total == EE_RPDF_UNKNOWN || ct->choices[wi].total < named)
        {
            continue;
        }
        ct->choices[wi].total -= named;
        if (ct->choices[wi].total == 0)
        {
            free(ct->choices[wi].name);
            free(ct->choices[wi].party);
            memmove(&ct->choices[wi], &ct->choices[wi + 1], (ct->nchoices - wi - 1) * sizeof(EeRpdfChoice));
            ct->nchoices--;
        }
    }
}

/* Common end: ballots by vote type, notes. */
static BOOL rd_finish(RdCx *cx, int kind, wchar_t *err, size_t errcch)
{
    EeRpdfDoc *d = cx->d;
    wchar_t msg[300];
    uint32_t k;
    if (!cx->oom && d->ncontests == 0)
    {
        rd_set_err(err, errcch, L"No contests were found in the Dominion report.");
        return FALSE;
    }
    d->kind = kind;
    if (kind == EE_RPDF_DOM_SUMMARY && d->nvt > 0)
    {
        d->ballots_vt_total = (int64_t *)malloc(d->nvt * sizeof(int64_t));
        if (d->ballots_vt_total == NULL)
        {
            cx->oom = TRUE;
        }
        else
        {
            for (k = 0; k < d->nvt; k++)
            {
                d->ballots_vt_total[k] = cx->bvt[k];
            }
        }
    }
    rd_vote_for(cx);
    rd_writeins(cx);
    if (cx->writeins > 0)
    {
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"%u qualified write-in candidates are listed by name (party WRITE-IN); Write-in "
                         L"holds the other write-ins.",
                         cx->writeins);
        rd_note(cx, msg);
    }
    if (cx->withheld_cells > 0)
    {
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"%llu precinct counts are not printed by vote type (\"****\": too few voters to keep "
                         L"their votes private). The precinct's Total gives each whole count, and the votes "
                         L"whose vote type is withheld are under the vote type \"Withheld\".",
                         (unsigned long long)cx->withheld_cells);
        rd_note(cx, msg);
    }
    if (cx->oom)
    {
        rd_set_err(err, errcch, L"Out of memory reading the Dominion report.");
        return FALSE;
    }
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Election Summary Report: XML                                               */
/* -------------------------------------------------------------------------- */

BOOL EeRdom_IsSummaryXml(const void *data, size_t size)
{
    const char *s = (const char *)data;
    size_t n = size < 2048 ? size : 2048, i;
    for (i = 0; i + 24 <= n; i++)
    {
        if (s[i] == 'E' && memcmp(s + i, "ElectionSummaryReportRPT", 24) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* An attribute's value in the tag [tag, end): its start, and its length in *vlen. */
static const char *rd_attr(const char *tag, const char *end, const char *name, size_t *vlen)
{
    size_t nl = strlen(name);
    const char *p;
    for (p = tag + 1; p + nl + 2 <= end; p++)
    {
        if (*p == name[0] && (p[-1] == ' ' || p[-1] == '\t' || p[-1] == '\n' || p[-1] == '\r') &&
            memcmp(p, name, nl) == 0 && p[nl] == '=' && p[nl + 1] == '"')
        {
            const char *v = p + nl + 2;
            const char *q = (const char *)memchr(v, '"', (size_t)(end - v));
            if (q == NULL)
            {
                return NULL;
            }
            *vlen = (size_t)(q - v);
            return v;
        }
    }
    return NULL;
}

static void rd_put_utf8(char *out, size_t *o, size_t cap, uint32_t cp)
{
    char b[4];
    size_t n, i;
    if (cp < 0x80)
    {
        b[0] = (char)cp;
        n = 1;
    }
    else if (cp < 0x800)
    {
        b[0] = (char)(0xC0 | (cp >> 6));
        b[1] = (char)(0x80 | (cp & 0x3F));
        n = 2;
    }
    else if (cp < 0x10000)
    {
        b[0] = (char)(0xE0 | (cp >> 12));
        b[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        b[2] = (char)(0x80 | (cp & 0x3F));
        n = 3;
    }
    else
    {
        b[0] = (char)(0xF0 | (cp >> 18));
        b[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
        b[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
        b[3] = (char)(0x80 | (cp & 0x3F));
        n = 4;
    }
    for (i = 0; i < n && *o + 1 < cap; i++)
    {
        out[(*o)++] = b[i];
    }
}

/* An attribute value with its entities decoded, as a malloc'd normalized wide string (an
 * empty string when absent; NULL out of memory). Line breaks are kept as '\n' when
 * @p keep_breaks, for the title's lines. */
static wchar_t *rd_attr_w(const char *tag, const char *end, const char *name, BOOL keep_breaks)
{
    size_t vlen = 0, i, o = 0;
    const char *v = rd_attr(tag, end, name, &vlen);
    char *buf;
    wchar_t *w;
    if (v == NULL)
    {
        vlen = 0;
    }
    buf = (char *)malloc(vlen + 1);
    if (buf == NULL)
    {
        return NULL;
    }
    for (i = 0; i < vlen;)
    {
        if (v[i] == '&')
        {
            const char *semi = (const char *)memchr(v + i, ';', vlen - i);
            size_t el = semi != NULL ? (size_t)(semi - (v + i)) : 0;
            uint32_t cp = 0;
            BOOL ok = TRUE;
            if (el == 3 && memcmp(v + i, "&lt", 3) == 0)
                cp = '<';
            else if (el == 3 && memcmp(v + i, "&gt", 3) == 0)
                cp = '>';
            else if (el == 4 && memcmp(v + i, "&amp", 4) == 0)
                cp = '&';
            else if (el == 5 && memcmp(v + i, "&quot", 5) == 0)
                cp = '"';
            else if (el == 5 && memcmp(v + i, "&apos", 5) == 0)
                cp = '\'';
            else if (el >= 3 && v[i + 1] == '#')
            {
                size_t j = i + 2;
                BOOL hex = v[j] == 'x' || v[j] == 'X';
                for (j += hex ? 1 : 0; j < i + el; j++)
                {
                    char c = v[j];
                    int dv = c >= '0' && c <= '9' ? c - '0'
                             : hex && c >= 'a' && c <= 'f' ? c - 'a' + 10
                             : hex && c >= 'A' && c <= 'F' ? c - 'A' + 10
                                                            : -1;
                    if (dv < 0 || cp > 0x10FFFF)
                    {
                        ok = FALSE;
                        break;
                    }
                    cp = cp * (hex ? 16u : 10u) + (uint32_t)dv;
                }
            }
            else
            {
                ok = FALSE;
            }
            if (ok && el > 0)
            {
                rd_put_utf8(buf, &o, vlen + 1, cp);
                i += el + 1;
                continue;
            }
        }
        buf[o++] = v[i++];
    }
    buf[o] = '\0';
    if (keep_breaks)
    {
        int wn = MultiByteToWideChar(CP_UTF8, 0, buf, (int)o + 1, NULL, 0);
        w = (wchar_t *)malloc(((size_t)wn + 1) * sizeof(wchar_t));
        if (w != NULL)
        {
            MultiByteToWideChar(CP_UTF8, 0, buf, (int)o + 1, w, wn);
        }
    }
    else
    {
        w = rd_u8w(buf, o);
    }
    free(buf);
    return w;
}

/* A counting group's voters (summary turnout). */
typedef struct RdVtRow
{
    wchar_t name[96];
    int64_t voters;
} RdVtRow;

typedef struct RdTurnout
{
    RdVtRow rows[RD_MAX_VT];
    uint32_t nrows;
    int64_t voters, registered;
    BOOL seen;
} RdTurnout;

static void rd_turnout_row(RdTurnout *t, const wchar_t *name, int64_t voters)
{
    if (t->nrows < RD_MAX_VT)
    {
        StringCchCopyW(t->rows[t->nrows].name, ARRAYSIZE(t->rows[t->nrows].name), name);
        t->rows[t->nrows].voters = voters;
        t->nrows++;
    }
}

/* The election's turnout (the elector group "Total", or the only group): ballots cast,
 * registered voters and ballots by vote type. */
static void rd_apply_turnout(RdCx *cx, const RdTurnout *t)
{
    uint32_t k;
    if (!t->seen)
    {
        return;
    }
    cx->d->ballots_total = t->voters;
    cx->d->registered_total = t->registered;
    for (k = 0; k < t->nrows; k++)
    {
        uint32_t vt;
        if (_wcsicmp(t->rows[k].name, L"Total") == 0)
        {
            continue;
        }
        vt = rd_vt(cx, t->rows[k].name);
        if (vt != UINT32_MAX)
        {
            cx->bvt[vt] = t->rows[k].voters;
        }
    }
}

/* A number with thousands separators at @p p ("206,122"); EE_RPDF_UNKNOWN when none. */
static int64_t rd_grouped(const wchar_t *p)
{
    int64_t v = 0;
    int digits = 0;
    while (*p == L' ')
    {
        p++;
    }
    for (; (*p >= L'0' && *p <= L'9') || (*p == L',' && digits > 0); p++)
    {
        if (*p != L',')
        {
            v = v * 10 + (*p - L'0');
            digits++;
        }
    }
    return digits > 0 ? v : EE_RPDF_UNKNOWN;
}

/* "Voters Cast: 412231 of 522265 (0.789...)", or before 2020 "Registered Voters: 206,122 of
 * 495,050 (41.64%)": the voters who cast a ballot, of the registered voters. */
static BOOL rd_voters_of(const wchar_t *s, int64_t *voters, int64_t *registered)
{
    const wchar_t *p;
    if (_wcsnicmp(s, L"Voters Cast:", 12) != 0 && _wcsnicmp(s, L"Registered Voters:", 18) != 0)
    {
        return FALSE;
    }
    p = wcschr(s, L':');
    *voters = rd_grouped(p + 1);
    p = wcsstr(p, L" of ");
    if (p != NULL)
    {
        *registered = rd_grouped(p + 4);
    }
    return *voters != EE_RPDF_UNKNOWN;
}

BOOL EeRdom_ParseSummaryXml(const void *data, size_t size, EeRpdfDoc *out, wchar_t *err, size_t errcch)
{
    const char *s = (const char *)data;
    const char *end = s + size;
    const char *p = s;
    RdCx cx;
    RdTurnout total, first;
    int target = 0; /* turnout rows go to: 1 the "Total" group, 2 the first group, 0 nowhere */
    uint32_t ngroups = 0;
    int64_t fb_voters = EE_RPDF_UNKNOWN, fb_registered = EE_RPDF_UNKNOWN;
    uint32_t ci = UINT32_MAX, ch = UINT32_MAX;
    wchar_t *pending_wi = NULL; /* a qualified write-in's name, until its votes */
    BOOL ok;

    ZeroMemory(out, sizeof(*out));
    out->registered_total = EE_RPDF_UNKNOWN;
    out->ballots_total = EE_RPDF_UNKNOWN;
    ZeroMemory(&cx, sizeof(cx));
    cx.d = out;
    ZeroMemory(&total, sizeof(total));
    ZeroMemory(&first, sizeof(first));
    while (p < end && !cx.oom)
    {
        const char *lt = (const char *)memchr(p, '<', (size_t)(end - p));
        const char *gt, *nm;
        size_t nl;
        BOOL quote = FALSE;
        if (lt == NULL)
        {
            break;
        }
        for (gt = lt + 1; gt < end && (quote || *gt != '>'); gt++)
        {
            if (*gt == '"')
            {
                quote = !quote;
            }
        }
        if (gt >= end)
        {
            break;
        }
        p = gt + 1;
        if (lt[1] == '/' || lt[1] == '?' || lt[1] == '!')
        {
            continue;
        }
        nm = lt + 1;
        for (nl = 0; nm + nl < gt && nm[nl] != ' ' && nm[nl] != '/' && nm[nl] != '>' && nm[nl] != '\r' && nm[nl] != '\n';
             nl++)
        {
        }
#define RD_IS(name) (nl == sizeof(name) - 1 && memcmp(nm, name, nl) == 0)
        if (RD_IS("Report"))
        {
            size_t vl = 0;
            const char *v = rd_attr(lt, gt, "Name", &vl);
            if (v != NULL && vl == 5 && memcmp(v, "Title", 5) == 0)
            {
                wchar_t *t8 = rd_attr_w(lt, gt, "Textbox8", TRUE);
                wchar_t *t9 = rd_attr_w(lt, gt, "Textbox9", FALSE);
                wchar_t *t2 = rd_attr_w(lt, gt, "Textbox2", FALSE);
                wchar_t *t3 = rd_attr_w(lt, gt, "Textbox3", FALSE);
                if (t8 == NULL || t9 == NULL || t2 == NULL || t3 == NULL)
                {
                    cx.oom = TRUE;
                }
                else
                {
                    /* "City and County of San Francisco\nNovember 5, 2024 Consolidated General
                     * Election\nFinal Summary Report" */
                    wchar_t *ctx = NULL;
                    wchar_t *line = wcstok_s(t8, L"\r\n", &ctx);
                    while (line != NULL)
                    {
                        rd_norm(line);
                        rd_title_line(&cx, line);
                        line = wcstok_s(NULL, L"\r\n", &ctx);
                    }
                    if (out->election_name == NULL && t3[0] != L'\0')
                    {
                        out->election_name = rd_wdup(t3);
                    }
                    if (out->region == NULL && t2[0] != L'\0')
                    {
                        out->region = rd_wdup(t2);
                    }
                    if (out->election_date == NULL && wcslen(t9) >= 10 && t9[4] == L'-' && t9[7] == L'-')
                    {
                        wchar_t date[32];
                        StringCchPrintfW(date, ARRAYSIZE(date), L"%d/%d/%.4s", _wtoi(t9 + 5), _wtoi(t9 + 8), t9);
                        out->election_date = rd_wdup(date);
                    }
                }
                free(t8);
                free(t9);
                free(t2);
                free(t3);
            }
        }
        else if (RD_IS("electorGroupId2"))
        {
            wchar_t *g = rd_attr_w(lt, gt, "electorGroupId2", FALSE);
            size_t vl = 0;
            const char *b3 = rd_attr(lt, gt, "ballots3", &vl);
            const char *reg = rd_attr(lt, gt, "Textbox32", &vl);
            RdTurnout *t;
            if (g == NULL)
            {
                cx.oom = TRUE;
                break;
            }
            ngroups++;
            target = _wcsicmp(g, L"Total") == 0 ? 1 : (ngroups == 1 ? 2 : 0);
            free(g);
            t = target == 1 ? &total : (target == 2 ? &first : NULL);
            if (t != NULL)
            {
                t->seen = TRUE;
                t->voters = b3 != NULL ? _atoi64(b3) : EE_RPDF_UNKNOWN;
                t->registered = reg != NULL && reg[0] != '"' ? _atoi64(reg) : EE_RPDF_UNKNOWN;
            }
        }
        else if (RD_IS("Details1"))
        {
            RdTurnout *t = target == 1 ? &total : (target == 2 ? &first : NULL);
            wchar_t *g = rd_attr_w(lt, gt, "countingGroup1", FALSE);
            size_t vl = 0;
            const char *v = rd_attr(lt, gt, "Textbox171", &vl);
            if (g == NULL)
            {
                cx.oom = TRUE;
                break;
            }
            if (t != NULL && g[0] != L'\0' && v != NULL)
            {
                rd_turnout_row(t, g, _atoi64(v));
            }
            free(g);
        }
        else if (RD_IS("Details2"))
        {
            wchar_t *r3 = rd_attr_w(lt, gt, "reported3", FALSE);
            if (r3 == NULL)
            {
                cx.oom = TRUE;
                break;
            }
            rd_voters_of(r3, &fb_voters, &fb_registered);
            free(r3);
        }
        else if (RD_IS("ContestIdGroup"))
        {
            wchar_t *t = rd_attr_w(lt, gt, "contestId", TRUE);
            if (t == NULL)
            {
                cx.oom = TRUE;
                break;
            }
            rd_contest_title(&cx, t);
            if (t[0] != L'\0')
            {
                ci = rd_contest(&cx, t, 0);
                ch = UINT32_MAX;
                free(pending_wi);
                pending_wi = NULL;
            }
            free(t);
        }
        else if (RD_IS("writeinGroup2"))
        {
            /* a qualified write-in candidate: its votes follow (votes2) */
            free(pending_wi);
            pending_wi = rd_attr_w(lt, gt, "name", FALSE);
            if (pending_wi == NULL)
            {
                cx.oom = TRUE;
                break;
            }
            cx.writeins++;
        }
        else if (ci != UINT32_MAX && !RD_IS("electorGroupId2"))
        {
            EeRpdfContest *ct = &out->contests[ci];
            size_t vl = 0;
            const char *v;
            if ((v = rd_attr(lt, gt, "ballots2", &vl)) != NULL && ct->ballots == EE_RPDF_UNKNOWN)
            {
                ct->ballots = _atoi64(v); /* "412121 / 522265 " */
            }
            if ((v = rd_attr(lt, gt, "undervotes3", &vl)) != NULL)
            {
                ct->under = _atoi64(v);
                ct->has_over_under = TRUE;
            }
            if ((v = rd_attr(lt, gt, "overvotes2", &vl)) != NULL)
            {
                ct->over = _atoi64(v);
                ct->has_over_under = TRUE;
            }
            if ((v = rd_attr(lt, gt, "votes3", &vl)) != NULL)
            {
                cx.votes[ci] = _atoi64(v);
            }
            if ((v = rd_attr(lt, gt, "votes2", &vl)) != NULL && pending_wi != NULL)
            {
                if (pending_wi[0] != L'\0')
                {
                    rd_choice(&cx, ci, pending_wi, L"WRITE-IN", _atoi64(v));
                }
                free(pending_wi);
                pending_wi = NULL;
            }
            if (RD_IS("candidateNameTextBox4"))
            {
                wchar_t *name = rd_attr_w(lt, gt, "candidateNameTextBox4", FALSE);
                if (name == NULL)
                {
                    cx.oom = TRUE;
                    break;
                }
                ch = name[0] != L'\0' ? rd_choice(&cx, ci, name, NULL, EE_RPDF_UNKNOWN) : UINT32_MAX;
                free(name);
            }
            else if ((v = rd_attr(lt, gt, "vot8", &vl)) != NULL && ch != UINT32_MAX &&
                     out->contests[ci].choices[ch].total == EE_RPDF_UNKNOWN)
            {
                out->contests[ci].choices[ch].total = _atoi64(v);
            }
        }
#undef RD_IS
    }
    free(pending_wi);
    if (total.seen)
    {
        rd_apply_turnout(&cx, &total);
    }
    else if (first.seen && ngroups == 1)
    {
        rd_apply_turnout(&cx, &first);
    }
    else
    {
        out->ballots_total = fb_voters;
        out->registered_total = fb_registered;
    }
    if (cx.party_lines > 0)
    {
        wchar_t msg[200];
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"%u contest titles name their party on a second line, which is not part of the title.",
                         cx.party_lines);
        rd_note(&cx, msg);
    }
    ok = rd_finish(&cx, EE_RPDF_DOM_SUMMARY, err, errcch);
    free(cx.votes);
    if (!ok)
    {
        EeRpdf_Free(out);
    }
    return ok;
}

/* -------------------------------------------------------------------------- */
/* Election Summary Report: workbook                                          */
/* -------------------------------------------------------------------------- */

BOOL EeRdom_IsSummaryXlsx(const void *data, size_t size)
{
    mz_zip_archive z;
    BOOL yes = FALSE;
    mz_zip_zero_struct(&z);
    if (size < 4 || !mz_zip_reader_init_mem(&z, data, size, 0))
    {
        return FALSE;
    }
    {
        size_t n = 0;
        char *wb = (char *)mz_zip_reader_extract_file_to_heap(&z, "xl/workbook.xml", &n, 0);
        if (wb != NULL)
        {
            size_t i;
            for (i = 0; i + 24 <= n && !yes; i++)
            {
                yes = wb[i] == 'E' && memcmp(wb + i, "ElectionSummaryReportRPT", 24) == 0;
            }
            mz_free(wb);
        }
    }
    mz_zip_reader_end(&z);
    return yes;
}

enum
{
    RX_START = 0,
    RX_TURNOUT,
    RX_STATS,
    RX_CANDIDATES,
    RX_SKIP,
    RX_WRITEINS /* the PDF's table of qualified write-in candidates */
};

typedef struct RdXlsx
{
    RdCx *cx;
    int mode;
    int col_cg, col_voters, col_reg, col_total, col_party;
    RdTurnout total, first, cur;
    uint32_t ngroups;
    BOOL cur_is_total;
    uint32_t ci;
} RdXlsx;

static BOOL rd_cell_is(const char *cell, const char *text)
{
    while (*cell == ' ')
    {
        cell++;
    }
    return _strnicmp(cell, text, strlen(text)) == 0;
}

static void rd_xl_close_group(RdXlsx *x)
{
    if (x->ngroups == 0)
    {
        return;
    }
    if (x->cur_is_total)
    {
        x->total = x->cur;
    }
    else if (x->ngroups == 1)
    {
        x->first = x->cur;
    }
}

static BOOL rd_xl_row(void *ctx, const char *const *cells, uint32_t n)
{
    RdXlsx *x = (RdXlsx *)ctx;
    RdCx *cx = x->cx;
    uint32_t i, filled = 0, firstc = UINT32_MAX;
    const char *c0 = n > 0 ? cells[0] : "";
    for (i = 0; i < n; i++)
    {
        const char *c = cells[i];
        while (*c == ' ')
        {
            c++;
        }
        if (*c != '\0')
        {
            filled++;
            if (firstc == UINT32_MAX)
            {
                firstc = i;
            }
        }
    }
    if (filled == 0 || cx->oom)
    {
        return !cx->oom;
    }
    if (rd_cell_is(c0, "Page:"))
    {
        return TRUE;
    }
    if (x->mode == RX_START && cx->d->election_name == NULL && strchr(c0, '\n') != NULL)
    {
        /* the title: "City and County of San Francisco\nNovember 5, 2024 ...\nFinal Summary Report" */
        int wn = MultiByteToWideChar(CP_UTF8, 0, c0, -1, NULL, 0);
        wchar_t *w = (wchar_t *)malloc(((size_t)wn + 1) * sizeof(wchar_t));
        if (w == NULL)
        {
            cx->oom = TRUE;
            return FALSE;
        }
        MultiByteToWideChar(CP_UTF8, 0, c0, -1, w, wn);
        {
            wchar_t *tctx = NULL;
            wchar_t *line = wcstok_s(w, L"\r\n", &tctx);
            while (line != NULL)
            {
                rd_norm(line);
                rd_title_line(cx, line);
                line = wcstok_s(NULL, L"\r\n", &tctx);
            }
        }
        free(w);
        return TRUE;
    }
    for (i = 0; i < n; i++)
    {
        if (rd_cell_is(cells[i], "Counting Group"))
        {
            uint32_t k;
            x->mode = RX_TURNOUT;
            x->col_cg = (int)i;
            x->col_voters = x->col_reg = -1;
            for (k = 0; k < n; k++)
            {
                if (_stricmp(cells[k], "Voters Cast") == 0 || _stricmp(cells[k], "Voters") == 0)
                    x->col_voters = (int)k;
                else if (_stricmp(cells[k], "Registered Voters") == 0)
                    x->col_reg = (int)k;
            }
            return TRUE;
        }
    }
    if (x->mode == RX_TURNOUT)
    {
        const char *cg = x->col_cg < (int)n ? cells[x->col_cg] : "";
        if (rd_cell_is(c0, "Precincts Reported"))
        {
            rd_xl_close_group(x);
            x->mode = RX_START;
            return TRUE;
        }
        if (c0[0] != '\0' && cg[0] == '\0')
        {
            rd_xl_close_group(x);
            ZeroMemory(&x->cur, sizeof(x->cur));
            x->cur.seen = TRUE;
            x->cur.voters = x->cur.registered = EE_RPDF_UNKNOWN;
            x->cur_is_total = rd_cell_is(c0, "Total") && c0[5] == '\0';
            x->ngroups++;
            return TRUE;
        }
        if (cg[0] != '\0' && x->col_voters >= 0 && x->col_voters < (int)n)
        {
            int64_t v = 0, reg = 0;
            wchar_t name[96];
            if (!rd_num_a(cells[x->col_voters], &v))
            {
                return TRUE;
            }
            MultiByteToWideChar(CP_UTF8, 0, cg, -1, name, ARRAYSIZE(name));
            rd_norm(name);
            if (_wcsicmp(name, L"Total") == 0)
            {
                x->cur.voters = v;
                if (x->col_reg >= 0 && x->col_reg < (int)n && rd_num_a(cells[x->col_reg], &reg))
                {
                    x->cur.registered = reg;
                }
            }
            else
            {
                rd_turnout_row(&x->cur, name, v);
            }
        }
        return TRUE;
    }
    if (rd_cell_is(c0, "Precincts Reported"))
    {
        return TRUE;
    }
    if (filled == 1 && firstc == 0 && !rd_cell_is(c0, "Total Votes") && !rd_cell_is(c0, "Candidate"))
    {
        /* a contest's title */
        int wn = MultiByteToWideChar(CP_UTF8, 0, c0, -1, NULL, 0);
        wchar_t *t = (wchar_t *)malloc(((size_t)wn + 1) * sizeof(wchar_t));
        if (t == NULL)
        {
            cx->oom = TRUE;
            return FALSE;
        }
        MultiByteToWideChar(CP_UTF8, 0, c0, -1, t, wn);
        rd_contest_title(cx, t);
        x->ci = t[0] != L'\0' ? rd_contest(cx, t, 0) : UINT32_MAX;
        free(t);
        x->mode = RX_STATS;
        x->col_total = -1;
        return !cx->oom;
    }
    if (x->ci == UINT32_MAX)
    {
        return TRUE;
    }
    if (rd_cell_is(c0, "Candidate"))
    {
        x->mode = RX_CANDIDATES;
        x->col_total = x->col_party = -1;
        for (i = 0; i < n; i++)
        {
            if (_stricmp(cells[i], "Total") == 0)
                x->col_total = (int)i;
            else if (_stricmp(cells[i], "Party") == 0)
                x->col_party = (int)i;
        }
        return TRUE;
    }
    if (x->mode == RX_STATS)
    {
        EeRpdfContest *ct = &cx->d->contests[x->ci];
        int64_t v = 0;
        if (c0[0] == '\0')
        {
            for (i = 0; i < n; i++)
            {
                if (_stricmp(cells[i], "Total") == 0)
                {
                    x->col_total = (int)i;
                }
            }
            return TRUE;
        }
        if (x->col_total < 0 || x->col_total >= (int)n || !rd_num_a(cells[x->col_total], &v))
        {
            return TRUE;
        }
        if (rd_cell_is(c0, "Undervotes"))
        {
            ct->under = v;
            ct->has_over_under = TRUE;
        }
        else if (rd_cell_is(c0, "Overvotes"))
        {
            ct->over = v;
            ct->has_over_under = TRUE;
        }
        else if (rd_cell_is(c0, "Times Cast"))
        {
            ct->ballots = v;
        }
        return TRUE;
    }
    if (x->mode == RX_CANDIDATES)
    {
        int64_t v = 0;
        wchar_t *name;
        wchar_t party[64] = L"";
        if (rd_cell_is(c0, "Total Votes"))
        {
            if (x->col_total >= 0 && x->col_total < (int)n && rd_num_a(cells[x->col_total], &v))
            {
                cx->votes[x->ci] = v;
            }
            x->mode = RX_SKIP;
            return TRUE;
        }
        if (x->col_total < 0 || x->col_total >= (int)n || !rd_num_a(cells[x->col_total], &v) || c0[0] == '\0')
        {
            return TRUE;
        }
        name = rd_u8w(c0, strlen(c0));
        if (name == NULL)
        {
            cx->oom = TRUE;
            return FALSE;
        }
        if (x->col_party >= 0 && x->col_party < (int)n)
        {
            MultiByteToWideChar(CP_UTF8, 0, cells[x->col_party], -1, party, ARRAYSIZE(party));
            rd_norm(party);
        }
        rd_choice(cx, x->ci, name, party, v);
        free(name);
        return !cx->oom;
    }
    if (x->mode == RX_SKIP)
    {
        /* the write-in table: "PETER SONSKI / LAUREN ONAK", "WRITE-IN", 33 */
        for (i = 0; i < n; i++)
        {
            int64_t v = 0;
            if (_stricmp(cells[i], "WRITE-IN") == 0 && c0[0] != '\0' && x->col_total >= 0 && x->col_total < (int)n &&
                rd_num_a(cells[x->col_total], &v))
            {
                wchar_t *name = rd_u8w(c0, strlen(c0));
                if (name == NULL)
                {
                    cx->oom = TRUE;
                    return FALSE;
                }
                rd_choice(cx, x->ci, name, L"WRITE-IN", v);
                free(name);
                cx->writeins++;
                break;
            }
        }
    }
    return TRUE;
}

static BOOL rd_xl_sheet(void *ctx, int sheet_index, const char *name_utf8)
{
    (void)ctx;
    (void)name_utf8;
    return sheet_index == 0;
}

BOOL EeRdom_ParseSummaryXlsx(const void *data, size_t size, EeRpdfDoc *out, wchar_t *err, size_t errcch)
{
    RdCx cx;
    RdXlsx x;
    wchar_t xerr[256] = L"";
    EeLoadStatus st;
    BOOL ok;
    ZeroMemory(out, sizeof(*out));
    out->registered_total = EE_RPDF_UNKNOWN;
    out->ballots_total = EE_RPDF_UNKNOWN;
    ZeroMemory(&cx, sizeof(cx));
    cx.d = out;
    ZeroMemory(&x, sizeof(x));
    x.cx = &cx;
    x.ci = UINT32_MAX;
    x.col_cg = x.col_voters = x.col_reg = x.col_total = x.col_party = -1;
    st = EeXlsx_ReadAllSheetsMem(data, size, rd_xl_sheet, rd_xl_row, &x, NULL, xerr, ARRAYSIZE(xerr));
    if (st == EeLoadStatus_Error && !cx.oom)
    {
        rd_set_err(err, errcch, xerr[0] != L'\0' ? xerr : L"The workbook could not be read.");
        EeRpdf_Free(out);
        free(cx.votes);
        return FALSE;
    }
    if (x.mode == RX_TURNOUT)
    {
        rd_xl_close_group(&x);
    }
    rd_apply_turnout(&cx, x.total.seen ? &x.total : &x.first);
    if (cx.party_lines > 0)
    {
        wchar_t msg[200];
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"%u contest titles name their party on a second line, which is not part of the title.",
                         cx.party_lines);
        rd_note(&cx, msg);
    }
    ok = rd_finish(&cx, EE_RPDF_DOM_SUMMARY, err, errcch);
    free(cx.votes);
    if (!ok)
    {
        EeRpdf_Free(out);
    }
    return ok;
}

/* -------------------------------------------------------------------------- */
/* PDF: positioned text in lines of cells                                     */
/* -------------------------------------------------------------------------- */

/* Runs on one baseline that touch each other, joined. */
typedef struct RdCell
{
    float x0, x1, y, size;
    wchar_t *text; /* normalized */
} RdCell;

typedef struct RdLine
{
    float y;
    float size; /* the largest */
    uint32_t first, n;
} RdLine;

typedef struct RdPage
{
    RdCell *cells;
    uint32_t ncells, cap_cells;
    RdLine *lines;
    uint32_t nlines, cap_lines;
    const EePdfTextRun **rot; /* runs turned 90 degrees (Statement of Votes Cast headings) */
    uint32_t nrot, cap_rot;
    const EePdfPageText *pt;
} RdPage;

static void rd_page_free(RdPage *pg)
{
    uint32_t i;
    for (i = 0; i < pg->ncells; i++)
    {
        free(pg->cells[i].text);
    }
    free(pg->cells);
    free(pg->lines);
    free((void *)pg->rot);
    ZeroMemory(pg, sizeof(*pg));
}

/* A run's advance width: from its font, or for a font that carries no widths (a standard
 * font) measured as Helvetica (AFM, 1/1000 em, ' ' through '~'). */
static float rd_run_width(const EePdfPageText *pt, const EePdfTextRun *r)
{
    static const unsigned short k_helv[95] = {
        278, 278, 355, 556, 556, 889, 667, 191, 333, 333, 389, 584, 278, 333, 278, 278, /* ' '-'/' */
        556, 556, 556, 556, 556, 556, 556, 556, 556, 556, 278, 278, 584, 584, 584, 556, /* '0'-'?' */
        1015, 667, 667, 722, 722, 667, 611, 778, 722, 278, 500, 667, 556, 833, 722, 778, /* '@'-'O' */
        667, 778, 722, 667, 611, 722, 667, 944, 667, 667, 611, 278, 278, 278, 469, 556,  /* 'P'-'_' */
        333, 556, 556, 500, 556, 556, 278, 556, 556, 222, 222, 500, 222, 833, 556, 556,  /* '`'-'o' */
        556, 556, 333, 500, 278, 556, 500, 722, 500, 500, 500, 334, 260, 334, 584};      /* 'p'-'~' */
    uint32_t i;
    float w = 0.0f;
    if (r->has_width)
    {
        return r->width;
    }
    for (i = 0; i < r->text_len; i++)
    {
        unsigned char c = (unsigned char)pt->text[r->text_off + i];
        if ((c & 0xC0) == 0x80)
        {
            continue; /* a UTF-8 continuation byte */
        }
        w += (float)(c >= ' ' && c <= '~' ? k_helv[c - ' '] : 556u) / 1000.0f * r->font_size;
    }
    return w;
}

static int rd_cmp_run(const void *pa, const void *pb)
{
    const EePdfTextRun *a = *(const EePdfTextRun *const *)pa;
    const EePdfTextRun *b = *(const EePdfTextRun *const *)pb;
    if (fabsf(a->y - b->y) > 0.6f)
    {
        return a->y > b->y ? -1 : 1;
    }
    return a->x < b->x ? -1 : (a->x > b->x ? 1 : 0);
}

/* Build @p pg's cells and lines from upright runs, top to bottom. */
static BOOL rd_page_build(RdPage *pg, const EePdfPageText *pt)
{
    const EePdfTextRun **runs;
    uint32_t i, n = 0;
    pg->pt = pt;
    runs = (const EePdfTextRun **)malloc(((size_t)pt->nruns + 1) * sizeof(*runs));
    if (runs == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < pt->nruns; i++)
    {
        const EePdfTextRun *r = &pt->runs[i];
        if (r->rot == 0)
        {
            runs[n++] = r;
        }
        else if (r->rot == 90)
        {
            if (!rd_grow((void **)&pg->rot, &pg->cap_rot, pg->nrot + 1, sizeof(*pg->rot)))
            {
                free(runs);
                return FALSE;
            }
            pg->rot[pg->nrot++] = r;
        }
    }
    qsort(runs, n, sizeof(*runs), rd_cmp_run);
    for (i = 0; i < n;)
    {
        RdLine *ln;
        uint32_t j = i;
        float y = runs[i]->y;
        if (!rd_grow((void **)&pg->lines, &pg->cap_lines, pg->nlines + 1, sizeof(RdLine)))
        {
            free(runs);
            return FALSE;
        }
        ln = &pg->lines[pg->nlines];
        ln->y = y;
        ln->size = 0.0f;
        ln->first = pg->ncells;
        ln->n = 0;
        while (j < n && fabsf(runs[j]->y - y) <= 0.6f)
        {
            /* one cell: runs that touch (a gap under 1.5 pt) at one font size */
            uint32_t k = j, start = j;
            float x1 = runs[j]->x + rd_run_width(pt, runs[j]);
            size_t len = runs[j]->text_len;
            RdCell *c;
            char *u8;
            size_t o = 0;
            while (k + 1 < n && fabsf(runs[k + 1]->y - y) <= 0.6f && runs[k + 1]->x - x1 < 1.5f &&
                   fabsf(runs[k + 1]->font_size - runs[j]->font_size) < 0.5f)
            {
                k++;
                x1 = runs[k]->x + rd_run_width(pt, runs[k]);
                len += runs[k]->text_len;
            }
            if (!rd_grow((void **)&pg->cells, &pg->cap_cells, pg->ncells + 1, sizeof(RdCell)) ||
                (u8 = (char *)malloc(len + 1)) == NULL)
            {
                free(runs);
                return FALSE;
            }
            for (; j <= k; j++)
            {
                memcpy(u8 + o, pt->text + runs[j]->text_off, runs[j]->text_len);
                o += runs[j]->text_len;
            }
            c = &pg->cells[pg->ncells];
            c->x0 = runs[start]->x;
            c->text = rd_u8w(u8, o);
            free(u8);
            if (c->text == NULL)
            {
                free(runs);
                return FALSE;
            }
            c->x1 = x1;
            c->y = y;
            c->size = runs[k]->font_size;
            pg->ncells++;
            ln->n++;
            if (c->size > ln->size)
            {
                ln->size = c->size;
            }
        }
        pg->nlines++;
        i = j;
    }
    free(runs);
    return TRUE;
}

static BOOL rd_is_value(const RdCell *c, int64_t *v)
{
    int64_t t = 0;
    BOOL ok = rd_num_w(c->text, &t);
    if (ok && v != NULL)
    {
        *v = t;
    }
    return ok;
}

static BOOL rd_is_pct(const RdCell *c)
{
    size_t n = wcslen(c->text);
    return n > 0 && c->text[n - 1] == L'%';
}

/* Table columns from a heading line: each heading's x range. */
#define RD_MAX_COLS 48

typedef struct RdCols
{
    uint32_t n;
    float x0[RD_MAX_COLS], x1[RD_MAX_COLS];
    wchar_t name[RD_MAX_COLS][128];
} RdCols;

static void rd_cols_from_line(RdCols *cols, const RdPage *pg, const RdLine *ln)
{
    uint32_t k;
    cols->n = 0;
    for (k = 0; k < ln->n && cols->n < RD_MAX_COLS; k++)
    {
        const RdCell *c = &pg->cells[ln->first + k];
        cols->x0[cols->n] = c->x0;
        cols->x1[cols->n] = c->x1;
        StringCchCopyW(cols->name[cols->n], ARRAYSIZE(cols->name[0]), c->text);
        cols->n++;
    }
}

/* The column a value cell belongs to: the heading it overlaps most, or else the one whose
 * right edge it lines up with (within 4 pt). -1 for none. */
static int rd_col_of(const RdCols *cols, float x0, float x1)
{
    int best = -1;
    float best_ov = 0.0f, best_d = 4.0f;
    uint32_t k;
    for (k = 0; k < cols->n; k++)
    {
        float lo = x0 > cols->x0[k] ? x0 : cols->x0[k];
        float hi = x1 < cols->x1[k] ? x1 : cols->x1[k];
        if (hi - lo > best_ov)
        {
            best_ov = hi - lo;
            best = (int)k;
        }
    }
    if (best >= 0)
    {
        return best;
    }
    for (k = 0; k < cols->n; k++)
    {
        float d = fabsf(x1 - cols->x1[k]);
        if (d <= best_d)
        {
            best_d = d;
            best = (int)k;
        }
    }
    return best;
}

static int rd_col_named(const RdCols *cols, const wchar_t *name)
{
    uint32_t k;
    for (k = 0; k < cols->n; k++)
    {
        if (_wcsicmp(cols->name[k], name) == 0)
        {
            return (int)k;
        }
    }
    return -1;
}

/* "Page: 1 of 16    12/2/2024 1:30:15 PM": the run time. */
static void rd_page_header(RdCx *cx, const RdPage *pg, const RdLine *ln)
{
    uint32_t k;
    if (cx->d->timestamp != NULL)
    {
        return;
    }
    for (k = 0; k < ln->n; k++)
    {
        const wchar_t *t = pg->cells[ln->first + k].text;
        if (iswdigit(t[0]) && wcschr(t, L'/') != NULL && wcschr(t, L':') != NULL)
        {
            cx->d->timestamp = rd_wdup(t);
        }
    }
}

static BOOL rd_line_starts(const RdPage *pg, const RdLine *ln, const wchar_t *text)
{
    return ln->n > 0 && _wcsnicmp(pg->cells[ln->first].text, text, wcslen(text)) == 0;
}

/* A contest's title from its heading line (14 pt at the left margin): "(Vote for  1)" gives
 * Vote For; a note at 8 pt on the same line is not part of it. */
static void rd_title_of(const RdPage *pg, const RdLine *ln, wchar_t *title, size_t cch, uint32_t *vote_for)
{
    uint32_t k;
    title[0] = L'\0';
    *vote_for = 0;
    for (k = 0; k < ln->n; k++)
    {
        const RdCell *c = &pg->cells[ln->first + k];
        const wchar_t *vf;
        if (c->size < 12.0f)
        {
            continue;
        }
        vf = wcsstr(c->text, L"(Vote for");
        if (vf == NULL)
        {
            vf = wcsstr(c->text, L"(Vote For");
        }
        if (vf != NULL)
        {
            *vote_for = (uint32_t)_wtoi(vf + 9);
            if (vf > c->text)
            {
                StringCchCatNW(title, cch, c->text, (size_t)(vf - c->text));
            }
            continue;
        }
        if (title[0] != L'\0')
        {
            StringCchCatW(title, cch, L" ");
        }
        StringCchCatW(title, cch, c->text);
    }
    rd_norm(title);
}

/* A contest heading's line: 14 pt at the left margin (the report's own title lines are
 * centred). */
static BOOL rd_is_title_line(const RdPage *pg, const RdLine *ln)
{
    return ln->n > 0 && ln->size >= 12.0f && pg->cells[ln->first].x0 < 30.0f;
}

/* The whole title of the contest heading at line *l: a long title wraps onto the next line,
 * and a primary contest names its party on a line of its own ("DEM"), which is dropped.
 * *l is left at the heading's last line. */
static void rd_gather_title(RdCx *cx, const RdPage *pg, uint32_t *l, wchar_t *title, size_t cch, uint32_t *vote_for)
{
    rd_title_of(pg, &pg->lines[*l], title, cch, vote_for);
    while (*l + 1 < pg->nlines && rd_is_title_line(pg, &pg->lines[*l + 1]) &&
           pg->lines[*l].y - pg->lines[*l + 1].y < 20.0f)
    {
        wchar_t more[512];
        uint32_t vf = 0;
        (*l)++;
        rd_title_of(pg, &pg->lines[*l], more, ARRAYSIZE(more), &vf);
        if (vf != 0)
        {
            *vote_for = vf;
        }
        if (wcslen(more) <= 5)
        {
            cx->party_lines++;
            continue;
        }
        if (more[0] != L'\0')
        {
            StringCchCatW(title, cch, L" ");
            StringCchCatW(title, cch, more);
        }
    }
}

/* -------------------------------------------------------------------------- */
/* PDF: Election Summary Report                                               */
/* -------------------------------------------------------------------------- */

/* A candidate table being read: rows with numbers and the name parts around them (a long
 * name wraps onto lines above and below its numbers). */
typedef struct RdCandRow
{
    float y;
    int64_t total;
    wchar_t name[256];
    wchar_t party[64];
    float frag_y[4];
    wchar_t frag[4][128];
    uint32_t nfrag;
} RdCandRow;

#define RD_MAX_CAND 400

typedef struct RdSum
{
    RdCx *cx;
    int mode;
    RdCols cols;
    int col_total, col_party, col_cg, col_voters, col_reg;
    float elector_x0, cg_x0;
    RdTurnout total, first, cur;
    BOOL cur_is_total;
    uint32_t ngroups;
    BOOL title_done;
    uint32_t ci;
    int64_t fb_voters, fb_registered; /* "Registered Voters: N of M" */
    RdCandRow *rows;
    uint32_t nrows;
    float frag_y[RD_MAX_CAND];
    wchar_t (*frag)[128];
    uint32_t nfrag;
} RdSum;

static void rd_sum_close_group(RdSum *s)
{
    if (s->ngroups == 0)
    {
        return;
    }
    if (s->cur_is_total)
    {
        s->total = s->cur;
    }
    else if (s->ngroups == 1)
    {
        s->first = s->cur;
    }
}

/* Attach the name parts to the nearest row with numbers and add the candidates. */
static void rd_sum_flush(RdSum *s)
{
    RdCx *cx = s->cx;
    uint32_t f, r;
    for (f = 0; f < s->nfrag; f++)
    {
        /* a name's lines are 10.6 pt apart; candidates' rows 25 pt */
        uint32_t best = UINT32_MAX;
        float bd = 12.0f;
        for (r = 0; r < s->nrows; r++)
        {
            float d = fabsf(s->rows[r].y - s->frag_y[f]);
            if (d < bd)
            {
                bd = d;
                best = r;
            }
        }
        if (best != UINT32_MAX && s->rows[best].nfrag < 4)
        {
            RdCandRow *row = &s->rows[best];
            row->frag_y[row->nfrag] = s->frag_y[f];
            StringCchCopyW(row->frag[row->nfrag], ARRAYSIZE(row->frag[0]), s->frag[f]);
            row->nfrag++;
        }
    }
    for (r = 0; r < s->nrows && s->ci != UINT32_MAX; r++)
    {
        RdCandRow *row = &s->rows[r];
        wchar_t name[512];
        uint32_t a, b;
        BOOL used_own = FALSE;
        /* top to bottom: the parts above, the row's own text, the parts below */
        for (a = 0; a < row->nfrag; a++)
        {
            for (b = a + 1; b < row->nfrag; b++)
            {
                if (row->frag_y[b] > row->frag_y[a])
                {
                    float ty = row->frag_y[a];
                    wchar_t tt[128];
                    row->frag_y[a] = row->frag_y[b];
                    row->frag_y[b] = ty;
                    StringCchCopyW(tt, ARRAYSIZE(tt), row->frag[a]);
                    StringCchCopyW(row->frag[a], ARRAYSIZE(row->frag[a]), row->frag[b]);
                    StringCchCopyW(row->frag[b], ARRAYSIZE(row->frag[b]), tt);
                }
            }
        }
        name[0] = L'\0';
        for (a = 0; a <= row->nfrag; a++)
        {
            const wchar_t *part = NULL;
            if (!used_own && (a == row->nfrag || row->frag_y[a] < row->y))
            {
                part = row->name;
                used_own = TRUE;
                a--; /* the same fragment again next */
            }
            else if (a < row->nfrag)
            {
                part = row->frag[a];
            }
            if (part != NULL && part[0] != L'\0')
            {
                size_t nl = wcslen(name);
                if (nl > 0 && name[nl - 1] != L'-')
                {
                    StringCchCatW(name, ARRAYSIZE(name), L" "); /* "MOYOWASIFZA-" "CURRY": no space */
                }
                StringCchCatW(name, ARRAYSIZE(name), part);
            }
        }
        rd_norm(name);
        if (name[0] != L'\0')
        {
            BOOL wi = s->mode == RX_WRITEINS;
            rd_choice(cx, s->ci, name, wi ? L"WRITE-IN" : row->party, row->total);
            cx->writeins += wi ? 1 : 0;
        }
    }
    s->nrows = 0;
    s->nfrag = 0;
}

static void rd_sum_turnout_line(RdSum *s, const RdPage *pg, const RdLine *ln)
{
    uint32_t k;
    const wchar_t *label = NULL;
    int64_t voters = EE_RPDF_UNKNOWN, reg = EE_RPDF_UNKNOWN;
    BOOL any_value = FALSE;
    for (k = 0; k < ln->n; k++)
    {
        const RdCell *c = &pg->cells[ln->first + k];
        int64_t v;
        if (rd_is_value(c, &v))
        {
            int col = rd_col_of(&s->cols, c->x0, c->x1);
            any_value = TRUE;
            if (col == s->col_voters)
                voters = v;
            else if (col == s->col_reg)
                reg = v;
        }
        else if (!rd_is_pct(c) && label == NULL)
        {
            label = c->text;
            if (!any_value && fabsf(c->x0 - s->elector_x0) < 3.0f && ln->n == 1)
            {
                /* an elector group ("Total", "Democratic", ...) */
                rd_sum_close_group(s);
                ZeroMemory(&s->cur, sizeof(s->cur));
                s->cur.seen = TRUE;
                s->cur.voters = s->cur.registered = EE_RPDF_UNKNOWN;
                s->cur_is_total = _wcsicmp(c->text, L"Total") == 0;
                s->ngroups++;
                return;
            }
        }
    }
    if (label == NULL || voters == EE_RPDF_UNKNOWN)
    {
        return;
    }
    if (s->ngroups == 0)
    {
        s->ngroups = 1; /* no group line: the one group */
        s->cur.seen = TRUE;
        s->cur.voters = s->cur.registered = EE_RPDF_UNKNOWN;
    }
    if (_wcsicmp(label, L"Total") == 0)
    {
        s->cur.voters = voters;
        s->cur.registered = reg;
    }
    else
    {
        rd_turnout_row(&s->cur, label, voters);
    }
}

/* A contest's heading (its whole title). */
static void rd_sum_title(RdSum *s, const wchar_t *title, uint32_t vote_for)
{
    if (s->mode == RX_CANDIDATES || s->mode == RX_WRITEINS)
    {
        rd_sum_flush(s);
    }
    if (s->mode == RX_TURNOUT)
    {
        rd_sum_close_group(s);
    }
    s->title_done = TRUE;
    s->ci = rd_contest(s->cx, title, vote_for);
    s->mode = RX_STATS;
    s->col_total = -1;
}

static void rd_sum_line(RdSum *s, const RdPage *pg, const RdLine *ln)
{
    RdCx *cx = s->cx;
    const RdCell *c0;
    uint32_t k;
    if (ln->n == 0)
    {
        return;
    }
    c0 = &pg->cells[ln->first];
    if (rd_line_starts(pg, ln, L"Page:"))
    {
        rd_page_header(cx, pg, ln);
        return;
    }
    if (rd_line_starts(pg, ln, L"Registered Voters:") || rd_line_starts(pg, ln, L"Voters Cast:"))
    {
        /* "Registered Voters: 206,122 of 495,050 (41.64%)": the turnout when there is no table */
        wchar_t line[300];
        line[0] = L'\0';
        for (k = 0; k < ln->n; k++)
        {
            StringCchCatW(line, ARRAYSIZE(line), pg->cells[ln->first + k].text);
            StringCchCatW(line, ARRAYSIZE(line), L" ");
        }
        rd_voters_of(line, &s->fb_voters, &s->fb_registered);
    }
    if (rd_line_starts(pg, ln, L"Precincts Reported") || rd_line_starts(pg, ln, L"Registered Voters:") ||
        rd_line_starts(pg, ln, L"Voters Cast:") || rd_line_starts(pg, ln, L"Ballots Cast:"))
    {
        if (s->mode == RX_TURNOUT)
        {
            rd_sum_close_group(s);
            s->mode = RX_START;
        }
        return;
    }
    if (ln->size >= 12.0f && c0->x0 >= 30.0f)
    {
        if (!s->title_done)
        {
            rd_title_line(cx, c0->text);
        }
        return;
    }
    for (k = 0; k < ln->n; k++)
    {
        if (_wcsicmp(pg->cells[ln->first + k].text, L"Counting Group") == 0)
        {
            rd_cols_from_line(&s->cols, pg, ln);
            s->col_cg = (int)k;
            s->cg_x0 = pg->cells[ln->first + k].x0;
            s->elector_x0 = c0->x0;
            s->col_voters = rd_col_named(&s->cols, L"Voters Cast");
            if (s->col_voters < 0)
            {
                s->col_voters = rd_col_named(&s->cols, L"Voters");
            }
            s->col_reg = rd_col_named(&s->cols, L"Registered Voters");
            s->mode = RX_TURNOUT;
            s->title_done = TRUE;
            return;
        }
    }
    if (s->mode == RX_TURNOUT)
    {
        rd_sum_turnout_line(s, pg, ln);
        return;
    }
    if (s->ci == UINT32_MAX)
    {
        return;
    }
    if (_wcsicmp(c0->text, L"Candidate") == 0)
    {
        if (s->mode == RX_CANDIDATES)
        {
            rd_sum_flush(s); /* the heading again on a new page */
        }
        rd_cols_from_line(&s->cols, pg, ln);
        s->col_total = rd_col_named(&s->cols, L"Total");
        s->col_party = rd_col_named(&s->cols, L"Party");
        s->mode = RX_CANDIDATES;
        return;
    }
    if (s->mode == RX_STATS)
    {
        EeRpdfContest *ct = &cx->d->contests[s->ci];
        int64_t v = 0;
        BOOL have = FALSE;
        if (c0->x0 >= 30.0f)
        {
            /* the statistics heading: "Election Day ... Total" or just "Total" */
            RdCols tmp;
            rd_cols_from_line(&tmp, pg, ln);
            if (rd_col_named(&tmp, L"Total") >= 0)
            {
                s->cols = tmp;
                s->col_total = rd_col_named(&s->cols, L"Total");
            }
            return;
        }
        for (k = 1; k < ln->n && s->col_total >= 0; k++)
        {
            const RdCell *c = &pg->cells[ln->first + k];
            if (rd_is_value(c, &v) && rd_col_of(&s->cols, c->x0, c->x1) == s->col_total)
            {
                have = TRUE;
                break;
            }
        }
        if (!have)
        {
            return;
        }
        if (_wcsicmp(c0->text, L"Undervotes") == 0)
        {
            ct->under = v;
            ct->has_over_under = TRUE;
        }
        else if (_wcsicmp(c0->text, L"Overvotes") == 0)
        {
            ct->over = v;
            ct->has_over_under = TRUE;
        }
        else if (_wcsicmp(c0->text, L"Times Cast") == 0)
        {
            ct->ballots = v;
        }
        return;
    }
    if (s->mode == RX_SKIP)
    {
        /* after the candidates, the write-in table's heading ("Total") */
        RdCols tmp;
        if (c0->x0 < 30.0f)
        {
            return;
        }
        rd_cols_from_line(&tmp, pg, ln);
        if (rd_col_named(&tmp, L"Total") >= 0)
        {
            s->cols = tmp;
            s->col_total = rd_col_named(&s->cols, L"Total");
            s->col_party = -1;
            s->mode = RX_WRITEINS;
        }
        return;
    }
    if (s->mode == RX_CANDIDATES || s->mode == RX_WRITEINS)
    {
        RdCandRow row;
        BOOL have = FALSE;
        if (s->mode == RX_CANDIDATES && _wcsicmp(c0->text, L"Total Votes") == 0)
        {
            for (k = 1; k < ln->n; k++)
            {
                const RdCell *c = &pg->cells[ln->first + k];
                int64_t v;
                if (rd_is_value(c, &v) && rd_col_of(&s->cols, c->x0, c->x1) == s->col_total)
                {
                    cx->votes[s->ci] = v;
                }
            }
            rd_sum_flush(s);
            s->mode = RX_SKIP;
            return;
        }
        ZeroMemory(&row, sizeof(row));
        row.y = ln->y;
        row.total = EE_RPDF_UNKNOWN;
        for (k = 0; k < ln->n; k++)
        {
            const RdCell *c = &pg->cells[ln->first + k];
            int64_t v;
            int col = rd_col_of(&s->cols, c->x0, c->x1);
            if (rd_is_value(c, &v))
            {
                if (col == s->col_total)
                {
                    row.total = v;
                    have = TRUE;
                }
            }
            else if (rd_is_pct(c))
            {
                continue;
            }
            else if (col == s->col_party && s->col_party >= 0)
            {
                StringCchCopyW(row.party, ARRAYSIZE(row.party), c->text);
            }
            else if (c->x0 < 30.0f)
            {
                StringCchCopyW(row.name, ARRAYSIZE(row.name), c->text);
            }
        }
        if (have)
        {
            if (s->nrows < RD_MAX_CAND)
            {
                s->rows[s->nrows++] = row;
            }
        }
        else if (row.name[0] != L'\0' && s->nfrag < RD_MAX_CAND)
        {
            s->frag_y[s->nfrag] = ln->y;
            StringCchCopyW(s->frag[s->nfrag], ARRAYSIZE(s->frag[0]), row.name);
            s->nfrag++;
        }
        return;
    }
}

static BOOL rd_parse_summary_pdf(EePdf *pdf, RdCx *cx, wchar_t *err, size_t errcch)
{
    RdSum s;
    EePdfPageText pt;
    uint32_t np = EePdf_PageCount(pdf), p, l;
    BOOL ok = TRUE;
    ZeroMemory(&s, sizeof(s));
    s.cx = cx;
    s.ci = UINT32_MAX;
    s.col_total = s.col_party = s.col_cg = s.col_voters = s.col_reg = -1;
    s.fb_voters = s.fb_registered = EE_RPDF_UNKNOWN;
    s.rows = (RdCandRow *)malloc(RD_MAX_CAND * sizeof(RdCandRow));
    s.frag = (wchar_t(*)[128])malloc(RD_MAX_CAND * sizeof(*s.frag));
    if (s.rows == NULL || s.frag == NULL)
    {
        free(s.rows);
        free(s.frag);
        cx->oom = TRUE;
        return FALSE;
    }
    EePdf_PageTextInit(&pt);
    pt.std_widths = 1; /* a font without widths is measured as Helvetica */
    for (p = 0; p < np && !cx->oom; p++)
    {
        RdPage pg;
        ZeroMemory(&pg, sizeof(pg));
        if (!EePdf_ExtractPageText(pdf, p, &pt))
        {
            continue;
        }
        if (!rd_page_build(&pg, &pt))
        {
            cx->oom = TRUE;
            rd_page_free(&pg);
            break;
        }
        for (l = 0; l < pg.nlines && !cx->oom; l++)
        {
            if (rd_is_title_line(&pg, &pg.lines[l]))
            {
                wchar_t title[1024];
                uint32_t vf = 0;
                rd_gather_title(cx, &pg, &l, title, ARRAYSIZE(title), &vf);
                rd_sum_title(&s, title, vf);
            }
            else
            {
                rd_sum_line(&s, &pg, &pg.lines[l]);
            }
        }
        if (s.mode == RX_CANDIDATES || s.mode == RX_WRITEINS)
        {
            rd_sum_flush(&s); /* name parts never wrap across a page */
        }
        rd_page_free(&pg);
    }
    EePdf_PageTextFree(&pt);
    if (s.mode == RX_TURNOUT)
    {
        rd_sum_close_group(&s);
    }
    if (s.total.seen || s.first.seen)
    {
        rd_apply_turnout(cx, s.total.seen ? &s.total : &s.first);
    }
    else
    {
        cx->d->ballots_total = s.fb_voters;
        cx->d->registered_total = s.fb_registered;
    }
    if (cx->party_lines > 0)
    {
        wchar_t msg[200];
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"%u contest titles name their party on a second line, which is not part of the title.",
                         cx->party_lines);
        rd_note(cx, msg);
    }
    (void)err;
    (void)errcch;
    free(s.rows);
    free(s.frag);
    return ok;
}

/* -------------------------------------------------------------------------- */
/* PDF: Statement of Votes Cast                                               */
/* -------------------------------------------------------------------------- */

/* A column of a contest page: its heading (turned 90 degrees) and the band of x it owns. */
typedef struct RdSCol
{
    float anchor, lo, hi;
    wchar_t name[200];
} RdSCol;

#define RD_MAX_PANELS 4
#define RD_MAX_SCOLS  64

typedef struct RdSovc
{
    RdCx *cx;
    BOOL title_done;
    BOOL in_contests;
    uint32_t ci;
    wchar_t ctitle[1024];
    uint32_t turnout_pre[RD_MAX_PANELS];
} RdSovc;

static int rd_cmp_rot(const void *pa, const void *pb)
{
    const EePdfTextRun *a = *(const EePdfTextRun *const *)pa;
    const EePdfTextRun *b = *(const EePdfTextRun *const *)pb;
    if (fabsf(a->x - b->x) > 0.6f)
    {
        return a->x < b->x ? -1 : 1;
    }
    return a->y < b->y ? -1 : (a->y > b->y ? 1 : 0);
}

/* The columns of panel [lo, hi) from the turned headings. */
static uint32_t rd_sovc_cols(const RdPage *pg, float lo, float hi, RdSCol *cols, uint32_t max)
{
    const EePdfTextRun **rr = NULL;
    uint32_t i, n = 0, nc = 0;
    float sum = 0.0f;
    uint32_t cnt = 0;
    if (pg->nrot == 0)
    {
        return 0;
    }
    rr = (const EePdfTextRun **)malloc(pg->nrot * sizeof(*rr));
    if (rr == NULL)
    {
        return 0;
    }
    for (i = 0; i < pg->nrot; i++)
    {
        if (pg->rot[i]->x >= lo && pg->rot[i]->x < hi)
        {
            rr[n++] = pg->rot[i];
        }
    }
    qsort(rr, n, sizeof(*rr), rd_cmp_rot);
    for (i = 0; i < n; i++)
    {
        const EePdfTextRun *r = rr[i];
        char buf[512];
        wchar_t w[256];
        size_t len = r->text_len < sizeof(buf) - 1 ? r->text_len : sizeof(buf) - 1;
        if (i == 0 || r->x - rr[i - 1]->x > 14.0f)
        {
            if (nc > 0)
            {
                cols[nc - 1].anchor = sum / (float)cnt;
            }
            if (nc >= max)
            {
                break;
            }
            ZeroMemory(&cols[nc], sizeof(cols[nc]));
            nc++;
            sum = 0.0f;
            cnt = 0;
        }
        memcpy(buf, pg->pt->text + r->text_off, len);
        buf[len] = '\0';
        w[0] = L'\0';
        MultiByteToWideChar(CP_UTF8, 0, buf, -1, w, ARRAYSIZE(w));
        StringCchCatW(cols[nc - 1].name, ARRAYSIZE(cols[0].name), w);
        if (i + 1 < n && fabsf(rr[i + 1]->x - r->x) > 0.6f && rr[i + 1]->x - r->x <= 14.0f)
        {
            /* the heading's next line ("Katherine E. Juhan-" "Arnold": no space after a hyphen) */
            wchar_t *nm = cols[nc - 1].name;
            size_t nlen = wcslen(nm);
            while (nlen > 0 && nm[nlen - 1] == L' ')
            {
                nm[--nlen] = L'\0';
            }
            if (nlen > 0 && nm[nlen - 1] != L'-')
            {
                StringCchCatW(nm, ARRAYSIZE(cols[0].name), L" ");
            }
        }
        sum += r->x;
        cnt++;
    }
    if (nc > 0 && cnt > 0)
    {
        cols[nc - 1].anchor = sum / (float)cnt;
    }
    for (i = 0; i < nc; i++)
    {
        rd_norm(cols[i].name);
        cols[i].lo = i == 0 ? lo : (cols[i - 1].anchor + cols[i].anchor) / 2.0f;
        cols[i].hi = i + 1 == nc ? hi : (cols[i].anchor + cols[i + 1].anchor) / 2.0f;
    }
    free(rr);
    return nc;
}

static BOOL rd_is_total_label(const wchar_t *t)
{
    return wcsstr(t, L" - Total") != NULL || _wcsicmp(t, L"Total") == 0;
}

/* One page of the Statement of Votes Cast. */
static void rd_sovc_page(RdSovc *sv, const RdPage *pg)
{
    RdCx *cx = sv->cx;
    float panel_lo[RD_MAX_PANELS], panel_hi[RD_MAX_PANELS], name_x[RD_MAX_PANELS];
    uint32_t npanels = 0, l, k, pi;
    float header_y = 0.0f;
    RdSCol cols[RD_MAX_PANELS][RD_MAX_SCOLS];
    uint32_t ncols[RD_MAX_PANELS];
    uint32_t cur_pre[RD_MAX_PANELS];
    int64_t acc_sum[RD_MAX_PANELS][RD_MAX_SCOLS];   /* this precinct's printed votes, by column */
    uint32_t acc_prot[RD_MAX_PANELS][RD_MAX_SCOLS]; /* its vote types withheld ("****"), as bits */
    RdCols tcols; /* the turnout pages' upright headings */
    int t_reg = -1, t_voters = -1;
    BOOL contest_page = pg->nrot > 0;

    tcols.n = 0;
    for (l = 0; l < pg->nlines; l++)
    {
        const RdLine *ln = &pg->lines[l];
        const RdCell *c0 = &pg->cells[ln->first];
        if (ln->n == 0)
        {
            continue;
        }
        if (rd_line_starts(pg, ln, L"Page:"))
        {
            rd_page_header(cx, pg, ln);
            continue;
        }
        if (ln->size >= 12.0f && c0->x0 >= 30.0f)
        {
            if (!sv->title_done)
            {
                rd_title_line(cx, c0->text);
            }
            continue;
        }
        if (rd_is_title_line(pg, ln))
        {
            /* a contest's heading, repeated on each of its pages */
            wchar_t title[1024];
            uint32_t vf = 0, parties = cx->party_lines;
            rd_gather_title(cx, pg, &l, title, ARRAYSIZE(title), &vf);
            sv->title_done = TRUE;
            sv->in_contests = TRUE;
            if (sv->ci != UINT32_MAX && wcscmp(title, sv->ctitle) == 0)
            {
                cx->party_lines = parties; /* counted on its first page */
                continue;
            }
            sv->ci = rd_contest(cx, title, vf);
            StringCchCopyW(sv->ctitle, ARRAYSIZE(sv->ctitle), title);
            continue;
        }
        for (k = 0; k < ln->n && npanels < RD_MAX_PANELS; k++)
        {
            if (_wcsicmp(pg->cells[ln->first + k].text, L"Precinct") == 0)
            {
                panel_lo[npanels++] = pg->cells[ln->first + k].x0;
                header_y = ln->y;
            }
        }
        if (!contest_page && header_y == ln->y)
        {
            /* the turnout headings: Registered Voters, Cards Cast, Voters Cast, % Turnout */
            rd_cols_from_line(&tcols, pg, ln);
            if (l > 0 && pg->lines[l - 1].y - ln->y < 12.0f)
            {
                const RdLine *up = &pg->lines[l - 1];
                for (k = 0; k < up->n && tcols.n < RD_MAX_COLS; k++)
                {
                    const RdCell *c = &pg->cells[up->first + k];
                    tcols.x0[tcols.n] = c->x0;
                    tcols.x1[tcols.n] = c->x1;
                    StringCchCopyW(tcols.name[tcols.n], ARRAYSIZE(tcols.name[0]), c->text);
                    tcols.n++;
                }
            }
            t_voters = rd_col_named(&tcols, L"Voters Cast");
            t_reg = rd_col_named(&tcols, L"Registered");
            if (t_reg < 0)
            {
                t_reg = rd_col_named(&tcols, L"Registered Voters");
            }
        }
    }
    if (npanels == 0 || cx->oom)
    {
        return;
    }
    if (contest_page && sv->ci == UINT32_MAX)
    {
        return;
    }
    if (!contest_page)
    {
        npanels = 1; /* the turnout table spans the page */
        panel_lo[0] = 0.0f;
    }
    /* sort the panels, find their edges and the precinct names' left edge */
    for (pi = 0; pi < npanels; pi++)
    {
        for (k = pi + 1; k < npanels; k++)
        {
            if (panel_lo[k] < panel_lo[pi])
            {
                float t = panel_lo[pi];
                panel_lo[pi] = panel_lo[k];
                panel_lo[k] = t;
            }
        }
    }
    for (pi = 0; pi < npanels; pi++)
    {
        panel_lo[pi] = pi == 0 ? 0.0f : panel_lo[pi] - 2.0f;
        panel_hi[pi] = 100000.0f;
        if (pi > 0)
        {
            panel_hi[pi - 1] = panel_lo[pi];
        }
        name_x[pi] = 100000.0f;
        cur_pre[pi] = UINT32_MAX;
        ncols[pi] = contest_page ? rd_sovc_cols(pg, panel_lo[pi], panel_hi[pi], cols[pi], RD_MAX_SCOLS) : 0;
    }
    for (l = 0; l < pg->nlines; l++)
    {
        const RdLine *ln = &pg->lines[l];
        if (ln->y >= header_y - 3.0f || ln->size >= 12.0f)
        {
            continue;
        }
        for (k = 0; k < ln->n; k++)
        {
            const RdCell *c = &pg->cells[ln->first + k];
            if (!rd_is_value(c, NULL) && !rd_is_pct(c))
            {
                for (pi = 0; pi < npanels; pi++)
                {
                    if (c->x0 >= panel_lo[pi] && c->x0 < panel_hi[pi] && c->x0 < name_x[pi])
                    {
                        name_x[pi] = c->x0;
                    }
                }
            }
        }
    }
    for (pi = 0; pi < npanels; pi++)
    {
        for (l = 0; l < pg->nlines && !cx->oom; l++)
        {
            const RdLine *ln = &pg->lines[l];
            const RdCell *label = NULL;
            BOOL values = FALSE;
            if (ln->y >= header_y - 3.0f || ln->size >= 12.0f)
            {
                continue;
            }
            for (k = 0; k < ln->n; k++)
            {
                const RdCell *c = &pg->cells[ln->first + k];
                if (c->x0 < panel_lo[pi] || c->x0 >= panel_hi[pi])
                {
                    continue;
                }
                if (rd_is_value(c, NULL) || rd_is_pct(c))
                {
                    values = TRUE;
                }
                else if (label == NULL)
                {
                    label = c;
                }
            }
            if (label == NULL)
            {
                continue;
            }
            if (rd_is_total_label(label->text) && wcsstr(label->text, L" - Total") != NULL)
            {
                cur_pre[pi] = UINT32_MAX; /* "County - Total": the totals that follow */
                if (!contest_page && values)
                {
                    for (k = 0; k < ln->n; k++)
                    {
                        const RdCell *c = &pg->cells[ln->first + k];
                        int64_t v;
                        int col;
                        if (!rd_is_value(c, &v) || v == EE_RPDF_PROTECTED)
                        {
                            continue;
                        }
                        col = rd_col_of(&tcols, c->x0, c->x1);
                        if (col == t_voters)
                            cx->d->ballots_total = v;
                        else if (col == t_reg)
                            cx->d->registered_total = v;
                    }
                }
                continue;
            }
            if (!values)
            {
                if (fabsf(label->x0 - name_x[pi]) < 2.0f)
                {
                    cur_pre[pi] = rd_pre(cx, label->text); /* a precinct */
                    ZeroMemory(acc_sum[pi], sizeof(acc_sum[pi]));
                    ZeroMemory(acc_prot[pi], sizeof(acc_prot[pi]));
                }
                continue;
            }
            if (cur_pre[pi] == UINT32_MAX)
            {
                continue;
            }
            if (!contest_page)
            {
                BOOL total = _wcsicmp(label->text, L"Total") == 0;
                uint32_t vt = total ? UINT32_MAX : rd_vt(cx, label->text);
                for (k = 0; k < ln->n; k++)
                {
                    const RdCell *c = &pg->cells[ln->first + k];
                    int64_t v;
                    int col;
                    if (!rd_is_value(c, &v) || v == EE_RPDF_PROTECTED)
                    {
                        continue;
                    }
                    col = rd_col_of(&tcols, c->x0, c->x1);
                    if (col == t_reg && col >= 0)
                    {
                        cx->d->precincts[cur_pre[pi]].registered = v;
                    }
                    else if (col == t_voters && col >= 0)
                    {
                        if (total)
                            cx->d->precincts[cur_pre[pi]].ballots = v;
                        else if (vt != UINT32_MAX)
                            rd_turnout(cx, cur_pre[pi], vt, v);
                    }
                }
                continue;
            }
            {
                /* a vote type's row, or the precinct's Total row: it gives the whole count of a
                 * choice whose vote types are withheld ("****"), and the votes not printed by
                 * vote type go to the vote type "Withheld" */
                BOOL total_row = _wcsicmp(label->text, L"Total") == 0;
                uint32_t vt = total_row ? UINT32_MAX : rd_vt(cx, label->text);
                BOOL taken[RD_MAX_SCOLS];
                ZeroMemory(taken, sizeof(taken));
                if (!total_row && vt == UINT32_MAX)
                {
                    continue;
                }
                for (k = 0; k < ln->n; k++)
                {
                    const RdCell *c = &pg->cells[ln->first + k];
                    int64_t v;
                    uint32_t j;
                    float mid = (c->x0 + c->x1) / 2.0f;
                    if (c->x0 < panel_lo[pi] || c->x0 >= panel_hi[pi] || rd_is_pct(c) || !rd_is_value(c, &v))
                    {
                        continue;
                    }
                    for (j = 0; j < ncols[pi]; j++)
                    {
                        if (mid >= cols[pi][j].lo && mid < cols[pi][j].hi)
                        {
                            break;
                        }
                    }
                    if (j == ncols[pi] || taken[j])
                    {
                        continue; /* outside the columns, or the column's percentage ("****") */
                    }
                    taken[j] = TRUE;
                    {
                        const wchar_t *cn = cols[pi][j].name;
                        if (_wcsicmp(cn, L"Times Cast") == 0 || _wcsicmp(cn, L"Registered Voters") == 0 ||
                            _wcsicmp(cn, L"Total Votes") == 0)
                        {
                            continue;
                        }
                        uint32_t ch;
                        if (_wcsicmp(cn, L"Undervotes") == 0 || _wcsicmp(cn, L"Overvotes") == 0)
                        {
                            cx->d->contests[sv->ci].has_over_under = TRUE;
                            ch = _wcsicmp(cn, L"Undervotes") == 0 ? EE_RPDF_UNDER : EE_RPDF_OVER;
                        }
                        else
                        {
                            ch = rd_choice(cx, sv->ci, cn, NULL, EE_RPDF_UNKNOWN);
                            if (ch == UINT32_MAX)
                            {
                                continue;
                            }
                        }
                        if (!total_row)
                        {
                            if (v == EE_RPDF_PROTECTED)
                            {
                                acc_prot[pi][j] |= 1u << vt;
                            }
                            else
                            {
                                acc_sum[pi][j] += v;
                                rd_count(cx, sv->ci, ch, cur_pre[pi], vt, v);
                            }
                        }
                        else if (acc_prot[pi][j] != 0)
                        {
                            if (v != EE_RPDF_PROTECTED)
                            {
                                uint32_t wvt = rd_vt(cx, L"Withheld");
                                if (wvt != UINT32_MAX && v > acc_sum[pi][j])
                                {
                                    rd_count(cx, sv->ci, ch, cur_pre[pi], wvt, v - acc_sum[pi][j]);
                                }
                                cx->withheld_cells++;
                            }
                            else
                            {
                                uint32_t b;
                                for (b = 0; b < RD_MAX_VT; b++)
                                {
                                    if (acc_prot[pi][j] & (1u << b))
                                    {
                                        rd_count(cx, sv->ci, ch, cur_pre[pi], b, EE_RPDF_PROTECTED);
                                    }
                                }
                            }
                        }
                    }
                }
                if (total_row)
                {
                    ZeroMemory(acc_sum[pi], sizeof(acc_sum[pi]));
                    ZeroMemory(acc_prot[pi], sizeof(acc_prot[pi]));
                }
            }
        }
    }
}

static BOOL rd_parse_sovc_pdf(EePdf *pdf, RdCx *cx)
{
    RdSovc sv;
    EePdfPageText pt;
    uint32_t np = EePdf_PageCount(pdf), p;
    ZeroMemory(&sv, sizeof(sv));
    sv.cx = cx;
    sv.ci = UINT32_MAX;
    EePdf_PageTextInit(&pt);
    pt.std_widths = 1; /* a font without widths is measured as Helvetica */
    for (p = 0; p < np && !cx->oom; p++)
    {
        RdPage pg;
        ZeroMemory(&pg, sizeof(pg));
        if (!EePdf_ExtractPageText(pdf, p, &pt))
        {
            continue;
        }
        if (!rd_page_build(&pg, &pt))
        {
            cx->oom = TRUE;
            rd_page_free(&pg);
            break;
        }
        rd_sovc_page(&sv, &pg);
        rd_page_free(&pg);
    }
    EePdf_PageTextFree(&pt);
    if (cx->party_lines > 0)
    {
        wchar_t msg[200];
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"%u contest titles name their party on a second line, which is not part of the title.",
                         cx->party_lines);
        rd_note(cx, msg);
    }
    return !cx->oom;
}

int EeRdom_PdfKind(EePdf *pdf)
{
    char title[128];
    if (!EePdf_GetInfoText(pdf, "Title", title, sizeof(title)))
    {
        return 0;
    }
    if (strcmp(title, "ElectionSummaryReportRPT") == 0)
    {
        return EE_RPDF_DOM_SUMMARY;
    }
    if (strcmp(title, "StatementOfVotesCastRPT") == 0)
    {
        return EE_RPDF_DOM_SOVC;
    }
    return 0;
}

BOOL EeRdom_ParsePdf(EePdf *pdf, int kind, EeRpdfDoc *out, wchar_t *err, size_t errcch)
{
    RdCx cx;
    BOOL ok;
    ZeroMemory(out, sizeof(*out));
    out->registered_total = EE_RPDF_UNKNOWN;
    out->ballots_total = EE_RPDF_UNKNOWN;
    ZeroMemory(&cx, sizeof(cx));
    cx.d = out;
    ok = kind == EE_RPDF_DOM_SOVC ? rd_parse_sovc_pdf(pdf, &cx) : rd_parse_summary_pdf(pdf, &cx, err, errcch);
    ok = ok && rd_finish(&cx, kind, err, errcch);
    if (!ok && (err == NULL || err[0] == L'\0'))
    {
        rd_set_err(err, errcch, cx.oom ? L"Out of memory reading the Dominion report." : L"The Dominion report could not be read.");
    }
    free(cx.votes);
    if (!ok)
    {
        EeRpdf_Free(out);
    }
    return ok;
}
