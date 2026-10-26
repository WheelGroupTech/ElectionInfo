/**
 * @file ee_results.c
 * @brief Election results model, Clarity loaders, multi-file cross-check, tabulation and
 *        the reloadable export. See ee_results.h and docs/election-results-design.md.
 */

#include "ee_results.h"
#include "csv_sheet.h"
#include "xlsx.h"

#include "third_party/miniz/miniz.h"

#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <wctype.h>
#include <strsafe.h>

/* -------------------------------------------------------------------------- */
/* Small helpers                                                              */
/* -------------------------------------------------------------------------- */

/* Largest results file read into memory (Clarity detail files are tens of MB). */
#define RES_MAX_FILE_BYTES (1024u * 1024u * 1024u)
/* Detailed cross-check issues kept per source before the rest are only counted. */
#define RES_MAX_ISSUES_PER_SOURCE 100u

static void res_set_err(wchar_t *dst, size_t cch, const wchar_t *msg)
{
    if (dst != NULL && cch > 0)
    {
        StringCchCopyW(dst, cch, msg);
    }
}

static BOOL res_grow(void **arr, size_t *cap, size_t need, size_t elem)
{
    if (need > *cap)
    {
        size_t ncap = *cap ? *cap : 16;
        void *np;
        while (ncap < need)
        {
            if (ncap > ((size_t)1 << 40) / elem)
            {
                return FALSE;
            }
            ncap *= 2;
        }
        np = realloc(*arr, ncap * elem);
        if (np == NULL)
        {
            return FALSE;
        }
        *arr = np;
        *cap = ncap;
    }
    return TRUE;
}

static wchar_t *wdupn(const wchar_t *s, size_t n)
{
    wchar_t *d = (wchar_t *)malloc((n + 1) * sizeof(wchar_t));
    if (d != NULL)
    {
        if (n > 0)
        {
            memcpy(d, s, n * sizeof(wchar_t));
        }
        d[n] = L'\0';
    }
    return d;
}

static wchar_t *wdup(const wchar_t *s)
{
    return s != NULL ? wdupn(s, wcslen(s)) : NULL;
}

/* UTF-8 (n bytes, or -1 for NUL-terminated) -> new wide string. */
static wchar_t *u8dup(const char *s, int n)
{
    int wn;
    wchar_t *w;
    if (s == NULL)
    {
        return NULL;
    }
    if (n < 0)
    {
        n = (int)strlen(s);
    }
    wn = n > 0 ? MultiByteToWideChar(CP_UTF8, 0, s, n, NULL, 0) : 0;
    w = (wchar_t *)malloc(((size_t)wn + 1) * sizeof(wchar_t));
    if (w == NULL)
    {
        return NULL;
    }
    if (wn > 0)
    {
        MultiByteToWideChar(CP_UTF8, 0, s, n, w, wn);
    }
    w[wn] = L'\0';
    return w;
}

static BOOL is_ws(wchar_t c)
{
    return c == L' ' || c == L'\t' || c == L'\r' || c == L'\n' || c == 0x00A0;
}

/* Trimmed copy of s[0..n). */
static wchar_t *wdup_trim(const wchar_t *s, size_t n)
{
    while (n > 0 && is_ws(s[0]))
    {
        s++;
        n--;
    }
    while (n > 0 && is_ws(s[n - 1]))
    {
        n--;
    }
    return wdupn(s, n);
}

/* Trim in place; returns s. */
static wchar_t *wtrim(wchar_t *s)
{
    size_t n = wcslen(s);
    size_t a = 0;
    while (a < n && is_ws(s[a]))
    {
        a++;
    }
    while (n > a && is_ws(s[n - 1]))
    {
        n--;
    }
    if (a > 0)
    {
        memmove(s, s + a, (n - a) * sizeof(wchar_t));
    }
    s[n - a] = L'\0';
    return s;
}

/* Case-insensitive substring search. */
static const wchar_t *wcsistr(const wchar_t *hay, const wchar_t *needle)
{
    size_t n = wcslen(needle);
    for (; *hay != L'\0'; hay++)
    {
        if (_wcsnicmp(hay, needle, n) == 0)
        {
            return hay;
        }
    }
    return NULL;
}

static BOOL wieq(const wchar_t *a, const wchar_t *b)
{
    return a != NULL && b != NULL && _wcsicmp(a, b) == 0;
}

/* Case-insensitive equality after trimming @p a. */
static BOOL wieq_trim(const wchar_t *a, const wchar_t *b)
{
    size_t n, m;
    if (a == NULL || b == NULL)
    {
        return FALSE;
    }
    while (is_ws(*a))
    {
        a++;
    }
    n = wcslen(a);
    while (n > 0 && is_ws(a[n - 1]))
    {
        n--;
    }
    m = wcslen(b);
    return n == m && _wcsnicmp(a, b, n) == 0;
}

/* Parse an integer allowing surrounding spaces and thousands separators. */
static BOOL parse_i64w(const wchar_t *s, int64_t *out)
{
    int64_t v = 0;
    BOOL neg = FALSE;
    BOOL digits = FALSE;
    if (s == NULL)
    {
        return FALSE;
    }
    while (is_ws(*s))
    {
        s++;
    }
    if (*s == L'-')
    {
        neg = TRUE;
        s++;
    }
    for (; *s != L'\0'; s++)
    {
        if (*s >= L'0' && *s <= L'9')
        {
            if (v > (INT64_MAX - 9) / 10)
            {
                return FALSE;
            }
            v = v * 10 + (*s - L'0');
            digits = TRUE;
        }
        else if (*s == L',' && digits)
        {
            continue;
        }
        else
        {
            break;
        }
    }
    while (is_ws(*s))
    {
        s++;
    }
    if (!digits || *s != L'\0')
    {
        return FALSE;
    }
    *out = neg ? -v : v;
    return TRUE;
}

static BOOL parse_i64a(const char *s, size_t n, int64_t *out)
{
    wchar_t buf[48];
    size_t i;
    if (s == NULL || n == 0 || n >= ARRAYSIZE(buf))
    {
        return FALSE;
    }
    for (i = 0; i < n; i++)
    {
        buf[i] = (wchar_t)(unsigned char)s[i];
    }
    buf[n] = L'\0';
    return parse_i64w(buf, out);
}

static int32_t clamp32(int64_t v)
{
    return v > INT32_MAX ? INT32_MAX : (v < INT32_MIN ? INT32_MIN : (int32_t)v);
}

/* 1234567 -> "1,234,567". */
static void fmt_count(int64_t v, wchar_t *out, size_t cch)
{
    wchar_t digits[32];
    wchar_t tmp[48];
    size_t n, i, k = 0;
    BOOL neg = v < 0;
    uint64_t u = neg ? (uint64_t)(-(v + 1)) + 1u : (uint64_t)v;
    StringCchPrintfW(digits, ARRAYSIZE(digits), L"%llu", (unsigned long long)u);
    n = wcslen(digits);
    if (neg)
    {
        tmp[k++] = L'-';
    }
    for (i = 0; i < n; i++)
    {
        if (i > 0 && (n - i) % 3 == 0)
        {
            tmp[k++] = L',';
        }
        tmp[k++] = digits[i];
    }
    tmp[k] = L'\0';
    StringCchCopyW(out, cch, tmp);
}

static const wchar_t *base_name(const wchar_t *p)
{
    const wchar_t *b = p;
    for (; p != NULL && *p != L'\0'; p++)
    {
        if (*p == L'\\' || *p == L'/')
        {
            b = p + 1;
        }
    }
    return b;
}

static BOOL is_valid_utf8(const uint8_t *s, size_t n)
{
    size_t i = 0;
    while (i < n)
    {
        uint8_t c = s[i];
        size_t k;
        if (c < 0x80)
        {
            i++;
            continue;
        }
        if (c >= 0xC2 && c <= 0xDF)
        {
            k = 1;
        }
        else if (c >= 0xE0 && c <= 0xEF)
        {
            k = 2;
        }
        else if (c >= 0xF0 && c <= 0xF4)
        {
            k = 3;
        }
        else
        {
            return FALSE;
        }
        for (; k > 0; k--)
        {
            i++;
            if (i >= n || (s[i] & 0xC0) != 0x80)
            {
                return FALSE;
            }
        }
        i++;
    }
    return TRUE;
}

/* Decode a text file: UTF-8 / UTF-16 BOMs honored, BOM-less valid UTF-8 as UTF-8,
 * otherwise the ANSI code page. Returns a NUL-terminated wide buffer (free()). */
static wchar_t *decode_text(const uint8_t *d, size_t n, size_t *out_len)
{
    wchar_t *w = NULL;
    size_t wn = 0;
    *out_len = 0;
    if (n > 0x7FFFFFF0u)
    {
        return NULL;
    }
    if (n >= 2 && ((d[0] == 0xFF && d[1] == 0xFE) || (d[0] == 0xFE && d[1] == 0xFF)))
    {
        BOOL be = d[0] == 0xFE;
        size_t i;
        wn = (n - 2) / 2;
        w = (wchar_t *)malloc((wn + 1) * sizeof(wchar_t));
        if (w == NULL)
        {
            return NULL;
        }
        for (i = 0; i < wn; i++)
        {
            const uint8_t *p = d + 2 + i * 2;
            w[i] = be ? (wchar_t)((p[0] << 8) | p[1]) : (wchar_t)((p[1] << 8) | p[0]);
        }
    }
    else
    {
        UINT cp = CP_UTF8;
        if (n >= 3 && d[0] == 0xEF && d[1] == 0xBB && d[2] == 0xBF)
        {
            d += 3;
            n -= 3;
        }
        else if (!is_valid_utf8(d, n))
        {
            cp = CP_ACP;
        }
        wn = n > 0 ? (size_t)MultiByteToWideChar(cp, 0, (const char *)d, (int)n, NULL, 0) : 0;
        w = (wchar_t *)malloc((wn + 1) * sizeof(wchar_t));
        if (w == NULL)
        {
            return NULL;
        }
        if (wn > 0)
        {
            MultiByteToWideChar(cp, 0, (const char *)d, (int)n, w, (int)wn);
        }
    }
    w[wn] = L'\0';
    *out_len = wn;
    return w;
}

BOOL EeResults_NamesMatch(const wchar_t *a, const wchar_t *b)
{
    if (a == NULL || b == NULL)
    {
        return a == b;
    }
    while (is_ws(*a))
    {
        a++;
    }
    while (is_ws(*b))
    {
        b++;
    }
    for (;;)
    {
        BOOL sa = FALSE, sb = FALSE;
        while (is_ws(*a))
        {
            a++;
            sa = TRUE;
        }
        while (is_ws(*b))
        {
            b++;
            sb = TRUE;
        }
        if (*a == L'\0' || *b == L'\0')
        {
            return *a == *b;
        }
        if (sa != sb)
        {
            /* a word break on one side only */
            return FALSE;
        }
        if (*a == L'?' || *b == L'?')
        {
            /* a '?' stands for one character (a surrogate pair is one character) */
            if (*a == L'?' && *b != L'?' && IS_HIGH_SURROGATE(*b) && IS_LOW_SURROGATE(b[1]))
            {
                b++;
            }
            else if (*b == L'?' && *a != L'?' && IS_HIGH_SURROGATE(*a) && IS_LOW_SURROGATE(a[1]))
            {
                a++;
            }
        }
        else if (towupper(*a) != towupper(*b))
        {
            return FALSE;
        }
        a++;
        b++;
    }
}

/* -------------------------------------------------------------------------- */
/* Builder                                                                    */
/* -------------------------------------------------------------------------- */

#define RES_OVER      0xFFFFFFFEu
#define RES_UNDER     0xFFFFFFFDu
#define RES_PROTECTED INT32_MIN

typedef struct ResEntry
{
    uint32_t contest;
    uint32_t choice; /* local choice index, RES_OVER or RES_UNDER */
    uint32_t pre;    /* global precinct index */
    uint32_t vt;     /* vote type index (0 when there are none) */
    int32_t votes;   /* RES_PROTECTED: the source withheld this cell */
} ResEntry;

typedef struct ResBuilder
{
    EeResults *r;
    size_t cap_vt, cap_pre, cap_contests, cap_choices, cap_issues;
    uint32_t *pre_hash; /* slot -> index + 1 (0 = empty) */
    uint32_t pre_hash_cap;
    ResEntry *ent;
    size_t nent, cap_ent;
    ResEntry *tvt;             /* turnout ballots by vote type: pre, vt, votes */
    size_t ntvt, cap_tvt;
    int64_t *vt_ballots_total; /* nvt, from a turnout Totals row */
    size_t cap_vtb;
    BOOL oom;
    BOOL bad; /* the parser met something it cannot read */
    wchar_t bad_msg[256];
} ResBuilder;

static void b_init(ResBuilder *b, EeResults *r)
{
    ZeroMemory(b, sizeof(*b));
    b->r = r;
    EeResults_Init(r);
}

static void b_free(ResBuilder *b)
{
    free(b->pre_hash);
    free(b->ent);
    free(b->tvt);
    free(b->vt_ballots_total);
    b->pre_hash = NULL;
    b->ent = NULL;
    b->tvt = NULL;
    b->vt_ballots_total = NULL;
}

static void b_fail(ResBuilder *b, const wchar_t *msg)
{
    if (!b->bad)
    {
        b->bad = TRUE;
        StringCchCopyW(b->bad_msg, ARRAYSIZE(b->bad_msg), msg);
    }
}

static void b_issue(ResBuilder *b, const wchar_t *text)
{
    EeResults *r = b->r;
    if (!res_grow((void **)&r->issues,
                  &b->cap_issues,
                  (size_t)r->nissues + 1,
                  sizeof(EeResultsIssue)))
    {
        b->oom = TRUE;
        return;
    }
    r->issues[r->nissues].source = 0;
    r->issues[r->nissues].text = wdup(text);
    if (r->issues[r->nissues].text == NULL)
    {
        b->oom = TRUE;
        return;
    }
    r->nissues++;
}

/* Index of vote type @p name, added when new. */
static uint32_t b_vt(ResBuilder *b, const wchar_t *name)
{
    EeResults *r = b->r;
    uint32_t i;
    for (i = 0; i < r->nvt; i++)
    {
        if (wieq(r->vote_types[i], name))
        {
            return i;
        }
    }
    if (!res_grow((void **)&r->vote_types, &b->cap_vt, (size_t)r->nvt + 1, sizeof(wchar_t *)) ||
        (r->vote_types[r->nvt] = wdup(name)) == NULL)
    {
        b->oom = TRUE;
        return 0;
    }
    return r->nvt++;
}

static uint32_t hash_w(const wchar_t *s)
{
    uint32_t h = 2166136261u;
    for (; *s != L'\0'; s++)
    {
        h = (h ^ (uint32_t)*s) * 16777619u;
    }
    return h;
}

static BOOL b_rehash(ResBuilder *b, uint32_t cap)
{
    EeResults *r = b->r;
    uint32_t *nh = (uint32_t *)calloc(cap, sizeof(uint32_t));
    uint32_t i;
    if (nh == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < r->nprecincts; i++)
    {
        uint32_t s = hash_w(r->precincts[i].name) & (cap - 1);
        while (nh[s] != 0)
        {
            s = (s + 1) & (cap - 1);
        }
        nh[s] = i + 1;
    }
    free(b->pre_hash);
    b->pre_hash = nh;
    b->pre_hash_cap = cap;
    return TRUE;
}

/* Index of precinct @p name (trimmed by the caller), added when new. */
static uint32_t b_pre(ResBuilder *b, const wchar_t *name)
{
    EeResults *r = b->r;
    uint32_t s;
    if (b->pre_hash_cap == 0 || (r->nprecincts + 1) * 2 > b->pre_hash_cap)
    {
        if (!b_rehash(b, b->pre_hash_cap ? b->pre_hash_cap * 2 : 1024))
        {
            b->oom = TRUE;
            return 0;
        }
    }
    s = hash_w(name) & (b->pre_hash_cap - 1);
    while (b->pre_hash[s] != 0)
    {
        if (wcscmp(r->precincts[b->pre_hash[s] - 1].name, name) == 0)
        {
            return b->pre_hash[s] - 1;
        }
        s = (s + 1) & (b->pre_hash_cap - 1);
    }
    if (!res_grow((void **)&r->precincts,
                  &b->cap_pre,
                  (size_t)r->nprecincts + 1,
                  sizeof(EeResultsPrecinct)) ||
        (r->precincts[r->nprecincts].name = wdup(name)) == NULL)
    {
        b->oom = TRUE;
        return 0;
    }
    r->precincts[r->nprecincts].registered = EE_RES_UNKNOWN;
    r->precincts[r->nprecincts].ballots = EE_RES_UNKNOWN;
    b->pre_hash[s] = r->nprecincts + 1;
    return r->nprecincts++;
}

/* Strip a trailing "(Vote For N)" (summary) or "( N)" (detail) from @p title in place.
 * With @p any_label, any words may stand before N: the detail files always end a title
 * with the Vote For suffix, and some counties' setups label it oddly ("(Voter Turnout 1)"
 * in Rockwall). */
static void split_title(wchar_t *title, uint32_t *vote_for, BOOL any_label)
{
    size_t n;
    wchar_t *open;
    wchar_t *p;
    uint32_t v = 0;
    BOOL digits = FALSE;
    BOOL neg = FALSE;
    wtrim(title);
    n = wcslen(title);
    if (n < 3 || title[n - 1] != L')')
    {
        return;
    }
    open = wcsrchr(title, L'(');
    if (open == NULL)
    {
        return;
    }
    p = open + 1;
    while (*p == L' ')
    {
        p++;
    }
    if (_wcsnicmp(p, L"Vote For", 8) == 0)
    {
        p += 8;
    }
    else if (any_label)
    {
        while ((*p >= L'A' && *p <= L'Z') || (*p >= L'a' && *p <= L'z') || *p == L' ')
        {
            p++;
        }
    }
    while (*p == L' ')
    {
        p++;
    }
    if (*p == L'-')
    {
        neg = TRUE; /* "(Vote For -1)": not set up in the county's system */
        p++;
    }
    for (; *p >= L'0' && *p <= L'9'; p++)
    {
        v = v * 10 + (uint32_t)(*p - L'0');
        digits = TRUE;
        if (v > 100000)
        {
            return;
        }
    }
    if (!digits || *p != L')' || p[1] != L'\0')
    {
        return;
    }
    *open = L'\0';
    wtrim(title);
    *vote_for = neg ? 0 : v;
}

/* Start a contest; @p title is copied. Returns its index. */
static uint32_t b_contest(ResBuilder *b, const wchar_t *title, uint32_t vote_for, int8_t isq)
{
    EeResults *r = b->r;
    EeResultsContest *c;
    if (!res_grow((void **)&r->contests,
                  &b->cap_contests,
                  (size_t)r->ncontests + 1,
                  sizeof(EeResultsContest)))
    {
        b->oom = TRUE;
        return r->ncontests ? r->ncontests - 1 : 0;
    }
    c = &r->contests[r->ncontests];
    ZeroMemory(c, sizeof(*c));
    c->title = wdup(title);
    if (c->title == NULL)
    {
        b->oom = TRUE;
    }
    c->vote_for = vote_for;
    c->is_question = isq;
    c->first_choice = r->nchoices;
    c->registered = EE_RES_UNKNOWN;
    c->ballots = EE_RES_UNKNOWN;
    c->precincts_total = EE_RES_UNKNOWN;
    c->precincts_reporting = EE_RES_UNKNOWN;
    return r->ncontests++;
}

/* Add a choice to the last contest; returns its local index. */
static uint32_t b_choice(ResBuilder *b, const wchar_t *name, const wchar_t *party, int64_t total)
{
    EeResults *r = b->r;
    EeResultsContest *c;
    EeResultsChoice *ch;
    if (r->ncontests == 0)
    {
        return 0;
    }
    c = &r->contests[r->ncontests - 1];
    if (!res_grow((void **)&r->choices,
                  &b->cap_choices,
                  (size_t)r->nchoices + 1,
                  sizeof(EeResultsChoice)))
    {
        b->oom = TRUE;
        return 0;
    }
    ch = &r->choices[r->nchoices];
    ch->name = wdup(name != NULL ? name : L"");
    ch->party = (party != NULL && party[0] != L'\0') ? wdup(party) : NULL;
    ch->total = total;
    if (ch->name == NULL)
    {
        b->oom = TRUE;
    }
    r->nchoices++;
    return c->nchoices++;
}

static void b_count(ResBuilder *b,
                    uint32_t contest,
                    uint32_t choice,
                    uint32_t pre,
                    uint32_t vt,
                    int64_t votes)
{
    ResEntry *e;
    if (!res_grow((void **)&b->ent, &b->cap_ent, b->nent + 1, sizeof(ResEntry)))
    {
        b->oom = TRUE;
        return;
    }
    e = &b->ent[b->nent++];
    e->contest = contest;
    e->choice = choice;
    e->pre = pre;
    e->vt = vt;
    e->votes = votes == RES_PROTECTED ? RES_PROTECTED : clamp32(votes);
}

static void b_turnout_vt(ResBuilder *b, uint32_t pre, uint32_t vt, int64_t ballots)
{
    ResEntry *e;
    if (!res_grow((void **)&b->tvt, &b->cap_tvt, b->ntvt + 1, sizeof(ResEntry)))
    {
        b->oom = TRUE;
        return;
    }
    e = &b->tvt[b->ntvt++];
    e->contest = 0;
    e->choice = 0;
    e->pre = pre;
    e->vt = vt;
    e->votes = clamp32(ballots);
}

static void b_vt_ballots_total(ResBuilder *b, uint32_t vt, int64_t ballots)
{
    size_t old = b->cap_vtb;
    if (!res_grow((void **)&b->vt_ballots_total, &b->cap_vtb, (size_t)vt + 1, sizeof(int64_t)))
    {
        b->oom = TRUE;
        return;
    }
    if (b->cap_vtb > old)
    {
        size_t i;
        for (i = old; i < b->cap_vtb; i++)
        {
            b->vt_ballots_total[i] = EE_RES_UNKNOWN;
        }
    }
    b->vt_ballots_total[vt] = ballots;
}

/* Primary party prefixes Clarity puts on contest titles (DEM United States Senator). */
static BOOL known_party_prefix(const wchar_t *tok, size_t n)
{
    static const wchar_t *const k[] = {L"DEM", L"REP", L"LIB", L"GRN", L"GRE"};
    size_t i;
    for (i = 0; i < ARRAYSIZE(k); i++)
    {
        if (wcslen(k[i]) == n && wcsncmp(tok, k[i], n) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* A contest's party: its title's first word when that is a party prefix every choice
 * carries, or a known primary prefix. */
static void derive_contest_party(EeResults *r, EeResultsContest *c)
{
    const wchar_t *t = c->title;
    size_t n = 0;
    uint32_t k;
    BOOL all = c->nchoices > 0;
    if (t == NULL)
    {
        return;
    }
    while (t[n] >= L'A' && t[n] <= L'Z')
    {
        n++;
    }
    if (n < 2 || n > 4 || t[n] != L' ')
    {
        return;
    }
    for (k = 0; k < c->nchoices; k++)
    {
        const wchar_t *p = r->choices[c->first_choice + k].party;
        if (p == NULL || wcslen(p) != n || wcsncmp(p, t, n) != 0)
        {
            all = FALSE;
            break;
        }
    }
    if (all || known_party_prefix(t, n))
    {
        c->party = wdupn(t, n);
    }
}

static int __cdecl ent_cmp(const void *pa, const void *pb)
{
    const ResEntry *a = (const ResEntry *)pa;
    const ResEntry *b = (const ResEntry *)pb;
    if (a->contest != b->contest)
    {
        return a->contest < b->contest ? -1 : 1;
    }
    return 0;
}

/* Turn the collected entries into the dense per-contest arrays and run the internal
 * consistency checks (choice totals vs precinct sums). */
static BOOL b_finish(ResBuilder *b)
{
    EeResults *r = b->r;
    uint32_t nvt = EE_RESULTS_NVT(r);
    uint32_t *local = NULL;
    uint32_t *order = NULL;
    size_t i = 0;
    uint32_t c;

    if (b->oom)
    {
        return FALSE;
    }
    /* Entries arrive contest by contest, but keep this robust to any order. */
    {
        BOOL sorted = TRUE;
        for (i = 1; i < b->nent; i++)
        {
            if (b->ent[i].contest < b->ent[i - 1].contest)
            {
                sorted = FALSE;
                break;
            }
        }
        if (!sorted)
        {
            qsort(b->ent, b->nent, sizeof(ResEntry), ent_cmp); /* contest order only */
        }
    }
    local = (uint32_t *)malloc(((size_t)r->nprecincts + 1) * sizeof(uint32_t));
    order = (uint32_t *)malloc(((size_t)r->nprecincts + 1) * sizeof(uint32_t));
    if (local == NULL || order == NULL)
    {
        free(local);
        free(order);
        return FALSE;
    }
    for (i = 0; i < r->nprecincts; i++)
    {
        local[i] = UINT32_MAX;
    }

    i = 0;
    for (c = 0; c < r->ncontests; c++)
    {
        EeResultsContest *ct = &r->contests[c];
        size_t s = i, e;
        size_t cells;
        uint32_t npre = 0;
        uint32_t k;
        BOOL any_ou = FALSE;
        while (i < b->nent && b->ent[i].contest < c)
        {
            i++; /* stray entry for a missing contest */
        }
        s = i;
        while (i < b->nent && b->ent[i].contest == c)
        {
            i++;
        }
        e = i;
        for (k = (uint32_t)s; k < e; k++)
        {
            uint32_t p = b->ent[k].pre;
            if (local[p] == UINT32_MAX)
            {
                local[p] = npre;
                order[npre++] = p;
            }
            if (b->ent[k].choice == RES_OVER || b->ent[k].choice == RES_UNDER)
            {
                any_ou = TRUE;
            }
        }
        cells = (size_t)ct->nchoices * npre * nvt;
        if (npre > 0)
        {
            ct->npre = npre;
            ct->pre = (uint32_t *)malloc((size_t)npre * sizeof(uint32_t));
            ct->counts = (int32_t *)calloc(cells ? cells : 1, sizeof(int32_t));
            if (ct->pre == NULL || ct->counts == NULL)
            {
                b->oom = TRUE;
                break;
            }
            memcpy(ct->pre, order, (size_t)npre * sizeof(uint32_t));
            if (any_ou)
            {
                ct->over = (int32_t *)calloc(npre, sizeof(int32_t));
                ct->under = (int32_t *)calloc(npre, sizeof(int32_t));
                if (ct->over == NULL || ct->under == NULL)
                {
                    b->oom = TRUE;
                    break;
                }
                ct->has_over_under = TRUE;
            }
            for (k = (uint32_t)s; k < e; k++)
            {
                const ResEntry *en = &b->ent[k];
                uint32_t lp = local[en->pre];
                if (en->votes == RES_PROTECTED)
                {
                    if (en->choice < ct->nchoices && en->vt < nvt)
                    {
                        if (ct->protected_cells == NULL)
                        {
                            ct->protected_cells = (uint8_t *)calloc(cells ? cells : 1, 1);
                            if (ct->protected_cells == NULL)
                            {
                                b->oom = TRUE;
                                break;
                            }
                        }
                        ct->protected_cells[((size_t)en->choice * npre + lp) * nvt + en->vt] = 1;
                        ct->nprotected++;
                    }
                    continue;
                }
                if (en->choice == RES_OVER)
                {
                    ct->over[lp] += en->votes;
                }
                else if (en->choice == RES_UNDER)
                {
                    ct->under[lp] += en->votes;
                }
                else if (en->choice < ct->nchoices && en->vt < nvt)
                {
                    ct->counts[((size_t)en->choice * npre + lp) * nvt + en->vt] += en->votes;
                }
            }
            for (k = 0; k < npre; k++)
            {
                local[order[k]] = UINT32_MAX;
            }
            /* Choice totals: reported vs the precinct sum. */
            for (k = 0; k < ct->nchoices; k++)
            {
                EeResultsChoice *ch = &r->choices[ct->first_choice + k];
                const int32_t *row = ct->counts + (size_t)k * npre * nvt;
                int64_t sum = 0;
                size_t q;
                for (q = 0; q < (size_t)npre * nvt; q++)
                {
                    sum += row[q];
                }
                if (ch->total == EE_RES_UNKNOWN)
                {
                    /* not reported: the precinct sum, unless cells were withheld (then the
                     * total stays unknown until another file supplies it) */
                    if (ct->nprotected == 0)
                    {
                        ch->total = sum;
                    }
                }
                else if (ch->total != sum && ct->nprotected == 0)
                {
                    wchar_t msg[600], a[32], bb[32];
                    fmt_count(ch->total, a, ARRAYSIZE(a));
                    fmt_count(sum, bb, ARRAYSIZE(bb));
                    StringCchPrintfW(msg,
                                     ARRAYSIZE(msg),
                                     L"%s \x2014 %s: the reported total %s differs from the sum "
                                     L"of its precincts, %s.",
                                     ct->title,
                                     ch->name,
                                     a,
                                     bb);
                    b_issue(b, msg);
                }
            }
            if (any_ou)
            {
                int64_t so = 0, su = 0;
                for (k = 0; k < npre; k++)
                {
                    so += ct->over[k];
                    su += ct->under[k];
                }
                if (ct->over_total == 0 && ct->under_total == 0)
                {
                    ct->over_total = so;
                    ct->under_total = su;
                }
            }
        }
        else
        {
            for (k = 0; k < ct->nchoices; k++)
            {
                if (r->choices[ct->first_choice + k].total == EE_RES_UNKNOWN)
                {
                    r->choices[ct->first_choice + k].total = 0;
                }
            }
        }
        derive_contest_party(r, ct);
    }

    {
        uint64_t prot = 0;
        for (c = 0; c < r->ncontests; c++)
        {
            prot += r->contests[c].nprotected;
        }
        if (prot > 0)
        {
            wchar_t msg[300], a[32];
            fmt_count((int64_t)prot, a, ARRAYSIZE(a));
            StringCchPrintfW(msg,
                             ARRAYSIZE(msg),
                             L"%s precinct counts are withheld (\"protected\") in the file; they "
                             L"count as 0, so precinct sums can be below the reported totals.",
                             a);
            b_issue(b, msg);
        }
    }

    /* Turnout ballots by vote type. */
    if (!b->oom && b->ntvt > 0 && r->nvt > 0 && r->nprecincts > 0)
    {
        r->prec_ballots_vt = (int32_t *)calloc((size_t)r->nprecincts * r->nvt, sizeof(int32_t));
        r->ballots_vt = (int64_t *)calloc(r->nvt, sizeof(int64_t));
        if (r->prec_ballots_vt == NULL || r->ballots_vt == NULL)
        {
            b->oom = TRUE;
        }
        else
        {
            for (i = 0; i < b->ntvt; i++)
            {
                const ResEntry *en = &b->tvt[i];
                if (en->pre < r->nprecincts && en->vt < r->nvt)
                {
                    r->prec_ballots_vt[(size_t)en->pre * r->nvt + en->vt] += en->votes;
                    r->ballots_vt[en->vt] += en->votes;
                }
            }
            for (c = 0; c < r->nvt && b->vt_ballots_total != NULL; c++)
            {
                if (c < b->cap_vtb && b->vt_ballots_total[c] != EE_RES_UNKNOWN &&
                    b->vt_ballots_total[c] != r->ballots_vt[c])
                {
                    wchar_t msg[300], a[32], bb[32];
                    fmt_count(b->vt_ballots_total[c], a, ARRAYSIZE(a));
                    fmt_count(r->ballots_vt[c], bb, ARRAYSIZE(bb));
                    StringCchPrintfW(msg,
                                     ARRAYSIZE(msg),
                                     L"Ballots cast (%s): the reported total %s differs from the "
                                     L"sum of its precincts, %s.",
                                     r->vote_types[c],
                                     a,
                                     bb);
                    b_issue(b, msg);
                }
            }
        }
    }
    /* Overall turnout from the precincts when no total was reported. */
    if (r->nprecincts > 0)
    {
        int64_t reg = 0, bal = 0;
        BOOL have_reg = TRUE, have_bal = TRUE;
        for (c = 0; c < r->nprecincts; c++)
        {
            if (r->precincts[c].registered == EE_RES_UNKNOWN)
            {
                have_reg = FALSE;
            }
            else
            {
                reg += r->precincts[c].registered;
            }
            if (r->precincts[c].ballots == EE_RES_UNKNOWN)
            {
                have_bal = FALSE;
            }
            else
            {
                bal += r->precincts[c].ballots;
            }
        }
        if (r->registered_total == EE_RES_UNKNOWN && have_reg)
        {
            r->registered_total = reg;
        }
        else if (have_reg && r->registered_total != reg)
        {
            /* Clarity's XML total can differ from its own precinct sum (about half of it in a
             * primary); note it, since the TXT/XLS totals are that sum. */
            wchar_t msg[300], a[32], bb[32];
            fmt_count(r->registered_total, a, ARRAYSIZE(a));
            fmt_count(reg, bb, ARRAYSIZE(bb));
            StringCchPrintfW(msg,
                             ARRAYSIZE(msg),
                             L"Registered voters: the reported total %s is not the sum of the "
                             L"precincts' registered voters, %s.",
                             a,
                             bb);
            b_issue(b, msg);
        }
        if (r->ballots_total == EE_RES_UNKNOWN && have_bal)
        {
            r->ballots_total = bal;
        }
    }
    free(local);
    free(order);
    return !b->oom;
}

/* -------------------------------------------------------------------------- */
/* Clarity summary.csv                                                        */
/* -------------------------------------------------------------------------- */

typedef struct SumCtx
{
    ResBuilder *b;
    BOOL header_done;
    int col_contest, col_choice, col_party, col_total, col_pct, col_reg, col_bal;
    int col_ptot, col_prpt, col_over, col_under;
    char *cur_raw; /* current contest's raw name (UTF-8) */
    double pct_sum;
    int64_t cur_reg, cur_bal;
    BOOL any_ou; /* any non-zero over/under value in the file */
} SumCtx;

static double parse_pct(const char *s)
{
    return (s != NULL && s[0] != '\0') ? atof(s) : 0.0;
}

static const char *cell_at(const char *const *cells, uint32_t n, int col)
{
    return (col >= 0 && (uint32_t)col < n) ? cells[col] : NULL;
}

static int64_t cell_i64(const char *const *cells, uint32_t n, int col)
{
    const char *s = cell_at(cells, n, col);
    int64_t v;
    if (s == NULL || !parse_i64a(s, strlen(s), &v))
    {
        return EE_RES_UNKNOWN;
    }
    return v;
}

static BOOL sum_header(SumCtx *sc, const char *const *cells, uint32_t n)
{
    uint32_t i;
    sc->col_contest = sc->col_choice = sc->col_party = sc->col_total = sc->col_pct = -1;
    sc->col_reg = sc->col_bal = sc->col_ptot = sc->col_prpt = sc->col_over = sc->col_under = -1;
    for (i = 0; i < n; i++)
    {
        const char *c = cells[i];
        size_t len = strlen(c);
        if (_stricmp(c, "contest name") == 0)
            sc->col_contest = (int)i;
        else if (_stricmp(c, "choice name") == 0)
            sc->col_choice = (int)i;
        else if (_stricmp(c, "party name") == 0)
            sc->col_party = (int)i;
        else if (_stricmp(c, "total votes") == 0)
            sc->col_total = (int)i;
        else if (_stricmp(c, "percent of votes") == 0)
            sc->col_pct = (int)i;
        else if (_stricmp(c, "registered voters") == 0)
            sc->col_reg = (int)i;
        else if (_stricmp(c, "ballots cast") == 0)
            sc->col_bal = (int)i;
        else if (_stricmp(c, "over votes") == 0)
            sc->col_over = (int)i;
        else if (_stricmp(c, "under votes") == 0)
            sc->col_under = (int)i;
        else if (len > 10 && _strnicmp(c, "num ", 4) == 0 && _stricmp(c + len - 6, " total") == 0)
            sc->col_ptot = (int)i;
        else if (len > 9 && _strnicmp(c, "num ", 4) == 0 && _stricmp(c + len - 5, " rptg") == 0)
            sc->col_prpt = (int)i;
    }
    return sc->col_contest >= 0 && sc->col_choice >= 0 && sc->col_total >= 0;
}

static BOOL sum_sink(void *ctx, const char *const *cells, uint32_t n)
{
    SumCtx *sc = (SumCtx *)ctx;
    ResBuilder *b = sc->b;
    EeResults *r = b->r;
    const char *raw, *choice;
    wchar_t *wchoice, *wparty;
    int64_t total, reg, bal;
    double pct;
    BOOL fresh;

    if (!sc->header_done)
    {
        if (n == 0)
        {
            return TRUE;
        }
        if (!sum_header(sc, cells, n))
        {
            b_fail(b,
                   L"The summary file's header has no contest name, choice name or total "
                   L"votes column.");
            return FALSE;
        }
        sc->header_done = TRUE;
        return TRUE;
    }
    raw = cell_at(cells, n, sc->col_contest);
    choice = cell_at(cells, n, sc->col_choice);
    if (raw == NULL || raw[0] == '\0' || choice == NULL)
    {
        return TRUE; /* blank line */
    }
    total = cell_i64(cells, n, sc->col_total);
    if (total == EE_RES_UNKNOWN)
    {
        total = 0;
    }
    pct = parse_pct(cell_at(cells, n, sc->col_pct));
    reg = cell_i64(cells, n, sc->col_reg);
    bal = cell_i64(cells, n, sc->col_bal);
    wchoice = u8dup(choice, -1);
    if (wchoice == NULL)
    {
        b->oom = TRUE;
        return FALSE;
    }
    wtrim(wchoice);

    /* A new contest starts when the name changes, or (for back-to-back contests with the
     * same name) when a choice repeats, the contest figures change or the percentages
     * pass 100. */
    fresh = sc->cur_raw == NULL || strcmp(sc->cur_raw, raw) != 0;
    if (!fresh && r->ncontests > 0)
    {
        const EeResultsContest *c = &r->contests[r->ncontests - 1];
        uint32_t k;
        for (k = 0; k < c->nchoices && !fresh; k++)
        {
            if (wcscmp(r->choices[c->first_choice + k].name, wchoice) == 0)
            {
                fresh = TRUE;
            }
        }
        if (reg != sc->cur_reg || bal != sc->cur_bal)
        {
            fresh = TRUE;
        }
        if (sc->col_pct >= 0 && pct > 0.0 &&
            sc->pct_sum + pct > 100.0 + 0.01 + 0.006 * (double)c->nchoices)
        {
            fresh = TRUE;
        }
    }
    if (fresh)
    {
        wchar_t *title = u8dup(raw, -1);
        uint32_t vote_for = 0;
        uint32_t ci;
        EeResultsContest *c;
        int64_t v;
        if (title == NULL)
        {
            free(wchoice);
            b->oom = TRUE;
            return FALSE;
        }
        split_title(title, &vote_for, TRUE);
        ci = b_contest(b, title, vote_for, -1);
        free(title);
        if (b->oom)
        {
            free(wchoice);
            return FALSE;
        }
        c = &r->contests[ci];
        c->registered = reg;
        c->ballots = bal;
        v = cell_i64(cells, n, sc->col_ptot);
        c->precincts_total = v == EE_RES_UNKNOWN ? EE_RES_UNKNOWN : clamp32(v);
        v = cell_i64(cells, n, sc->col_prpt);
        c->precincts_reporting = v == EE_RES_UNKNOWN ? EE_RES_UNKNOWN : clamp32(v);
        v = cell_i64(cells, n, sc->col_over);
        c->over_total = v == EE_RES_UNKNOWN ? 0 : v;
        v = cell_i64(cells, n, sc->col_under);
        c->under_total = v == EE_RES_UNKNOWN ? 0 : v;
        if (c->over_total != 0 || c->under_total != 0)
        {
            sc->any_ou = TRUE;
        }
        free(sc->cur_raw);
        sc->cur_raw = _strdup(raw);
        sc->pct_sum = 0.0;
        sc->cur_reg = reg;
        sc->cur_bal = bal;
    }
    sc->pct_sum += pct;
    wparty = sc->col_party >= 0 ? u8dup(cell_at(cells, n, sc->col_party), -1) : NULL;
    if (wparty != NULL)
    {
        wtrim(wparty);
    }
    b_choice(b, wchoice, wparty, total);
    free(wchoice);
    free(wparty);
    return !b->oom;
}

static void parse_clarity_summary(ResBuilder *b, const uint8_t *d, size_t n, const wchar_t *name)
{
    SumCtx sc;
    wchar_t err[256] = L"";
    EeLoadStatus st;
    uint32_t c;
    ZeroMemory(&sc, sizeof(sc));
    sc.b = b;
    st = EeCsv_ReadSheetMem(d, n, name, sum_sink, &sc, NULL, NULL, NULL, err, ARRAYSIZE(err));
    free(sc.cur_raw);
    if (st != EeLoadStatus_Ok && !b->bad && !b->oom)
    {
        b_fail(b, err[0] ? err : L"The summary file could not be read.");
    }
    /* Over/under columns: Travis fills them with 0 (not reported); a file with any
     * non-zero value reports them for every contest. */
    for (c = 0; c < b->r->ncontests; c++)
    {
        EeResultsContest *ct = &b->r->contests[c];
        ct->has_over_under = sc.any_ou && sc.col_over >= 0 && sc.col_under >= 0;
        if (!ct->has_over_under)
        {
            ct->over_total = 0;
            ct->under_total = 0;
        }
    }
}

/* -------------------------------------------------------------------------- */
/* Clarity detail.xml                                                         */
/* -------------------------------------------------------------------------- */

/* Append the UTF-8 encoding of @p cp to buf[*k] (bounded by cap). */
static void put_utf8(char *buf, size_t cap, size_t *k, uint32_t cp)
{
    char tmp[4];
    size_t n, i;
    if (cp < 0x80)
    {
        tmp[0] = (char)cp;
        n = 1;
    }
    else if (cp < 0x800)
    {
        tmp[0] = (char)(0xC0 | (cp >> 6));
        tmp[1] = (char)(0x80 | (cp & 0x3F));
        n = 2;
    }
    else if (cp < 0x10000)
    {
        tmp[0] = (char)(0xE0 | (cp >> 12));
        tmp[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        tmp[2] = (char)(0x80 | (cp & 0x3F));
        n = 3;
    }
    else
    {
        tmp[0] = (char)(0xF0 | (cp >> 18));
        tmp[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
        tmp[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
        tmp[3] = (char)(0x80 | (cp & 0x3F));
        n = 4;
    }
    for (i = 0; i < n && *k + 1 < cap; i++)
    {
        buf[(*k)++] = tmp[i];
    }
}

/* Decode XML text s[0..n) (entities) into buf as UTF-8. */
static void xml_decode(const char *s, size_t n, char *buf, size_t cap)
{
    size_t i = 0, k = 0;
    while (i < n && k + 1 < cap)
    {
        if (s[i] == '&')
        {
            const char *semi = memchr(s + i, ';', n - i < 12 ? n - i : 12);
            if (semi != NULL)
            {
                size_t len = (size_t)(semi - (s + i)) - 1;
                const char *e = s + i + 1;
                uint32_t cp = 0;
                BOOL ok = TRUE;
                if (len == 3 && strncmp(e, "amp", 3) == 0)
                    cp = '&';
                else if (len == 2 && strncmp(e, "lt", 2) == 0)
                    cp = '<';
                else if (len == 2 && strncmp(e, "gt", 2) == 0)
                    cp = '>';
                else if (len == 4 && strncmp(e, "quot", 4) == 0)
                    cp = '"';
                else if (len == 4 && strncmp(e, "apos", 4) == 0)
                    cp = '\'';
                else if (len >= 2 && len < 11 && e[0] == '#')
                {
                    char num[12];
                    memcpy(num, e + 1, len - 1);
                    num[len - 1] = '\0';
                    cp = (num[0] == 'x' || num[0] == 'X') ? (uint32_t)strtoul(num + 1, NULL, 16)
                                                          : (uint32_t)strtoul(num, NULL, 10);
                    ok = cp > 0 && cp <= 0x10FFFF;
                }
                else
                {
                    ok = FALSE;
                }
                if (ok)
                {
                    put_utf8(buf, cap, &k, cp);
                    i = (size_t)(semi - s) + 1;
                    continue;
                }
            }
        }
        buf[k++] = s[i++];
    }
    buf[k] = '\0';
}

/* Find attribute @p name in the start tag s..e and decode its value into buf. */
static BOOL xattr(const char *s, const char *e, const char *name, char *buf, size_t cap)
{
    size_t nl = strlen(name);
    const char *q;
    buf[0] = '\0';
    for (q = s + 1; q + nl + 2 < e; q++)
    {
        if ((q[-1] == ' ' || q[-1] == '\t' || q[-1] == '\r' || q[-1] == '\n') &&
            strncmp(q, name, nl) == 0)
        {
            const char *v = q + nl;
            while (v < e && (*v == ' ' || *v == '\t'))
            {
                v++;
            }
            if (v < e && *v == '=')
            {
                char quote;
                const char *ve;
                v++;
                while (v < e && (*v == ' ' || *v == '\t'))
                {
                    v++;
                }
                if (v >= e || (*v != '"' && *v != '\''))
                {
                    return FALSE;
                }
                quote = *v++;
                ve = memchr(v, quote, (size_t)(e - v));
                if (ve == NULL)
                {
                    return FALSE;
                }
                xml_decode(v, (size_t)(ve - v), buf, cap);
                return TRUE;
            }
        }
    }
    return FALSE;
}

static int64_t xattr_i64(const char *s, const char *e, const char *name)
{
    char buf[48];
    int64_t v;
    if (!xattr(s, e, name, buf, sizeof(buf)) || !parse_i64a(buf, strlen(buf), &v))
    {
        return EE_RES_UNKNOWN;
    }
    return v;
}

static wchar_t *xattr_w(const char *s, const char *e, const char *name)
{
    char buf[2048];
    wchar_t *w;
    if (!xattr(s, e, name, buf, sizeof(buf)))
    {
        return NULL;
    }
    w = u8dup(buf, -1);
    if (w != NULL)
    {
        wtrim(w);
    }
    return w;
}

static BOOL tag_is(const char *name, size_t nlen, const char *want)
{
    return strlen(want) == nlen && memcmp(name, want, nlen) == 0;
}

/* The '>' ending a tag that starts before s, skipping quoted attribute values. */
static const char *tag_end(const char *s, const char *end)
{
    char quote = 0;
    for (; s < end; s++)
    {
        if (quote != 0)
        {
            if (*s == quote)
            {
                quote = 0;
            }
        }
        else if (*s == '"' || *s == '\'')
        {
            quote = *s;
        }
        else if (*s == '>')
        {
            return s;
        }
    }
    return NULL;
}

static void parse_clarity_xml(ResBuilder *b, const char *x, size_t n)
{
    EeResults *r = b->r;
    const char *end = x + n;
    const char *p = x;
    BOOL in_turnout = FALSE;
    int64_t contest = -1;
    int64_t choice = -1;
    int mode = 0; /* 0 none, 1 choice vote type, 2 over, 3 under, 4 skip */
    uint32_t cur_vt = 0;

    if (n >= 3 && (uint8_t)x[0] == 0xEF && (uint8_t)x[1] == 0xBB && (uint8_t)x[2] == 0xBF)
    {
        p += 3;
    }
    while (p < end && !b->oom)
    {
        const char *lt = memchr(p, '<', (size_t)(end - p));
        const char *gt;
        const char *name;
        size_t nlen = 0;
        BOOL closing, self_close;
        if (lt == NULL)
        {
            break;
        }
        if (lt + 4 <= end && strncmp(lt, "<!--", 4) == 0)
        {
            const char *ce = NULL;
            const char *q;
            for (q = lt + 4; q + 3 <= end; q++)
            {
                if (q[0] == '-' && q[1] == '-' && q[2] == '>')
                {
                    ce = q;
                    break;
                }
            }
            p = ce != NULL ? ce + 3 : end;
            continue;
        }
        gt = tag_end(lt + 1, end);
        if (gt == NULL)
        {
            break;
        }
        p = gt + 1;
        if (lt[1] == '?' || lt[1] == '!')
        {
            continue;
        }
        closing = lt[1] == '/';
        name = lt + (closing ? 2 : 1);
        while (name + nlen < gt && name[nlen] != ' ' && name[nlen] != '\t' && name[nlen] != '\r' &&
               name[nlen] != '\n' && name[nlen] != '/')
        {
            nlen++;
        }
        self_close = gt[-1] == '/';
        if (closing)
        {
            if (tag_is(name, nlen, "VoterTurnout"))
            {
                in_turnout = FALSE;
            }
            else if (tag_is(name, nlen, "Contest"))
            {
                contest = -1;
                choice = -1;
                mode = 0;
            }
            else if (tag_is(name, nlen, "Choice"))
            {
                choice = -1;
                mode = 0;
            }
            else if (tag_is(name, nlen, "VoteType"))
            {
                mode = 0;
            }
            continue;
        }
        if (tag_is(name, nlen, "Precinct"))
        {
            wchar_t *pn = xattr_w(lt, gt, "name");
            if (pn == NULL)
            {
                continue;
            }
            if (contest >= 0 && mode >= 1 && mode <= 3)
            {
                int64_t v = xattr_i64(lt, gt, "votes");
                uint32_t pi = b_pre(b, pn);
                if (v == EE_RES_UNKNOWN)
                {
                    char vb[32];
                    v = (xattr(lt, gt, "votes", vb, sizeof(vb)) && _stricmp(vb, "protected") == 0)
                            ? RES_PROTECTED
                            : 0;
                }
                if (mode == 1)
                {
                    b_count(b, (uint32_t)contest, (uint32_t)choice, pi, cur_vt, v);
                }
                else
                {
                    b_count(b, (uint32_t)contest, mode == 2 ? RES_OVER : RES_UNDER, pi, 0, v);
                }
            }
            else if (in_turnout && contest < 0)
            {
                uint32_t pi = b_pre(b, pn);
                if (!b->oom)
                {
                    r->precincts[pi].registered = xattr_i64(lt, gt, "totalVoters");
                    r->precincts[pi].ballots = xattr_i64(lt, gt, "ballotsCast");
                }
            }
            free(pn);
        }
        else if (tag_is(name, nlen, "VoteType"))
        {
            wchar_t *vn = xattr_w(lt, gt, "name");
            if (vn == NULL)
            {
                mode = 4;
            }
            else if (choice >= 0)
            {
                cur_vt = b_vt(b, vn);
                mode = 1;
            }
            else if (contest >= 0)
            {
                EeResultsContest *c = &r->contests[contest];
                int64_t v = xattr_i64(lt, gt, "votes");
                if (wieq(vn, L"Undervotes") || wieq(vn, L"Under Votes"))
                {
                    mode = 3;
                    c->has_over_under = TRUE;
                    c->under_total = v == EE_RES_UNKNOWN ? 0 : v;
                }
                else if (wieq(vn, L"Overvotes") || wieq(vn, L"Over Votes"))
                {
                    mode = 2;
                    c->has_over_under = TRUE;
                    c->over_total = v == EE_RES_UNKNOWN ? 0 : v;
                }
                else
                {
                    mode = 4; /* regVotersCounty and other contest-level placeholders */
                }
            }
            free(vn);
            if (self_close)
            {
                mode = 0;
            }
        }
        else if (tag_is(name, nlen, "Choice"))
        {
            wchar_t *cn = xattr_w(lt, gt, "text");
            wchar_t *party = xattr_w(lt, gt, "party");
            if (contest >= 0)
            {
                choice = b_choice(b, cn != NULL ? cn : L"", party, xattr_i64(lt, gt, "totalVotes"));
            }
            free(cn);
            free(party);
            if (self_close)
            {
                choice = -1;
            }
        }
        else if (tag_is(name, nlen, "Contest"))
        {
            wchar_t *t = xattr_w(lt, gt, "text");
            char q[16];
            int64_t vf = xattr_i64(lt, gt, "voteFor");
            int8_t isq = -1;
            if (xattr(lt, gt, "isQuestion", q, sizeof(q)))
            {
                isq = (_stricmp(q, "true") == 0) ? 1 : 0;
            }
            contest = b_contest(b, t != NULL ? t : L"", vf > 0 ? (uint32_t)vf : 0, isq);
            free(t);
            choice = -1;
            mode = 0;
            if (self_close)
            {
                contest = -1;
            }
        }
        else if (tag_is(name, nlen, "VoterTurnout"))
        {
            r->registered_total = xattr_i64(lt, gt, "totalVoters");
            r->ballots_total = xattr_i64(lt, gt, "ballotsCast");
            in_turnout = !self_close;
        }
        else if (!self_close &&
                 (tag_is(name, nlen, "ElectionName") || tag_is(name, nlen, "ElectionDate") ||
                  tag_is(name, nlen, "Region") || tag_is(name, nlen, "Timestamp")))
        {
            const char *te = memchr(p, '<', (size_t)(end - p));
            char buf[1024];
            wchar_t **dst = tag_is(name, nlen, "ElectionName")   ? &r->election_name
                            : tag_is(name, nlen, "ElectionDate") ? &r->election_date
                            : tag_is(name, nlen, "Region")       ? &r->region
                                                                 : &r->timestamp;
            if (te == NULL)
            {
                break;
            }
            xml_decode(p, (size_t)(te - p), buf, sizeof(buf));
            free(*dst);
            *dst = u8dup(buf, -1);
            if (*dst != NULL)
            {
                wtrim(*dst);
            }
            p = te;
        }
    }
    if (r->ncontests == 0 && !b->oom)
    {
        b_fail(b, L"The XML file holds no Clarity <Contest> results.");
    }
}

/* -------------------------------------------------------------------------- */
/* Clarity detail.txt and detail.xls(x): precinct grids                       */
/* -------------------------------------------------------------------------- */

enum
{
    GR_NONE = 0,
    GR_REG,          /* registered voters */
    GR_VOTE,         /* one choice, one vote type */
    GR_CHOICE_TOTAL, /* one choice's total */
    GR_TOTAL,        /* the contest total */
    GR_BALLOTS_VT,   /* turnout: ballots cast by vote type */
    GR_BALLOTS       /* turnout: ballots cast */
};

#define GRID_MAX_COLS 4096

typedef struct Grid
{
    BOOL turnout;
    uint32_t ncols;
    uint32_t nchoices;
    uint8_t role[GRID_MAX_COLS];
    uint32_t vt[GRID_MAX_COLS];
    uint32_t choice[GRID_MAX_COLS];
    uint32_t gstart[GRID_MAX_COLS]; /* per choice: first and last (total) column */
    uint32_t gend[GRID_MAX_COLS];
} Grid;

static BOOL is_total_label(const wchar_t *s)
{
    return wieq(s, L"Total") || wieq(s, L"Total Votes") || wieq(s, L"Totals") ||
           wieq(s, L"Choice Total");
}

/* The label of a grid's first column: "Precinct", or "County" / "Area" in setups that
 * report by another unit name. */
static BOOL is_area_label(const wchar_t *s)
{
    return wieq(s, L"Precinct") || wieq(s, L"County") || wieq(s, L"Area");
}

/* A grid header row: the area label (or a blank first cell) then "Registered Voters", or
 * the area label then any column titles (older exports have no Registered Voters). */
static BOOL is_grid_header(wchar_t *const *cells, uint32_t n)
{
    return n >= 2 && (is_area_label(cells[0]) || wieq(cells[1], L"Registered Voters"));
}

static BOOL is_total_row(const wchar_t *s)
{
    return wieq(s, L"Total:") || wieq(s, L"Totals:") || wieq(s, L"Total") || wieq(s, L"Totals");
}

/* Read a header row (cells trimmed). Returns FALSE when it is not a Clarity grid header. */
static BOOL grid_header(Grid *g, ResBuilder *b, wchar_t *const *cells, uint32_t n)
{
    uint32_t i;
    BOOL in_group = FALSE;
    ZeroMemory(g->role, sizeof(g->role));
    g->ncols = n < GRID_MAX_COLS ? n : GRID_MAX_COLS;
    g->nchoices = 0;
    g->turnout = FALSE;
    if (!is_grid_header(cells, g->ncols))
    {
        return FALSE;
    }
    for (i = 1; i < g->ncols; i++)
    {
        if (wieq(cells[i], L"Ballots Cast"))
        {
            g->turnout = TRUE;
        }
    }
    if (g->turnout)
    {
        BOOL before = TRUE;
        for (i = 1; i < g->ncols; i++)
        {
            const wchar_t *c = cells[i];
            if (wieq(c, L"Registered Voters"))
            {
                g->role[i] = GR_REG;
            }
            else if (wieq(c, L"Ballots Cast"))
            {
                g->role[i] = GR_BALLOTS;
                before = FALSE;
            }
            else if (before && c[0] != L'\0' && !wieq(c, L"Voter Turnout"))
            {
                g->role[i] = GR_BALLOTS_VT;
                g->vt[i] = b_vt(b, c);
            }
        }
        return TRUE;
    }
    for (i = 1; i < g->ncols; i++)
    {
        const wchar_t *c = cells[i];
        if (i == 1 && wieq(c, L"Registered Voters"))
        {
            g->role[i] = GR_REG;
        }
        else if (is_total_label(c))
        {
            if (in_group)
            {
                g->role[i] = GR_CHOICE_TOTAL;
                g->choice[i] = g->nchoices;
                g->gend[g->nchoices] = i;
                g->nchoices++;
                in_group = FALSE;
            }
            else
            {
                g->role[i] = GR_TOTAL;
            }
        }
        else if (c[0] != L'\0')
        {
            if (!in_group)
            {
                g->gstart[g->nchoices] = i;
                in_group = TRUE;
            }
            g->role[i] = GR_VOTE;
            g->vt[i] = b_vt(b, c);
            g->choice[i] = g->nchoices;
        }
    }
    return g->nchoices > 0;
}

/* One data row (cells[0] = precinct, trimmed). */
static void grid_row(Grid *g, ResBuilder *b, uint32_t contest, wchar_t *const *cells, uint32_t n)
{
    EeResults *r = b->r;
    BOOL total = is_total_row(cells[0]);
    uint32_t pi = 0;
    uint32_t i;
    if (cells[0][0] == L'\0')
    {
        return;
    }
    if (!total)
    {
        pi = b_pre(b, cells[0]);
        if (b->oom)
        {
            return;
        }
    }
    for (i = 1; i < n && i < g->ncols; i++)
    {
        int64_t v;
        if (g->role[i] == GR_NONE || g->role[i] == GR_TOTAL)
        {
            continue;
        }
        if (!parse_i64w(cells[i], &v))
        {
            if (wieq(cells[i], L"protected"))
            {
                /* a withheld count: a vote cell is recorded as withheld; a turnout or
                 * total cell stays unknown */
                if (g->role[i] == GR_VOTE && !total)
                {
                    b_count(b, contest, g->choice[i], pi, g->vt[i], RES_PROTECTED);
                }
            }
            else if (cells[i][0] != L'\0')
            {
                wchar_t msg[400];
                StringCchPrintfW(msg,
                                 ARRAYSIZE(msg),
                                 L"Precinct %s: \"%s\" is not a number.",
                                 cells[0],
                                 cells[i]);
                b_issue(b, msg);
            }
            continue;
        }
        switch (g->role[i])
        {
            case GR_REG:
                if (total)
                {
                    if (g->turnout)
                    {
                        r->registered_total = v;
                    }
                }
                else if (g->turnout || r->precincts[pi].registered == EE_RES_UNKNOWN)
                {
                    r->precincts[pi].registered = v;
                }
                break;
            case GR_BALLOTS:
                if (total)
                {
                    r->ballots_total = v;
                }
                else
                {
                    r->precincts[pi].ballots = v;
                }
                break;
            case GR_BALLOTS_VT:
                if (total)
                {
                    b_vt_ballots_total(b, g->vt[i], v);
                }
                else
                {
                    b_turnout_vt(b, pi, g->vt[i], v);
                }
                break;
            case GR_VOTE:
                if (!total)
                {
                    b_count(b, contest, g->choice[i], pi, g->vt[i], v);
                }
                break;
            case GR_CHOICE_TOTAL:
                if (total && contest < r->ncontests && g->choice[i] < r->contests[contest].nchoices)
                {
                    r->choices[r->contests[contest].first_choice + g->choice[i]].total = v;
                }
                break;
            default:
                break;
        }
    }
}

/* Start a contest from a grid header: choices named from the names row. */
static uint32_t grid_contest(Grid *g,
                             ResBuilder *b,
                             const wchar_t *title_raw,
                             wchar_t *const *names,
                             uint32_t nnames)
{
    wchar_t *title = wdup(title_raw);
    uint32_t vote_for = 0;
    uint32_t ci, k;
    if (title == NULL)
    {
        b->oom = TRUE;
        return 0;
    }
    split_title(title, &vote_for, TRUE);
    ci = b_contest(b, title, vote_for, -1);
    free(title);
    for (k = 0; k < g->nchoices; k++)
    {
        const wchar_t *nm = L"";
        uint32_t c;
        for (c = g->gstart[k]; c <= g->gend[k] && c < nnames; c++)
        {
            if (names[c] != NULL && names[c][0] != L'\0')
            {
                nm = names[c];
                break;
            }
        }
        b_choice(b, nm, NULL, EE_RES_UNKNOWN);
    }
    return ci;
}

/* --- detail.txt (fixed 30-character columns) ------------------------------ */

#define TXT_COL 30

typedef struct TxtLine
{
    const wchar_t *s;
    size_t n; /* without trailing whitespace */
} TxtLine;

/* Could @p ln be a grid header: "Registered Voters" in the second column, or the area
 * label alone in the first? */
static BOOL txt_header_line(const TxtLine *ln)
{
    size_t k;
    if (ln->n >= TXT_COL + 17 && wcsncmp(ln->s + TXT_COL, L"Registered Voters", 17) == 0)
    {
        return TRUE;
    }
    if (ln->n <= TXT_COL)
    {
        return FALSE;
    }
    for (k = 0; k < 3; k++)
    {
        static const wchar_t *const k_lab[] = {L"Precinct", L"County", L"Area"};
        size_t n = wcslen(k_lab[k]);
        size_t i;
        if (wcsncmp(ln->s, k_lab[k], n) != 0)
        {
            continue;
        }
        for (i = n; i < TXT_COL && is_ws(ln->s[i]); i++)
        {
        }
        if (i == TXT_COL && !is_ws(ln->s[TXT_COL]))
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* Split @p ln into 30-character cells (trimmed) in scratch storage. */
static uint32_t txt_cells(const TxtLine *ln, wchar_t *scratch, wchar_t **cells, uint32_t maxc)
{
    uint32_t nc = (uint32_t)((ln->n + TXT_COL - 1) / TXT_COL);
    uint32_t k;
    wchar_t *w = scratch;
    if (nc > maxc)
    {
        nc = maxc;
    }
    for (k = 0; k < nc; k++)
    {
        size_t a = (size_t)k * TXT_COL;
        size_t e = a + TXT_COL < ln->n ? a + TXT_COL : ln->n;
        size_t len;
        while (a < e && is_ws(ln->s[a]))
        {
            a++;
        }
        while (e > a && is_ws(ln->s[e - 1]))
        {
            e--;
        }
        len = e - a;
        memcpy(w, ln->s + a, len * sizeof(wchar_t));
        w[len] = L'\0';
        cells[k] = w;
        w += len + 1;
    }
    return nc;
}

/* A detail.txt value: digits (with , or .), a percentage, or "protected". */
static BOOL txt_value_token(const wchar_t *s, size_t n)
{
    size_t i;
    BOOL digit = FALSE;
    if (n == 9 && _wcsnicmp(s, L"protected", 9) == 0)
    {
        return TRUE;
    }
    for (i = 0; i < n; i++)
    {
        if (s[i] >= L'0' && s[i] <= L'9')
        {
            digit = TRUE;
        }
        else if (s[i] != L',' && s[i] != L'.' && s[i] != L'%' && !(i == 0 && s[i] == L'-'))
        {
            return FALSE;
        }
    }
    return digit;
}

/* Split a data row into the precinct name and its last @p ntok values. A precinct name
 * longer than its 30-character column overwrites the values after it, so when fewer
 * than @p ntok values follow the name, the leftmost value cells are left blank. */
static BOOL txt_split_right(const TxtLine *ln,
                            uint32_t ntok,
                            size_t *pos,
                            wchar_t *scratch,
                            wchar_t **cells)
{
    size_t e = ln->n;
    size_t *starts = pos;
    size_t *ends = pos + GRID_MAX_COLS;
    uint32_t k;
    uint32_t got = 0;
    wchar_t *w = scratch;
    size_t a;
    if (ntok + 1 >= GRID_MAX_COLS)
    {
        return FALSE;
    }
    for (k = ntok; k > 0; k--)
    {
        size_t s;
        size_t ee = e;
        while (ee > 0 && is_ws(ln->s[ee - 1]))
        {
            ee--;
        }
        s = ee;
        while (s > 0 && !is_ws(ln->s[s - 1]))
        {
            s--;
        }
        if (s == ee || !txt_value_token(ln->s + s, ee - s))
        {
            break; /* the name's end: the remaining cells were overwritten */
        }
        starts[k] = s;
        ends[k] = ee;
        e = s;
        got++;
    }
    if (got == 0 || ntok - got > 2)
    {
        return FALSE;
    }
    for (; k > 0; k--)
    {
        starts[k] = ends[k] = 0; /* blank */
    }
    a = 0;
    while (a < e && is_ws(ln->s[a]))
    {
        a++;
    }
    while (e > a && is_ws(ln->s[e - 1]))
    {
        e--;
    }
    if (e == a)
    {
        return FALSE;
    }
    starts[0] = a;
    ends[0] = e;
    for (k = 0; k <= ntok; k++)
    {
        size_t len = ends[k] - starts[k];
        memcpy(w, ln->s + starts[k], len * sizeof(wchar_t));
        w[len] = L'\0';
        cells[k] = w;
        w += len + 1;
    }
    return TRUE;
}

static void parse_clarity_txt(ResBuilder *b, const wchar_t *w, size_t wn)
{
    TxtLine *lines = NULL;
    size_t nlines = 0, cap = 0;
    size_t i;
    Grid *g = (Grid *)calloc(1, sizeof(Grid));
    wchar_t **cells = (wchar_t **)malloc(GRID_MAX_COLS * sizeof(wchar_t *));
    wchar_t **names = (wchar_t **)malloc(GRID_MAX_COLS * sizeof(wchar_t *));
    size_t *pos = (size_t *)malloc(2 * GRID_MAX_COLS * sizeof(size_t));
    wchar_t *scratch = NULL;
    wchar_t *nscratch = NULL;
    size_t maxlen = 0;
    BOOL any = FALSE;

    if (g == NULL || cells == NULL || names == NULL || pos == NULL)
    {
        b->oom = TRUE;
        goto cleanup;
    }
    {
        size_t s = 0;
        for (i = 0; i <= wn; i++)
        {
            if (i == wn || w[i] == L'\n')
            {
                size_t e = i;
                while (e > s && is_ws(w[e - 1]))
                {
                    e--;
                }
                if (!res_grow((void **)&lines, &cap, nlines + 1, sizeof(TxtLine)))
                {
                    b->oom = TRUE;
                    goto cleanup;
                }
                lines[nlines].s = w + s;
                lines[nlines].n = e - s;
                if (e - s > maxlen)
                {
                    maxlen = e - s;
                }
                nlines++;
                s = i + 1;
            }
        }
    }
    scratch = (wchar_t *)malloc((maxlen + GRID_MAX_COLS + 2) * sizeof(wchar_t));
    nscratch = (wchar_t *)malloc((maxlen + GRID_MAX_COLS + 2) * sizeof(wchar_t));
    if (scratch == NULL || nscratch == NULL)
    {
        b->oom = TRUE;
        goto cleanup;
    }

    for (i = 0; i < nlines && !b->oom; i++)
    {
        const TxtLine *ln = &lines[i];
        uint32_t nc, contest = 0;
        size_t j;
        if (!txt_header_line(ln))
        {
            continue;
        }
        nc = txt_cells(ln, scratch, cells, GRID_MAX_COLS);
        if (!grid_header(g, b, cells, nc))
        {
            continue;
        }
        if (!g->turnout)
        {
            const TxtLine *nl, *tl;
            wchar_t *title;
            wchar_t *nw = nscratch;
            uint32_t k;
            if (i < 2 || lines[i - 1].n == 0 || lines[i - 2].n == 0)
            {
                b_issue(b, L"A contest header without its title and choice names was skipped.");
                continue;
            }
            nl = &lines[i - 1];
            tl = &lines[i - 2];
            for (k = 0; k < g->ncols; k++)
            {
                names[k] = NULL;
            }
            /* Choice names start at their group's first column. */
            for (k = 0; k < g->nchoices; k++)
            {
                size_t a = (size_t)g->gstart[k] * TXT_COL;
                size_t e = ((size_t)g->gend[k] + 1) * TXT_COL;
                size_t len;
                if (e > nl->n)
                {
                    e = nl->n;
                }
                if (a > e)
                {
                    a = e;
                }
                while (a < e && is_ws(nl->s[a]))
                {
                    a++;
                }
                while (e > a && is_ws(nl->s[e - 1]))
                {
                    e--;
                }
                len = e - a;
                memcpy(nw, nl->s + a, len * sizeof(wchar_t));
                nw[len] = L'\0';
                names[g->gstart[k]] = nw;
                nw += len + 1;
            }
            title = wdup_trim(tl->s, tl->n);
            if (title == NULL)
            {
                b->oom = TRUE;
                break;
            }
            contest = grid_contest(g, b, title, names, g->ncols);
            free(title);
        }
        any = TRUE;
        for (j = i + 1; j < nlines; j++)
        {
            const TxtLine *dl = &lines[j];
            if (dl->n == 0)
            {
                break;
            }
            if (!txt_split_right(dl, g->ncols - 1, pos, scratch, cells))
            {
                wchar_t msg[200];
                StringCchPrintfW(msg,
                                 ARRAYSIZE(msg),
                                 L"Line %zu does not have the header's %u columns; skipped.",
                                 j + 1,
                                 g->ncols);
                b_issue(b, msg);
                continue;
            }
            grid_row(g, b, contest, cells, g->ncols);
            if (is_total_row(cells[0]))
            {
                j++;
                break;
            }
        }
        i = j - 1;
    }
    if (!any && !b->oom)
    {
        b_fail(b, L"The text file holds no Clarity precinct tables.");
    }

cleanup:
    free(lines);
    free(g);
    free(cells);
    free(names);
    free(pos);
    free(scratch);
    free(nscratch);
}

/* --- detail.xls / detail.xlsx (one sheet per contest) ---------------------- */

typedef struct XlsCtx
{
    ResBuilder *b;
    Grid *g;
    int state; /* 0 first row, 1 names row, 2 header row, 3 data rows, 4 done/skip */
    wchar_t *title;
    wchar_t **names;
    uint32_t nnames;
    uint32_t contest;
    wchar_t **cells;
    BOOL any;
} XlsCtx;

static void xls_free_names(XlsCtx *xc)
{
    uint32_t i;
    for (i = 0; i < xc->nnames; i++)
    {
        free(xc->names[i]);
        xc->names[i] = NULL;
    }
    xc->nnames = 0;
}

static BOOL xls_sheet(void *ctx, int index, const char *name)
{
    XlsCtx *xc = (XlsCtx *)ctx;
    (void)index;
    xls_free_names(xc);
    free(xc->title);
    xc->title = NULL;
    xc->state = (_stricmp(name, "Table of Contents") == 0) ? 4 : 0;
    return TRUE;
}

static BOOL xls_row(void *ctx, const char *const *cells8, uint32_t n)
{
    XlsCtx *xc = (XlsCtx *)ctx;
    ResBuilder *b = xc->b;
    uint32_t i;
    BOOL blank = TRUE;
    if (xc->state == 4 || n == 0)
    {
        return TRUE;
    }
    if (n > GRID_MAX_COLS)
    {
        n = GRID_MAX_COLS;
    }
    for (i = 0; i < n; i++)
    {
        xc->cells[i] = u8dup(cells8[i], -1);
        if (xc->cells[i] == NULL)
        {
            n = i;
            b->oom = TRUE;
            goto done;
        }
        wtrim(xc->cells[i]);
        if (xc->cells[i][0] != L'\0')
        {
            blank = FALSE;
        }
    }
    if (blank)
    {
        goto done;
    }
    switch (xc->state)
    {
        case 0:
            if (is_grid_header(xc->cells, n))
            {
                if (grid_header(xc->g, b, xc->cells, n) && xc->g->turnout)
                {
                    xc->state = 3;
                    xc->any = TRUE;
                }
                else
                {
                    xc->state = 4;
                }
            }
            else if (wieq(xc->cells[0], L"Table of Contents"))
            {
                xc->state = 4;
            }
            else
            {
                for (i = 0; i < n && xc->cells[i][0] == L'\0'; i++)
                {
                }
                xc->title = wdup(xc->cells[i < n ? i : 0]);
                xc->state = 1;
            }
            break;
        case 1:
            for (i = 0; i < n; i++)
            {
                xc->names[i] = xc->cells[i];
                xc->cells[i] = NULL; /* now owned by names */
            }
            xc->nnames = n;
            xc->state = 2;
            break;
        case 2:
            if (grid_header(xc->g, b, xc->cells, n) && !xc->g->turnout && xc->title != NULL)
            {
                xc->contest = grid_contest(xc->g, b, xc->title, xc->names, xc->nnames);
                xc->state = 3;
                xc->any = TRUE;
            }
            else
            {
                xc->state = 4;
            }
            break;
        case 3:
            if (n < xc->g->ncols)
            {
                for (i = n; i < xc->g->ncols; i++)
                {
                    xc->cells[i] = wdup(L"");
                    if (xc->cells[i] == NULL)
                    {
                        n = i;
                        b->oom = TRUE;
                        goto done;
                    }
                }
                n = xc->g->ncols;
            }
            grid_row(xc->g, b, xc->contest, xc->cells, n);
            if (is_total_row(xc->cells[0]))
            {
                xc->state = 4;
            }
            break;
        default:
            break;
    }
done:
    for (i = 0; i < n; i++)
    {
        free(xc->cells[i]);
        xc->cells[i] = NULL;
    }
    return !b->oom;
}

/* --- Excel 2003 XML Spreadsheet (Clarity's detail.xls before 2025) --------- */

/* Local name of an element or attribute at s (after any "prefix:"), length in *len. */
static const char *xml_local(const char *s, size_t n, size_t *len)
{
    const char *colon = memchr(s, ':', n);
    if (colon != NULL)
    {
        *len = n - (size_t)(colon + 1 - s);
        return colon + 1;
    }
    *len = n;
    return s;
}

/* Attribute by local name (any prefix): "ss:Index", "s:Index" and "Index" all match. */
static BOOL xattr_local(const char *s, const char *e, const char *local, char *buf, size_t cap)
{
    size_t ll = strlen(local);
    const char *q;
    buf[0] = '\0';
    for (q = s + 1; q + ll + 1 < e; q++)
    {
        if ((q[-1] == ':' || q[-1] == ' ' || q[-1] == '\t' || q[-1] == '\r' || q[-1] == '\n') &&
            strncmp(q, local, ll) == 0 && (q[ll] == '=' || q[ll] == ' '))
        {
            const char *v = q + ll;
            while (v < e && *v == ' ')
            {
                v++;
            }
            if (v < e && *v == '=')
            {
                char quote;
                const char *ve;
                v++;
                while (v < e && *v == ' ')
                {
                    v++;
                }
                if (v >= e || (*v != '"' && *v != '\''))
                {
                    return FALSE;
                }
                quote = *v++;
                ve = memchr(v, quote, (size_t)(e - v));
                if (ve == NULL)
                {
                    return FALSE;
                }
                xml_decode(v, (size_t)(ve - v), buf, cap);
                return TRUE;
            }
        }
    }
    return FALSE;
}

typedef struct SmlRow
{
    char **cells; /* GRID_MAX_COLS slots (NULL = blank) */
    uint32_t n;
} SmlRow;

static BOOL sml_set(SmlRow *row, uint32_t col, const char *s, size_t len)
{
    char *d;
    if (col >= GRID_MAX_COLS)
    {
        return TRUE;
    }
    d = (char *)malloc(len + 1);
    if (d == NULL)
    {
        return FALSE;
    }
    memcpy(d, s, len);
    d[len] = '\0';
    free(row->cells[col]);
    row->cells[col] = d;
    if (col + 1 > row->n)
    {
        row->n = col + 1;
    }
    return TRUE;
}

static BOOL sml_emit(XlsCtx *xc, SmlRow *row)
{
    const char **cells = (const char **)malloc(((size_t)row->n + 1) * sizeof(char *));
    uint32_t i;
    BOOL ok;
    if (cells == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < row->n; i++)
    {
        cells[i] = row->cells[i] != NULL ? row->cells[i] : "";
    }
    ok = xls_row(xc, cells, row->n);
    free(cells);
    for (i = 0; i < row->n; i++)
    {
        free(row->cells[i]);
        row->cells[i] = NULL;
    }
    row->n = 0;
    return ok;
}

/* Stream an Excel 2003 XML Spreadsheet's worksheets through the XlsCtx row handler. */
static BOOL parse_spreadsheetml(XlsCtx *xc, const char *x, size_t n)
{
    const char *end = x + n;
    const char *p = x;
    SmlRow row;
    uint32_t col = 0;
    int sheet = -1;
    BOOL in_row = FALSE;
    BOOL ok = TRUE;
    char *text = (char *)malloc(65536);
    ZeroMemory(&row, sizeof(row));
    row.cells = (char **)calloc(GRID_MAX_COLS, sizeof(char *));
    if (text == NULL || row.cells == NULL)
    {
        free(text);
        free(row.cells);
        return FALSE;
    }
    while (p < end && ok)
    {
        const char *lt = memchr(p, '<', (size_t)(end - p));
        const char *gt, *name, *local;
        size_t nlen = 0, llen = 0;
        BOOL closing, self_close;
        if (lt == NULL)
        {
            break;
        }
        gt = tag_end(lt + 1, end);
        if (gt == NULL)
        {
            break;
        }
        p = gt + 1;
        if (lt[1] == '?' || lt[1] == '!')
        {
            continue;
        }
        closing = lt[1] == '/';
        name = lt + (closing ? 2 : 1);
        while (name + nlen < gt && name[nlen] != ' ' && name[nlen] != '\t' && name[nlen] != '\r' &&
               name[nlen] != '\n' && name[nlen] != '/')
        {
            nlen++;
        }
        local = xml_local(name, nlen, &llen);
        self_close = gt[-1] == '/';
        if (tag_is(local, llen, "Worksheet") && !closing)
        {
            char nm[256];
            xattr_local(lt, gt, "Name", nm, sizeof(nm));
            ok = xls_sheet(xc, ++sheet, nm);
        }
        else if (tag_is(local, llen, "Row"))
        {
            if (closing || self_close)
            {
                if (in_row || self_close)
                {
                    ok = sml_emit(xc, &row);
                }
                in_row = FALSE;
            }
            else
            {
                in_row = TRUE;
                col = 0;
            }
        }
        else if (tag_is(local, llen, "Cell") && !closing && in_row)
        {
            char buf[32];
            if (xattr_local(lt, gt, "Index", buf, sizeof(buf)) && atoi(buf) > 0)
            {
                col = (uint32_t)atoi(buf) - 1;
            }
            if (!self_close)
            {
                /* The cell's text: everything up to </Cell>, tags (Data, Font, …) dropped. */
                size_t k = 0;
                const char *q = p;
                for (;;)
                {
                    const char *t = memchr(q, '<', (size_t)(end - q));
                    const char *te, *tn, *tl;
                    size_t tnl = 0, tll = 0;
                    if (t == NULL)
                    {
                        q = end;
                        break;
                    }
                    if (k + (size_t)(t - q) < 65535)
                    {
                        xml_decode(q, (size_t)(t - q), text + k, 65536 - k);
                        k += strlen(text + k);
                    }
                    te = tag_end(t + 1, end);
                    if (te == NULL)
                    {
                        q = end;
                        break;
                    }
                    tn = t + (t[1] == '/' ? 2 : 1);
                    while (tn + tnl < te && tn[tnl] != ' ' && tn[tnl] != '/' && tn[tnl] != '>')
                    {
                        tnl++;
                    }
                    tl = xml_local(tn, tnl, &tll);
                    q = te + 1;
                    if (t[1] == '/' && tag_is(tl, tll, "Cell"))
                    {
                        break;
                    }
                }
                p = q;
                text[k] = '\0';
                ok = sml_set(&row, col, text, k);
            }
            if (xattr_local(lt, gt, "MergeAcross", buf, sizeof(buf)) && atoi(buf) > 0)
            {
                col += (uint32_t)atoi(buf);
            }
            col++;
        }
    }
    if (ok && row.n > 0)
    {
        ok = sml_emit(xc, &row);
    }
    {
        uint32_t i;
        for (i = 0; i < GRID_MAX_COLS; i++)
        {
            free(row.cells[i]);
        }
    }
    free(row.cells);
    free(text);
    return ok;
}

static BOOL is_spreadsheetml(const uint8_t *d, size_t n)
{
    size_t head = n < 4096 ? n : 4096;
    size_t i = 0;
    if (head >= 3 && d[0] == 0xEF && d[1] == 0xBB && d[2] == 0xBF)
    {
        i = 3;
    }
    while (i < head && (d[i] == ' ' || d[i] == '\r' || d[i] == '\n' || d[i] == '\t'))
    {
        i++;
    }
    if (i >= head || d[i] != '<')
    {
        return FALSE;
    }
    for (; i + 43 < head; i++)
    {
        if (memcmp(d + i, "urn:schemas-microsoft-com:office:spreadsheet", 44) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

static void parse_clarity_xls(ResBuilder *b, const uint8_t *d, size_t n)
{
    XlsCtx xc;
    wchar_t err[256] = L"";
    EeLoadStatus st;
    ZeroMemory(&xc, sizeof(xc));
    xc.b = b;
    xc.g = (Grid *)calloc(1, sizeof(Grid));
    xc.names = (wchar_t **)calloc(GRID_MAX_COLS, sizeof(wchar_t *));
    xc.cells = (wchar_t **)calloc(GRID_MAX_COLS, sizeof(wchar_t *));
    if (xc.g == NULL || xc.names == NULL || xc.cells == NULL)
    {
        b->oom = TRUE;
    }
    else
    {
        if (is_spreadsheetml(d, n))
        {
            st =
                parse_spreadsheetml(&xc, (const char *)d, n) ? EeLoadStatus_Ok : EeLoadStatus_Error;
            if (st != EeLoadStatus_Ok && !b->oom)
            {
                b->oom = TRUE;
            }
        }
        else
        {
            st = EeXlsx_ReadAllSheetsMem(d, n, xls_sheet, xls_row, &xc, NULL, err, ARRAYSIZE(err));
        }
        if (st != EeLoadStatus_Ok && !b->oom)
        {
            b_fail(b, err[0] ? err : L"The workbook could not be read.");
        }
        else if (!xc.any && !b->oom)
        {
            b_fail(b, L"The workbook holds no Clarity precinct tables.");
        }
        xls_free_names(&xc);
    }
    free(xc.title);
    free(xc.g);
    free(xc.names);
    free(xc.cells);
}

/* -------------------------------------------------------------------------- */
/* Election Explorer results export (long form)                               */
/* -------------------------------------------------------------------------- */

static const char *const k_ExportCols[] = {"Record Type",
                                           "Election",
                                           "Date",
                                           "Contest Order",
                                           "Contest",
                                           "Vote For",
                                           "Choice",
                                           "Party",
                                           "Precinct",
                                           "Vote Type",
                                           "Votes"};
enum
{
    XC_TYPE,
    XC_ELECTION,
    XC_DATE,
    XC_ORDER,
    XC_CONTEST,
    XC_VOTEFOR,
    XC_CHOICE,
    XC_PARTY,
    XC_PRECINCT,
    XC_VT,
    XC_VOTES,
    XC_COUNT
};

typedef struct OutBuf
{
    char *p;
    size_t len, cap;
    BOOL oom;
} OutBuf;

static void ob_put(OutBuf *o, const char *s, size_t n)
{
    if (o->oom)
    {
        return;
    }
    if (!res_grow((void **)&o->p, &o->cap, o->len + n + 1, 1))
    {
        o->oom = TRUE;
        return;
    }
    memcpy(o->p + o->len, s, n);
    o->len += n;
    o->p[o->len] = '\0';
}

/* One field, RFC-4180 quoted when it holds the delimiter, a quote or a line break. */
static void ob_field_a(OutBuf *o, const char *s, char delim)
{
    size_t n = s != NULL ? strlen(s) : 0;
    size_t i;
    BOOL q = FALSE;
    for (i = 0; i < n; i++)
    {
        if (s[i] == delim || s[i] == '"' || s[i] == '\r' || s[i] == '\n')
        {
            q = TRUE;
            break;
        }
    }
    if (!q)
    {
        ob_put(o, s != NULL ? s : "", n);
        return;
    }
    ob_put(o, "\"", 1);
    for (i = 0; i < n; i++)
    {
        if (s[i] == '"')
        {
            ob_put(o, "\"\"", 2);
        }
        else
        {
            ob_put(o, s + i, 1);
        }
    }
    ob_put(o, "\"", 1);
}

static void ob_field_w(OutBuf *o, const wchar_t *w, char delim)
{
    char stack[512];
    char *u = stack;
    int n;
    if (w == NULL || w[0] == L'\0')
    {
        return;
    }
    n = WideCharToMultiByte(CP_UTF8, 0, w, -1, NULL, 0, NULL, NULL);
    if (n <= 0)
    {
        return;
    }
    if ((size_t)n > sizeof(stack))
    {
        u = (char *)malloc((size_t)n);
        if (u == NULL)
        {
            o->oom = TRUE;
            return;
        }
    }
    WideCharToMultiByte(CP_UTF8, 0, w, -1, u, n, NULL, NULL);
    ob_field_a(o, u, delim);
    if (u != stack)
    {
        free(u);
    }
}

/* Fields of one export record (wide strings; NULL = blank). */
typedef struct XRec
{
    const wchar_t *f[XC_COUNT];
    int64_t votes;
    BOOL has_votes;
    uint32_t order; /* 0 = blank */
    uint32_t vote_for;
} XRec;

static void ob_record(OutBuf *o, const EeResults *r, const XRec *x, char delim)
{
    char num[32];
    int k;
    for (k = 0; k < XC_COUNT; k++)
    {
        if (k > 0)
        {
            ob_put(o, &delim, 1);
        }
        switch (k)
        {
            case XC_ELECTION:
                ob_field_w(o, r->election_name, delim);
                break;
            case XC_DATE:
                ob_field_w(o, r->election_date, delim);
                break;
            case XC_ORDER:
                if (x->order > 0)
                {
                    StringCchPrintfA(num, sizeof(num), "%u", x->order);
                    ob_put(o, num, strlen(num));
                }
                break;
            case XC_VOTEFOR:
                if (x->vote_for > 0)
                {
                    StringCchPrintfA(num, sizeof(num), "%u", x->vote_for);
                    ob_put(o, num, strlen(num));
                }
                break;
            case XC_VOTES:
                if (x->has_votes)
                {
                    StringCchPrintfA(num, sizeof(num), "%lld", (long long)x->votes);
                    ob_put(o, num, strlen(num));
                }
                else
                {
                    ob_field_w(o, x->f[XC_VOTES], delim);
                }
                break;
            default:
                ob_field_w(o, x->f[k], delim);
                break;
        }
    }
    ob_put(o, "\r\n", 2);
}

static void xr_init(XRec *x, const wchar_t *type)
{
    ZeroMemory(x, sizeof(*x));
    x->f[XC_TYPE] = type;
}

static void xr_votes(XRec *x, int64_t v)
{
    if (v != EE_RES_UNKNOWN)
    {
        x->votes = v;
        x->has_votes = TRUE;
    }
}

BOOL EeResults_FormatDetailUtf8(const EeResults *r,
                                char delim,
                                const uint8_t *precinct_mask,
                                const uint8_t *vt_mask,
                                const uint8_t *contest_mask,
                                char **out,
                                size_t *out_len)
{
    OutBuf o = {0};
    XRec x;
    uint32_t nvt = EE_RESULTS_NVT(r);
    int64_t *tot = NULL, *ovr = NULL, *und = NULL;
    BOOL masked = precinct_mask != NULL || vt_mask != NULL;
    uint32_t c, k, p, v;

    *out = NULL;
    *out_len = 0;
    tot = (int64_t *)calloc((size_t)r->nchoices + 1, sizeof(int64_t));
    ovr = (int64_t *)calloc((size_t)r->ncontests + 1, sizeof(int64_t));
    und = (int64_t *)calloc((size_t)r->ncontests + 1, sizeof(int64_t));
    if (tot == NULL || ovr == NULL || und == NULL)
    {
        goto fail;
    }
    EeResults_Tabulate(r, precinct_mask, vt_mask, tot, NULL, ovr, und);

    for (k = 0; k < XC_COUNT; k++)
    {
        if (k > 0)
        {
            ob_put(&o, &delim, 1);
        }
        ob_field_a(&o, k_ExportCols[k], delim);
    }
    ob_put(&o, "\r\n", 2);

    if (r->region != NULL)
    {
        xr_init(&x, L"Info");
        x.f[XC_CONTEST] = L"Region";
        x.f[XC_CHOICE] = r->region;
        ob_record(&o, r, &x, delim);
    }
    if (r->timestamp != NULL)
    {
        xr_init(&x, L"Info");
        x.f[XC_CONTEST] = L"Timestamp";
        x.f[XC_CHOICE] = r->timestamp;
        ob_record(&o, r, &x, delim);
    }
    for (v = 0; v < r->nvt; v++)
    {
        if (vt_mask == NULL || vt_mask[v])
        {
            xr_init(&x, L"Vote Type");
            x.f[XC_VT] = r->vote_types[v];
            ob_record(&o, r, &x, delim);
        }
    }
    /* Turnout: the overall figures (unfiltered only), then each precinct. */
    if (!masked)
    {
        xr_init(&x, L"Registered Voters");
        xr_votes(&x, r->registered_total);
        ob_record(&o, r, &x, delim);
        xr_init(&x, L"Ballots Cast");
        xr_votes(&x, r->ballots_total);
        ob_record(&o, r, &x, delim);
    }
    for (p = 0; p < r->nprecincts; p++)
    {
        if (precinct_mask != NULL && !precinct_mask[p])
        {
            continue;
        }
        xr_init(&x, L"Registered Voters");
        x.f[XC_PRECINCT] = r->precincts[p].name;
        xr_votes(&x, r->precincts[p].registered);
        ob_record(&o, r, &x, delim);
        if (vt_mask == NULL)
        {
            xr_init(&x, L"Ballots Cast");
            x.f[XC_PRECINCT] = r->precincts[p].name;
            xr_votes(&x, r->precincts[p].ballots);
            ob_record(&o, r, &x, delim);
        }
        if (r->prec_ballots_vt != NULL)
        {
            for (v = 0; v < r->nvt; v++)
            {
                if (vt_mask == NULL || vt_mask[v])
                {
                    xr_init(&x, L"Ballots Cast");
                    x.f[XC_PRECINCT] = r->precincts[p].name;
                    x.f[XC_VT] = r->vote_types[v];
                    xr_votes(&x, r->prec_ballots_vt[(size_t)p * r->nvt + v]);
                    ob_record(&o, r, &x, delim);
                }
            }
        }
    }

    for (c = 0; c < r->ncontests; c++)
    {
        const EeResultsContest *ct = &r->contests[c];
        if (contest_mask != NULL && !contest_mask[c])
        {
            continue;
        }
        xr_init(&x, L"Contest");
        x.order = c + 1;
        x.f[XC_CONTEST] = ct->title;
        x.vote_for = ct->vote_for;
        x.f[XC_PARTY] = ct->party;
        ob_record(&o, r, &x, delim);
        if (ct->is_question >= 0)
        {
            xr_init(&x, L"Contest Is Question");
            x.order = c + 1;
            x.f[XC_CONTEST] = ct->title;
            xr_votes(&x, ct->is_question);
            ob_record(&o, r, &x, delim);
        }
        if (!masked)
        {
            static const wchar_t *const k_fig[] = {L"Contest Registered Voters",
                                                   L"Contest Ballots Cast",
                                                   L"Contest Precincts",
                                                   L"Contest Precincts Reporting"};
            int64_t vals[4];
            int f;
            vals[0] = ct->registered;
            vals[1] = ct->ballots;
            vals[2] = ct->precincts_total;
            vals[3] = ct->precincts_reporting;
            for (f = 0; f < 4; f++)
            {
                if (vals[f] != EE_RES_UNKNOWN)
                {
                    xr_init(&x, k_fig[f]);
                    x.order = c + 1;
                    x.f[XC_CONTEST] = ct->title;
                    xr_votes(&x, vals[f]);
                    ob_record(&o, r, &x, delim);
                }
            }
        }
        for (k = 0; k < ct->nchoices; k++)
        {
            const EeResultsChoice *ch = &r->choices[ct->first_choice + k];
            xr_init(&x, L"Choice");
            x.order = c + 1;
            x.f[XC_CONTEST] = ct->title;
            x.vote_for = ct->vote_for;
            x.f[XC_CHOICE] = ch->name;
            x.f[XC_PARTY] = ch->party;
            xr_votes(&x, tot[ct->first_choice + k]);
            ob_record(&o, r, &x, delim);
            for (p = 0; p < ct->npre && ct->counts != NULL; p++)
            {
                if (precinct_mask != NULL && !precinct_mask[ct->pre[p]])
                {
                    continue;
                }
                for (v = 0; v < nvt; v++)
                {
                    if (r->nvt > 0 && vt_mask != NULL && !vt_mask[v])
                    {
                        continue;
                    }
                    xr_init(&x, L"Choice");
                    x.order = c + 1;
                    x.f[XC_CONTEST] = ct->title;
                    x.vote_for = ct->vote_for;
                    x.f[XC_CHOICE] = ch->name;
                    x.f[XC_PARTY] = ch->party;
                    x.f[XC_PRECINCT] = r->precincts[ct->pre[p]].name;
                    x.f[XC_VT] = r->nvt > 0 ? r->vote_types[v] : NULL;
                    if (ct->protected_cells != NULL &&
                        ct->protected_cells[((size_t)k * ct->npre + p) * nvt + v])
                    {
                        x.f[XC_VOTES] = L"protected";
                    }
                    else
                    {
                        xr_votes(&x, ct->counts[((size_t)k * ct->npre + p) * nvt + v]);
                    }
                    ob_record(&o, r, &x, delim);
                }
            }
        }
        if (ct->has_over_under)
        {
            int side;
            for (side = 0; side < 2; side++)
            {
                const int32_t *arr = side == 0 ? ct->over : ct->under;
                xr_init(&x, side == 0 ? L"Over Votes" : L"Under Votes");
                x.order = c + 1;
                x.f[XC_CONTEST] = ct->title;
                xr_votes(&x, side == 0 ? ovr[c] : und[c]);
                ob_record(&o, r, &x, delim);
                for (p = 0; p < ct->npre && arr != NULL && vt_mask == NULL; p++)
                {
                    if (precinct_mask != NULL && !precinct_mask[ct->pre[p]])
                    {
                        continue;
                    }
                    xr_init(&x, side == 0 ? L"Over Votes" : L"Under Votes");
                    x.order = c + 1;
                    x.f[XC_CONTEST] = ct->title;
                    x.f[XC_PRECINCT] = r->precincts[ct->pre[p]].name;
                    xr_votes(&x, arr[p]);
                    ob_record(&o, r, &x, delim);
                }
            }
        }
    }
    if (o.oom)
    {
        goto fail;
    }
    free(tot);
    free(ovr);
    free(und);
    if (o.p == NULL)
    {
        ob_put(&o, "", 0);
    }
    *out = o.p;
    *out_len = o.len;
    return TRUE;

fail:
    free(tot);
    free(ovr);
    free(und);
    free(o.p);
    return FALSE;
}

typedef struct ExpCtx
{
    ResBuilder *b;
    BOOL header_done;
    int col[XC_COUNT];
    int64_t contest; /* current contest index, -1 none */
    int64_t choice;  /* current choice (local), -1 none */
} ExpCtx;

static BOOL exp_sink(void *ctx, const char *const *cells, uint32_t n)
{
    ExpCtx *ec = (ExpCtx *)ctx;
    ResBuilder *b = ec->b;
    EeResults *r = b->r;
    wchar_t *f[XC_COUNT];
    int64_t votes = EE_RES_UNKNOWN;
    const wchar_t *type;
    int k;

    if (!ec->header_done)
    {
        uint32_t i;
        if (n == 0)
        {
            return TRUE;
        }
        for (k = 0; k < XC_COUNT; k++)
        {
            ec->col[k] = -1;
            for (i = 0; i < n; i++)
            {
                if (_stricmp(cells[i], k_ExportCols[k]) == 0)
                {
                    ec->col[k] = (int)i;
                    break;
                }
            }
        }
        if (ec->col[XC_TYPE] < 0 || ec->col[XC_VOTES] < 0 || ec->col[XC_CONTEST] < 0)
        {
            b_fail(b, L"The results export's header is missing Record Type, Contest or Votes.");
            return FALSE;
        }
        ec->header_done = TRUE;
        return TRUE;
    }
    for (k = 0; k < XC_COUNT; k++)
    {
        const char *s = cell_at(cells, n, ec->col[k]);
        f[k] = u8dup(s != NULL ? s : "", -1);
        if (f[k] == NULL)
        {
            b->oom = TRUE;
            for (--k; k >= 0; k--)
            {
                free(f[k]);
            }
            return FALSE;
        }
        wtrim(f[k]);
    }
    type = f[XC_TYPE];
    if (f[XC_VOTES][0] != L'\0' && !parse_i64w(f[XC_VOTES], &votes))
    {
        votes = EE_RES_UNKNOWN;
    }
    if (r->election_name == NULL && f[XC_ELECTION][0] != L'\0')
    {
        r->election_name = wdup(f[XC_ELECTION]);
    }
    if (r->election_date == NULL && f[XC_DATE][0] != L'\0')
    {
        r->election_date = wdup(f[XC_DATE]);
    }

    if (wieq(type, L"Info"))
    {
        if (wieq(f[XC_CONTEST], L"Region"))
        {
            free(r->region);
            r->region = wdup(f[XC_CHOICE]);
        }
        else if (wieq(f[XC_CONTEST], L"Timestamp"))
        {
            free(r->timestamp);
            r->timestamp = wdup(f[XC_CHOICE]);
        }
    }
    else if (wieq(type, L"Vote Type"))
    {
        b_vt(b, f[XC_VT]);
    }
    else if (wieq(type, L"Registered Voters"))
    {
        if (f[XC_PRECINCT][0] == L'\0')
        {
            r->registered_total = votes;
        }
        else
        {
            uint32_t pi = b_pre(b, f[XC_PRECINCT]);
            if (!b->oom)
            {
                r->precincts[pi].registered = votes;
            }
        }
    }
    else if (wieq(type, L"Ballots Cast"))
    {
        if (f[XC_PRECINCT][0] == L'\0')
        {
            if (f[XC_VT][0] == L'\0')
            {
                r->ballots_total = votes;
            }
        }
        else
        {
            uint32_t pi = b_pre(b, f[XC_PRECINCT]);
            if (b->oom)
            {
            }
            else if (f[XC_VT][0] == L'\0')
            {
                r->precincts[pi].ballots = votes;
            }
            else if (votes != EE_RES_UNKNOWN)
            {
                b_turnout_vt(b, pi, b_vt(b, f[XC_VT]), votes);
            }
        }
    }
    else if (wieq(type, L"Contest"))
    {
        int64_t vf = 0;
        uint32_t ci;
        parse_i64w(f[XC_VOTEFOR], &vf);
        ci = b_contest(b, f[XC_CONTEST], vf > 0 ? (uint32_t)vf : 0, -1);
        if (!b->oom && f[XC_PARTY][0] != L'\0')
        {
            r->contests[ci].party = wdup(f[XC_PARTY]);
        }
        ec->contest = ci;
        ec->choice = -1;
    }
    else if (ec->contest >= 0 && ec->contest < r->ncontests)
    {
        EeResultsContest *ct = &r->contests[ec->contest];
        if (wieq(type, L"Contest Is Question"))
            ct->is_question = votes == 1 ? 1 : 0;
        else if (wieq(type, L"Contest Registered Voters"))
            ct->registered = votes;
        else if (wieq(type, L"Contest Ballots Cast"))
            ct->ballots = votes;
        else if (wieq(type, L"Contest Precincts"))
            ct->precincts_total = votes == EE_RES_UNKNOWN ? EE_RES_UNKNOWN : clamp32(votes);
        else if (wieq(type, L"Contest Precincts Reporting"))
            ct->precincts_reporting = votes == EE_RES_UNKNOWN ? EE_RES_UNKNOWN : clamp32(votes);
        else if (wieq(type, L"Choice"))
        {
            if (f[XC_PRECINCT][0] == L'\0' && f[XC_VT][0] == L'\0')
            {
                ec->choice = b_choice(b, f[XC_CHOICE], f[XC_PARTY], votes);
            }
            else if (ec->choice >= 0 && f[XC_PRECINCT][0] != L'\0' &&
                     (votes != EE_RES_UNKNOWN || wieq(f[XC_VOTES], L"protected")))
            {
                if (votes == EE_RES_UNKNOWN)
                {
                    votes = RES_PROTECTED;
                }
                uint32_t pi = b_pre(b, f[XC_PRECINCT]);
                uint32_t vi = f[XC_VT][0] != L'\0' ? b_vt(b, f[XC_VT]) : 0;
                b_count(b, (uint32_t)ec->contest, (uint32_t)ec->choice, pi, vi, votes);
            }
        }
        else if (wieq(type, L"Over Votes") || wieq(type, L"Under Votes"))
        {
            BOOL over = wieq(type, L"Over Votes");
            ct->has_over_under = TRUE;
            if (f[XC_PRECINCT][0] == L'\0')
            {
                if (over)
                    ct->over_total = votes == EE_RES_UNKNOWN ? 0 : votes;
                else
                    ct->under_total = votes == EE_RES_UNKNOWN ? 0 : votes;
            }
            else if (votes != EE_RES_UNKNOWN)
            {
                b_count(b,
                        (uint32_t)ec->contest,
                        over ? RES_OVER : RES_UNDER,
                        b_pre(b, f[XC_PRECINCT]),
                        0,
                        votes);
            }
        }
    }
    for (k = 0; k < XC_COUNT; k++)
    {
        free(f[k]);
    }
    return !b->oom;
}

static void parse_ee_export(ResBuilder *b, const uint8_t *d, size_t n, const wchar_t *name)
{
    ExpCtx ec;
    wchar_t err[256] = L"";
    EeLoadStatus st;
    size_t i, tabs = 0, commas = 0;
    ZeroMemory(&ec, sizeof(ec));
    ec.b = b;
    ec.contest = -1;
    ec.choice = -1;
    /* The delimiter comes from the header line, not the extension (a renamed file). */
    for (i = 0; i < n && d[i] != '\n'; i++)
    {
        tabs += d[i] == '\t';
        commas += d[i] == ',';
    }
    (void)name;
    st = EeCsv_ReadSheetMem(d,
                            n,
                            tabs > commas ? L"export.tsv" : L"export.csv",
                            exp_sink,
                            &ec,
                            NULL,
                            NULL,
                            NULL,
                            err,
                            ARRAYSIZE(err));
    if (st != EeLoadStatus_Ok && !b->bad && !b->oom)
    {
        b_fail(b, err[0] ? err : L"The results export could not be read.");
    }
    else if (b->r->ncontests == 0 && !b->bad && !b->oom)
    {
        b_fail(b, L"The results export holds no contests.");
    }
}

/* -------------------------------------------------------------------------- */
/* Format detection and single-file loading                                  */
/* -------------------------------------------------------------------------- */

typedef struct Parsed
{
    EeResults r;
    wchar_t *path;
    wchar_t *member;
    uint32_t format;
    uint32_t group;
} Parsed;

static uint32_t sniff_format(const uint8_t *d, size_t n)
{
    static const uint8_t k_ole[8] = {0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1};
    size_t head = n < 16384 ? n : 16384;
    size_t wn = 0;
    wchar_t *w;
    uint32_t fmt = 0;
    size_t i, lines = 0;
    if (n >= 8 && memcmp(d, k_ole, 8) == 0)
    {
        return EE_RES_FMT_CLARITY_XLS;
    }
    if (n >= 4 && d[0] == 'P' && d[1] == 'K' && d[2] == 3 && d[3] == 4)
    {
        return EE_RES_FMT_CLARITY_XLS; /* .xlsx (the caller has ruled out a plain ZIP) */
    }
    w = decode_text(d, head, &wn);
    if (w == NULL)
    {
        return 0;
    }
    for (i = 0; i < wn && is_ws(w[i]); i++)
    {
    }
    if (i < wn && w[i] == L'<')
    {
        if (wcsstr(w, L"<ElectionResult") != NULL)
        {
            fmt = EE_RES_FMT_CLARITY_XML;
        }
        else if (wcsstr(w, L"urn:schemas-microsoft-com:office:spreadsheet") != NULL)
        {
            fmt = EE_RES_FMT_CLARITY_XLS; /* Excel 2003 XML Spreadsheet saved as .xls */
        }
        free(w);
        return fmt;
    }
    /* Look at the first lines: a CSV header, or the TXT's "Precinct  Registered Voters". */
    for (i = 0; i < wn && lines < 60 && fmt == 0; lines++)
    {
        size_t e = i;
        wchar_t save;
        while (e < wn && w[e] != L'\n')
        {
            e++;
        }
        save = w[e < wn ? e : wn];
        w[e < wn ? e : wn] = L'\0';
        if (lines == 0 || fmt == 0)
        {
            const wchar_t *line = w + i;
            if (wcsistr(line, L"contest name") != NULL && wcsistr(line, L"choice name") != NULL)
            {
                fmt = EE_RES_FMT_CLARITY_SUMMARY;
            }
            else if (wcsistr(line, L"Record Type") != NULL &&
                     wcsistr(line, L"Contest Order") != NULL)
            {
                fmt = EE_RES_FMT_EE_EXPORT;
            }
            else if ((e - i > 47 && wcsncmp(line + 30, L"Registered Voters", 17) == 0) ||
                     (e - i > 42 &&
                      (wcsncmp(line, L"County                        Ballots Cast", 42) == 0 ||
                       wcsncmp(line, L"Precinct                      Ballots Cast", 42) == 0)))
            {
                fmt = EE_RES_FMT_CLARITY_TXT; /* the 30-character grid of detail.txt */
            }
        }
        w[e < wn ? e : wn] = save;
        i = e + 1;
    }
    free(w);
    return fmt;
}

static const wchar_t *format_label(uint32_t fmt)
{
    switch (fmt)
    {
        case EE_RES_FMT_CLARITY_SUMMARY:
            return L"Clarity summary (CSV)";
        case EE_RES_FMT_CLARITY_TXT:
            return L"Clarity detail (TXT)";
        case EE_RES_FMT_CLARITY_XLS:
            return L"Clarity detail (XLS)";
        case EE_RES_FMT_CLARITY_XML:
            return L"Clarity detail (XML)";
        case EE_RES_FMT_EE_EXPORT:
            return L"Election Explorer export";
        default:
            return L"results";
    }
}

/* Parse one in-memory file into @p out. Returns FALSE with a message on failure. */
static BOOL parse_buffer(const uint8_t *d,
                         size_t n,
                         const wchar_t *name,
                         Parsed *out,
                         wchar_t *err,
                         size_t errcch)
{
    ResBuilder b;
    uint32_t fmt = sniff_format(d, n);
    b_init(&b, &out->r);
    out->format = fmt;
    switch (fmt)
    {
        case EE_RES_FMT_CLARITY_SUMMARY:
            parse_clarity_summary(&b, d, n, name);
            break;
        case EE_RES_FMT_CLARITY_XML:
            parse_clarity_xml(&b, (const char *)d, n);
            break;
        case EE_RES_FMT_CLARITY_XLS:
            parse_clarity_xls(&b, d, n);
            break;
        case EE_RES_FMT_EE_EXPORT:
            parse_ee_export(&b, d, n, name);
            break;
        case EE_RES_FMT_CLARITY_TXT:
        {
            size_t wn = 0;
            wchar_t *w = decode_text(d, n, &wn);
            if (w == NULL)
            {
                b.oom = TRUE;
            }
            else
            {
                parse_clarity_txt(&b, w, wn);
                free(w);
            }
            break;
        }
        default:
            b_fail(&b,
                   L"It is not a recognized election results file (Clarity summary.csv, "
                   L"detail.txt, detail.xls, detail.xlsx or detail.xml, or an Election "
                   L"Explorer results export).");
            break;
    }
    if (!b.bad && !b.oom)
    {
        b_finish(&b);
    }
    if (b.oom || b.bad)
    {
        res_set_err(err, errcch, b.oom ? L"Out of memory loading the results." : b.bad_msg);
        b_free(&b);
        EeResults_Clear(&out->r);
        return FALSE;
    }
    b_free(&b);
    return TRUE;
}

static BOOL read_whole_file(const wchar_t *path, uint8_t **out, size_t *out_len)
{
    HANDLE h;
    LARGE_INTEGER sz;
    uint8_t *buf;
    size_t total = 0;
    *out = NULL;
    *out_len = 0;
    h = CreateFileW(path,
                    GENERIC_READ,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    NULL,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
                    NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        return FALSE;
    }
    if (!GetFileSizeEx(h, &sz) || sz.QuadPart < 0 || (uint64_t)sz.QuadPart > RES_MAX_FILE_BYTES)
    {
        CloseHandle(h);
        return FALSE;
    }
    buf = (uint8_t *)malloc((size_t)sz.QuadPart + 1);
    if (buf == NULL)
    {
        CloseHandle(h);
        return FALSE;
    }
    while (total < (size_t)sz.QuadPart)
    {
        DWORD got = 0;
        DWORD want =
            (DWORD)(((size_t)sz.QuadPart - total) > 0x10000000u ? 0x10000000u
                                                                : ((size_t)sz.QuadPart - total));
        if (!ReadFile(h, buf + total, want, &got, NULL) || got == 0)
        {
            break;
        }
        total += got;
    }
    CloseHandle(h);
    if (total != (size_t)sz.QuadPart)
    {
        free(buf);
        return FALSE;
    }
    buf[total] = 0;
    *out = buf;
    *out_len = total;
    return TRUE;
}

/* Is this ZIP an .xlsx workbook rather than an archive of results files? */
static BOOL zip_is_workbook(mz_zip_archive *z)
{
    return mz_zip_reader_locate_file(z, "xl/workbook.xml", NULL, 0) >= 0;
}

typedef struct ParsedList
{
    Parsed *p;
    size_t n, cap;
} ParsedList;

static Parsed *pl_add(ParsedList *pl)
{
    Parsed *p;
    if (!res_grow((void **)&pl->p, &pl->cap, pl->n + 1, sizeof(Parsed)))
    {
        return NULL;
    }
    p = &pl->p[pl->n];
    ZeroMemory(p, sizeof(*p));
    return p;
}

static void parsed_free(Parsed *p)
{
    EeResults_Clear(&p->r);
    free(p->path);
    free(p->member);
    p->path = NULL;
    p->member = NULL;
}

/* Load @p path (a results file or a ZIP of them) into @p pl. */
static BOOL load_path(const wchar_t *path, ParsedList *pl, wchar_t *err, size_t errcch)
{
    uint8_t *d = NULL;
    size_t n = 0;
    wchar_t msg[MAX_PATH + 400];
    wchar_t perr[300];
    BOOL ok = FALSE;

    if (!read_whole_file(path, &d, &n))
    {
        StringCchPrintfW(msg, ARRAYSIZE(msg), L"Could not read %s.", base_name(path));
        res_set_err(err, errcch, msg);
        return FALSE;
    }
    if (n >= 4 && d[0] == 'P' && d[1] == 'K' && d[2] == 3 && d[3] == 4)
    {
        mz_zip_archive z;
        mz_zip_zero_struct(&z);
        if (!mz_zip_reader_init_mem(&z, d, n, 0))
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg), L"%s is not a valid ZIP file.", base_name(path));
            res_set_err(err, errcch, msg);
            free(d);
            return FALSE;
        }
        if (zip_is_workbook(&z))
        {
            mz_zip_reader_end(&z);
            goto single;
        }
        {
            mz_uint e, cnt = mz_zip_reader_get_num_files(&z);
            size_t found = 0;
            ok = TRUE;
            for (e = 0; e < cnt && ok; e++)
            {
                mz_zip_archive_file_stat st;
                wchar_t wname[MAX_PATH];
                uint8_t *ed;
                size_t elen = 0;
                Parsed *pp;
                if (!mz_zip_reader_file_stat(&z, e, &st) ||
                    mz_zip_reader_is_file_a_directory(&z, e) ||
                    strncmp(st.m_filename, "__MACOSX/", 9) == 0 ||
                    st.m_uncomp_size > RES_MAX_FILE_BYTES)
                {
                    continue;
                }
                if (MultiByteToWideChar(CP_UTF8,
                                        MB_ERR_INVALID_CHARS,
                                        st.m_filename,
                                        -1,
                                        wname,
                                        ARRAYSIZE(wname)) == 0)
                {
                    MultiByteToWideChar(CP_ACP, 0, st.m_filename, -1, wname, ARRAYSIZE(wname));
                }
                ed = (uint8_t *)mz_zip_reader_extract_to_heap(&z, e, &elen, 0);
                if (ed == NULL)
                {
                    StringCchPrintfW(msg,
                                     ARRAYSIZE(msg),
                                     L"Could not extract %s from %s.",
                                     wname,
                                     base_name(path));
                    res_set_err(err, errcch, msg);
                    ok = FALSE;
                    break;
                }
                pp = pl_add(pl);
                if (pp == NULL)
                {
                    mz_free(ed);
                    res_set_err(err, errcch, L"Out of memory loading the results.");
                    ok = FALSE;
                    break;
                }
                if (!parse_buffer(ed, elen, wname, pp, perr, ARRAYSIZE(perr)))
                {
                    StringCchPrintfW(msg,
                                     ARRAYSIZE(msg),
                                     L"%s (%s): %s",
                                     base_name(path),
                                     wname,
                                     perr);
                    res_set_err(err, errcch, msg);
                    mz_free(ed);
                    ok = FALSE;
                    break;
                }
                mz_free(ed);
                pp->path = wdup(path);
                pp->member = wdup(wname);
                pl->n++;
                found++;
            }
            if (ok && found == 0)
            {
                StringCchPrintfW(msg,
                                 ARRAYSIZE(msg),
                                 L"%s holds no election results files.",
                                 base_name(path));
                res_set_err(err, errcch, msg);
                ok = FALSE;
            }
        }
        mz_zip_reader_end(&z);
        free(d);
        return ok;
    }
single:
{
    Parsed *pp = pl_add(pl);
    if (pp == NULL)
    {
        res_set_err(err, errcch, L"Out of memory loading the results.");
    }
    else if (!parse_buffer(d, n, base_name(path), pp, perr, ARRAYSIZE(perr)))
    {
        StringCchPrintfW(msg, ARRAYSIZE(msg), L"%s: %s", base_name(path), perr);
        res_set_err(err, errcch, msg);
    }
    else
    {
        pp->path = wdup(path);
        pl->n++;
        ok = TRUE;
    }
}
    free(d);
    return ok;
}

/* -------------------------------------------------------------------------- */
/* Grouping and cross-checking                                                */
/* -------------------------------------------------------------------------- */

typedef struct FlatRow
{
    const wchar_t *contest;
    const wchar_t *choice;
    int64_t total;
    uint32_t ci; /* contest index */
} FlatRow;

static FlatRow *flat_rows(const EeResults *r, size_t *out_n)
{
    FlatRow *f = (FlatRow *)malloc(((size_t)r->nchoices + 1) * sizeof(FlatRow));
    uint32_t c, k;
    size_t n = 0;
    if (f == NULL)
    {
        *out_n = 0;
        return NULL;
    }
    for (c = 0; c < r->ncontests; c++)
    {
        const EeResultsContest *ct = &r->contests[c];
        for (k = 0; k < ct->nchoices; k++)
        {
            f[n].contest = ct->title;
            f[n].choice = r->choices[ct->first_choice + k].name;
            f[n].total = r->choices[ct->first_choice + k].total;
            f[n].ci = c;
            n++;
        }
    }
    *out_n = n;
    return f;
}

static BOOL rows_match(const FlatRow *a, const FlatRow *b)
{
    return EeResults_NamesMatch(a->contest, b->contest) &&
           EeResults_NamesMatch(a->choice, b->choice);
}

/* Align two flat row lists (in order, tolerating insertions on either side). pair[i] =
 * index in b matched to a[i], or SIZE_MAX. Returns the number of matched rows. */
static size_t align_rows(const FlatRow *a, size_t na, const FlatRow *b, size_t nb, size_t *pair)
{
    size_t i = 0, j = 0, matched = 0;
    size_t k;
    for (k = 0; k < na; k++)
    {
        pair[k] = SIZE_MAX;
    }
    while (i < na && j < nb)
    {
        size_t la, lb;
        if (rows_match(&a[i], &b[j]))
        {
            pair[i++] = j++;
            matched++;
            continue;
        }
        /* Look ahead on each side for the nearer resynchronization point. */
        for (la = 1; la <= 64 && i + la < na; la++)
        {
            if (rows_match(&a[i + la], &b[j]))
            {
                break;
            }
        }
        for (lb = 1; lb <= 64 && j + lb < nb; lb++)
        {
            if (rows_match(&a[i], &b[j + lb]))
            {
                break;
            }
        }
        if (la <= 64 && i + la < na && (lb > 64 || j + lb >= nb || la <= lb))
        {
            i += la;
        }
        else if (lb <= 64 && j + lb < nb)
        {
            j += lb;
        }
        else
        {
            i++;
            j++;
        }
    }
    return matched;
}

static int format_rank(uint32_t fmt)
{
    switch (fmt)
    {
        case EE_RES_FMT_CLARITY_XML:
            return 5;
        case EE_RES_FMT_EE_EXPORT:
            return 4;
        case EE_RES_FMT_CLARITY_XLS:
            return 3;
        case EE_RES_FMT_CLARITY_TXT:
            return 2;
        default:
            return 1;
    }
}

/* Fraction (0..1) of rows two results share, in order. */
static double similarity(const EeResults *a, const EeResults *b)
{
    size_t na = 0, nb = 0, m;
    FlatRow *fa = flat_rows(a, &na);
    FlatRow *fb = flat_rows(b, &nb);
    size_t *pair = (size_t *)malloc((na + 1) * sizeof(size_t));
    double s = 0.0;
    if (fa != NULL && fb != NULL && pair != NULL && na > 0 && nb > 0)
    {
        m = align_rows(fa, na, fb, nb, pair);
        s = (double)m / (double)(na > nb ? na : nb);
    }
    free(fa);
    free(fb);
    free(pair);
    return s;
}

typedef struct IssueSink
{
    EeResults *r;
    size_t cap;
    uint32_t source;
    uint32_t count; /* issues for this source, including the ones not kept */
    BOOL oom;
} IssueSink;

static void is_add(IssueSink *s, const wchar_t *text)
{
    EeResults *r = s->r;
    s->count++;
    if (s->count > RES_MAX_ISSUES_PER_SOURCE || s->oom)
    {
        return;
    }
    if (!res_grow((void **)&r->issues, &s->cap, (size_t)r->nissues + 1, sizeof(EeResultsIssue)))
    {
        s->oom = TRUE;
        return;
    }
    r->issues[r->nissues].source = s->source;
    r->issues[r->nissues].text = wdup(text);
    if (r->issues[r->nissues].text != NULL)
    {
        r->nissues++;
    }
}

static void is_finish(IssueSink *s)
{
    if (s->count > RES_MAX_ISSUES_PER_SOURCE)
    {
        wchar_t msg[160];
        uint32_t keep = s->count;
        StringCchPrintfW(msg,
                         ARRAYSIZE(msg),
                         L"\x2026 and %u more differences.",
                         keep - RES_MAX_ISSUES_PER_SOURCE);
        s->count = 0; /* let the note through */
        is_add(s, msg);
        s->count = keep;
    }
}

/* Sum of the precincts' registered voters (or ballots), over the precincts that report
 * them (some precincts appear only inside contests); FALSE when none does. */
static BOOL precinct_sum(const EeResults *r, BOOL registered, int64_t *out)
{
    uint32_t p;
    int64_t s = 0;
    BOOL any = FALSE;
    for (p = 0; p < r->nprecincts; p++)
    {
        int64_t v = registered ? r->precincts[p].registered : r->precincts[p].ballots;
        if (v != EE_RES_UNKNOWN)
        {
            s += v;
            any = TRUE;
        }
    }
    *out = s;
    return any;
}

static void check_turnout(IssueSink *is, const EeResults *prim, const EeResults *oth)
{
    wchar_t msg[600], a[32], b[32];
    uint32_t p;
    int64_t psum = 0, osum = 0;
    BOOL same_basis = FALSE;
    /* A total that equals the other file's precinct sum is the same figure computed the
     * other way (Clarity primaries), not a disagreement. */
    if (precinct_sum(prim, TRUE, &psum) && psum == oth->registered_total)
    {
        same_basis = TRUE;
    }
    if (precinct_sum(oth, TRUE, &osum) && osum == prim->registered_total)
    {
        same_basis = TRUE;
    }
    if (prim->registered_total != EE_RES_UNKNOWN && oth->registered_total != EE_RES_UNKNOWN &&
        prim->registered_total != oth->registered_total && !same_basis)
    {
        fmt_count(oth->registered_total, a, ARRAYSIZE(a));
        fmt_count(prim->registered_total, b, ARRAYSIZE(b));
        StringCchPrintfW(msg,
                         ARRAYSIZE(msg),
                         L"Registered voters: %s here, %s in the shown file.",
                         a,
                         b);
        is_add(is, msg);
    }
    if (prim->ballots_total != EE_RES_UNKNOWN && oth->ballots_total != EE_RES_UNKNOWN &&
        prim->ballots_total != oth->ballots_total)
    {
        fmt_count(oth->ballots_total, a, ARRAYSIZE(a));
        fmt_count(prim->ballots_total, b, ARRAYSIZE(b));
        StringCchPrintfW(msg,
                         ARRAYSIZE(msg),
                         L"Ballots cast: %s here, %s in the shown file.",
                         a,
                         b);
        is_add(is, msg);
    }
    /* Per precinct (by name). */
    for (p = 0; p < oth->nprecincts; p++)
    {
        const EeResultsPrecinct *op = &oth->precincts[p];
        uint32_t q;
        for (q = 0; q < prim->nprecincts; q++)
        {
            if (wcscmp(prim->precincts[q].name, op->name) == 0)
            {
                break;
            }
        }
        if (q == prim->nprecincts)
        {
            continue;
        }
        if (op->registered != EE_RES_UNKNOWN && prim->precincts[q].registered != EE_RES_UNKNOWN &&
            op->registered != prim->precincts[q].registered)
        {
            fmt_count(op->registered, a, ARRAYSIZE(a));
            fmt_count(prim->precincts[q].registered, b, ARRAYSIZE(b));
            StringCchPrintfW(msg,
                             ARRAYSIZE(msg),
                             L"Precinct %s registered voters: %s here, %s in the shown file.",
                             op->name,
                             a,
                             b);
            is_add(is, msg);
        }
        if (op->ballots != EE_RES_UNKNOWN && prim->precincts[q].ballots != EE_RES_UNKNOWN &&
            op->ballots != prim->precincts[q].ballots)
        {
            fmt_count(op->ballots, a, ARRAYSIZE(a));
            fmt_count(prim->precincts[q].ballots, b, ARRAYSIZE(b));
            StringCchPrintfW(msg,
                             ARRAYSIZE(msg),
                             L"Precinct %s ballots cast: %s here, %s in the shown file.",
                             op->name,
                             a,
                             b);
            is_add(is, msg);
        }
    }
}

/* Precinct × vote type counts of contests that line up one to one. */
static void check_precincts(IssueSink *is, const EeResults *prim, const EeResults *oth)
{
    uint32_t c;
    uint32_t pv = EE_RESULTS_NVT(prim), ov = EE_RESULTS_NVT(oth);
    uint32_t *vmap = NULL; /* oth vote type -> prim vote type, or UINT32_MAX */
    uint32_t *pmap = NULL; /* oth precinct -> prim precinct, or UINT32_MAX */
    uint32_t *lmap = NULL; /* prim precinct -> local index in the current prim contest */
    uint32_t v, p;
    if (prim->ncontests != oth->ncontests || !EeResults_HasPrecinctDetail(prim) ||
        !EeResults_HasPrecinctDetail(oth))
    {
        return;
    }
    vmap = (uint32_t *)malloc((size_t)ov * sizeof(uint32_t));
    pmap = (uint32_t *)malloc(((size_t)oth->nprecincts + 1) * sizeof(uint32_t));
    lmap = (uint32_t *)malloc(((size_t)prim->nprecincts + 1) * sizeof(uint32_t));
    if (vmap == NULL || pmap == NULL || lmap == NULL)
    {
        goto done;
    }
    for (v = 0; v < ov; v++)
    {
        uint32_t w;
        vmap[v] = UINT32_MAX;
        for (w = 0; w < pv; w++)
        {
            if ((oth->nvt == 0 && prim->nvt == 0) ||
                (oth->nvt > 0 && prim->nvt > 0 && wieq(oth->vote_types[v], prim->vote_types[w])))
            {
                vmap[v] = w;
                break;
            }
        }
    }
    for (p = 0; p < oth->nprecincts; p++)
    {
        uint32_t q;
        pmap[p] = UINT32_MAX;
        for (q = 0; q < prim->nprecincts; q++)
        {
            if (wcscmp(prim->precincts[q].name, oth->precincts[p].name) == 0)
            {
                pmap[p] = q;
                break;
            }
        }
    }
    for (p = 0; p < prim->nprecincts; p++)
    {
        lmap[p] = UINT32_MAX;
    }
    for (c = 0; c < prim->ncontests; c++)
    {
        const EeResultsContest *pc = &prim->contests[c];
        const EeResultsContest *oc = &oth->contests[c];
        uint32_t k;
        if (pc->counts == NULL || oc->counts == NULL || pc->nchoices != oc->nchoices ||
            !EeResults_NamesMatch(pc->title, oc->title))
        {
            continue;
        }
        for (p = 0; p < pc->npre; p++)
        {
            lmap[pc->pre[p]] = p;
        }
        for (k = 0; k < oc->nchoices; k++)
        {
            for (p = 0; p < oc->npre; p++)
            {
                uint32_t gp = pmap[oc->pre[p]];
                uint32_t lp = gp == UINT32_MAX ? UINT32_MAX : lmap[gp];
                for (v = 0; v < ov; v++)
                {
                    int64_t ovv = oc->counts[((size_t)k * oc->npre + p) * ov + v];
                    int64_t pvv = 0;
                    if (vmap[v] == UINT32_MAX)
                    {
                        continue;
                    }
                    if (lp != UINT32_MAX)
                    {
                        pvv = pc->counts[((size_t)k * pc->npre + lp) * pv + vmap[v]];
                    }
                    if (ovv != pvv)
                    {
                        wchar_t msg[900], a[32], bb[32];
                        fmt_count(ovv, a, ARRAYSIZE(a));
                        fmt_count(pvv, bb, ARRAYSIZE(bb));
                        StringCchPrintfW(msg,
                                         ARRAYSIZE(msg),
                                         L"%s \x2014 %s, precinct %s%s%s: %s here, %s in the "
                                         L"shown file.",
                                         pc->title,
                                         prim->choices[pc->first_choice + k].name,
                                         oth->precincts[oc->pre[p]].name,
                                         oth->nvt > 0 ? L", " : L"",
                                         oth->nvt > 0 ? oth->vote_types[v] : L"",
                                         a,
                                         bb);
                        is_add(is, msg);
                    }
                }
            }
        }
        for (p = 0; p < pc->npre; p++)
        {
            lmap[pc->pre[p]] = UINT32_MAX;
        }
    }
done:
    free(vmap);
    free(pmap);
    free(lmap);
}

/* Cross-check @p oth against the shown result @p prim; issues go to @p dst. */
static void cross_check(EeResults *dst,
                        size_t *issue_cap,
                        uint32_t source,
                        const EeResults *prim,
                        const EeResults *oth)
{
    IssueSink is;
    size_t na = 0, nb = 0, i;
    FlatRow *fa = flat_rows(prim, &na);
    FlatRow *fb = flat_rows(oth, &nb);
    size_t *pair = (size_t *)malloc((na + 1) * sizeof(size_t));
    uint8_t *used = (uint8_t *)calloc(nb + 1, 1);
    uint32_t last_vf_contest = UINT32_MAX;
    wchar_t msg[900], a[32], b[32];

    ZeroMemory(&is, sizeof(is));
    is.r = dst;
    is.cap = *issue_cap;
    is.source = source;
    if (fa == NULL || fb == NULL || pair == NULL || used == NULL)
    {
        goto done;
    }
    align_rows(fa, na, fb, nb, pair);
    for (i = 0; i < na; i++)
    {
        const FlatRow *pr = &fa[i];
        if (pair[i] == SIZE_MAX)
        {
            StringCchPrintfW(msg,
                             ARRAYSIZE(msg),
                             L"%s \x2014 %s: missing here.",
                             pr->contest,
                             pr->choice);
            is_add(&is, msg);
            continue;
        }
        used[pair[i]] = 1;
        {
            const FlatRow *orow = &fb[pair[i]];
            const EeResultsContest *pc = &prim->contests[pr->ci];
            const EeResultsContest *oc = &oth->contests[orow->ci];
            if (orow->total != pr->total && orow->total != EE_RES_UNKNOWN &&
                pr->total != EE_RES_UNKNOWN)
            {
                fmt_count(orow->total, a, ARRAYSIZE(a));
                fmt_count(pr->total, b, ARRAYSIZE(b));
                StringCchPrintfW(msg,
                                 ARRAYSIZE(msg),
                                 L"%s \x2014 %s: %s votes here, %s in the shown file.",
                                 pr->contest,
                                 pr->choice,
                                 a,
                                 b);
                is_add(&is, msg);
            }
            if (pr->ci != last_vf_contest)
            {
                last_vf_contest = pr->ci;
                if (pc->vote_for != 0 && oc->vote_for != 0 && pc->vote_for != oc->vote_for)
                {
                    StringCchPrintfW(msg,
                                     ARRAYSIZE(msg),
                                     L"%s: vote for %u here, %u in the shown file.",
                                     pc->title,
                                     oc->vote_for,
                                     pc->vote_for);
                    is_add(&is, msg);
                }
                if (pc->has_over_under && oc->has_over_under &&
                    (pc->over_total != oc->over_total || pc->under_total != oc->under_total))
                {
                    wchar_t c1[32], c2[32];
                    fmt_count(oc->over_total, a, ARRAYSIZE(a));
                    fmt_count(oc->under_total, b, ARRAYSIZE(b));
                    fmt_count(pc->over_total, c1, ARRAYSIZE(c1));
                    fmt_count(pc->under_total, c2, ARRAYSIZE(c2));
                    StringCchPrintfW(msg,
                                     ARRAYSIZE(msg),
                                     L"%s: over/under votes %s/%s here, %s/%s in the shown file.",
                                     pc->title,
                                     a,
                                     b,
                                     c1,
                                     c2);
                    is_add(&is, msg);
                }
            }
        }
    }
    for (i = 0; i < nb; i++)
    {
        if (!used[i])
        {
            fmt_count(fb[i].total, a, ARRAYSIZE(a));
            StringCchPrintfW(msg,
                             ARRAYSIZE(msg),
                             L"%s \x2014 %s (%s votes): not in the shown file.",
                             fb[i].contest,
                             fb[i].choice,
                             a);
            is_add(&is, msg);
        }
    }
    check_turnout(&is, prim, oth);
    check_precincts(&is, prim, oth);
    is_finish(&is);
done:
    *issue_cap = is.cap;
    free(fa);
    free(fb);
    free(pair);
    free(used);
}

/* Take figures the shown result lacks from another file of the same election: contest
 * registered voters / ballots cast / precinct counts / over-under (summary.csv), and
 * ballots cast by vote type (the XLS turnout sheet). */
static void adopt_figures(EeResults *dst, const EeResults *oth)
{
    size_t na = 0, nb = 0, i;
    FlatRow *fa = flat_rows(dst, &na);
    FlatRow *fb = flat_rows(oth, &nb);
    size_t *pair = (size_t *)malloc((na + 1) * sizeof(size_t));
    uint32_t last = UINT32_MAX;
    if (fa != NULL && fb != NULL && pair != NULL)
    {
        align_rows(fa, na, fb, nb, pair);
        for (i = 0; i < na; i++)
        {
            EeResultsContest *pc;
            const EeResultsContest *oc;
            if (pair[i] == SIZE_MAX)
            {
                continue;
            }
            /* a choice total the shown file withholds */
            {
                const EeResultsContest *pc0 = &dst->contests[fa[i].ci];
                uint32_t k;
                for (k = 0; k < pc0->nchoices; k++)
                {
                    EeResultsChoice *ch = &dst->choices[pc0->first_choice + k];
                    if (ch->name == fa[i].choice && ch->total == EE_RES_UNKNOWN)
                    {
                        ch->total = fb[pair[i]].total;
                    }
                }
            }
            if (fa[i].ci == last)
            {
                continue;
            }
            last = fa[i].ci;
            pc = &dst->contests[fa[i].ci];
            oc = &oth->contests[fb[pair[i]].ci];
            if (pc->registered == EE_RES_UNKNOWN)
                pc->registered = oc->registered;
            if (pc->ballots == EE_RES_UNKNOWN)
                pc->ballots = oc->ballots;
            if (pc->precincts_total == EE_RES_UNKNOWN)
                pc->precincts_total = oc->precincts_total;
            if (pc->precincts_reporting == EE_RES_UNKNOWN)
                pc->precincts_reporting = oc->precincts_reporting;
            if (!pc->has_over_under && oc->has_over_under)
            {
                pc->has_over_under = TRUE;
                pc->over_total = oc->over_total;
                pc->under_total = oc->under_total;
            }
            if (pc->vote_for == 0)
                pc->vote_for = oc->vote_for;
        }
    }
    free(fa);
    free(fb);
    free(pair);

    /* Ballots cast by vote type, when the vote types are the same. */
    if (dst->prec_ballots_vt == NULL && oth->prec_ballots_vt != NULL && dst->nvt == oth->nvt &&
        dst->nvt > 0 && dst->nprecincts > 0)
    {
        uint32_t *vmap = (uint32_t *)malloc((size_t)oth->nvt * sizeof(uint32_t));
        uint32_t v, w, p, q;
        BOOL ok = vmap != NULL;
        for (v = 0; ok && v < oth->nvt; v++)
        {
            vmap[v] = UINT32_MAX;
            for (w = 0; w < dst->nvt; w++)
            {
                if (wieq(oth->vote_types[v], dst->vote_types[w]))
                {
                    vmap[v] = w;
                    break;
                }
            }
            ok = vmap[v] != UINT32_MAX;
        }
        if (ok)
        {
            dst->prec_ballots_vt =
                (int32_t *)calloc((size_t)dst->nprecincts * dst->nvt, sizeof(int32_t));
            dst->ballots_vt = (int64_t *)calloc(dst->nvt, sizeof(int64_t));
            if (dst->prec_ballots_vt == NULL || dst->ballots_vt == NULL)
            {
                free(dst->prec_ballots_vt);
                free(dst->ballots_vt);
                dst->prec_ballots_vt = NULL;
                dst->ballots_vt = NULL;
            }
            else
            {
                for (q = 0; q < oth->nprecincts; q++)
                {
                    for (p = 0; p < dst->nprecincts; p++)
                    {
                        if (wcscmp(dst->precincts[p].name, oth->precincts[q].name) == 0)
                        {
                            break;
                        }
                    }
                    if (p == dst->nprecincts)
                    {
                        continue;
                    }
                    for (v = 0; v < oth->nvt; v++)
                    {
                        int32_t val = oth->prec_ballots_vt[(size_t)q * oth->nvt + v];
                        dst->prec_ballots_vt[(size_t)p * dst->nvt + vmap[v]] = val;
                        dst->ballots_vt[vmap[v]] += val;
                    }
                }
            }
        }
        free(vmap);
    }
}

static BOOL add_source(EeResults *dst, size_t *cap, const Parsed *p, BOOL primary)
{
    EeResultsSource *s;
    if (!res_grow((void **)&dst->sources, cap, (size_t)dst->nsources + 1, sizeof(EeResultsSource)))
    {
        return FALSE;
    }
    s = &dst->sources[dst->nsources];
    ZeroMemory(s, sizeof(*s));
    s->path = wdup(p->path);
    s->member = p->member != NULL ? wdup(p->member) : NULL;
    s->format = p->format;
    s->primary = primary;
    s->ncontests = p->r.ncontests;
    s->nchoices = p->r.nchoices;
    s->nprecincts = p->r.nprecincts;
    dst->nsources++;
    dst->formats |= p->format;
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */

void EeResults_Init(EeResults *r)
{
    ZeroMemory(r, sizeof(*r));
    r->registered_total = EE_RES_UNKNOWN;
    r->ballots_total = EE_RES_UNKNOWN;
}

void EeResults_Clear(EeResults *r)
{
    uint32_t i;
    if (r == NULL)
    {
        return;
    }
    free(r->election_name);
    free(r->election_date);
    free(r->region);
    free(r->timestamp);
    for (i = 0; i < r->nvt; i++)
    {
        free(r->vote_types[i]);
    }
    free(r->vote_types);
    for (i = 0; i < r->nprecincts; i++)
    {
        free(r->precincts[i].name);
    }
    free(r->precincts);
    free(r->prec_ballots_vt);
    free(r->ballots_vt);
    for (i = 0; i < r->ncontests; i++)
    {
        EeResultsContest *c = &r->contests[i];
        free(c->title);
        free(c->party);
        free(c->pre);
        free(c->counts);
        free(c->protected_cells);
        free(c->over);
        free(c->under);
    }
    free(r->contests);
    for (i = 0; i < r->nchoices; i++)
    {
        free(r->choices[i].name);
        free(r->choices[i].party);
    }
    free(r->choices);
    for (i = 0; i < r->nsources; i++)
    {
        free(r->sources[i].path);
        free(r->sources[i].member);
    }
    free(r->sources);
    for (i = 0; i < r->nissues; i++)
    {
        free(r->issues[i].text);
    }
    free(r->issues);
    EeResults_Init(r);
}

void EeResults_FreeArray(EeResults *arr, uint32_t count)
{
    uint32_t i;
    if (arr == NULL)
    {
        return;
    }
    for (i = 0; i < count; i++)
    {
        EeResults_Clear(&arr[i]);
    }
    free(arr);
}

BOOL EeResults_HasPrecinctDetail(const EeResults *r)
{
    uint32_t c;
    for (c = 0; c < r->ncontests; c++)
    {
        if (r->contests[c].counts != NULL)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* Fill in name/date/region/timestamp the shown file lacks from another source. */
static void adopt_meta(EeResults *dst, EeResults *src)
{
    wchar_t **d[4];
    wchar_t **s[4];
    int i;
    d[0] = &dst->election_name;
    d[1] = &dst->election_date;
    d[2] = &dst->region;
    d[3] = &dst->timestamp;
    s[0] = &src->election_name;
    s[1] = &src->election_date;
    s[2] = &src->region;
    s[3] = &src->timestamp;
    for (i = 0; i < 4; i++)
    {
        if (*d[i] == NULL && *s[i] != NULL)
        {
            *d[i] = *s[i];
            *s[i] = NULL;
        }
    }
}

EeLoadStatus EeResults_LoadFiles(const wchar_t *const *paths,
                                 uint32_t count,
                                 EeResults **out,
                                 uint32_t *out_count,
                                 volatile LONG *cancel_flag,
                                 EeLoadProgressFn progress_fn,
                                 void *progress_user,
                                 wchar_t *error_message,
                                 size_t error_cch)
{
    ParsedList pl = {0};
    EeResults *res = NULL;
    uint32_t ngroups = 0;
    uint32_t *gfirst = NULL; /* per group: index of its first member */
    uint32_t i, g;
    EeLoadStatus status = EeLoadStatus_Error;

    if (out != NULL)
    {
        *out = NULL;
    }
    if (out_count != NULL)
    {
        *out_count = 0;
    }
    if (paths == NULL || count == 0 || out == NULL || out_count == NULL)
    {
        res_set_err(error_message, error_cch, L"Invalid arguments.");
        return EeLoadStatus_Error;
    }
    for (i = 0; i < count; i++)
    {
        if (cancel_flag != NULL && *cancel_flag != 0)
        {
            status = EeLoadStatus_Cancelled;
            goto cleanup;
        }
        if (progress_fn != NULL)
        {
            EeLoadProgress pr;
            ZeroMemory(&pr, sizeof(pr));
            pr.percent = (uint32_t)((uint64_t)i * 90u / count);
            pr.bytes_read = i;
            pr.bytes_total = count;
            if (!progress_fn(&pr, progress_user))
            {
                status = EeLoadStatus_Cancelled;
                goto cleanup;
            }
        }
        if (!load_path(paths[i], &pl, error_message, error_cch))
        {
            goto cleanup;
        }
    }

    /* Group: same contest list, one file per format per group. */
    gfirst = (uint32_t *)malloc((pl.n + 1) * sizeof(uint32_t));
    if (gfirst == NULL)
    {
        res_set_err(error_message, error_cch, L"Out of memory loading the results.");
        goto cleanup;
    }
    for (i = 0; i < (uint32_t)pl.n; i++)
    {
        uint32_t found = UINT32_MAX;
        for (g = 0; g < ngroups && found == UINT32_MAX; g++)
        {
            uint32_t j;
            BOOL clash = FALSE;
            for (j = 0; j < i; j++)
            {
                if (pl.p[j].group == g && pl.p[j].format == pl.p[i].format)
                {
                    clash = TRUE;
                    break;
                }
            }
            if (!clash && similarity(&pl.p[gfirst[g]].r, &pl.p[i].r) >= 0.8)
            {
                found = g;
            }
        }
        if (found == UINT32_MAX)
        {
            found = ngroups;
            gfirst[ngroups++] = i;
        }
        pl.p[i].group = found;
    }

    res = (EeResults *)calloc(ngroups, sizeof(EeResults));
    if (res == NULL)
    {
        res_set_err(error_message, error_cch, L"Out of memory loading the results.");
        goto cleanup;
    }
    for (g = 0; g < ngroups; g++)
    {
        uint32_t prim = UINT32_MAX;
        size_t src_cap = 0, issue_cap = 0;
        uint32_t own_issues;
        EeResults *dst = &res[g];
        EeResults_Init(dst);
        for (i = 0; i < (uint32_t)pl.n; i++)
        {
            if (pl.p[i].group == g && (prim == UINT32_MAX || format_rank(pl.p[i].format) >
                                                                 format_rank(pl.p[prim].format)))
            {
                prim = i;
            }
        }
        /* The shown file's data moves into the result, with its own load issues. */
        *dst = pl.p[prim].r;
        EeResults_Init(&pl.p[prim].r);
        own_issues = dst->nissues;
        issue_cap = dst->nissues;
        for (i = 0; i < own_issues; i++)
        {
            dst->issues[i].source = 0;
        }
        dst->sources = NULL;
        dst->nsources = 0;
        dst->formats = 0;
        pl.p[prim].r.ncontests = dst->ncontests; /* for the source summary */
        pl.p[prim].r.nchoices = dst->nchoices;
        pl.p[prim].r.nprecincts = dst->nprecincts;
        if (!add_source(dst, &src_cap, &pl.p[prim], TRUE))
        {
            res_set_err(error_message, error_cch, L"Out of memory loading the results.");
            goto cleanup;
        }
        pl.p[prim].r.ncontests = pl.p[prim].r.nchoices = pl.p[prim].r.nprecincts = 0;
        for (i = 0; i < (uint32_t)pl.n; i++)
        {
            Parsed *p = &pl.p[i];
            uint32_t k;
            uint32_t sidx;
            if (p->group != g || i == prim)
            {
                continue;
            }
            sidx = dst->nsources;
            if (!add_source(dst, &src_cap, p, FALSE))
            {
                res_set_err(error_message, error_cch, L"Out of memory loading the results.");
                goto cleanup;
            }
            /* The other file's own load notes, then the cross-check. */
            for (k = 0; k < p->r.nissues; k++)
            {
                if (res_grow((void **)&dst->issues,
                             &issue_cap,
                             (size_t)dst->nissues + 1,
                             sizeof(EeResultsIssue)))
                {
                    dst->issues[dst->nissues].source = sidx;
                    dst->issues[dst->nissues].text = p->r.issues[k].text;
                    p->r.issues[k].text = NULL;
                    dst->nissues++;
                }
            }
            cross_check(dst, &issue_cap, sidx, dst, &p->r);
            adopt_meta(dst, &p->r);
            adopt_figures(dst, &p->r);
        }
    }
    if (progress_fn != NULL)
    {
        EeLoadProgress pr;
        ZeroMemory(&pr, sizeof(pr));
        pr.percent = 100;
        pr.bytes_read = count;
        pr.bytes_total = count;
        progress_fn(&pr, progress_user);
    }
    *out = res;
    *out_count = ngroups;
    res = NULL;
    status = EeLoadStatus_Ok;

cleanup:
    if (res != NULL)
    {
        EeResults_FreeArray(res, ngroups);
    }
    for (i = 0; i < (uint32_t)pl.n; i++)
    {
        parsed_free(&pl.p[i]);
    }
    free(pl.p);
    free(gfirst);
    return status;
}

BOOL EeResults_Tabulate(const EeResults *r,
                        const uint8_t *precinct_mask,
                        const uint8_t *vt_mask,
                        int64_t *choice_totals,
                        int64_t *choice_vt,
                        int64_t *over,
                        int64_t *under)
{
    uint32_t nvt = EE_RESULTS_NVT(r);
    BOOL exact = TRUE;
    BOOL masked = precinct_mask != NULL || vt_mask != NULL;
    uint32_t c, k, p, v;
    if (choice_vt != NULL)
    {
        memset(choice_vt, 0, (size_t)r->nchoices * r->nvt * sizeof(int64_t));
    }
    for (c = 0; c < r->ncontests; c++)
    {
        const EeResultsContest *ct = &r->contests[c];
        if (ct->counts == NULL || !masked)
        {
            for (k = 0; k < ct->nchoices; k++)
            {
                choice_totals[ct->first_choice + k] = r->choices[ct->first_choice + k].total;
            }
            if (ct->counts == NULL && masked)
            {
                exact = FALSE;
            }
        }
        else
        {
            if (ct->nprotected > 0)
            {
                exact = FALSE;
            }
            for (k = 0; k < ct->nchoices; k++)
            {
                int64_t sum = 0;
                for (p = 0; p < ct->npre; p++)
                {
                    const int32_t *cell = ct->counts + ((size_t)k * ct->npre + p) * nvt;
                    if (precinct_mask != NULL && !precinct_mask[ct->pre[p]])
                    {
                        continue;
                    }
                    for (v = 0; v < nvt; v++)
                    {
                        if (r->nvt > 0 && vt_mask != NULL && !vt_mask[v])
                        {
                            continue;
                        }
                        sum += cell[v];
                    }
                }
                choice_totals[ct->first_choice + k] = sum;
            }
        }
        if (choice_vt != NULL && ct->counts != NULL && r->nvt > 0)
        {
            for (k = 0; k < ct->nchoices; k++)
            {
                for (p = 0; p < ct->npre; p++)
                {
                    const int32_t *cell = ct->counts + ((size_t)k * ct->npre + p) * nvt;
                    if (precinct_mask != NULL && !precinct_mask[ct->pre[p]])
                    {
                        continue;
                    }
                    for (v = 0; v < r->nvt; v++)
                    {
                        choice_vt[(size_t)(ct->first_choice + k) * r->nvt + v] += cell[v];
                    }
                }
            }
        }
        if (over != NULL || under != NULL)
        {
            int64_t so = 0, su = 0;
            if (!ct->has_over_under)
            {
                /* not reported */
            }
            else if (ct->over == NULL || precinct_mask == NULL)
            {
                so = ct->over_total;
                su = ct->under_total;
                if (ct->over == NULL && precinct_mask != NULL)
                {
                    exact = FALSE;
                }
            }
            else
            {
                for (p = 0; p < ct->npre; p++)
                {
                    if (precinct_mask[ct->pre[p]])
                    {
                        so += ct->over[p];
                        su += ct->under[p];
                    }
                }
            }
            if (over != NULL)
            {
                over[c] = so;
            }
            if (under != NULL)
            {
                under[c] = su;
            }
        }
    }
    return exact;
}

const wchar_t *EeResults_FormatLabel(uint32_t format)
{
    return format_label(format);
}
