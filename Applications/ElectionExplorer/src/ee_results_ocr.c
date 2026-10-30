/**
 * @file ee_results_ocr.c
 * @brief Scanned ES&S results reports (Electionware, and the older monospaced EL30A reports)
 *        read by OCR into EeRpdfDoc. See ee_results_ocr.h.
 */

#include "ee_results_ocr.h"
#include "ocr_win.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <wctype.h>
#include <strsafe.h>

#define RO_MAX_VT    8   /* vote-type columns kept */
#define RO_MAX_CELLS 24  /* numbers on one row */
#define RO_MAX_ROWS  128 /* rows of one contest in one precinct */
#define RO_MAX_TMPL  6   /* column layouts remembered (statistics and contest tables) */
#define RO_TEXT_CCH  400
#define RO_LABEL_CCH 256
#define RO_PEND      4   /* lines kept as a contest title */
#define RO_REVIEW_EX 3   /* rows that do not add up named in the note */

/* -------------------------------------------------------------------------- */
/* Types                                                                      */
/* -------------------------------------------------------------------------- */

/* One OCR word, in page units (the page image is 1000 wide), origin top left. */
typedef struct RoWord
{
    float x0, y0, x1, y1;
    float yc; /* centre, with the page's skew taken out */
    uint32_t line;
    wchar_t t[48];
} RoWord;

enum
{
    RO_INT = 1,
    RO_PCT = 2
};

typedef struct RoCell
{
    float x0, x1;
    int kind; /* RO_INT, RO_PCT */
    int64_t v;
    double pct;
} RoCell;

enum
{
    RL_TEXT,     /* words only */
    RL_HEAD,     /* a table heading ("TOTAL VOTES  EV  MAIL ...") */
    RL_HEADCONT, /* the second line of a heading */
    RL_FOOTER
};

typedef struct RoLine
{
    float y, x0, x1, h; /* centre, extent, tallest word */
    uint32_t w0, nw;    /* its words (by x) in RoPage.w */
    int kind;
    wchar_t text[RO_TEXT_CCH];   /* all its words */
    wchar_t label[RO_LABEL_CCH]; /* the words left of the numbers */
    float lab_x0;
    RoCell c[RO_MAX_CELLS];
    int nc;
} RoLine;

typedef struct RoPage
{
    RoWord *w;
    uint32_t nw, capw;
    RoLine *ln;
    uint32_t nl, capl;
    float h;      /* median word height */
    float space;  /* median gap between words of a line */
    float pitch;  /* median distance between rows */
    float height; /* page height in units */
} RoPage;

/* A table heading as read: the TOTAL column's right edge and the vote-type names. */
typedef struct RoHead
{
    float tot; /* < 0 when TOTAL was not read */
    float tot_x0;
    BOOL vote_pct; /* Electionware's "VOTE %" (a contest table, not statistics) */
    int nvt;
    float x0[RO_MAX_VT], x1[RO_MAX_VT];
    wchar_t name[RO_MAX_VT][40];
} RoHead;

/* The numeric columns in force: right edges. */
typedef struct RoCols
{
    BOOL valid;
    float tot;
    int nvt;
    float vt[RO_MAX_VT];
    uint32_t vti[RO_MAX_VT]; /* index in EeRpdfDoc.vote_types */
    float pct;               /* the percentage column's right edge, < 0 if not seen */
    int tmpl;                /* its layout in RoCx.tmpl, or -1 */
} RoCols;

/* A column layout remembered for headings OCR reads only in part. */
typedef struct RoTmpl
{
    BOOL vote_pct;
    int nvt;
    float rel[RO_MAX_VT]; /* right edge - the TOTAL column's */
    uint32_t vti[RO_MAX_VT];
    BOOL guessed[RO_MAX_VT]; /* found from its numbers, named by ro_guess_vt */
    float pct_rel;           /* < -999 if not seen */
} RoTmpl;

enum
{
    RR_CHOICE,
    RR_TOTAL,
    RR_OVER,
    RR_UNDER,
    RR_BALLOTS /* "Contest Totals": the choices, over and under votes */
};

/* One table row: v[0] the total, v[1 + k] vote type k. */
typedef struct RoRow
{
    wchar_t label[RO_LABEL_CCH];
    int kind;
    int64_t v[1 + RO_MAX_VT];
    BOOL has[1 + RO_MAX_VT];
    double pct;
    BOOL has_pct;
    BOOL synthetic; /* the precinct's ballots cast, standing in for a ballots row */
} RoRow;

/* A contest's rows in one precinct (or the summary), read before they are checked. */
typedef struct RoBlock
{
    BOOL open;
    wchar_t title[512];
    uint32_t vote_for;
    RoRow rows[RO_MAX_ROWS];
    int nrows;
} RoBlock;

/* A precinct's (or the summary's) ballots cast row, held until its counts are settled. */
typedef struct RoBal
{
    BOOL pending;
    int64_t tot;
    int ncols;
    uint32_t vti[RO_MAX_VT];
    int64_t vals[RO_MAX_VT];
    BOOL has[RO_MAX_VT];
    int tries;
} RoBal;

typedef struct RoVar
{
    wchar_t *name;
    uint32_t count;
} RoVar;

/* Spellings OCR gave one name, with how often: the commonest is kept. */
typedef struct RoVarList
{
    RoVar *v;
    uint32_t n, cap;
} RoVarList;

enum
{
    PK_UNKNOWN,
    PK_SUMMARY,
    PK_PRECINCT
};

enum
{
    RS_NONE,
    RS_STATS,
    RS_CONTEST
};

typedef struct RoCx
{
    EeRpdfDoc *d;
    BOOL oom;
    RoPage pg;
    RoCols cols;
    RoTmpl tmpl[RO_MAX_TMPL];
    int ntmpl;
    float label_lim; /* numbers start right of this (< 0 until a heading is read) */
    int section;
    uint32_t pre; /* current precinct, UINT32_MAX for none */
    BOOL erm;     /* the older monospaced report */
    int page_kind;
    BOOL head_done; /* election name, date and county read */
    float pre_y;    /* the precinct line's place (Electionware), < 0 until seen */
    wchar_t pend[RO_PEND][512];
    int npend;
    RoBlock blk;
    /* summary figures, kept until a precinct page shows the file is a precinct report */
    BOOL summary_data;
    int64_t bvt[RO_MAX_VT];
    BOOL reg_total_seen, bal_total_seen;
    RoBal bal;
    /* matching */
    RoVarList *cvar; /* per contest */
    uint32_t cap_cvar;
    RoVarList **chvar; /* per contest: per choice */
    uint32_t *cap_chvar;
    RoVarList *pvar; /* per precinct */
    uint32_t cap_pvar;
    unsigned char *used; /* contests seen in the current precinct */
    uint32_t cap_used;
    uint32_t last_contest; /* the previous contest, to try the next one first */
    /* what the load summary says */
    uint32_t pages, pages_read, pages_blank, pages_failed, pages_summary_skipped, pages_enlarged;
    wchar_t ocr_err[200];
    uint64_t rows, rows_fixed, rows_review, rows_unlabeled, rows_untitled, writein_rows;
    wchar_t review_ex[RO_REVIEW_EX][200];
    uint32_t nreview_ex;
    uint32_t dropped;
    wchar_t dropped_first[200];
    uint32_t turns_changed;
    uint32_t page;       /* the page being read (0-based) */
    BOOL learning;       /* the first pages' pass that learns the column layouts (RoTmpl) */
    BOOL reg_row_seen;   /* the current precinct's registered voters row was read */
    uint32_t pre_unread; /* precinct lines OCR could not read */
    BOOL pre_placeholder; /* the current precinct's name was not read (yet) */
    size_t turnout_mark;  /* the current part's first turnout entry */
    int64_t reg_base, bal_base; /* the precinct's registered / ballots before this part */
} RoCx;

/* -------------------------------------------------------------------------- */
/* Small helpers                                                              */
/* -------------------------------------------------------------------------- */

static void ro_set_err(wchar_t *err, size_t cch, const wchar_t *msg)
{
    if (err != NULL && cch > 0)
    {
        StringCchCopyW(err, cch, msg);
    }
}

static BOOL ro_grow(void **arr, uint32_t *cap, uint32_t need, size_t elem)
{
    uint32_t nc;
    void *p;
    if (need <= *cap)
    {
        return TRUE;
    }
    nc = *cap < 16 ? 16 : *cap * 2;
    while (nc < need)
    {
        nc *= 2;
    }
    p = realloc(*arr, (size_t)nc * elem);
    if (p == NULL)
    {
        return FALSE;
    }
    memset((char *)p + (size_t)*cap * elem, 0, (size_t)(nc - *cap) * elem);
    *arr = p;
    *cap = nc;
    return TRUE;
}

static BOOL ro_growz(void **arr, size_t *cap, size_t need, size_t elem)
{
    size_t nc;
    void *p;
    if (need <= *cap)
    {
        return TRUE;
    }
    nc = *cap < 64 ? 64 : *cap * 2;
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
    return TRUE;
}

static wchar_t *ro_wdup(const wchar_t *s)
{
    size_t n = wcslen(s) + 1;
    wchar_t *p = (wchar_t *)malloc(n * sizeof(wchar_t));
    if (p != NULL)
    {
        memcpy(p, s, n * sizeof(wchar_t));
    }
    return p;
}

/* Uppercase letters and digits of @p s only ("Over Votes ." -> "OVERVOTES"). */
static void ro_key(const wchar_t *s, wchar_t *out, size_t cch)
{
    size_t o = 0;
    for (; *s != L'\0' && o + 1 < cch; s++)
    {
        if (iswalnum(*s))
        {
            out[o++] = (wchar_t)towupper(*s);
        }
    }
    out[o] = L'\0';
}

static BOOL ro_starts(const wchar_t *s, const wchar_t *prefix)
{
    return wcsncmp(s, prefix, wcslen(prefix)) == 0;
}

static BOOL ro_ends(const wchar_t *s, const wchar_t *suffix)
{
    size_t n = wcslen(s), m = wcslen(suffix);
    return n >= m && wcscmp(s + n - m, suffix) == 0;
}

/* Edit distance of two keys (each cut at 96 characters). */
static int ro_lev(const wchar_t *a, const wchar_t *b)
{
    int prev[97], cur[97];
    int n = (int)wcslen(a), m = (int)wcslen(b), i, j;
    if (n > 96)
    {
        n = 96;
    }
    if (m > 96)
    {
        m = 96;
    }
    for (j = 0; j <= m; j++)
    {
        prev[j] = j;
    }
    for (i = 1; i <= n; i++)
    {
        cur[0] = i;
        for (j = 1; j <= m; j++)
        {
            int c = prev[j - 1] + (a[i - 1] == b[j - 1] ? 0 : 1);
            if (prev[j] + 1 < c)
            {
                c = prev[j] + 1;
            }
            if (cur[j - 1] + 1 < c)
            {
                c = cur[j - 1] + 1;
            }
            cur[j] = c;
        }
        memcpy(prev, cur, sizeof(int) * (size_t)(m + 1));
    }
    return prev[m];
}

/* The parts of a name that tell two otherwise alike names apart: its numbers and one-letter
 * words ("Pl 2" / "Pl 4", "Proposition A" / "Proposition B"). */
static void ro_sig(const wchar_t *s, wchar_t *out, size_t cch)
{
    size_t o = 0;
    while (*s != L'\0' && o + 2 < cch)
    {
        const wchar_t *b = s, *e = s;
        BOOL digit = FALSE;
        while (*e != L'\0' && iswalnum(*e))
        {
            digit = digit || iswdigit(*e);
            e++;
        }
        /* (a lone letter or digit before a word starting in lowercase is a word OCR split:
         * "Point 1 sabel") */
        if (e > b && (digit || e - b == 1) && !(e - b == 1 && e[0] == L' ' && iswlower(e[1])))
        {
            for (s = b; s < e && o + 2 < cch; s++)
            {
                if (iswdigit(*s) || e - b == 1)
                {
                    out[o++] = (wchar_t)towupper(*s);
                }
            }
            out[o++] = L' ';
        }
        s = *e != L'\0' ? e + 1 : e;
    }
    out[o] = L'\0';
}

/* How alike two names are, 0..1: 0 when their numbers or one-letter words differ. */
static double ro_similar(const wchar_t *a, const wchar_t *b)
{
    wchar_t ka[128], kb[128], sa[64], sb[64];
    int n, m, d;
    ro_sig(a, sa, ARRAYSIZE(sa));
    ro_sig(b, sb, ARRAYSIZE(sb));
    if (wcscmp(sa, sb) != 0)
    {
        return 0.0;
    }
    ro_key(a, ka, ARRAYSIZE(ka));
    ro_key(b, kb, ARRAYSIZE(kb));
    n = (int)wcslen(ka);
    m = (int)wcslen(kb);
    if (n == 0 || m == 0)
    {
        return n == m ? 1.0 : 0.0;
    }
    if (abs(n - m) * 3 > (n > m ? n : m))
    {
        return 0.0;
    }
    d = ro_lev(ka, kb);
    return 1.0 - (double)d / (double)(n > m ? n : m);
}

static void ro_var_add(RoCx *cx, RoVarList *vl, const wchar_t *name)
{
    uint32_t i;
    for (i = 0; i < vl->n; i++)
    {
        if (wcscmp(vl->v[i].name, name) == 0)
        {
            vl->v[i].count++;
            return;
        }
    }
    if (vl->n >= 24)
    {
        return; /* enough spellings to choose from */
    }
    if (!ro_grow((void **)&vl->v, &vl->cap, vl->n + 1, sizeof(RoVar)) || (vl->v[vl->n].name = ro_wdup(name)) == NULL)
    {
        cx->oom = TRUE;
        return;
    }
    vl->v[vl->n++].count = 1;
}

/* The commonest spelling (the first seen of equals), or NULL. */
static const wchar_t *ro_var_best(const RoVarList *vl)
{
    uint32_t i, best = 0;
    if (vl->n == 0)
    {
        return NULL;
    }
    for (i = 1; i < vl->n; i++)
    {
        if (vl->v[i].count > vl->v[best].count)
        {
            best = i;
        }
    }
    return vl->v[best].name;
}

/* The best similarity of @p name to the spellings in @p vl. */
static double ro_var_similar(const RoVarList *vl, const wchar_t *name)
{
    uint32_t i;
    double best = 0.0;
    for (i = 0; i < vl->n && best < 1.0; i++)
    {
        double s = wcscmp(vl->v[i].name, name) == 0 ? 1.0 : ro_similar(vl->v[i].name, name);
        if (s > best)
        {
            best = s;
        }
    }
    return best;
}

static void ro_var_free(RoVarList *vl)
{
    uint32_t i;
    for (i = 0; i < vl->n; i++)
    {
        free(vl->v[i].name);
    }
    free(vl->v);
    ZeroMemory(vl, sizeof(*vl));
}

/* -------------------------------------------------------------------------- */
/* Building the document                                                      */
/* -------------------------------------------------------------------------- */

static void ro_note(RoCx *cx, const wchar_t *text)
{
    EeRpdfDoc *d = cx->d;
    if (!ro_grow((void **)&d->notes, &d->cap_notes, d->nnotes + 1, sizeof(wchar_t *)) ||
        (d->notes[d->nnotes] = ro_wdup(text)) == NULL)
    {
        cx->oom = TRUE;
        return;
    }
    d->nnotes++;
}

/* A vote type's index, added on first use. */
static uint32_t ro_vt(RoCx *cx, const wchar_t *name)
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
    if (d->nvt >= RO_MAX_VT)
    {
        return RO_MAX_VT - 1;
    }
    if (!ro_grow((void **)&d->vote_types, &d->cap_vt, d->nvt + 1, sizeof(wchar_t *)) ||
        (d->vote_types[d->nvt] = ro_wdup(name)) == NULL)
    {
        cx->oom = TRUE;
        return 0;
    }
    cx->bvt[d->nvt] = EE_RPDF_UNKNOWN;
    return d->nvt++;
}

static wchar_t ro_digit_of(wchar_t c);

/* A precinct name's key: the older report's leading number ("0051 Precinct 51" -> "0051"), else
 * the name without "Precinct" ("Precinct 12A", "12A" -> "12A"), uppercase without spaces; ""
 * when that leaves no digit (then names are matched by likeness). */
static void ro_pre_key(const wchar_t *name, wchar_t *key, size_t cch)
{
    size_t k = 0, o = 0;
    BOOL digit = FALSE;
    while (iswdigit(name[k]) && k < 15)
    {
        k++;
    }
    if (k >= 3 && name[k] == L' ' && iswalpha(name[k + 1]))
    {
        StringCchCopyNW(key, cch, name, k);
        return;
    }
    k = 0;
    if (_wcsnicmp(name, L"Precinct ", 9) == 0)
    {
        k = 9;
    }
    else if (_wcsnicmp(name, L"Pct ", 4) == 0 || _wcsnicmp(name, L"Pct. ", 5) == 0)
    {
        k = name[3] == L'.' ? 5 : 4;
    }
    for (; name[k] != L'\0' && o + 1 < cch; k++)
    {
        if (iswalnum(name[k]) || name[k] == L',')
        {
            key[o++] = (wchar_t)towupper(ro_digit_of(name[k]) != name[k] && o > 0 && iswdigit(key[o - 1])
                                             ? ro_digit_of(name[k])
                                             : name[k]);
            digit = digit || iswdigit(key[o - 1]);
        }
    }
    key[o] = L'\0';
    if (!digit)
    {
        key[0] = L'\0';
    }
}

/* The precinct a precinct line names: by its key (ro_pre_key), else by the name's likeness. */
static uint32_t ro_precinct(RoCx *cx, const wchar_t *name)
{
    EeRpdfDoc *d = cx->d;
    uint32_t i;
    wchar_t key[48], pk[48];
    ro_pre_key(name, key, ARRAYSIZE(key));
    for (i = d->nprecincts; i-- > 0;)
    {
        BOOL same;
        if (key[0] != L'\0')
        {
            ro_pre_key(d->precincts[i].name, pk, ARRAYSIZE(pk));
            same = wcscmp(pk, key) == 0;
        }
        else
        {
            same = ro_var_similar(&cx->pvar[i], name) >= 0.8;
        }
        if (same)
        {
            ro_var_add(cx, &cx->pvar[i], name);
            return i;
        }
    }
    if (!ro_grow((void **)&d->precincts, &d->cap_pre, d->nprecincts + 1, sizeof(EeRpdfPrecinct)) ||
        !ro_grow((void **)&cx->pvar, &cx->cap_pvar, d->nprecincts + 1, sizeof(RoVarList)) ||
        (d->precincts[d->nprecincts].name = ro_wdup(name)) == NULL)
    {
        cx->oom = TRUE;
        return 0;
    }
    d->precincts[d->nprecincts].registered = EE_RPDF_UNKNOWN;
    d->precincts[d->nprecincts].ballots = EE_RPDF_UNKNOWN;
    ro_var_add(cx, &cx->pvar[d->nprecincts], name);
    return d->nprecincts++;
}

/* The digits of a title, less a lone digit that is a split letter ("P 1 ace 8"). */
static void ro_title_digits(const wchar_t *t, wchar_t *out, size_t cch)
{
    size_t o = 0;
    const wchar_t *p;
    for (p = t; *p != L'\0' && o + 1 < cch; p++)
    {
        if (iswdigit(*p) && !(p > t && p[-1] == L' ' && p[1] == L' ' && iswlower(p[2])))
        {
            out[o++] = *p;
        }
    }
    out[o] = L'\0';
}

/* @p sub can be @p full with characters left out. */
static BOOL ro_subseq(const wchar_t *sub, const wchar_t *full)
{
    for (; *sub != L'\0'; sub++)
    {
        while (*full != L'\0' && *full != *sub)
        {
            full++;
        }
        if (*full == L'\0')
        {
            return FALSE;
        }
        full++;
    }
    return TRUE;
}

/* A title read @p b alike the title @p a but for a misread mark or letter or a lost digit ("P] ace
 * 4", "Place g", "State Senator, District"): @p b's digits are @p a's, some perhaps lost, and their
 * letters and digits differ by little. */
static BOOL ro_near_title(const wchar_t *a, const wchar_t *b)
{
    wchar_t da[32], db[32], ka[128], kb[128];
    int n, m;
    ro_title_digits(a, da, ARRAYSIZE(da));
    ro_title_digits(b, db, ARRAYSIZE(db));
    if (!ro_subseq(db, da))
    {
        return FALSE; /* ("Place 7" / "Place 8" are two contests, one after the other) */
    }
    ro_key(a, ka, ARRAYSIZE(ka));
    ro_key(b, kb, ARRAYSIZE(kb));
    n = (int)wcslen(ka);
    m = (int)wcslen(kb);
    return n > 0 && m > 0 && 1.0 - (double)ro_lev(ka, kb) / (double)(n > m ? n : m) >= 0.8;
}

/* Room for @p need contests in the per-contest spellings arrays. */
static BOOL ro_grow_cside(RoCx *cx, uint32_t need)
{
    uint32_t nc, old = cx->cap_cvar;
    void *p;
    if (need <= old)
    {
        return TRUE;
    }
    nc = old < 16 ? 16 : old * 2;
    while (nc < need)
    {
        nc *= 2;
    }
    if ((p = realloc(cx->cvar, (size_t)nc * sizeof(RoVarList))) == NULL)
    {
        return FALSE;
    }
    cx->cvar = (RoVarList *)p;
    memset(cx->cvar + old, 0, (size_t)(nc - old) * sizeof(RoVarList));
    if ((p = realloc(cx->chvar, (size_t)nc * sizeof(RoVarList *))) == NULL)
    {
        return FALSE;
    }
    cx->chvar = (RoVarList **)p;
    memset(cx->chvar + old, 0, (size_t)(nc - old) * sizeof(RoVarList *));
    if ((p = realloc(cx->cap_chvar, (size_t)nc * sizeof(uint32_t))) == NULL)
    {
        return FALSE;
    }
    cx->cap_chvar = (uint32_t *)p;
    memset(cx->cap_chvar + old, 0, (size_t)(nc - old) * sizeof(uint32_t));
    cx->cap_cvar = nc;
    return TRUE;
}

static BOOL ro_is_used(const RoCx *cx, uint32_t c)
{
    return c < cx->cap_used && cx->used[c] != 0;
}

/* The contest a title names: the likest one not yet seen in this precinct, else a new one. */
static uint32_t ro_contest(RoCx *cx, const wchar_t *title, uint32_t vote_for)
{
    EeRpdfDoc *d = cx->d;
    uint32_t i, best = UINT32_MAX;
    double bs = 0.0;
    EeRpdfContest *c;
    /* contests come in the same order in each precinct: the one after the last first */
    if (cx->last_contest + 1 < d->ncontests)
    {
        uint32_t n = cx->last_contest + 1;
        if (!ro_is_used(cx, n) && d->contests[n].vote_for == vote_for &&
            ro_var_similar(&cx->cvar[n], title) >= 0.85)
        {
            best = n;
            bs = 1.0;
        }
    }
    for (i = 0; i < d->ncontests && bs < 1.0; i++)
    {
        double s;
        if (ro_is_used(cx, i) || d->contests[i].vote_for != vote_for)
        {
            continue;
        }
        s = ro_var_similar(&cx->cvar[i], title);
        if (s > bs)
        {
            bs = s;
            best = i;
        }
    }
    if ((best == UINT32_MAX || bs < 0.8) && cx->last_contest + 1 < d->ncontests)
    {
        /* the contest that comes next, with a mark misread or a digit lost ("P] ace 4", "Place g") */
        uint32_t n = cx->last_contest + 1;
        if (!ro_is_used(cx, n) && d->contests[n].vote_for == vote_for && ro_near_title(d->contests[n].title, title))
        {
            best = n;
            bs = 1.0;
        }
    }
    if (best == UINT32_MAX || bs < 0.8)
    {
        if (!ro_grow((void **)&d->contests, &d->cap_contests, d->ncontests + 1, sizeof(EeRpdfContest)) ||
            !ro_grow_cside(cx, d->ncontests + 1))
        {
            cx->oom = TRUE;
            return 0;
        }
        c = &d->contests[d->ncontests];
        ZeroMemory(c, sizeof(*c));
        c->title = ro_wdup(title);
        c->vote_for = vote_for;
        c->over = EE_RPDF_UNKNOWN;
        c->under = EE_RPDF_UNKNOWN;
        c->ballots = EE_RPDF_UNKNOWN;
        if (c->title == NULL)
        {
            cx->oom = TRUE;
            return 0;
        }
        best = d->ncontests++;
    }
    ro_var_add(cx, &cx->cvar[best], title);
    if (ro_grow((void **)&cx->used, &cx->cap_used, best + 1, 1))
    {
        cx->used[best] = 1;
    }
    else
    {
        cx->oom = TRUE;
    }
    cx->last_contest = best;
    return best;
}

/* A choice name's key for likeness: letters and digits, uppercase, with the digits OCR gives for
 * letters as those letters ("Eddie Lucio 111" = "Eddie Lucio III", "(L 1B)" = "(LIB)"). */
static void ro_choice_key(const wchar_t *s, wchar_t *out, size_t cch)
{
    size_t o = 0;
    for (; *s != L'\0' && o + 1 < cch; s++)
    {
        wchar_t c = (wchar_t)towupper(*s);
        if (!iswalnum(c))
        {
            continue;
        }
        switch (c)
        {
            case L'1':
            case L'L':
                c = L'I';
                break;
            case L'0':
                c = L'O';
                break;
            case L'8':
                c = L'B';
                break;
            case L'5':
                c = L'S';
                break;
            default:
                break;
        }
        out[o++] = c;
    }
    out[o] = L'\0';
}

/* How alike two choice names are, 0..1: by edit distance of their keys, or by one key holding
 * the other at its start or end when OCR cut the name ("B. Vela (DEM)" for "Filemon B. Vela
 * (DEM)"). */
static double ro_like_choice(const wchar_t *a, const wchar_t *b)
{
    wchar_t ka[128], kb[128];
    int n, m, d;
    double s;
    ro_choice_key(a, ka, ARRAYSIZE(ka));
    ro_choice_key(b, kb, ARRAYSIZE(kb));
    n = (int)wcslen(ka);
    m = (int)wcslen(kb);
    if (n == 0 || m == 0)
    {
        return n == m ? 1.0 : 0.0;
    }
    d = ro_lev(ka, kb);
    s = 1.0 - (double)d / (double)(n > m ? n : m);
    if (n >= 6 && m >= 6)
    {
        const wchar_t *lng = n > m ? ka : kb, *sht = n > m ? kb : ka;
        size_t ls = wcslen(lng), ss = wcslen(sht);
        if (ss * 2 >= ls && (wcsncmp(lng, sht, ss) == 0 || wcscmp(lng + ls - ss, sht) == 0) && s < 0.8)
        {
            s = 0.8;
        }
    }
    return s;
}

static double ro_var_like_choice(const RoVarList *vl, const wchar_t *name)
{
    uint32_t i;
    double best = 0.0;
    for (i = 0; i < vl->n && best < 1.0; i++)
    {
        double s = ro_like_choice(vl->v[i].name, name);
        if (s > best)
        {
            best = s;
        }
    }
    return best;
}

/* The choice a row's name gives, the likest not in @p taken (one flag per choice). */
static uint32_t ro_choice(RoCx *cx, uint32_t ci, const wchar_t *name, const wchar_t *party, const unsigned char *taken,
                          uint32_t ntaken, uint32_t at)
{
    EeRpdfContest *c = &cx->d->contests[ci];
    uint32_t i, best = UINT32_MAX;
    double bs = 0.0;
    for (i = 0; i < c->nchoices; i++)
    {
        double s;
        if (i < ntaken && taken[i])
        {
            continue;
        }
        s = ro_var_like_choice(&cx->chvar[ci][i], name);
        if (s > bs || (s == bs && s > 0.0 && i == at))
        {
            bs = s;
            best = i;
        }
    }
    /* a badly read name in its usual place ("LE Hawk Dunlap" for "LIB Hawk Dunlap") */
    if ((best == UINT32_MAX || bs < 0.75) && at < c->nchoices && !(at < ntaken && taken[at]) &&
        ro_var_like_choice(&cx->chvar[ci][at], name) >= 0.5)
    {
        best = at;
        bs = 1.0;
    }
    if (best == UINT32_MAX || bs < 0.75)
    {
        if (!ro_grow((void **)&c->choices, &c->cap_choices, c->nchoices + 1, sizeof(EeRpdfChoice)) ||
            !ro_grow((void **)&cx->chvar[ci], &cx->cap_chvar[ci], c->nchoices + 1, sizeof(RoVarList)) ||
            (c->choices[c->nchoices].name = ro_wdup(name)) == NULL)
        {
            cx->oom = TRUE;
            return 0;
        }
        c->choices[c->nchoices].party = (party != NULL && party[0] != L'\0') ? ro_wdup(party) : NULL;
        c->choices[c->nchoices].total = EE_RPDF_UNKNOWN;
        best = c->nchoices++;
    }
    ro_var_add(cx, &cx->chvar[ci][best], name);
    return best;
}

static void ro_count(RoCx *cx, BOOL turnout, uint32_t contest, uint32_t choice, uint32_t vt, int64_t v)
{
    EeRpdfDoc *d = cx->d;
    EeRpdfCount **arr = turnout ? &d->turnout : &d->counts;
    size_t *n = turnout ? &d->nturnout : &d->ncounts;
    size_t *cap = turnout ? &d->cap_turnout : &d->cap_counts;
    EeRpdfCount *e;
    if (!ro_growz((void **)arr, cap, *n + 1, sizeof(EeRpdfCount)))
    {
        cx->oom = TRUE;
        return;
    }
    e = &(*arr)[(*n)++];
    e->contest = contest;
    e->choice = choice;
    e->pre = cx->pre;
    e->vt = vt;
    e->votes = v;
}

/* A precinct page after summary pages: the file is a precinct report, so the summary's
 * figures are dropped (its contests stay, their counts come from the precincts). */
static void ro_drop_summary(RoCx *cx)
{
    EeRpdfDoc *d = cx->d;
    uint32_t i, k;
    for (i = 0; i < d->ncontests; i++)
    {
        for (k = 0; k < d->contests[i].nchoices; k++)
        {
            d->contests[i].choices[k].total = EE_RPDF_UNKNOWN;
        }
        d->contests[i].over = d->contests[i].under = EE_RPDF_UNKNOWN;
    }
    d->registered_total = d->ballots_total = EE_RPDF_UNKNOWN;
    for (i = 0; i < RO_MAX_VT; i++)
    {
        cx->bvt[i] = EE_RPDF_UNKNOWN;
    }
    cx->summary_data = FALSE;
}

/* -------------------------------------------------------------------------- */
/* Numbers                                                                    */
/* -------------------------------------------------------------------------- */

/* A digit OCR may give as a letter in a number. */
static wchar_t ro_digit_of(wchar_t c)
{
    switch (c)
    {
        case L'o':
        case L'O':
            return L'0';
        case L'l':
        case L'I':
        case L'i':
        case L'|':
        case L'!':
            return L'1';
        default:
            return c;
    }
}

/* The token could be (part of) a number: digits, separators, look-alike letters, "%". */
static BOOL ro_numish(const wchar_t *s)
{
    size_t n = wcslen(s), i;
    if (n == 0)
    {
        return FALSE;
    }
    for (i = 0; i < n; i++)
    {
        wchar_t c = ro_digit_of(s[i]);
        if (!(iswdigit(c) || c == L',' || c == L'.' || c == L'%' || c == L'/'))
        {
            return FALSE;
        }
    }
    return TRUE;
}

/* Read a cell: RO_INT ("1,700", "o" = 0, "18.150"), RO_PCT ("62.06", ".19", "44.66%",
 * "44.690/0" for "44.69%"), or 0. */
static int ro_number(const wchar_t *s, int64_t *v, double *pct)
{
    wchar_t b[48];
    size_t n = 0, i, ndig = 0, real = 0;
    BOOL is_pct = FALSE;
    const wchar_t *dot;
    for (i = 0; s[i] != L'\0' && n + 1 < ARRAYSIZE(b); i++)
    {
        if (s[i] != L' ')
        {
            b[n++] = s[i];
        }
    }
    b[n] = L'\0';
    while (n > 0 && (b[n - 1] == L'.' || b[n - 1] == L',' || b[n - 1] == L':'))
    {
        b[--n] = L'\0';
    }
    if (n >= 3 && wcscmp(b + n - 3, L"0/0") == 0 && wcschr(b, L'.') != NULL)
    {
        n -= 3;
        b[n] = L'\0';
        is_pct = TRUE;
    }
    else if (n >= 1 && b[n - 1] == L'%')
    {
        b[--n] = L'\0';
        is_pct = TRUE;
    }
    if (n == 0)
    {
        return 0;
    }
    /* "34!589": a mark with three digits after it, at the end, is the thousands comma */
    if (n >= 5 && (b[n - 4] == L'!' || b[n - 4] == L'|' || b[n - 4] == L'\'') && iswdigit(b[n - 5]) && iswdigit(b[n - 3]) &&
        iswdigit(b[n - 2]) && iswdigit(b[n - 1]))
    {
        b[n - 4] = L',';
    }
    for (i = 0; i < n; i++)
    {
        wchar_t c = ro_digit_of(b[i]);
        if (iswdigit(b[i]))
        {
            real++;
        }
        if (iswdigit(c))
        {
            b[i] = c;
            ndig++;
        }
        else if (c != L',' && c != L'.')
        {
            return 0;
        }
    }
    if (ndig == 0 || (real == 0 && n > 1))
    {
        return 0;
    }
    dot = wcsrchr(b, L'.');
    {
        /* "55,16": a percentage whose point OCR read as a comma (no thousands group has 2) */
        wchar_t *cm = wcsrchr(b, L',');
        if (dot == NULL && cm != NULL && wcslen(cm + 1) == 2 && wcschr(b, L',') == cm && cm - b <= 3)
        {
            *cm = L'.';
            dot = cm;
        }
    }
    if (is_pct || (dot != NULL && wcslen(dot + 1) == 2 && wcschr(b, L',') == NULL))
    {
        double whole = 0.0, frac = 0.0, scale = 0.1;
        BOOL after = FALSE;
        for (i = 0; i < n; i++)
        {
            if (b[i] == L'.')
            {
                after = TRUE;
            }
            else if (iswdigit(b[i]))
            {
                if (after)
                {
                    frac += (b[i] - L'0') * scale;
                    scale /= 10.0;
                }
                else
                {
                    whole = whole * 10.0 + (b[i] - L'0');
                }
            }
        }
        *pct = whole + frac;
        return *pct <= 100.0 ? RO_PCT : 0;
    }
    /* thousands separators: every group after the first is 3 digits ("18.150" is 18,150) */
    {
        int64_t val = 0;
        size_t grp = 0, ngrp = 0;
        for (i = 0; i < n; i++)
        {
            if (b[i] == L',' || b[i] == L'.')
            {
                if ((ngrp > 0 && grp != 3) || grp == 0 || grp > 3)
                {
                    return 0;
                }
                ngrp++;
                grp = 0;
            }
            else
            {
                val = val * 10 + (b[i] - L'0');
                grp++;
                if (val > 100000000)
                {
                    return 0;
                }
            }
        }
        if (ngrp > 0 && grp != 3)
        {
            return 0;
        }
        *v = val;
        return RO_INT;
    }
}

/* -------------------------------------------------------------------------- */
/* Page OCR to rows                                                           */
/* -------------------------------------------------------------------------- */

static int __cdecl ro_float_cmp(const void *pa, const void *pb)
{
    float a = *(const float *)pa, b = *(const float *)pb;
    return a < b ? -1 : a > b ? 1 : 0;
}

static int __cdecl ro_word_y_cmp(const void *pa, const void *pb)
{
    const RoWord *a = (const RoWord *)pa, *b = (const RoWord *)pb;
    return a->yc < b->yc ? -1 : a->yc > b->yc ? 1 : 0;
}

static int __cdecl ro_word_x_cmp(const void *pa, const void *pb)
{
    const RoWord *a = (const RoWord *)pa, *b = (const RoWord *)pb;
    return a->x0 < b->x0 ? -1 : a->x0 > b->x0 ? 1 : 0;
}

/* Words a page reads well enough to tell its orientation: words of 3+ letters with a vowel,
 * and numbers of 2+ digits. */
static uint32_t ro_score(const EeOcrText *t)
{
    uint32_t i, score = 0;
    for (i = 0; i < t->nwords; i++)
    {
        const char *s = t->text + t->words[i].text_off;
        uint32_t n = t->words[i].text_len, k, letters = 0, digits = 0, vowels = 0, other = 0;
        for (k = 0; k < n; k++)
        {
            char c = s[k];
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))
            {
                letters++;
                vowels += strchr("AEIOUaeiou", c) != NULL;
            }
            else if (c >= '0' && c <= '9')
            {
                digits++;
            }
            else if (c != ',' && c != '.' && c != '-' && c != '\'' && c != '%')
            {
                other++;
            }
        }
        if (other == 0 && ((digits == 0 && letters >= 3 && vowels > 0) || (letters == 0 && digits >= 2)))
        {
            score++;
        }
    }
    return score;
}

/* Word @p k continues the word before it, which OCR split ("Sheri ff", "Di strict"): a gap
 * well under the page's usual space, a letter before and a lowercase letter after. */
static BOOL ro_split_word(const RoPage *pg, uint32_t k)
{
    const RoWord *a = &pg->w[k - 1], *b = &pg->w[k];
    size_t n = wcslen(a->t);
    return n > 0 && iswalpha(a->t[n - 1]) && iswlower(b->t[0]) && b->x0 - a->x1 < 0.55f * pg->space;
}

/* Turn an OCR result into words in page units and group them into rows (RoPage.ln). */
static BOOL ro_build_rows(RoCx *cx, const EeOcrText *t)
{
    RoPage *pg = &cx->pg;
    float f, *tmp;
    uint32_t i, n = 0;
    float slope = 0.0f;
    pg->nw = pg->nl = 0;
    if (t->width == 0 || t->nwords == 0)
    {
        return TRUE;
    }
    f = 1000.0f / (float)t->width;
    pg->height = (float)t->height * f;
    if (!ro_grow((void **)&pg->w, &pg->capw, t->nwords, sizeof(RoWord)))
    {
        return FALSE;
    }
    for (i = 0; i < t->nwords; i++)
    {
        const EeOcrWord *ow = &t->words[i];
        RoWord *w = &pg->w[n];
        int wn = MultiByteToWideChar(CP_UTF8, 0, t->text + ow->text_off, (int)ow->text_len, w->t,
                                     (int)ARRAYSIZE(w->t) - 1);
        if (wn <= 0)
        {
            continue;
        }
        w->t[wn] = L'\0';
        w->x0 = ow->x0 * f;
        w->x1 = ow->x1 * f;
        w->y0 = ow->y0 * f;
        w->y1 = ow->y1 * f;
        w->yc = (w->y0 + w->y1) * 0.5f;
        w->line = ow->line;
        n++;
    }
    pg->nw = n;
    if (n == 0)
    {
        return TRUE;
    }
    tmp = (float *)malloc((size_t)n * sizeof(float));
    if (tmp == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < n; i++)
    {
        tmp[i] = pg->w[i].y1 - pg->w[i].y0;
    }
    qsort(tmp, n, sizeof(float), ro_float_cmp);
    pg->h = tmp[n / 2] > 1.0f ? tmp[n / 2] : 1.0f;
    /* skew: the slope across each OCR line of several words, the median of them */
    {
        uint32_t ns = 0, a = 0;
        while (a < n)
        {
            uint32_t b = a;
            while (b + 1 < n && pg->w[b + 1].line == pg->w[a].line)
            {
                b++;
            }
            if (b > a)
            {
                float dx = (pg->w[b].x0 + pg->w[b].x1 - pg->w[a].x0 - pg->w[a].x1) * 0.5f;
                if (dx > 80.0f)
                {
                    tmp[ns++] = (pg->w[b].yc - pg->w[a].yc) / dx;
                }
            }
            a = b + 1;
        }
        if (ns >= 3)
        {
            qsort(tmp, ns, sizeof(float), ro_float_cmp);
            slope = tmp[ns / 2];
            if (slope > 0.05f || slope < -0.05f)
            {
                slope = 0.0f;
            }
        }
    }
    for (i = 0; i < n; i++)
    {
        pg->w[i].yc -= slope * ((pg->w[i].x0 + pg->w[i].x1) * 0.5f - 500.0f);
    }
    qsort(pg->w, n, sizeof(RoWord), ro_word_y_cmp);
    /* rows: words whose centres are within half a word height of the row's */
    {
        uint32_t a = 0;
        while (a < n)
        {
            uint32_t b = a + 1, k;
            float sum = pg->w[a].yc;
            RoLine *ln;
            while (b < n && pg->w[b].yc - sum / (float)(b - a) < 0.5f * pg->h)
            {
                sum += pg->w[b].yc;
                b++;
            }
            qsort(pg->w + a, b - a, sizeof(RoWord), ro_word_x_cmp);
            if (!ro_grow((void **)&pg->ln, &pg->capl, pg->nl + 1, sizeof(RoLine)))
            {
                free(tmp);
                return FALSE;
            }
            ln = &pg->ln[pg->nl++];
            ZeroMemory(ln, sizeof(*ln));
            ln->y = sum / (float)(b - a);
            ln->w0 = a;
            ln->nw = b - a;
            ln->x0 = pg->w[a].x0;
            ln->x1 = pg->w[b - 1].x1;
            for (k = a; k < b; k++)
            {
                if (pg->w[k].y1 - pg->w[k].y0 > ln->h)
                {
                    ln->h = pg->w[k].y1 - pg->w[k].y0;
                }
            }
            a = b;
        }
    }
    /* the usual gap between words, to join a word OCR split ("Sheri ff") */
    {
        uint32_t ng = 0, l2, k2;
        for (l2 = 0; l2 < pg->nl; l2++)
        {
            for (k2 = pg->ln[l2].w0 + 1; k2 < pg->ln[l2].w0 + pg->ln[l2].nw && ng < n; k2++)
            {
                float g = pg->w[k2].x0 - pg->w[k2 - 1].x1;
                if (g > 0.0f && g < 1.2f * pg->h && iswalpha(pg->w[k2].t[0]) && iswalpha(pg->w[k2 - 1].t[0]))
                {
                    tmp[ng++] = g;
                }
            }
        }
        pg->space = 0.5f * pg->h;
        if (ng >= 5)
        {
            qsort(tmp, ng, sizeof(float), ro_float_cmp);
            pg->space = tmp[ng / 2];
        }
        for (l2 = 0; l2 < pg->nl; l2++)
        {
            RoLine *ln = &pg->ln[l2];
            for (k2 = ln->w0; k2 < ln->w0 + ln->nw; k2++)
            {
                if (k2 > ln->w0 && !ro_split_word(pg, k2))
                {
                    StringCchCatW(ln->text, ARRAYSIZE(ln->text), L" ");
                }
                StringCchCatW(ln->text, ARRAYSIZE(ln->text), pg->w[k2].t);
            }
        }
    }
    /* row pitch */
    {
        uint32_t np = 0;
        for (i = 1; i < pg->nl; i++)
        {
            tmp[np++] = pg->ln[i].y - pg->ln[i - 1].y;
        }
        pg->pitch = 2.0f * pg->h;
        if (np >= 3)
        {
            qsort(tmp, np, sizeof(float), ro_float_cmp);
            pg->pitch = tmp[np / 2];
        }
    }
    free(tmp);
    return TRUE;
}

/* Words of a result that read as numbers. */
static uint32_t ro_numbers(const EeOcrText *t)
{
    uint32_t i, n = 0;
    for (i = 0; i < t->nwords; i++)
    {
        const char *s = t->text + t->words[i].text_off;
        uint32_t k, len = t->words[i].text_len, digits = 0;
        for (k = 0; k < len; k++)
        {
            digits += s[k] >= '0' && s[k] <= '9';
        }
        n += digits > 0 && digits * 2 >= len;
    }
    return n;
}

/* OCR page @p page into RoPage rows, turning it the way it reads best. */
static BOOL ro_ocr_page(RoCx *cx, EePdf *pdf, uint32_t page, EeOcr *ocr, EeOcrText *t, EePdfImage *img, int *turns,
                        uint32_t *typical)
{
    wchar_t oerr[200] = L"";
    uint32_t score, best_score;
    int best = *turns, k;
    double scale;
    BOOL ok;
    cx->pg.nw = cx->pg.nl = 0;
    if (!EePdf_GetPageImage(pdf, page, img))
    {
        cx->pages_failed++;
        return TRUE;
    }
    if (img->kind == EE_PDF_IMAGE_NONE || img->width < 200 || img->height < 200)
    {
        cx->pages_blank++;
        return TRUE;
    }
    /* about 2,200 pixels on the long side: 200 dpi for a letter page */
    {
        uint32_t lng = img->width > img->height ? img->width : img->height;
        scale = 2200.0 / (double)lng;
        if (scale > 1.6)
        {
            scale = 1.6;
        }
        if (scale > 0.9 && scale < 1.1)
        {
            scale = 1.0;
        }
    }
#define RO_RECOGNIZE()                                                                                                    \
    (img->kind == EE_PDF_IMAGE_ENCODED                                                                                    \
         ? EeOcr_RecognizeEncoded(ocr, img->data, img->len, scale, t, oerr, ARRAYSIZE(oerr))                             \
         : EeOcr_RecognizePixels(ocr, img->data, img->width, img->height, img->stride, (int)img->channels, scale, t, oerr, \
                                 ARRAYSIZE(oerr)))
    EeOcr_SetRotation(ocr, *turns);
    ok = RO_RECOGNIZE();
    if (!ok)
    {
        if (cx->pages_failed++ == 0)
        {
            StringCchCopyW(cx->ocr_err, ARRAYSIZE(cx->ocr_err), oerr);
        }
        return TRUE;
    }
    score = best_score = ro_score(t);
    /* a page that reads poorly may be turned another way (or nearly blank) */
    if (score < 30 || score * 3 < *typical)
    {
        static const int k_try[3] = {1, 3, 2};
        for (k = 0; k < 3; k++)
        {
            int tr = (*turns + k_try[k]) & 3;
            uint32_t s;
            EeOcr_SetRotation(ocr, tr);
            if (!RO_RECOGNIZE())
            {
                continue;
            }
            s = ro_score(t);
            if (s > best_score)
            {
                best_score = s;
                best = tr;
            }
        }
        if (best != *turns && best_score > score + 10)
        {
            *turns = best;
            cx->turns_changed++;
        }
        EeOcr_SetRotation(ocr, *turns);
        if (!RO_RECOGNIZE())
        {
            cx->pages_failed++;
            return TRUE;
        }
        best_score = ro_score(t);
    }
    if (best_score > *typical)
    {
        *typical = best_score;
    }
    cx->pages_read++;
    if (!ro_build_rows(cx, t))
    {
        return FALSE;
    }
    /* a faint scan: most rows with a name at the left have no numbers at the right. Read
     * enlarged, OCR finds them (keep the reading with more numbers). */
    {
        uint32_t l, named = 0, bare = 0, nums0 = ro_numbers(t), nums1;
        for (l = 0; l < cx->pg.nl; l++)
        {
            const RoLine *ln = &cx->pg.ln[l];
            if (ln->x0 < 300.0f)
            {
                uint32_t q, nums = 0;
                for (q = ln->w0; q < ln->w0 + ln->nw; q++)
                {
                    nums += cx->pg.w[q].x0 > 350.0f && iswdigit(cx->pg.w[q].t[0]);
                }
                named++;
                bare += nums < 2; /* (a row of counts has its total, its percentage and more) */
            }
        }
        if (named >= 8 && bare * 20 > named * 11 && scale < 2.0)
        {
            scale *= 1.5;
            if (RO_RECOGNIZE() && (nums1 = ro_numbers(t)) > nums0)
            {
                cx->pages_enlarged++;
                return ro_build_rows(cx, t);
            }
            scale /= 1.5;
            if (!RO_RECOGNIZE())
            {
                cx->pages_failed++;
                cx->pg.nw = cx->pg.nl = 0;
                return TRUE;
            }
            return ro_build_rows(cx, t);
        }
    }
#undef RO_RECOGNIZE
    return TRUE;
}

/* -------------------------------------------------------------------------- */
/* Headings and columns                                                       */
/* -------------------------------------------------------------------------- */

static BOOL ro_word_is(const wchar_t *w, const wchar_t *k)
{
    wchar_t key[48];
    ro_key(w, key, ARRAYSIZE(key));
    return wcscmp(key, k) == 0;
}

/* A word of a vote-type heading. */
static BOOL ro_vt_word(const wchar_t *w)
{
    static const wchar_t *const k[] = {L"EV",       L"ED",     L"MAIL",     L"MAILIN", L"PROV",  L"PROVISIONAL",
                                       L"EARLY",    L"VOTING", L"ELECTION", L"DAY",    L"ABSENTEE", L"ABS",
                                       L"INPERSON", L"PERSON", L"BY",       L"VOTE"};
    wchar_t key[48];
    size_t i;
    ro_key(w, key, ARRAYSIZE(key));
    for (i = 0; i < ARRAYSIZE(k); i++)
    {
        if (wcscmp(key, k[i]) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* A word that starts a vote type's name ("Early", "Absentee", "Election", "EV"). */
static BOOL ro_vt_lead(const wchar_t *w)
{
    static const wchar_t *const k[] = {L"EV",       L"ED",  L"EARLY",       L"MAIL", L"MAILIN", L"ABSENTEE",
                                       L"ABS",      L"ELECTION", L"PROV", L"PROVISIONAL", L"INPERSON"};
    wchar_t key[48];
    size_t i;
    ro_key(w, key, ARRAYSIZE(key));
    for (i = 0; i < ARRAYSIZE(k); i++)
    {
        if (wcscmp(key, k[i]) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* A name has a word that starts a vote type's name. */
static BOOL ro_vt_has_lead(const wchar_t *name)
{
    wchar_t w[48];
    while (*name != L'\0')
    {
        size_t n = 0;
        while (*name == L' ')
        {
            name++;
        }
        while (*name != L'\0' && *name != L' ' && n + 1 < ARRAYSIZE(w))
        {
            w[n++] = *name++;
        }
        w[n] = L'\0';
        if (n > 0 && ro_vt_lead(w))
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* A vote type's name as the file's other reports write it ("MAIL", "Early Voting"), from
 * the heading words OCR gave ("Mail", "Maii", "prov"). FALSE when it is none of the usual
 * names (then it is the heading as read). */
static BOOL ro_vt_name(const wchar_t *raw, wchar_t *out, size_t cch)
{
    static const wchar_t *const k[] = {L"EV",           L"ED",           L"MAIL",        L"PROV",
                                       L"Early Voting", L"Election Day", L"Provisional", L"Absentee",
                                       L"Mail-In",      L"In Person",    L"Mail Ballot"};
    wchar_t key[48], kk[48];
    size_t i, best = SIZE_MAX;
    double bs = 0.0;
    ro_key(raw, key, ARRAYSIZE(key));
    for (i = 0; i < ARRAYSIZE(k); i++)
    {
        int n, m, dd;
        double s;
        ro_key(k[i], kk, ARRAYSIZE(kk));
        n = (int)wcslen(key);
        m = (int)wcslen(kk);
        if (n == 0)
        {
            break;
        }
        dd = ro_lev(key, kk);
        s = 1.0 - (double)dd / (double)(n > m ? n : m);
        if (s > bs)
        {
            bs = s;
            best = i;
        }
    }
    if (best != SIZE_MAX && (bs >= 0.99 || (bs >= 0.7 && wcslen(key) >= 4)))
    {
        StringCchCopyW(out, cch, k[best]);
        return TRUE;
    }
    StringCchCopyW(out, cch, raw);
    if (wcslen(key) <= 5)
    {
        _wcsupr_s(out, cch);
    }
    return FALSE;
}

/* Read a row as a table heading: TOTAL (and "VOTES", or "VOTE %") right of the labels, then
 * vote-type names; or two or more vote-type words with no numbers. */
static BOOL ro_head_parse(RoCx *cx, const RoLine *ln, RoHead *hd)
{
    RoPage *pg = &cx->pg;
    uint32_t k, first = UINT32_MAX, nvtw = 0;
    BOOL total = FALSE;
    ZeroMemory(hd, sizeof(*hd));
    hd->tot = -1.0f;
    for (k = ln->w0; k < ln->w0 + ln->nw; k++)
    {
        const RoWord *w = &pg->w[k];
        int64_t v;
        double p;
        if (w->x0 < 200.0f)
        {
            continue;
        }
        if (first == UINT32_MAX)
        {
            first = k;
        }
        if (ro_word_is(w->t, L"TOTAL") || ro_word_is(w->t, L"TOTALS"))
        {
            total = TRUE;
        }
        else if (ro_vt_word(w->t))
        {
            nvtw++;
        }
        else if (ro_number(w->t, &v, &p) == RO_INT && v >= 10)
        {
            return FALSE;
        }
    }
    if (first == UINT32_MAX || (!total && nvtw < 2))
    {
        return FALSE;
    }
    for (k = first; k < ln->w0 + ln->nw; k++)
    {
        const RoWord *w = &pg->w[k];
        if (ro_word_is(w->t, L"TOTAL") || ro_word_is(w->t, L"TOTALS"))
        {
            hd->tot = w->x1;
            hd->tot_x0 = w->x0;
            if (k + 1 < ln->w0 + ln->nw && ro_word_is(pg->w[k + 1].t, L"VOTES"))
            {
                hd->tot = pg->w[++k].x1; /* "TOTAL VOTES", right-aligned on the column */
            }
            continue;
        }
        if (ro_word_is(w->t, L"VOTE") && k + 1 < ln->w0 + ln->nw &&
            (wcschr(pg->w[k + 1].t, L'%') != NULL || ro_starts(pg->w[k + 1].t, L"0/0")))
        {
            hd->vote_pct = TRUE;
            k++;
            continue;
        }
        if (wcschr(w->t, L'%') != NULL || ro_starts(w->t, L"0/0") || ro_word_is(w->t, L"VOTES") ||
            ro_word_is(w->t, L"PERCENT") || !iswalpha(w->t[0]))
        {
            if (ro_word_is(w->t, L"VOTE") || wcschr(w->t, L'%') != NULL)
            {
                hd->vote_pct = TRUE;
            }
            continue;
        }
        /* a vote-type name: words a little apart ("Early Voting"), up to the word that starts
         * the next one ("Absentee | Early Voting") */
        if (hd->nvt > 0 && w->x0 - hd->x1[hd->nvt - 1] < 1.2f * pg->h &&
            !(ro_vt_lead(w->t) && ro_vt_has_lead(hd->name[hd->nvt - 1])))
        {
            StringCchCatW(hd->name[hd->nvt - 1], ARRAYSIZE(hd->name[0]), L" ");
            StringCchCatW(hd->name[hd->nvt - 1], ARRAYSIZE(hd->name[0]), w->t);
            hd->x1[hd->nvt - 1] = w->x1;
        }
        else if (hd->nvt < RO_MAX_VT)
        {
            StringCchCopyW(hd->name[hd->nvt], ARRAYSIZE(hd->name[0]), w->t);
            hd->x0[hd->nvt] = w->x0;
            hd->x1[hd->nvt] = w->x1;
            hd->nvt++;
        }
    }
    return TRUE;
}

/* A heading line above or below the TOTAL line ("Early" over "Voting", or "Election" over
 * "Day" with nothing on the TOTAL line): words only; each joins the name it is over or under,
 * or starts one of its own. */
static BOOL ro_head_cont(RoCx *cx, RoHead *hd, const RoLine *ln, BOOL above)
{
    RoPage *pg = &cx->pg;
    uint32_t k;
    int at[RO_MAX_CELLS];
    if (ln->nw == 0 || ln->nw > RO_MAX_CELLS)
    {
        return FALSE;
    }
    for (k = 0; k < ln->nw; k++)
    {
        const RoWord *w = &pg->w[ln->w0 + k];
        float c = (w->x0 + w->x1) * 0.5f;
        int j;
        at[k] = -1;
        if (w->x0 < 200.0f || !iswalpha(w->t[0]))
        {
            return FALSE;
        }
        for (j = 0; j < hd->nvt; j++)
        {
            if (c >= hd->x0[j] - pg->h && c <= hd->x1[j] + pg->h)
            {
                at[k] = j;
            }
        }
        if (at[k] < 0)
        {
            if (!ro_vt_word(w->t) || hd->nvt >= RO_MAX_VT || (hd->tot >= 0.0f && w->x1 < hd->tot + pg->h))
            {
                return FALSE;
            }
            /* a name of its own (its other words are on the other line) */
            j = hd->nvt++;
            hd->name[j][0] = L'\0';
            hd->x0[j] = w->x0;
            hd->x1[j] = w->x1;
            at[k] = j;
            continue;
        }
    }
    for (k = 0; k < ln->nw; k++)
    {
        wchar_t *nm = hd->name[at[k]];
        const wchar_t *wt = pg->w[ln->w0 + k].t;
        if (nm[0] == L'\0')
        {
            StringCchCopyW(nm, ARRAYSIZE(hd->name[0]), wt);
        }
        else if (above)
        {
            wchar_t tmp[40];
            StringCchPrintfW(tmp, ARRAYSIZE(tmp), L"%s %s", wt, nm);
            StringCchCopyW(nm, ARRAYSIZE(hd->name[0]), tmp);
        }
        else
        {
            StringCchCatW(nm, ARRAYSIZE(hd->name[0]), L" ");
            StringCchCatW(nm, ARRAYSIZE(hd->name[0]), wt);
        }
        if (pg->w[ln->w0 + k].x1 > hd->x1[at[k]])
        {
            hd->x1[at[k]] = pg->w[ln->w0 + k].x1;
        }
    }
    {
        int i, j;
        for (i = 1; i < hd->nvt; i++)
        {
            for (j = i; j > 0 && hd->x1[j] < hd->x1[j - 1]; j--)
            {
                float t0 = hd->x0[j], t1 = hd->x1[j];
                wchar_t tn[40];
                StringCchCopyW(tn, ARRAYSIZE(tn), hd->name[j]);
                hd->x0[j] = hd->x0[j - 1];
                hd->x1[j] = hd->x1[j - 1];
                StringCchCopyW(hd->name[j], ARRAYSIZE(hd->name[0]), hd->name[j - 1]);
                hd->x0[j - 1] = t0;
                hd->x1[j - 1] = t1;
                StringCchCopyW(hd->name[j - 1], ARRAYSIZE(hd->name[0]), tn);
            }
        }
    }
    return TRUE;
}

/* Sort the vote-type columns by position. */
static void ro_cols_sort(RoCols *c)
{
    int i, j;
    for (i = 1; i < c->nvt; i++)
    {
        for (j = i; j > 0 && c->vt[j] < c->vt[j - 1]; j--)
        {
            float tx = c->vt[j];
            uint32_t ti = c->vti[j];
            c->vt[j] = c->vt[j - 1];
            c->vti[j] = c->vti[j - 1];
            c->vt[j - 1] = tx;
            c->vti[j - 1] = ti;
        }
    }
}

/* The columns a heading sets, completed from the layout remembered for it (OCR leaves out
 * heading words: "TOTAL VOTES  ED  prov" for "TOTAL VOTES  EV  MAIL  ED  PROV"). */
static void ro_cols_from_head(RoCx *cx, const RoHead *hd)
{
    RoCols c;
    float tol = 3.0f * cx->pg.h;
    int i, j, t, best = -1, best_score = 0;
    uint32_t vti[RO_MAX_VT];
    wchar_t nmv[RO_MAX_VT][40];
    BOOL known[RO_MAX_VT];
    for (i = 0; i < hd->nvt; i++)
    {
        wchar_t *nm = nmv[i];
        uint32_t q;
        known[i] = ro_vt_name(hd->name[i], nm, ARRAYSIZE(nmv[0]));
        /* a column found from its numbers and given a guessed name, now named by its heading */
        for (t = 0; t < cx->ntmpl && hd->tot >= 0.0f; t++)
        {
            RoTmpl *tp = &cx->tmpl[t];
            for (j = 0; j < tp->nvt; j++)
            {
                uint32_t g = tp->vti[j];
                if (tp->guessed[j] && known[i] && fabsf(hd->tot + tp->rel[j] - hd->x1[i]) <= tol &&
                    g < cx->d->nvt && _wcsicmp(cx->d->vote_types[g], nm) != 0)
                {
                    BOOL taken = FALSE;
                    for (q = 0; q < cx->d->nvt; q++)
                    {
                        taken = taken || _wcsicmp(cx->d->vote_types[q], nm) == 0;
                    }
                    if (!taken)
                    {
                        wchar_t *nn = ro_wdup(nm);
                        if (nn != NULL)
                        {
                            free(cx->d->vote_types[g]);
                            cx->d->vote_types[g] = nn;
                        }
                    }
                    tp->guessed[j] = FALSE;
                }
            }
        }
        vti[i] = UINT32_MAX; /* (added once it is known not to be a remembered column) */
        for (q = 0; q < cx->d->nvt; q++)
        {
            if (_wcsicmp(cx->d->vote_types[q], nm) == 0)
            {
                vti[i] = q;
            }
        }
    }
    /* the remembered layout these columns fit: by place (and name) when TOTAL was read, else
     * by the names */
    for (t = 0; t < cx->ntmpl; t++)
    {
        const RoTmpl *tp = &cx->tmpl[t];
        int score = 0, clash = 0;
        float off = 0.0f;
        int noff = 0;
        if (hd->tot >= 0.0f)
        {
            off = hd->tot;
            noff = 1;
        }
        else
        {
            for (i = 0; i < hd->nvt; i++)
            {
                for (j = 0; j < tp->nvt; j++)
                {
                    if (tp->vti[j] == vti[i])
                    {
                        off += hd->x1[i] - tp->rel[j];
                        noff++;
                    }
                }
            }
            if (noff == 0)
            {
                continue;
            }
        }
        off /= (float)noff;
        for (i = 0; i < hd->nvt; i++)
        {
            for (j = 0; j < tp->nvt; j++)
            {
                BOOL near_ = fabsf(off + tp->rel[j] - hd->x1[i]) <= tol;
                if (tp->vti[j] == vti[i])
                {
                    if (near_)
                    {
                        score += 2;
                    }
                    else
                    {
                        clash++;
                    }
                }
                else if (near_ && hd->tot >= 0.0f)
                {
                    score++; /* the same column read under another name ("Mail" for "Mail-In") */
                }
            }
        }
        if (tp->vote_pct == hd->vote_pct)
        {
            score++;
        }
        if (clash == 0 && score > best_score && (score >= 2 || hd->nvt == 0))
        {
            best = t;
            best_score = score;
        }
    }
    /* a name read at a remembered column's place is that column (its first name stays) */
    if (best >= 0 && hd->tot >= 0.0f)
    {
        const RoTmpl *tp = &cx->tmpl[best];
        for (i = 0; i < hd->nvt; i++)
        {
            for (j = 0; j < tp->nvt; j++)
            {
                if (fabsf(hd->tot + tp->rel[j] - hd->x1[i]) <= tol && tp->vti[j] != vti[i])
                {
                    BOOL used = FALSE;
                    int q;
                    for (q = 0; q < hd->nvt; q++)
                    {
                        used = used || vti[q] == tp->vti[j];
                    }
                    if (!used)
                    {
                        vti[i] = tp->vti[j];
                    }
                }
            }
        }
    }
    for (i = 0; i < hd->nvt; i++)
    {
        wchar_t kk[48];
        ro_key(nmv[i], kk, ARRAYSIZE(kk));
        if (vti[i] == UINT32_MAX && (known[i] || wcslen(kk) > 3 || cx->d->nvt == 0))
        {
            vti[i] = ro_vt(cx, nmv[i]);
        }
        /* else a misread heading ("MAN", "X") at no known column: left out (its numbers find
         * their column, ro_discover) */
    }
    ZeroMemory(&c, sizeof(c));
    c.pct = -1.0f;
    c.tmpl = best;
    if (hd->tot >= 0.0f)
    {
        c.tot = hd->tot;
    }
    else if (best >= 0)
    {
        const RoTmpl *tp = &cx->tmpl[best];
        float off = 0.0f;
        int noff = 0;
        for (i = 0; i < hd->nvt; i++)
        {
            for (j = 0; j < tp->nvt; j++)
            {
                if (tp->vti[j] == vti[i])
                {
                    off += hd->x1[i] - tp->rel[j];
                    noff++;
                }
            }
        }
        c.tot = noff > 0 ? off / (float)noff : cx->cols.tot;
    }
    else if (cx->cols.valid)
    {
        c.tot = cx->cols.tot;
    }
    else
    {
        return; /* vote types with no TOTAL and nothing remembered */
    }
    for (i = 0; i < hd->nvt && c.nvt < RO_MAX_VT; i++)
    {
        BOOL dup = vti[i] == UINT32_MAX;
        for (j = 0; j < c.nvt; j++)
        {
            dup = dup || c.vti[j] == vti[i];
        }
        if (!dup && hd->x1[i] > c.tot + cx->pg.h)
        {
            c.vt[c.nvt] = hd->x1[i];
            c.vti[c.nvt++] = vti[i];
        }
    }
    if (best >= 0)
    {
        RoTmpl *tp = &cx->tmpl[best];
        for (j = 0; j < tp->nvt && c.nvt < RO_MAX_VT; j++)
        {
            BOOL have = FALSE;
            for (i = 0; i < c.nvt; i++)
            {
                have = have || c.vti[i] == tp->vti[j] || fabsf(c.vt[i] - (c.tot + tp->rel[j])) < tol;
            }
            if (!have)
            {
                c.vt[c.nvt] = c.tot + tp->rel[j];
                c.vti[c.nvt++] = tp->vti[j];
            }
        }
        if (tp->pct_rel > -999.0f)
        {
            c.pct = c.tot + tp->pct_rel;
        }
    }
    ro_cols_sort(&c);
    /* remember the layout: new, or with the columns it lacked */
    if (best < 0 && cx->ntmpl < RO_MAX_TMPL && hd->tot >= 0.0f && (cx->learning || c.nvt >= 2))
    {
        RoTmpl *tp = &cx->tmpl[cx->ntmpl];
        ZeroMemory(tp, sizeof(*tp));
        tp->vote_pct = hd->vote_pct;
        tp->pct_rel = -1000.0f;
        for (i = 0; i < c.nvt; i++)
        {
            tp->rel[i] = c.vt[i] - c.tot;
            tp->vti[i] = c.vti[i];
        }
        tp->nvt = c.nvt;
        c.tmpl = cx->ntmpl++;
    }
    else if (best >= 0 && cx->learning)
    {
        RoTmpl *tp = &cx->tmpl[best];
        for (i = 0; i < c.nvt && tp->nvt < RO_MAX_VT; i++)
        {
            BOOL have = FALSE;
            for (j = 0; j < tp->nvt; j++)
            {
                have = have || tp->vti[j] == c.vti[i];
            }
            if (!have)
            {
                tp->rel[tp->nvt] = c.vt[i] - c.tot;
                tp->vti[tp->nvt++] = c.vti[i];
            }
        }
    }
    c.valid = TRUE;
    cx->cols = c;
    if (hd->tot >= 0.0f)
    {
        cx->label_lim = hd->tot_x0 - 1.5f * cx->pg.h;
    }
    else if (cx->label_lim < 0.0f)
    {
        cx->label_lim = c.tot - 8.0f * cx->pg.h;
    }
}

/* Words right of the labels as cells (pieces of a number joined: "39 , 497", "50 . 48",
 * "31 080", "49 i"), the rest as the label. */
static void ro_cells(RoCx *cx, RoLine *ln)
{
    RoPage *pg = &cx->pg;
    float lim = cx->label_lim >= 0.0f ? cx->label_lim : 330.0f;
    uint32_t k = ln->w0, end = ln->w0 + ln->nw, lastw = UINT32_MAX - 1;
    BOOL label_done = FALSE;
    ln->nc = 0;
    ln->label[0] = L'\0';
    ln->lab_x0 = -1.0f;
    while (k < end)
    {
        const RoWord *w = &pg->w[k];
        wchar_t tok[64];
        float x0 = w->x0, x1 = w->x1;
        uint32_t j = k + 1;
        int kind;
        int64_t v = 0;
        double p = 0.0;
        StringCchCopyW(tok, ARRAYSIZE(tok), w->t);
        if (x0 >= lim && ro_numish(tok))
        {
            while (j < end && ro_numish(pg->w[j].t))
            {
                float gap = pg->w[j].x0 - x1;
                const wchar_t *nt = pg->w[j].t;
                size_t tl = wcslen(tok);
                BOOL punct = tok[tl - 1] == L',' || tok[tl - 1] == L'.' || nt[0] == L',' || nt[0] == L'.';
                if (!(gap < 0.6f * pg->h || (punct && gap < 1.2f * pg->h)))
                {
                    break;
                }
                StringCchCatW(tok, ARRAYSIZE(tok), nt);
                x1 = pg->w[j].x1;
                j++;
            }
            kind = ro_number(tok, &v, &p);
            if (kind != 0 && ln->nc < RO_MAX_CELLS)
            {
                RoCell *c = &ln->c[ln->nc++];
                c->x0 = x0;
                c->x1 = x1;
                c->kind = kind;
                c->v = v;
                c->pct = p;
                label_done = TRUE;
                k = j;
                continue;
            }
            if (kind == 0 && iswdigit(tok[0]))
            {
                k = j; /* a number OCR garbled, past the labels: not part of the name */
                continue;
            }
        }
        if (!label_done)
        {
            BOOL leader = TRUE;
            const wchar_t *s;
            for (s = w->t; *s != L'\0'; s++)
            {
                leader = leader && (*s == L'.' || *s == L'\x2022' || *s == L'-' || *s == L'_' || *s == L'\x00b7' ||
                                    *s == L',' || *s == L':');
            }
            if (!leader)
            {
                if (ln->label[0] == L'\0')
                {
                    ln->lab_x0 = w->x0;
                }
                else if (!(k == lastw + 1 && ro_split_word(pg, k)))
                {
                    StringCchCatW(ln->label, ARRAYSIZE(ln->label), L" ");
                }
                lastw = k;
                StringCchCatW(ln->label, ARRAYSIZE(ln->label), w->t);
            }
        }
        k = j;
    }
    /* trailing dot leaders and stray marks ("TOTAL.", "John Cornyn (REP) .") */
    {
        size_t n = wcslen(ln->label);
        while (n > 0 && (ln->label[n - 1] == L'.' || ln->label[n - 1] == L' ' || ln->label[n - 1] == L'\x2022' ||
                         ln->label[n - 1] == L',' || ln->label[n - 1] == L':' || ln->label[n - 1] == L'-'))
        {
            ln->label[--n] = L'\0';
        }
    }
}

/* The column of an integer cell: 0 = TOTAL, 1 + k = vote type k (document index), -1 =
 * none, -2 = the percentage column. */
static int ro_col_of(const RoCx *cx, const RoCell *c, float shift)
{
    const RoCols *cl = &cx->cols;
    float best = fabsf(c->x1 - (cl->tot + shift)), minsp = 1e9f, tol;
    int bi = 0, k;
    float prev = cl->tot;
    for (k = 0; k < cl->nvt; k++)
    {
        float d = fabsf(c->x1 - (cl->vt[k] + shift));
        if (cl->vt[k] - prev < minsp)
        {
            minsp = cl->vt[k] - prev;
        }
        prev = cl->vt[k];
        if (d < best)
        {
            best = d;
            bi = 1 + (int)cl->vti[k];
        }
    }
    if (cl->pct >= 0.0f && fabsf(c->x1 - (cl->pct + shift)) < best)
    {
        return -2;
    }
    tol = 0.45f * minsp;
    if (tol > 4.0f * cx->pg.h)
    {
        tol = 4.0f * cx->pg.h;
    }
    return best <= tol ? bi : -1;
}

/* The family of a vote type's name: 0 early voting, 1 mail, 2 election day, 3 provisional,
 * -1 other. */
static int ro_vt_family(const wchar_t *name)
{
    wchar_t k[48];
    ro_key(name, k, ARRAYSIZE(k));
    if (wcscmp(k, L"EV") == 0 || ro_starts(k, L"EARLY"))
    {
        return 0;
    }
    if (ro_starts(k, L"MAIL") || ro_starts(k, L"ABS"))
    {
        return 1;
    }
    if (wcscmp(k, L"ED") == 0 || ro_starts(k, L"ELECTION"))
    {
        return 2;
    }
    if (ro_starts(k, L"PROV"))
    {
        return 3;
    }
    return -1;
}

/* A name for a column of numbers whose heading OCR never read: a vote type the file names
 * in another table and this one lacks, else the usual one this table lacks (early voting,
 * mail, election day, provisional), in the style of the names it has. */
static uint32_t ro_guess_vt(RoCx *cx, const RoCols *c)
{
    static const wchar_t *const k_short[4] = {L"EV", L"MAIL", L"ED", L"PROV"};
    static const wchar_t *const k_long[4] = {L"Early Voting", L"Mail", L"Election Day", L"Provisional"};
    EeRpdfDoc *d = cx->d;
    BOOL fam[4] = {FALSE, FALSE, FALSE, FALSE}, shorts = TRUE;
    uint32_t i;
    int k;
    for (i = 0; i < d->nvt; i++)
    {
        BOOL here = FALSE;
        for (k = 0; k < c->nvt; k++)
        {
            here = here || c->vti[k] == i;
        }
        if (!here)
        {
            return i;
        }
    }
    for (k = 0; k < c->nvt; k++)
    {
        int f = ro_vt_family(d->vote_types[c->vti[k]]);
        if (f >= 0)
        {
            fam[f] = TRUE;
        }
        shorts = shorts && wcslen(d->vote_types[c->vti[k]]) <= 5;
    }
    for (k = 0; k < 4; k++)
    {
        if (!fam[k])
        {
            return ro_vt(cx, shorts ? k_short[k] : k_long[k]);
        }
    }
    {
        wchar_t nm[32];
        StringCchPrintfW(nm, ARRAYSIZE(nm), L"Column %u", d->nvt + 1);
        return ro_vt(cx, nm);
    }
}

/* A vote type the file names that these columns lack. */
static BOOL ro_vt_missing(const RoCx *cx, const RoCols *c)
{
    uint32_t i;
    int k;
    for (i = 0; i < cx->d->nvt; i++)
    {
        BOOL here = FALSE;
        for (k = 0; k < c->nvt; k++)
        {
            here = here || c->vti[k] == i;
        }
        if (!here)
        {
            return TRUE;
        }
    }
    return FALSE;
}

static void ro_cells(RoCx *cx, RoLine *ln);
static int ro_col_of(const RoCx *cx, const RoCell *c, float shift);
static uint32_t ro_vote_for(const wchar_t *key, const wchar_t *text);
static void ro_bal_finish(RoCx *cx, const int64_t *s);
static BOOL ro_block_has(const RoBlock *b, int kind);

/* Columns of numbers under a heading that no heading word placed (OCR left out "EV" in
 * "TOTAL VOTES EV MAIL ED PROV"): two or more numbers lined up on the right. */
static void ro_discover(RoCx *cx, uint32_t from, uint32_t to)
{
    RoPage *pg = &cx->pg;
    float xs[256], px[256];
    int n = 0, a = 0, npx = 0;
    uint32_t m;
    if (!cx->cols.valid)
    {
        return;
    }
    for (m = from; m < to && m < pg->nl; m++)
    {
        RoLine *ln = &pg->ln[m];
        int q;
        if (ln->kind != RL_TEXT)
        {
            continue;
        }
        ro_cells(cx, ln);
        for (q = 0; q < ln->nc && n < (int)ARRAYSIZE(xs); q++)
        {
            if (ln->c[q].kind == RO_INT && ln->c[q].x1 > cx->cols.tot + 2.0f * pg->h &&
                ro_col_of(cx, &ln->c[q], 0.0f) == -1)
            {
                xs[n++] = ln->c[q].x1;
            }
            else if (ln->c[q].kind == RO_PCT && npx < (int)ARRAYSIZE(px))
            {
                px[npx++] = ln->c[q].x1;
            }
        }
    }
    /* each vote-type column where its numbers end (a heading is centred, or read in part:
     * "Mail" of "Mail-In" ends well left of its numbers) */
    {
        int k;
        for (k = 0; k < cx->cols.nvt; k++)
        {
            float near_[128];
            int nn = 0;
            for (m = from; m < to && m < pg->nl; m++)
            {
                const RoLine *ln = &pg->ln[m];
                int q;
                for (q = 0; ln->kind == RL_TEXT && q < ln->nc && nn < (int)ARRAYSIZE(near_); q++)
                {
                    if (ln->c[q].kind == RO_INT && fabsf(ln->c[q].x1 - cx->cols.vt[k]) < 3.0f * pg->h)
                    {
                        near_[nn++] = ln->c[q].x1;
                    }
                }
            }
            if (nn >= 3)
            {
                float med;
                qsort(near_, (size_t)nn, sizeof(float), ro_float_cmp);
                med = near_[nn / 2];
                if (fabsf(med - cx->cols.vt[k]) > 0.5f * pg->h)
                {
                    int q;
                    cx->cols.vt[k] = med;
                    if (cx->cols.tmpl >= 0 && cx->learning)
                    {
                        RoTmpl *tp = &cx->tmpl[cx->cols.tmpl];
                        for (q = 0; q < tp->nvt; q++)
                        {
                            if (tp->vti[q] == cx->cols.vti[k])
                            {
                                tp->rel[q] = med - cx->cols.tot;
                            }
                        }
                    }
                }
            }
        }
        /* (the cells were taken before the columns moved) */
        n = 0;
        for (m = from; m < to && m < pg->nl; m++)
        {
            const RoLine *ln = &pg->ln[m];
            int q;
            for (q = 0; ln->kind == RL_TEXT && q < ln->nc && n < (int)ARRAYSIZE(xs); q++)
            {
                if (ln->c[q].kind == RO_INT && ln->c[q].x1 > cx->cols.tot + 2.0f * pg->h &&
                    ro_col_of(cx, &ln->c[q], 0.0f) == -1)
                {
                    xs[n++] = ln->c[q].x1;
                }
            }
        }
    }
    qsort(xs, (size_t)n, sizeof(float), ro_float_cmp);
    qsort(px, (size_t)npx, sizeof(float), ro_float_cmp);
    while (a < n)
    {
        int b = a + 1;
        while (b < n && xs[b] - xs[b - 1] < 1.2f * pg->h)
        {
            b++;
        }
        float x = xs[a + (b - a) / 2];
        BOOL at_pct = npx > 0 && fabsf(x - px[npx / 2]) < 3.0f * pg->h; /* pieces of percentages */
        if ((b - a >= 2 || (b - a == 1 && ro_vt_missing(cx, &cx->cols))) && cx->cols.nvt < RO_MAX_VT && !at_pct)
        {
            uint32_t vt;
            if (!cx->learning && (!ro_vt_missing(cx, &cx->cols) || b - a < 3))
            {
                a = b; /* (a stray column on one page names no new vote type) */
                continue;
            }
            vt = ro_guess_vt(cx, &cx->cols);
            cx->cols.vt[cx->cols.nvt] = x;
            cx->cols.vti[cx->cols.nvt++] = vt;
            ro_cols_sort(&cx->cols);
            if (cx->cols.tmpl >= 0 && cx->learning)
            {
                RoTmpl *tp = &cx->tmpl[cx->cols.tmpl];
                if (tp->nvt < RO_MAX_VT)
                {
                    tp->rel[tp->nvt] = x - cx->cols.tot;
                    tp->guessed[tp->nvt] = TRUE;
                    tp->vti[tp->nvt++] = vt;
                }
            }
        }
        a = b;
    }
}

/* -------------------------------------------------------------------------- */
/* Checking a contest's counts                                                */
/* -------------------------------------------------------------------------- */

static void ro_review(RoCx *cx, const wchar_t *what)
{
    cx->rows_review++;
    if (cx->nreview_ex < RO_REVIEW_EX)
    {
        wchar_t *e = cx->review_ex[cx->nreview_ex++];
        if (cx->pre != UINT32_MAX)
        {
            StringCchPrintfW(e, ARRAYSIZE(cx->review_ex[0]), L"%s (%s)", what, cx->d->precincts[cx->pre].name);
        }
        else
        {
            StringCchCopyW(e, ARRAYSIZE(cx->review_ex[0]), what);
        }
    }
}

/* A column check of a contest: the target row's count is the sum of the member rows'. */
typedef struct RoCons
{
    int target;
    BOOL no_fill; /* only checks a count, fills none (the precinct's ballots, ro_block_close) */
    BOOL member[RO_MAX_ROWS];
} RoCons;

/* Fill and correct a contest's counts: each row's total is the sum of its vote types; each
 * column of the choices sums to the Total row ("Total Votes Cast"), and with the over and
 * under votes to the ballots row ("Contest Totals"). A count OCR left out is filled from them,
 * the rest are zeros it dropped; a misread count is corrected when one change makes both its
 * row and its columns add up, or when the row's percentage tells the total. */
static void ro_repair(RoCx *cx, RoBlock *b, int nvt, const int *vcols)
{
    RoCons cons[2];
    int ncons = 0, r, c, k, pass, T = -1, B = -1;
    BOOL changed = TRUE;
    BOOL fixed[RO_MAX_ROWS];
    unsigned char filled[RO_MAX_ROWS][1 + RO_MAX_VT]; /* found by arithmetic, not read */
    int64_t res[2][1 + RO_MAX_VT];
    memset(filled, 0, sizeof(filled));
    for (r = 0; r < b->nrows; r++)
    {
        fixed[r] = FALSE;
        if (b->rows[r].kind == RR_TOTAL)
        {
            T = r;
        }
        else if (b->rows[r].kind == RR_BALLOTS)
        {
            B = r;
        }
    }
    if (T >= 0)
    {
        RoCons *k0 = &cons[ncons++];
        k0->target = T;
        k0->no_fill = FALSE;
        for (r = 0; r < b->nrows; r++)
        {
            k0->member[r] = b->rows[r].kind == RR_CHOICE;
        }
    }
    if (B >= 0 && (ro_block_has(b, RR_OVER) || ro_block_has(b, RR_UNDER)))
    {
        /* (Midland's reports print Contest Totals with no over and under votes: no check) */
        RoCons *k1 = &cons[ncons++];
        k1->target = B;
        k1->no_fill = b->rows[B].synthetic;
        for (r = 0; r < b->nrows; r++)
        {
            k1->member[r] = b->rows[r].kind == RR_CHOICE || b->rows[r].kind == RR_OVER || b->rows[r].kind == RR_UNDER;
        }
    }
    for (pass = 0; pass < 6 && changed; pass++)
    {
        changed = FALSE;
        for (r = 0; r < b->nrows && nvt > 0; r++)
        {
            RoRow *w = &b->rows[r];
            int unk = 0, at = -1;
            int64_t sum = 0;
            for (c = 0; c < nvt; c++)
            {
                if (w->has[vcols[c]])
                {
                    sum += w->v[vcols[c]];
                }
                else
                {
                    unk++;
                    at = vcols[c];
                }
            }
            if (w->has[0] && unk == 1 && w->v[0] - sum >= 0)
            {
                w->v[at] = w->v[0] - sum;
                w->has[at] = TRUE;
                filled[r][at] = 1;
                fixed[r] = fixed[r] || w->v[at] != 0;
                changed = TRUE;
            }
            else if (w->has[0] && unk > 1 && w->v[0] == sum)
            {
                for (c = 0; c < nvt; c++)
                {
                    if (!w->has[vcols[c]])
                    {
                        w->v[vcols[c]] = 0; /* the row adds up already: the rest are zeros */
                        w->has[vcols[c]] = TRUE;
                    }
                }
                changed = TRUE;
            }
            else if (!w->has[0] && unk == 0)
            {
                w->v[0] = sum;
                w->has[0] = TRUE;
                filled[r][0] = 1;
                changed = TRUE;
            }
        }
        for (k = 0; k < ncons; k++)
        {
            const RoCons *cn = &cons[k];
            if (cn->no_fill)
            {
                continue;
            }
            for (c = -1; c < nvt; c++)
            {
                int col = c < 0 ? 0 : vcols[c], unk = 0, at = -1;
                int64_t sum = 0;
                for (r = 0; r < b->nrows; r++)
                {
                    if (!cn->member[r] && r != cn->target)
                    {
                        continue;
                    }
                    if (!b->rows[r].has[col])
                    {
                        unk++;
                        at = r;
                    }
                    else if (r != cn->target)
                    {
                        sum += b->rows[r].v[col];
                    }
                }
                if (unk != 1)
                {
                    continue;
                }
                if (at == cn->target)
                {
                    b->rows[at].v[col] = sum;
                    b->rows[at].has[col] = TRUE;
                    filled[at][col] = 1;
                    changed = TRUE;
                }
                else if (b->rows[cn->target].v[col] - sum >= 0)
                {
                    b->rows[at].v[col] = b->rows[cn->target].v[col] - sum;
                    b->rows[at].has[col] = TRUE;
                    filled[at][col] = 1;
                    fixed[at] = fixed[at] || b->rows[at].v[col] != 0;
                    changed = TRUE;
                }
            }
        }
    }
    /* what is still missing was a zero (OCR drops a lone "0") */
    for (r = 0; r < b->nrows; r++)
    {
        for (c = -1; c < nvt; c++)
        {
            int col = c < 0 ? 0 : vcols[c];
            if (!b->rows[r].has[col])
            {
                b->rows[r].v[col] = 0;
                b->rows[r].has[col] = TRUE;
            }
        }
    }
    /* each check's residual per column: the target row less its members */
    for (k = 0; k < ncons; k++)
    {
        for (c = -1; c < nvt; c++)
        {
            int col = c < 0 ? 0 : vcols[c];
            res[k][col] = b->rows[cons[k].target].v[col];
            for (r = 0; r < b->nrows; r++)
            {
                if (cons[k].member[r])
                {
                    res[k][col] -= b->rows[r].v[col];
                }
            }
        }
    }
    for (r = 0; r < b->nrows; r++)
    {
        RoRow *w = &b->rows[r];
        int64_t sum = 0, dr;
        BOOL ok = FALSE, in_any = FALSE;
        if (nvt == 0)
        {
            continue;
        }
        cx->rows++;
        for (c = 0; c < nvt; c++)
        {
            sum += w->v[vcols[c]];
        }
        dr = w->v[0] - sum;
        if (dr == 0)
        {
            if (fixed[r])
            {
                cx->rows_fixed++;
            }
            continue;
        }
        for (k = 0; k < ncons; k++)
        {
            in_any = in_any || cons[k].member[r];
        }
        /* one misread count that throws its columns off by as much: every check the row is
         * in agrees (and at least one) */
        for (c = -1; c < nvt && in_any && !ok; c++)
        {
            int col = c < 0 ? 0 : vcols[c];
            int64_t delta = c < 0 ? -dr : dr; /* the change that makes the row add up */
            BOOL all = TRUE;
            for (k = 0; k < ncons; k++)
            {
                if (cons[k].member[r] && res[k][col] != delta)
                {
                    all = FALSE;
                }
            }
            if (all && w->v[col] + delta >= 0)
            {
                w->v[col] += delta;
                for (k = 0; k < ncons; k++)
                {
                    if (cons[k].member[r])
                    {
                        res[k][col] -= delta;
                    }
                }
                ok = TRUE;
            }
        }
        /* a check's own target row (Total, Contest Totals): the count its members agree on */
        for (k = 0; k < ncons && !ok; k++)
        {
            if (cons[k].target != r)
            {
                continue;
            }
            if (res[k][0] == dr)
            {
                /* its total is off by as much as the row: the members' sum is the total */
                w->v[0] -= dr;
                res[k][0] = 0;
                ok = TRUE;
            }
            else
            {
                for (c = 0; c < nvt && !ok; c++)
                {
                    int col = vcols[c];
                    if (res[k][col] == -dr)
                    {
                        w->v[col] -= res[k][col];
                        res[k][col] = 0;
                        ok = TRUE;
                    }
                }
            }
        }
        /* the percentage gives the total ("868 56.44%" of 652 is 368) */
        if (!ok && w->has_pct && w->kind == RR_CHOICE)
        {
            int64_t all = 0;
            if (T >= 0)
            {
                all = b->rows[T].v[0];
            }
            else if (B >= 0)
            {
                all = b->rows[B].v[0];
                for (k = 0; k < b->nrows; k++)
                {
                    if (b->rows[k].kind == RR_OVER || b->rows[k].kind == RR_UNDER)
                    {
                        all -= b->rows[k].v[0];
                    }
                }
            }
            if (all > 0)
            {
                double expect = w->pct * (double)all / 100.0;
                double tol = (double)all * 0.00006 + 0.6;
                if (fabs((double)sum - expect) <= tol)
                {
                    for (k = 0; k < ncons; k++)
                    {
                        if (cons[k].member[r])
                        {
                            res[k][0] += w->v[0] - sum;
                        }
                    }
                    w->v[0] = sum;
                    ok = TRUE;
                }
            }
        }
        if (ok)
        {
            cx->rows_fixed++;
        }
        else
        {
            wchar_t what[200];
            StringCchPrintfW(what, ARRAYSIZE(what), L"%s, %s: %lld is not the sum of its vote types (%lld)", b->title,
                             w->label, (long long)w->v[0], (long long)sum);
            ro_review(cx, what);
        }
    }
    /* a count filled from its row while another count of the row was misread: the columns are
     * off by +d and -d ("28,364  676  [21,819]  5,869" for 21,919 and 5,769) */
    for (r = 0; r < b->nrows && ncons > 0 && nvt > 1; r++)
    {
        int c1, c2;
        for (c1 = 0; c1 < nvt; c1++)
        {
            for (c2 = 0; c2 < nvt; c2++)
            {
                int a1 = vcols[c1], a2 = vcols[c2];
                int64_t dd;
                BOOL all = TRUE, inany = FALSE;
                if (c1 == c2 || !(filled[r][a1] || filled[r][a2]))
                {
                    continue;
                }
                dd = -1;
                for (k = 0; k < ncons; k++)
                {
                    if (!cons[k].member[r])
                    {
                        continue;
                    }
                    if (!inany)
                    {
                        dd = res[k][a1];
                    }
                    inany = TRUE;
                    all = all && res[k][a1] == dd && res[k][a2] == -dd;
                }
                if (inany && all && dd > 0 && b->rows[r].v[a2] - dd >= 0)
                {
                    b->rows[r].v[a1] += dd;
                    b->rows[r].v[a2] -= dd;
                    for (k = 0; k < ncons; k++)
                    {
                        if (cons[k].member[r])
                        {
                            res[k][a1] = 0;
                            res[k][a2] = 0;
                        }
                    }
                    cx->rows_fixed++;
                }
            }
        }
    }
    for (k = 0; k < ncons; k++)
    {
        for (c = -1; c < nvt; c++)
        {
            int col = c < 0 ? 0 : vcols[c];
            if (res[k][col] != 0)
            {
                wchar_t what[200];
                StringCchPrintfW(what, ARRAYSIZE(what), L"%s: the choices' counts differ from its %s row by %lld",
                                 b->title, b->rows[cons[k].target].label, (long long)res[k][col]);
                ro_review(cx, what);
                k = ncons;
                break;
            }
        }
    }
}

/* A choice's party: Electionware's leading code ("REP Donald J. Trump"), or the older
 * report's trailing one ("John Cornyn (REP)"). */
static void ro_party(const wchar_t *label, wchar_t *party, size_t cch)
{
    static const wchar_t *const k[] = {L"REP", L"DEM", L"LIB", L"GRE", L"GRN", L"IND",
                                       L"NON", L"NPA", L"CON", L"REF", L"SOC"};
    size_t i, n = wcslen(label);
    party[0] = L'\0';
    for (i = 0; i < ARRAYSIZE(k); i++)
    {
        if (wcsncmp(label, k[i], 3) == 0 && label[3] == L' ')
        {
            StringCchCopyW(party, cch, k[i]);
            return;
        }
        if (n >= 6 && label[n - 1] == L')' && label[n - 5] == L'(' && wcsncmp(label + n - 4, k[i], 3) == 0)
        {
            StringCchCopyW(party, cch, k[i]);
            return;
        }
    }
}

/* Check and store the contest read into the block. */
static void ro_block_close(RoCx *cx)
{
    RoBlock *b = &cx->blk;
    EeRpdfDoc *d = cx->d;
    int vcols[RO_MAX_VT];
    int nvt = 0, r, k;
    uint32_t ci, at = 0;
    unsigned char taken[256];
    if (!b->open)
    {
        return;
    }
    b->open = FALSE;
    if (b->nrows == 0 || b->title[0] == L'\0')
    {
        return;
    }
    for (k = 0; k < RO_MAX_VT; k++)
    {
        for (r = 0; r < b->nrows; r++)
        {
            if (b->rows[r].has[1 + k])
            {
                vcols[nvt++] = 1 + k;
                break;
            }
        }
    }
    {
        uint64_t s_rows = cx->rows, s_fixed = cx->rows_fixed, s_rev = cx->rows_review;
        uint32_t s_nex = cx->nreview_ex;
        RoBlock *orig = (RoBlock *)malloc(sizeof(RoBlock));
        if (orig != NULL)
        {
            *orig = *b;
        }
        ro_repair(cx, b, nvt, vcols);
        if (cx->rows_review > s_rev && orig != NULL && b->vote_for == 1 && nvt > 0 && !cx->bal.pending &&
            !ro_block_has(orig, RR_BALLOTS) && orig->nrows < RO_MAX_ROWS)
        {
            /* counts still off: try again with the ballots cast by vote type as the contest's
             * ballots row (choices + over + under), kept only if then everything adds up (it does
             * not hold for a contest on some of a precinct's ballots) */
            RoBlock *alt = (RoBlock *)malloc(sizeof(RoBlock));
            RoRow *br;
            BOOL any = FALSE;
            if (alt != NULL)
            {
                *alt = *orig;
                br = &alt->rows[alt->nrows];
                ZeroMemory(br, sizeof(*br));
                br->kind = RR_BALLOTS;
                br->synthetic = TRUE;
                StringCchCopyW(br->label, ARRAYSIZE(br->label), L"Ballots Cast");
                if (cx->pre != UINT32_MAX)
                {
                    size_t q = d->nturnout;
                    while (q > 0 && d->turnout[q - 1].pre == cx->pre)
                    {
                        q--;
                        if (d->turnout[q].vt < RO_MAX_VT)
                        {
                            br->v[1 + d->turnout[q].vt] = d->turnout[q].votes;
                            br->has[1 + d->turnout[q].vt] = TRUE;
                            any = TRUE;
                        }
                    }
                    if (d->precincts[cx->pre].ballots != EE_RPDF_UNKNOWN)
                    {
                        br->v[0] = d->precincts[cx->pre].ballots;
                        br->has[0] = TRUE;
                    }
                }
                else
                {
                    for (k = 0; k < RO_MAX_VT; k++)
                    {
                        if (cx->bvt[k] != EE_RPDF_UNKNOWN)
                        {
                            br->v[1 + k] = cx->bvt[k];
                            br->has[1 + k] = TRUE;
                            any = TRUE;
                        }
                    }
                    if (d->ballots_total != EE_RPDF_UNKNOWN)
                    {
                        br->v[0] = d->ballots_total;
                        br->has[0] = TRUE;
                    }
                }
                if (any && br->has[0])
                {
                    uint64_t a_rows = cx->rows, a_fixed = cx->rows_fixed, a_rev = cx->rows_review;
                    uint32_t a_nex = cx->nreview_ex;
                    wchar_t keep_ex[RO_REVIEW_EX][200];
                    memcpy(keep_ex, cx->review_ex, sizeof(keep_ex));
                    alt->nrows++;
                    cx->rows = s_rows;
                    cx->rows_fixed = s_fixed;
                    cx->rows_review = s_rev;
                    cx->nreview_ex = s_nex;
                    ro_repair(cx, alt, nvt, vcols);
                    if (cx->rows_review == s_rev)
                    {
                        alt->nrows--; /* (the ballots row is not the contest's own) */
                        cx->rows--;
                        *b = *alt;
                    }
                    else
                    {
                        cx->rows = a_rows;
                        cx->rows_fixed = a_fixed;
                        cx->rows_review = a_rev;
                        cx->nreview_ex = a_nex;
                        memcpy(cx->review_ex, keep_ex, sizeof(keep_ex));
                    }
                }
                free(alt);
            }
        }
        free(orig);
    }
    if (cx->bal.pending && b->vote_for == 1 && nvt > 0)
    {
        int64_t s[RO_MAX_VT];
        BOOL ou = FALSE;
        for (k = 0; k < RO_MAX_VT; k++)
        {
            s[k] = 0;
        }
        for (r = 0; r < b->nrows; r++)
        {
            if (b->rows[r].kind == RR_CHOICE || b->rows[r].kind == RR_OVER || b->rows[r].kind == RR_UNDER)
            {
                ou = ou || b->rows[r].kind == RR_UNDER;
                for (k = 0; k < RO_MAX_VT; k++)
                {
                    s[k] += b->rows[r].v[1 + k];
                }
            }
        }
        if (ou)
        {
            ro_bal_finish(cx, s);
        }
    }
    ci = ro_contest(cx, b->title, b->vote_for);
    if (cx->oom)
    {
        return;
    }
    ZeroMemory(taken, sizeof(taken));
    for (r = 0; r < b->nrows && !cx->oom; r++)
    {
        RoRow *w = &b->rows[r];
        EeRpdfContest *c = &d->contests[ci];
        if (w->kind == RR_TOTAL)
        {
            continue;
        }
        if (w->kind == RR_BALLOTS)
        {
            if (cx->pre == UINT32_MAX)
            {
                c->ballots = w->v[0];
            }
            continue;
        }
        if (w->kind == RR_OVER || w->kind == RR_UNDER)
        {
            c->has_over_under = TRUE;
            if (cx->pre != UINT32_MAX)
            {
                ro_count(cx, FALSE, ci, w->kind == RR_OVER ? EE_RPDF_OVER : EE_RPDF_UNDER, EE_RPDF_NO_VT, w->v[0]);
            }
            else if (w->kind == RR_OVER)
            {
                c->over = w->v[0];
            }
            else
            {
                c->under = w->v[0];
            }
            continue;
        }
        {
            wchar_t party[8];
            uint32_t ch;
            ro_party(w->label, party, ARRAYSIZE(party));
            ch = ro_choice(cx, ci, w->label, party, taken, ARRAYSIZE(taken), at++);
            if (cx->oom)
            {
                return;
            }
            if (ch < ARRAYSIZE(taken))
            {
                taken[ch] = 1;
            }
            c = &d->contests[ci];
            if (cx->pre == UINT32_MAX)
            {
                c->choices[ch].total = w->v[0];
                cx->summary_data = TRUE;
            }
            else if (nvt > 0)
            {
                for (k = 0; k < nvt; k++)
                {
                    ro_count(cx, FALSE, ci, ch, (uint32_t)(vcols[k] - 1), w->v[vcols[k]]);
                }
            }
            else
            {
                ro_count(cx, FALSE, ci, ch, EE_RPDF_NO_VT, w->v[0]);
            }
        }
    }
}

/* -------------------------------------------------------------------------- */
/* Reading a page                                                             */
/* -------------------------------------------------------------------------- */

/* "November 3, 2020" from a date OCR read ("NOVEMBER 3. 2020"), else FALSE. */
static BOOL ro_date(const wchar_t *s, wchar_t *out, size_t cch)
{
    static const wchar_t *const k[] = {L"January", L"February", L"March",     L"April",   L"May",      L"June",
                                       L"July",    L"August",   L"September", L"October", L"November", L"December"};
    size_t i;
    for (i = 0; i < ARRAYSIZE(k); i++)
    {
        size_t n = wcslen(k[i]);
        const wchar_t *p = s;
        int day = 0, year = 0, nd = 0;
        if (_wcsnicmp(p, k[i], n) != 0 || p[n] != L' ')
        {
            continue;
        }
        p += n + 1;
        while (iswdigit(*p) && nd < 2)
        {
            day = day * 10 + (*p++ - L'0');
            nd++;
            if (nd == 1 && p[0] == L' ' && iswdigit(p[1]) && !iswdigit(p[2]))
            {
                p++; /* "June 1 5, 2024" */
            }
        }
        if (nd == 0 || day < 1 || day > 31)
        {
            return FALSE;
        }
        while (*p == L',' || *p == L'.' || *p == L' ')
        {
            p++;
        }
        for (nd = 0; iswdigit(*p) && nd < 4; nd++)
        {
            year = year * 10 + (*p++ - L'0');
        }
        if (nd != 4 || year < 1990 || year > 2099)
        {
            return FALSE;
        }
        StringCchPrintfW(out, cch, L"%s %d, %d", k[i], day, year);
        return TRUE;
    }
    return FALSE;
}

/* The segments of a row: its words in runs with small gaps. */
static int ro_segments(RoCx *cx, const RoLine *ln, wchar_t seg[][160], float *sx0, int max)
{
    RoPage *pg = &cx->pg;
    int ns = 0;
    uint32_t k;
    for (k = ln->w0; k < ln->w0 + ln->nw; k++)
    {
        const RoWord *w = &pg->w[k];
        if (ns > 0 && w->x0 - pg->w[k - 1].x1 < 2.5f * pg->h)
        {
            StringCchCatW(seg[ns - 1], 160, L" ");
            StringCchCatW(seg[ns - 1], 160, w->t);
        }
        else if (ns < max)
        {
            StringCchCopyW(seg[ns], 160, w->t);
            sx0[ns] = w->x0;
            ns++;
        }
    }
    return ns;
}

/* A precinct line's name: "0051 Precinct 51" (the older report), "Precinct 1" (Electionware,
 * whose large type OCR may cut: "ncinct 1"), else FALSE. */
static BOOL ro_precinct_name(const wchar_t *seg, wchar_t *out, size_t cch)
{
    const wchar_t *p = seg;
    size_t nd = 0;
    while (iswdigit(p[nd]))
    {
        nd++;
    }
    if (nd >= 3 && nd <= 5 && p[nd] == L' ' && iswalpha(p[nd + 1]) && _wcsnicmp(p + nd + 1, L"Page", 4) != 0)
    {
        StringCchCopyW(out, cch, seg);
        return TRUE;
    }
    {
        const wchar_t *c = NULL, *s;
        for (s = seg; *s != L'\0'; s++)
        {
            if (_wcsnicmp(s, L"cinct ", 6) == 0)
            {
                c = s;
                break;
            }
        }
        if (c != NULL && c[6] != L'\0' && _wcsnicmp(c + 6, L"Results", 7) != 0 && _wcsnicmp(c + 6, L"Summary", 7) != 0 &&
            _wcsnicmp(c + 6, L"Report", 6) != 0 && _wcsnicmp(c + 6, L"by", 2) != 0)
        {
            StringCchPrintfW(out, cch, L"Precinct %s", c + 6);
            return TRUE;
        }
    }
    return FALSE;
}

/* The page's heading lines: the report kind, the precinct and (on the first page) the
 * election's name, date and county. Returns the precinct line's name in @p pre (or ""). */
static void ro_header(RoCx *cx, uint32_t n, wchar_t *pre, size_t pcch)
{
    RoPage *pg = &cx->pg;
    uint32_t l;
    wchar_t election[160] = L"";
    pre[0] = L'\0';
    cx->page_kind = PK_UNKNOWN;
    for (l = 0; l < n; l++)
    {
        RoLine *ln = &pg->ln[l];
        wchar_t seg[8][160], up[RO_TEXT_CCH];
        float sx0[8];
        int ns = ro_segments(cx, ln, seg, sx0, 8), s;
        StringCchCopyW(up, ARRAYSIZE(up), ln->text);
        _wcsupr_s(up, ARRAYSIZE(up));
        if (wcsstr(up, L"REPORT-GROUP") != NULL || wcsstr(up, L"REPT-GROUP") != NULL || wcsstr(up, L"EL30") != NULL)
        {
            cx->erm = TRUE;
        }
        if (wcsstr(up, L"SUMMRY") != NULL || wcsstr(up, L"SUMMARY REPT") != NULL ||
            ((wcsstr(up, L"SUMMARY REPORT") != NULL || wcsstr(up, L"SUMMARY RESULTS") != NULL) &&
             wcsstr(up, L"PRECIN") == NULL && wcsstr(up, L"PCT") == NULL) ||
            wcsstr(up, L"CUMULATIVE") != NULL)
        {
            cx->page_kind = PK_SUMMARY;
        }
        else if (wcsstr(up, L"PREC REPORT") != NULL || wcsstr(up, L"PCTS REPORT") != NULL ||
                 wcsstr(up, L"PRECINCT REPORT") != NULL || wcsstr(up, L"PCT BY PCT") != NULL ||
                 wcsstr(up, L"PCT-BY-PCT") != NULL || wcsstr(up, L"PRECINCT BY PRECINCT") != NULL ||
                 (wcsstr(up, L"PRECINCT") != NULL && (wcsstr(up, L"REPORT") != NULL || wcsstr(up, L"RESULTS") != NULL)))
        {
            cx->page_kind = PK_PRECINCT;
        }
        for (s = 0; s < ns; s++)
        {
            wchar_t d8[64], us[160];
            StringCchCopyW(us, ARRAYSIZE(us), seg[s]);
            _wcsupr_s(us, ARRAYSIZE(us));
            if (pre[0] == L'\0' && l > 0 && ro_precinct_name(seg[s], pre, pcch))
            {
                if (!cx->erm)
                {
                    cx->pre_y = ln->y;
                }
                continue;
            }
            if (cx->head_done)
            {
                continue;
            }
            if (ro_date(seg[s], d8, ARRAYSIZE(d8)))
            {
                if (cx->d->election_date == NULL)
                {
                    cx->d->election_date = ro_wdup(d8);
                }
            }
            else if (ro_starts(us, L"RUN DATE"))
            {
                const wchar_t *c = wcschr(seg[s], L':');
                if (c != NULL && cx->d->timestamp == NULL)
                {
                    while (*++c == L' ')
                    {
                    }
                    cx->d->timestamp = ro_wdup(c);
                }
            }
            else if ((wcsstr(us, L"COUNTY") != NULL || ro_ends(us, L" TX") || ro_ends(us, L" TEXAS")) &&
                     wcsstr(us, L"ELECTION") == NULL)
            {
                if (cx->d->region == NULL)
                {
                    wchar_t rg[160];
                    wchar_t *dot;
                    StringCchCopyW(rg, ARRAYSIZE(rg), seg[s]);
                    if ((dot = wcsstr(rg, L". TX")) != NULL)
                    {
                        *dot = L',';
                    }
                    cx->d->region = ro_wdup(rg);
                }
            }
            else if (wcsstr(us, L"REPORT") != NULL || wcsstr(us, L"OFFICIAL") != NULL || wcsstr(us, L"RESULTS") != NULL ||
                     wcsstr(us, L"PAGE") != NULL || wcsstr(us, L"REPT") != NULL)
            {
                continue;
            }
            else if (election[0] == L'\0' || (wcsstr(election, L"lection") == NULL && wcsstr(us, L"ELECTION") != NULL))
            {
                StringCchCopyW(election, ARRAYSIZE(election), seg[s]);
            }
        }
    }
    if (!cx->head_done && cx->d->election_date != NULL)
    {
        if (election[0] != L'\0' && cx->d->election_name == NULL)
        {
            cx->d->election_name = ro_wdup(election);
        }
        cx->head_done = TRUE;
    }
    /* Electionware: a precinct line OCR could not read at the place it was before */
    if (pre[0] == L'\0' && !cx->erm && cx->pre_y >= 0.0f && cx->page_kind != PK_SUMMARY)
    {
        for (l = 0; l < n; l++)
        {
            RoLine *ln = &pg->ln[l];
            if (fabsf(ln->y - cx->pre_y) < 0.6f * pg->pitch && ln->x0 < 200.0f && ln->text[0] != L'\0' &&
                wcsstr(ln->text, L"Report") == NULL)
            {
                StringCchCopyW(pre, pcch, ln->text);
                break;
            }
        }
    }
    /* Electionware: the heading's last line, alone at the left under the date, names the
     * precinct ("1, 59, 83": Cameron's precincts reported together) */
    if (pre[0] == L'\0' && !cx->erm && cx->page_kind == PK_PRECINCT && n >= 3)
    {
        const RoLine *ln = &pg->ln[n - 1];
        wchar_t uk[RO_TEXT_CCH], tmp[64];
        BOOL after_date = FALSE;
        ro_key(ln->text, uk, ARRAYSIZE(uk));
        for (l = 0; l + 1 < n; l++)
        {
            wchar_t seg[8][160];
            float sx0[8];
            int ns = ro_segments(cx, &pg->ln[l], seg, sx0, 8), q;
            for (q = 0; q < ns; q++)
            {
                after_date = after_date || ro_date(seg[q], tmp, ARRAYSIZE(tmp));
            }
        }
        if (after_date && ln->x0 < 200.0f && ln->x1 < 600.0f && uk[0] != L'\0' && !ro_date(ln->text, tmp, ARRAYSIZE(tmp)) &&
            wcsstr(uk, L"COUNTY") == NULL && wcsstr(uk, L"REPORT") == NULL && wcsstr(uk, L"RESULTS") == NULL &&
            wcsstr(uk, L"ELECTION") == NULL && wcsstr(uk, L"OFFICIAL") == NULL)
        {
            const wchar_t *s = ln->text;
            size_t o = 0;
            for (; *s != L'\0' && o + 1 < pcch; s++)
            {
                if (!(*s == L' ' && o > 0 && pre[o - 1] == L','))
                {
                    pre[o++] = *s; /* "1, 59, 83" as Cameron's text reports write it: "1,59,83" */
                }
            }
            pre[o] = L'\0';
            cx->pre_y = ln->y;
        }
    }
}

/* Store the ballots row read (RoCx.bal): with @p s, a vote-for-1 contest's choices, over and
 * under votes by vote type (index in vote_types), which fill the counts OCR left out when they
 * agree with the rest; without, what is missing is taken as zero. Keeps the row waiting when
 * @p s does not agree (another contest may). */
static void ro_bal_finish(RoCx *cx, const int64_t *s)
{
    RoBal *bl = &cx->bal;
    EeRpdfDoc *d = cx->d;
    BOOL in_pre = cx->pre != UINT32_MAX;
    int64_t sum = 0;
    int k;
    if (!bl->pending)
    {
        return;
    }
    if (s != NULL)
    {
        int64_t ssum = 0;
        BOOL agree = TRUE;
        for (k = 0; k < bl->ncols; k++)
        {
            ssum += s[bl->vti[k]];
            agree = agree && (!bl->has[k] || s[bl->vti[k]] == bl->vals[k]);
        }
        if (!agree || ssum != bl->tot)
        {
            if (++bl->tries < 3)
            {
                return;
            }
        }
        else
        {
            for (k = 0; k < bl->ncols; k++)
            {
                if (!bl->has[k])
                {
                    bl->vals[k] = s[bl->vti[k]];
                    bl->has[k] = TRUE;
                }
            }
            cx->rows_fixed++;
        }
    }
    bl->pending = FALSE;
    for (k = 0; k < bl->ncols; k++)
    {
        sum += bl->has[k] ? bl->vals[k] : 0;
    }
    if (bl->ncols > 0)
    {
        if (bl->tot == EE_RPDF_UNKNOWN)
        {
            bl->tot = sum;
        }
        else if (bl->tot != sum)
        {
            ro_review(cx, L"Ballots Cast: the total is not the sum of its vote types");
        }
    }
    if (in_pre)
    {
        size_t keep = d->nturnout;
        while (keep > cx->turnout_mark && d->turnout[keep - 1].pre == cx->pre)
        {
            keep--; /* a "- Total" row replaces a party row read before it */
        }
        d->nturnout = keep;
        if (bl->tot != EE_RPDF_UNKNOWN)
        {
            d->precincts[cx->pre].ballots = cx->bal_base != EE_RPDF_UNKNOWN ? cx->bal_base + bl->tot : bl->tot;
        }
    }
    else
    {
        d->ballots_total = bl->tot;
        cx->summary_data = TRUE;
    }
    for (k = 0; k < bl->ncols; k++)
    {
        int64_t v = bl->has[k] ? bl->vals[k] : 0;
        if (in_pre)
        {
            ro_count(cx, TRUE, 0, 0, bl->vti[k], v);
        }
        else
        {
            cx->bvt[bl->vti[k]] = v;
        }
    }
}

/* Statistics: registered voters and ballots cast (by vote type). */
static void ro_stats_row(RoCx *cx, const wchar_t *key, RoLine *ln)
{
    EeRpdfDoc *d = cx->d;
    BOOL in_pre = cx->pre != UINT32_MAX;
    BOOL is_total = wcsstr(key + 11, L"TOTAL") != NULL;
    int64_t tot = EE_RPDF_UNKNOWN, vals[RO_MAX_VT];
    BOOL has[RO_MAX_VT];
    int k;
    for (k = 0; k < RO_MAX_VT; k++)
    {
        has[k] = FALSE;
    }
    for (k = 0; k < ln->nc; k++)
    {
        int col;
        if (ln->c[k].kind != RO_INT)
        {
            continue;
        }
        col = ro_col_of(cx, &ln->c[k], 0.0f);
        if (col == 0 && tot == EE_RPDF_UNKNOWN)
        {
            tot = ln->c[k].v;
        }
        else if (col >= 1 && !has[col - 1])
        {
            vals[col - 1] = ln->c[k].v;
            has[col - 1] = TRUE;
        }
    }
    if (ro_starts(key, L"REGISTEREDVOTERS"))
    {
        if (tot == EE_RPDF_UNKNOWN)
        {
            for (k = 0; k < ln->nc && tot == EE_RPDF_UNKNOWN; k++)
            {
                if (ln->c[k].kind == RO_INT)
                {
                    tot = ln->c[k].v;
                }
            }
        }
        if (tot != EE_RPDF_UNKNOWN && (is_total || !cx->reg_total_seen))
        {
            if (in_pre)
            {
                /* (a precinct printed in parts: county-only, city, ... adds up) */
                d->precincts[cx->pre].registered = cx->reg_base != EE_RPDF_UNKNOWN ? cx->reg_base + tot : tot;
            }
            else
            {
                d->registered_total = tot;
                cx->summary_data = TRUE;
            }
            cx->reg_total_seen = cx->reg_total_seen || is_total;
        }
        return;
    }
    if (!ro_starts(key, L"BALLOTSCAST") || wcsstr(key, L"BLANK") != NULL || !(is_total || !cx->bal_total_seen))
    {
        return;
    }
    /* the row's total is the sum of its vote types; a missing one is a dropped zero, unless
     * two or more are missing and the rest fall short: the first contest settles those */
    {
        int64_t sum = 0;
        int unk = 0, at = -1;
        for (k = 0; k < cx->cols.nvt; k++)
        {
            uint32_t vt = cx->cols.vti[k];
            if (has[vt])
            {
                sum += vals[vt];
            }
            else
            {
                unk++;
                at = (int)vt;
            }
        }
        if (tot != EE_RPDF_UNKNOWN && unk == 1 && tot - sum >= 0)
        {
            vals[at] = tot - sum;
            has[at] = TRUE;
        }
        else if (tot != EE_RPDF_UNKNOWN && unk >= 2 && sum < tot)
        {
            RoBal *bl = &cx->bal;
            ro_bal_finish(cx, NULL);
            bl->pending = TRUE;
            bl->tot = tot;
            bl->tries = 0;
            bl->ncols = cx->cols.nvt;
            for (k = 0; k < cx->cols.nvt; k++)
            {
                uint32_t vt = cx->cols.vti[k];
                bl->vti[k] = vt;
                bl->has[k] = has[vt];
                bl->vals[k] = has[vt] ? vals[vt] : 0;
            }
            cx->bal_total_seen = cx->bal_total_seen || is_total;
            return;
        }
    }
    {
        RoBal *bl = &cx->bal;
        ro_bal_finish(cx, NULL);
        bl->tot = tot;
        bl->ncols = cx->cols.nvt;
        for (k = 0; k < cx->cols.nvt; k++)
        {
            uint32_t vt = cx->cols.vti[k];
            bl->vti[k] = vt;
            bl->has[k] = has[vt];
            bl->vals[k] = has[vt] ? vals[vt] : 0;
        }
        bl->pending = TRUE;
        ro_bal_finish(cx, NULL);
    }
    cx->bal_total_seen = cx->bal_total_seen || is_total;
}

/* A "Vote For" line: "Vote For 1", "(VOTE FOR) 1", "Vote For I". Returns the number, or 0. */
static uint32_t ro_vote_for(const wchar_t *key, const wchar_t *text)
{
    const wchar_t *p;
    uint32_t v = 0;
    /* (OCR may leave out a word: "For 1", "(VOTE 1") */
    if (!ro_starts(key, L"VOTEFOR") && !(ro_starts(key, L"FOR") && wcslen(key) <= 5 && wcslen(key) >= 4) &&
        !(ro_starts(key, L"VOTE") && wcslen(key) <= 6 && wcslen(key) >= 5 && iswdigit(key[4])))
    {
        return 0;
    }
    p = text + wcslen(text);
    while (p > text && (p[-1] == L' ' || p[-1] == L'.'))
    {
        p--;
    }
    while (p > text && p[-1] != L' ' && p[-1] != L')')
    {
        p--;
    }
    for (; *p != L'\0' && *p != L' ' && *p != L'.'; p++)
    {
        wchar_t c = ro_digit_of(*p);
        if (!iswdigit(c))
        {
            return 0;
        }
        v = v * 10 + (uint32_t)(c - L'0');
    }
    return v > 0 && v < 100 ? v : 0;
}

static BOOL ro_block_has(const RoBlock *b, int kind)
{
    int r;
    for (r = 0; r < b->nrows; r++)
    {
        if (b->rows[r].kind == kind)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* The page has a statistics block (registered voters) before its first contest. */
static BOOL ro_page_has_stats(RoCx *cx, uint32_t from)
{
    uint32_t l;
    for (l = from; l < cx->pg.nl; l++)
    {
        wchar_t key[RO_TEXT_CCH];
        ro_key(cx->pg.ln[l].text, key, ARRAYSIZE(key));
        if (wcsstr(key, L"REDVOTERS") != NULL || ro_starts(key, L"STATISTICS"))
        {
            return TRUE;
        }
        if (ro_vote_for(key, cx->pg.ln[l].text) != 0)
        {
            return FALSE;
        }
    }
    return FALSE;
}

/* The page after an unnamed precinct's first page names it (the page goes on with its
 * contests): rename it, or fold it into the precinct of that name read before. */
static void ro_name_placeholder(RoCx *cx, const wchar_t *name)
{
    EeRpdfDoc *d = cx->d;
    uint32_t ph = cx->pre, i, target = UINT32_MAX;
    wchar_t key[48], pk[48];
    size_t q;
    ro_pre_key(name, key, ARRAYSIZE(key));
    for (i = 0; i < d->nprecincts; i++)
    {
        if (i == ph)
        {
            continue;
        }
        if (key[0] != L'\0')
        {
            ro_pre_key(d->precincts[i].name, pk, ARRAYSIZE(pk));
            if (wcscmp(pk, key) == 0)
            {
                target = i;
                break;
            }
        }
        else if (ro_var_similar(&cx->pvar[i], name) >= 0.8)
        {
            target = i;
            break;
        }
    }
    cx->pre_placeholder = FALSE;
    cx->pre_unread--;
    if (target == UINT32_MAX)
    {
        wchar_t *nn = ro_wdup(name);
        if (nn == NULL)
        {
            cx->oom = TRUE;
            return;
        }
        free(d->precincts[ph].name);
        d->precincts[ph].name = nn;
        ro_var_free(&cx->pvar[ph]);
        ro_var_add(cx, &cx->pvar[ph], name);
        return;
    }
    /* fold: its counts go to the precinct read before, as another part of it */
    for (q = 0; q < d->ncounts; q++)
    {
        if (d->counts[q].pre == ph)
        {
            d->counts[q].pre = target;
        }
    }
    for (q = 0; q < d->nturnout; q++)
    {
        if (d->turnout[q].pre == ph)
        {
            d->turnout[q].pre = target;
        }
    }
    if (d->precincts[ph].registered != EE_RPDF_UNKNOWN)
    {
        d->precincts[target].registered = d->precincts[target].registered != EE_RPDF_UNKNOWN
                                              ? d->precincts[target].registered + d->precincts[ph].registered
                                              : d->precincts[ph].registered;
    }
    if (d->precincts[ph].ballots != EE_RPDF_UNKNOWN)
    {
        d->precincts[target].ballots = d->precincts[target].ballots != EE_RPDF_UNKNOWN
                                           ? d->precincts[target].ballots + d->precincts[ph].ballots
                                           : d->precincts[ph].ballots;
    }
    cx->reg_base = d->precincts[target].registered;
    cx->bal_base = d->precincts[target].ballots;
    if (ph + 1 == d->nprecincts)
    {
        free(d->precincts[ph].name);
        ro_var_free(&cx->pvar[ph]);
        d->nprecincts--;
    }
    cx->pre = target;
    ro_var_add(cx, &cx->pvar[target], name);
}

/* A comma OCR read as a point in a title ("District Clerk. Unexpired Term"): a point after a
 * word of four letters or more, before a capital. */
static void ro_title_commas(wchar_t *t)
{
    size_t i, n = wcslen(t);
    for (i = 4; i + 2 < n; i++)
    {
        if (t[i] == L'.' && t[i + 1] == L' ' && iswupper(t[i + 2]) && iswalpha(t[i - 1]) && iswalpha(t[i - 2]) &&
            iswalpha(t[i - 3]) && iswalpha(t[i - 4]))
        {
            t[i] = L',';
        }
    }
}

static void ro_new_precinct(RoCx *cx, const wchar_t *name)
{
    uint32_t p = ro_precinct(cx, name);
    if (p != cx->pre)
    {
        ro_block_close(cx); /* (a contest that goes on over a page stays open) */
        ro_bal_finish(cx, NULL);
        cx->pre = p;
        if (cx->used != NULL)
        {
            memset(cx->used, 0, cx->cap_used);
        }
        cx->last_contest = UINT32_MAX;
        cx->reg_total_seen = cx->bal_total_seen = FALSE;
        cx->reg_row_seen = FALSE;
        cx->pre_placeholder = FALSE;
        cx->turnout_mark = cx->d->nturnout;
        cx->reg_base = cx->bal_base = EE_RPDF_UNKNOWN;
        cx->section = RS_NONE;
        cx->npend = 0;
    }
}

/* Mark the page's headings and footer; returns the first anchor line (a heading, statistics
 * or "Vote For"), or the line count. */
static uint32_t ro_mark(RoCx *cx)
{
    RoPage *pg = &cx->pg;
    uint32_t l, anchor = pg->nl;
    RoHead hd;
    for (l = 0; l < pg->nl; l++)
    {
        RoLine *ln = &pg->ln[l];
        wchar_t key[RO_TEXT_CCH];
        if (ln->kind == RL_HEADCONT)
        {
            continue;
        }
        ro_key(ln->text, key, ARRAYSIZE(key));
        if (ro_starts(key, L"PAGE") && wcsstr(key, L"OF") != NULL && l > pg->nl / 2)
        {
            uint32_t m;
            for (m = l; m < pg->nl; m++)
            {
                pg->ln[m].kind = RL_FOOTER;
            }
            break;
        }
        if (ro_head_parse(cx, ln, &hd))
        {
            ln->kind = RL_HEAD;
            if (anchor == pg->nl)
            {
                anchor = l;
            }
        }
        else if (anchor == pg->nl && (ro_starts(key, L"REGISTEREDVOTERS") || ro_starts(key, L"BALLOTSCAST") ||
                                      ro_starts(key, L"STATISTICS") || ro_vote_for(key, ln->text) != 0))
        {
            anchor = l;
        }
        if ((wcsstr(ln->text, L"Page ") != NULL && wcsstr(ln->text, L" of ") != NULL && ln->y > pg->height * 0.9f) ||
            ln->y > pg->height * 0.94f || wcsstr(ln->text, L"Electionware") != NULL)
        {
            ln->kind = RL_FOOTER;
            if (cx->d->timestamp == NULL && ln->y > pg->height * 0.85f)
            {
                /* "Election Summary - 11/14/2024 5:04 PM" */
                const wchar_t *sl = wcschr(ln->text, L'/');
                const wchar_t *c = sl != NULL ? wcschr(sl, L':') : NULL;
                if (c != NULL && sl - ln->text >= 2 && iswdigit(sl[-1]))
                {
                    const wchar_t *b = sl - 1;
                    wchar_t ts[40];
                    size_t n = 0;
                    while (b > ln->text && iswdigit(b[-1]))
                    {
                        b--;
                    }
                    while (b[n] != L'\0' && n + 1 < ARRAYSIZE(ts) && (iswdigit(b[n]) || wcschr(L"/: ", b[n]) != NULL))
                    {
                        n++;
                    }
                    if ((b[n] == L'A' || b[n] == L'P' || b[n] == L'a' || b[n] == L'p') && (b[n + 1] == L'M' || b[n + 1] == L'm'))
                    {
                        n += 2;
                    }
                    while (n > 0 && b[n - 1] == L' ')
                    {
                        n--;
                    }
                    StringCchCopyNW(ts, ARRAYSIZE(ts), b, n);
                    if (n >= 10)
                    {
                        cx->d->timestamp = ro_wdup(ts);
                    }
                }
            }
        }
    }
    return anchor;
}

/* A table heading line and the second line under it, if any: the columns. */
static void ro_take_head(RoCx *cx, uint32_t l)
{
    RoPage *pg = &cx->pg;
    RoLine *ln = &pg->ln[l];
    RoHead hd;
    ro_head_parse(cx, ln, &hd);
    if (l > 0 && ln->y - pg->ln[l - 1].y < 2.0f * pg->h && pg->ln[l - 1].kind == RL_TEXT &&
        ro_head_cont(cx, &hd, &pg->ln[l - 1], TRUE))
    {
        pg->ln[l - 1].kind = RL_HEADCONT;
    }
    if (l + 1 < pg->nl && pg->ln[l + 1].y - ln->y < 2.0f * pg->h)
    {
        RoLine *nx = &pg->ln[l + 1];
        RoHead h2;
        /* "Early" over "Voting": the second line is words only (marked a heading when it has
         * two vote-type words, "Voting Day") */
        if ((nx->kind == RL_TEXT || (nx->kind == RL_HEAD && ro_head_parse(cx, nx, &h2) && h2.tot < 0.0f)) &&
            ro_head_cont(cx, &hd, nx, FALSE))
        {
            nx->kind = RL_HEADCONT;
        }
    }
    ro_cols_from_head(cx, &hd);
}

static void ro_page(RoCx *cx)
{
    RoPage *pg = &cx->pg;
    uint32_t l, anchor, hdr_end;
    wchar_t pre[160];
    float shift = 0.0f;
    BOOL head_on_page = FALSE;
    if (pg->nl == 0)
    {
        return;
    }
    /* headings first: they place the columns and the labels' right limit */
    anchor = ro_mark(cx);
    /* the heading block: above the first anchor, less a contest title right over a "Vote For" */
    hdr_end = anchor;
    if (anchor < pg->nl)
    {
        wchar_t key[RO_TEXT_CCH];
        ro_key(pg->ln[anchor].text, key, ARRAYSIZE(key));
        if (ro_vote_for(key, pg->ln[anchor].text) != 0)
        {
            /* the title: a line or two right over it, not a heading line (date, county,
             * report name, precinct) */
            int ntitle = 0;
            while (hdr_end > 1 && ntitle < 2 && pg->ln[hdr_end].y - pg->ln[hdr_end - 1].y < 1.7f * pg->pitch)
            {
                const RoLine *up = &pg->ln[hdr_end - 1];
                wchar_t tmp[160], uk[RO_TEXT_CCH];
                ro_key(up->text, uk, ARRAYSIZE(uk));
                if (ro_precinct_name(up->text, tmp, ARRAYSIZE(tmp)) || ro_date(up->text, tmp, ARRAYSIZE(tmp)) ||
                    wcsstr(uk, L"COUNTY") != NULL || wcsstr(uk, L"REPORT") != NULL || wcsstr(uk, L"RESULTS") != NULL ||
                    wcsstr(uk, L"REPT") != NULL)
                {
                    break;
                }
                hdr_end--;
                ntitle++;
            }
        }
    }
    ro_header(cx, hdr_end, pre, ARRAYSIZE(pre));
    if (cx->page_kind == PK_SUMMARY && cx->d->nprecincts > 0)
    {
        ro_block_close(cx);
        cx->pages_summary_skipped++;
        return; /* a summary section of a precinct report */
    }
    if (pre[0] != L'\0' && cx->pre_placeholder && !ro_page_has_stats(cx, hdr_end))
    {
        ro_name_placeholder(cx, pre);
    }
    else if (pre[0] != L'\0')
    {
        if (cx->summary_data)
        {
            ro_drop_summary(cx);
            cx->pages_summary_skipped++;
        }
        ro_new_precinct(cx, pre);
    }
    for (l = hdr_end; l < pg->nl && !cx->oom; l++)
    {
        RoLine *ln = &pg->ln[l];
        wchar_t key[RO_LABEL_CCH], tkey[RO_TEXT_CCH];
        uint32_t vf;
        int k, nint = 0;
        if (ln->kind == RL_FOOTER || ln->kind == RL_HEADCONT)
        {
            continue;
        }
        if (ln->kind == RL_HEAD)
        {
            uint32_t nx = l + 1;
            ro_take_head(cx, l);
            while (nx < pg->nl && pg->ln[nx].kind != RL_HEAD && pg->ln[nx].kind != RL_FOOTER)
            {
                nx++;
            }
            ro_discover(cx, l + 1, nx);
            head_on_page = TRUE;
            shift = 0.0f;
            {
                wchar_t lk[64];
                ro_key(ln->text, lk, ARRAYSIZE(lk));
                if (ro_starts(lk, L"STATISTICS"))
                {
                    ro_block_close(cx);
                    cx->section = RS_STATS;
                }
            }
            continue;
        }
        ro_cells(cx, ln);
        ro_key(ln->label, key, ARRAYSIZE(key));
        ro_key(ln->text, tkey, ARRAYSIZE(tkey));
        for (k = 0; k < ln->nc; k++)
        {
            nint += ln->c[k].kind == RO_INT;
        }
        if (ro_starts(key, L"STATISTICS") && nint == 0)
        {
            ro_block_close(cx);
            cx->section = RS_STATS;
            cx->npend = 0;
            continue;
        }
        if (!ro_starts(key, L"REGISTEREDVOTERS") && wcsstr(key, L"REDVOTERS") != NULL && wcslen(key) < 40)
        {
            StringCchCopyW(key, ARRAYSIZE(key), L"REGISTEREDVOTERS"); /* ":red Voters - Total" (OCR cut it) */
            StringCchCatW(key, ARRAYSIZE(key), wcsstr(ln->label, L"Total") != NULL || wcsstr(ln->label, L"TOTAL") != NULL ? L"TOTAL" : L"");
        }
        if (ro_starts(key, L"WRITELN") || ro_starts(key, L"WRITE1N"))
        {
            key[5] = L'I'; /* "Write-ln" */
            key[6] = L'N';
        }
        if (ro_starts(key, L"REGISTEREDVOTERS") && cx->pre != UINT32_MAX && cx->reg_row_seen)
        {
            ro_block_close(cx);
            ro_bal_finish(cx, NULL);
            if (pre[0] == L'\0')
            {
                /* a second statistics block with no precinct line read: the next page may name it */
                wchar_t nm[64];
                StringCchPrintfW(nm, ARRAYSIZE(nm), L"(precinct not read, page %u)", cx->page + 1);
                ro_new_precinct(cx, nm);
                cx->pre_placeholder = TRUE;
                cx->pre_unread++;
            }
            else
            {
                /* another part of the same precinct (Cameron's county-only and city parts) */
                if (cx->used != NULL)
                {
                    memset(cx->used, 0, cx->cap_used);
                }
                cx->last_contest = UINT32_MAX;
                cx->reg_total_seen = cx->bal_total_seen = FALSE;
                cx->turnout_mark = cx->d->nturnout;
                cx->reg_base = cx->d->precincts[cx->pre].registered;
                cx->bal_base = cx->d->precincts[cx->pre].ballots;
                cx->npend = 0;
            }
        }
        if (ro_starts(key, L"REGISTEREDVOTERS"))
        {
            cx->reg_row_seen = TRUE;
        }
        if (ro_starts(key, L"REGISTEREDVOTERS") || ro_starts(key, L"BALLOTSCAST") || ro_starts(key, L"VOTERTURNOUT") ||
            ro_starts(key, L"PRECINCTSCOUNTED") || ro_starts(key, L"PRECINCTSREPORTING"))
        {
            ro_block_close(cx);
            cx->section = RS_STATS;
            cx->npend = 0;
            if (cx->cols.valid)
            {
                ro_stats_row(cx, key, ln);
            }
            continue;
        }
        if (nint == 0 && (vf = ro_vote_for(tkey, ln->text)) != 0)
        {
            int p;
            ro_block_close(cx);
            ZeroMemory(cx->blk.title, sizeof(cx->blk.title));
            for (p = cx->npend > 2 ? cx->npend - 2 : 0; p < cx->npend; p++)
            {
                if (cx->blk.title[0] != L'\0')
                {
                    StringCchCatW(cx->blk.title, ARRAYSIZE(cx->blk.title), L" ");
                }
                StringCchCatW(cx->blk.title, ARRAYSIZE(cx->blk.title), cx->pend[p]);
            }
            {
                size_t n = wcslen(cx->blk.title);
                while (n > 0 && (cx->blk.title[n - 1] == L'.' || cx->blk.title[n - 1] == L' ' ||
                                 cx->blk.title[n - 1] == L','))
                {
                    cx->blk.title[--n] = L'\0';
                }
            }
            ro_title_commas(cx->blk.title);
            cx->blk.vote_for = vf;
            cx->blk.nrows = 0;
            cx->blk.open = cx->blk.title[0] != L'\0';
            cx->section = RS_CONTEST;
            cx->npend = 0;
            continue;
        }
        if (ro_starts(tkey, L"WITH") && wcsstr(tkey, L"PRECINCTS") != NULL)
        {
            continue; /* "(WITH 103 OF 103 PRECINCTS COUNTED)" */
        }
        if (nint > 0 && nint <= 2 && cx->cols.valid)
        {
            int q, na = 0, npc = 0;
            for (q = 0; q < ln->nc; q++)
            {
                if (ln->c[q].kind == RO_PCT)
                {
                    npc++;
                }
                else if (ro_col_of(cx, &ln->c[q], head_on_page ? 0.0f : shift) >= 0)
                {
                    na++;
                }
            }
            if (na == 0 && npc == 0)
            {
                nint = 0; /* a title's number past the labels ("... District 34") */
                StringCchCopyW(ln->label, ARRAYSIZE(ln->label), ln->text);
            }
        }
        if (nint == 0 && cx->section == RS_CONTEST && cx->blk.open && cx->blk.nrows > 0 && cx->blk.nrows < RO_MAX_ROWS)
        {
            /* an over / under votes row of zeros, which OCR dropped */
            int kk = -1;
            if (ro_ends(key, L"DERVOTES") || ro_starts(key, L"UNDERVOTE"))
            {
                kk = RR_UNDER;
            }
            else if (ro_ends(key, L"VERVOTES") || ro_starts(key, L"OVERVOTE"))
            {
                kk = RR_OVER;
            }
            else if ((ro_ends(key, L"VOTES") || ro_ends(key, L"VOTE")) && wcslen(key) <= 9 &&
                     ro_block_has(&cx->blk, RR_TOTAL))
            {
                kk = ro_block_has(&cx->blk, RR_OVER) ? RR_UNDER : RR_OVER;
            }
            if (kk >= 0 && !ro_block_has(&cx->blk, kk))
            {
                RoRow *zr = &cx->blk.rows[cx->blk.nrows++];
                ZeroMemory(zr, sizeof(*zr));
                zr->kind = kk;
                StringCchCopyW(zr->label, ARRAYSIZE(zr->label), ln->label);
                for (k = 0; k <= RO_MAX_VT; k++)
                {
                    zr->has[k] = TRUE; /* printed with no number OCR could read: zeros */
                }
                continue;
            }
        }
        if (nint == 0 && (ro_ends(key, L"VOTES") || ro_ends(key, L"VOTE")) && wcslen(key) <= 12)
        {
            cx->npend = 0; /* "Over Votes", "Under Votes" with no count read: not a title */
            continue;
        }
        if (nint == 0)
        {
            /* text: the next contest's title (its lines up to "Vote For") */
            if (ln->text[0] != L'\0')
            {
                if (cx->npend > 0 && l > 0 && ln->y - pg->ln[l - 1].y > 2.2f * pg->pitch)
                {
                    cx->npend = 0; /* not the line before: a new title starts */
                }
                if (cx->npend == RO_PEND)
                {
                    memmove(cx->pend[0], cx->pend[1], sizeof(cx->pend[0]) * (RO_PEND - 1));
                    cx->npend--;
                }
                StringCchCopyW(cx->pend[cx->npend++], ARRAYSIZE(cx->pend[0]), ln->label[0] != L'\0' ? ln->label : ln->text);
            }
            continue;
        }
        cx->npend = 0;
        if (cx->section != RS_CONTEST || !cx->blk.open || !cx->cols.valid)
        {
            continue;
        }
        if (!head_on_page && shift == 0.0f)
        {
            /* no heading on this page: how far it sits from the last page's columns */
            float sum = 0.0f;
            int ns = 0;
            uint32_t m;
            for (m = l; m < pg->nl; m++)
            {
                int q;
                if (m > l && pg->ln[m].kind == RL_TEXT)
                {
                    ro_cells(cx, &pg->ln[m]);
                }
                for (q = 0; q < pg->ln[m].nc; q++)
                {
                    float dd = pg->ln[m].c[q].x1 - cx->cols.tot;
                    if (pg->ln[m].c[q].kind == RO_INT && fabsf(dd) < 4.0f * pg->h)
                    {
                        sum += dd;
                        ns++;
                    }
                }
            }
            shift = ns > 0 ? sum / (float)ns : 0.0001f;
        }
        {
            RoRow row;
            ZeroMemory(&row, sizeof(row));
            if (ro_starts(key, L"TOTALVOTE") || wcscmp(key, L"TOTAL") == 0 || wcscmp(key, L"TOTALS") == 0 ||
                ro_ends(key, L"TESCAST"))
            {
                row.kind = RR_TOTAL;
            }
            else if (ro_starts(key, L"CONTESTTOTAL") || ro_ends(key, L"STTOTALS"))
            {
                row.kind = RR_BALLOTS; /* "Contest Totals" (OCR may cut it: "est Totals") */
            }
            else if (ro_ends(key, L"DERVOTES") || ro_ends(key, L"DERVOTE") || ro_starts(key, L"UNDERVOTE") ||
                     (wcslen(key) >= 8 && wcslen(key) <= 12 && ro_lev(key, L"UNDERVOTES") <= 2))
            {
                row.kind = RR_UNDER; /* (and misread: "Undep Votes") */
            }
            else if (ro_ends(key, L"VERVOTES") || ro_ends(key, L"VERVOTE") || ro_starts(key, L"OVERVOTE") ||
                     (wcslen(key) >= 7 && wcslen(key) <= 11 && ro_lev(key, L"OVERVOTES") <= 2))
            {
                row.kind = RR_OVER;
            }
            else if ((ro_ends(key, L"VOTES") || ro_ends(key, L"VOTE")) && wcslen(key) <= 9 && ro_block_has(&cx->blk, RR_TOTAL))
            {
                /* a cut or garbled "Overvotes" / "Undervotes" ("ervotes", "-A-votes"): they come
                 * in that order after the Total row */
                row.kind = ro_block_has(&cx->blk, RR_OVER) ? RR_UNDER : RR_OVER;
            }
            else if (ro_starts(key, L"WRITEINTOTAL") || wcscmp(key, L"WRITEIN") == 0 || wcscmp(key, L"WRITEINS") == 0)
            {
                row.kind = RR_CHOICE;
                StringCchCopyW(row.label, ARRAYSIZE(row.label), L"Write-in");
            }
            else if (ro_starts(key, L"WRITEIN") || wcscmp(key, L"NOTASSIGNED") == 0)
            {
                cx->writein_rows++;
                continue;
            }
            else if (key[0] == L'\0')
            {
                cx->rows_unlabeled++;
                continue;
            }
            else
            {
                row.kind = RR_CHOICE;
                StringCchCopyW(row.label, ARRAYSIZE(row.label), ln->label);
            }
            if (row.label[0] == L'\0')
            {
                StringCchCopyW(row.label, ARRAYSIZE(row.label), ln->label);
            }
            for (k = 0; k < ln->nc; k++)
            {
                const RoCell *c = &ln->c[k];
                int col;
                if (c->kind == RO_PCT)
                {
                    if (!row.has_pct)
                    {
                        row.pct = c->pct;
                        row.has_pct = TRUE;
                    }
                    if (cx->cols.tmpl >= 0 && c->x0 > cx->cols.tot && (cx->cols.nvt == 0 || c->x1 < cx->cols.vt[0]))
                    {
                        RoTmpl *tp = &cx->tmpl[cx->cols.tmpl];
                        float rel = c->x1 - cx->cols.tot;
                        tp->pct_rel = tp->pct_rel < -999.0f ? rel : 0.9f * tp->pct_rel + 0.1f * rel;
                        cx->cols.pct = cx->cols.tot + tp->pct_rel;
                    }
                    continue;
                }
                col = ro_col_of(cx, c, head_on_page ? 0.0f : shift);
                if (col == -2)
                {
                    continue; /* a percentage read without its point */
                }
                if (col < 0)
                {
                    if (cx->dropped++ == 0)
                    {
                        StringCchPrintfW(cx->dropped_first, ARRAYSIZE(cx->dropped_first), L"%lld in %s, %s",
                                         (long long)c->v, cx->blk.title, row.label);
                    }
                    continue;
                }
                if (!row.has[col])
                {
                    row.v[col] = c->v;
                    row.has[col] = TRUE;
                }
            }
            if (row.kind == RR_CHOICE && !row.has_pct)
            {
                int q;
                BOOL any = FALSE;
                for (q = 0; q <= RO_MAX_VT; q++)
                {
                    any = any || row.has[q];
                }
                if (!any)
                {
                    continue; /* no count read: a title's line, not a choice */
                }
            }
            if (row.kind == RR_CHOICE && (ro_block_has(&cx->blk, RR_UNDER) || ro_block_has(&cx->blk, RR_BALLOTS)))
            {
                /* the contest's last rows were read: this is the next contest, its title not read */
                ro_block_close(cx);
                cx->rows_untitled++;
                continue;
            }
            if (cx->blk.nrows < RO_MAX_ROWS)
            {
                cx->blk.rows[cx->blk.nrows++] = row;
            }
        }
    }
}

/* Vote types in the order of the widest layout's columns (OCR may have read a later column's
 * heading first). */
static void ro_order_vt(RoCx *cx)
{
    EeRpdfDoc *d = cx->d;
    uint32_t map[RO_MAX_VT], order[RO_MAX_VT], n = 0, i;
    wchar_t *names[RO_MAX_VT];
    int t, best = -1, j;
    for (t = 0; t < cx->ntmpl; t++)
    {
        if (best < 0 || cx->tmpl[t].nvt > cx->tmpl[best].nvt)
        {
            best = t;
        }
    }
    for (i = 0; i < d->nvt; i++)
    {
        map[i] = UINT32_MAX;
    }
    if (best >= 0)
    {
        const RoTmpl *tp = &cx->tmpl[best];
        int idx[RO_MAX_VT];
        for (j = 0; j < tp->nvt; j++)
        {
            idx[j] = j;
        }
        for (j = 1; j < tp->nvt; j++)
        {
            int q = j;
            while (q > 0 && tp->rel[idx[q]] < tp->rel[idx[q - 1]])
            {
                int x = idx[q];
                idx[q] = idx[q - 1];
                idx[q - 1] = x;
                q--;
            }
        }
        for (j = 0; j < tp->nvt; j++)
        {
            uint32_t v = tp->vti[idx[j]];
            if (v < d->nvt && map[v] == UINT32_MAX)
            {
                map[v] = n;
                order[n++] = v;
            }
        }
    }
    for (i = 0; i < d->nvt; i++)
    {
        if (map[i] == UINT32_MAX)
        {
            map[i] = n;
            order[n++] = i;
        }
    }
    for (i = 0; i < n; i++)
    {
        names[i] = d->vote_types[order[i]];
    }
    for (i = 0; i < n; i++)
    {
        d->vote_types[i] = names[i];
    }
    for (t = 0; t < cx->ntmpl; t++)
    {
        for (j = 0; j < cx->tmpl[t].nvt; j++)
        {
            cx->tmpl[t].vti[j] = map[cx->tmpl[t].vti[j]];
        }
    }
}

/* Contests that came out with the same title (one read in a few precincts under a misread title,
 * then named by its commonest spelling) and no precinct in common: one contest. */
static void ro_merge_same_titles(RoCx *cx)
{
    EeRpdfDoc *d = cx->d;
    uint32_t i, j, k;
    size_t q;
    unsigned char *pi = NULL, *pj = NULL;
    if (d->nprecincts == 0)
    {
        return;
    }
    pi = (unsigned char *)calloc(d->nprecincts, 1);
    pj = (unsigned char *)calloc(d->nprecincts, 1);
    if (pi == NULL || pj == NULL)
    {
        free(pi);
        free(pj);
        return;
    }
    for (i = 0; i < d->ncontests; i++)
    {
        for (j = i + 1; j < d->ncontests; j++)
        {
            EeRpdfContest *ci = &d->contests[i], *cj = &d->contests[j];
            BOOL overlap = FALSE;
            uint32_t map[256];
            if (cj->vote_for != ci->vote_for || wcscmp(cj->title, ci->title) != 0 || cj->nchoices > ARRAYSIZE(map))
            {
                continue;
            }
            memset(pi, 0, d->nprecincts);
            memset(pj, 0, d->nprecincts);
            for (q = 0; q < d->ncounts; q++)
            {
                if (d->counts[q].pre < d->nprecincts)
                {
                    if (d->counts[q].contest == i)
                    {
                        pi[d->counts[q].pre] = 1;
                    }
                    else if (d->counts[q].contest == j)
                    {
                        pj[d->counts[q].pre] = 1;
                    }
                }
            }
            for (k = 0; k < d->nprecincts && !overlap; k++)
            {
                overlap = pi[k] && pj[k];
            }
            if (overlap)
            {
                continue;
            }
            /* its choices onto the first's, by name */
            for (k = 0; k < cj->nchoices && !cx->oom; k++)
            {
                uint32_t m, best = UINT32_MAX;
                double bs = 0.0;
                for (m = 0; m < ci->nchoices; m++)
                {
                    double sim = ro_like_choice(ci->choices[m].name, cj->choices[k].name);
                    if (sim > bs)
                    {
                        bs = sim;
                        best = m;
                    }
                }
                if (best == UINT32_MAX || bs < 0.75)
                {
                    if (!ro_grow((void **)&ci->choices, &ci->cap_choices, ci->nchoices + 1, sizeof(EeRpdfChoice)))
                    {
                        cx->oom = TRUE;
                        break;
                    }
                    ci->choices[ci->nchoices] = cj->choices[k];
                    cj->choices[k].name = NULL;
                    cj->choices[k].party = NULL;
                    best = ci->nchoices++;
                }
                map[k] = best;
            }
            if (cx->oom)
            {
                break;
            }
            ci->has_over_under = ci->has_over_under || cj->has_over_under;
            for (q = 0; q < d->ncounts; q++)
            {
                EeRpdfCount *e = &d->counts[q];
                if (e->contest == j)
                {
                    e->contest = i;
                    if (e->choice < cj->nchoices)
                    {
                        e->choice = map[e->choice];
                    }
                }
                else if (e->contest > j)
                {
                    e->contest--;
                }
            }
            for (k = 0; k < cj->nchoices; k++)
            {
                free(cj->choices[k].name);
                free(cj->choices[k].party);
            }
            free(cj->choices);
            free(cj->title);
            memmove(cj, cj + 1, (size_t)(d->ncontests - j - 1) * sizeof(EeRpdfContest));
            d->ncontests--;
            j--;
        }
    }
    free(pi);
    free(pj);
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */

BOOL EeRocr_IsScannedPage(EePdf *pdf, uint32_t page)
{
    EePdfPageText pt;
    EePdfImage img;
    BOOL scanned = FALSE;
    EePdf_PageTextInit(&pt);
    ZeroMemory(&img, sizeof(img));
    if (EePdf_ExtractPageText(pdf, page, &pt) && pt.nruns == 0 && EePdf_GetPageImage(pdf, page, &img) &&
        img.kind != EE_PDF_IMAGE_NONE)
    {
        scanned = TRUE;
    }
    EePdf_ImageFree(&img);
    EePdf_PageTextFree(&pt);
    return scanned;
}

BOOL EeRocr_Parse(EePdf *pdf,
                  EeRpdfDoc *out,
                  BOOL (*page_fn)(uint32_t done, uint32_t total, void *user),
                  void *user,
                  wchar_t *err,
                  size_t errcch)
{
    RoCx *cx;
    EeOcr *ocr = NULL;
    EeOcrText t;
    EePdfImage img;
    wchar_t oerr[256] = L"";
    uint32_t np, p, i, k, typical = 0;
    int turns = 0;
    BOOL ok = FALSE, cancelled = FALSE;

    ZeroMemory(out, sizeof(*out));
    out->registered_total = EE_RPDF_UNKNOWN;
    out->ballots_total = EE_RPDF_UNKNOWN;
    cx = (RoCx *)calloc(1, sizeof(RoCx));
    if (cx == NULL)
    {
        ro_set_err(err, errcch, L"Out of memory reading the PDF.");
        return FALSE;
    }
    if (!EeOcr_Create(&ocr, oerr, ARRAYSIZE(oerr)))
    {
        wchar_t msg[400];
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"The report is scanned (page images only) and Windows OCR, which reads it, is not "
                         L"available: %s",
                         oerr);
        ro_set_err(err, errcch, msg);
        free(cx);
        return FALSE;
    }
    cx->d = out;
    cx->pre = UINT32_MAX;
    cx->label_lim = -1.0f;
    cx->pre_y = -1.0f;
    cx->last_contest = UINT32_MAX;
    for (i = 0; i < RO_MAX_VT; i++)
    {
        cx->bvt[i] = EE_RPDF_UNKNOWN;
    }
    EeOcr_TextInit(&t);
    ZeroMemory(&img, sizeof(img));
    np = EePdf_PageCount(pdf);
    cx->pages = np;
    /* the first pages' headings first: OCR leaves out heading words here and there, and
     * together they give each table's columns in full */
    cx->learning = TRUE;
    for (p = 0; p < np && p < 4 && !cx->oom; p++)
    {
        uint32_t l;
        if (!ro_ocr_page(cx, pdf, p, ocr, &t, &img, &turns, &typical))
        {
            cx->oom = TRUE;
            break;
        }
        ro_mark(cx);
        for (l = 0; l < cx->pg.nl; l++)
        {
            if (cx->pg.ln[l].kind == RL_HEAD)
            {
                uint32_t nx = l + 1;
                ro_take_head(cx, l);
                while (nx < cx->pg.nl && cx->pg.ln[nx].kind != RL_HEAD && cx->pg.ln[nx].kind != RL_FOOTER)
                {
                    nx++;
                }
                ro_discover(cx, l + 1, nx);
            }
        }
    }
    cx->learning = FALSE;
    ro_order_vt(cx);
    cx->cols.valid = FALSE;
    cx->label_lim = -1.0f;
    cx->pages_read = cx->pages_blank = cx->pages_failed = 0;
    for (p = 0; p < np && !cx->oom; p++)
    {
        if (!ro_ocr_page(cx, pdf, p, ocr, &t, &img, &turns, &typical))
        {
            cx->oom = TRUE;
            break;
        }
        cx->page = p;
        ro_page(cx);
        if (page_fn != NULL && !page_fn(p + 1, np, user))
        {
            cancelled = TRUE;
            break;
        }
        /* not a report this reader knows: stop after a few pages */
        if (p == 4 && out->ncontests == 0 && !cx->blk.open)
        {
            break;
        }
    }
    ro_block_close(cx);
    ro_bal_finish(cx, NULL);
    EePdf_ImageFree(&img);
    EeOcr_TextFree(&t);
    EeOcr_Destroy(ocr);

    /* each name as OCR read it most often */
    for (i = 0; i < out->ncontests && !cx->oom; i++)
    {
        const wchar_t *b = ro_var_best(&cx->cvar[i]);
        EeRpdfContest *c = &out->contests[i];
        if (b != NULL && wcscmp(b, c->title) != 0)
        {
            wchar_t *nt = ro_wdup(b);
            if (nt != NULL)
            {
                free(c->title);
                c->title = nt;
            }
        }
        for (k = 0; k < c->nchoices; k++)
        {
            const wchar_t *cb = ro_var_best(&cx->chvar[i][k]);
            if (cb != NULL && wcscmp(cb, c->choices[k].name) != 0)
            {
                wchar_t *nn = ro_wdup(cb);
                wchar_t party[8];
                if (nn != NULL)
                {
                    free(c->choices[k].name);
                    c->choices[k].name = nn;
                    ro_party(nn, party, ARRAYSIZE(party));
                    free(c->choices[k].party);
                    c->choices[k].party = party[0] != L'\0' ? ro_wdup(party) : NULL;
                }
            }
        }
    }
    for (i = 0; i < out->nprecincts && !cx->oom; i++)
    {
        const wchar_t *b = ro_var_best(&cx->pvar[i]);
        if (b != NULL && iswdigit(b[0]))
        {
            /* "1" where OCR lost "Precinct": the spelled-out name, if read */
            for (k = 0; k < cx->pvar[i].n; k++)
            {
                if (_wcsnicmp(cx->pvar[i].v[k].name, L"Precinct ", 9) == 0)
                {
                    b = cx->pvar[i].v[k].name;
                    break;
                }
            }
        }
        if (b != NULL && wcscmp(b, out->precincts[i].name) != 0)
        {
            wchar_t *nn = ro_wdup(b);
            if (nn != NULL)
            {
                free(out->precincts[i].name);
                out->precincts[i].name = nn;
            }
        }
    }

    if (!cx->oom)
    {
        ro_merge_same_titles(cx);
    }
    if (cancelled)
    {
        ro_set_err(err, errcch, L"Load cancelled.");
    }
    else if (cx->oom)
    {
        ro_set_err(err, errcch, L"Out of memory reading the PDF.");
    }
    else if (cx->pages_read == 0)
    {
        wchar_t msg[400];
        StringCchPrintfW(msg, ARRAYSIZE(msg), L"The report is scanned (page images only) and its pages could not be read%s%s.",
                         cx->ocr_err[0] ? L": " : L"", cx->ocr_err);
        ro_set_err(err, errcch, msg);
    }
    else if (out->ncontests == 0)
    {
        ro_set_err(err, errcch,
                   L"The report is scanned (page images only) and is not a results report this version can read "
                   L"by OCR. Scanned summary and precinct reports from ES&S (Electionware, and the older "
                   L"monospaced reports) can be loaded.");
    }
    else
    {
        wchar_t msg[700];
        out->kind = out->nprecincts > 0 ? EE_RPDF_ESS_PRECINCT : EE_RPDF_ESS_SUMMARY;
        if (out->nprecincts == 0 && out->nvt > 0)
        {
            BOOL any = FALSE;
            out->ballots_vt_total = (int64_t *)malloc(out->nvt * sizeof(int64_t));
            if (out->ballots_vt_total == NULL)
            {
                cx->oom = TRUE;
            }
            else
            {
                for (i = 0; i < out->nvt; i++)
                {
                    out->ballots_vt_total[i] = cx->bvt[i];
                    any = any || cx->bvt[i] != EE_RPDF_UNKNOWN;
                }
                if (!any)
                {
                    free(out->ballots_vt_total);
                    out->ballots_vt_total = NULL;
                }
            }
        }
        StringCchPrintfW(msg, ARRAYSIZE(msg),
                         L"The report is scanned: its %u pages were read by OCR (Windows OCR). Of %llu rows of counts, "
                         L"%llu had a count OCR left out or misread that their row and column totals gave",
                         cx->pages_read, (unsigned long long)cx->rows, (unsigned long long)cx->rows_fixed);
        if (cx->rows_review > 0)
        {
            StringCchPrintfW(msg + wcslen(msg), ARRAYSIZE(msg) - wcslen(msg),
                             L"; %llu do not add up and need checking against the report", (unsigned long long)cx->rows_review);
            for (i = 0; i < cx->nreview_ex; i++)
            {
                StringCchPrintfW(msg + wcslen(msg), ARRAYSIZE(msg) - wcslen(msg), L"%s %s", i == 0 ? L", such as" : L";",
                                 cx->review_ex[i]);
            }
        }
        StringCchCatW(msg, ARRAYSIZE(msg), L".");
        ro_note(cx, msg);
        if (cx->pages_blank + cx->pages_failed > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg), L"%u pages had no image to read%s.", cx->pages_blank + cx->pages_failed,
                             cx->pages_failed > 0 ? L" or could not be read" : L"");
            ro_note(cx, msg);
        }
        if (cx->pages_summary_skipped > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"%u summary pages of the precinct report were not read; its precinct pages were.",
                             cx->pages_summary_skipped);
            ro_note(cx, msg);
        }
        if (cx->dropped > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg), L"%u numbers did not line up with a column and were not read; the first: %s.",
                             cx->dropped, cx->dropped_first);
            ro_note(cx, msg);
        }
        if (cx->rows_untitled > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"%llu rows of counts came after a contest's last row with no contest title OCR could "
                             L"read, and were not read.",
                             (unsigned long long)cx->rows_untitled);
            ro_note(cx, msg);
        }
        if (cx->pre_unread > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"%u precinct names could not be read; their precincts are named by their page, "
                             L"\"(precinct not read, page n)\".",
                             cx->pre_unread);
            ro_note(cx, msg);
        }
        if (cx->rows_unlabeled > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg), L"%llu rows of counts had no name OCR could read and were not read.",
                             (unsigned long long)cx->rows_unlabeled);
            ro_note(cx, msg);
        }
        if (cx->writein_rows > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"%llu write-in rows (\"Write-In: name\", \"Not Assigned\") are part of Write-In Totals and "
                             L"are not listed separately.",
                             (unsigned long long)cx->writein_rows);
            ro_note(cx, msg);
        }
        ok = !cx->oom;
    }
    for (i = 0; i < cx->cap_cvar; i++) /* (merged contests leave theirs past ncontests) */
    {
        ro_var_free(&cx->cvar[i]);
        if (cx->chvar != NULL && cx->chvar[i] != NULL)
        {
            for (k = 0; k < cx->cap_chvar[i]; k++)
            {
                ro_var_free(&cx->chvar[i][k]);
            }
            free(cx->chvar[i]);
        }
    }
    for (i = 0; i < cx->cap_pvar; i++)
    {
        ro_var_free(&cx->pvar[i]);
    }
    free(cx->cvar);
    free(cx->chvar);
    free(cx->cap_chvar);
    free(cx->pvar);
    free(cx->used);
    free(cx->pg.w);
    free(cx->pg.ln);
    free(cx);
    if (!ok)
    {
        EeRpdf_Free(out);
    }
    return ok;
}
