/**
 * @file ee_results_pdf.c
 * @brief ES&S Electionware results report PDFs (summary and precinct reports) read into
 *        an EeRpdfDoc. See ee_results_pdf.h.
 *
 * Layout (JasperReports, as printed by Electionware):
 * - page heading: three 13 pt lines (report title, election, date, county, status);
 *   precinct reports add a fourth 13 pt line lower down with the precinct name;
 * - "STATISTICS" (10 pt), its column headings, then 8 pt rows "Registered Voters - Total",
 *   "Ballots Cast - Total" (or "- Democratic Party" in a primary), "Voter Turnout - Total";
 * - each contest: its title (12 pt), "Vote For N" (10 pt), column headings (10 pt; a
 *   heading may take two lines, "Election" over "Day"), then 8 pt rows: candidates (a
 *   party code may lead the name), "Write-In Totals" with indented "Write-In: name" and
 *   "Not Assigned" rows, "Total Votes Cast", "Overvotes", "Undervotes", "Contest Totals";
 * - footer: "Report generated with Electionware", the run date and "Page N of M". Some
 *   counties' reports leave the footer out (Cameron, Grayson, Dallas, Williamson); a
 *   JasperReports PDF is then taken as Electionware when it shows a "STATISTICS" block
 *   or a contest's "Vote For" line.
 * Numbers are right-aligned in their columns, so each is given to the column whose right
 * edge (heading centre plus half the column pitch) is nearest its own right edge.
 *
 * Hart Verity reports (Microsoft Reporting Services) are read by hart_page():
 * - every page starts with the same heading, ending with "Run Date";
 * - cumulative: each contest is a 10 pt title, a 9 pt "Choice  Party  <vote types>  Total"
 *   heading, rows of name, party and a count and percentage under each heading (the
 *   percentage right-aligned with the heading, the count just left of it), then "Cast
 *   Votes:", "Undervotes:" and "Overvotes:";
 * - canvass: one contest per page, a 9 pt title, column headings turned 90 degrees (the
 *   choices "Name, PARTY", Cast Votes, Undervotes, Overvotes, ballots cast by vote type,
 *   Total Ballots Cast, Registered Voters, Turnout Percentage), "Precinct", a row per
 *   precinct and a "Totals" row. A contest goes on over pages; columns that do not fit
 *   go on to a following page under the same title.
 */

#include "ee_results_pdf.h"
#include "ee_results_dom.h"
#include "ee_results_ocr.h"
#include "ee_glyph_ocr.h"
#include "pdf_reader.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <wctype.h>
#include <strsafe.h>

#define RP_FOOTER_Y   55.0f /* runs below this are the footer */
#define RP_LINE_TOL   1.5f  /* runs this close in y share a line */
#define RP_ATTACH_TOL 6.0f  /* a line of numbers this close to a labelled row belongs to it */
#define RP_TITLE_SIZE 11.5f /* contest titles are 12 pt */
#define RP_HEAD_SIZE  9.5f  /* headings and "Vote For" are 10 pt; rows are 8 pt */
#define RP_PAGE_SIZE  12.5f /* the page heading is 13 pt */
#define RP_MAX_PIECES 96
#define RP_MAX_COLS   32

/* -------------------------------------------------------------------------- */
/* Helpers                                                                    */
/* -------------------------------------------------------------------------- */

static BOOL rp_grow(void **arr, size_t *cap, size_t need, size_t elem)
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

/* uint32_t-capacity variant for the doc's arrays */
static BOOL rp_grow32(void **arr, uint32_t *cap, uint32_t need, size_t elem)
{
    size_t c = *cap;
    if (!rp_grow(arr, &c, need, elem) || c > UINT32_MAX)
    {
        return FALSE;
    }
    *cap = (uint32_t)c;
    return TRUE;
}

static void rp_set_err(wchar_t *dst, size_t cch, const wchar_t *msg)
{
    if (dst != NULL && cch > 0)
    {
        StringCchCopyW(dst, cch, msg);
    }
}

static BOOL rp_ws(wchar_t c)
{
    return c == L' ' || c == L'\t' || c == L'\r' || c == L'\n' || c == 0xA0;
}

/* Trim in place; collapse inner whitespace runs to one space. */
static void rp_norm(wchar_t *s)
{
    wchar_t *w = s;
    const wchar_t *r = s;
    BOOL space = FALSE;
    while (*r != L'\0' && rp_ws(*r))
    {
        r++;
    }
    for (; *r != L'\0'; r++)
    {
        if (rp_ws(*r))
        {
            space = TRUE;
            continue;
        }
        if (space)
        {
            *w++ = L' ';
            space = FALSE;
        }
        *w++ = *r;
    }
    *w = L'\0';
}

/* Words in cells 2+ spaces apart ("TOTAL  VOTE %  Absentee"): two or more cells and a letter. */
static BOOL rp_split_cells(const wchar_t *s)
{
    BOOL letter = FALSE, gap = FALSE, seen = FALSE, cells = FALSE;
    for (; *s != L'\0'; s++)
    {
        if (rp_ws(s[0]) && rp_ws(s[1]))
        {
            gap = gap || seen;
        }
        else if (!rp_ws(*s))
        {
            cells = cells || gap;
            seen = TRUE;
            letter = letter || iswalpha(*s);
        }
    }
    return cells && letter;
}

static wchar_t *rp_wdup(const wchar_t *s)
{
    size_t n = wcslen(s) + 1;
    wchar_t *d = (wchar_t *)malloc(n * sizeof(wchar_t));
    if (d != NULL)
    {
        memcpy(d, s, n * sizeof(wchar_t));
    }
    return d;
}

static BOOL rp_starts(const wchar_t *s, const wchar_t *prefix)
{
    return _wcsnicmp(s, prefix, wcslen(prefix)) == 0;
}

/* Advance width of @p s at @p size in Helvetica (Electionware's numbers and headings use a
 * standard font that carries no glyph widths). Williamson's headings sit 1 pt apart, so the
 * widths are Helvetica's own (AFM, 1/1000 em, ' ' through '~'). */
static float rp_char_width(wchar_t c, float size)
{
    static const unsigned short k_helv[95] = {
        278, 278, 355, 556, 556, 889, 667, 191, 333, 333, 389, 584, 278, 333, 278, 278, /* ' '-'/' */
        556, 556, 556, 556, 556, 556, 556, 556, 556, 556, 278, 278, 584, 584, 584, 556, /* '0'-'?' */
        1015, 667, 667, 722, 722, 667, 611, 778, 722, 278, 500, 667, 556, 833, 722, 778, /* '@'-'O' */
        667, 778, 722, 667, 611, 722, 667, 944, 667, 667, 611, 278, 278, 278, 469, 556,  /* 'P'-'_' */
        333, 556, 556, 500, 556, 556, 278, 556, 556, 222, 222, 500, 222, 833, 556, 556,  /* '`'-'o' */
        556, 556, 333, 500, 278, 556, 500, 722, 500, 500, 500, 334, 260, 334, 584};      /* 'p'-'~' */
    return (float)((c >= L' ' && c <= L'~') ? k_helv[c - L' '] : 556u) / 1000.0f * size;
}

/* Only digits, ",", ".", "%" and spaces in the @p n characters at @p s. */
static BOOL rp_numeric_chars(const wchar_t *s, size_t n)
{
    size_t i;
    for (i = 0; i < n; i++)
    {
        if (!((s[i] >= L'0' && s[i] <= L'9') || s[i] == L',' || s[i] == L'.' || s[i] == L'%' || s[i] == L' '))
        {
            return FALSE;
        }
    }
    return n > 0;
}

static float rp_text_width_n(const wchar_t *s, size_t n, float size)
{
    float w = 0.0f;
    size_t i;
    for (i = 0; i < n; i++)
    {
        w += rp_char_width(s[i], size);
    }
    return w;
}

static float rp_text_width(const wchar_t *s, float size)
{
    float w = 0.0f;
    for (; *s != L'\0'; s++)
    {
        w += rp_char_width(*s, size);
    }
    return w;
}

/* A number cell: "1,234" (kind 1) or "12.34%" (kind 2). Returns 0 for anything else. */
static int rp_number(const wchar_t *s, int64_t *out)
{
    int64_t v = 0;
    BOOL digits = FALSE, dot = FALSE;
    const wchar_t *p = s;
    if (*p == L'\0')
    {
        return 0;
    }
    for (; *p != L'\0'; p++)
    {
        if (*p >= L'0' && *p <= L'9')
        {
            if (!dot)
            {
                if (v > (INT64_MAX - 9) / 10)
                {
                    return 0;
                }
                v = v * 10 + (*p - L'0');
            }
            digits = TRUE;
        }
        else if (*p == L',' && !dot)
        {
            continue;
        }
        else if (*p == L'.' && !dot)
        {
            dot = TRUE;
        }
        else if (*p == L'%' && p[1] == L'\0' && digits)
        {
            *out = v;
            return 2;
        }
        else
        {
            return 0;
        }
    }
    if (!digits || dot)
    {
        return 0;
    }
    *out = v;
    return 1;
}

/* "November 3, 2020" */
static BOOL rp_is_date(const wchar_t *s)
{
    static const wchar_t *const k_months[] = {L"January ", L"February ", L"March ",    L"April ",
                                              L"May ",     L"June ",     L"July ",     L"August ",
                                              L"September ", L"October ", L"November ", L"December "};
    size_t i;
    for (i = 0; i < ARRAYSIZE(k_months); i++)
    {
        size_t n = wcslen(k_months[i]);
        if (wcsncmp(s, k_months[i], n) == 0 && s[n] >= L'0' && s[n] <= L'9')
        {
            size_t len = wcslen(s);
            return len >= n + 7 && s[len - 5] == L' ' && s[len - 4] >= L'1' && s[len - 4] <= L'2';
        }
    }
    return FALSE;
}

/* A list of numbers ("1,59,83": precincts Cameron reports together), not one number: a comma
 * not followed by exactly three digits. */
static BOOL rp_number_list(const wchar_t *s)
{
    const wchar_t *c = wcschr(s, L',');
    BOOL list = FALSE;
    if (c == NULL || !iswdigit(s[0]))
    {
        return FALSE;
    }
    for (; *s != L'\0'; s++)
    {
        if (*s == L',')
        {
            list = list || !(iswdigit(s[1]) && iswdigit(s[2]) && iswdigit(s[3]) && !iswdigit(s[4]));
        }
        else if (!iswdigit(*s))
        {
            return FALSE;
        }
    }
    return list;
}

/* Party codes Electionware prints before a candidate's name ("REP Donald J. Trump"). */
static BOOL rp_party_code(const wchar_t *s, size_t n)
{
    static const wchar_t *const k[] = {L"REP", L"DEM", L"LIB", L"GRE", L"GRN", L"IND",
                                       L"NON", L"NPA", L"CON", L"REF", L"SOC", L"WRI"};
    size_t i;
    for (i = 0; i < ARRAYSIZE(k); i++)
    {
        if (wcslen(k[i]) == n && wcsncmp(s, k[i], n) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

/* -------------------------------------------------------------------------- */
/* Document building                                                          */
/* -------------------------------------------------------------------------- */

typedef struct RpItem
{
    float x, y, x1; /* x1: right edge */
    float size;
    wchar_t *text; /* in the page's text pool */
    int num;       /* rp_number kind */
    int64_t value;
    int multi;     /* numbers in a run of several cells ("4,219 59.75% 295"), else 0 */
    BOOL exact;    /* x1 comes from the font's own widths */
    BOOL sp_before, sp_after; /* the shown string began / ended with a space */
    int rot; /* text direction (pdf_reader), 90 for Hart's column headings */
} RpItem;

typedef struct RpPiece
{
    float x0, x1, y;
    wchar_t text[96];
} RpPiece;

enum
{
    RC_TOTAL,
    RC_PCT,
    RC_VT
};

typedef struct RpCol
{
    float x0, x1, center, anchor;
    int kind;
    uint32_t vt;
    int pieces[8];
    int npieces;
} RpCol;

enum
{
    RS_NONE,
    RS_STATS,
    RS_CONTEST
};

typedef struct RpOcc
{
    uint32_t contest; /* first contest with the title */
    uint32_t count;
} RpOcc;

typedef struct RpCtx
{
    EeRpdfDoc *d;
    BOOL oom;
    BOOL electionware; /* the footer names Electionware */
    BOOL jasper;       /* the Creator is JasperReports, or the Producer ReportLab (Electionware's
                        * layout with no footer) */
    BOOL aligned;      /* ReportLab: numbers line up with their headings (rp_aligned_col) */
    BOOL structure;    /* a "STATISTICS" block or a contest's "Vote For" line was read */
    BOOL any_text;
    uint32_t pre; /* current precinct, UINT32_MAX before the first */
    int section;
    wchar_t title[512];
    wchar_t ptitle[512]; /* body text that may be the next contest's title */
    BOOL title_pending;
    uint32_t vote_for;
    uint32_t contest; /* current contest (valid once the title is resolved) */
    BOOL have_contest;
    RpPiece pieces[RP_MAX_PIECES];
    int npieces;
    BOOL hdr_open;
    RpCol cols[RP_MAX_COLS];
    int ncols;
    float pitch;
    BOOL reg_total_seen, bal_total_seen; /* per precinct (or the document) */
    RpOcc *occ;                          /* contest titles seen in the current precinct */
    size_t nocc, cap_occ;
    uint64_t writein_rows;
    uint32_t dropped_numbers;
    wchar_t dropped_first[300]; /* where the first one was */
    uint32_t place_pages;       /* pages of results by polling place, not read */
    wchar_t place_first[128];
    uint32_t nbvt; /* entries in d->ballots_vt_total */
    uint32_t *seq; /* contests in the order each precinct shows them (UINT32_MAX = next precinct) */
    size_t nseq, cap_seq;
    /* Hart */
    BOOL hart;          /* Microsoft Reporting Services report */
    BOOL hart_canvass;  /* a canvass page was read */
    BOOL hart_cum;      /* a cumulative table was read */
    float party_x;      /* cumulative: the Party column */
    struct HtCol *hcols; /* cumulative: the table heading's columns (RP_MAX_COLS) */
    int nhcols;
    wchar_t hpending[512]; /* cumulative: a title waiting for its table heading */
    BOOL hart_title_line; /* the previous line was (part of) a contest title */
    float hart_title_y;
    /* Hart before Verity (hl_page) */
    BOOL hart_old;       /* the first page has "Precincts Reporting" and a voters line */
    BOOL hl_table;       /* a cumulative or precinct report table was read */
    BOOL hl_canvass;     /* a canvass grid was read */
    BOOL hl_head_done;   /* the page heading was read (from the first report page) */
    wchar_t hl_party[8]; /* a primary's party code from the report title ("REP"), else "" */
    wchar_t hl_title[512]; /* a title waiting for its first row */
    float hl_cols[16];   /* table: the heading centres after "Candidate" */
    int hl_colvt[16];    /* their vote types (rp_vt), or -1 for Total */
    int hl_ncols;
    uint32_t hl_bad_rows; /* canvass rows whose numbers did not match the columns */
} RpCtx;

static void rp_note(RpCtx *cx, const wchar_t *text)
{
    EeRpdfDoc *d = cx->d;
    if (!rp_grow32((void **)&d->notes, &d->cap_notes, d->nnotes + 1, sizeof(wchar_t *)) ||
        (d->notes[d->nnotes] = rp_wdup(text)) == NULL)
    {
        cx->oom = TRUE;
        return;
    }
    d->nnotes++;
}

static uint32_t rp_vt(RpCtx *cx, const wchar_t *name)
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
    if (!rp_grow32((void **)&d->vote_types, &d->cap_vt, d->nvt + 1, sizeof(wchar_t *)) ||
        (d->vote_types[d->nvt] = rp_wdup(name)) == NULL)
    {
        cx->oom = TRUE;
        return 0;
    }
    return d->nvt++;
}

static uint32_t rp_precinct(RpCtx *cx, const wchar_t *name)
{
    EeRpdfDoc *d = cx->d;
    uint32_t i;
    for (i = d->nprecincts; i-- > 0;)
    {
        if (wcscmp(d->precincts[i].name, name) == 0)
        {
            return i;
        }
    }
    if (!rp_grow32((void **)&d->precincts, &d->cap_pre, d->nprecincts + 1, sizeof(EeRpdfPrecinct)) ||
        (d->precincts[d->nprecincts].name = rp_wdup(name)) == NULL)
    {
        cx->oom = TRUE;
        return 0;
    }
    d->precincts[d->nprecincts].registered = EE_RPDF_UNKNOWN;
    d->precincts[d->nprecincts].ballots = EE_RPDF_UNKNOWN;
    return d->nprecincts++;
}

/* The contest for @p title: the n-th contest of that title for its n-th appearance in the
 * current precinct (two contests can share a title, "Proposition A" of two cities). */
static uint32_t rp_contest(RpCtx *cx, const wchar_t *title, uint32_t vote_for)
{
    EeRpdfDoc *d = cx->d;
    uint32_t first = UINT32_MAX, i, nth = 0, seen = 0;
    size_t k;
    EeRpdfContest *c;
    for (i = 0; i < d->ncontests; i++)
    {
        if (wcscmp(d->contests[i].title, title) == 0 && d->contests[i].vote_for == vote_for)
        {
            first = i;
            break;
        }
    }
    if (first != UINT32_MAX)
    {
        for (k = 0; k < cx->nocc; k++)
        {
            if (cx->occ[k].contest == first)
            {
                nth = cx->occ[k].count++;
                break;
            }
        }
        if (k == cx->nocc)
        {
            if (!rp_grow((void **)&cx->occ, &cx->cap_occ, cx->nocc + 1, sizeof(RpOcc)))
            {
                cx->oom = TRUE;
                return 0;
            }
            cx->occ[cx->nocc].contest = first;
            cx->occ[cx->nocc].count = 1;
            cx->nocc++;
        }
        for (i = first; i < d->ncontests; i++)
        {
            if (wcscmp(d->contests[i].title, title) == 0 && d->contests[i].vote_for == vote_for &&
                seen++ == nth)
            {
                return i;
            }
        }
    }
    if (!rp_grow32((void **)&d->contests, &d->cap_contests, d->ncontests + 1, sizeof(EeRpdfContest)))
    {
        cx->oom = TRUE;
        return 0;
    }
    c = &d->contests[d->ncontests];
    ZeroMemory(c, sizeof(*c));
    c->title = rp_wdup(title);
    c->vote_for = vote_for;
    c->over = EE_RPDF_UNKNOWN;
    c->under = EE_RPDF_UNKNOWN;
    c->ballots = EE_RPDF_UNKNOWN;
    if (c->title == NULL)
    {
        cx->oom = TRUE;
    }
    if (first == UINT32_MAX)
    {
        /* first appearance in this precinct */
        if (!rp_grow((void **)&cx->occ, &cx->cap_occ, cx->nocc + 1, sizeof(RpOcc)))
        {
            cx->oom = TRUE;
        }
        else
        {
            cx->occ[cx->nocc].contest = d->ncontests;
            cx->occ[cx->nocc].count = 1;
            cx->nocc++;
        }
    }
    return d->ncontests++;
}

static uint32_t rp_choice(RpCtx *cx, uint32_t contest, const wchar_t *name, const wchar_t *party)
{
    EeRpdfContest *c = &cx->d->contests[contest];
    uint32_t i;
    for (i = 0; i < c->nchoices; i++)
    {
        if (wcscmp(c->choices[i].name, name) == 0)
        {
            return i;
        }
    }
    if (!rp_grow32((void **)&c->choices, &c->cap_choices, c->nchoices + 1, sizeof(EeRpdfChoice)) ||
        (c->choices[c->nchoices].name = rp_wdup(name)) == NULL)
    {
        cx->oom = TRUE;
        return 0;
    }
    c->choices[c->nchoices].party = (party != NULL && party[0] != L'\0') ? rp_wdup(party) : NULL;
    c->choices[c->nchoices].total = EE_RPDF_UNKNOWN;
    return c->nchoices++;
}

/* A summary report's ballots cast in vote type @p vt. */
static void rp_set_bvt(RpCtx *cx, uint32_t vt, int64_t v)
{
    EeRpdfDoc *d = cx->d;
    if (vt >= cx->nbvt)
    {
        uint32_t i;
        int64_t *nb = (int64_t *)realloc(d->ballots_vt_total, ((size_t)vt + 1) * sizeof(int64_t));
        if (nb == NULL)
        {
            cx->oom = TRUE;
            return;
        }
        for (i = cx->nbvt; i <= vt; i++)
        {
            nb[i] = EE_RPDF_UNKNOWN;
        }
        d->ballots_vt_total = nb;
        cx->nbvt = vt + 1;
    }
    d->ballots_vt_total[vt] = v;
}

static void rp_count(RpCtx *cx, BOOL turnout, uint32_t contest, uint32_t choice, uint32_t vt, int64_t v)
{
    EeRpdfDoc *d = cx->d;
    EeRpdfCount **arr = turnout ? &d->turnout : &d->counts;
    size_t *n = turnout ? &d->nturnout : &d->ncounts;
    size_t *cap = turnout ? &d->cap_turnout : &d->cap_counts;
    EeRpdfCount *e;
    if (!rp_grow((void **)arr, cap, *n + 1, sizeof(EeRpdfCount)))
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

/* -------------------------------------------------------------------------- */
/* Page reading                                                               */
/* -------------------------------------------------------------------------- */

static int __cdecl rp_item_cmp(const void *pa, const void *pb)
{
    const RpItem *a = (const RpItem *)pa;
    const RpItem *b = (const RpItem *)pb;
    if (a->y != b->y)
    {
        return a->y > b->y ? -1 : 1;
    }
    return a->x < b->x ? -1 : (a->x > b->x ? 1 : 0);
}

static int __cdecl rp_ptr_x_cmp(const void *pa, const void *pb)
{
    const RpItem *a = *(const RpItem *const *)pa;
    const RpItem *b = *(const RpItem *const *)pb;
    return a->x < b->x ? -1 : (a->x > b->x ? 1 : 0);
}

/* Join the texts of @p n items with single spaces into @p out; an item that starts where
 * the one before it ends goes on the same word (Smith: "City of Linda" + "le" by the font's
 * widths, "H" + "ershal" by Helvetica's, trusted for a letter or two). */
static void rp_join(RpItem *const *it, size_t n, wchar_t *out, size_t cch)
{
    size_t i;
    out[0] = L'\0';
    for (i = 0; i < n; i++)
    {
        if (i > 0 && !((it[i - 1]->exact || wcslen(it[i - 1]->text) <= 2) && !it[i - 1]->sp_after && !it[i]->sp_before &&
                       fabsf(it[i]->x - it[i - 1]->x1) < 0.5f && fabsf(it[i]->y - it[i - 1]->y) < 1.0f))
        {
            StringCchCatW(out, cch, L" ");
        }
        StringCchCatW(out, cch, it[i]->text);
    }
    rp_norm(out);
}

/* Columns from the collected heading pieces. With @p edges (the right edges of the table's
 * numbers), a column's right edge is where its numbers end: the edges are clustered and, when
 * there is one cluster per column, taken in order; otherwise it is half way to the next
 * heading's centre. */
static void rp_build_cols(RpCtx *cx, float *edges, size_t nedges)
{
    int i, j;
    RpPiece *p = cx->pieces;
    cx->ncols = 0;
    /* pieces left to right; a piece overlapping the last column joins it */
    for (i = 1; i < cx->npieces; i++)
    {
        RpPiece t = p[i];
        for (j = i - 1; j >= 0 && p[j].x0 > t.x0; j--)
        {
            p[j + 1] = p[j];
        }
        p[j + 1] = t;
    }
    for (i = 0; i < cx->npieces; i++)
    {
        RpCol *c = cx->ncols > 0 ? &cx->cols[cx->ncols - 1] : NULL;
        if (c != NULL && p[i].x0 < c->x1 - 1.0f && p[i].x1 > c->x0 + 1.0f && c->npieces < 8)
        {
            c->pieces[c->npieces++] = i;
            if (p[i].x0 < c->x0)
            {
                c->x0 = p[i].x0;
            }
            if (p[i].x1 > c->x1)
            {
                c->x1 = p[i].x1;
            }
            continue;
        }
        if (cx->ncols >= RP_MAX_COLS)
        {
            break;
        }
        c = &cx->cols[cx->ncols++];
        ZeroMemory(c, sizeof(*c));
        c->x0 = p[i].x0;
        c->x1 = p[i].x1;
        c->pieces[c->npieces++] = i;
    }
    for (i = 0; i < cx->ncols; i++)
    {
        RpCol *c = &cx->cols[i];
        wchar_t label[256];
        float widest = 0.0f;
        int a, b;
        c->center = (c->x0 + c->x1) / 2.0f;
        /* the heading reads top line first */
        for (a = 1; a < c->npieces; a++)
        {
            int t = c->pieces[a];
            for (b = a - 1; b >= 0 && (p[c->pieces[b]].y < p[t].y - 1.0f ||
                                       (fabsf(p[c->pieces[b]].y - p[t].y) <= 1.0f && p[c->pieces[b]].x0 > p[t].x0));
                 b--)
            {
                c->pieces[b + 1] = c->pieces[b];
            }
            c->pieces[b + 1] = t;
        }
        for (a = 0; a < c->npieces; a++)
        {
            if (p[c->pieces[a]].x1 - p[c->pieces[a]].x0 > widest)
            {
                widest = p[c->pieces[a]].x1 - p[c->pieces[a]].x0;
            }
        }
        label[0] = L'\0';
        for (a = 0; a < c->npieces; a++)
        {
            const RpPiece *q = &p[c->pieces[a]];
            if (a > 0)
            {
                /* A heading wrapped inside a word ("Provision" / "al", "Mail/Corr" /
                 * "ective"): the line above fills the heading's width and this one goes on in
                 * lower case. */
                const RpPiece *prev = &p[c->pieces[a - 1]];
                size_t plen = wcslen(prev->text);
                BOOL mid_word = (prev->x1 - prev->x0 >= 0.85f * widest && iswlower(q->text[0])) ||
                                (plen > 1 && prev->text[plen - 1] == L'-' && iswalpha(prev->text[plen - 2])); /* "Day In-" / "Person" */
                if (!mid_word)
                {
                    StringCchCatW(label, ARRAYSIZE(label), L" ");
                }
            }
            StringCchCatW(label, ARRAYSIZE(label), q->text);
        }
        rp_norm(label);
        if (_wcsicmp(label, L"TOTAL") == 0 || _wcsicmp(label, L"TOTAL VOTES") == 0)
        {
            c->kind = RC_TOTAL;
        }
        else if (wcschr(label, L'%') != NULL)
        {
            c->kind = RC_PCT;
        }
        else
        {
            c->kind = RC_VT;
            c->vt = rp_vt(cx, label);
        }
    }
    /* column pitch: the median gap between heading centres */
    cx->pitch = 55.0f;
    if (cx->ncols > 1)
    {
        float gaps[RP_MAX_COLS];
        int n = cx->ncols - 1, a, b;
        for (a = 0; a < n; a++)
        {
            gaps[a] = cx->cols[a + 1].center - cx->cols[a].center;
        }
        for (a = 1; a < n; a++)
        {
            float t = gaps[a];
            for (b = a - 1; b >= 0 && gaps[b] > t; b--)
            {
                gaps[b + 1] = gaps[b];
            }
            gaps[b + 1] = t;
        }
        cx->pitch = gaps[n / 2];
    }
    for (i = 0; i < cx->ncols; i++)
    {
        cx->cols[i].anchor = i + 1 < cx->ncols ? (cx->cols[i].center + cx->cols[i + 1].center) / 2.0f
                                               : cx->cols[i].center + cx->pitch / 2.0f;
    }
    /* where the numbers end, when they make one cluster per column */
    if (nedges > 0 && cx->ncols > 0)
    {
        float cl[RP_MAX_COLS];
        int ncl = 0, k;
        size_t e, start = 0;
        for (e = 1; e < nedges; e++)
        {
            float t = edges[e];
            size_t f = e;
            while (f > 0 && edges[f - 1] > t)
            {
                edges[f] = edges[f - 1];
                f--;
            }
            edges[f] = t;
        }
        for (e = 1; e <= nedges && ncl <= RP_MAX_COLS; e++)
        {
            if (e == nedges || edges[e] - edges[e - 1] > 4.0f)
            {
                if (ncl < RP_MAX_COLS)
                {
                    float sum = 0.0f;
                    size_t f;
                    for (f = start; f < e; f++)
                    {
                        sum += edges[f];
                    }
                    cl[ncl] = sum / (float)(e - start);
                }
                ncl++;
                start = e;
            }
        }
        if (ncl == cx->ncols)
        {
            for (k = 0; k < ncl; k++)
            {
                cx->cols[k].anchor = cl[k];
            }
            if (ncl > 1)
            {
                float mg = 1e9f;
                for (k = 1; k < ncl; k++)
                {
                    if (cl[k] - cl[k - 1] < mg)
                    {
                        mg = cl[k] - cl[k - 1];
                    }
                }
                cx->pitch = mg; /* numbers stay within 0.6 of the narrowest gap */
            }
        }
    }
}

/* Cameron's ReportLab reports align each number with its heading, left edges in the contest
 * tables and right edges in Statistics: the column whose heading starts or ends where the
 * number [x0, x1] does (within 1.5 pt), or -1. */
static int rp_aligned_col(const RpCtx *cx, float x0, float x1)
{
    int k;
    for (k = 0; k < cx->ncols; k++)
    {
        if (fabsf(x0 - cx->cols[k].x0) <= 1.5f)
        {
            return k;
        }
    }
    for (k = 0; k < cx->ncols; k++)
    {
        if (fabsf(x1 - cx->cols[k].x1) <= 1.5f)
        {
            return k;
        }
    }
    return -1;
}

/* The column whose right edge is nearest @p x1 (or -1), and that distance. */
static int rp_nearest_col(const RpCtx *cx, float x1, float *dist)
{
    int best = -1, k;
    float bd = 1e9f;
    for (k = 0; k < cx->ncols; k++)
    {
        float dd = fabsf(x1 - cx->cols[k].anchor);
        if (dd < bd)
        {
            bd = dd;
            best = k;
        }
    }
    *dist = bd;
    return best;
}

/* Split "263 1,361" or "4,219 59.75% 295" into its numbers; @p kinds receives each one's
 * rp_number kind (1 count, 2 percentage). Returns how many, 0 if it is not that. */
static int rp_multi_number(const wchar_t *s, wchar_t tok[][32], int64_t *vals, int *kinds, int cap)
{
    int n = 0;
    while (*s != L'\0')
    {
        size_t len = 0;
        while (*s == L' ')
        {
            s++;
        }
        if (*s == L'\0')
        {
            break;
        }
        while (s[len] != L'\0' && s[len] != L' ')
        {
            len++;
        }
        if (n >= cap || len >= 32)
        {
            return 0;
        }
        wcsncpy_s(tok[n], 32, s, len);
        kinds[n] = rp_number(tok[n], &vals[n]);
        if (kinds[n] == 0)
        {
            return 0;
        }
        n++;
        s += len;
    }
    return n >= 2 ? n : 0;
}

/* One table row: a label and numbers. */
static void rp_row(RpCtx *cx, RpItem *const *it, size_t n)
{
    EeRpdfDoc *d = cx->d;
    wchar_t label[512];
    RpItem *lab[64];
    size_t nlab = 0, i;
    int64_t val[RP_MAX_COLS];
    BOOL has[RP_MAX_COLS];
    int total_col = -1;
    int64_t total = EE_RPDF_UNKNOWN;
    BOOL any_vt = FALSE;
    float left = cx->cols[0].x0 - 30.0f;
    BOOL in_precinct = cx->pre != UINT32_MAX;
    int k;

    for (k = 0; k < cx->ncols; k++)
    {
        has[k] = FALSE;
        val[k] = 0;
        if (cx->cols[k].kind == RC_TOTAL && total_col < 0)
        {
            total_col = k;
        }
    }
    for (i = 0; i < n; i++)
    {
        const RpItem *t = it[i];
        if (t->num == 0 && t->x >= left)
        {
            /* Electionware sometimes shows two cells as one string ("263 1,361"): the first
             * number keeps its own position, each next one goes to the next column */
            wchar_t tok[8][32];
            int64_t tv[8];
            int tk[8];
            int nt = rp_multi_number(t->text, tok, tv, tk, 8), q;
            if (nt > 0)
            {
                float bd;
                int col = rp_nearest_col(cx, t->x + rp_text_width(tok[0], t->size), &bd);
                BOOL fits = col >= 0 && bd <= cx->pitch * 0.6f && col + nt <= cx->ncols;
                for (q = 0; fits && q < nt; q++)
                {
                    fits = tk[q] != 2 || cx->cols[col + q].kind == RC_PCT; /* a % under VOTE % */
                }
                if (fits)
                {
                    for (q = 0; q < nt; q++)
                    {
                        if (cx->cols[col + q].kind != RC_PCT)
                        {
                            val[col + q] = tv[q];
                            has[col + q] = TRUE;
                            any_vt = any_vt || cx->cols[col + q].kind == RC_VT;
                        }
                    }
                    continue;
                }
            }
        }
        if (t->num == 0 || t->x < left)
        {
            if (t->num != 0 && nlab > 0 && fabsf(t->size - lab[0]->size) > 0.5f)
            {
                continue; /* Smith 2025: a stray 9 pt "183" beside an 8 pt "Total Votes Cast" */
            }
            if (nlab < ARRAYSIZE(lab))
            {
                lab[nlab++] = it[i];
            }
            continue;
        }
        {
            float bd = 0.0f;
            int best = cx->aligned ? rp_aligned_col(cx, t->x, t->x1) : -1;
            if (best < 0)
            {
                best = rp_nearest_col(cx, t->x1, &bd);
            }
            if (t->num == 2 && (best < 0 || cx->cols[best].kind != RC_PCT))
            {
                continue; /* a percentage outside a % column: turnout, derived */
            }
            if (best < 0 || bd > cx->pitch * 0.6f || (t->num == 2) != (cx->cols[best].kind == RC_PCT))
            {
                if (cx->dropped_numbers++ == 0)
                {
                    StringCchPrintfW(cx->dropped_first, ARRAYSIZE(cx->dropped_first), L"%s (%s%s%s)",
                                     t->text, cx->section == RS_STATS ? L"Statistics" : cx->title,
                                     cx->pre != UINT32_MAX ? L", precinct " : L"",
                                     cx->pre != UINT32_MAX ? cx->d->precincts[cx->pre].name : L"");
                }
                continue;
            }
            val[best] = t->value;
            has[best] = TRUE;
            if (cx->cols[best].kind == RC_VT)
            {
                any_vt = TRUE;
            }
        }
    }
    if (nlab == 0)
    {
        return;
    }
    rp_join(lab, nlab, label, ARRAYSIZE(label));
    if (total_col >= 0 && has[total_col])
    {
        total = val[total_col];
    }

    if (cx->section == RS_STATS)
    {
        const wchar_t *dash = wcsstr(label, L" - ");
        const wchar_t *suffix = dash != NULL ? dash + 3 : L"";
        BOOL is_total = _wcsicmp(suffix, L"Total") == 0;
        if (rp_starts(label, L"Registered Voters"))
        {
            if (total != EE_RPDF_UNKNOWN && (is_total || !cx->reg_total_seen))
            {
                if (in_precinct)
                {
                    d->precincts[cx->pre].registered = total;
                }
                else
                {
                    d->registered_total = total;
                }
                cx->reg_total_seen = cx->reg_total_seen || is_total;
            }
        }
        else if (rp_starts(label, L"Ballots Cast") && _wcsicmp(suffix, L"Blank") != 0)
        {
            if (is_total || !cx->bal_total_seen)
            {
                if (in_precinct)
                {
                    size_t keep = d->nturnout;
                    /* a "- Total" row replaces a party row read before it */
                    while (keep > 0 && d->turnout[keep - 1].pre == cx->pre)
                    {
                        keep--;
                    }
                    d->nturnout = keep;
                    if (total != EE_RPDF_UNKNOWN)
                    {
                        d->precincts[cx->pre].ballots = total;
                    }
                }
                else if (total != EE_RPDF_UNKNOWN)
                {
                    d->ballots_total = total;
                }
                for (k = 0; k < cx->ncols; k++)
                {
                    if (has[k] && cx->cols[k].kind == RC_VT)
                    {
                        if (in_precinct)
                        {
                            rp_count(cx, TRUE, 0, 0, cx->cols[k].vt, val[k]);
                        }
                        else
                        {
                            rp_set_bvt(cx, cx->cols[k].vt, val[k]);
                        }
                    }
                }
                cx->bal_total_seen = cx->bal_total_seen || is_total;
            }
        }
        return;
    }
    if (cx->section != RS_CONTEST)
    {
        return;
    }
    if (cx->title_pending)
    {
        cx->contest = rp_contest(cx, cx->title, cx->vote_for);
        if (rp_grow((void **)&cx->seq, &cx->cap_seq, cx->nseq + 1, sizeof(uint32_t)))
        {
            cx->seq[cx->nseq++] = cx->contest;
        }
        else
        {
            cx->oom = TRUE;
        }
        cx->title_pending = FALSE;
        cx->have_contest = !cx->oom;
    }
    if (!cx->have_contest)
    {
        return;
    }
    {
        EeRpdfContest *c = &d->contests[cx->contest];
        BOOL indented = lab[0]->x > 26.0f;
        if (_wcsicmp(label, L"Total Votes Cast") == 0 || _wcsicmp(label, L"Total Votes") == 0)
        {
            return;
        }
        if (_wcsicmp(label, L"Overvotes") == 0 || _wcsicmp(label, L"Undervotes") == 0)
        {
            BOOL over = _wcsicmp(label, L"Overvotes") == 0;
            c->has_over_under = TRUE;
            if (total == EE_RPDF_UNKNOWN)
            {
                return;
            }
            if (in_precinct)
            {
                rp_count(cx, FALSE, cx->contest, over ? EE_RPDF_OVER : EE_RPDF_UNDER, EE_RPDF_NO_VT, total);
            }
            else if (over)
            {
                c->over = total;
            }
            else
            {
                c->under = total;
            }
            return;
        }
        if (_wcsicmp(label, L"Contest Totals") == 0)
        {
            if (!in_precinct)
            {
                c->ballots = total;
            }
            return;
        }
        if (rp_starts(label, L"Write-In:") || (indented && _wcsicmp(label, L"Not Assigned") == 0))
        {
            cx->writein_rows++;
            return;
        }
        {
            wchar_t name[512];
            wchar_t party[8] = L"";
            uint32_t ch;
            const wchar_t *sp = wcschr(label, L' ');
            if (_wcsicmp(label, L"Write-In Totals") == 0)
            {
                StringCchCopyW(name, ARRAYSIZE(name), L"Write-in");
            }
            else if (sp != NULL && rp_party_code(label, (size_t)(sp - label)))
            {
                /* the name keeps its party code, as the ES&S CVR writes it ("REP Donald J.
                 * Trump"); the code is also the choice's party */
                StringCchCopyNW(party, ARRAYSIZE(party), label, (size_t)(sp - label));
                StringCchCopyW(name, ARRAYSIZE(name), label);
            }
            else
            {
                StringCchCopyW(name, ARRAYSIZE(name), label);
            }
            ch = rp_choice(cx, cx->contest, name, party);
            if (cx->oom)
            {
                return;
            }
            c = &d->contests[cx->contest];
            if (!in_precinct)
            {
                c->choices[ch].total = total;
                return;
            }
            if (any_vt)
            {
                for (k = 0; k < cx->ncols; k++)
                {
                    if (has[k] && cx->cols[k].kind == RC_VT)
                    {
                        rp_count(cx, FALSE, cx->contest, ch, cx->cols[k].vt, val[k]);
                    }
                }
            }
            else if (total != EE_RPDF_UNKNOWN)
            {
                rp_count(cx, FALSE, cx->contest, ch, EE_RPDF_NO_VT, total);
            }
        }
    }
}

enum
{
    RL_TEXT,     /* words only: perhaps a contest title */
    RL_DATA,     /* a row: a label and numbers */
    RL_VOTEFOR,  /* "Vote For N" */
    RL_STATS,    /* "STATISTICS" */
    RL_HEAD,     /* the column heading line with TOTAL */
    RL_HEADPART, /* another line of a column heading (owner: its RL_HEAD line) */
    RL_CONT,     /* a row's label going on (owner: its RL_DATA line) */
    RL_EMPTY     /* nothing left (stray heading words dropped) */
};

typedef struct RpLine
{
    size_t first, n; /* items in the line pool, left to right */
    float y;
    int kind;
    int owner;
    BOOL num;
    float minx, maxx;
    float total_x; /* RL_HEAD: where TOTAL starts */
    float num_x;   /* RL_DATA: where the first number starts */
} RpLine;

/* Start a contest from a title (the line(s) before "Vote For", or before a heading). */
static void rp_start_contest(RpCtx *cx, const wchar_t *title, uint32_t vote_for)
{
    StringCchCopyW(cx->title, ARRAYSIZE(cx->title), title);
    cx->title_pending = TRUE;
    cx->have_contest = FALSE;
    cx->vote_for = vote_for;
    cx->section = RS_CONTEST;
    cx->ncols = 0;
}

/* The body of an Electionware page, told apart by structure rather than font sizes (the
 * templates differ: 12/10/8 pt in Travis 2020-22, 9/7/8 pt later): a contest title is the
 * line before "Vote For N"; a column heading is the line holding TOTAL, with the lines of a
 * wrapped heading above and below it; a row has numbers; a label that wraps goes on below
 * its row. */
static void rp_body(RpCtx *cx, RpItem *items, size_t n)
{
    size_t *line_of = (size_t *)malloc((n + 1) * sizeof(size_t));
    size_t *attach = (size_t *)malloc((n + 1) * sizeof(size_t));
    float *ly = (float *)malloc((n + 1) * sizeof(float));
    uint8_t *lab = (uint8_t *)calloc(n + 1, 1);
    uint8_t *allnum = (uint8_t *)calloc(n + 1, 1);
    RpItem **pool = (RpItem **)malloc((n + 1) * sizeof(RpItem *));
    RpItem **row = (RpItem **)malloc((n + 1) * sizeof(RpItem *));
    float *edges = (float *)malloc((n + 1) * sizeof(float));
    RpLine *L = (RpLine *)calloc(n + 1, sizeof(RpLine));
    size_t nraw = 0, nl = 0, used = 0, i, k;
    wchar_t text[1024];
    BOOL cols_fresh = FALSE;

    if (line_of == NULL || attach == NULL || ly == NULL || lab == NULL || allnum == NULL || pool == NULL ||
        row == NULL || edges == NULL || L == NULL)
    {
        cx->oom = TRUE;
        goto out;
    }
    /* raw lines; a line of numbers only joins the nearest labelled line */
    for (i = 0; i < n; i++)
    {
        if (nraw == 0 || items[i].y < ly[nraw - 1] - RP_LINE_TOL)
        {
            ly[nraw] = items[i].y;
            lab[nraw] = 0;
            allnum[nraw] = 1;
            nraw++;
        }
        line_of[i] = nraw - 1;
        if (items[i].num == 0 && items[i].multi == 0)
        {
            lab[nraw - 1] = 1;
            allnum[nraw - 1] = 0;
        }
    }
    for (k = 0; k < nraw; k++)
    {
        attach[k] = k;
        if (allnum[k])
        {
            size_t best = SIZE_MAX, j;
            float bd = RP_ATTACH_TOL + 0.01f;
            for (j = 0; j < nraw; j++)
            {
                float dd = fabsf(ly[j] - ly[k]);
                if (j != k && lab[j] && dd < bd)
                {
                    bd = dd;
                    best = j;
                }
            }
            if (best != SIZE_MAX)
            {
                attach[k] = best;
            }
        }
    }
    for (k = 0; k < nraw; k++)
    {
        RpLine *ln;
        if (attach[k] != k)
        {
            continue;
        }
        ln = &L[nl++];
        ln->first = used;
        ln->y = ly[k];
        ln->owner = -1;
        ln->minx = 1e9f;
        ln->maxx = -1e9f;
        ln->num_x = 1e9f;
        for (i = 0; i < n; i++)
        {
            if (attach[line_of[i]] == k)
            {
                pool[used++] = &items[i];
            }
        }
        ln->n = used - ln->first;
        qsort(pool + ln->first, ln->n, sizeof(RpItem *), rp_ptr_x_cmp);
    }
    /* kinds */
    for (k = 0; k < nl; k++)
    {
        RpLine *ln = &L[k];
        for (i = 0; i < ln->n; i++)
        {
            const RpItem *t = pool[ln->first + i];
            if (t->x < ln->minx)
            {
                ln->minx = t->x;
            }
            if (t->x1 > ln->maxx)
            {
                ln->maxx = t->x1;
            }
            if (t->num != 0 || t->multi != 0)
            {
                ln->num = TRUE;
                if (t->x < ln->num_x)
                {
                    ln->num_x = t->x;
                }
            }
            if (t->num == 0 && _wcsicmp(t->text, L"TOTAL") == 0 && t->x >= 100.0f && ln->kind != RL_HEAD)
            {
                ln->kind = RL_HEAD;
                ln->total_x = t->x;
            }
        }
        rp_join(pool + ln->first, ln->n, text, ARRAYSIZE(text));
        if (ln->kind == RL_HEAD && ln->num)
        {
            ln->kind = RL_DATA;
        }
        else if (ln->kind == RL_HEAD)
        {
            continue;
        }
        else if (rp_starts(text, L"Vote For ") && iswdigit(text[9]))
        {
            ln->kind = RL_VOTEFOR;
        }
        else if (_wcsicmp(text, L"STATISTICS") == 0)
        {
            ln->kind = RL_STATS;
        }
        else
        {
            ln->kind = ln->num ? RL_DATA : RL_TEXT;
        }
    }
    /* Smith 2025 has stray 9 pt "TOTAL" and "VOTE %" beside a contest's title, not shown on
     * the page; column headings come after "Vote For", so heading words in the two lines
     * above it are dropped */
    for (k = 1; k < nl; k++)
    {
        size_t up;
        if (L[k].kind != RL_VOTEFOR)
        {
            continue;
        }
        for (up = k; up > 0 && up + 2 > k && L[up - 1].y - L[k].y <= 20.0f; up--)
        {
            RpLine *ln = &L[up - 1];
            size_t m = 0;
            if (ln->kind != RL_HEAD && ln->kind != RL_TEXT)
            {
                continue;
            }
            for (i = 0; i < ln->n; i++)
            {
                RpItem *t = pool[ln->first + i];
                if (_wcsicmp(t->text, L"TOTAL") != 0 && _wcsicmp(t->text, L"VOTE %") != 0)
                {
                    pool[ln->first + m++] = t;
                }
            }
            if (m != ln->n)
            {
                ln->n = m;
                ln->kind = m == 0 ? RL_EMPTY : RL_TEXT;
                ln->minx = 1e9f;
                ln->maxx = -1e9f;
                for (i = 0; i < m; i++)
                {
                    ln->minx = fminf(ln->minx, pool[ln->first + i]->x);
                    ln->maxx = fmaxf(ln->maxx, pool[ln->first + i]->x1);
                }
            }
        }
    }
    /* the other lines of each column heading */
    for (k = 0; k < nl; k++)
    {
        size_t j, last;
        float tx;
        if (L[k].kind != RL_HEAD)
        {
            continue;
        }
        tx = L[k].total_x - 10.0f;
        /* "Election" above "TOTAL ... Day"; Williamson stacks two lines above ("Election" over
         * "Early Voting ... Day In-"), Bowie sets "Late" (over "Mail/Correcti") 11.7 pt above.
         * A title starts left of TOTAL, so it is never taken. */
        for (j = k; j > 0 && L[j - 1].kind == RL_TEXT && L[j - 1].y - L[j].y <= 13.0f && L[j - 1].minx >= tx; j--)
        {
            L[j - 1].kind = RL_HEADPART;
            L[j - 1].owner = (int)k;
        }
        last = k;
        for (j = k + 1; j < nl && L[k].y - L[j].y <= 45.0f; j++)
        {
            if (L[last].y - L[j].y > 13.0f)
            {
                break;
            }
            if (L[j].kind == RL_TEXT && L[j].minx >= tx)
            {
                L[j].kind = RL_HEADPART;
                L[j].owner = (int)k;
                last = j;
            }
            else if ((L[j].kind == RL_TEXT || L[j].kind == RL_STATS) && L[j].maxx < tx)
            {
                continue; /* "Statistics" printed beside the heading */
            }
            else
            {
                break;
            }
        }
    }
    /* a row label going on below its row (not a title: a title is followed by "Vote For") */
    for (k = 1; k < nl; k++)
    {
        size_t prev = k - 1;
        if (L[k].kind != RL_TEXT)
        {
            continue;
        }
        while (prev > 0 && L[prev].kind == RL_CONT)
        {
            prev--;
        }
        if (L[prev].kind == RL_DATA && L[k - 1].y - L[k].y <= 12.0f && L[k].maxx < L[prev].num_x - 5.0f &&
            !(k + 1 < nl && L[k + 1].kind == RL_VOTEFOR))
        {
            L[k].kind = RL_CONT;
            L[k].owner = (int)prev;
        }
    }
    /* walk the lines */
    for (k = 0; k < nl && !cx->oom; k++)
    {
        RpLine *ln = &L[k];
        rp_join(pool + ln->first, ln->n, text, ARRAYSIZE(text));
        switch (ln->kind)
        {
            case RL_TEXT:
                if (cx->ptitle[0] != L'\0' && k > 0 && L[k - 1].kind == RL_TEXT && L[k - 1].y - ln->y <= 15.0f)
                {
                    StringCchCatW(cx->ptitle, ARRAYSIZE(cx->ptitle), L" ");
                    StringCchCatW(cx->ptitle, ARRAYSIZE(cx->ptitle), text); /* a wrapped title */
                }
                else
                {
                    StringCchCopyW(cx->ptitle, ARRAYSIZE(cx->ptitle), text);
                }
                break;
            case RL_VOTEFOR:
                cx->structure = TRUE;
                if (cx->ptitle[0] != L'\0')
                {
                    rp_start_contest(cx, cx->ptitle, (uint32_t)_wtoi(text + 9));
                    cx->ptitle[0] = L'\0';
                }
                else if (cx->section == RS_CONTEST && cx->title_pending)
                {
                    cx->vote_for = (uint32_t)_wtoi(text + 9);
                }
                cols_fresh = FALSE;
                break;
            case RL_STATS:
                cx->structure = TRUE;
                cx->section = RS_STATS;
                cx->ptitle[0] = L'\0';
                /* a new precinct section: a precinct name can come back in a later section
                 * (Bexar), whose contests add to the same ones */
                cx->nocc = 0;
                if (!cols_fresh)
                {
                    cx->ncols = 0; /* its heading follows */
                }
                break;
            case RL_HEAD:
            {
                size_t j, ne = 0;
                if (cx->aligned && ln->n > 0 && _wcsicmp(pool[ln->first]->text, L"Statistics") == 0 &&
                    pool[ln->first]->x < ln->total_x - 10.0f)
                {
                    /* Cameron 2026: "Statistics" at 8 pt on its heading's own line */
                    cx->structure = TRUE;
                    cx->section = RS_STATS;
                    cx->ptitle[0] = L'\0';
                    cx->nocc = 0;
                }
                /* a title with no "Vote For" line just before the heading */
                if (cx->ptitle[0] != L'\0' && k > 0 &&
                    (L[k - 1].kind == RL_TEXT || (L[k - 1].kind == RL_HEADPART && k > 1 && L[k - 2].kind == RL_TEXT)))
                {
                    rp_start_contest(cx, cx->ptitle, 0);
                }
                cx->ptitle[0] = L'\0';
                cx->npieces = 0;
                for (j = 0; j < nl; j++)
                {
                    if (j == k || (L[j].kind == RL_HEADPART && L[j].owner == (int)k))
                    {
                        for (i = 0; i < L[j].n && cx->npieces < RP_MAX_PIECES; i++)
                        {
                            const RpItem *t = pool[L[j].first + i];
                            RpPiece *pc;
                            if (t->x < ln->total_x - 10.0f)
                            {
                                continue; /* left of TOTAL: not a heading (Smith's stray "Statistics") */
                            }
                            pc = &cx->pieces[cx->npieces++];
                            pc->x0 = t->x;
                            pc->x1 = t->x1;
                            pc->y = t->y;
                            StringCchCopyW(pc->text, ARRAYSIZE(pc->text), t->text);
                        }
                    }
                }
                /* where the table's numbers end */
                for (j = k + 1; j < nl; j++)
                {
                    if (L[j].kind == RL_HEAD || L[j].kind == RL_VOTEFOR || L[j].kind == RL_STATS)
                    {
                        break;
                    }
                    if (L[j].kind != RL_DATA)
                    {
                        continue;
                    }
                    for (i = 0; i < L[j].n; i++)
                    {
                        const RpItem *t = pool[L[j].first + i];
                        if (t->num != 0 && t->x >= ln->total_x - 40.0f)
                        {
                            edges[ne++] = t->x1;
                        }
                    }
                }
                rp_build_cols(cx, edges, ne);
                cols_fresh = TRUE;
                break;
            }
            case RL_DATA:
            {
                size_t m = 0, j;
                cx->ptitle[0] = L'\0';
                cols_fresh = FALSE;
                if (cx->section == RS_NONE || cx->ncols == 0)
                {
                    break;
                }
                for (i = 0; i < ln->n; i++)
                {
                    row[m++] = pool[ln->first + i];
                }
                for (j = k + 1; j < nl && L[j].kind == RL_CONT; j++)
                {
                    for (i = 0; i < L[j].n; i++)
                    {
                        row[m++] = pool[L[j].first + i];
                    }
                }
                rp_row(cx, row, m);
                break;
            }
            default:
                break;
        }
    }
out:
    free(line_of);
    free(attach);
    free(ly);
    free(lab);
    free(allnum);
    free(pool);
    free(row);
    free(edges);
    free(L);
}

/* A report status ("Final Unofficial", "OFFICIAL RESULTS"): only status words. */
static BOOL rp_is_status(const wchar_t *s)
{
    static const wchar_t *const k[] = {L"Final", L"Official", L"Unofficial", L"Results", L"Preliminary", L"Semi-Official"};
    BOOL any = FALSE;
    while (*s != L'\0')
    {
        size_t len = 0, i;
        BOOL known = FALSE;
        while (*s == L' ')
        {
            s++;
        }
        while (s[len] != L'\0' && s[len] != L' ')
        {
            len++;
        }
        if (len == 0)
        {
            break;
        }
        for (i = 0; i < ARRAYSIZE(k) && !known; i++)
        {
            known = wcslen(k[i]) == len && _wcsnicmp(s, k[i], len) == 0;
        }
        if (!known)
        {
            return FALSE;
        }
        any = TRUE;
        s += len;
    }
    return any;
}

/* The page heading's election name, date and county (first page). */
static void rp_heading(RpCtx *cx, RpItem *const *it, size_t n)
{
    EeRpdfDoc *d = cx->d;
    const wchar_t *election = NULL, *party = NULL;
    size_t i;
    for (i = 0; i < n; i++)
    {
        const wchar_t *s = it[i]->text;
        size_t len = wcslen(s);
        if (rp_is_date(s))
        {
            if (d->election_date == NULL)
            {
                d->election_date = rp_wdup(s);
            }
        }
        else if (wcsstr(s, L"County") != NULL && wcsstr(s, L"Election") == NULL && wcsstr(s, L" Joint ") == NULL &&
                 wcsstr(s, L" General") == NULL && wcsstr(s, L" Special") == NULL && wcsstr(s, L" Primary") == NULL &&
                 wcsstr(s, L" Runoff") == NULL)
        {
            /* (Williamson's election line "May Primary Run-Off Elections of Williamson County,
             * TX" names the county too, and Travis's cut-off "Travis County Joint General and
             * Special") */
            if (d->region == NULL)
            {
                d->region = rp_wdup(s);
            }
        }
        else if (rp_is_status(s) || wcsstr(s, L"Report") != NULL || wcsstr(s, L"Canvass") != NULL ||
                 (len >= 7 && _wcsicmp(s + len - 7, L"Results") == 0))
        {
            continue; /* the report's own title or status */
        }
        else if (len >= 6 && wcscmp(s + len - 6, L" Party") == 0)
        {
            party = s;
        }
        else if (election == NULL || (wcsstr(election, L"Election") == NULL && wcsstr(s, L"Election") != NULL))
        {
            election = s;
        }
    }
    if (election != NULL && d->election_name == NULL)
    {
        wchar_t buf[512];
        if (party != NULL)
        {
            StringCchPrintfW(buf, ARRAYSIZE(buf), L"%s (%s)", election, party);
        }
        else
        {
            StringCchCopyW(buf, ARRAYSIZE(buf), election);
        }
        d->election_name = rp_wdup(buf);
    }
}

/* The page's text runs as items (rotated runs only with @p keep_rot), sorted top down.
 * pdf_reader does not move the text position after a string is shown, so strings shown one
 * after another come at the same point (Cameron's "104" "," "443", numbers edited in Acrobat;
 * Smith's "TOTAL " " VOTE % " and "R." " Wade Washmon"): such runs are joined first, spaces
 * kept, and then read as one string. */
static size_t rp_items(RpCtx *cx, const EePdfPageText *pt, RpItem *items, wchar_t *pool, BOOL keep_rot)
{
    size_t nitems = 0, i, poolpos = 0;
    size_t cap = pt->text_len + pt->nruns; /* pool capacity before the final NUL */
    wchar_t tok[8][32];
    int64_t tv[8];
    int tk[8];
    for (i = 0; i < pt->nruns; i++)
    {
        const EePdfTextRun *r = &pt->runs[i];
        size_t len = 0, last_off = 0;
        BOOL has_width = r->has_width, sp_before, sp_after;
        float width = r->width, endx;
        RpItem *t;
        float w;
        int wn;
        if ((r->rot != 0 && !(keep_rot && r->rot == 90)) || r->text_len == 0)
        {
            continue;
        }
        wn = MultiByteToWideChar(CP_UTF8, 0, pt->text + r->text_off, (int)r->text_len, pool + poolpos,
                                 (int)(cap - poolpos));
        if (wn <= 0)
        {
            continue;
        }
        len = (size_t)wn;
        endx = r->x + (r->has_width ? r->width : rp_text_width_n(pool + poolpos, len, r->font_size));
        /* the runs shown after it from the same point (not a repeat of the same text), or
         * starting where it ends in the same size ("104" "," "443" placed one after another) */
        while (i + 1 < pt->nruns && !cx->hart) /* (Hart reports are read as in step 8) */
        {
            const EePdfTextRun *n = &pt->runs[i + 1];
            BOOL same = fabsf(n->x - r->x) < 0.05f;
            /* a glyph outline's width is measured from its drawing: near enough */
            float touch = r->type3 && n->type3 ? 0.3f * r->font_size : 0.5f;
            int nn;
            if (n->text_len == 0 || n->rot != r->rot || fabsf(n->y - r->y) >= 0.05f ||
                !(same || (fabsf(n->x - endx) < touch && fabsf(n->font_size - r->font_size) < 0.5f)))
            {
                break;
            }
            nn = MultiByteToWideChar(CP_UTF8, 0, pt->text + n->text_off, (int)n->text_len, pool + poolpos + len,
                                     (int)(cap - poolpos - len));
            if (nn <= 0)
            {
                break;
            }
            /* touching joins the pieces of a number, or pieces with a space between them
             * (Cameron's "2" " of" " 2"); Bowie's "Early" and "Voting" touch by their widths
             * (the space is in "Early"'s) and stay two words */
            if (!same && !(rp_numeric_chars(pool + poolpos, len) && rp_numeric_chars(pool + poolpos + len, (size_t)nn)) &&
                !rp_ws(pool[poolpos + len - 1]) && !rp_ws(pool[poolpos + len]) &&
                !(r->type3 && n->type3)) /* (glyph outlines are shown a letter or two at a time) */
            {
                break;
            }
            if (same && (size_t)nn == len - last_off &&
                memcmp(pool + poolpos + last_off, pool + poolpos + len, (size_t)nn * sizeof(wchar_t)) == 0)
            {
                break; /* the same text again: drawn twice, its own item as before */
            }
            endx = n->x + (n->has_width ? n->width : rp_text_width_n(pool + poolpos + len, (size_t)nn, n->font_size));
            last_off = len;
            len += (size_t)nn;
            has_width = has_width && n->has_width;
            width = endx - r->x;
            i++;
        }
        pool[poolpos + len] = L'\0';
        if (!cx->hart && !cx->hart_old && r->rot == 0 && r->x >= 100.0f && rp_split_cells(pool + poolpos))
        {
            /* Smith prints a contest's column headings as one string ("TOTAL  VOTE %  Absentee
             * Early  Election", then "Voting  Day"): cells are 2+ spaces apart, each is an item
             * at its place measured in Helvetica (rows of numbers stay one item, see multi) */
            const wchar_t *raw = pool + poolpos;
            size_t rd = 0, wr = poolpos;
            float xoff = 0.0f;
            while (raw[rd] != L'\0' && rp_ws(raw[rd]))
            {
                xoff += rp_char_width(L' ', r->font_size);
                rd++;
            }
            while (raw[rd] != L'\0')
            {
                size_t s0 = rd, e, slen;
                float segw = 0.0f, gap = 0.0f;
                while (raw[rd] != L'\0' && !(rp_ws(raw[rd]) && rp_ws(raw[rd + 1])))
                {
                    segw += rp_char_width(raw[rd], r->font_size);
                    rd++;
                }
                e = rd;
                while (raw[rd] != L'\0' && rp_ws(raw[rd]))
                {
                    gap += rp_char_width(L' ', r->font_size);
                    rd++;
                }
                /* the copy and its NUL end at or before e, behind the reader (now past the gap) */
                memmove(pool + wr, raw + s0, (e - s0) * sizeof(wchar_t));
                pool[wr + (e - s0)] = L'\0';
                rp_norm(pool + wr);
                slen = wcslen(pool + wr);
                if (slen > 0)
                {
                    t = &items[nitems++];
                    t->text = pool + wr;
                    t->x = r->x + xoff;
                    t->y = r->y;
                    t->size = r->font_size;
                    t->x1 = t->x + segw;
                    t->exact = FALSE;
                    t->sp_before = t->sp_after = TRUE;
                    t->num = rp_number(t->text, &t->value);
                    t->multi = 0;
                    t->rot = r->rot;
                    wr += slen + 1;
                }
                xoff += segw + gap;
            }
            poolpos = wr;
            cx->any_text = TRUE;
            continue;
        }
        sp_before = rp_ws(pool[poolpos]);
        sp_after = len > 0 && rp_ws(pool[poolpos + len - 1]);
        rp_norm(pool + poolpos);
        if (pool[poolpos] == L'\0')
        {
            continue;
        }
        w = has_width ? width : rp_text_width(pool + poolpos, r->font_size);
        t = &items[nitems++];
        t->sp_before = sp_before;
        t->sp_after = sp_after;
        t->text = pool + poolpos;
        poolpos += wcslen(pool + poolpos) + 1;
        t->x = r->x;
        t->y = r->y;
        t->size = r->font_size;
        t->x1 = r->x + w;
        t->exact = has_width;
        t->num = rp_number(t->text, &t->value);
        t->multi = t->num == 0 ? rp_multi_number(t->text, tok, tv, tk, 8) : 0;
        t->rot = r->rot;
        cx->any_text = TRUE;
    }
    qsort(items, nitems, sizeof(RpItem), rp_item_cmp);
    return nitems;
}

static void rp_page(RpCtx *cx, const EePdfPageText *pt, BOOL first_page)
{
    RpItem *items = NULL;
    RpItem **ptrs = NULL;
    wchar_t *pool = NULL;
    size_t nitems = 0, i;

    if (pt->nruns == 0)
    {
        return;
    }
    /* a run split into cells (rp_items) gives up to one item per 3 characters */
    items = (RpItem *)malloc((pt->nruns + pt->text_len / 3 + 1) * sizeof(RpItem));
    ptrs = (RpItem **)malloc((pt->nruns + pt->text_len / 3 + 1) * sizeof(RpItem *));
    pool = (wchar_t *)malloc((pt->text_len + pt->nruns + 1) * sizeof(wchar_t));
    if (items == NULL || ptrs == NULL || pool == NULL)
    {
        cx->oom = TRUE;
        goto done;
    }
    nitems = rp_items(cx, pt, items, pool, FALSE);

    /* footer, page heading and precinct line */
    {
        size_t nhead = 0, npre = 0, j;
        float ly[24];
        int nl = 0, l;
        BOOL skip_body = FALSE; /* a page of results by polling place */
        float hsize = 0.0f, top_y = 0.0f;
        float pre_lo = 1e9f, pre_hi = -1e9f; /* the precinct line's y range */
        float head_lo = 1e9f;               /* the heading's lowest line */
        for (i = 0; i < nitems; i++)
        {
            if (items[i].y < RP_FOOTER_Y && wcsstr(items[i].text, L"Electionware") != NULL)
            {
                cx->electionware = TRUE;
            }
        }
        /* the heading: the topmost run's size, in lines near the top */
        for (i = 0; i < nitems; i++)
        {
            if (items[i].y >= RP_FOOTER_Y)
            {
                hsize = items[i].size;
                top_y = items[i].y;
                break;
            }
        }
        for (i = 0; i < nitems; i++)
        {
            if (items[i].y >= RP_FOOTER_Y && items[i].y >= top_y - 100.0f && fabsf(items[i].size - hsize) < 0.6f &&
                (nl == 0 || items[i].y < ly[nl - 1] - 3.0f) && nl < 24)
            {
                ly[nl++] = items[i].y;
            }
        }
        /* Travis 2020-22: a last heading-size line set further below the one above it (about
         * 27 pt) than the heading's lines are apart (14-20 pt), alone at the left, names the
         * precinct (a heading line after a blank one has the county at the right) */
        for (l = 1; l < nl; l++)
        {
            if (ly[l - 1] - ly[l] > 23.0f && l == nl - 1)
            {
                BOOL left_only = TRUE;
                for (i = 0; i < nitems; i++)
                {
                    if (items[i].y >= RP_FOOTER_Y && fabsf(items[i].size - hsize) < 0.6f && fabsf(items[i].y - ly[l]) < 3.0f &&
                        items[i].x > 250.0f)
                    {
                        left_only = FALSE;
                    }
                }
                if (left_only)
                {
                    pre_lo = ly[l] - 3.0f;
                    pre_hi = ly[l] + 3.0f;
                    nl = l;
                }
            }
        }
        if (nl > 0)
        {
            head_lo = ly[nl - 1] - 3.0f;
        }
        for (i = 0; i < nitems; i++)
        {
            if (items[i].y >= RP_FOOTER_Y && items[i].y >= head_lo && fabsf(items[i].size - hsize) < 0.6f &&
                !(items[i].y >= pre_lo && items[i].y <= pre_hi))
            {
                ptrs[nhead++] = &items[i];
            }
        }
        if (first_page && nhead > 0)
        {
            rp_heading(cx, ptrs, nhead);
        }
        /* the newer template: the first line below the heading, in a larger font than the
         * heading, alone at the left ("Precinct 1", "1001 BS 1"; not "Statistics") */
        if (pre_hi < 0.0f)
        {
            for (i = 0; i < nitems; i++)
            {
                if (items[i].y >= RP_FOOTER_Y && items[i].y < head_lo &&
                    !(fabsf(items[i].size - hsize) < 0.6f && items[i].y >= top_y - 100.0f))
                {
                    break; /* items[i]: the first item of the body */
                }
            }
            /* (Cameron's ReportLab reports name precincts reported together "1,59,83") */
            if (i < nitems && items[i].size > hsize + 0.6f &&
                (items[i].num == 0 || (cx->aligned && rp_number_list(items[i].text))))
            {
                float y0 = items[i].y;
                BOOL ok = TRUE;
                for (j = 0; j < nitems; j++)
                {
                    if (fabsf(items[j].y - y0) < RP_LINE_TOL &&
                        (items[j].x > 250.0f || (items[j].num != 0 && !(cx->aligned && rp_number_list(items[j].text))) ||
                                                                 _wcsicmp(items[j].text, L"Statistics") == 0))
                    {
                        ok = FALSE;
                    }
                }
                if (ok)
                {
                    pre_lo = y0 - RP_LINE_TOL;
                    pre_hi = y0 + RP_LINE_TOL;
                }
            }
        }
        if (first_page && cx->d->timestamp == NULL)
        {
            /* "Election Summary - 11/12/2020    1:41 PM" */
            for (i = 0; i < nitems; i++)
            {
                const wchar_t *dash;
                if (items[i].y < RP_FOOTER_Y && items[i].x < 100.0f && (dash = wcsstr(items[i].text, L" - ")) != NULL &&
                    wcschr(dash, L'/') != NULL)
                {
                    cx->d->timestamp = rp_wdup(dash + 3);
                    break;
                }
            }
        }
        for (i = 0; i < nitems; i++)
        {
            if (items[i].y >= RP_FOOTER_Y && items[i].y >= pre_lo && items[i].y <= pre_hi)
            {
                ptrs[npre++] = &items[i];
            }
        }
        {
            wchar_t name[512] = L"";
            if (npre > 0)
            {
                qsort(ptrs, npre, sizeof(RpItem *), rp_ptr_x_cmp);
                rp_join(ptrs, npre, name, ARRAYSIZE(name));
            }
            else
            {
                /* Williamson 2020-21 print results by polling place first, headed "Precinct 119 -
                 * Results" at the right with the county's registered voters (its vote centres
                 * take any precinct's voters), then the precinct pages: not read */
                for (i = 0; i < nitems; i++)
                {
                    size_t len = wcslen(items[i].text);
                    if (items[i].y >= RP_FOOTER_Y && items[i].y >= head_lo && fabsf(items[i].size - hsize) < 0.6f &&
                        items[i].x > 250.0f && len > 10 && _wcsicmp(items[i].text + len - 10, L" - Results") == 0)
                    {
                        if (cx->place_pages++ == 0)
                        {
                            StringCchCopyW(cx->place_first, ARRAYSIZE(cx->place_first), items[i].text);
                        }
                        skip_body = TRUE;
                        break;
                    }
                }
            }
            if (cx->aligned && _wcsicmp(name, L"Summary") == 0)
            {
                name[0] = L'\0'; /* Cameron 2026: a summary report's "precinct" line */
            }
            if (name[0] != L'\0' && (cx->pre == UINT32_MAX || wcscmp(cx->d->precincts[cx->pre].name, name) != 0))
            {
                cx->pre = rp_precinct(cx, name);
                cx->nocc = 0;
                if (rp_grow((void **)&cx->seq, &cx->cap_seq, cx->nseq + 1, sizeof(uint32_t)))
                {
                    cx->seq[cx->nseq++] = UINT32_MAX;
                }
                else
                {
                    cx->oom = TRUE;
                }
                cx->reg_total_seen = FALSE;
                cx->bal_total_seen = FALSE;
            }
        }
        /* the body: what is left above the footer */
        if (!skip_body)
        {
            size_t nb = 0;
            for (i = 0; i < nitems; i++)
            {
                BOOL head = items[i].y >= head_lo && fabsf(items[i].size - hsize) < 0.6f;
                BOOL pre = items[i].y >= pre_lo && items[i].y <= pre_hi;
                if (items[i].y >= RP_FOOTER_Y && !head && !pre)
                {
                    items[nb++] = items[i]; /* compact in place (order kept) */
                }
            }
            rp_body(cx, items, nb);
        }
    }

done:
    free(items);
    free(ptrs);
    free(pool);
}

/* Natural order of two titles: digit runs compare as numbers ("District 9" < "District 10"). */
static int rp_natural_cmp(const wchar_t *a, const wchar_t *b)
{
    while (*a != L'\0' && *b != L'\0')
    {
        if (iswdigit(*a) && iswdigit(*b))
        {
            unsigned long long x = 0, y = 0;
            while (iswdigit(*a))
            {
                x = x * 10 + (unsigned long long)(*a++ - L'0');
            }
            while (iswdigit(*b))
            {
                y = y * 10 + (unsigned long long)(*b++ - L'0');
            }
            if (x != y)
            {
                return x < y ? -1 : 1;
            }
            continue;
        }
        if (towlower(*a) != towlower(*b))
        {
            return towlower(*a) < towlower(*b) ? -1 : 1;
        }
        a++;
        b++;
    }
    return *a == L'\0' ? (*b == L'\0' ? 0 : -1) : 1;
}

/* -------------------------------------------------------------------------- */
/* Hart Verity reports                                                        */
/* -------------------------------------------------------------------------- */

enum
{
    HCOL_CHOICE,
    HCOL_SKIP,
    HCOL_UNDER,
    HCOL_OVER,
    HCOL_REG,
    HCOL_BALLOTS,
    HCOL_TOTAL
};

struct HtCol
{
    float x0, x1, anchor;
    int kind;
    uint32_t choice;
};
typedef struct HtCol HtCol;

/* Split a known party code off the end of a canvass heading ("Ken Paxton, REP"). */
static void ht_split_party(const wchar_t *label, wchar_t *name, size_t ncch, wchar_t *party, size_t pcch)
{
    const wchar_t *comma = wcsrchr(label, L',');
    party[0] = L'\0';
    if (comma != NULL && comma[1] == L' ' && rp_party_code(comma + 2, wcslen(comma + 2)))
    {
        StringCchCopyNW(name, ncch, label, (size_t)(comma - label));
        StringCchCopyW(party, pcch, comma + 2);
        rp_norm(name);
        return;
    }
    StringCchCopyW(name, ncch, label);
}

/* The number in a Hart voting instruction: "(2)", a digit, or a number word ("Vote for
 * none, one, two, or three." is 3: the largest one named). 0 when there is none. */
static uint32_t ht_vote_for(const wchar_t *s)
{
    static const wchar_t *const k_words[] = {L"one",   L"two",   L"three", L"four", L"five",
                                             L"six",   L"seven", L"eight", L"nine", L"ten"};
    uint32_t best = 0, i;
    const wchar_t *p;
    for (p = s; *p != L'\0'; p++)
    {
        if (iswdigit(*p) && (p == s || !iswalnum(p[-1])))
        {
            uint32_t v = (uint32_t)_wtoi(p);
            if (v > best && v < 1000)
            {
                best = v;
            }
        }
    }
    for (i = 0; i < ARRAYSIZE(k_words); i++)
    {
        size_t n = wcslen(k_words[i]);
        for (p = s; (p = wcsstr(p, k_words[i])) != NULL; p += n)
        {
            if ((p == s || !iswalpha(p[-1])) && !iswalpha(p[n]) && i + 1 > best)
            {
                best = i + 1;
            }
        }
    }
    return best;
}

/* A Hart contest title as the rest of the program names contests: the voting instruction
 * Hart appends ("... - Vote for none, one, two, or three.", "... - Select up to two (2)
 * candidates.") becomes Vote For, and a primary's " - Democratic Party" suffix becomes the
 * "DEM " prefix of the Hart CVR and Clarity ("DEM Lieutenant Governor (Dem)"). */
static void ht_title(const wchar_t *raw, wchar_t *title, size_t cch, uint32_t *vote_for)
{
    static const struct
    {
        const wchar_t *suffix;
        const wchar_t *code;
    } k_parties[] = {{L" - Democratic Party", L"DEM"},
                     {L" - Republican Party", L"REP"},
                     {L" - Libertarian Party", L"LIB"},
                     {L" - Green Party", L"GRN"}};
    wchar_t t[512];
    const wchar_t *party = NULL;
    wchar_t *cut;
    size_t i, n;
    *vote_for = 0;
    StringCchCopyW(t, ARRAYSIZE(t), raw);
    n = wcslen(t);
    for (i = 0; i < ARRAYSIZE(k_parties); i++)
    {
        size_t sn = wcslen(k_parties[i].suffix);
        if (n > sn && _wcsicmp(t + n - sn, k_parties[i].suffix) == 0)
        {
            t[n - sn] = L'\0';
            party = k_parties[i].code;
            break;
        }
    }
    if ((cut = wcsstr(t, L" - Vote for ")) != NULL || (cut = wcsstr(t, L" - Select up to ")) != NULL ||
        (cut = wcsstr(t, L" - Vote For ")) != NULL)
    {
        *vote_for = ht_vote_for(cut + 3);
        *cut = L'\0';
    }
    rp_norm(t);
    if (party != NULL)
    {
        StringCchPrintfW(title, cch, L"%s %s", party, t);
    }
    else
    {
        StringCchCopyW(title, cch, t);
    }
}

/* A new contest, or the current one when a page goes on with it. */
static void ht_contest(RpCtx *cx, const wchar_t *raw_title)
{
    EeRpdfDoc *d = cx->d;
    EeRpdfContest *c;
    wchar_t title[512];
    uint32_t vote_for = 0;
    ht_title(raw_title, title, ARRAYSIZE(title), &vote_for);
    if (cx->have_contest && wcscmp(d->contests[cx->contest].title, title) == 0)
    {
        return;
    }
    if (!rp_grow32((void **)&d->contests, &d->cap_contests, d->ncontests + 1, sizeof(EeRpdfContest)))
    {
        cx->oom = TRUE;
        return;
    }
    c = &d->contests[d->ncontests];
    ZeroMemory(c, sizeof(*c));
    c->title = rp_wdup(title);
    c->vote_for = vote_for;
    c->over = EE_RPDF_UNKNOWN;
    c->under = EE_RPDF_UNKNOWN;
    c->ballots = EE_RPDF_UNKNOWN;
    if (c->title == NULL)
    {
        cx->oom = TRUE;
        return;
    }
    cx->contest = d->ncontests++;
    cx->have_contest = TRUE;
}

/* The heading of the first page: "<ballots> of <registered> = n%", the election name (the
 * left line above "Run Time"), the election date, the county and the run date and time. */
static void ht_heading(RpCtx *cx, RpItem *items, size_t n, float body_top)
{
    EeRpdfDoc *d = cx->d;
    size_t i, k;
    const wchar_t *run_date = NULL, *run_time = NULL;
    BOOL got_counts = FALSE;
    for (i = 0; i < n; i++)
    {
        const wchar_t *s = items[i].text;
        if (items[i].y <= body_top || items[i].rot != 0)
        {
            continue;
        }
        if (!got_counts && wcsstr(s, L" of ") != NULL && wcschr(s, L'=') != NULL)
        {
            /* "5931 of 84996 = 6.98%": ballots cast of registered voters */
            long long a = 0, b = 0;
            if (swscanf_s(s, L"%lld of %lld", &a, &b) == 2)
            {
                d->ballots_total = a;
                d->registered_total = b;
            }
            got_counts = TRUE;
        }
        else if (wcscmp(s, L"Run Date") == 0 || wcscmp(s, L"Run Time") == 0)
        {
            /* the value is the next item on the line */
            for (k = 0; k < n; k++)
            {
                if (k != i && fabsf(items[k].y - items[i].y) < 2.0f && items[k].x > items[i].x &&
                    items[k].x < items[i].x + 200.0f)
                {
                    if (wcscmp(s, L"Run Date") == 0)
                    {
                        run_date = items[k].text;
                    }
                    else
                    {
                        run_time = items[k].text;
                    }
                    break;
                }
            }
        }
        else if (wcsstr(s, L"County") != NULL && d->region == NULL)
        {
            d->region = rp_wdup(s);
        }
        else if (d->election_date == NULL && iswdigit(s[0]) && wcschr(s, L'/') != NULL && wcslen(s) <= 10 &&
                 items[i].size >= RP_HEAD_SIZE)
        {
            d->election_date = rp_wdup(s); /* "12/13/2025" */
        }
        else if (d->election_name == NULL && items[i].x < 40.0f && items[i].size < RP_HEAD_SIZE &&
                 wcscmp(s, L"Run Time") != 0 && wcscmp(s, L"Run Date") != 0)
        {
            /* the small left line: "December 13, 2025 Run-Off Election", "26 May 2026
             * Primary Run-off Official" */
            d->election_name = rp_wdup(s);
        }
    }
    if (run_date != NULL && d->timestamp == NULL)
    {
        wchar_t buf[96];
        StringCchPrintfW(buf, ARRAYSIZE(buf), L"%s%s%s", run_date, run_time != NULL ? L" " : L"",
                         run_time != NULL ? run_time : L"");
        d->timestamp = rp_wdup(buf);
    }
}

static int ht_kind(const wchar_t *label)
{
    size_t n = wcslen(label);
    if (_wcsicmp(label, L"Cast Votes") == 0 || _wcsicmp(label, L"Turnout Percentage") == 0)
    {
        return HCOL_SKIP;
    }
    if (_wcsicmp(label, L"Undervotes") == 0)
    {
        return HCOL_UNDER;
    }
    if (_wcsicmp(label, L"Overvotes") == 0)
    {
        return HCOL_OVER;
    }
    if (_wcsicmp(label, L"Registered Voters") == 0)
    {
        return HCOL_REG;
    }
    if (_wcsicmp(label, L"Total Ballots Cast") == 0)
    {
        return HCOL_BALLOTS;
    }
    if (n >= 12 && _wcsicmp(label + n - 12, L"Ballots Cast") == 0)
    {
        return HCOL_SKIP; /* ballots by vote type: the canvass splits no counts by vote type */
    }
    if (_wcsicmp(label, L"Rejected write-in votes") == 0 || _wcsicmp(label, L"Unresolved write-in votes") == 0)
    {
        return HCOL_SKIP;
    }
    return HCOL_CHOICE;
}

/* A canvass page: rotated headings, then one row per precinct. */
static void ht_canvass(RpCtx *cx, RpItem *items, size_t n, float body_top)
{
    EeRpdfDoc *d = cx->d;
    HtCol cols[64];
    int ncols = 0, k;
    size_t i, j;
    wchar_t labels[64][256];
    BOOL titled = FALSE;

    /* the title: the first left 9-10 pt line of the body */
    for (i = 0; i < n && !titled; i++)
    {
        if (items[i].y < body_top && items[i].rot == 0 && items[i].x < 40.0f && items[i].size >= 8.5f &&
            items[i].num == 0 && wcscmp(items[i].text, L"Precinct") != 0)
        {
            wchar_t title[512];
            RpItem *line[32];
            size_t nl = 0;
            for (j = 0; j < n && nl < 32; j++)
            {
                if (items[j].rot == 0 && fabsf(items[j].y - items[i].y) < RP_LINE_TOL)
                {
                    line[nl++] = &items[j];
                }
            }
            qsort(line, nl, sizeof(RpItem *), rp_ptr_x_cmp);
            rp_join(line, nl, title, ARRAYSIZE(title));
            ht_contest(cx, title);
            titled = TRUE;
        }
    }
    if (!titled || cx->oom)
    {
        return;
    }
    /* columns: rotated headings, a wrapped one in two runs close together */
    for (i = 0; i < n; i++)
    {
        if (items[i].rot != 90 || items[i].y >= body_top)
        {
            continue;
        }
        for (k = 0; k < ncols; k++)
        {
            if (fabsf(items[i].x - cols[k].x0) < 10.0f || fabsf(items[i].x - cols[k].x1) < 10.0f)
            {
                break;
            }
        }
        if (k == ncols)
        {
            if (ncols >= 64)
            {
                continue;
            }
            cols[ncols].x0 = cols[ncols].x1 = items[i].x;
            labels[ncols][0] = L'\0';
            ncols++;
        }
        if (items[i].x < cols[k].x0)
        {
            /* an earlier line of a wrapped heading */
            wchar_t t[256];
            StringCchPrintfW(t, ARRAYSIZE(t), L"%s %s", items[i].text, labels[k]);
            StringCchCopyW(labels[k], ARRAYSIZE(labels[k]), t);
            cols[k].x0 = items[i].x;
        }
        else
        {
            if (labels[k][0] != L'\0')
            {
                StringCchCatW(labels[k], ARRAYSIZE(labels[k]), L" ");
            }
            StringCchCatW(labels[k], ARRAYSIZE(labels[k]), items[i].text);
            if (items[i].x > cols[k].x1)
            {
                cols[k].x1 = items[i].x;
            }
        }
    }
    for (k = 0; k < ncols; k++)
    {
        rp_norm(labels[k]);
        cols[k].anchor = cols[k].x1 + 12.5f; /* numbers end just right of the turned heading */
        cols[k].kind = ht_kind(labels[k]);
        cols[k].choice = 0;
        if (cols[k].kind == HCOL_CHOICE)
        {
            wchar_t name[256], party[16];
            ht_split_party(labels[k], name, ARRAYSIZE(name), party, ARRAYSIZE(party));
            cols[k].choice = rp_choice(cx, cx->contest, name, party);
        }
        else if (cols[k].kind == HCOL_UNDER || cols[k].kind == HCOL_OVER)
        {
            d->contests[cx->contest].has_over_under = TRUE;
        }
    }
    if (ncols == 0 || cx->oom)
    {
        return;
    }
    cx->hart_canvass = TRUE;
    /* rows: the precinct name at the left, then numbers */
    for (i = 0; i < n && !cx->oom; i++)
    {
        RpItem *first;
        BOOL totals;
        if (items[i].rot != 0 || items[i].y >= body_top || items[i].x >= 60.0f)
        {
            continue;
        }
        first = &items[i];
        if (first->size >= 8.5f || wcscmp(first->text, L"Precinct") == 0)
        {
            continue; /* the title, or the "Precinct" heading */
        }
        totals = _wcsicmp(first->text, L"Totals") == 0 || _wcsicmp(first->text, L"Total") == 0;
        if (!totals)
        {
            /* a primary's canvass names the precinct "110 - DEM": the precinct is 110 (the
             * Hart CVR writes 110DEM / 110-DEM) */
            wchar_t pname[256];
            const wchar_t *dash;
            StringCchCopyW(pname, ARRAYSIZE(pname), first->text);
            dash = wcsstr(pname, L" - ");
            if (dash != NULL && rp_party_code(dash + 3, wcslen(dash + 3)))
            {
                pname[dash - pname] = L'\0';
            }
            cx->pre = rp_precinct(cx, pname);
            if (cx->oom)
            {
                return;
            }
        }
        for (j = 0; j < n; j++)
        {
            RpItem *t = &items[j];
            float best_d = 1e9f;
            int best = -1;
            EeRpdfContest *c;
            if (t == first || t->rot != 0 || t->num != 1 || fabsf(t->y - first->y) >= RP_LINE_TOL)
            {
                continue;
            }
            for (k = 0; k < ncols; k++)
            {
                float dd = fabsf(t->x1 - cols[k].anchor);
                if (dd < best_d)
                {
                    best_d = dd;
                    best = k;
                }
            }
            if (best < 0 || best_d > 20.0f)
            {
                cx->dropped_numbers++;
                continue;
            }
            c = &d->contests[cx->contest];
            switch (cols[best].kind)
            {
                case HCOL_CHOICE:
                    if (totals)
                    {
                        c->choices[cols[best].choice].total = t->value;
                    }
                    else
                    {
                        rp_count(cx, FALSE, cx->contest, cols[best].choice, EE_RPDF_NO_VT, t->value);
                    }
                    break;
                case HCOL_UNDER:
                case HCOL_OVER:
                    if (!totals)
                    {
                        rp_count(cx, FALSE, cx->contest, cols[best].kind == HCOL_OVER ? EE_RPDF_OVER : EE_RPDF_UNDER,
                                 EE_RPDF_NO_VT, t->value);
                    }
                    break;
                case HCOL_REG:
                    if (!totals && d->precincts[cx->pre].registered == EE_RPDF_UNKNOWN)
                    {
                        d->precincts[cx->pre].registered = t->value;
                    }
                    break;
                case HCOL_BALLOTS:
                    if (!totals && d->precincts[cx->pre].ballots == EE_RPDF_UNKNOWN)
                    {
                        d->precincts[cx->pre].ballots = t->value;
                    }
                    break;
                default:
                    break;
            }
        }
    }
}

/* A cumulative page: contests of "Choice Party <vote types> Total" tables. */
static void ht_cumulative(RpCtx *cx, RpItem *items, size_t n, float body_top)
{
    EeRpdfDoc *d = cx->d;
    size_t i = 0;
    HtCol *cols = cx->hcols;
    wchar_t *pending = cx->hpending;
    const size_t pending_cch = ARRAYSIZE(cx->hpending);
    RpItem *line[64];

    while (i < n && !cx->oom)
    {
        size_t nl = 0, j;
        float y = items[i].y;
        BOOL any_num = FALSE;
        float maxsize = 0.0f;
        if (items[i].y >= body_top || items[i].rot != 0)
        {
            i++;
            continue;
        }
        for (j = i; j < n && items[j].y > y - RP_LINE_TOL; j++)
        {
            if (items[j].rot == 0 && nl < 64)
            {
                line[nl++] = &items[j];
                any_num = any_num || items[j].num != 0;
                if (items[j].size > maxsize)
                {
                    maxsize = items[j].size;
                }
            }
        }
        i = j;
        if (nl == 0)
        {
            continue;
        }
        qsort(line, nl, sizeof(RpItem *), rp_ptr_x_cmp);
        if (wcscmp(line[0]->text, L"Choice") == 0 && line[0]->x < 40.0f)
        {
            /* the table heading */
            int ncols = 0;
            cx->party_x = 0.0f;
            for (j = 1; j < nl && ncols < RP_MAX_COLS; j++)
            {
                if (wcscmp(line[j]->text, L"Party") == 0)
                {
                    cx->party_x = line[j]->x;
                    continue;
                }
                cols[ncols].x0 = line[j]->x;
                cols[ncols].x1 = line[j]->x1;
                cols[ncols].kind = _wcsicmp(line[j]->text, L"Total") == 0 ? HCOL_TOTAL : HCOL_SKIP;
                ncols++;
            }
            cx->nhcols = ncols;
            if (pending[0] != L'\0')
            {
                ht_contest(cx, pending);
                pending[0] = L'\0';
            }
            cx->hart_title_line = FALSE;
            cx->hart_cum = TRUE;
            continue;
        }
        if (!any_num && maxsize >= RP_HEAD_SIZE && line[0]->x < 40.0f)
        {
            /* a contest title (a long one wraps onto a second line) */
            wchar_t t[512];
            rp_join(line, nl, t, ARRAYSIZE(t));
            if (cx->hart_title_line && cx->hart_title_y - y < 16.0f && pending[0] != L'\0')
            {
                StringCchCatW(pending, pending_cch, L" ");
                StringCchCatW(pending, pending_cch, t);
            }
            else
            {
                StringCchCopyW(pending, pending_cch, t);
            }
            cx->hart_title_line = TRUE;
            cx->hart_title_y = y;
            continue;
        }
        cx->hart_title_line = FALSE;
        if (cx->nhcols == 0 || !cx->have_contest || !any_num)
        {
            continue;
        }
        {
            wchar_t name[256] = L"", party[32] = L"";
            int64_t total = EE_RPDF_UNKNOWN;
            float first_x = cols[0].x0 - 30.0f;
            int k;
            EeRpdfContest *c = &d->contests[cx->contest];
            for (j = 0; j < nl; j++)
            {
                RpItem *t = line[j];
                if (t->num == 0 || t->x < first_x)
                {
                    BOOL is_party = cx->party_x > 0.0f && t->x >= cx->party_x - 3.0f && t->x < first_x;
                    wchar_t *dst = is_party ? party : name;
                    size_t cch = is_party ? ARRAYSIZE(party) : ARRAYSIZE(name);
                    if (dst[0] != L'\0')
                    {
                        StringCchCatW(dst, cch, L" ");
                    }
                    StringCchCatW(dst, cch, t->text);
                    continue;
                }
                if (t->num == 1)
                {
                    /* a count: the column whose heading ends just right of it */
                    for (k = 0; k < cx->nhcols; k++)
                    {
                        if (cols[k].x1 > t->x1 + 8.0f)
                        {
                            break;
                        }
                    }
                    if (k < cx->nhcols && cols[k].kind == HCOL_TOTAL)
                    {
                        total = t->value;
                    }
                }
            }
            rp_norm(name);
            rp_norm(party);
            if (_wcsicmp(name, L"Cast Votes:") == 0 || name[0] == L'\0')
            {
                continue;
            }
            if (_wcsicmp(name, L"Undervotes:") == 0 || _wcsicmp(name, L"Overvotes:") == 0)
            {
                c->has_over_under = TRUE;
                if (_wcsicmp(name, L"Overvotes:") == 0)
                {
                    c->over = total;
                }
                else
                {
                    c->under = total;
                }
                continue;
            }
            if (ht_kind(name) == HCOL_SKIP || rp_starts(name, L"Rejected write-in") ||
                rp_starts(name, L"Unresolved write-in"))
            {
                continue;
            }
            {
                uint32_t ch = rp_choice(cx, cx->contest, name, party);
                if (!cx->oom)
                {
                    d->contests[cx->contest].choices[ch].total = total;
                }
            }
        }
    }
}

static void hart_page(RpCtx *cx, const EePdfPageText *pt, BOOL first_page)
{
    RpItem *items = NULL;
    wchar_t *pool = NULL;
    size_t n, i;
    float body_top = 1e9f;
    BOOL rotated = FALSE;

    if (pt->nruns == 0)
    {
        return;
    }
    items = (RpItem *)malloc(pt->nruns * sizeof(RpItem));
    pool = (wchar_t *)malloc((pt->text_len + pt->nruns + 1) * sizeof(wchar_t));
    if (items == NULL || pool == NULL)
    {
        cx->oom = TRUE;
        goto done;
    }
    n = rp_items(cx, pt, items, pool, TRUE);
    for (i = 0; i < n; i++)
    {
        if (items[i].rot == 0 && wcscmp(items[i].text, L"Run Date") == 0)
        {
            body_top = items[i].y - 4.0f; /* the heading ends with "Run Date" */
            break;
        }
    }
    if (first_page)
    {
        ht_heading(cx, items, n, body_top < 1e8f ? body_top : -1e9f);
    }
    for (i = 0; i < n; i++)
    {
        if (items[i].rot == 90 && items[i].y < body_top)
        {
            rotated = TRUE;
            break;
        }
    }
    if (rotated)
    {
        ht_canvass(cx, items, n, body_top);
    }
    else
    {
        ht_cumulative(cx, items, n, body_top);
    }
done:
    free(items);
    free(pool);
}

/* -------------------------------------------------------------------------- */
/* Hart reports before Verity (McLennan, Montgomery, Harris 2020-22)          */
/* -------------------------------------------------------------------------- */

/* Landscape pages with no Producer or Creator. Every page starts with the report title
 * ("<entity> Cumulative Report  -  Official", "Canvass Report  -  Total Voters  -
 * Official"; counties edit it), "<County>  -  <election>  -  <date>", "Page N of M", the run
 * date and "Total Number of Voters : 98,898 of 149,737 = 66.05%" beside "Precincts
 * Reporting". Then:
 * - cumulative and precinct reports: a "Party  Candidate  Absentee  Early  Election  Total"
 *   heading, contest titles ("..., Vote For 1"), and rows with a count and a percentage under
 *   each heading; the precinct report repeats the contests under "Precinct 075  (Ballots
 *   Cast: 17)". Baselines wander by up to 2 pt within a row;
 * - canvass: per contest a precinct grid, the ballots cast by vote type, total, registered
 *   voters and turnout, then one column per choice with its heading turned 90 degrees and
 *   wrapped (the party on the first line), and a "Totals" column. Choices that do not fit go
 *   on to a following page under the same title ("Continued"). */

/* A page looks like one: a voters line and "Precincts Reporting" or a report title (Harris's
 * 2020 reports, retyped in Word, have no "Precincts Reporting"). */
static BOOL hl_detect(const EePdfPageText *pt)
{
    static const char *const k_need[] = {"Number of Voters", "Party Voters", "Precincts Reporting",
                                         "Cumulative Report", "Precinct Report", "Canvass Report"};
    BOOL found[6] = {FALSE, FALSE, FALSE, FALSE, FALSE, FALSE};
    size_t i, k;
    for (k = 0; k < 6; k++)
    {
        size_t m = strlen(k_need[k]);
        for (i = 0; i + m <= pt->text_len && !found[k]; i++)
        {
            found[k] = memcmp(pt->text + i, k_need[k], m) == 0;
        }
    }
    return (found[0] || found[1]) && (found[2] || found[3] || found[4] || found[5]);
}

/* The vote type a heading names ("Early" is "Early Voting", "Election" is "Election Day"). */
static uint32_t hl_vt(RpCtx *cx, const wchar_t *word)
{
    if (_wcsicmp(word, L"Early") == 0)
    {
        return rp_vt(cx, L"Early Voting");
    }
    if (_wcsicmp(word, L"Election") == 0)
    {
        return rp_vt(cx, L"Election Day");
    }
    return rp_vt(cx, word);
}

/* "A of B" after the colon of a voters line: ballots cast of registered voters. A line drawn
 * in two strings ("... : 273,098 of " and "372,882 = 73.24%") gives B from @p rest. */
static void hl_voters(RpCtx *cx, const wchar_t *s, const wchar_t *rest)
{
    const wchar_t *p = wcschr(s, L':');
    wchar_t num[32];
    size_t k = 0;
    int64_t a, b;
    if (p == NULL)
    {
        return;
    }
    for (p++; *p == L' '; p++)
    {
    }
    while ((iswdigit(*p) || *p == L',') && k + 1 < ARRAYSIZE(num))
    {
        num[k++] = *p++;
    }
    num[k] = L'\0';
    if (rp_number(num, &a) != 1)
    {
        return;
    }
    cx->d->ballots_total = a;
    p = wcsstr(p, L" of ");
    if (p != NULL)
    {
        p += 4;
    }
    else if (rest != NULL)
    {
        p = rest;
    }
    else
    {
        return;
    }
    for (k = 0; (iswdigit(*p) || *p == L',') && k + 1 < ARRAYSIZE(num); p++)
    {
        num[k++] = *p;
    }
    num[k] = L'\0';
    if (rp_number(num, &b) == 1)
    {
        cx->d->registered_total = b;
    }
}

/* The first page's heading: report title (a primary's party), "<County> - <election> -
 * <date>", the run date and the voters line. */
static void hl_heading(RpCtx *cx, RpItem *items, size_t n)
{
    EeRpdfDoc *d = cx->d;
    wchar_t line[2][512] = {L"", L""};
    float ly[2] = {0.0f, 0.0f};
    int nl = 0, l;
    size_t i;
    BOOL district = FALSE;
    for (i = 0; i < n && nl <= 2; i++)
    {
        if (items[i].rot != 0 || items[i].size < 10.9f)
        {
            continue;
        }
        if (nl == 0 || items[i].y < ly[nl - 1] - 4.0f)
        {
            if (nl == 2)
            {
                break;
            }
            ly[nl++] = items[i].y;
        }
        else if (line[nl - 1][0] != L'\0')
        {
            StringCchCatW(line[nl - 1], ARRAYSIZE(line[0]), L" ");
        }
        StringCchCatW(line[nl - 1], ARRAYSIZE(line[0]), items[i].text);
    }
    for (l = 0; l < nl; l++)
    {
        rp_norm(line[l]);
    }
    if (nl > 0)
    {
        /* "Democratic Party Canvass Report", "Democratic Primary Runoff Canvass Report" */
        if (rp_starts(line[0], L"Democratic "))
        {
            StringCchCopyW(cx->hl_party, ARRAYSIZE(cx->hl_party), L"DEM");
        }
        else if (rp_starts(line[0], L"Republican "))
        {
            StringCchCopyW(cx->hl_party, ARRAYSIZE(cx->hl_party), L"REP");
        }
    }
    if (nl > 1)
    {
        /* "McLennan County  -  JOINT GENERAL ELECTION  -  November 03, 2020" (the dash may come
         * out as an unmapped character) */
        wchar_t *part[4] = {NULL, NULL, NULL, NULL}, *p = line[1];
        int np = 0, k;
        BOOL dash = FALSE;
        for (; *p != L'\0'; p++)
        {
            if (*p == 0x2014 || *p == 0xFFFD || *p == 0x2013)
            {
                *p = L'|';
                dash = TRUE;
            }
        }
        for (p = line[1]; !dash && *p != L'\0'; p++)
        {
            if (p[0] == L' ' && p[1] == L'-' && p[2] == L' ')
            {
                p[1] = L'|'; /* plain " - " (McLennan's Crawford ISD reports) */
            }
        }
        part[np++] = line[1];
        for (p = line[1]; *p != L'\0' && np < 4; p++)
        {
            if (*p == L'|')
            {
                *p = L'\0';
                part[np++] = p + 1;
            }
        }
        for (k = 0; k < np; k++)
        {
            rp_norm(part[k]);
        }
        if (np >= 1 && part[0][0] != L'\0' && d->region == NULL)
        {
            d->region = rp_wdup(part[0]);
        }
        if (np >= 2 && part[1][0] != L'\0' && d->election_name == NULL)
        {
            wchar_t name[512];
            StringCchPrintfW(name, ARRAYSIZE(name), L"%s%s%s%s", part[1], cx->hl_party[0] ? L" (" : L"",
                             cx->hl_party[0] ? (cx->hl_party[0] == L'D' ? L"Democratic Party" : L"Republican Party")
                                             : L"",
                             cx->hl_party[0] ? L")" : L"");
            d->election_name = rp_wdup(name);
        }
        if (np >= 3 && part[2][0] != L'\0' && d->election_date == NULL)
        {
            d->election_date = rp_wdup(part[2]);
        }
    }
    /* an entity's report (McLennan's per city and ISD) also gives "Number of District Voters":
     * its precincts are the district's, and the county's totals come from the county's report */
    for (i = 0; i < n; i++)
    {
        if (items[i].rot == 0 && wcsstr(items[i].text, L"District Voters") != NULL)
        {
            district = TRUE;
        }
    }
    /* the run date and the voters line, each read as a joined line (Harris draws "11" and
     * "/16/2020 08:18 AM", "Total Number of Voters" and ": 1,656,686 of 2,431,457 = 68.14%") */
    for (i = 0; i < n; i++)
    {
        RpItem *row[32];
        size_t nr = 0, j;
        wchar_t s[512];
        const wchar_t *p;
        if (items[i].rot != 0 || (i > 0 && items[i - 1].rot == 0 && fabsf(items[i - 1].y - items[i].y) < 1.0f))
        {
            continue; /* not the first item of its line */
        }
        for (j = i; j < n && fabsf(items[j].y - items[i].y) < 1.0f && nr < 32; j++)
        {
            if (items[j].rot == 0)
            {
                row[nr++] = &items[j];
            }
        }
        qsort(row, nr, sizeof(RpItem *), rp_ptr_x_cmp);
        rp_join(row, nr, s, ARRAYSIZE(s));
        for (p = s; d->timestamp == NULL && *p != L'\0'; p++)
        {
            /* "11/10/2020 04:59 PM" */
            if (iswdigit(p[0]) && iswdigit(p[1]) && p[2] == L'/' && iswdigit(p[3]) && p[5] == L'/' && wcslen(p) >= 19 &&
                p[10] == L' ' && p[13] == L':' && p[16] == L' ' && (p[17] == L'A' || p[17] == L'P'))
            {
                wchar_t ts[20];
                StringCchCopyNW(ts, ARRAYSIZE(ts), p, 19);
                d->timestamp = rp_wdup(ts);
            }
        }
        /* (a primary party's report counts that party's voters: "Total Democratic Party Voters";
         * McLennan's 2022 "Total Number of Voters" there is both parties') */
        if (!district && d->ballots_total == EE_RPDF_UNKNOWN && (p = wcsstr(s, L"Voters")) != NULL &&
            wcschr(p, L':') != NULL && wcsstr(s, L"District") == NULL &&
            (cx->hl_party[0] == L'\0' || wcsstr(s, L"Party Voters") != NULL))
        {
            hl_voters(cx, p, NULL);
        }
    }
}

/* Spacing as the cumulative report prints it; Harris's Word copies of the canvass split words
 * into pieces with spaces between: "14 th", "Assessor -Collector", "( Four Year Term)",
 * "Appeals , Place 3". A " - " with spaces on both sides stays. */
static void hl_tidy(wchar_t *t)
{
    size_t r, w = 0;
    BOOL inq = FALSE, opened = FALSE; /* inside a quote; the last thing written opened one */
    for (r = 0; t[r] != L'\0'; r++)
    {
        wchar_t c = t[r];
        BOOL quote = c == L'"' || (c == L'\'' && t[r + 1] == L'\'');
        if (quote)
        {
            /* a quote mark ('"' or "''"): no space just inside it ("Tamika " Tami " Craft",
             * "''STORMCHASER '' NELSON") */
            if (inq && w > 0 && t[w - 1] == L' ')
            {
                w--;
            }
            t[w++] = c;
            if (c == L'\'')
            {
                t[w++] = t[++r];
            }
            inq = !inq;
            opened = inq;
            continue;
        }
        if (c == L' ' && opened)
        {
            continue;
        }
        opened = FALSE;
        if (c == L' ')
        {
            if (t[r + 1] == L',' || t[r + 1] == L')' || (w > 0 && t[w - 1] == L'('))
            {
                continue;
            }
            if (t[r + 1] == L'-' && t[r + 2] != L' ' && t[r + 2] != L'\0')
            {
                continue; /* " -Collector" */
            }
            if (w > 0 && t[w - 1] == L'-' && w > 1 && t[w - 2] != L' ')
            {
                continue; /* "Rivas- Molloy" */
            }
            if (w > 0 && iswdigit(t[w - 1]) &&
                (_wcsnicmp(t + r + 1, L"st", 2) == 0 || _wcsnicmp(t + r + 1, L"nd", 2) == 0 ||
                 _wcsnicmp(t + r + 1, L"rd", 2) == 0 || _wcsnicmp(t + r + 1, L"th", 2) == 0) &&
                !iswalpha(t[r + 3]))
            {
                continue; /* "14 th" */
            }
        }
        t[w++] = c;
    }
    t[w] = L'\0';
}

/* A contest title as the rest of the program names it: ", Vote For 1" becomes Vote For,
 * and a primary's titles take the party code ("REP Governor"). */
static void hl_title(RpCtx *cx, const wchar_t *raw, wchar_t *title, size_t cch, uint32_t *vote_for)
{
    wchar_t t[512];
    wchar_t *cut;
    *vote_for = 0;
    StringCchCopyW(t, ARRAYSIZE(t), raw);
    if ((cut = wcsstr(t, L", Vote For ")) != NULL || (cut = wcsstr(t, L", Vote for ")) != NULL)
    {
        *vote_for = (uint32_t)_wtoi(cut + 11);
        *cut = L'\0';
    }
    rp_norm(t);
    hl_tidy(t);
    {
        /* Montgomery's primary canvass titles a contest's first page "President D - Democratic
         * Party" and the pages after it "President D" */
        static const wchar_t *const k_sfx[] = {L" - Democratic Party", L" - Republican Party", L" Democratic Party",
                                               L" Republican Party"};
        size_t s, tl = wcslen(t);
        for (s = 0; s < ARRAYSIZE(k_sfx); s++)
        {
            size_t sl = wcslen(k_sfx[s]);
            if (tl > sl && _wcsicmp(t + tl - sl, k_sfx[s]) == 0)
            {
                t[tl - sl] = L'\0';
                break;
            }
        }
    }
    if (cx->hl_party[0] != L'\0')
    {
        StringCchPrintfW(title, cch, L"%s %s", cx->hl_party, t);
    }
    else
    {
        StringCchCopyW(title, cch, t);
    }
}

/* A choice's name as all three reports give it: one spacing around "/" (the canvass wraps
 * "Donald J. Trump/ " onto "Michael R. Pence") and no write-in mark (" (W)", which only some
 * reports print). */
static void hl_name(wchar_t *s, size_t cch)
{
    wchar_t out[512];
    size_t i, o = 0, n;
    hl_tidy(s);
    for (i = 0; s[i] != L'\0' && o + 4 < ARRAYSIZE(out); i++)
    {
        if (s[i] == L'/')
        {
            while (o > 0 && out[o - 1] == L' ')
            {
                o--;
            }
            out[o++] = L' ';
            out[o++] = L'/';
            out[o++] = L' ';
            while (s[i + 1] == L' ')
            {
                i++;
            }
            continue;
        }
        out[o++] = s[i];
    }
    out[o] = L'\0';
    rp_norm(out);
    n = wcslen(out);
    if (n > 4 && wcscmp(out + n - 4, L" (W)") == 0)
    {
        out[n - 4] = L'\0';
    }
    StringCchCopyW(s, cch, out);
}

/* Start (or go on with) the contest titled @p raw. In a precinct report the same contests
 * come back for every precinct. */
static void hl_start_contest(RpCtx *cx, const wchar_t *raw)
{
    wchar_t title[512];
    uint32_t vote_for;
    hl_title(cx, raw, title, ARRAYSIZE(title), &vote_for);
    if (cx->have_contest && wcscmp(cx->d->contests[cx->contest].title, title) == 0)
    {
        return; /* the title again at the top of a following page */
    }
    cx->contest = rp_contest(cx, title, vote_for);
    cx->have_contest = !cx->oom;
    if (cx->pre != UINT32_MAX && rp_grow((void **)&cx->seq, &cx->cap_seq, cx->nseq + 1, sizeof(uint32_t)))
    {
        cx->seq[cx->nseq++] = cx->contest;
    }
}

typedef struct HlLine
{
    float y;
    RpItem *text[24]; /* the label's items, left to right */
    int ntext;
    int64_t val[16]; /* by column */
    BOOL has[16];
    BOOL any; /* a count lined up with it */
} HlLine;

/* A cumulative or precinct report page; @p head_y is the "Party ... Total" heading's line. */
static void hl_table(RpCtx *cx, RpItem *items, size_t n, float head_y)
{
    EeRpdfDoc *d = cx->d;
    HlLine *lines = (HlLine *)calloc(n + 1, sizeof(HlLine));
    size_t nl = 0, i, l;
    int k, total_col = -1;
    if (lines == NULL)
    {
        cx->oom = TRUE;
        return;
    }
    for (k = 0; k < cx->hl_ncols; k++)
    {
        if (cx->hl_colvt[k] < 0)
        {
            total_col = k;
        }
    }
    /* the labels' lines (a row's baselines wander by up to 2 pt) */
    for (i = 0; i < n; i++)
    {
        RpItem *t = &items[i];
        if (t->rot != 0 || t->y >= head_y - 3.0f || t->num != 0) /* (no footer: rows go down to 20 pt) */
        {
            continue;
        }
        if (nl == 0 || t->y < lines[nl - 1].y - 2.5f)
        {
            lines[nl++].y = t->y;
        }
        if (lines[nl - 1].ntext < 24)
        {
            lines[nl - 1].text[lines[nl - 1].ntext++] = t;
        }
    }
    /* each count to the nearest label line and the heading nearest just right of it */
    for (i = 0; i < n; i++)
    {
        RpItem *t = &items[i];
        size_t best = SIZE_MAX;
        float bd = 5.01f, cd = 61.0f;
        int col = -1;
        if (t->rot != 0 || t->y >= head_y - 3.0f || t->num != 1)
        {
            continue;
        }
        for (l = 0; l < nl; l++)
        {
            if (fabsf(lines[l].y - t->y) < bd)
            {
                bd = fabsf(lines[l].y - t->y);
                best = l;
            }
        }
        for (k = 0; k < cx->hl_ncols; k++)
        {
            float dd = fabsf(cx->hl_cols[k] - (t->x1 + 10.0f));
            if (dd < cd)
            {
                cd = dd;
                col = k;
            }
        }
        if (best != SIZE_MAX && col >= 0)
        {
            lines[best].val[col] = t->value;
            lines[best].has[col] = TRUE;
            lines[best].any = TRUE;
        }
    }
    for (l = 0; l < nl && !cx->oom; l++)
    {
        HlLine *ln = &lines[l];
        wchar_t text[512] = L"", party[16] = L"";
        RpItem *words[24];
        int j, nw = 0;
        qsort(ln->text, (size_t)ln->ntext, sizeof(RpItem *), rp_ptr_x_cmp);
        for (j = 0; j < ln->ntext; j++)
        {
            RpItem *t = ln->text[j];
            if (ln->any && t->x < 60.0f && rp_party_code(t->text, wcslen(t->text)))
            {
                StringCchCopyW(party, ARRAYSIZE(party), t->text);
                continue;
            }
            words[nw++] = t;
        }
        rp_join(words, (size_t)nw, text, ARRAYSIZE(text)); /* touching pieces are one word (Harris's Word copies) */
        if (text[0] == L'\0')
        {
            continue;
        }
        if (rp_starts(text, L"Precinct ") && wcsstr(text, L"(Ballots Cast:") != NULL)
        {
            /* "Precinct 075  (Ballots Cast: 17)" */
            wchar_t name[128];
            const wchar_t *open = wcsstr(text, L"(Ballots Cast:");
            int64_t bal;
            wchar_t num[32];
            size_t m = 0;
            const wchar_t *p = open + 14;
            StringCchCopyNW(name, ARRAYSIZE(name), text + 9, (size_t)(open - text) - 9);
            rp_norm(name);
            cx->pre = rp_precinct(cx, name);
            cx->nocc = 0;
            cx->have_contest = FALSE;
            cx->hl_title[0] = L'\0';
            if (rp_grow((void **)&cx->seq, &cx->cap_seq, cx->nseq + 1, sizeof(uint32_t)))
            {
                cx->seq[cx->nseq++] = UINT32_MAX;
            }
            for (; *p == L' '; p++)
            {
            }
            while ((iswdigit(*p) || *p == L',') && m + 1 < ARRAYSIZE(num))
            {
                num[m++] = *p++;
            }
            num[m] = L'\0';
            if (!cx->oom && rp_number(num, &bal) == 1)
            {
                d->precincts[cx->pre].ballots = bal;
            }
            continue;
        }
        if (!ln->any)
        {
            /* a contest title (a long one may wrap) */
            if (cx->hl_title[0] != L'\0' && l > 0 && !lines[l - 1].any && lines[l - 1].y - ln->y < 12.0f)
            {
                StringCchCatW(cx->hl_title, ARRAYSIZE(cx->hl_title), L" ");
                StringCchCatW(cx->hl_title, ARRAYSIZE(cx->hl_title), text);
            }
            else
            {
                StringCchCopyW(cx->hl_title, ARRAYSIZE(cx->hl_title), text);
            }
            continue;
        }
        if (cx->hl_title[0] != L'\0')
        {
            hl_start_contest(cx, cx->hl_title);
            cx->hl_title[0] = L'\0';
        }
        if (!cx->have_contest || cx->oom)
        {
            continue;
        }
        cx->hl_table = TRUE;
        {
            EeRpdfContest *c = &d->contests[cx->contest];
            int64_t total = total_col >= 0 && ln->has[total_col] ? ln->val[total_col] : EE_RPDF_UNKNOWN;
            BOOL in_pre = cx->pre != UINT32_MAX;
            uint32_t ch;
            if (_wcsicmp(text, L"Cast Votes:") == 0)
            {
                continue;
            }
            if (_wcsicmp(text, L"Over Votes:") == 0 || _wcsicmp(text, L"Under Votes:") == 0)
            {
                BOOL over = _wcsicmp(text, L"Over Votes:") == 0;
                c->has_over_under = TRUE;
                ch = over ? EE_RPDF_OVER : EE_RPDF_UNDER;
                if (!in_pre)
                {
                    if (over)
                    {
                        c->over = total;
                    }
                    else
                    {
                        c->under = total;
                    }
                    continue;
                }
            }
            else
            {
                hl_name(text, ARRAYSIZE(text));
                ch = rp_choice(cx, cx->contest, text, party);
                if (cx->oom)
                {
                    break;
                }
                if (!in_pre)
                {
                    d->contests[cx->contest].choices[ch].total = total;
                    continue;
                }
            }
            /* a precinct's counts by vote type (or its total when there are none) */
            {
                BOOL any_vt = FALSE;
                for (k = 0; k < cx->hl_ncols; k++)
                {
                    if (cx->hl_colvt[k] >= 0 && ln->has[k])
                    {
                        rp_count(cx, FALSE, cx->contest, ch, (uint32_t)cx->hl_colvt[k], ln->val[k]);
                        any_vt = TRUE;
                    }
                }
                if (!any_vt && total != EE_RPDF_UNKNOWN)
                {
                    rp_count(cx, FALSE, cx->contest, ch, EE_RPDF_NO_VT, total);
                }
            }
        }
    }
    free(lines);
}

/* Turned headings in reading order: line by line (left to right), and up each line (Harris's
 * Word copies draw "Tom Hoefling" "/" on one line as separate pieces). */
static int __cdecl hl_rot_cmp(const void *pa, const void *pb)
{
    const RpItem *a = *(const RpItem *const *)pa;
    const RpItem *b = *(const RpItem *const *)pb;
    if (fabsf(a->x - b->x) > 1.0f)
    {
        return a->x < b->x ? -1 : 1;
    }
    return a->y < b->y ? -1 : (a->y > b->y ? 1 : 0);
}

/* A canvass page; @p pre_y is the "Precinct" heading's line. */
static void hl_canvass(RpCtx *cx, RpItem *items, size_t n, float pre_y)
{
    EeRpdfDoc *d = cx->d;
    float top = 1e9f, rot_lo = 1e9f;
    float hx[16];      /* fixed headings' centres */
    int hkind[16];     /* -1 skip, -2 total ballots, -3 registered, else rp_vt */
    int nh = 0, k;
    float fixed_end = 0.0f; /* where the choice columns start */
    RpItem *rot[96];
    int nrot = 0;
    struct
    {
        wchar_t name[256];
        wchar_t party[16];
        int kind; /* 0 choice, 1 totals, 2 "Continued" */
        uint32_t ch;
    } cols[24];
    int ncols = 0, c;
    wchar_t title[512] = L"";
    size_t i;

    /* the title: between the voters lines ("Total ...", "Number of District ...") and the
     * "Precinct" heading */
    for (i = 0; i < n; i++)
    {
        if (items[i].rot == 0 && items[i].y > pre_y + 6.0f && items[i].y < top &&
            (wcsstr(items[i].text, L"Voters") != NULL || wcsstr(items[i].text, L"Precincts Reporting") != NULL))
        {
            top = items[i].y;
        }
    }
    {
        RpItem *tw[48];
        size_t ntw = 0;
        for (i = 0; i < n && ntw < 48; i++)
        {
            if (items[i].rot == 0 && items[i].y < top - 3.0f && items[i].y > pre_y + 6.0f)
            {
                tw[ntw++] = &items[i]; /* (top down, left to right) */
            }
        }
        rp_join(tw, ntw, title, ARRAYSIZE(title));
    }
    {
        size_t tl = wcslen(title);
        if (tl > 10 && wcscmp(title + tl - 10, L" Continued") == 0)
        {
            title[tl - 10] = L'\0'; /* Harris's Word copies title a following page so */
        }
    }
    if (title[0] == L'\0')
    {
        return;
    }
    /* the fixed headings: their first line, on the "Precinct" line */
    for (i = 0; i < n && nh < 16; i++)
    {
        const RpItem *t = &items[i];
        if (t->rot != 0 || fabsf(t->y - pre_y) > 1.5f || _wcsicmp(t->text, L"Precinct") == 0)
        {
            continue;
        }
        hx[nh] = (t->x + t->x1) / 2.0f;
        if (_wcsicmp(t->text, L"Total") == 0)
        {
            hkind[nh] = -2;
        }
        else if (_wcsicmp(t->text, L"Registered") == 0)
        {
            hkind[nh] = -3;
        }
        else
        {
            /* turnout, and ballots cast by vote type: the canvass splits no choice's votes by
             * vote type (some have no Absentee column), so it has none, as in Verity */
            hkind[nh] = -1;
        }
        if (t->x1 > fixed_end)
        {
            fixed_end = t->x1;
        }
        nh++;
    }
    /* the choices' turned headings, grouped into columns by the gaps between them */
    for (i = 0; i < n && nrot < 96; i++)
    {
        if (items[i].rot == 90 && items[i].x > fixed_end)
        {
            rot[nrot++] = &items[i];
            if (items[i].y < rot_lo)
            {
                rot_lo = items[i].y;
            }
        }
    }
    qsort(rot, (size_t)nrot, sizeof(RpItem *), hl_rot_cmp);
    for (k = 0; k < nrot; k++)
    {
        const wchar_t *s = rot[k]->text;
        /* a heading's lines are 10.8 pt apart; the next heading starts 15 pt or more on */
        if (k == 0 || rot[k]->x - rot[k - 1]->x > 13.0f)
        {
            if (ncols == 24)
            {
                break;
            }
            ZeroMemory(&cols[ncols], sizeof(cols[0]));
            ncols++;
            if (rp_party_code(s, wcslen(s)))
            {
                StringCchCopyW(cols[ncols - 1].party, ARRAYSIZE(cols[0].party), s);
                continue;
            }
        }
        /* a piece that starts where the one before it on the line ends (up the line, by its
         * width) goes on the same word ("Alma A" "." in Harris's Word copies) */
        /* a line that ends inside a word ("Sanchez-Miramo" / "ntez") has no space at its end:
         * one that ends a word does ("Donald J. " / "Trump") */
        if (cols[ncols - 1].name[0] != L'\0' &&
            !(k > 0 && rot[k - 1]->exact && !rot[k - 1]->sp_after && !rot[k]->sp_before &&
              fabsf(rot[k]->x - rot[k - 1]->x) < 1.0f &&
              fabsf(rot[k]->y - (rot[k - 1]->y + (rot[k - 1]->x1 - rot[k - 1]->x))) < 0.5f) &&
            !(k > 0 && fabsf(rot[k]->x - rot[k - 1]->x) >= 1.0f && !rot[k - 1]->sp_after && !rot[k]->sp_before &&
              iswalpha(rot[k - 1]->text[wcslen(rot[k - 1]->text) - 1]) && iswalpha(rot[k]->text[0])))
        {
            StringCchCatW(cols[ncols - 1].name, ARRAYSIZE(cols[0].name), L" ");
        }
        StringCchCatW(cols[ncols - 1].name, ARRAYSIZE(cols[0].name), s);
    }
    hl_start_contest(cx, title);
    if (!cx->have_contest)
    {
        return;
    }
    for (c = 0; c < ncols; c++)
    {
        rp_norm(cols[c].name);
        if (wcsstr(cols[c].name, L"tinued") != NULL)
        {
            cols[c].kind = 2; /* "Con tinued" */
        }
        else if (_wcsicmp(cols[c].name, L"Totals") == 0)
        {
            cols[c].kind = 1;
        }
        else
        {
            hl_name(cols[c].name, ARRAYSIZE(cols[c].name));
            cols[c].ch = rp_choice(cx, cx->contest, cols[c].name, cols[c].party);
        }
    }
    cx->hl_canvass = TRUE;
    /* rows: a precinct name at the left, then the numbers */
    i = 0;
    while (i < n && !cx->oom)
    {
        float y = items[i].y;
        size_t j, first = i;
        const RpItem *label = NULL;
        int64_t vals[32];
        int nv = 0, m, h;
        if (items[i].rot != 0 || y >= rot_lo - 2.0f || y >= pre_y - 2.0f)
        {
            i++;
            continue;
        }
        for (j = i; j < n && items[j].y > y - 2.5f; j++)
        {
        }
        i = j;
        for (j = first; j < i; j++)
        {
            if (items[j].rot == 0 && items[j].x < 40.0f && label == NULL)
            {
                label = &items[j];
            }
        }
        if (label == NULL || _wcsicmp(label->text, L"Totals") == 0)
        {
            continue;
        }
        cx->pre = rp_precinct(cx, label->text);
        if (cx->oom)
        {
            break;
        }
        {
            /* a contest's ballots are those of its part of a split precinct: a precinct's own
             * are the largest of its contests' */
            for (j = first; j < i; j++)
            {
                const RpItem *t = &items[j];
                float bd = 41.0f;
                int best = -1;
                if (t == label || t->rot != 0 || t->num != 1)
                {
                    continue;
                }
                if (t->x1 > fixed_end + 12.0f)
                {
                    if (nv < 32)
                    {
                        vals[nv++] = t->value; /* a choice's (left to right) */
                    }
                    continue;
                }
                for (h = 0; h < nh; h++)
                {
                    float dd = fabsf(hx[h] - (t->x1 - 15.0f));
                    if (dd < bd)
                    {
                        bd = dd;
                        best = h;
                    }
                }
                if (best >= 0 && hkind[best] == -2 && t->value > d->precincts[cx->pre].ballots)
                {
                    d->precincts[cx->pre].ballots = t->value;
                }
                else if (best >= 0 && hkind[best] == -3 && t->value > 0 && t->value > d->precincts[cx->pre].registered)
                {
                    /* (McLennan's 2020 Republican canvass prints 0 for every precinct: not reported) */
                    d->precincts[cx->pre].registered = t->value;
                }
            }
        }
        /* the choices' columns in order ("Continued" holds no numbers) */
        m = 0;
        for (c = 0; c < ncols; c++)
        {
            m += cols[c].kind != 2;
        }
        if (nv != m)
        {
            cx->hl_bad_rows++;
            continue;
        }
        for (c = 0, m = 0; c < ncols; c++)
        {
            if (cols[c].kind == 2)
            {
                continue;
            }
            if (cols[c].kind == 0)
            {
                rp_count(cx, FALSE, cx->contest, cols[c].ch, EE_RPDF_NO_VT, vals[m]);
            }
            m++;
        }
    }
}

static void hl_page(RpCtx *cx, const EePdfPageText *pt, BOOL first_page)
{
    RpItem *items = NULL;
    wchar_t *pool = NULL;
    size_t n, i, k;
    float head_y = 0.0f, pre_y = 0.0f;
    BOOL table = FALSE, canvass = FALSE;

    if (pt->nruns == 0)
    {
        return;
    }
    items = (RpItem *)malloc((pt->nruns + 1) * sizeof(RpItem));
    pool = (wchar_t *)malloc((pt->text_len + pt->nruns + 1) * sizeof(wchar_t));
    if (items == NULL || pool == NULL)
    {
        cx->oom = TRUE;
        goto done;
    }
    n = rp_items(cx, pt, items, pool, TRUE);
    (void)first_page;
    /* "Party  Candidate  ...  Total" (a table) or "Precinct" over turned headings (a canvass) */
    for (i = 0; i < n && !table && !canvass; i++)
    {
        if (items[i].rot != 0)
        {
            continue;
        }
        if (wcscmp(items[i].text, L"Candidate") == 0)
        {
            for (k = 0; k < n; k++)
            {
                if (items[k].rot == 0 && fabsf(items[k].y - items[i].y) < 2.0f && wcscmp(items[k].text, L"Total") == 0)
                {
                    table = TRUE;
                    head_y = items[i].y;
                }
            }
        }
        else if (wcscmp(items[i].text, L"Precinct") == 0 && items[i].x < 120.0f)
        {
            for (k = 0; k < n; k++)
            {
                if (items[k].rot == 90)
                {
                    canvass = TRUE;
                    pre_y = items[i].y;
                    break;
                }
            }
        }
    }
    if ((table || canvass) && !cx->hl_head_done)
    {
        /* the heading of the first report page (McLennan's 2020 primaries open with a cover) */
        hl_heading(cx, items, n);
        cx->hl_head_done = TRUE;
    }
    if (table)
    {
        float cand_x1 = 0.0f;
        int nc = 0;
        for (i = 0; i < n; i++)
        {
            if (items[i].rot == 0 && fabsf(items[i].y - head_y) < 2.0f && wcscmp(items[i].text, L"Candidate") == 0)
            {
                cand_x1 = items[i].x1;
            }
        }
        for (i = 0; i < n && nc < 16; i++)
        {
            const RpItem *t = &items[i];
            if (t->rot != 0 || fabsf(t->y - head_y) >= 2.0f || t->x <= cand_x1)
            {
                continue;
            }
            cx->hl_cols[nc] = (t->x + t->x1) / 2.0f;
            cx->hl_colvt[nc] = _wcsicmp(t->text, L"Total") == 0 ? -1 : (int)hl_vt(cx, t->text);
            nc++;
        }
        cx->hl_ncols = nc;
        hl_table(cx, items, n, head_y);
    }
    else if (canvass)
    {
        hl_canvass(cx, items, n, pre_y);
    }
done:
    free(items);
    free(pool);
}

/* A precinct report meets contests precinct by precinct, so one missing from the first
 * precincts would come late. Each precinct shows its contests in ballot order: put the
 * contests in an order that agrees with every precinct (a topological sort of "comes just
 * before" pairs) and renumber the counts. Contests that never share a ballot (the districts
 * of one office) have no order of their own; they go in natural title order, as the summary
 * report lists them. */
static void rp_order_contests(RpCtx *cx)
{
    EeRpdfDoc *d = cx->d;
    uint32_t n = d->ncontests, i, done = 0;
    uint32_t *indeg = NULL, *order = NULL, *newidx = NULL, *ehead = NULL;
    uint32_t *enext = NULL, *eto = NULL;
    uint8_t *placed = NULL;
    size_t ne = 0, k;
    EeRpdfContest *nc = NULL;
    BOOL ok = FALSE;

    if (n < 2 || cx->nseq < 2)
    {
        return;
    }
    indeg = (uint32_t *)calloc(n, sizeof(uint32_t));
    order = (uint32_t *)malloc(n * sizeof(uint32_t));
    newidx = (uint32_t *)malloc(n * sizeof(uint32_t));
    ehead = (uint32_t *)malloc(n * sizeof(uint32_t));
    enext = (uint32_t *)malloc(cx->nseq * sizeof(uint32_t));
    eto = (uint32_t *)malloc(cx->nseq * sizeof(uint32_t));
    placed = (uint8_t *)calloc(n, 1);
    nc = (EeRpdfContest *)malloc(n * sizeof(EeRpdfContest));
    if (indeg == NULL || order == NULL || newidx == NULL || ehead == NULL || enext == NULL || eto == NULL ||
        placed == NULL || nc == NULL)
    {
        goto out; /* keep the order as read */
    }
    for (i = 0; i < n; i++)
    {
        ehead[i] = UINT32_MAX;
    }
    for (k = 1; k < cx->nseq; k++)
    {
        uint32_t a = cx->seq[k - 1], b = cx->seq[k];
        uint32_t e;
        if (a == UINT32_MAX || b == UINT32_MAX || a == b || a >= n || b >= n)
        {
            continue;
        }
        for (e = ehead[a]; e != UINT32_MAX; e = enext[e])
        {
            if (eto[e] == b)
            {
                break;
            }
        }
        if (e != UINT32_MAX)
        {
            continue; /* pair already known */
        }
        eto[ne] = b;
        enext[ne] = ehead[a];
        ehead[a] = (uint32_t)ne;
        ne++;
        indeg[b]++;
    }
    while (done < n)
    {
        /* of the contests with nothing left before them, the first in natural title order
         * (on a cycle, the earliest-read contest not yet placed) */
        uint32_t pick = UINT32_MAX, e;
        for (i = 0; i < n; i++)
        {
            if (!placed[i] && indeg[i] == 0 &&
                (pick == UINT32_MAX || rp_natural_cmp(d->contests[i].title, d->contests[pick].title) < 0))
            {
                pick = i;
            }
        }
        for (i = 0; pick == UINT32_MAX && i < n; i++)
        {
            if (!placed[i])
            {
                pick = i;
            }
        }
        placed[pick] = 1;
        order[done++] = pick;
        for (e = ehead[pick]; e != UINT32_MAX; e = enext[e])
        {
            if (indeg[eto[e]] > 0)
            {
                indeg[eto[e]]--;
            }
        }
    }
    for (i = 0; i < n; i++)
    {
        newidx[order[i]] = i;
        nc[i] = d->contests[order[i]];
    }
    memcpy(d->contests, nc, n * sizeof(EeRpdfContest));
    for (k = 0; k < d->ncounts; k++)
    {
        d->counts[k].contest = newidx[d->counts[k].contest];
    }
    ok = TRUE;
out:
    (void)ok;
    free(indeg);
    free(order);
    free(newidx);
    free(ehead);
    free(enext);
    free(eto);
    free(placed);
    free(nc);
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */

BOOL EeRpdf_LooksLikePdf(const void *data, size_t size)
{
    const uint8_t *d = (const uint8_t *)data;
    size_t i;
    /* "%PDF-" within the first bytes (some writers put junk first) */
    for (i = 0; i + 5 <= size && i < 1024; i++)
    {
        if (d[i] == '%' && memcmp(d + i, "%PDF-", 5) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

void EeRpdf_Free(EeRpdfDoc *doc)
{
    uint32_t i, k;
    if (doc == NULL)
    {
        return;
    }
    free(doc->election_name);
    free(doc->election_date);
    free(doc->region);
    free(doc->timestamp);
    for (i = 0; i < doc->nvt; i++)
    {
        free(doc->vote_types[i]);
    }
    free(doc->vote_types);
    for (i = 0; i < doc->nprecincts; i++)
    {
        free(doc->precincts[i].name);
    }
    free(doc->precincts);
    for (i = 0; i < doc->ncontests; i++)
    {
        EeRpdfContest *c = &doc->contests[i];
        for (k = 0; k < c->nchoices; k++)
        {
            free(c->choices[k].name);
            free(c->choices[k].party);
        }
        free(c->choices);
        free(c->title);
    }
    free(doc->contests);
    free(doc->counts);
    free(doc->turnout);
    free(doc->ballots_vt_total);
    for (i = 0; i < doc->nnotes; i++)
    {
        free(doc->notes[i]);
    }
    free(doc->notes);
    ZeroMemory(doc, sizeof(*doc));
}

BOOL EeRpdf_Parse(const void *data, size_t size, EeRpdfDoc *out, wchar_t *err, size_t errcch)
{
    return EeRpdf_ParseEx(data, size, out, NULL, NULL, err, errcch);
}

BOOL EeRpdf_ParseEx(const void *data,
                    size_t size,
                    EeRpdfDoc *out,
                    BOOL (*page_fn)(uint32_t done, uint32_t total, void *user),
                    void *user,
                    wchar_t *err,
                    size_t errcch)
{
    EePdf *pdf = NULL;
    EePdfPageText pt;
    RpCtx cx;
    wchar_t perr[256] = L"";
    uint32_t np, p, p0, hl_first = 0;
    BOOL ok = FALSE;
    BOOL glyphs_learned = FALSE; /* the text was learned through OCR (ee_glyph_ocr.c) */
    EeGlyphOcrStats gst = {0};

    ZeroMemory(out, sizeof(*out));
    out->registered_total = EE_RPDF_UNKNOWN;
    out->ballots_total = EE_RPDF_UNKNOWN;
    if (!EePdf_OpenMem(data, size, &pdf, perr, ARRAYSIZE(perr)))
    {
        wchar_t msg[400];
        StringCchPrintfW(msg, ARRAYSIZE(msg), L"The PDF could not be read (%s).", perr);
        rp_set_err(err, errcch, msg);
        return FALSE;
    }
    {
        /* a Dominion report (Microsoft Reporting Services, like Hart's): ee_results_dom.c */
        int dk = EeRdom_PdfKind(pdf);
        if (dk != 0)
        {
            ok = EeRdom_ParsePdf(pdf, dk, out, err, errcch);
            EePdf_Close(pdf);
            return ok;
        }
    }
    ZeroMemory(&cx, sizeof(cx));
    cx.d = out;
    cx.pre = UINT32_MAX;
    {
        char prod[256];
        cx.hart = EePdf_GetInfoText(pdf, "Producer", prod, sizeof(prod)) && strstr(prod, "Reporting Services") != NULL;
        cx.jasper = EePdf_GetInfoText(pdf, "Creator", prod, sizeof(prod)) && strstr(prod, "JasperReports") != NULL;
        if (!cx.jasper && EePdf_GetInfoText(pdf, "Producer", prod, sizeof(prod)) && strstr(prod, "ReportLab") != NULL)
        {
            cx.jasper = TRUE; /* Cameron's 2024-26 reports, set again in Electionware's layout through ReportLab */
            cx.aligned = TRUE;
        }
        if (cx.hart &&(cx.hcols = (struct HtCol *)calloc(RP_MAX_COLS, sizeof(struct HtCol))) == NULL)
        {
            cx.oom = TRUE;
        }
    }
    EePdf_PageTextInit(&pt);
    pt.split_gaps = 1; /* "263 1,361": two numbers shown by one TJ with a gap between */
    pt.std_widths = !cx.hart; /* Smith's "(TOTAL ) Tj [-831.5 (VOTE %)] TJ": each string at its place */
    np = EePdf_PageCount(pdf);
    if (np > 0 && EePdf_ExtractPageText(pdf, 0, &pt) && pt.unknown_glyphs >= 40)
    {
        /* printed through PScript5 (Travis L22, LR22, PR22): the text is glyph outlines with no
         * characters; learn them through OCR, then read the report as usual */
        wchar_t gerr[256] = L"";
        if (!EeGlyphOcr_Learn(pdf, np, &gst, gerr, ARRAYSIZE(gerr)))
        {
            wchar_t msg[400];
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"The report's text is drawn as glyph outlines (printed through a printer "
                             L"driver) and could not be read by OCR: %s",
                             gerr);
            EePdf_PageTextFree(&pt);
            EePdf_Close(pdf);
            free(cx.hcols);
            rp_set_err(err, errcch, msg);
            return FALSE;
        }
        glyphs_learned = TRUE;
    }
    /* a scanned cover page (Liberty's signed certificate) before the report's text pages: the
     * report starts at the first page with text */
    for (p0 = 0; p0 + 1 < np && p0 < 3; p0++)
    {
        if (!EePdf_ExtractPageText(pdf, p0, &pt) || pt.nruns > 0)
        {
            break;
        }
    }
    if (np > 0 && !glyphs_learned && EePdf_ExtractPageText(pdf, p0, &pt) && pt.nruns == 0 &&
        EeRocr_IsScannedPage(pdf, p0))
    {
        /* a scanned report (page images only): read by OCR, ee_results_ocr.c */
        EePdf_PageTextFree(&pt);
        free(cx.hcols);
        ok = EeRocr_Parse(pdf, out, page_fn, user, err, errcch);
        EePdf_Close(pdf);
        return ok;
    }
    for (p = p0; p < np && !cx.oom; p++)
    {
        if (!EePdf_ExtractPageText(pdf, p, &pt))
        {
            continue;
        }
        if (cx.hart)
        {
            hart_page(&cx, &pt, p == p0);
            if (p == p0 && !cx.hart_canvass && !cx.hart_cum)
            {
                break; /* no canvass grid or cumulative table on the first page */
            }
            continue;
        }
        if (p <= p0 + 1 && !cx.hart_old && !cx.jasper && !cx.electionware && hl_detect(&pt))
        {
            cx.hart_old = TRUE; /* (McLennan's 2020 primaries start with a cover page) */
            hl_first = p;
        }
        if (cx.hart_old)
        {
            hl_page(&cx, &pt, p == hl_first);
            if (p == hl_first + 1 && !cx.hl_table && !cx.hl_canvass)
            {
                break; /* no table or canvass grid on its first two pages */
            }
            continue;
        }
        rp_page(&cx, &pt, p == p0);
        if (p == p0 + 1 && !cx.electionware && !(cx.jasper && cx.structure))
        {
            break; /* not an Electionware report: stop early */
        }
    }
    if (cx.jasper && cx.structure)
    {
        cx.electionware = TRUE;
    }
    EePdf_PageTextFree(&pt);
    EePdf_Close(pdf);

    if (cx.oom)
    {
        rp_set_err(err, errcch, L"Out of memory reading the PDF.");
    }
    else if (!cx.any_text)
    {
        rp_set_err(err, errcch,
                   L"The PDF has no text to read, and no page image this version can read by OCR "
                   L"(JPEG 2000 and JBIG2 images are not read).");
    }
    else if (!cx.electionware && !cx.hart_canvass && !cx.hart_cum && !cx.hl_table && !cx.hl_canvass)
    {
        rp_set_err(err, errcch,
                   L"It is not a results report this version can read. Summary and precinct "
                   L"results reports from ES&S Electionware, cumulative, precinct and canvass "
                   L"reports from Hart, and Election Summary and Statement of Votes Cast reports "
                   L"from Dominion can be loaded.");
    }
    else if (cx.hart_old && (out->election_date == NULL || !rp_is_date(out->election_date)))
    {
        /* Ector's scans carry an OCR text layer ("Governor, voto For l"): its heading's
         * "<County> - <election> - <date>" line does not read, nor can its numbers be trusted */
        rp_set_err(err, errcch,
                   L"The report's text cannot be read reliably (it looks like the text layer of a "
                   L"scanned report).");
    }
    else if (out->ncontests == 0)
    {
        rp_set_err(err, errcch, L"No contests were found in the results report.");
    }
    else
    {
        wchar_t msg[300];
        if (cx.hart)
        {
            out->kind = cx.hart_canvass ? EE_RPDF_HART_PRECINCT : EE_RPDF_HART_SUMMARY;
        }
        else if (cx.hart_old)
        {
            out->kind = cx.hl_canvass         ? EE_RPDF_HART_PRECINCT
                        : out->nprecincts > 0 ? EE_RPDF_HART_PCT_REPORT
                                              : EE_RPDF_HART_SUMMARY;
        }
        else
        {
            out->kind = out->nprecincts > 0 ? EE_RPDF_ESS_PRECINCT : EE_RPDF_ESS_SUMMARY;
        }
        if (glyphs_learned)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"The report's text is drawn as glyph outlines (printed through a printer "
                             L"driver); its %u distinct characters were read by OCR over %u pages%s.",
                             gst.learned, gst.pages,
                             gst.uncertain > 0 ? L", some with few or split readings: check its figures" : L"");
            rp_note(&cx, msg);
        }
        if (cx.hl_bad_rows > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"%u canvass rows did not have a number for each choice and were not read.",
                             cx.hl_bad_rows);
            rp_note(&cx, msg);
        }
        if (out->kind == EE_RPDF_ESS_PRECINCT || out->kind == EE_RPDF_HART_PCT_REPORT)
        {
            rp_order_contests(&cx);
        }
        if (out->ballots_vt_total != NULL && out->nvt > 0)
        {
            rp_set_bvt(&cx, out->nvt - 1, out->nvt - 1 < cx.nbvt ? out->ballots_vt_total[out->nvt - 1]
                                                                 : EE_RPDF_UNKNOWN);
        }
        if (cx.writein_rows > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"%llu write-in rows (\"Write-In: name\", \"Not Assigned\") are part of "
                             L"Write-In Totals and are not listed separately.",
                             (unsigned long long)cx.writein_rows);
            rp_note(&cx, msg);
        }
        if (cx.place_pages > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"%u pages of results by polling place (the first: \"%s\") were not read; the "
                             L"precinct pages were.",
                             cx.place_pages, cx.place_first);
            rp_note(&cx, msg);
        }
        if (cx.dropped_numbers > 0)
        {
            StringCchPrintfW(msg, ARRAYSIZE(msg),
                             L"%u numbers did not line up with a column and were not read; the first: %s.",
                             cx.dropped_numbers, cx.dropped_first);
            rp_note(&cx, msg);
        }
        ok = !cx.oom;
    }
    free(cx.occ);
    free(cx.seq);
    free(cx.hcols);
    if (!ok)
    {
        EeRpdf_Free(out);
    }
    return ok;
}
