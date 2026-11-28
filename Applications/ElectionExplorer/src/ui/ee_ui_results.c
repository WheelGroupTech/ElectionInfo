/**
 * @file ee_ui_results.c
 * @brief The Election Results window's data for macOS and Linux (see ee_ui_results.h): rows,
 *        columns, cells, sorting, status line, Copy and the Load Summary, as Windows' main.c
 *        builds them (Res_BuildRows, Res_BuildColumns, Res_CellText, Res_Sort, ...).
 */
#include "ee_ui_results.h"
#include "ee_results.h"
#include "filter.h"
#include "settings.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>
#include <time.h> /* (clock_gettime: the self-test's times, on stderr; the bridge is POSIX-only) */
#include <unistd.h> /* (getpid: the self-test's scratch file) */
#include <wctype.h>

/* Kinds of column (Windows' RC_*). */
enum
{
    RC_CONTEST = 0,
    RC_VOTEFOR,
    RC_CHOICE,
    RC_PARTY,
    RC_PRECINCT,
    RC_VT, /* one vote type (col_vt) */
    RC_TOTAL,
    RC_PERCENT
};

/* Kinds of row (Windows' RR_*). */
enum
{
    RR_CHOICE = 0,
    RR_OVER,
    RR_UNDER
};

#define RES_MAX_COLS      96
#define RES_ALL_PRECINCTS UINT32_MAX

typedef struct ResRow
{
    uint32_t contest;
    uint32_t choice; /* local choice index (RR_CHOICE) */
    uint32_t pre;    /* local precinct index in the contest, or RES_ALL_PRECINCTS */
    uint32_t seq;    /* position in display order, the sort's tie-break */
    uint8_t kind;    /* RR_* */
} ResRow;

struct EeUiResults
{
    EeResults res;
    char *path;  /* the first file (UTF-8) */
    char *title; /* the window's title */
    char *summary;
    char status[1600];
    char *col_utf8[RES_MAX_COLS]; /* headings (UTF-8) */
    int by_precinct;
    int show_vt;
    int party_first;
    ResRow *rows;
    uint32_t nrows;
    int sort_col; /* -1 = display order */
    int sort_asc;
    int ncols;
    uint8_t col_kind[RES_MAX_COLS];
    uint32_t col_vt[RES_MAX_COLS];
    int col_width[RES_MAX_COLS];
    /* Tabulation (every precinct and vote type until a filter chooses some). */
    int64_t *tot;    /* nchoices */
    int64_t *tot_vt; /* nchoices * nvt */
    int64_t *ovr;    /* ncontests */
    int64_t *und;    /* ncontests */
    int64_t *ctot;   /* ncontests: the known choice totals' sum, for percentages */
    int tab_exact;
    /* Filter: is / is not rules on the EE_UI_RF_* columns (Windows' rw->filters). Precinct and
     * vote type rules choose what is counted; contest, choice and party rules hide rows. */
    EeFilterSet filters;
    int filt_active;
    uint8_t *pmask; /* nprecincts: counted precincts (NULL = all) */
    uint8_t *vmask; /* nvt: counted vote types (NULL = all) */
    uint32_t pre_included;
    char *base;     /* the first file's name without its extension, for the files it names */
};

/* -------------------------------------------------------------------------- */
/* Text                                                                       */
/* -------------------------------------------------------------------------- */

/* wide -> heap UTF-8 */
static char *utf8_of(const wchar_t *w)
{
    int n;
    char *s;
    if (w == NULL)
    {
        w = L"";
    }
    n = WideCharToMultiByte(CP_UTF8, 0, w, -1, NULL, 0, NULL, NULL);
    s = (char *)malloc(n > 0 ? (size_t)n : 1);
    if (s != NULL && (n <= 0 || WideCharToMultiByte(CP_UTF8, 0, w, -1, s, n, NULL, NULL) <= 0))
    {
        s[0] = '\0';
    }
    return s;
}

/* wide -> UTF-8 into a buffer (cut short when it does not fit) */
static const char *utf8_into(const wchar_t *w, char *buf, size_t cap)
{
    if (buf == NULL || cap == 0)
    {
        return "";
    }
    buf[0] = '\0';
    if (w != NULL && w[0] != L'\0' &&
        WideCharToMultiByte(CP_UTF8,
                            0,
                            w,
                            -1,
                            buf,
                            (int)(cap > 0x7FFFFFFF ? 0x7FFFFFFF : cap),
                            NULL,
                            NULL) <= 0)
    {
        /* too long for the buffer: as much as fits, by whole characters */
        char *s = utf8_of(w);
        size_t n;
        if (s == NULL)
        {
            return buf;
        }
        n = strlen(s);
        if (n >= cap)
        {
            n = cap - 1;
            while (n > 0 && ((unsigned char)s[n] & 0xC0) == 0x80)
            {
                n--;
            }
        }
        memcpy(buf, s, n);
        buf[n] = '\0';
        free(s);
    }
    return buf;
}

/* UTF-8 -> heap wide */
static wchar_t *wide_of(const char *s)
{
    int n = MultiByteToWideChar(CP_UTF8, 0, s != NULL ? s : "", -1, NULL, 0);
    wchar_t *w = n > 0 ? (wchar_t *)malloc((size_t)n * sizeof(wchar_t)) : NULL;
    if (w != NULL && MultiByteToWideChar(CP_UTF8, 0, s != NULL ? s : "", -1, w, n) <= 0)
    {
        w[0] = L'\0';
    }
    return w;
}

/* a path's last part (Windows' App_PathBaseName: either slash) */
static const wchar_t *base_name(const wchar_t *path)
{
    const wchar_t *p, *b;
    if (path == NULL)
    {
        return L"";
    }
    for (p = b = path; *p; p++)
    {
        if (*p == L'/' || *p == L'\\')
        {
            b = p + 1;
        }
    }
    return b;
}

/* a growing wide text */
typedef struct WBuf
{
    wchar_t *p;
    size_t len, cap;
    int failed;
} WBuf;

static void wb_cat(WBuf *b, const wchar_t *s)
{
    size_t n = wcslen(s);
    if (b->failed)
    {
        return;
    }
    if (b->len + n + 1 > b->cap)
    {
        size_t cap = b->cap ? b->cap : 256;
        wchar_t *p;
        while (b->len + n + 1 > cap)
        {
            cap *= 2;
        }
        p = (wchar_t *)realloc(b->p, cap * sizeof(wchar_t));
        if (p == NULL)
        {
            b->failed = 1;
            return;
        }
        b->p = p;
        b->cap = cap;
    }
    memcpy(b->p + b->len, s, n * sizeof(wchar_t));
    b->len += n;
    b->p[b->len] = L'\0';
}

/* a growing UTF-8 text */
typedef struct CBuf
{
    char *p;
    size_t len, cap;
    int failed;
} CBuf;

static void cb_cat(CBuf *b, const char *s, size_t n)
{
    if (b->failed)
    {
        return;
    }
    if (b->len + n + 1 > b->cap)
    {
        size_t cap = b->cap ? b->cap : 1024;
        char *p;
        while (b->len + n + 1 > cap)
        {
            cap *= 2;
        }
        p = (char *)realloc(b->p, cap);
        if (p == NULL)
        {
            b->failed = 1;
            return;
        }
        b->p = p;
        b->cap = cap;
    }
    memcpy(b->p + b->len, s, n);
    b->len += n;
    b->p[b->len] = '\0';
}

/* -------------------------------------------------------------------------- */
/* Loading                                                                    */
/* -------------------------------------------------------------------------- */

typedef struct ProgressCx
{
    EeUiProgressFn fn;
    void *user;
} ProgressCx;

static BOOL on_progress(const EeLoadProgress *p, void *user)
{
    ProgressCx *c = (ProgressCx *)user;
    return c->fn == NULL || c->fn(c->user, p->percent > 100u ? 100u : p->percent, 0) != 0;
}

static void free_tab(EeUiResults *r)
{
    free(r->tot);
    free(r->tot_vt);
    free(r->ovr);
    free(r->und);
    free(r->ctot);
    r->tot = r->tot_vt = r->ovr = r->und = r->ctot = NULL;
}

/* Windows' Res_Tabulate */
static int tabulate(EeUiResults *u)
{
    const EeResults *r = &u->res;
    uint32_t c, k;
    free_tab(u);
    u->tot = (int64_t *)calloc((size_t)r->nchoices + 1, sizeof(int64_t));
    u->tot_vt = (int64_t *)calloc((size_t)r->nchoices * EE_RESULTS_NVT(r) + 1, sizeof(int64_t));
    u->ovr = (int64_t *)calloc((size_t)r->ncontests + 1, sizeof(int64_t));
    u->und = (int64_t *)calloc((size_t)r->ncontests + 1, sizeof(int64_t));
    u->ctot = (int64_t *)calloc((size_t)r->ncontests + 1, sizeof(int64_t));
    if (u->tot == NULL || u->tot_vt == NULL || u->ovr == NULL || u->und == NULL || u->ctot == NULL)
    {
        free_tab(u);
        return 0;
    }
    u->tab_exact = EeResults_Tabulate(r,
                                      u->pmask,
                                      u->vmask,
                                      u->tot,
                                      r->nvt > 0 ? u->tot_vt : NULL,
                                      u->ovr,
                                      u->und)
                       ? 1
                       : 0;
    for (c = 0; c < r->ncontests; c++)
    {
        const EeResultsContest *ct = &r->contests[c];
        for (k = 0; k < ct->nchoices; k++)
        {
            int64_t v = u->tot[ct->first_choice + k];
            if (v != EE_RES_UNKNOWN)
            {
                u->ctot[c] += v;
            }
        }
    }
    return 1;
}

void EeUiResults_Free(EeUiResults *r)
{
    int i;
    if (r == NULL)
    {
        return;
    }
    free_tab(r);
    free(r->rows);
    free(r->pmask);
    free(r->vmask);
    for (i = 0; i < RES_MAX_COLS; i++)
    {
        free(r->col_utf8[i]);
    }
    free(r->path);
    free(r->title);
    free(r->summary);
    free(r->base);
    EeFilter_Clear(&r->filters);
    EeResults_Clear(&r->res);
    free(r);
}

/* a result for the window, taking @p res over (Windows' App_CreateResultsWindow) */
static EeUiResults *results_new(EeResults *res)
{
    EeUiResults *u = (EeUiResults *)calloc(1, sizeof(EeUiResults));
    const wchar_t *first;
    wchar_t title[320];
    if (u == NULL)
    {
        EeResults_Clear(res);
        return NULL;
    }
    u->res = *res; /* (moved) */
    EeResults_Init(res);
    u->sort_col = -1;
    u->sort_asc = 1;
    u->show_vt = 1;
    u->party_first = EE_UI_PARTY_REP;
    EeFilter_Init(&u->filters);
    first = u->res.nsources > 0 && u->res.sources[0].path != NULL ? u->res.sources[0].path : L"";
    StringCchPrintfW(title,
                     320,
                     L"%ls \x2014 Election Results",
                     u->res.election_name != NULL ? u->res.election_name : base_name(first));
    u->path = utf8_of(first);
    u->title = utf8_of(title);
    u->base = utf8_of(base_name(first));
    if (u->base != NULL && strrchr(u->base, '.') != NULL && strrchr(u->base, '.') != u->base)
    {
        *strrchr(u->base, '.') = '\0'; /* (Windows' App_BaseNameNoExt) */
    }
    if (u->path == NULL || u->title == NULL || u->base == NULL || !tabulate(u) ||
        !EeUiResults_SetView(u, 0, u->show_vt, u->party_first))
    {
        EeUiResults_Free(u);
        return NULL;
    }
    return u;
}

int EeUiResults_Load(const char *const *paths_utf8,
                     uint32_t n,
                     EeUiProgressFn fn,
                     void *user,
                     volatile int32_t *cancel,
                     EeUiResults ***out,
                     uint32_t *nout,
                     char *err,
                     size_t errcap)
{
    wchar_t **paths;
    wchar_t werr[512];
    EeResults *results = NULL;
    uint32_t nresults = 0, i;
    EeLoadStatus st;
    ProgressCx cx;
    int ok = 1;
    if (out != NULL)
    {
        *out = NULL;
    }
    if (nout != NULL)
    {
        *nout = 0;
    }
    if (err != NULL && errcap > 0)
    {
        err[0] = '\0';
    }
    if (paths_utf8 == NULL || n == 0 || out == NULL || nout == NULL)
    {
        return EE_UI_RESULTS_ERROR;
    }
    paths = (wchar_t **)calloc(n, sizeof(wchar_t *));
    for (i = 0; paths != NULL && i < n && ok; i++)
    {
        paths[i] = wide_of(paths_utf8[i]);
        ok = paths[i] != NULL;
    }
    if (paths == NULL || !ok)
    {
        for (i = 0; paths != NULL && i < n; i++)
        {
            free(paths[i]);
        }
        free(paths);
        StringCchCopyA(err, errcap, "Out of memory.");
        return EE_UI_RESULTS_ERROR;
    }
    cx.fn = fn;
    cx.user = user;
    werr[0] = L'\0';
    st = EeResults_LoadFiles((const wchar_t *const *)paths,
                             n,
                             &results,
                             &nresults,
                             (volatile LONG *)cancel,
                             on_progress,
                             &cx,
                             werr,
                             512);
    for (i = 0; i < n; i++)
    {
        free(paths[i]);
    }
    free(paths);
    if (st == EeLoadStatus_Cancelled)
    {
        EeResults_FreeArray(results, nresults);
        return EE_UI_RESULTS_CANCELLED;
    }
    if (st != EeLoadStatus_Ok)
    {
        EeResults_FreeArray(results, nresults);
        utf8_into(werr[0] != L'\0' ? werr : L"Could not load the results.", err, errcap);
        return EE_UI_RESULTS_ERROR;
    }
    *out = (EeUiResults **)calloc(nresults > 0 ? nresults : 1, sizeof(EeUiResults *));
    for (i = 0; *out != NULL && i < nresults; i++)
    {
        (*out)[i] = results_new(&results[i]);
        if ((*out)[i] == NULL)
        {
            uint32_t j;
            for (j = 0; j < i; j++)
            {
                EeUiResults_Free((*out)[j]);
            }
            free(*out);
            *out = NULL;
        }
    }
    EeResults_FreeArray(results, nresults); /* (the moved ones are empty) */
    if (*out == NULL)
    {
        StringCchCopyA(err, errcap, "Out of memory.");
        return EE_UI_RESULTS_ERROR;
    }
    *nout = nresults;
    return EE_UI_RESULTS_OK;
}

const char *EeUiResults_WindowTitle(const EeUiResults *r)
{
    return r != NULL && r->title != NULL ? r->title : "Election Results";
}

const char *EeUiResults_Path(const EeUiResults *r)
{
    return r != NULL && r->path != NULL ? r->path : "";
}

int EeUiResults_HasPrecinctDetail(const EeUiResults *r)
{
    return r != NULL && EeResults_HasPrecinctDetail(&r->res) ? 1 : 0;
}

/* Windows' Res_HasDifferences */
int EeUiResults_HasDifferences(const EeUiResults *r)
{
    uint32_t i;
    for (i = 0; r != NULL && i < r->res.nissues; i++)
    {
        if (r->res.issues[i].source != 0)
        {
            return 1;
        }
    }
    return 0;
}

/* Windows' App_ShowResultsLoadSummary */
const char *EeUiResults_LoadSummary(EeUiResults *u)
{
    const EeResults *r;
    WBuf b = {NULL, 0, 0, 0};
    uint32_t i, s;
    char *text, *d;
    const char *p;
    if (u == NULL)
    {
        return "";
    }
    if (u->summary != NULL)
    {
        return u->summary;
    }
    r = &u->res;
    wb_cat(&b, L"Files:\n");
    for (s = 0; s < r->nsources; s++)
    {
        const EeResultsSource *src = &r->sources[s];
        wchar_t line[1400];
        StringCchPrintfW(line,
                         1400,
                         L"%u. %ls%ls%ls%ls \x2014 %ls: %u contests, %u choices, %u precincts%ls\n",
                         s + 1,
                         base_name(src->path),
                         src->member != NULL ? L" (" : L"",
                         src->member != NULL ? src->member : L"",
                         src->member != NULL ? L")" : L"",
                         EeResults_FormatLabel(src->format),
                         src->ncontests,
                         src->nchoices,
                         src->nprecincts,
                         src->primary ? L" \x2014 shown" : L"");
        wb_cat(&b, line);
    }
    for (s = 0; s < r->nsources; s++)
    {
        int header = 0;
        for (i = 0; i < r->nissues; i++)
        {
            if (r->issues[i].source != s)
            {
                continue;
            }
            if (!header)
            {
                wchar_t line[96];
                if (s == 0)
                {
                    StringCchCopyW(line, 96, L"\nNotes on file 1 (shown):\n");
                }
                else
                {
                    StringCchPrintfW(line, 96, L"\nFile %u compared with file 1:\n", s + 1);
                }
                wb_cat(&b, line);
                header = 1;
            }
            wb_cat(&b, L"\x2022 ");
            wb_cat(&b, r->issues[i].text != NULL ? r->issues[i].text : L"");
            wb_cat(&b, L"\n");
        }
        if (!header && s > 0)
        {
            wchar_t line[96];
            StringCchPrintfW(line, 96, L"\nFile %u agrees with file 1.\n", s + 1);
            wb_cat(&b, line);
        }
    }
    text = b.failed ? NULL : utf8_of(b.p);
    free(b.p);
    if (text == NULL)
    {
        return "";
    }
    /* (an issue's own "\r\n" without the "\r", and no line end at the very end) */
    for (p = text, d = text; *p; p++)
    {
        if (*p != '\r')
        {
            *d++ = *p;
        }
    }
    while (d > text && d[-1] == '\n')
    {
        d--;
    }
    *d = '\0';
    u->summary = text;
    return u->summary;
}

/* -------------------------------------------------------------------------- */
/* Options                                                                    */
/* -------------------------------------------------------------------------- */

void EeUiResultsOptions_Load(EeUiResultsOptions *out)
{
    EeSettings s;
    if (out == NULL)
    {
        return;
    }
    EeSettings_Load(&s); /* (the defaults for any value never saved) */
    out->zoom_percent = EeUiOptions_ClampZoom(s.zoom_percent);
    out->show_vote_types = s.results_show_vote_types ? 1 : 0;
    out->party_first =
        s.cvr_tab_party_first == EE_TAB_PARTY_DEM ? EE_UI_PARTY_DEM : EE_UI_PARTY_REP;
}

int EeUiResultsOptions_Save(const EeUiResultsOptions *in)
{
    EeSettings s;
    if (in == NULL)
    {
        return 0;
    }
    EeSettings_Load(&s); /* (the settings other windows keep stay as they are) */
    s.zoom_percent = EeUiOptions_ClampZoom(in->zoom_percent);
    s.results_show_vote_types = in->show_vote_types ? TRUE : FALSE;
    s.cvr_tab_party_first =
        in->party_first == EE_UI_PARTY_DEM ? EE_TAB_PARTY_DEM : EE_TAB_PARTY_REP;
    return EeSettings_Save(&s) ? 1 : 0;
}

/* -------------------------------------------------------------------------- */
/* Rows                                                                       */
/* -------------------------------------------------------------------------- */

/* Windows' Res_PartyRank: 0 = the party shown first, 1 = the other major party, 2 = the rest */
static int party_rank(const EeUiResults *u, const EeResultsContest *c)
{
    int rep = c->party != NULL && _wcsicmp(c->party, L"REP") == 0;
    int dem = c->party != NULL && _wcsicmp(c->party, L"DEM") == 0;
    if (!rep && !dem)
    {
        return 2;
    }
    if (u->party_first == EE_UI_PARTY_DEM)
    {
        return dem ? 0 : 1;
    }
    return rep ? 0 : 1;
}

/* One column's rules on @p value (Windows' Filter_ColumnAccepts, ProcMon-style): an enabled
 * exclude that matches rejects; when the column has enabled includes, one must match. Rules
 * on other columns are left out (each column filters its own dimension). */
static int column_accepts(const EeFilterSet *set, uint32_t column, const wchar_t *value)
{
    uint32_t i;
    int any_include = 0, included = 0;
    for (i = 0; i < set->count; i++)
    {
        const EeFilterRule *r = &set->rules[i];
        int eq;
        if (!r->enabled || r->column != column)
        {
            continue;
        }
        eq = CompareStringOrdinal(value, -1, r->value, -1, TRUE) == CSTR_EQUAL;
        if (r->relation == EeRel_IsNot)
        {
            eq = !eq;
        }
        if (r->action == EeFilt_Exclude)
        {
            if (eq)
            {
                return 0;
            }
        }
        else
        {
            any_include = 1;
            included = included || eq;
        }
    }
    return !any_include || included;
}

/* Is a row shown under the contest / choice / party rules? (Windows' Res_RowPasses) */
static int row_passes(const EeUiResults *u, uint32_t contest, uint8_t kind, uint32_t choice)
{
    const EeResultsContest *ct = &u->res.contests[contest];
    const wchar_t *name, *party = L"";
    if (!u->filt_active)
    {
        return 1;
    }
    if (kind == RR_OVER)
    {
        name = L"Over Votes";
    }
    else if (kind == RR_UNDER)
    {
        name = L"Under Votes";
    }
    else
    {
        name = u->res.choices[ct->first_choice + choice].name;
        if (u->res.choices[ct->first_choice + choice].party != NULL)
        {
            party = u->res.choices[ct->first_choice + choice].party;
        }
    }
    return column_accepts(&u->filters, EE_UI_RF_CONTEST, ct->title != NULL ? ct->title : L"") &&
           column_accepts(&u->filters, EE_UI_RF_CHOICE, name != NULL ? name : L"") &&
           column_accepts(&u->filters, EE_UI_RF_PARTY, party);
}

/* Does a contest's district include a counted precinct? (Windows' Res_ContestHasPrecinct) */
static int contest_has_precinct(const EeUiResults *u, const EeResultsContest *ct)
{
    uint32_t p;
    if (u->pmask == NULL || ct->counts == NULL)
    {
        return 1;
    }
    for (p = 0; p < ct->npre; p++)
    {
        if (u->pmask[ct->pre[p]])
        {
            return 1;
        }
    }
    return 0;
}

/* Windows' Res_BuildRows: one pass to count, one to fill */
static int build_rows(EeUiResults *u)
{
    const EeResults *r = &u->res;
    size_t n = 0;
    ResRow *rows = NULL;
    int pass;
    uint32_t c;
    for (pass = 0; pass < 2; pass++)
    {
        int rank;
        n = 0;
        for (rank = 0; rank < 3; rank++)
        {
            for (c = 0; c < r->ncontests; c++)
            {
                const EeResultsContest *ct = &r->contests[c];
                uint32_t k, p;
                uint32_t npre = (u->by_precinct && ct->counts != NULL) ? ct->npre : 1;
                if (party_rank(u, ct) != rank || !contest_has_precinct(u, ct))
                {
                    continue;
                }
                for (p = 0; p < npre; p++)
                {
                    uint32_t pre = (u->by_precinct && ct->counts != NULL) ? p : RES_ALL_PRECINCTS;
                    uint32_t extra = 0;
                    if (pre != RES_ALL_PRECINCTS && u->pmask != NULL && !u->pmask[ct->pre[p]])
                    {
                        continue; /* a precinct the filter leaves out */
                    }
                    if (ct->has_over_under &&
                        (pre == RES_ALL_PRECINCTS || (ct->over != NULL && ct->under != NULL)))
                    {
                        extra = 2;
                    }
                    for (k = 0; k < ct->nchoices; k++)
                    {
                        if (!row_passes(u, c, RR_CHOICE, k))
                        {
                            continue;
                        }
                        if (pass == 1)
                        {
                            rows[n].contest = c;
                            rows[n].choice = k;
                            rows[n].pre = pre;
                            rows[n].kind = RR_CHOICE;
                            rows[n].seq = (uint32_t)n;
                        }
                        n++;
                    }
                    for (k = 0; k < extra; k++)
                    {
                        uint8_t kind = (uint8_t)(k == 0 ? RR_OVER : RR_UNDER);
                        if (!row_passes(u, c, kind, 0))
                        {
                            continue;
                        }
                        if (pass == 1)
                        {
                            rows[n].contest = c;
                            rows[n].choice = 0;
                            rows[n].pre = pre;
                            rows[n].kind = kind;
                            rows[n].seq = (uint32_t)n;
                        }
                        n++;
                    }
                }
            }
        }
        if (pass == 0)
        {
            rows = (ResRow *)malloc((n + 1) * sizeof(ResRow));
            if (rows == NULL)
            {
                return 0;
            }
        }
    }
    free(u->rows);
    u->rows = rows;
    u->nrows = (uint32_t)n;
    return 1;
}

/* Votes of a row in one vote type (v < nvt), or over all (v == UINT32_MAX); EE_RES_UNKNOWN
 * for a withheld cell or total (Windows' Res_RowVotes). */
static int64_t row_votes(const EeUiResults *u, const ResRow *row, uint32_t v)
{
    const EeResults *r = &u->res;
    const EeResultsContest *ct = &r->contests[row->contest];
    uint32_t nvt = EE_RESULTS_NVT(r);
    if (row->pre == RES_ALL_PRECINCTS)
    {
        if (row->kind == RR_OVER)
        {
            return v == UINT32_MAX ? u->ovr[row->contest] : EE_RES_UNKNOWN;
        }
        if (row->kind == RR_UNDER)
        {
            return v == UINT32_MAX ? u->und[row->contest] : EE_RES_UNKNOWN;
        }
        if (v == UINT32_MAX)
        {
            return u->tot[ct->first_choice + row->choice];
        }
        return (r->nvt > 0 && ct->counts != NULL)
                   ? u->tot_vt[(size_t)(ct->first_choice + row->choice) * r->nvt + v]
                   : EE_RES_UNKNOWN;
    }
    if (row->kind == RR_OVER || row->kind == RR_UNDER)
    {
        const int32_t *a = row->kind == RR_OVER ? ct->over : ct->under;
        return (a != NULL && v == UINT32_MAX) ? a[row->pre] : EE_RES_UNKNOWN;
    }
    {
        size_t base = ((size_t)row->choice * ct->npre + row->pre) * nvt;
        int64_t s = 0;
        uint32_t q;
        if (v != UINT32_MAX)
        {
            if (ct->protected_cells != NULL && ct->protected_cells[base + v])
            {
                return EE_RES_UNKNOWN;
            }
            return ct->counts[base + v];
        }
        for (q = 0; q < nvt; q++)
        {
            if (r->nvt == 0 || u->vmask == NULL || u->vmask[q])
            {
                s += ct->counts[base + q];
            }
        }
        return s;
    }
}

/* all the choices' votes of a row's contest in its precinct, for percentages (Res_RowBase) */
static int64_t row_base(const EeUiResults *u, const ResRow *row)
{
    const EeResults *r = &u->res;
    const EeResultsContest *ct = &r->contests[row->contest];
    uint32_t nvt = EE_RESULTS_NVT(r);
    int64_t s = 0;
    uint32_t k, q;
    if (row->pre == RES_ALL_PRECINCTS)
    {
        return u->ctot[row->contest];
    }
    for (k = 0; k < ct->nchoices; k++)
    {
        const int32_t *cell = ct->counts + ((size_t)k * ct->npre + row->pre) * nvt;
        for (q = 0; q < nvt; q++)
        {
            if (r->nvt == 0 || u->vmask == NULL || u->vmask[q])
            {
                s += cell[q];
            }
        }
    }
    return s;
}

static const wchar_t *row_choice_name(const EeUiResults *u, const ResRow *row)
{
    const EeResultsContest *ct = &u->res.contests[row->contest];
    if (row->kind == RR_OVER)
    {
        return L"Over Votes";
    }
    if (row->kind == RR_UNDER)
    {
        return L"Under Votes";
    }
    return u->res.choices[ct->first_choice + row->choice].name;
}

static const wchar_t *row_precinct(const EeUiResults *u, const ResRow *row)
{
    const EeResultsContest *ct = &u->res.contests[row->contest];
    if (row->pre == RES_ALL_PRECINCTS)
    {
        return L"";
    }
    return u->res.precincts[ct->pre[row->pre]].name;
}

/* -------------------------------------------------------------------------- */
/* Columns and cells                                                          */
/* -------------------------------------------------------------------------- */

/* Windows' Res_BuildColumns */
static void build_columns(EeUiResults *u)
{
    const EeResults *r = &u->res;
    int n = 0, i;
    uint32_t v;
    for (i = 0; i < RES_MAX_COLS; i++)
    {
        free(u->col_utf8[i]);
        u->col_utf8[i] = NULL;
    }
#define RES_ADD_COL(k, t, w, vv)                                                                   \
    do                                                                                             \
    {                                                                                              \
        if (n < RES_MAX_COLS)                                                                      \
        {                                                                                          \
            u->col_kind[n] = (uint8_t)(k);                                                         \
            u->col_utf8[n] = utf8_of(t);                                                           \
            u->col_width[n] = (w);                                                                 \
            u->col_vt[n] = (vv);                                                                   \
            n++;                                                                                   \
        }                                                                                          \
    } while (0)
    RES_ADD_COL(RC_CONTEST, L"Contest", 320, 0);
    RES_ADD_COL(RC_VOTEFOR, L"Vote For", 64, 0);
    RES_ADD_COL(RC_CHOICE, L"Choice", 240, 0);
    RES_ADD_COL(RC_PARTY, L"Party", 60, 0);
    if (u->by_precinct)
    {
        RES_ADD_COL(RC_PRECINCT, L"Precinct", 120, 0);
    }
    if (u->show_vt)
    {
        for (v = 0; v < r->nvt; v++)
        {
            if (u->vmask == NULL || u->vmask[v])
            {
                RES_ADD_COL(RC_VT, r->vote_types[v], 100, v);
            }
        }
    }
    RES_ADD_COL(RC_TOTAL, L"Total Votes", 96, 0);
    RES_ADD_COL(RC_PERCENT, L"Percent", 72, 0);
#undef RES_ADD_COL
    u->ncols = n;
}

/* Windows' Res_CellText, in wide */
static void cell_text(const EeUiResults *u, uint32_t ri, int col, wchar_t *out, size_t cch)
{
    const ResRow *row;
    const EeResultsContest *ct;
    int64_t v;
    if (cch == 0)
    {
        return;
    }
    out[0] = L'\0';
    if (ri >= u->nrows || col < 0 || col >= u->ncols)
    {
        return;
    }
    row = &u->rows[ri];
    ct = &u->res.contests[row->contest];
    switch (u->col_kind[col])
    {
        case RC_CONTEST:
            StringCchCopyW(out, cch, ct->title != NULL ? ct->title : L"");
            break;
        case RC_VOTEFOR:
            if (ct->vote_for > 0)
            {
                StringCchPrintfW(out, cch, L"%u", ct->vote_for);
            }
            break;
        case RC_CHOICE:
            StringCchCopyW(out, cch, row_choice_name(u, row));
            break;
        case RC_PARTY:
            if (row->kind == RR_CHOICE)
            {
                const wchar_t *p = u->res.choices[ct->first_choice + row->choice].party;
                StringCchCopyW(out, cch, p != NULL ? p : L"");
            }
            break;
        case RC_PRECINCT:
            StringCchCopyW(out,
                           cch,
                           row->pre == RES_ALL_PRECINCTS ? L"(all)" : row_precinct(u, row));
            break;
        case RC_VT:
        case RC_TOTAL:
            v = row_votes(u, row, u->col_kind[col] == RC_VT ? u->col_vt[col] : UINT32_MAX);
            if (v != EE_RES_UNKNOWN)
            {
                StringCchPrintfW(out, cch, L"%lld", (long long)v);
            }
            else if (row->kind == RR_CHOICE && row->pre != RES_ALL_PRECINCTS &&
                     u->col_kind[col] == RC_VT)
            {
                StringCchCopyW(out, cch, L"protected");
            }
            else if (row->kind == RR_CHOICE && u->col_kind[col] == RC_TOTAL)
            {
                StringCchCopyW(out, cch, L"withheld");
            }
            break;
        case RC_PERCENT:
            if (row->kind == RR_CHOICE)
            {
                int64_t base = row_base(u, row);
                v = row_votes(u, row, UINT32_MAX);
                if (base > 0 && v != EE_RES_UNKNOWN)
                {
                    StringCchPrintfW(out, cch, L"%.2f%%", (double)v * 100.0 / (double)base);
                }
            }
            break;
        default:
            break;
    }
}

/* a numeric column's value, for sorting (-1 when blank: Windows' Res_CellNumber) */
static double cell_number(const EeUiResults *u, const ResRow *row, int col)
{
    int64_t v;
    switch (u->col_kind[col])
    {
        case RC_VOTEFOR:
            return (double)u->res.contests[row->contest].vote_for;
        case RC_VT:
        case RC_TOTAL:
            v = row_votes(u, row, u->col_kind[col] == RC_VT ? u->col_vt[col] : UINT32_MAX);
            return v == EE_RES_UNKNOWN ? -1.0 : (double)v;
        case RC_PERCENT:
            if (row->kind == RR_CHOICE)
            {
                int64_t base = row_base(u, row);
                v = row_votes(u, row, UINT32_MAX);
                if (base > 0 && v != EE_RES_UNKNOWN)
                {
                    return (double)v * 100.0 / (double)base;
                }
            }
            return -1.0;
        default:
            return 0.0;
    }
}

static const wchar_t *cell_string(const EeUiResults *u, const ResRow *row, int col)
{
    const EeResultsContest *ct = &u->res.contests[row->contest];
    switch (u->col_kind[col])
    {
        case RC_CONTEST:
            return ct->title != NULL ? ct->title : L"";
        case RC_CHOICE:
            return row_choice_name(u, row);
        case RC_PARTY:
            if (row->kind == RR_CHOICE)
            {
                const wchar_t *p = u->res.choices[ct->first_choice + row->choice].party;
                return p != NULL ? p : L"";
            }
            return L"";
        case RC_PRECINCT:
            return row_precinct(u, row);
        default:
            return L"";
    }
}

/* two texts, as numbers when both are whole numbers (precinct 99 < 100: Res_CompareText) */
static int compare_text(const wchar_t *a, const wchar_t *b)
{
    if (*a != L'\0' && *b != L'\0' && iswdigit((wint_t)*a) && iswdigit((wint_t)*b))
    {
        wchar_t *ea, *eb;
        unsigned long long na = wcstoull(a, &ea, 10);
        unsigned long long nb = wcstoull(b, &eb, 10);
        if (*ea == L'\0' && *eb == L'\0')
        {
            return na < nb ? -1 : (na > nb ? 1 : 0);
        }
    }
    return _wcsicmp(a, b);
}

typedef struct SortCx
{
    const EeUiResults *u;
    int col;
    int asc;
} SortCx;

/* Windows' Res_SortCmp */
static int sort_cmp(void *ctx, const void *pa, const void *pb)
{
    const SortCx *sc = (const SortCx *)ctx;
    const ResRow *a = (const ResRow *)pa;
    const ResRow *b = (const ResRow *)pb;
    int r;
    uint8_t kind = sc->u->col_kind[sc->col];
    if (kind == RC_VOTEFOR || kind == RC_VT || kind == RC_TOTAL || kind == RC_PERCENT)
    {
        double x = cell_number(sc->u, a, sc->col);
        double y = cell_number(sc->u, b, sc->col);
        r = x < y ? -1 : (x > y ? 1 : 0);
    }
    else
    {
        r = compare_text(cell_string(sc->u, a, sc->col), cell_string(sc->u, b, sc->col));
    }
    if (!sc->asc)
    {
        r = -r;
    }
    if (r == 0)
    {
        r = a->seq < b->seq ? -1 : (a->seq > b->seq ? 1 : 0);
    }
    return r;
}

static int seq_cmp(const void *pa, const void *pb)
{
    const ResRow *a = (const ResRow *)pa;
    const ResRow *b = (const ResRow *)pb;
    return a->seq < b->seq ? -1 : (a->seq > b->seq ? 1 : 0);
}

/* Windows' Res_Sort */
static void sort_rows(EeUiResults *u)
{
    if (u->nrows < 2)
    {
        return;
    }
    if (u->sort_col < 0 || u->sort_col >= u->ncols)
    {
        qsort(u->rows, u->nrows, sizeof(ResRow), seq_cmp);
    }
    else
    {
        SortCx sc;
        sc.u = u;
        sc.col = u->sort_col;
        sc.asc = u->sort_asc;
        qsort_s(u->rows, u->nrows, sizeof(ResRow), sort_cmp, &sc);
    }
}

/* Windows' Res_Rebuild: rows and columns again, keeping the sort if its column is still there */
int EeUiResults_SetView(EeUiResults *u, int by_precinct, int show_vote_types, int party_first)
{
    uint8_t sort_kind;
    uint32_t sort_vt;
    int i, ok;
    if (u == NULL)
    {
        return 0;
    }
    sort_kind = (u->sort_col >= 0 && u->sort_col < u->ncols) ? u->col_kind[u->sort_col] : 0xFF;
    sort_vt = (u->sort_col >= 0 && u->sort_col < u->ncols) ? u->col_vt[u->sort_col] : 0;
    u->by_precinct = by_precinct && EeResults_HasPrecinctDetail(&u->res) ? 1 : 0;
    u->show_vt = show_vote_types ? 1 : 0;
    u->party_first = party_first == EE_UI_PARTY_DEM ? EE_UI_PARTY_DEM : EE_UI_PARTY_REP;
    ok = build_rows(u);
    if (!ok)
    {
        u->nrows = 0;
    }
    build_columns(u);
    u->sort_col = -1;
    for (i = 0; i < u->ncols && sort_kind != 0xFF; i++)
    {
        if (u->col_kind[i] == sort_kind && (sort_kind != RC_VT || u->col_vt[i] == sort_vt))
        {
            u->sort_col = i;
            break;
        }
    }
    sort_rows(u);
    return ok;
}

int EeUiResults_ByPrecinct(const EeUiResults *r)
{
    return r != NULL ? r->by_precinct : 0;
}

int EeUiResults_ShowVoteTypes(const EeUiResults *r)
{
    return r != NULL ? r->show_vt : 0;
}

int EeUiResults_PartyFirst(const EeUiResults *r)
{
    return r != NULL ? r->party_first : EE_UI_PARTY_REP;
}

uint32_t EeUiResults_Rows(const EeUiResults *r)
{
    return r != NULL ? r->nrows : 0;
}

int EeUiResults_Columns(const EeUiResults *r)
{
    return r != NULL ? r->ncols : 0;
}

const char *EeUiResults_ColumnTitle(const EeUiResults *r, int col)
{
    return r != NULL && col >= 0 && col < r->ncols && r->col_utf8[col] != NULL ? r->col_utf8[col]
                                                                               : "";
}

int EeUiResults_ColumnWidth(const EeUiResults *r, int col)
{
    return r != NULL && col >= 0 && col < r->ncols ? r->col_width[col] : 100;
}

int EeUiResults_ColumnNumeric(const EeUiResults *r, int col)
{
    uint8_t k;
    if (r == NULL || col <= 0 || col >= r->ncols) /* (column 0 is always left-aligned) */
    {
        return 0;
    }
    k = r->col_kind[col];
    return k == RC_VOTEFOR || k == RC_VT || k == RC_TOTAL || k == RC_PERCENT;
}

const char *EeUiResults_Cell(const EeUiResults *r, uint32_t row, int col, char *buf, size_t cap)
{
    wchar_t cell[1024];
    if (buf == NULL || cap == 0)
    {
        return "";
    }
    buf[0] = '\0';
    if (r == NULL)
    {
        return buf;
    }
    cell_text(r, row, col, cell, 1024);
    return utf8_into(cell, buf, cap);
}

void EeUiResults_Sort(EeUiResults *r, int col, int ascending)
{
    if (r == NULL)
    {
        return;
    }
    r->sort_col = col >= 0 && col < r->ncols ? col : -1;
    r->sort_asc = ascending ? 1 : 0;
    sort_rows(r);
}

/* Windows' LVN_COLUMNCLICK */
void EeUiResults_ClickHeading(EeUiResults *r, int col)
{
    uint8_t k;
    if (r == NULL || col < 0 || col >= r->ncols)
    {
        return;
    }
    if (col == r->sort_col)
    {
        r->sort_asc = !r->sort_asc;
    }
    else
    {
        r->sort_col = col;
        k = r->col_kind[col];
        /* counts and percentages sort largest first */
        r->sort_asc = !(k == RC_VT || k == RC_TOTAL || k == RC_PERCENT);
    }
    sort_rows(r);
}

int EeUiResults_SortColumn(const EeUiResults *r, int *ascending)
{
    if (ascending != NULL)
    {
        *ascending = r != NULL ? r->sort_asc : 1;
    }
    return r != NULL ? r->sort_col : -1;
}

/* Windows' Res_UpdateStatus */
const char *EeUiResults_StatusText(EeUiResults *u)
{
    const EeResults *r;
    wchar_t buf[600];
    wchar_t num[64];
    uint32_t i, nsrc_issues = 0;
    if (u == NULL)
    {
        return "";
    }
    r = &u->res;
    buf[0] = L'\0';
    if (r->election_name != NULL)
    {
        StringCchCatW(buf, 600, r->election_name);
        if (r->election_date != NULL)
        {
            StringCchCatW(buf, 600, L" (");
            StringCchCatW(buf, 600, r->election_date);
            StringCchCatW(buf, 600, L")");
        }
        StringCchCatW(buf, 600, L"  \x2022  ");
    }
    StringCchPrintfW(num, 64, L"%u contests", r->ncontests);
    StringCchCatW(buf, 600, num);
    if (r->nprecincts > 0)
    {
        StringCchPrintfW(num, 64, L"  \x2022  %u precincts", r->nprecincts);
        StringCchCatW(buf, 600, num);
    }
    if (u->by_precinct || u->filt_active)
    {
        StringCchPrintfW(num, 64, L"  \x2022  %u rows", u->nrows);
        StringCchCatW(buf, 600, num);
    }
    if (u->filt_active)
    {
        wchar_t f[160];
        if (u->pmask != NULL)
        {
            StringCchPrintfW(f,
                             160,
                             L"  \x2022  filtered: %u of %u precincts",
                             u->pre_included,
                             r->nprecincts);
        }
        else
        {
            StringCchCopyW(f, 160, L"  \x2022  filtered");
        }
        StringCchCatW(buf, 600, f);
        if (!u->tab_exact)
        {
            StringCchCatW(buf, 600, L" (some totals cannot be filtered)");
        }
    }
    StringCchCatW(buf, 600, L"  \x2022  ");
    for (i = 0; i < r->nsources; i++)
    {
        if (i > 0)
        {
            StringCchCatW(buf, 600, L", ");
        }
        StringCchCatW(buf, 600, EeResults_FormatLabel(r->sources[i].format));
    }
    if (r->timestamp != NULL)
    {
        StringCchCatW(buf, 600, L"  \x2022  as of ");
        StringCchCatW(buf, 600, r->timestamp);
    }
    for (i = 0; i < r->nissues; i++)
    {
        if (r->issues[i].source != 0)
        {
            nsrc_issues++;
        }
    }
    if (nsrc_issues > 0)
    {
        StringCchPrintfW(num, 64, L"  \x2022  %u differences", nsrc_issues);
        StringCchCatW(buf, 600, num);
    }
    else if (r->nissues > 0)
    {
        StringCchPrintfW(num, 64, L"  \x2022  %u notes", r->nissues);
        StringCchCatW(buf, 600, num);
    }
    return utf8_into(buf, u->status, sizeof(u->status));
}

/* Windows' Res_CopySelected: every column, tab-separated */
char *EeUiResults_CopyText(const EeUiResults *r, const uint32_t *rows, uint32_t n)
{
    CBuf b = {NULL, 0, 0, 0};
    char cell[4096];
    uint32_t i;
    int c;
    if (r == NULL || rows == NULL || n == 0)
    {
        return NULL;
    }
    for (i = 0; i < n; i++)
    {
        for (c = 0; c < r->ncols; c++)
        {
            EeUiResults_Cell(r, rows[i], c, cell, sizeof(cell));
            cb_cat(&b, cell, strlen(cell));
            cb_cat(&b, c + 1 < r->ncols ? "\t" : "\n", 1);
        }
    }
    if (b.failed)
    {
        free(b.p);
        return NULL;
    }
    return b.p;
}

/* -------------------------------------------------------------------------- */
/* Filter                                                                     */
/* -------------------------------------------------------------------------- */

static const wchar_t *const k_filter_titles[EE_UI_RF_COUNT] = {L"Precinct",
                                                               L"Contest",
                                                               L"Choice",
                                                               L"Party",
                                                               L"Vote Type"};

/* Windows' rw->filt_hidden: no Precinct without precinct detail, no Vote Type without vote
 * types */
static int filter_offered(const EeUiResults *r, uint32_t column)
{
    if (r == NULL || column >= EE_UI_RF_COUNT)
    {
        return 0;
    }
    if (column == EE_UI_RF_PRECINCT)
    {
        return EeResults_HasPrecinctDetail(&r->res) ? 1 : 0;
    }
    if (column == EE_UI_RF_VOTETYPE)
    {
        return r->res.nvt > 0;
    }
    return 1;
}

const char *EeUiResults_FilterTitle(const EeUiResults *r, uint32_t column)
{
    static const char *const titles[EE_UI_RF_COUNT] = {"Precinct",
                                                       "Contest",
                                                       "Choice",
                                                       "Party",
                                                       "Vote Type"};
    return filter_offered(r, column) ? titles[column] : NULL;
}

/* add @p v unless it is there already (ignoring case: Windows' Res_AddValue) */
static int add_value(const wchar_t ***vals, uint32_t *n, size_t *cap, const wchar_t *v)
{
    uint32_t i;
    if (v == NULL || v[0] == L'\0' || *n >= EE_FILTER_MAX_DISTINCT)
    {
        return 1;
    }
    for (i = 0; i < *n; i++)
    {
        if (CompareStringOrdinal((*vals)[i], -1, v, -1, TRUE) == CSTR_EQUAL)
        {
            return 1;
        }
    }
    if (*n + 1 > *cap)
    {
        size_t nc = *cap ? *cap * 2 : 64;
        const wchar_t **nv = (const wchar_t **)realloc((void *)*vals, nc * sizeof(wchar_t *));
        if (nv == NULL)
        {
            return 0;
        }
        *vals = nv;
        *cap = nc;
    }
    (*vals)[(*n)++] = v;
    return 1;
}

static int value_cmp(const void *pa, const void *pb)
{
    return compare_text(*(const wchar_t *const *)pa, *(const wchar_t *const *)pb);
}

/* Windows' Res_FilterCollect: the column's distinct values, sorted (precincts as numbers),
 * the vote types in file order */
char **EeUiResults_FilterValues(const EeUiResults *u, uint32_t column, uint32_t *n)
{
    const EeResults *r;
    const wchar_t **vals = NULL;
    size_t cap = 0;
    uint32_t i, k = 0;
    int ok = 1;
    char **out;
    if (n != NULL)
    {
        *n = 0;
    }
    if (u == NULL || n == NULL || !filter_offered(u, column))
    {
        return NULL;
    }
    r = &u->res;
    switch (column)
    {
        case EE_UI_RF_PRECINCT:
            for (i = 0; i < r->nprecincts && ok; i++)
            {
                ok = add_value(&vals, &k, &cap, r->precincts[i].name);
            }
            break;
        case EE_UI_RF_CONTEST:
            for (i = 0; i < r->ncontests && ok; i++)
            {
                ok = add_value(&vals, &k, &cap, r->contests[i].title);
            }
            break;
        case EE_UI_RF_CHOICE:
            for (i = 0; i < r->nchoices && ok; i++)
            {
                ok = add_value(&vals, &k, &cap, r->choices[i].name);
            }
            for (i = 0; i < r->ncontests && ok; i++)
            {
                if (r->contests[i].has_over_under)
                {
                    ok = add_value(&vals, &k, &cap, L"Over Votes") &&
                         add_value(&vals, &k, &cap, L"Under Votes");
                    break;
                }
            }
            break;
        case EE_UI_RF_PARTY:
            for (i = 0; i < r->nchoices && ok; i++)
            {
                ok = add_value(&vals, &k, &cap, r->choices[i].party);
            }
            break;
        default: /* EE_UI_RF_VOTETYPE */
            for (i = 0; i < r->nvt && ok; i++)
            {
                ok = add_value(&vals, &k, &cap, r->vote_types[i]);
            }
            break;
    }
    if (k > 1 && column != EE_UI_RF_VOTETYPE)
    {
        qsort((void *)vals, k, sizeof(wchar_t *), value_cmp);
    }
    out = ok ? (char **)calloc(k + 1, sizeof(char *)) : NULL;
    for (i = 0; out != NULL && i < k; i++)
    {
        out[i] = utf8_of(vals[i]);
        if (out[i] == NULL)
        {
            EeUiResults_FreeValues(out, i);
            out = NULL;
        }
    }
    free((void *)vals);
    if (out != NULL)
    {
        *n = k;
    }
    return out;
}

void EeUiResults_FreeValues(char **values, uint32_t n)
{
    uint32_t i;
    for (i = 0; values != NULL && i < n; i++)
    {
        free(values[i]);
    }
    free(values);
}

int EeUiResults_RuleIsValid(const EeUiRule *rule, char *why, size_t whycap)
{
    if (why != NULL && whycap > 0)
    {
        why[0] = '\0';
    }
    if (rule == NULL || rule->value[0] == '\0')
    {
        if (why != NULL && whycap > 0)
        {
            StringCchCopyA(why, whycap, "Choose a value to filter on."); /* (Windows' words) */
        }
        return 0;
    }
    return 1;
}

static void to_engine_rule(const EeUiRule *r, EeFilterRule *out)
{
    memset(out, 0, sizeof(*out));
    out->column = r->column;
    out->relation = r->relation == EE_UI_REL_IS_NOT ? EeRel_IsNot : EeRel_Is;
    out->action = r->exclude ? EeFilt_Exclude : EeFilt_Include;
    out->enabled = r->enabled ? TRUE : FALSE;
    if (MultiByteToWideChar(CP_UTF8, 0, r->value, -1, out->value, EE_FILTER_VALUE_CCH) <= 0)
    {
        /* (too long: as much as fits) */
        wchar_t *w = wide_of(r->value);
        StringCchCopyW(out->value, EE_FILTER_VALUE_CCH, w != NULL ? w : L"");
        free(w);
    }
}

static void from_engine_rule(const EeFilterRule *r, EeUiRule *out)
{
    memset(out, 0, sizeof(*out));
    out->column = r->column;
    out->relation = r->relation == EeRel_IsNot ? EE_UI_REL_IS_NOT : EE_UI_REL_IS;
    out->exclude = r->action == EeFilt_Exclude;
    out->enabled = r->enabled != FALSE;
    utf8_into(r->value, out->value, sizeof(out->value));
}

/* Windows' Res_ApplyFilter: the precinct and vote type masks, the totals again, the rows */
static int apply_filter(EeUiResults *u)
{
    const EeResults *r = &u->res;
    uint32_t i, j;
    int has_pre = 0, has_vt = 0;
    free(u->pmask);
    free(u->vmask);
    u->pmask = NULL;
    u->vmask = NULL;
    u->pre_included = r->nprecincts;
    u->filt_active = EeFilter_HasEnabled(&u->filters) ? 1 : 0;
    for (i = 0; i < u->filters.count; i++)
    {
        if (u->filters.rules[i].enabled)
        {
            has_pre = has_pre || u->filters.rules[i].column == EE_UI_RF_PRECINCT;
            has_vt = has_vt || u->filters.rules[i].column == EE_UI_RF_VOTETYPE;
        }
    }
    if (has_pre && r->nprecincts > 0)
    {
        u->pmask = (uint8_t *)malloc(r->nprecincts);
        if (u->pmask != NULL)
        {
            u->pre_included = 0;
            for (j = 0; j < r->nprecincts; j++)
            {
                u->pmask[j] = (uint8_t)column_accepts(
                    &u->filters,
                    EE_UI_RF_PRECINCT,
                    r->precincts[j].name != NULL ? r->precincts[j].name : L"");
                u->pre_included += u->pmask[j];
            }
        }
    }
    if (has_vt && r->nvt > 0)
    {
        u->vmask = (uint8_t *)malloc(r->nvt);
        if (u->vmask != NULL)
        {
            for (j = 0; j < r->nvt; j++)
            {
                u->vmask[j] =
                    (uint8_t)column_accepts(&u->filters,
                                            EE_UI_RF_VOTETYPE,
                                            r->vote_types[j] != NULL ? r->vote_types[j] : L"");
            }
        }
    }
    if (!tabulate(u))
    {
        return 0;
    }
    return EeUiResults_SetView(u, u->by_precinct, u->show_vt, u->party_first);
}

int EeUiResults_SetRules(EeUiResults *u, const EeUiRule *rules, uint32_t n)
{
    EeFilterSet set;
    uint32_t i;
    int ok = 1;
    if (u == NULL)
    {
        return 0;
    }
    EeFilter_Init(&set);
    for (i = 0; ok && rules != NULL && i < n; i++)
    {
        EeFilterRule r;
        to_engine_rule(&rules[i], &r);
        ok = EeFilter_Add(&set, &r) ? 1 : 0;
    }
    if (ok)
    {
        EeFilter_Clear(&u->filters);
        u->filters = set; /* (moved) */
        return apply_filter(u);
    }
    EeFilter_Clear(&set);
    return 0;
}

uint32_t EeUiResults_RuleCount(const EeUiResults *r)
{
    return r != NULL ? r->filters.count : 0;
}

int EeUiResults_GetRule(const EeUiResults *r, uint32_t i, EeUiRule *out)
{
    if (r == NULL || out == NULL || i >= r->filters.count)
    {
        return 0;
    }
    from_engine_rule(&r->filters.rules[i], out);
    return 1;
}

int EeUiResults_IsFiltered(const EeUiResults *r)
{
    return r != NULL && r->filt_active;
}

void EeUiResults_FilterFileName(const EeUiResults *r, char *out, size_t cap)
{
    if (out == NULL || cap == 0)
    {
        return;
    }
    StringCchPrintfA(out,
                     cap,
                     "%s Filter.filter.csv",
                     r != NULL && r->base != NULL && r->base[0] != '\0' ? r->base
                                                                        : "Election Results");
}

/* the offered columns by title, as Windows' Filter window gives them to a saved filter */
static void filter_columns(const EeUiResults *r, EeFilterColumnRef *cols)
{
    uint32_t c;
    for (c = 0; c < EE_UI_RF_COUNT; c++)
    {
        cols[c].column = c;
        cols[c].title = filter_offered(r, c) ? k_filter_titles[c] : L"";
    }
}

/* the engine's message, UTF-8, without its "\r" or a last line end */
static void message_utf8(const wchar_t *w, char *out, size_t cap)
{
    char *d;
    const char *p;
    if (out == NULL || cap == 0)
    {
        return;
    }
    utf8_into(w, out, cap);
    for (p = out, d = out; *p; p++)
    {
        if (*p != '\r')
        {
            *d++ = *p;
        }
    }
    while (d > out && d[-1] == '\n')
    {
        d--;
    }
    *d = '\0';
}

int EeUiResults_SaveFilter(const EeUiResults *r,
                           const char *path_utf8,
                           const EeUiRule *rules,
                           uint32_t n,
                           char delim,
                           char *err,
                           size_t errcap)
{
    EeFilterColumnRef cols[EE_UI_RF_COUNT];
    EeFilterSet set;
    wchar_t *path, werr[256] = L"";
    uint32_t i;
    int ok = 1;
    if (err != NULL && errcap > 0)
    {
        err[0] = '\0';
    }
    if (n == 0 || rules == NULL)
    {
        StringCchCopyA(err, errcap, "There are no filter rules to export."); /* (Windows' words) */
        return 0;
    }
    if (r == NULL || path_utf8 == NULL || (path = wide_of(path_utf8)) == NULL)
    {
        StringCchCopyA(err, errcap, "Could not create the filter file.");
        return 0;
    }
    filter_columns(r, cols);
    EeFilter_Init(&set);
    for (i = 0; ok && i < n; i++)
    {
        EeFilterRule e;
        to_engine_rule(&rules[i], &e);
        ok = EeFilter_Add(&set, &e) ? 1 : 0;
    }
    if (!ok)
    {
        StringCchCopyA(err, errcap, "Out of memory saving the filter.");
    }
    else if (!EeFilter_SaveFile(path,
                                &set,
                                cols,
                                EE_UI_RF_COUNT,
                                delim == '\t' ? L'\t' : L',',
                                werr,
                                256))
    {
        message_utf8(werr[0] ? werr : L"Could not write the filter file.", err, errcap);
        ok = 0;
    }
    EeFilter_Clear(&set);
    free(path);
    return ok;
}

int EeUiResults_LoadFilter(const EeUiResults *r,
                           const char *path_utf8,
                           EeUiRule **rules,
                           uint32_t *n,
                           char *msg,
                           size_t msgcap,
                           char *detail,
                           size_t detailcap)
{
    EeFilterColumnRef cols[EE_UI_RF_COUNT];
    EeFilterSet loaded;
    EeUiRule *out = NULL;
    wchar_t *path, werr[256] = L"", skipped[2048] = L"";
    uint32_t nskip = 0, i;
    if (msg != NULL && msgcap > 0)
    {
        msg[0] = '\0';
    }
    if (detail != NULL && detailcap > 0)
    {
        detail[0] = '\0';
    }
    if (rules != NULL)
    {
        *rules = NULL;
    }
    if (n != NULL)
    {
        *n = 0;
    }
    if (r == NULL || path_utf8 == NULL || rules == NULL || n == NULL ||
        (path = wide_of(path_utf8)) == NULL)
    {
        StringCchCopyA(msg, msgcap, "Could not open the filter file.");
        return -1;
    }
    filter_columns(r, cols);
    EeFilter_Init(&loaded);
    if (!EeFilter_LoadFile(path,
                           cols,
                           EE_UI_RF_COUNT,
                           EE_FILTER_REL_EQUALITY, /* (is / is not, as Windows' results filter) */
                           &loaded,
                           &nskip,
                           skipped,
                           2048,
                           werr,
                           256))
    {
        message_utf8(werr[0] ? werr : L"Could not read the filter file.", msg, msgcap);
        free(path);
        return -1;
    }
    free(path);
    if (loaded.count == 0 && nskip == 0)
    {
        StringCchCopyA(msg, msgcap, "The file holds no filter rules.");
        EeFilter_Clear(&loaded);
        return 0;
    }
    if (loaded.count > 0 && (out = (EeUiRule *)calloc(loaded.count, sizeof(EeUiRule))) == NULL)
    {
        StringCchCopyA(msg, msgcap, "Out of memory loading the filter.");
        EeFilter_Clear(&loaded);
        return -1;
    }
    for (i = 0; i < loaded.count; i++)
    {
        from_engine_rule(&loaded.rules[i], &out[i]);
    }
    if (nskip > 0)
    {
        if (msg != NULL && msgcap > 0)
        {
            StringCchPrintfA(msg,
                             msgcap,
                             "Loaded %u rule%s. %u rule%s could not be used here:",
                             loaded.count,
                             loaded.count == 1 ? "" : "s",
                             nskip,
                             nskip == 1 ? "" : "s");
        }
        message_utf8(skipped, detail, detailcap);
    }
    *rules = out;
    *n = loaded.count;
    EeFilter_Clear(&loaded);
    return 1;
}

/* -------------------------------------------------------------------------- */
/* Self-test                                                                  */
/* -------------------------------------------------------------------------- */

static void st_columns(const EeUiResults *r)
{
    int c;
    printf("columns:");
    for (c = 0; c < EeUiResults_Columns(r); c++)
    {
        printf(" %s%s(%d)",
               EeUiResults_ColumnTitle(r, c),
               EeUiResults_ColumnNumeric(r, c) ? "#" : "",
               EeUiResults_ColumnWidth(r, c));
        printf(c + 1 < EeUiResults_Columns(r) ? " |" : "\n");
    }
}

static void st_row(const EeUiResults *r, uint32_t i)
{
    char cell[512];
    int c;
    printf("row %u:", i);
    for (c = 0; c < EeUiResults_Columns(r); c++)
    {
        printf(" %s%s",
               EeUiResults_Cell(r, i, c, cell, sizeof(cell)),
               c + 1 < EeUiResults_Columns(r) ? " |" : "\n");
    }
}

static void st_rows(const EeUiResults *r, uint32_t max)
{
    uint32_t i;
    for (i = 0; i < EeUiResults_Rows(r) && i < max; i++)
    {
        st_row(r, i);
    }
}

static void st_first(const EeUiResults *r, const char *what)
{
    char a[256], b[256], t[256];
    int asc, col = EeUiResults_SortColumn(r, &asc);
    int tc = col >= 0 ? col : EeUiResults_Columns(r) - 2;
    printf("%s: sorted by %d, ascending %d: [%s | %s | %s]\n",
           what,
           col,
           asc,
           EeUiResults_Cell(r, 0, 0, a, sizeof(a)),
           EeUiResults_Cell(r, 0, 2, b, sizeof(b)),
           EeUiResults_Cell(r, 0, tc, t, sizeof(t)));
}

static void st_rule(EeUiRule *r, uint32_t column, int relation, const char *value, int exclude)
{
    memset(r, 0, sizeof(*r));
    r->column = column;
    r->relation = relation;
    r->exclude = exclude;
    r->enabled = 1;
    StringCchCopyA(r->value, sizeof(r->value), value);
}

static double st_now(void);

static void st_set(EeUiResults *r,
                   const char *what,
                   const EeUiRule *rules,
                   uint32_t n,
                   uint32_t rows)
{
    double t0 = st_now();
    int ok = EeUiResults_SetRules(r, rules, n);
    fprintf(stderr, "time: filter %s %.3f s\n", what, st_now() - t0);
    printf("filter %s: ok %d, filtered %d, %u rows\n",
           what,
           ok,
           EeUiResults_IsFiltered(r),
           EeUiResults_Rows(r));
    printf("status: %s\n", EeUiResults_StatusText(r));
    st_rows(r, rows);
}

/* the Filter window's columns and values, rules on each kind of column, a saved filter */
static void st_filter(EeUiResults *r)
{
    EeUiRule rules[6], *loaded = NULL;
    uint32_t c, i, n = 0, nl = 0;
    char path[256], name[300], err[256], msg[256], detail[1024];
    for (c = 0; c < EE_UI_RF_COUNT; c++)
    {
        char **vals = EeUiResults_FilterValues(r, c, &n);
        const char *title = EeUiResults_FilterTitle(r, c);
        printf("filter column %u: %s:", c, title != NULL ? title : "(not offered)");
        for (i = 0; i < n && i < 12; i++)
        {
            printf(" %s%s", vals[i], i + 1 < n && i + 1 < 12 ? " |" : "");
        }
        printf("%s (%u)\n", n > 12 ? " ..." : "", n);
        EeUiResults_FreeValues(vals, n);
    }
    st_rule(&rules[0], EE_UI_RF_CHOICE, EE_UI_REL_IS, "", 0);
    printf("rule without a value: %d %s\n",
           EeUiResults_RuleIsValid(&rules[0], err, sizeof(err)),
           err);
    /* a precinct: everything counted again from it (the withheld count: some totals cannot be) */
    st_rule(&rules[0], EE_UI_RF_PRECINCT, EE_UI_REL_IS, "102", 0);
    st_set(r, "precinct is 102", rules, 1, 12);
    /* and a vote type left out: its column goes, the totals follow */
    st_rule(&rules[1], EE_UI_RF_VOTETYPE, EE_UI_REL_IS_NOT, "Mail", 0);
    st_set(r, "and vote type is not Mail", rules, 2, 3);
    /* rows hidden: a party's contests, without the over and under votes (excludes win) */
    st_rule(&rules[0], EE_UI_RF_PARTY, EE_UI_REL_IS, "DEM", 0);
    st_rule(&rules[1], EE_UI_RF_PARTY, EE_UI_REL_IS, "NON", 0);
    st_rule(&rules[2], EE_UI_RF_CHOICE, EE_UI_REL_IS, "over votes", 1);
    st_set(r, "party DEM or NON, choice not Over Votes", rules, 3, 8);
    st_rule(&rules[0], EE_UI_RF_CONTEST, EE_UI_REL_IS, "REP Governor", 0);
    rules[1] = rules[0];
    rules[1].enabled = 0; /* (off: counts for nothing) */
    StringCchCopyA(rules[1].value, sizeof(rules[1].value), "DEM Governor");
    st_set(r, "contest REP Governor, a rule off", rules, 2, 4);
    for (i = 0; i < EeUiResults_RuleCount(r); i++)
    {
        EeUiRule g;
        EeUiResults_GetRule(r, i, &g);
        printf("rule %u: column %u, relation %d, exclude %d, enabled %d, \"%s\"\n",
               i,
               g.column,
               g.relation,
               g.exclude,
               g.enabled,
               g.value);
    }
    /* Export Filter... and Load Filter... */
    EeUiResults_FilterFileName(r, name, sizeof(name));
    printf("filter file name: %s\n", name);
    StringCchPrintfA(path, sizeof(path), "/tmp/ee_results_selftest_%ld.filter.csv", (long)getpid());
    st_rule(&rules[0], EE_UI_RF_PRECINCT, EE_UI_REL_IS, "101", 0);
    st_rule(&rules[1], EE_UI_RF_VOTETYPE, EE_UI_REL_IS, "Early Voting", 1);
    st_rule(&rules[2], EE_UI_RF_CHOICE, EE_UI_REL_IS_NOT, "Ann Lee", 0);
    rules[2].enabled = 0;
    printf("save filter: %d %s\n",
           EeUiResults_SaveFilter(r, path, rules, 3, ',', err, sizeof(err)),
           err);
    printf("load filter: %d %s",
           EeUiResults_LoadFilter(r, path, &loaded, &nl, msg, sizeof(msg), detail, sizeof(detail)),
           msg);
    printf(", %u rules\n", nl);
    for (i = 0; i < nl; i++)
    {
        printf("loaded rule %u: column %u, relation %d, exclude %d, enabled %d, \"%s\"\n",
               i,
               loaded[i].column,
               loaded[i].relation,
               loaded[i].exclude,
               loaded[i].enabled,
               loaded[i].value);
    }
    free(loaded);
    loaded = NULL;
    /* a saved filter with rules results cannot use: a voter list's column, a "contains" rule */
    {
        FILE *f = fopen(path, "wb");
        if (f != NULL)
        {
            fputs("Column,Relation,Value,Action,Enabled\r\nParty,is,DEM,Include,Yes\r\n"
                  "Voter ID,is,1,Include,Yes\r\nChoice,contains,Ann,Include,Yes\r\n",
                  f);
            fclose(f);
        }
    }
    printf("load other filter: %d %s",
           EeUiResults_LoadFilter(r, path, &loaded, &nl, msg, sizeof(msg), detail, sizeof(detail)),
           msg);
    printf(", %u rules\n%s\n", nl, detail);
    free(loaded);
    remove(path);
    printf("save no rules: %d %s\n",
           EeUiResults_SaveFilter(r, path, rules, 0, ',', err, sizeof(err)),
           err);
    st_set(r, "reset", NULL, 0, 0);
}

static double st_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

int EeUiResults_SelfTest(const char *const *paths_utf8, uint32_t n)
{
    EeUiResults **res = NULL;
    uint32_t nres = 0, g;
    char err[600];
    double t0 = st_now();
    int st = EeUiResults_Load(paths_utf8, n, NULL, NULL, NULL, &res, &nres, err, sizeof(err));
    fprintf(stderr, "time: load %.2f s\n", st_now() - t0);
    if (st != EE_UI_RESULTS_OK)
    {
        printf("results: load failed: %s\n", err);
        return 1;
    }
    printf("results: %u files, %u elections\n", n, nres);
    for (g = 0; g < nres; g++)
    {
        EeUiResults *r = res[g];
        uint32_t i, all[4];
        char *copy;
        int col;
        printf("election %u: \"%s\", precinct detail %d, differences %d\n",
               g + 1,
               EeUiResults_WindowTitle(r),
               EeUiResults_HasPrecinctDetail(r),
               EeUiResults_HasDifferences(r));
        printf("status: %s\n", EeUiResults_StatusText(r));
        st_columns(r);
        printf("rows: %u\n", EeUiResults_Rows(r));
        st_rows(r, 40);
        /* a heading click on Total Votes (largest first), again (smallest), then Choice */
        col = EeUiResults_Columns(r) - 2;
        t0 = st_now();
        EeUiResults_ClickHeading(r, col);
        fprintf(stderr, "time: sort %u rows %.3f s\n", EeUiResults_Rows(r), st_now() - t0);
        st_first(r, "sort");
        EeUiResults_ClickHeading(r, col);
        st_first(r, "sort");
        EeUiResults_ClickHeading(r, 2);
        st_first(r, "sort");
        for (i = 0; i < 4; i++)
        {
            all[i] = i;
        }
        copy = EeUiResults_CopyText(r, all, EeUiResults_Rows(r) < 4 ? EeUiResults_Rows(r) : 4);
        printf("copy:\n%s", copy != NULL ? copy : "(none)\n");
        free(copy);
        /* Reset View, then the other views */
        EeUiResults_Sort(r, -1, 1);
        st_first(r, "reset view");
        if (EeUiResults_HasPrecinctDetail(r))
        {
            t0 = st_now();
            EeUiResults_SetView(r, 1, 1, EE_UI_PARTY_REP);
            fprintf(stderr, "time: by precinct %.3f s\n", st_now() - t0);
            printf("by precinct: by precinct %d, %u rows\n",
                   EeUiResults_ByPrecinct(r),
                   EeUiResults_Rows(r));
            printf("status: %s\n", EeUiResults_StatusText(r));
            st_columns(r);
            st_rows(r, 12);
            for (i = 0; i < EeUiResults_Rows(r); i++)
            {
                /* (the first row with a count the files withhold) */
                char cell[64];
                int c, found = 0;
                for (c = 0; c < EeUiResults_Columns(r) && !found; c++)
                {
                    found = strcmp(EeUiResults_Cell(r, i, c, cell, sizeof(cell)), "protected") == 0;
                }
                if (found)
                {
                    printf("withheld: ");
                    st_row(r, i);
                    break;
                }
            }
            /* the sort is kept when its column stays */
            t0 = st_now();
            EeUiResults_ClickHeading(r, 4);
            fprintf(stderr,
                    "time: precinct sort %u rows %.3f s\n",
                    EeUiResults_Rows(r),
                    st_now() - t0);
            t0 = st_now();
            EeUiResults_ClickHeading(r, EeUiResults_Columns(r) - 2);
            fprintf(stderr,
                    "time: total sort %u rows %.3f s\n",
                    EeUiResults_Rows(r),
                    st_now() - t0);
            EeUiResults_ClickHeading(r, 4);
            EeUiResults_SetView(r, 1, 0, EE_UI_PARTY_REP);
            st_first(r, "precinct sort kept without vote types");
            EeUiResults_Sort(r, -1, 1);
        }
        EeUiResults_SetView(r, 0, 0, EE_UI_PARTY_REP);
        printf("without vote types:\n");
        st_columns(r);
        st_rows(r, 3);
        EeUiResults_SetView(r, 0, 1, EE_UI_PARTY_DEM);
        printf("democratic first: party first %d\n", EeUiResults_PartyFirst(r));
        st_rows(r, 3);
        EeUiResults_SetView(r, 0, 1, EE_UI_PARTY_REP);
        st_filter(r);
        printf("load summary:\n%s\n", EeUiResults_LoadSummary(r));
        EeUiResults_Free(r);
    }
    free(res);
    return 0;
}
