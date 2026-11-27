/**
 * @file ee_ui_voters.c
 * @brief The voter list of ee_ui_voters.h over the engine's EeVoterTable and EeFilterSet.
 */

#include "ee_ui_voters.h"
#include "filter.h"
#include "settings.h"
#include "voter_roster.h"
#include "voter_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>
#include <unistd.h> /* (getpid: the self-test's scratch file; the bridge is POSIX-only) */

struct EeUiVoters
{
    EeVoterTable t;
    char *titles[EE_MAX_COLUMNS]; /* UTF-8 */
    EeFilterSet filters;          /* the shown view's rules */
    BOOL filtered;                /* some rule is enabled */
    uint32_t *map;  /* shown row -> view row; NULL when every row is shown or none is */
    uint32_t shown;
    uint8_t *marks; /* per physical row: 1 for a duplicate (a duplicates view), else NULL */
    int mark_kind;  /* EE_UI_MARKS_* */
    uint32_t marked;

    /* the shown columns: every column of a voter list; a roster's display columns (Windows'
     * App_BuildRosterColumns), so a shown column c is the table's column cols[c] */
    BOOL roster;
    uint32_t ncols;
    uint32_t cols[EE_MAX_COLUMNS];
    char *summary;        /* a roster's load summary (UTF-8), or NULL */
    BOOL summary_notable; /* it says more than the counts (Windows opens it after loading) */
    BOOL method_read[EE_VM_COUNT];   /* a roster sheet of that method was read */
    BOOL method_source[EE_VM_COUNT]; /* a file of that method was present (read or not) */
};

/* the table's column for shown column @p c, or UINT32_MAX */
static uint32_t tcol(const EeUiVoters *v, uint32_t c)
{
    return v != NULL && c < v->ncols ? v->cols[c] : UINT32_MAX;
}

/* the shown column of the table's column @p t, or -1 */
static int scol(const EeUiVoters *v, uint32_t t)
{
    uint32_t i;
    for (i = 0; v != NULL && i < v->ncols; i++)
    {
        if (v->cols[i] == t)
        {
            return (int)i;
        }
    }
    return -1;
}

/* A computed order and filter, ready to swap in. */
struct EeUiView
{
    uint32_t *view_index; /* view row -> physical row */
    int sort_column;
    BOOL sort_ascending;
    EeFilterSet filters;
    BOOL filtered;
    uint32_t *map;
    uint32_t shown;
    uint8_t *marks;
    int mark_kind;
    uint32_t marked;
};

typedef struct ProgressCx
{
    EeUiProgressFn fn;
    void *user;
} ProgressCx;

static BOOL on_progress(const EeLoadProgress *p, void *user)
{
    ProgressCx *c = (ProgressCx *)user;
    return c->fn == NULL || c->fn(c->user, p->percent, p->rows_loaded) != 0;
}

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
    if (s != NULL)
    {
        if (n <= 0 || WideCharToMultiByte(CP_UTF8, 0, w, -1, s, n, NULL, NULL) <= 0)
        {
            s[0] = '\0';
        }
    }
    return s;
}

/* UTF-8 -> a rule's wide value (cut at the engine's limit) */
static void wide_value(const char *s, wchar_t *out, size_t outcch)
{
    wchar_t buf[EE_UI_RULE_VALUE_BYTES];
    if (MultiByteToWideChar(CP_UTF8, 0, s != NULL ? s : "", -1, buf, (int)ARRAYSIZE(buf)) <= 0)
    {
        buf[0] = L'\0';
    }
    StringCchCopyW(out, outcch, buf);
}

static void to_engine_rule(const EeUiVoters *v, const EeUiRule *r, EeFilterRule *out)
{
    memset(out, 0, sizeof(*out));
    out->column = tcol(v, r->column);
    out->relation = (r->relation >= 0 && r->relation < EE_UI_REL_COUNT)
                        ? (EeFilterRelation)r->relation
                        : EeRel_Is;
    out->action = r->exclude ? EeFilt_Exclude : EeFilt_Include;
    out->enabled = r->enabled ? TRUE : FALSE;
    wide_value(r->value, out->value, ARRAYSIZE(out->value));
}

static void set_err(char *err, size_t errcap, const char *text);

/* After a load: on failure the message (UTF-8) and the list freed (FALSE); else the titles */
static BOOL loaded(EeUiVoters *v,
                   EeLoadStatus st,
                   const wchar_t *werr,
                   const wchar_t *fallback,
                   char *err,
                   size_t errcap)
{
    uint32_t c;
    if (st != EeLoadStatus_Ok)
    {
        if (err != NULL && errcap > 0)
        {
            if (st == EeLoadStatus_Cancelled)
            {
                StringCchCopyA(err, errcap, "Loading was cancelled.");
            }
            else if (WideCharToMultiByte(CP_UTF8,
                                         0,
                                         werr[0] ? werr : fallback,
                                         -1,
                                         err,
                                         (int)errcap,
                                         NULL,
                                         NULL) <= 0)
            {
                StringCchCopyA(err, errcap, "The file could not be loaded.");
            }
        }
        free(v->summary);
        EeVoterTable_Clear(&v->t);
        free(v);
        return FALSE;
    }
    for (c = 0; c < v->t.column_count; c++)
    {
        v->titles[c] = utf8_of(v->t.column_titles[c]);
    }
    v->shown = v->t.row_count;
    return TRUE;
}

EeUiVoters *EeUiVoters_Load(const char *path_utf8,
                            int surname_first,
                            EeUiProgressFn fn,
                            void *user,
                            volatile int32_t *cancel,
                            char *err,
                            size_t errcap)
{
    EeUiVoters *v = (EeUiVoters *)calloc(1, sizeof(EeUiVoters));
    wchar_t path[4096], werr[1024] = L"";
    ProgressCx pc;
    EeLoadStatus st;
    uint32_t c;
    if (err != NULL && errcap > 0)
    {
        err[0] = '\0';
    }
    if (v == NULL ||
        MultiByteToWideChar(CP_UTF8, 0, path_utf8, -1, path, (int)ARRAYSIZE(path)) <= 0)
    {
        free(v);
        if (err != NULL && errcap > 0)
        {
            StringCchCopyA(err, errcap, "The file could not be opened.");
        }
        return NULL;
    }
    pc.fn = fn;
    pc.user = user;
    EeVoterTable_Init(&v->t);
    v->t.name_surname_first = surname_first ? TRUE : FALSE; /* (the loader composes the names so) */
    EeFilter_Init(&v->filters);
    st = EeVoterTable_LoadFromFile(path,
                                   &v->t,
                                   (volatile LONG *)cancel,
                                   on_progress,
                                   &pc,
                                   werr,
                                   ARRAYSIZE(werr));
    if (!loaded(v, st, werr, L"The file could not be loaded.", err, errcap))
    {
        return NULL;
    }
    for (c = 0; c < v->t.column_count; c++)
    {
        v->cols[v->ncols++] = c;
    }
    return v;
}

/* Windows' App_BuildRosterColumns: Voter ID, Precinct, Name, then the source columns other than
 * the VUID and name parts behind them (Voting Method, Date Voted, Party, extras, Source File) */
static void roster_columns(EeUiVoters *v)
{
    static const wchar_t *const hidden[] =
        {L"VUID", L"Precinct", L"Last Name", L"First Name", L"Middle Name", L"Name Suffix"};
    uint32_t c;
    size_t k;
    v->ncols = 0;
    v->cols[v->ncols++] = EE_COL_VOTER_ID;
    v->cols[v->ncols++] = EE_COL_PRECINCT;
    v->cols[v->ncols++] = EE_COL_NAME;
    for (c = EE_FROZEN_COLUMN_COUNT; c < v->t.column_count && v->ncols < EE_MAX_COLUMNS; c++)
    {
        const wchar_t *t = v->t.column_titles[c] != NULL ? v->t.column_titles[c] : L"";
        BOOL hide = FALSE;
        for (k = 0; k < ARRAYSIZE(hidden) && !hide; k++)
        {
            hide = _wcsicmp(t, hidden[k]) == 0;
        }
        if (!hide)
        {
            v->cols[v->ncols++] = c;
        }
    }
}

/* (Windows' Roster_InfoIsNotable: skipped or unread files, recovered sheets, unusual or
 * duplicate Voter IDs) */
static BOOL roster_notable(const EeRosterLoadInfo *in)
{
    return in->repeated_files > 0 || in->copy_files > 0 || in->unsupported_files > 0 ||
           in->recovered_sheets > 0 || in->skipped_sheets > 0 || in->vuids_malformed > 0 ||
           in->vuids_missing > 0 || in->vuids_duplicated > 0;
}

EeUiVoters *EeUiVoters_LoadRoster(const char *const *paths_utf8,
                                  uint32_t n,
                                  int surname_first,
                                  EeUiProgressFn fn,
                                  void *user,
                                  volatile int32_t *cancel,
                                  char *err,
                                  size_t errcap)
{
    EeUiVoters *v = (EeUiVoters *)calloc(1, sizeof(EeUiVoters));
    wchar_t **paths = (wchar_t **)calloc(n > 0 ? n : 1, sizeof(wchar_t *));
    wchar_t werr[1024] = L"";
    EeRosterLoadInfo info;
    ProgressCx pc;
    EeLoadStatus st;
    uint32_t i;
    BOOL ok = v != NULL && paths != NULL && paths_utf8 != NULL && n > 0;
    set_err(err, errcap, "");
    for (i = 0; ok && i < n; i++)
    {
        int cch = MultiByteToWideChar(CP_UTF8, 0, paths_utf8[i], -1, NULL, 0);
        paths[i] = cch > 0 ? (wchar_t *)malloc((size_t)cch * sizeof(wchar_t)) : NULL;
        ok = paths[i] != NULL &&
             MultiByteToWideChar(CP_UTF8, 0, paths_utf8[i], -1, paths[i], cch) > 0;
    }
    if (!ok)
    {
        for (i = 0; paths != NULL && i < n; i++)
        {
            free(paths[i]);
        }
        free(paths);
        free(v);
        set_err(err, errcap, "The voter roster could not be opened.");
        return NULL;
    }
    pc.fn = fn;
    pc.user = user;
    memset(&info, 0, sizeof(info));
    EeVoterTable_Init(&v->t);
    v->t.name_surname_first = surname_first ? TRUE : FALSE;
    EeFilter_Init(&v->filters);
    st = EeRoster_LoadFiles((const wchar_t *const *)paths,
                            (int)n,
                            &v->t,
                            &info,
                            (volatile LONG *)cancel,
                            on_progress,
                            &pc,
                            werr,
                            ARRAYSIZE(werr));
    for (i = 0; i < n; i++)
    {
        free(paths[i]);
    }
    free(paths);
    if (st == EeLoadStatus_Ok && info.note != NULL)
    {
        char *s = utf8_of(info.note), *d;
        const char *p;
        for (p = s, d = s; s != NULL && *p; p++)
        {
            if (*p != '\r')
            {
                *d++ = *p;
            }
        }
        if (s != NULL)
        {
            *d = '\0';
        }
        v->summary = s;
        v->summary_notable = roster_notable(&info);
    }
    if (st == EeLoadStatus_Ok)
    {
        memcpy(v->method_read, info.method_read, sizeof(v->method_read));
        memcpy(v->method_source, info.method_source, sizeof(v->method_source));
    }
    EeRoster_FreeInfo(&info);
    if (!loaded(v, st, werr, L"Could not load the voter roster.", err, errcap))
    {
        return NULL;
    }
    v->roster = TRUE;
    roster_columns(v);
    return v;
}

int EeUiVoters_IsRoster(const EeUiVoters *v)
{
    return v != NULL && v->roster;
}

const char *EeUiVoters_LoadSummary(const EeUiVoters *v, int *notable)
{
    if (notable != NULL)
    {
        *notable = v != NULL && v->summary_notable;
    }
    return v != NULL ? v->summary : NULL;
}

int EeUiVoters_AddressColumn(const EeUiVoters *v)
{
    return v != NULL && !v->roster ? scol(v, EE_COL_ADDRESS) : -1;
}

void EeUiVoters_Free(EeUiVoters *v)
{
    uint32_t c;
    if (v == NULL)
    {
        return;
    }
    for (c = 0; c < v->t.column_count; c++)
    {
        free(v->titles[c]);
    }
    EeFilter_Clear(&v->filters);
    free(v->map);
    free(v->marks);
    free(v->summary);
    EeVoterTable_Clear(&v->t);
    free(v);
}

uint32_t EeUiVoters_Rows(const EeUiVoters *v)
{
    return v != NULL ? v->shown : 0;
}

uint32_t EeUiVoters_TotalRows(const EeUiVoters *v)
{
    return v != NULL ? v->t.row_count : 0;
}

uint32_t EeUiVoters_Columns(const EeUiVoters *v)
{
    return v != NULL ? v->ncols : 0;
}

const char *EeUiVoters_Title(const EeUiVoters *v, uint32_t c)
{
    uint32_t t = tcol(v, c);
    return v != NULL && t < v->t.column_count && v->titles[t] != NULL ? v->titles[t] : "";
}

const char *EeUiVoters_Cell(const EeUiVoters *v, uint32_t row, uint32_t c)
{
    uint32_t t = tcol(v, c);
    if (v == NULL || row >= v->shown || t >= v->t.column_count)
    {
        return "";
    }
    return EeVoterTable_GetViewCellUtf8(&v->t, v->map != NULL ? v->map[row] : row, t);
}

int EeUiVoters_IsNumeric(const EeUiVoters *v, uint32_t c)
{
    uint32_t t = tcol(v, c);
    return v != NULL && t < v->t.column_count && EeVoterTable_ColumnIsNumericOrDate(&v->t, t);
}

int EeUiVoters_SortColumn(const EeUiVoters *v, int *ascending)
{
    if (ascending != NULL)
    {
        *ascending = v != NULL && v->t.sort_ascending;
    }
    return v != NULL && v->t.sort_column >= 0 ? scol(v, (uint32_t)v->t.sort_column) : -1;
}

uint32_t EeUiVoters_RuleCount(const EeUiVoters *v)
{
    return v != NULL ? v->filters.count : 0;
}

int EeUiVoters_GetRule(const EeUiVoters *v, uint32_t i, EeUiRule *out)
{
    const EeFilterRule *r;
    if (v == NULL || out == NULL || i >= v->filters.count)
    {
        return 0;
    }
    r = &v->filters.rules[i];
    memset(out, 0, sizeof(*out));
    out->column = (uint32_t)scol(v, r->column);
    out->relation = (int)r->relation;
    out->exclude = r->action == EeFilt_Exclude;
    out->enabled = r->enabled != FALSE;
    if (WideCharToMultiByte(CP_UTF8,
                            0,
                            r->value,
                            -1,
                            out->value,
                            (int)sizeof(out->value),
                            NULL,
                            NULL) <= 0)
    {
        out->value[0] = '\0';
    }
    return 1;
}

int EeUiVoters_IsFiltered(const EeUiVoters *v)
{
    return v != NULL && v->filtered;
}

int EeUiVoters_RuleIsValid(const EeUiVoters *v, const EeUiRule *rule, char *why, size_t whycap)
{
    EeFilterRule r;
    if (why != NULL && whycap > 0)
    {
        why[0] = '\0';
    }
    if (v == NULL || rule == NULL || rule->column >= v->ncols)
    {
        if (why != NULL && whycap > 0)
        {
            StringCchCopyA(why, whycap, "Choose a column.");
        }
        return 0;
    }
    to_engine_rule(v, rule, &r);
    if (!EeFilter_RuleIsValid(&r, &v->t))
    {
        if (why != NULL && whycap > 0)
        {
            /* (the Windows Filter window's message) */
            StringCchCopyA(why,
                           whycap,
                           "Enter a date for this comparison (for example 1/15/2020 or 20200115).");
        }
        return 0;
    }
    return 1;
}

const char *EeUiVoters_RelationName(int relation)
{
    static const char *const names[EE_UI_REL_COUNT] = {
        "is",
        "is not",
        "less than",
        "more than",
        "begins with",
        "ends with",
        "contains",
        "excludes",
    };
    return relation >= 0 && relation < EE_UI_REL_COUNT ? names[relation] : "is";
}

int EeUiVoters_Marks(const EeUiVoters *v, uint32_t *marked)
{
    if (marked != NULL)
    {
        *marked = v != NULL ? v->marked : 0;
    }
    return v != NULL ? v->mark_kind : EE_UI_MARKS_NONE;
}

int EeUiVoters_HasBirthDate(const EeUiVoters *v)
{
    return v != NULL && EeVoterTable_FindBirthdateColumn(&v->t) >= 0;
}

/* (the Windows words) */
const char *EeUiVoters_MarksLabel(int marks)
{
    return marks == EE_UI_MARKS_DUP_VOTERS ? "duplicate voters (name + DOB)"
           : marks == EE_UI_MARKS_DUP_IDS  ? "duplicate Voter IDs"
                                           : "";
}

const char *EeUiVoters_MarksNoneText(int marks)
{
    return marks == EE_UI_MARKS_DUP_VOTERS ? "No duplicate voters (same name and date of birth)."
                                           : "No duplicate Voter IDs.";
}

void EeUiVoters_CountText(const EeUiVoters *v, char *out, size_t cap)
{
    const char *noun = v != NULL && v->roster ? "voter records" : "voters"; /* (Windows' words) */
    if (out == NULL || cap == 0)
    {
        return;
    }
    if (v == NULL)
    {
        out[0] = '\0';
    }
    else if (v->mark_kind != EE_UI_MARKS_NONE && v->filtered)
    {
        StringCchPrintfA(out,
                         cap,
                         "%s within filter: %u of %u %s",
                         EeUiVoters_MarksLabel(v->mark_kind),
                         v->shown,
                         v->t.row_count,
                         noun);
    }
    else if (v->mark_kind != EE_UI_MARKS_NONE)
    {
        StringCchPrintfA(out,
                         cap,
                         "Showing %u %s of %u %s",
                         v->shown,
                         EeUiVoters_MarksLabel(v->mark_kind),
                         v->t.row_count,
                         noun);
    }
    else if (v->filtered)
    {
        StringCchPrintfA(out, cap, "Filtered: %u of %u %s", v->shown, v->t.row_count, noun);
    }
    else
    {
        StringCchPrintfA(out, cap, "%u %s", v->shown, noun);
    }
}

void EeUiView_Free(EeUiView *view)
{
    if (view != NULL)
    {
        free(view->view_index);
        EeFilter_Clear(&view->filters);
        free(view->map);
        free(view->marks);
        free(view);
    }
}

int EeUiView_Marks(const EeUiView *view, uint32_t *marked)
{
    if (marked != NULL)
    {
        *marked = view != NULL ? view->marked : 0;
    }
    return view != NULL ? view->mark_kind : EE_UI_MARKS_NONE;
}

/* the view's duplicates: those of the shown view when the same kind, else found anew */
static BOOL view_marks(const EeUiVoters *v, EeUiView *view, int marks)
{
    uint32_t n = v->t.row_count;
    if (marks <= EE_UI_MARKS_NONE || marks >= EE_UI_MARKS_COUNT)
    {
        return TRUE;
    }
    view->mark_kind = marks;
    view->marks = (uint8_t *)calloc(n > 0 ? n : 1, 1);
    if (view->marks == NULL)
    {
        return FALSE;
    }
    if (marks == v->mark_kind && v->marks != NULL)
    {
        memcpy(view->marks, v->marks, n);
        view->marked = v->marked;
        return TRUE;
    }
    if (marks == EE_UI_MARKS_DUP_VOTERS)
    {
        return EeVoterTable_MarkDuplicateVotersByNameDob(&v->t,
                                                         view->marks,
                                                         &view->marked,
                                                         NULL,
                                                         NULL,
                                                         NULL);
    }
    return EeVoterTable_MarkDuplicateVoterIds(&v->t, view->marks, &view->marked, NULL, NULL, NULL);
}

/* keep only the shown rows that are duplicates (as Windows ANDs its marks with the filter) */
static BOOL view_keep_marked(EeUiView *view, uint32_t n)
{
    uint32_t *kept, k, m = 0;
    if (view->marks == NULL)
    {
        return TRUE;
    }
    kept = (uint32_t *)malloc((size_t)(view->shown > 0 ? view->shown : 1) * sizeof(uint32_t));
    if (kept == NULL)
    {
        return FALSE;
    }
    for (k = 0; k < view->shown; k++)
    {
        uint32_t vr = view->map != NULL ? view->map[k] : k;
        uint32_t phys = view->view_index[vr];
        if (phys < n && view->marks[phys])
        {
            kept[m++] = vr;
        }
    }
    free(view->map);
    view->map = kept;
    view->shown = m;
    return TRUE;
}

EeUiView *EeUiVoters_ComputeView(const EeUiVoters *v,
                                 int sort_column,
                                 int ascending,
                                 const EeUiRule *rules,
                                 uint32_t nrules,
                                 int marks)
{
    EeUiView *view;
    EeVoterTable *tmp;
    uint32_t i, n;
    if (v == NULL)
    {
        return NULL;
    }
    n = v->t.row_count;
    view = (EeUiView *)calloc(1, sizeof(EeUiView));
    tmp = (EeVoterTable *)malloc(sizeof(EeVoterTable)); /* (about 10 KB: not on a worker's stack) */
    if (view == NULL || tmp == NULL)
    {
        free(view);
        free(tmp);
        return NULL;
    }
    EeFilter_Init(&view->filters);
    view->view_index = (uint32_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(uint32_t));
    if (view->view_index == NULL)
    {
        free(tmp);
        EeUiView_Free(view);
        return NULL;
    }

    /* the order: sort a copy of the table's header with its own view_index (the cells and
     * the string pool are only read), so the shown view is untouched meanwhile */
    *tmp = v->t;
    tmp->view_index = view->view_index;
    for (i = 0; i < n; i++)
    {
        tmp->view_index[i] = i;
    }
    tmp->sort_column = -1;
    tmp->sort_ascending = TRUE;
    if (sort_column >= 0 && tcol(v, (uint32_t)sort_column) < v->t.column_count && n > 0)
    {
        uint32_t t = tcol(v, (uint32_t)sort_column);
        if (!ascending)
        {
            /* (SortByColumn reverses a column that is already the sort column) */
            tmp->sort_column = (int)t;
            tmp->sort_ascending = TRUE;
        }
        EeVoterTable_SortByColumn(tmp, t);
    }
    view->sort_column = tmp->sort_column;
    view->sort_ascending = tmp->sort_ascending;

    /* the filter, over the new order */
    for (i = 0; i < nrules; i++)
    {
        EeFilterRule r;
        to_engine_rule(v, &rules[i], &r);
        if (!EeFilter_Add(&view->filters, &r))
        {
            free(tmp);
            EeUiView_Free(view);
            return NULL;
        }
    }
    view->filtered = EeFilter_HasEnabled(&view->filters);
    if (!EeFilter_BuildMap(&view->filters, tmp, &view->map, &view->shown))
    {
        free(tmp);
        EeUiView_Free(view);
        return NULL;
    }
    free(tmp);

    /* the duplicates, if asked for */
    if (!view_marks(v, view, marks) || !view_keep_marked(view, n))
    {
        EeUiView_Free(view);
        return NULL;
    }
    return view;
}

void EeUiVoters_SetView(EeUiVoters *v, EeUiView *view)
{
    if (v == NULL || view == NULL)
    {
        EeUiView_Free(view);
        return;
    }
    free(v->t.view_index);
    v->t.view_index = view->view_index;
    v->t.sort_column = view->sort_column;
    v->t.sort_ascending = view->sort_ascending;
    EeFilter_Clear(&v->filters);
    v->filters = view->filters;
    v->filtered = view->filtered;
    free(v->map);
    v->map = view->map;
    v->shown = view->shown;
    free(v->marks);
    v->marks = view->marks;
    v->mark_kind = view->mark_kind;
    v->marked = view->marked;
    free(view);
}

/* -------------------------------------------------------------------------- */
/* Copy and Export                                                            */
/* -------------------------------------------------------------------------- */

/* shown rows -> view rows (NULL rows: all shown rows) */
static uint32_t *view_rows(const EeUiVoters *v, const uint32_t *rows, uint32_t *n)
{
    uint32_t i, count = rows != NULL ? *n : v->shown, k = 0;
    uint32_t *out = (uint32_t *)malloc((size_t)(count > 0 ? count : 1) * sizeof(uint32_t));
    if (out == NULL)
    {
        return NULL;
    }
    for (i = 0; i < count; i++)
    {
        uint32_t r = rows != NULL ? rows[i] : i;
        if (r < v->shown)
        {
            out[k++] = v->map != NULL ? v->map[r] : r;
        }
    }
    *n = k;
    return out;
}

char *EeUiVoters_CopyText(const EeUiVoters *v,
                          const uint32_t *rows,
                          uint32_t n,
                          int normalized,
                          size_t *len)
{
    uint32_t *vr;
    char *text = NULL;
    if (len != NULL)
    {
        *len = 0;
    }
    if (v == NULL || rows == NULL || (vr = view_rows(v, rows, &n)) == NULL)
    {
        return NULL;
    }
    if (!EeVoterTable_FormatCopyUtf8(&v->t,
                                     vr,
                                     n,
                                     normalized && !v->roster ? TRUE : FALSE,
                                     &text,
                                     len))
    {
        text = NULL;
    }
    free(vr);
    return text;
}

static void set_err(char *err, size_t errcap, const char *text)
{
    if (err != NULL && errcap > 0)
    {
        StringCchCopyA(err, errcap, text);
    }
}

long EeUiVoters_Export(const EeUiVoters *v,
                       const char *path_utf8,
                       const uint32_t *rows,
                       uint32_t n,
                       int normalized,
                       char delim,
                       char *err,
                       size_t errcap)
{
    static const unsigned char bom[3] = {0xEF, 0xBB, 0xBF};
    uint32_t *vr;
    char *text = NULL;
    size_t len = 0;
    FILE *f;
    BOOL ok;
    set_err(err, errcap, "");
    if (v == NULL || path_utf8 == NULL || (vr = view_rows(v, rows, &n)) == NULL)
    {
        set_err(err, errcap, "Out of memory building the export.");
        return -1;
    }
    ok = EeVoterTable_FormatDelimitedUtf8(&v->t,
                                          vr,
                                          n,
                                          normalized && !v->roster ? TRUE : FALSE,
                                          delim == '\t' ? '\t' : ',',
                                          TRUE,
                                          &text,
                                          &len);
    free(vr);
    if (!ok)
    {
        set_err(err, errcap, "Out of memory building the export.");
        return -1;
    }
    /* (the Windows messages) */
    f = fopen(path_utf8, "wb");
    if (f == NULL)
    {
        set_err(err, errcap, "Could not create the export file.");
        free(text);
        return -1;
    }
    ok = fwrite(bom, 1, sizeof(bom), f) == sizeof(bom) && fwrite(text, 1, len, f) == len;
    if (fclose(f) != 0)
    {
        ok = FALSE;
    }
    free(text);
    if (!ok)
    {
        set_err(err, errcap, "Could not write the export file.");
        remove(path_utf8);
        return -1;
    }
    return (long)n;
}

void EeUiVoters_ExportName(const EeUiVoters *v,
                           const char *list_path,
                           int selection,
                           char *out,
                           size_t cap)
{
    const char *base, *slash, *dot, *what;
    size_t blen;
    char name[256];
    if (out == NULL || cap == 0)
    {
        return;
    }
    base = list_path != NULL ? list_path : "";
    slash = strrchr(base, '/');
    if (slash != NULL)
    {
        base = slash + 1;
    }
    dot = strrchr(base, '.');
    blen = dot != NULL && dot != base ? (size_t)(dot - base) : strlen(base);
    if (blen >= sizeof(name))
    {
        blen = sizeof(name) - 1;
    }
    memcpy(name, base, blen);
    name[blen] = '\0';
    if (selection)
    {
        what = v != NULL && v->roster ? "-Selected_Roster" : "-Selected_Voters";
    }
    else if (v != NULL && (v->filtered || v->mark_kind != EE_UI_MARKS_NONE))
    {
        what = v->roster ? "-Filtered_Roster" : "-Filtered_Voters";
    }
    else
    {
        what = v != NULL && v->roster ? "-Roster" : "-All_Voters";
    }
    StringCchPrintfA(out, cap, "%s%s.csv", name, what);
}

/* -------------------------------------------------------------------------- */
/* Saved filters                                                              */
/* -------------------------------------------------------------------------- */

static void file_name(const char *path, BOOL extension, char *out, size_t cap);

void EeUiFilterFile_Name(const char *list_path, char *out, size_t cap)
{
    char base[256];
    if (out == NULL || cap == 0)
    {
        return;
    }
    file_name(list_path, FALSE, base, sizeof(base));
    StringCchPrintfA(out, cap, "%s Filter.filter.csv", base[0] ? base : "Voter List");
}

static BOOL ends_with_ci(const char *s, const char *ext)
{
    size_t n = strlen(s), e = strlen(ext), i;
    if (n < e)
    {
        return FALSE;
    }
    for (i = 0; i < e; i++)
    {
        char a = s[n - e + i], b = ext[i];
        if (a >= 'A' && a <= 'Z')
        {
            a = (char)(a - 'A' + 'a');
        }
        if (a != b)
        {
            return FALSE;
        }
    }
    return TRUE;
}

char EeUiFilterFile_Path(const char *chosen, int type, char *out, size_t cap)
{
    char delim;
    if (chosen == NULL || out == NULL || cap == 0)
    {
        return 0;
    }
    if (ends_with_ci(chosen, ".tsv") || ends_with_ci(chosen, ".csv"))
    {
        delim = ends_with_ci(chosen, ".tsv") ? '\t' : ',';
        return SUCCEEDED(StringCchCopyA(out, cap, chosen)) ? delim : 0;
    }
    delim = type == 1 ? '\t' : ',';
    return SUCCEEDED(StringCchPrintfA(out,
                                      cap,
                                      "%s%s",
                                      chosen,
                                      delim == '\t' ? ".filter.tsv" : ".filter.csv"))
               ? delim
               : 0;
}

/* the list's columns, for a saved filter's titles (free() it) */
static EeFilterColumnRef *filter_columns(const EeUiVoters *v, uint32_t *n)
{
    EeFilterColumnRef *cols;
    uint32_t c;
    *n = v->ncols; /* (a roster offers the columns it shows, as on Windows) */
    cols = (EeFilterColumnRef *)calloc(*n ? *n : 1, sizeof(EeFilterColumnRef));
    for (c = 0; cols != NULL && c < *n; c++)
    {
        uint32_t t = v->cols[c];
        cols[c].column = t;
        cols[c].title = v->t.column_titles[t] != NULL ? v->t.column_titles[t] : L"";
    }
    return cols;
}

/* a wide message to UTF-8, without the "\r" of the engine's "\r\n" or a last line end */
static void utf8_text(const wchar_t *w, char *out, size_t cap)
{
    char *s = utf8_of(w), *d;
    const char *p;
    if (out == NULL || cap == 0)
    {
        free(s);
        return;
    }
    out[0] = '\0';
    if (s == NULL)
    {
        return;
    }
    for (p = s, d = s; *p; p++)
    {
        if (*p != '\r')
        {
            *d++ = *p;
        }
    }
    while (d > s && d[-1] == '\n')
    {
        d--;
    }
    *d = '\0';
    StringCchCopyA(out, cap, s);
    free(s);
}

int EeUiVoters_SaveFilter(const EeUiVoters *v,
                          const char *path_utf8,
                          const EeUiRule *rules,
                          uint32_t n,
                          char delim,
                          char *err,
                          size_t errcap)
{
    wchar_t path[4096], werr[256] = L"";
    EeFilterColumnRef *cols;
    EeFilterSet set;
    uint32_t ncols = 0, i;
    BOOL ok = TRUE;
    set_err(err, errcap, "");
    if (n == 0 || rules == NULL)
    {
        set_err(err, errcap, "There are no filter rules to export."); /* (Windows' words) */
        return 0;
    }
    if (v == NULL || path_utf8 == NULL ||
        MultiByteToWideChar(CP_UTF8, 0, path_utf8, -1, path, (int)ARRAYSIZE(path)) <= 0)
    {
        set_err(err, errcap, "Could not create the filter file.");
        return 0;
    }
    if ((cols = filter_columns(v, &ncols)) == NULL)
    {
        set_err(err, errcap, "Out of memory saving the filter.");
        return 0;
    }
    EeFilter_Init(&set);
    for (i = 0; ok && i < n; i++)
    {
        EeFilterRule r;
        to_engine_rule(v, &rules[i], &r);
        ok = EeFilter_Add(&set, &r);
    }
    if (!ok)
    {
        set_err(err, errcap, "Out of memory saving the filter.");
    }
    else if (!EeFilter_SaveFile(path,
                                &set,
                                cols,
                                ncols,
                                delim == '\t' ? L'\t' : L',',
                                werr,
                                ARRAYSIZE(werr)))
    {
        utf8_text(werr[0] ? werr : L"Could not write the filter file.", err, errcap);
        ok = FALSE;
    }
    EeFilter_Clear(&set);
    free(cols);
    return ok ? 1 : 0;
}

int EeUiVoters_LoadFilter(const EeUiVoters *v,
                          const char *path_utf8,
                          EeUiRule **rules,
                          uint32_t *n,
                          char *msg,
                          size_t msgcap,
                          char *detail,
                          size_t detailcap)
{
    wchar_t path[4096], werr[256] = L"", skipped[2048] = L"";
    EeFilterColumnRef *cols;
    EeFilterSet loaded;
    EeUiRule *out = NULL;
    uint32_t ncols = 0, nskip = 0, i;
    set_err(msg, msgcap, "");
    set_err(detail, detailcap, "");
    if (rules != NULL)
    {
        *rules = NULL;
    }
    if (n != NULL)
    {
        *n = 0;
    }
    if (v == NULL || path_utf8 == NULL || rules == NULL || n == NULL ||
        MultiByteToWideChar(CP_UTF8, 0, path_utf8, -1, path, (int)ARRAYSIZE(path)) <= 0)
    {
        set_err(msg, msgcap, "Could not open the filter file.");
        return -1;
    }
    if ((cols = filter_columns(v, &ncols)) == NULL)
    {
        set_err(msg, msgcap, "Out of memory loading the filter.");
        return -1;
    }
    EeFilter_Init(&loaded);
    if (!EeFilter_LoadFile(path,
                           cols,
                           ncols,
                           EE_FILTER_REL_ALL,
                           &loaded,
                           &nskip,
                           skipped,
                           ARRAYSIZE(skipped),
                           werr,
                           ARRAYSIZE(werr)))
    {
        utf8_text(werr[0] ? werr : L"Could not read the filter file.", msg, msgcap);
        free(cols);
        return -1;
    }
    free(cols);
    /* A date column needs a date to compare with less than / more than (as on Windows). */
    for (i = 0; i < loaded.count;)
    {
        if (!EeFilter_RuleIsValid(&loaded.rules[i], &v->t))
        {
            wchar_t line[EE_FILTER_VALUE_CCH + 96];
            StringCchPrintfW(line,
                             ARRAYSIZE(line),
                             L"\"%s\": not a date for a date column.\r\n",
                             loaded.rules[i].value);
            StringCchCatW(skipped, ARRAYSIZE(skipped), line);
            EeFilter_Remove(&loaded, i);
            nskip++;
            continue;
        }
        i++;
    }
    if (loaded.count == 0 && nskip == 0)
    {
        set_err(msg, msgcap, "The file holds no filter rules.");
        EeFilter_Clear(&loaded);
        return 0;
    }
    if (loaded.count > 0 && (out = (EeUiRule *)calloc(loaded.count, sizeof(EeUiRule))) == NULL)
    {
        set_err(msg, msgcap, "Out of memory loading the filter.");
        EeFilter_Clear(&loaded);
        return -1;
    }
    for (i = 0; i < loaded.count; i++)
    {
        const EeFilterRule *r = &loaded.rules[i];
        out[i].column = (uint32_t)scol(v, r->column);
        out[i].relation = (int)r->relation;
        out[i].exclude = r->action == EeFilt_Exclude;
        out[i].enabled = r->enabled != FALSE;
        if (WideCharToMultiByte(CP_UTF8,
                                0,
                                r->value,
                                -1,
                                out[i].value,
                                (int)sizeof(out[i].value),
                                NULL,
                                NULL) <= 0)
        {
            out[i].value[0] = '\0';
        }
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
        utf8_text(skipped, detail, detailcap);
    }
    *rules = out;
    *n = loaded.count;
    EeFilter_Clear(&loaded);
    return 1;
}

/* -------------------------------------------------------------------------- */
/* Reports                                                                    */
/* -------------------------------------------------------------------------- */

typedef struct ReportItem
{
    wchar_t *wide; /* the engine's value ("" for the blank row): sorting compares these */
    char *text;    /* UTF-8, as shown ("(blank)" for the blank row) */
    uint32_t count;
} ReportItem;

struct EeUiReport
{
    int kind;
    uint32_t column;      /* the list's column (EE_COL_PRECINCT / EE_COL_ADDRESS) */
    uint32_t rule_column; /* the window's (shown) column for Include / Exclude */
    ReportItem *items;
    uint32_t count;
    BOOL has_blank;
    int sort_column;
    BOOL sort_ascending;
};

static const char k_blank[] = "(blank)"; /* (Windows' label for empty cells) */

typedef struct ReportSortCx
{
    int column;
    BOOL ascending;
    BOOL numeric; /* precincts sort as numbers */
} ReportSortCx;

/* Windows' report_sort_cmp */
static int report_cmp(void *cxv, const void *a, const void *b)
{
    const ReportSortCx *cx = (const ReportSortCx *)cxv;
    const ReportItem *pa = (const ReportItem *)a;
    const ReportItem *pb = (const ReportItem *)b;
    int c;
    if (cx->column == EE_UI_REPORT_COLUMN_COUNT)
    {
        c = pa->count < pb->count ? -1 : (pa->count > pb->count ? 1 : 0);
        if (c == 0)
        {
            c = _wcsicmp(pa->wide, pb->wide);
        }
    }
    else if (cx->numeric)
    {
        unsigned long na = wcstoul(pa->wide, NULL, 10);
        unsigned long nb = wcstoul(pb->wide, NULL, 10);
        c = na < nb ? -1 : (na > nb ? 1 : 0);
        if (c == 0)
        {
            c = _wcsicmp(pa->wide, pb->wide);
        }
    }
    else
    {
        c = _wcsicmp(pa->wide, pb->wide);
    }
    return cx->ascending ? c : -c;
}

void EeUiReport_Free(EeUiReport *r)
{
    uint32_t i;
    if (r == NULL)
    {
        return;
    }
    for (i = 0; i < r->count; i++)
    {
        free(r->items[i].wide);
        free(r->items[i].text);
    }
    free(r->items);
    free(r);
}

EeUiReport *EeUiVoters_Report(const EeUiVoters *v, int kind, char *err, size_t errcap)
{
    EeValueCount *values = NULL;
    uint32_t n = 0, blank = 0, i;
    EeUiReport *r;
    uint32_t column = kind == EE_UI_REPORT_ADDRESS ? EE_COL_ADDRESS : EE_COL_PRECINCT;
    set_err(err, errcap, "");
    if (v == NULL)
    {
        return NULL;
    }
    if (!EeVoterTable_CollectValueCounts(&v->t, column, &values, &n, &blank) || n == 0)
    {
        /* (n == 0: every cell is empty, or there is no such column) */
        EeVoterTable_FreeValueCounts(values, n);
        set_err(err,
                errcap,
                kind == EE_UI_REPORT_ADDRESS ? "No address information available"
                                             : "No precinct information available");
        return NULL;
    }
    r = (EeUiReport *)calloc(1, sizeof(*r));
    if (r != NULL)
    {
        r->items = (ReportItem *)calloc((size_t)n + 1, sizeof(ReportItem));
    }
    if (r == NULL || r->items == NULL)
    {
        free(r);
        EeVoterTable_FreeValueCounts(values, n);
        set_err(err, errcap, "Out of memory.");
        return NULL;
    }
    r->kind = kind == EE_UI_REPORT_ADDRESS ? EE_UI_REPORT_ADDRESS : EE_UI_REPORT_PRECINCT;
    r->column = column;
    r->rule_column = scol(v, column) >= 0 ? (uint32_t)scol(v, column) : column;
    for (i = 0; i < n; i++)
    {
        ReportItem *it = &r->items[r->count++];
        it->wide = values[i].value; /* (taken over) */
        values[i].value = NULL;
        it->text = utf8_of(it->wide);
        it->count = values[i].count;
        if (it->wide == NULL || it->text == NULL)
        {
            break;
        }
    }
    EeVoterTable_FreeValueCounts(values, n);
    if (i < n)
    {
        EeUiReport_Free(r);
        set_err(err, errcap, "Out of memory.");
        return NULL;
    }
    if (blank > 0)
    {
        /* (as on Windows: the empty cells, shown because real values exist too) */
        ReportItem *it = &r->items[r->count];
        it->wide = (wchar_t *)calloc(1, sizeof(wchar_t));
        it->text = (char *)malloc(sizeof(k_blank));
        if (it->wide != NULL && it->text != NULL)
        {
            memcpy(it->text, k_blank, sizeof(k_blank));
            it->count = blank;
            r->count++;
            r->has_blank = TRUE;
        }
        else
        {
            free(it->wide);
            free(it->text);
            it->wide = NULL;
            it->text = NULL;
        }
    }
    EeUiReport_Sort(r, EE_UI_REPORT_COLUMN_VALUE, 1);
    return r;
}

int EeUiReport_Kind(const EeUiReport *r)
{
    return r != NULL ? r->kind : EE_UI_REPORT_PRECINCT;
}

const char *EeUiReport_Heading(int kind)
{
    return kind == EE_UI_REPORT_ADDRESS ? "Address" : "Precinct";
}

uint32_t EeUiReport_Rows(const EeUiReport *r)
{
    return r != NULL ? r->count : 0;
}

const char *EeUiReport_Value(const EeUiReport *r, uint32_t row)
{
    return r != NULL && row < r->count ? r->items[row].text : "";
}

int EeUiReport_IsBlank(const EeUiReport *r, uint32_t row)
{
    return r != NULL && row < r->count && r->items[row].wide[0] == L'\0';
}

uint32_t EeUiReport_Count(const EeUiReport *r, uint32_t row)
{
    return r != NULL && row < r->count ? r->items[row].count : 0;
}

void EeUiReport_Rule(const EeUiReport *r, uint32_t row, int exclude, EeUiRule *out)
{
    memset(out, 0, sizeof(*out));
    out->column = r != NULL ? r->rule_column : EE_COL_PRECINCT;
    out->relation = EE_UI_REL_IS;
    out->exclude = exclude ? 1 : 0;
    out->enabled = 1;
    if (r != NULL && row < r->count && !EeUiReport_IsBlank(r, row))
    {
        StringCchCopyA(out->value, sizeof(out->value), r->items[row].text);
    }
}

void EeUiReport_Sort(EeUiReport *r, int column, int ascending)
{
    ReportSortCx cx;
    if (r == NULL)
    {
        return;
    }
    r->sort_column =
        column == EE_UI_REPORT_COLUMN_COUNT ? EE_UI_REPORT_COLUMN_COUNT : EE_UI_REPORT_COLUMN_VALUE;
    r->sort_ascending = ascending ? TRUE : FALSE;
    cx.column = r->sort_column;
    cx.ascending = r->sort_ascending;
    cx.numeric = r->column == EE_COL_PRECINCT;
    if (r->count > 1)
    {
        qsort_s(r->items, r->count, sizeof(ReportItem), report_cmp, &cx);
    }
}

int EeUiReport_SortColumn(const EeUiReport *r, int *ascending)
{
    if (ascending != NULL)
    {
        *ascending = r != NULL && r->sort_ascending;
    }
    return r != NULL ? r->sort_column : EE_UI_REPORT_COLUMN_VALUE;
}

void EeUiReport_StatusText(const EeUiReport *r, char *out, size_t cap)
{
    const char *noun = EeUiReport_Kind(r) == EE_UI_REPORT_ADDRESS ? "addresses" : "precincts";
    if (out == NULL || cap == 0)
    {
        return;
    }
    StringCchPrintfA(out,
                     cap,
                     "%u unique %s%s",
                     EeUiReport_Rows(r),
                     noun,
                     r != NULL && r->has_blank ? " (includes blank)" : "");
}

/* the file's name, with or without its extension */
static void file_name(const char *path, BOOL extension, char *out, size_t cap)
{
    const char *base = path != NULL ? path : "", *slash = strrchr(base, '/'), *dot;
    size_t len;
    if (slash != NULL)
    {
        base = slash + 1;
    }
    dot = strrchr(base, '.');
    len = !extension && dot != NULL && dot != base ? (size_t)(dot - base) : strlen(base);
    if (len >= cap)
    {
        len = cap - 1;
    }
    memcpy(out, base, len);
    out[len] = '\0';
}

void EeUiReport_WindowTitle(const EeUiReport *r, const char *list_path, char *out, size_t cap)
{
    char name[256];
    if (out == NULL || cap == 0)
    {
        return;
    }
    file_name(list_path, TRUE, name, sizeof(name));
    StringCchPrintfA(out,
                     cap,
                     "%s Report - %s",
                     EeUiReport_Heading(EeUiReport_Kind(r)),
                     name[0] != '\0' ? name : "(voter list)");
}

typedef struct TextBuf
{
    char *data;
    size_t len, cap;
    BOOL failed;
} TextBuf;

static void tb_add(TextBuf *b, const char *s, size_t n)
{
    if (b->failed)
    {
        return;
    }
    if (b->len + n + 1 > b->cap)
    {
        size_t cap = b->cap > 0 ? b->cap : 4096;
        char *grown;
        while (b->len + n + 1 > cap)
        {
            cap *= 2;
        }
        grown = (char *)realloc(b->data, cap);
        if (grown == NULL)
        {
            b->failed = TRUE;
            return;
        }
        b->data = grown;
        b->cap = cap;
    }
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
}

/* one field, quoted as Windows' export quotes it (a delimiter, quote or line break inside) */
static void tb_field(TextBuf *b, const char *s, char delim)
{
    const char *p;
    if (strchr(s, delim) == NULL && strpbrk(s, "\"\r\n") == NULL)
    {
        tb_add(b, s, strlen(s));
        return;
    }
    tb_add(b, "\"", 1);
    for (p = s; *p != '\0'; p++)
    {
        tb_add(b, p, 1);
        if (*p == '"')
        {
            tb_add(b, "\"", 1);
        }
    }
    tb_add(b, "\"", 1);
}

char *EeUiReport_CopyText(const EeUiReport *r, const uint32_t *rows, uint32_t n, size_t *len)
{
    TextBuf b = {NULL, 0, 0, FALSE};
    uint32_t i;
    char num[16];
    if (len != NULL)
    {
        *len = 0;
    }
    if (r == NULL || rows == NULL)
    {
        return NULL;
    }
    tb_add(&b, "", 0);
    for (i = 0; i < n; i++)
    {
        if (rows[i] < r->count)
        {
            /* (Windows' Copy: "<value>\t<count>\r\n", not quoted) */
            const ReportItem *it = &r->items[rows[i]];
            StringCchPrintfA(num, sizeof(num), "%u", it->count);
            tb_add(&b, it->text, strlen(it->text));
            tb_add(&b, "\t", 1);
            tb_add(&b, num, strlen(num));
            tb_add(&b, "\r\n", 2);
        }
    }
    if (b.failed)
    {
        free(b.data);
        return NULL;
    }
    if (len != NULL)
    {
        *len = b.len;
    }
    return b.data;
}

long EeUiReport_Export(const EeUiReport *r,
                       const char *path_utf8,
                       const uint32_t *rows,
                       uint32_t n,
                       char delim,
                       char *err,
                       size_t errcap)
{
    static const unsigned char bom[3] = {0xEF, 0xBB, 0xBF};
    TextBuf b = {NULL, 0, 0, FALSE};
    uint32_t i, count = rows != NULL ? n : EeUiReport_Rows(r), wrote = 0;
    char num[16];
    FILE *f;
    BOOL ok;
    set_err(err, errcap, "");
    if (r == NULL || path_utf8 == NULL)
    {
        return -1;
    }
    delim = delim == '\t' ? '\t' : ',';
    tb_field(&b, EeUiReport_Heading(r->kind), delim);
    tb_add(&b, &delim, 1);
    tb_field(&b, "Number of Voters", delim);
    tb_add(&b, "\r\n", 2);
    for (i = 0; i < count; i++)
    {
        uint32_t row = rows != NULL ? rows[i] : i;
        if (row < r->count)
        {
            StringCchPrintfA(num, sizeof(num), "%u", r->items[row].count);
            tb_field(&b, r->items[row].text, delim);
            tb_add(&b, &delim, 1);
            tb_add(&b, num, strlen(num));
            tb_add(&b, "\r\n", 2);
            wrote++;
        }
    }
    if (b.failed)
    {
        free(b.data);
        set_err(err, errcap, "Out of memory building the export.");
        return -1;
    }
    f = fopen(path_utf8, "wb");
    if (f == NULL)
    {
        free(b.data);
        set_err(err, errcap, "Could not create the export file.");
        return -1;
    }
    ok = fwrite(bom, 1, sizeof(bom), f) == sizeof(bom) && fwrite(b.data, 1, b.len, f) == b.len;
    if (fclose(f) != 0)
    {
        ok = FALSE;
    }
    free(b.data);
    if (!ok)
    {
        set_err(err, errcap, "Could not write the export file.");
        remove(path_utf8);
        return -1;
    }
    return (long)wrote;
}

void EeUiReport_ExportName(const EeUiReport *r,
                           const char *list_path,
                           int selection,
                           char *out,
                           size_t cap)
{
    char name[256];
    if (out == NULL || cap == 0)
    {
        return;
    }
    file_name(list_path, FALSE, name, sizeof(name));
    StringCchPrintfA(out,
                     cap,
                     "%s-%s_%s.csv",
                     name,
                     selection ? "Selected" : "All",
                     EeUiReport_Kind(r) == EE_UI_REPORT_ADDRESS ? "Addresses" : "Precincts");
}

char *EeUiReport_MapUrl(const EeUiReport *r, uint32_t row, int engine)
{
    if (r == NULL || row >= r->count || EeUiReport_IsBlank(r, row))
    {
        return NULL;
    }
    return EeUiOptions_MapUrl(engine, r->items[row].text);
}

/* -------------------------------------------------------------------------- */
/* Voting Totals (Windows' RosterTotalsWindow, main.c)                        */
/* -------------------------------------------------------------------------- */

#define TOT_TOTAL_ROW   UINT32_MAX /* a Total row's date */
#define TOT_ALL_PARTIES (-1)       /* an all-parties row's party */

enum
{
    TOT_AVAIL = 0,     /* counts shown */
    TOT_NOT_AVAILABLE, /* no file or record of this method */
    TOT_NOT_LOADED     /* its files were found but not read (PDF) */
};

struct EeUiTotals
{
    EeRosterTotals tot;
    uint32_t *row_date;          /* row -> date index, or TOT_TOTAL_ROW */
    int32_t *row_party;          /* row -> party index, or TOT_ALL_PARTIES */
    uint32_t nrows;
    int method_col[EE_VM_COUNT]; /* the column of each method, or -1 */
    int avail[EE_VM_COUNT];      /* TOT_* */
    int col_party;               /* -1 when the roster has no parties */
    int col_total;
    int ncols;
};

/* voters for a (date, party) group and method @p m (-1: all); TOT_TOTAL_ROW and
 * TOT_ALL_PARTIES sum over every date / party (Windows' RosterTotals_Count) */
static uint32_t totals_count(const EeUiTotals *t, uint32_t date, int32_t party, int m)
{
    const EeRosterTotals *r = &t->tot;
    uint32_t d0 = date == TOT_TOTAL_ROW ? 0 : date,
             d1 = date == TOT_TOTAL_ROW ? r->ndates : date + 1;
    uint32_t p0 = party == TOT_ALL_PARTIES ? 0 : (uint32_t)party;
    uint32_t p1 = party == TOT_ALL_PARTIES ? r->nparties : (uint32_t)party + 1;
    uint32_t sum = 0, d, p;
    int k;
    for (d = d0; d < d1; d++)
    {
        for (p = p0; p < p1; p++)
        {
            const uint32_t *c = &r->counts[((size_t)d * r->nparties + p) * EE_VM_COUNT];
            for (k = 0; k < EE_VM_COUNT; k++)
            {
                if (m < 0 || k == m)
                {
                    sum += c[k];
                }
            }
        }
    }
    return sum;
}

/* "1234567" -> "1,234,567" (Windows' RosterTotals_Group) */
static void group_digits(uint32_t value, char *out, size_t cap)
{
    char raw[16];
    size_t n, i, o = 0;
    if (cap == 0)
    {
        return;
    }
    StringCchPrintfA(raw, sizeof(raw), "%u", value);
    n = strlen(raw);
    for (i = 0; i < n && o + 1 < cap; i++)
    {
        if (i > 0 && (n - i) % 3 == 0 && o + 2 < cap)
        {
            out[o++] = ',';
        }
        out[o++] = raw[i];
    }
    out[o] = '\0';
}

void EeUiTotals_Free(EeUiTotals *t)
{
    if (t == NULL)
    {
        return;
    }
    EeRoster_FreeTotals(&t->tot);
    free(t->row_date);
    free(t->row_party);
    free(t);
}

/* the columns and rows, as Windows' RosterTotals_Build lays them out: a row per date (per
 * party, and an all-parties row when several parties voted that day), then the Total rows */
static BOOL totals_build(EeUiTotals *t, const EeUiVoters *v)
{
    static const int order[] =
        {EE_VM_MAIL, EE_VM_EARLY, EE_VM_ELECTION_DAY, EE_VM_PROVISIONAL, EE_VM_LIMITED, EE_VM_NONE};
    const EeRosterTotals *r = &t->tot;
    size_t cap, k;
    uint32_t d, p;
    int col = 1;
    t->col_party = r->has_party ? col++ : -1;
    for (k = 0; k < ARRAYSIZE(order); k++)
    {
        int m = order[k];
        t->method_col[m] = -1;
        if (m == EE_VM_NONE && !r->method_present[EE_VM_NONE])
        {
            continue; /* ("Other" only for Voting Method values that are none of the five) */
        }
        t->method_col[m] = col++;
        if (r->method_present[m] || v->method_read[m])
        {
            t->avail[m] = TOT_AVAIL;
        }
        else
        {
            t->avail[m] = v->method_source[m] ? TOT_NOT_LOADED : TOT_NOT_AVAILABLE;
        }
    }
    t->col_total = col++;
    t->ncols = col;
    cap = (size_t)r->ndates * (r->nparties + 1u) + r->nparties + 2u;
    t->row_date = (uint32_t *)malloc(cap * sizeof(uint32_t));
    t->row_party = (int32_t *)malloc(cap * sizeof(int32_t));
    if (t->row_date == NULL || t->row_party == NULL)
    {
        return FALSE;
    }
    for (d = 0; d < r->ndates; d++)
    {
        uint32_t shown = 0;
        if (!r->has_party)
        {
            t->row_date[t->nrows] = d;
            t->row_party[t->nrows++] = TOT_ALL_PARTIES;
            continue;
        }
        for (p = 0; p < r->nparties; p++)
        {
            if (totals_count(t, d, (int32_t)p, -1) > 0)
            {
                t->row_date[t->nrows] = d;
                t->row_party[t->nrows++] = (int32_t)p;
                shown++;
            }
        }
        if (shown > 1)
        {
            t->row_date[t->nrows] = d;
            t->row_party[t->nrows++] = TOT_ALL_PARTIES;
        }
    }
    for (p = 0; r->has_party && p < r->nparties; p++)
    {
        t->row_date[t->nrows] = TOT_TOTAL_ROW;
        t->row_party[t->nrows++] = (int32_t)p;
    }
    t->row_date[t->nrows] = TOT_TOTAL_ROW;
    t->row_party[t->nrows++] = TOT_ALL_PARTIES;
    return TRUE;
}

EeUiTotals *EeUiVoters_Totals(const EeUiVoters *v, char *err, size_t errcap)
{
    EeUiTotals *t;
    set_err(err, errcap, "");
    if (v == NULL || !v->roster || v->t.row_count == 0)
    {
        set_err(err, errcap, "No voter roster loaded.");
        return NULL;
    }
    t = (EeUiTotals *)calloc(1, sizeof(EeUiTotals));
    if (t == NULL || !EeRoster_ComputeTotals(&v->t, &t->tot) || !totals_build(t, v))
    {
        EeUiTotals_Free(t);
        set_err(err,
                errcap,
                "Could not compute the voting totals (out of memory, or the roster has no Voting "
                "Method column).");
        return NULL;
    }
    return t;
}

uint32_t EeUiTotals_Rows(const EeUiTotals *t)
{
    return t != NULL ? t->nrows : 0;
}

uint32_t EeUiTotals_Columns(const EeUiTotals *t)
{
    return t != NULL ? (uint32_t)t->ncols : 0;
}

const char *EeUiTotals_Title(const EeUiTotals *t, uint32_t c)
{
    static const char *const titles[EE_VM_COUNT] = {"Other",
                                                    "Mail Ballot",
                                                    "Early Vote In-Person",
                                                    "Election Day In-Person",
                                                    "Provisional",
                                                    "Limited"};
    int m;
    if (t == NULL || c >= (uint32_t)t->ncols)
    {
        return "";
    }
    if (c == 0)
    {
        return "Date Voted";
    }
    if ((int)c == t->col_party)
    {
        return "Party";
    }
    if ((int)c == t->col_total)
    {
        return "Total";
    }
    for (m = 0; m < EE_VM_COUNT; m++)
    {
        if (t->method_col[m] == (int)c)
        {
            return titles[m];
        }
    }
    return "";
}

int EeUiTotals_IsTextColumn(const EeUiTotals *t, uint32_t c)
{
    return t != NULL && (c == 0 || (int)c == t->col_party);
}

int EeUiTotals_ColumnWidth(const EeUiTotals *t, uint32_t c)
{
    if (t == NULL || c >= (uint32_t)t->ncols)
    {
        return 0;
    }
    if (c == 0)
    {
        return 110;
    }
    if ((int)c == t->col_party || (int)c == t->col_total)
    {
        return 100;
    }
    if ((int)c == t->method_col[EE_VM_EARLY] || (int)c == t->method_col[EE_VM_ELECTION_DAY])
    {
        return 165;
    }
    return 120;
}

void EeUiTotals_Cell(const EeUiTotals *t,
                     uint32_t row,
                     uint32_t c,
                     int display,
                     char *out,
                     size_t cap)
{
    uint32_t date;
    int32_t party;
    int m;
    if (out == NULL || cap == 0)
    {
        return;
    }
    out[0] = '\0';
    if (t == NULL || row >= t->nrows || c >= (uint32_t)t->ncols)
    {
        return;
    }
    date = t->row_date[row];
    party = t->row_party[row];
    if (c == 0)
    {
        if (date == TOT_TOTAL_ROW)
        {
            StringCchCopyA(out, cap, "Total");
        }
        else if (t->tot.dates[date] == 0)
        {
            StringCchCopyA(out, cap, "(no date)");
        }
        else
        {
            uint32_t ymd = t->tot.dates[date];
            StringCchPrintfA(out,
                             cap,
                             "%02u/%02u/%04u",
                             (ymd / 100u) % 100u,
                             ymd % 100u,
                             ymd / 10000u);
        }
        return;
    }
    if ((int)c == t->col_party)
    {
        if (party == TOT_ALL_PARTIES)
        {
            StringCchCopyA(out, cap, "All parties");
        }
        else
        {
            const char *name = t->tot.parties[party];
            StringCchCopyA(out, cap, name[0] != '\0' ? name : k_blank);
        }
        return;
    }
    m = -1;
    if ((int)c != t->col_total)
    {
        for (m = 0; m < EE_VM_COUNT && t->method_col[m] != (int)c; m++)
        {
        }
        if (m == EE_VM_COUNT)
        {
            return;
        }
        if (t->avail[m] != TOT_AVAIL)
        {
            StringCchCopyA(out,
                           cap,
                           t->avail[m] == TOT_NOT_LOADED ? "not loaded" : "not available");
            return;
        }
    }
    if (display)
    {
        group_digits(totals_count(t, date, party, m), out, cap);
    }
    else
    {
        StringCchPrintfA(out, cap, "%u", totals_count(t, date, party, m));
    }
}

int EeUiTotals_IsBold(const EeUiTotals *t, uint32_t row)
{
    return t != NULL && row < t->nrows &&
           (t->row_date[row] == TOT_TOTAL_ROW ||
            (t->tot.has_party && t->row_party[row] == TOT_ALL_PARTIES));
}

void EeUiTotals_StatusText(const EeUiTotals *t, char *out, size_t cap)
{
    char a[32], b[32], c[32];
    int m;
    if (out == NULL || cap == 0)
    {
        return;
    }
    out[0] = '\0';
    if (t == NULL)
    {
        return;
    }
    group_digits(t->tot.counted, a, sizeof(a));
    if (t->tot.repeats > 0)
    {
        group_digits(t->tot.repeats, b, sizeof(b));
        group_digits(t->tot.repeated_ids, c, sizeof(c));
        StringCchPrintfA(out,
                         cap,
                         "%s voters, each counted once (%s later record%s of %s Voter ID%s not "
                         "counted)",
                         a,
                         b,
                         t->tot.repeats == 1 ? "" : "s",
                         c,
                         t->tot.repeated_ids == 1 ? "" : "s");
    }
    else
    {
        StringCchPrintfA(out, cap, "%s voters", a);
    }
    for (m = 0; m < EE_VM_COUNT; m++)
    {
        if (t->method_col[m] >= 0 && t->avail[m] == TOT_NOT_LOADED)
        {
            StringCchCatA(out, cap, " \xE2\x80\x94 not loaded = PDF rosters that cannot be read");
            break;
        }
    }
    StringCchCatA(out, cap, " \xE2\x80\x94 right-click to copy or export");
}

void EeUiTotals_WindowTitle(const EeUiTotals *t, const char *list_path, char *out, size_t cap)
{
    char name[256];
    (void)t;
    if (out == NULL || cap == 0)
    {
        return;
    }
    file_name(list_path, TRUE, name, sizeof(name));
    StringCchPrintfA(out, cap, "Voting Totals - %s", name[0] != '\0' ? name : "(voter roster)");
}

/* the heading and rows @p rows (NULL: all) with plain numbers, as Windows' Copy and Export */
static BOOL totals_text(const EeUiTotals *t,
                        const uint32_t *rows,
                        uint32_t n,
                        char delim,
                        TextBuf *b,
                        uint32_t *wrote)
{
    uint32_t i, c, count = rows != NULL ? n : EeUiTotals_Rows(t);
    char cell[512];
    *wrote = 0;
    for (c = 0; c < EeUiTotals_Columns(t); c++)
    {
        if (c > 0)
        {
            tb_add(b, &delim, 1);
        }
        tb_field(b, EeUiTotals_Title(t, c), delim);
    }
    tb_add(b, "\r\n", 2);
    for (i = 0; i < count; i++)
    {
        uint32_t row = rows != NULL ? rows[i] : i;
        if (row >= EeUiTotals_Rows(t))
        {
            continue;
        }
        for (c = 0; c < EeUiTotals_Columns(t); c++)
        {
            if (c > 0)
            {
                tb_add(b, &delim, 1);
            }
            EeUiTotals_Cell(t, row, c, 0, cell, sizeof(cell));
            tb_field(b, cell, delim);
        }
        tb_add(b, "\r\n", 2);
        (*wrote)++;
    }
    return !b->failed;
}

char *EeUiTotals_CopyText(const EeUiTotals *t, const uint32_t *rows, uint32_t n, size_t *len)
{
    TextBuf b = {NULL, 0, 0, FALSE};
    uint32_t wrote;
    if (len != NULL)
    {
        *len = 0;
    }
    if (t == NULL || rows == NULL || n == 0)
    {
        return NULL;
    }
    if (!totals_text(t, rows, n, '\t', &b, &wrote))
    {
        free(b.data);
        return NULL;
    }
    if (len != NULL)
    {
        *len = b.len;
    }
    return b.data;
}

long EeUiTotals_Export(const EeUiTotals *t,
                       const char *path_utf8,
                       const uint32_t *rows,
                       uint32_t n,
                       char delim,
                       char *err,
                       size_t errcap)
{
    static const unsigned char bom[3] = {0xEF, 0xBB, 0xBF};
    TextBuf b = {NULL, 0, 0, FALSE};
    uint32_t wrote = 0;
    FILE *f;
    BOOL ok;
    set_err(err, errcap, "");
    if (t == NULL || path_utf8 == NULL)
    {
        return -1;
    }
    if (!totals_text(t, rows, n, delim == '\t' ? '\t' : ',', &b, &wrote))
    {
        free(b.data);
        set_err(err, errcap, "Out of memory building the export.");
        return -1;
    }
    f = fopen(path_utf8, "wb");
    if (f == NULL)
    {
        free(b.data);
        set_err(err, errcap, "Could not create the export file.");
        return -1;
    }
    ok = fwrite(bom, 1, sizeof(bom), f) == sizeof(bom) && fwrite(b.data, 1, b.len, f) == b.len;
    if (fclose(f) != 0)
    {
        ok = FALSE;
    }
    free(b.data);
    if (!ok)
    {
        set_err(err, errcap, "Could not write the export file.");
        remove(path_utf8);
        return -1;
    }
    return (long)wrote;
}

void EeUiTotals_ExportName(const EeUiTotals *t,
                           const char *list_path,
                           int selection,
                           char *out,
                           size_t cap)
{
    char name[256];
    (void)t;
    if (out == NULL || cap == 0)
    {
        return;
    }
    file_name(list_path, FALSE, name, sizeof(name));
    StringCchPrintfA(out, cap, "%s-%sVoting_Totals.csv", name, selection ? "Selected_" : "");
}

/* -------------------------------------------------------------------------- */
/* Options                                                                    */
/* -------------------------------------------------------------------------- */

void EeUiOptions_Defaults(EeUiOptions *out)
{
    EeSettings s;
    if (out == NULL)
    {
        return;
    }
    EeSettings_Defaults(&s);
    out->copy_normalized = s.copy_prepend_normalized != FALSE;
    out->surname_first = s.name_surname_first != FALSE;
    out->zoom_percent = EeUiOptions_ClampZoom(s.zoom_percent);
    out->map_engine = s.map_engine;
}

int EeUiOptions_ClampZoom(int zoom)
{
    return zoom < EE_UI_ZOOM_MIN ? EE_UI_ZOOM_MIN : zoom > EE_UI_ZOOM_MAX ? EE_UI_ZOOM_MAX : zoom;
}

static int clamp_engine(int engine)
{
    return engine >= 0 && engine < EE_UI_MAP_COUNT ? engine
                                                   : EE_UI_MAP_GOOGLE; /* (as on Windows) */
}

void EeUiOptions_Load(EeUiOptions *out)
{
    EeSettings s;
    if (out == NULL)
    {
        return;
    }
    EeSettings_Load(&s); /* (the defaults for any value never saved) */
    out->copy_normalized = s.copy_prepend_normalized != FALSE;
    out->surname_first = s.name_surname_first != FALSE;
    out->zoom_percent = EeUiOptions_ClampZoom(s.zoom_percent);
    out->map_engine = clamp_engine(s.map_engine);
}

int EeUiOptions_Save(const EeUiOptions *in)
{
    EeSettings s;
    if (in == NULL)
    {
        return 0;
    }
    EeSettings_Load(&s); /* (the settings other windows keep stay as they are) */
    s.copy_prepend_normalized = in->copy_normalized ? TRUE : FALSE;
    s.name_surname_first = in->surname_first ? TRUE : FALSE;
    s.zoom_percent = EeUiOptions_ClampZoom(in->zoom_percent);
    s.map_engine = clamp_engine(in->map_engine);
    return EeSettings_Save(&s) ? 1 : 0;
}

const char *EeUiOptions_MapName(int engine)
{
    static const char *const names[EE_UI_MAP_COUNT] = {"Google Maps",
                                                       "Bing Maps",
                                                       "Apple Maps",
                                                       "Open Street Maps"};
    return names[clamp_engine(engine)];
}

char *EeUiOptions_MapUrl(int engine, const char *address)
{
    /* (Windows' App_BuildMapUrl) */
    static const char *const prefixes[EE_UI_MAP_COUNT] = {
        "https://www.google.com/maps/search/?api=1&query=",
        "https://www.bing.com/maps?q=",
        "https://maps.apple.com/?q=",
        "https://www.openstreetmap.org/search?query="};
    static const char hex[] = "0123456789ABCDEF";
    const char *prefix = prefixes[clamp_engine(engine)];
    size_t plen = strlen(prefix);
    const unsigned char *p;
    char *url, *q;
    if (address == NULL || address[0] == '\0')
    {
        return NULL;
    }
    url = (char *)malloc(plen + strlen(address) * 3 + 1);
    if (url == NULL)
    {
        return NULL;
    }
    memcpy(url, prefix, plen);
    q = url + plen;
    /* (Windows' App_AppendUrlEncoded: the UTF-8 bytes, all but A-Z a-z 0-9 - _ . ~ as %XX) */
    for (p = (const unsigned char *)address; *p != '\0'; p++)
    {
        if ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') ||
            *p == '-' || *p == '_' || *p == '.' || *p == '~')
        {
            *q++ = (char)*p;
        }
        else
        {
            *q++ = '%';
            *q++ = hex[*p >> 4];
            *q++ = hex[*p & 15];
        }
    }
    *q = '\0';
    return url;
}

char *EeUiVoters_CellLink(const EeUiVoters *v, uint32_t column, const char *text)
{
    /* (Windows' App_ExtractHttpsUrl, which Open link... uses in the scrolling pane only: a
     * voter list's own columns, or every column of a roster, whose other pane is hidden) */
    static const char prefix[] = "https://";
    const char *start = NULL, *p;
    size_t n, i;
    char *url;
    BOOL scrolling;
    if (v == NULL)
    {
        scrolling = column >= EE_FROZEN_COLUMN_COUNT; /* (a column of a voter list) */
    }
    else
    {
        scrolling = v->roster ? column < v->ncols : tcol(v, column) >= EE_FROZEN_COLUMN_COUNT;
    }
    if (text == NULL || !scrolling)
    {
        return NULL;
    }
    for (p = text; *p != '\0' && start == NULL; p++)
    {
        for (i = 0; prefix[i] != '\0'; i++)
        {
            char c = p[i];
            if (c >= 'A' && c <= 'Z')
            {
                c = (char)(c - 'A' + 'a');
            }
            if (c != prefix[i])
            {
                break;
            }
        }
        if (prefix[i] == '\0')
        {
            start = p;
        }
    }
    if (start == NULL)
    {
        return NULL;
    }
    n = strcspn(start, " \t\r\n<>\"'()");
    while (n > 0 && strchr(".,;:]", start[n - 1]) != NULL)
    {
        n--;
    }
    if (n <= 8)
    {
        return NULL;
    }
    if (n > 2047)
    {
        /* (Windows keeps 2047 UTF-16 units; here bytes, not cutting a character in two) */
        n = 2047;
        while (n > 0 && ((unsigned char)start[n] & 0xC0) == 0x80)
        {
            n--;
        }
    }
    url = (char *)malloc(n + 1);
    if (url != NULL)
    {
        memcpy(url, start, n);
        url[n] = '\0';
    }
    return url;
}

int EeUiVoters_SetSurnameFirst(EeUiVoters *v, int surname_first)
{
    BOOL want = surname_first ? TRUE : FALSE, used;
    int sort_column;
    uint32_t i;
    if (v == NULL || v->t.name_surname_first == want)
    {
        return 0;
    }
    /* (the engine re-sorts its own order when it is by Name; the window asks for a new view
     * instead, off the UI thread, which also brings the filter's map up to date) */
    sort_column = v->t.sort_column;
    used = sort_column == EE_COL_NAME;
    for (i = 0; i < v->filters.count && !used; i++)
    {
        used = v->filters.rules[i].enabled && v->filters.rules[i].column == EE_COL_NAME;
    }
    v->t.sort_column = -1;
    if (!EeVoterTable_SetNameSurnameFirst(&v->t, want, NULL, NULL))
    {
        v->t.sort_column = sort_column;
        return -1;
    }
    v->t.sort_column = sort_column;
    return used ? 1 : 0;
}

int EeUiVoters_SurnameFirst(const EeUiVoters *v)
{
    return v != NULL && v->t.name_surname_first != FALSE;
}

/* the current rules, for a view that only changes the order */
static EeUiRule *current_rules(const EeUiVoters *v, uint32_t *n)
{
    uint32_t i, count = EeUiVoters_RuleCount(v);
    EeUiRule *rules = (EeUiRule *)calloc(count > 0 ? count : 1, sizeof(EeUiRule));
    *n = 0;
    if (rules != NULL)
    {
        for (i = 0; i < count; i++)
        {
            EeUiVoters_GetRule(v, i, &rules[i]);
        }
        *n = count;
    }
    return rules;
}

void EeUiVoters_SortTo(EeUiVoters *v, uint32_t c, int ascending)
{
    uint32_t n;
    EeUiRule *rules;
    if (v == NULL || c >= v->ncols)
    {
        return;
    }
    rules = current_rules(v, &n);
    if (rules != NULL)
    {
        EeUiView *view = EeUiVoters_ComputeView(v, (int)c, ascending, rules, n, v->mark_kind);
        if (view != NULL)
        {
            EeUiVoters_SetView(v, view);
        }
        free(rules);
    }
}

void EeUiVoters_Sort(EeUiVoters *v, uint32_t c)
{
    int asc = 0;
    if (v != NULL && c < v->ncols)
    {
        int sc = EeUiVoters_SortColumn(v, &asc);
        EeUiVoters_SortTo(v, c, sc == (int)c ? !asc : 1);
    }
}

/* -------------------------------------------------------------------------- */
/* --selftest                                                                 */
/* -------------------------------------------------------------------------- */

static void selftest_rows(const EeUiVoters *v)
{
    uint32_t r;
    int asc = 0;
    int c = EeUiVoters_SortColumn(v, &asc);
    printf("sorted by column %d, ascending %d; %u of %u rows shown%s\n",
           c,
           asc,
           EeUiVoters_Rows(v),
           EeUiVoters_TotalRows(v),
           EeUiVoters_IsFiltered(v) ? " (filtered)" : "");
    for (r = 0; r < EeUiVoters_Rows(v) && r < 3; r++)
    {
        printf("row %u: \"%s\"\n", r, EeUiVoters_Cell(v, r, 1));
    }
}

/* Copy: the first two shown rows (counts only: a real list's cells are voters' data) */
static void selftest_copy(const EeUiVoters *v)
{
    uint32_t rows[2] = {0, 1}, n = EeUiVoters_Rows(v) < 2 ? EeUiVoters_Rows(v) : 2;
    size_t len = 0, lines = 0, seps = 0, i;
    char *text = EeUiVoters_CopyText(v, rows, n, 1, &len);
    if (text == NULL)
    {
        printf("copy failed\n");
        return;
    }
    for (i = 0; i < len; i++)
    {
        lines += text[i] == '\n';
        seps += text[i] == '\t' || text[i] == ',';
    }
    printf("copy: %u rows, %u lines, %u separators\n", n, (unsigned)lines, (unsigned)seps);
    free(text);
}

/* a duplicates view, as Windows shows it: grouped by Voter ID or Name, ascending */
static void selftest_marks(EeUiVoters *v, int marks, int sort_column)
{
    EeUiView *view;
    uint32_t marked = 0;
    char text[256];
    if (marks == EE_UI_MARKS_DUP_VOTERS && !EeUiVoters_HasBirthDate(v))
    {
        printf("%s: no birth date column\n", EeUiVoters_MarksLabel(marks));
        return;
    }
    view = EeUiVoters_ComputeView(v, sort_column, 1, NULL, 0, marks);
    if (view == NULL)
    {
        printf("%s: out of memory\n", EeUiVoters_MarksLabel(marks));
        return;
    }
    EeUiView_Marks(view, &marked);
    if (marked == 0)
    {
        printf("%s\n", EeUiVoters_MarksNoneText(marks));
        EeUiView_Free(view);
        return;
    }
    EeUiVoters_SetView(v, view);
    EeUiVoters_CountText(v, text, sizeof(text));
    printf("%s\n", text);
    selftest_rows(v);
}

/* a report, as its window first shows it (by value), then by Number of Voters, most first */
static void selftest_report(const EeUiVoters *v, int kind)
{
    char err[256], text[128];
    EeUiReport *r = EeUiVoters_Report(v, kind, err, sizeof(err));
    uint32_t i, all[3] = {0, 1, 2};
    size_t len = 0;
    char *copy;
    EeUiRule rule;
    if (r == NULL)
    {
        printf("%s report: %s\n", EeUiReport_Heading(kind), err);
        return;
    }
    EeUiReport_StatusText(r, text, sizeof(text));
    printf("%s report: %s\n", EeUiReport_Heading(kind), text);
    for (i = 0; i < EeUiReport_Rows(r) && i < 3; i++)
    {
        printf("  \"%s\" %u\n", EeUiReport_Value(r, i), EeUiReport_Count(r, i));
    }
    EeUiReport_Sort(r, EE_UI_REPORT_COLUMN_COUNT, 0);
    printf("  most voters: \"%s\" %u\n", EeUiReport_Value(r, 0), EeUiReport_Count(r, 0));
    copy = EeUiReport_CopyText(r, all, EeUiReport_Rows(r) < 3 ? EeUiReport_Rows(r) : 3, &len);
    EeUiReport_Rule(r, 0, 0, &rule);
    printf("  copy: %u bytes; include: column %u is \"%s\"\n",
           (unsigned)len,
           rule.column,
           rule.value);
    free(copy);
    EeUiReport_Free(r);
}

/* the Options: the names given name first and back (counts only), Copy without the normalized
 * fields, and Show in Maps' link in each map (a made-up address) */
static uint32_t selftest_comma_names(const EeUiVoters *v)
{
    uint32_t r, n = 0;
    for (r = 0; r < EeUiVoters_Rows(v); r++)
    {
        n += strchr(EeUiVoters_Cell(v, r, EE_UI_COLUMN_NAME), ',') != NULL;
    }
    return n;
}

static void selftest_options(EeUiVoters *v)
{
    uint32_t rows[2] = {0, 1}, n = EeUiVoters_Rows(v) < 2 ? EeUiVoters_Rows(v) : 2;
    size_t len[2] = {0, 0};
    char *text[2];
    int e, changed;
    EeUiOptions o;
    EeUiOptions_Defaults(&o);
    printf("options: copy normalized %d, surname first %d, zoom %d, %s\n",
           o.copy_normalized,
           o.surname_first,
           o.zoom_percent,
           EeUiOptions_MapName(o.map_engine));
    if (EeUiVoters_Columns(v) > EE_UI_COLUMN_NAME)
    {
        printf("names with a comma: %u of %u", selftest_comma_names(v), EeUiVoters_Rows(v));
        changed = EeUiVoters_SetSurnameFirst(v, 0);
        printf("; given name first: %u (new view %d)", selftest_comma_names(v), changed);
        changed = EeUiVoters_SetSurnameFirst(v, 1);
        printf("; surname first again: %u (new view %d)\n", selftest_comma_names(v), changed);
        /* (sorted by Name, a change of the names asks for a new view) */
        EeUiVoters_SetView(
            v,
            EeUiVoters_ComputeView(v, EE_UI_COLUMN_NAME, 1, NULL, 0, EE_UI_MARKS_NONE));
        changed = EeUiVoters_SetSurnameFirst(v, 0);
        printf("sorted by Name: given name first (new view %d)", changed);
        changed = EeUiVoters_SetSurnameFirst(v, 1);
        printf(", surname first (new view %d)\n", changed);
    }
    text[0] = EeUiVoters_CopyText(v, rows, n, 1, &len[0]);
    text[1] = EeUiVoters_CopyText(v, rows, n, 0, &len[1]);
    printf("copy of %u rows: %u bytes normalized, %u without\n",
           n,
           (unsigned)len[0],
           (unsigned)len[1]);
    free(text[0]);
    free(text[1]);
    for (e = 0; e < EE_UI_MAP_COUNT; e++)
    {
        char *url = EeUiOptions_MapUrl(e, "100 Main St, Austin TX");
        printf("%s: %s\n", EeUiOptions_MapName(e), url != NULL ? url : "(none)");
        free(url);
    }
}

/* a saved filter of two rules (a value with a comma and quotes) */
static void selftest_filter_rules(EeUiRule *rules)
{
    memset(rules, 0, 2 * sizeof(EeUiRule));
    rules[0].column = 1;
    rules[0].relation = EE_UI_REL_IS;
    rules[0].enabled = 1;
    StringCchCopyA(rules[0].value, sizeof(rules[0].value), "101");
    rules[1].column = EE_UI_COLUMN_NAME;
    rules[1].relation = EE_UI_REL_CONTAINS;
    rules[1].exclude = 1;
    StringCchCopyA(rules[1].value, sizeof(rules[1].value), "Smith, \"J\" ");
}

/* Open link...: Windows' rule on made-up cells, then the shown cells that have a link */
static void selftest_links(const EeUiVoters *v)
{
    static const char *const cells[] = {"See HTTPS://example.org/a?b=1&c=2.",
                                        "(https://example.org/x_(y)), more",
                                        "<https://example.org/q> \"https://example.net/\"",
                                        "https://",
                                        "http://example.org/",
                                        ""};
    uint32_t r, c, n = 0, columns = EeUiVoters_Columns(v);
    size_t i;
    char *url;
    for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++)
    {
        url = EeUiVoters_CellLink(NULL, EE_FROZEN_COLUMN_COUNT, cells[i]);
        printf("link %u: %s\n", (unsigned)i + 1, url != NULL ? url : "(none)");
        free(url);
    }
    url = EeUiVoters_CellLink(NULL, EE_UI_COLUMN_ADDRESS, cells[0]);
    printf("link in the Address column: %s\n", url != NULL ? url : "(none)");
    free(url);
    for (r = 0; r < EeUiVoters_Rows(v); r++)
    {
        for (c = 0; c < columns; c++)
        {
            url = EeUiVoters_CellLink(v, c, EeUiVoters_Cell(v, r, c));
            n += url != NULL;
            free(url);
        }
    }
    printf("shown cells with a link: %u\n", n);
}

/* Export Filter... / Load Filter...: save and load in CSV and TSV, then the files Windows
 * warns about (a column the list lacks, a date that is not one, no rules, no file) */
static void selftest_filter_file(const EeUiVoters *v)
{
    const char *tmp = getenv("TMPDIR");
    char base[512], path[640], msg[256], detail[1024], name[256];
    EeUiRule rules[2], *got = NULL;
    uint32_t n = 0;
    int t, r, saved;
    FILE *f;
    if (EeUiVoters_Columns(v) <= EE_UI_COLUMN_ADDRESS)
    {
        return;
    }
    StringCchPrintfA(base,
                     sizeof(base),
                     "%s/ee_selftest_%ld",
                     tmp != NULL && tmp[0] ? tmp : "/tmp",
                     (long)getpid());
    EeUiFilterFile_Name("lists/sample.csv", name, sizeof(name));
    printf("filter file name: \"%s\"", name);
    EeUiFilterFile_Name(NULL, name, sizeof(name));
    printf(", without a list \"%s\"\n", name);
    selftest_filter_rules(rules);
    for (t = 0; t < 2; t++)
    {
        char delim = EeUiFilterFile_Path(base, t, path, sizeof(path));
        saved = EeUiVoters_SaveFilter(v, path, rules, 2, delim, msg, sizeof(msg));
        r = EeUiVoters_LoadFilter(v, path, &got, &n, msg, sizeof(msg), detail, sizeof(detail));
        printf("filter file %s (%s): saved %d, loaded %d, %u rules, the same %d\n",
               t == 1 ? "TSV" : "CSV",
               path + strlen(base),
               saved,
               r,
               n,
               n == 2 && got != NULL && memcmp(got, rules, sizeof(rules)) == 0);
        free(got);
        got = NULL;
        remove(path);
    }
    t = EeUiFilterFile_Path("a.TSV", 0, path, sizeof(path)) == '\t';
    printf("a typed extension: \"%s\" (tabs %d)\n", path, t);

    StringCchPrintfA(path, sizeof(path), "%s.windows.filter.csv", base);
    if ((f = fopen(path, "wb")) != NULL)
    {
        fprintf(
            f,
            "\xEF\xBB\xBF"
            "Column,Relation,Value,Action\r\nprecinct,is,101,Include\r\nNo Such Column,is,1,Include\r\n"
            "EDRDAT,less than,soon,Include\r\n");
        fclose(f);
    }
    r = EeUiVoters_LoadFilter(v, path, &got, &n, msg, sizeof(msg), detail, sizeof(detail));
    printf(
        "filter file with a column the list lacks and a date that is not one: %d, %u rule (column %u); "
        "%s\n%s\n",
        r,
        n,
        got != NULL ? got[0].column : 99,
        msg,
        detail);
    free(got);
    got = NULL;
    if ((f = fopen(path, "wb")) != NULL)
    {
        fprintf(f, "Column,Relation,Value,Action\r\n");
        fclose(f);
    }
    r = EeUiVoters_LoadFilter(v, path, &got, &n, msg, sizeof(msg), detail, sizeof(detail));
    printf("filter file without rules: %d, %s\n", r, msg);
    remove(path);
    r = EeUiVoters_LoadFilter(v, path, &got, &n, msg, sizeof(msg), detail, sizeof(detail));
    printf("no filter file: %d, %s\n", r, msg);
    saved = EeUiVoters_SaveFilter(v, path, rules, 0, ',', msg, sizeof(msg));
    printf("saving no rules: %d, %s\n", saved, msg);
}

int EeUiVoters_SelfTest(const char *path_utf8)
{
    char err[1024] = "";
    EeUiVoters *v = EeUiVoters_Load(path_utf8, 1, NULL, NULL, NULL, err, sizeof(err));
    uint32_t c, nc;
    if (v == NULL)
    {
        printf("load failed: %s\n", err);
        return 1;
    }
    nc = EeUiVoters_Columns(v);
    printf("rows %u, columns %u\n", EeUiVoters_Rows(v), nc);
    for (c = 0; c < nc && c < 8; c++)
    {
        printf("column %u: \"%s\"%s\n",
               c,
               EeUiVoters_Title(v, c),
               EeUiVoters_IsNumeric(v, c) ? " (numeric)" : "");
    }
    if (nc > 1 && EeUiVoters_Rows(v) > 0)
    {
        EeUiRule rules[2];
        EeUiView *view;
        EeUiVoters_Sort(v, 1);
        selftest_rows(v);
        EeUiVoters_Sort(v, 1); /* (again: reversed) */
        selftest_rows(v);
        EeUiVoters_SortTo(v, 1, 1);
        selftest_rows(v);

        /* filter: include the first row's column-1 value, then also exclude that value
         * (an exclude wins), as the window's Include / Exclude commands add them */
        memset(rules, 0, sizeof(rules));
        rules[0].column = 1;
        rules[0].relation = EE_UI_REL_IS;
        rules[0].enabled = 1;
        StringCchCopyA(rules[0].value, sizeof(rules[0].value), EeUiVoters_Cell(v, 0, 1));
        printf("filter: column 1 is \"%s\", include\n", rules[0].value);
        view = EeUiVoters_ComputeView(v, 1, 0, rules, 1, EE_UI_MARKS_NONE);
        EeUiVoters_SetView(v, view);
        selftest_rows(v);
        rules[1] = rules[0];
        rules[1].exclude = 1;
        printf("filter: and column 1 is \"%s\", exclude\n", rules[1].value);
        EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, 1, 0, rules, 2, EE_UI_MARKS_NONE));
        selftest_rows(v);
        rules[1].enabled = 0;
        printf("filter: the exclude disabled; file order\n");
        EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, -1, 1, rules, 2, EE_UI_MARKS_NONE));
        selftest_rows(v);
        printf("rules kept: %u\n", EeUiVoters_RuleCount(v));
        selftest_copy(v);
        EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, -1, 1, NULL, 0, EE_UI_MARKS_NONE));
        selftest_marks(v, EE_UI_MARKS_DUP_IDS, EE_UI_COLUMN_VOTER_ID);
        selftest_marks(v, EE_UI_MARKS_DUP_VOTERS, EE_UI_COLUMN_NAME);
        /* (a filter does not change a report: it counts every loaded row) */
        EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, -1, 1, rules, 1, EE_UI_MARKS_NONE));
        selftest_report(v, EE_UI_REPORT_PRECINCT);
        selftest_report(v, EE_UI_REPORT_ADDRESS);
        selftest_options(v);
        selftest_filter_file(v);
        selftest_links(v);
    }
    EeUiVoters_Free(v);
    return 0;
}

/* the shown column titled @p title, or -1 */
static int selftest_column(const EeUiVoters *v, const char *title)
{
    uint32_t c;
    for (c = 0; c < EeUiVoters_Columns(v); c++)
    {
        if (strcmp(EeUiVoters_Title(v, c), title) == 0)
        {
            return (int)c;
        }
    }
    return -1;
}

/* Display Voting Totals...: every cell as shown, the bold rows, the status, Copy and Export
 * (to @p scratch, removed after) */
static void selftest_totals(const EeUiVoters *v, const char *scratch)
{
    char err[256], text[600], cell[128];
    EeUiTotals *t = EeUiVoters_Totals(v, err, sizeof(err));
    uint32_t r, c, rows[2];
    char *copy;
    long wrote;
    FILE *f;
    if (t == NULL)
    {
        printf("voting totals: %s\n", err);
        return;
    }
    EeUiTotals_WindowTitle(t, "rosters/sample_roster.csv", text, sizeof(text));
    printf("voting totals: %u rows, %u columns, \"%s\"\n",
           EeUiTotals_Rows(t),
           EeUiTotals_Columns(t),
           text);
    printf("totals columns:");
    for (c = 0; c < EeUiTotals_Columns(t); c++)
    {
        printf(" \"%s\"%s %d,",
               EeUiTotals_Title(t, c),
               EeUiTotals_IsTextColumn(t, c) ? " (left)" : "",
               EeUiTotals_ColumnWidth(t, c));
    }
    printf("\n");
    for (r = 0; r < EeUiTotals_Rows(t); r++)
    {
        printf("totals row %u%s:", r, EeUiTotals_IsBold(t, r) ? " (bold)" : "");
        for (c = 0; c < EeUiTotals_Columns(t); c++)
        {
            EeUiTotals_Cell(t, r, c, 1, cell, sizeof(cell));
            printf(" %s |", cell);
        }
        printf("\n");
    }
    EeUiTotals_StatusText(t, text, sizeof(text));
    printf("totals status: %s\n", text);
    rows[0] = 0;
    rows[1] = EeUiTotals_Rows(t) - 1;
    copy = EeUiTotals_CopyText(t, rows, 2, NULL);
    printf("totals copy:\n%s", copy != NULL ? copy : "(none)\n");
    free(copy);
    EeUiTotals_ExportName(t, "rosters/sample_roster.csv", 0, text, sizeof(text));
    printf("totals export names: \"%s\"", text);
    EeUiTotals_ExportName(t, "rosters/sample_roster.csv", 1, text, sizeof(text));
    printf(", \"%s\"\n", text);
    wrote = EeUiTotals_Export(t, scratch, NULL, 0, ',', err, sizeof(err));
    printf("totals export: %ld rows%s%s", wrote, err[0] ? ", " : "", err);
    if ((f = fopen(scratch, "rb")) != NULL)
    {
        size_t got = fread(text, 1, 120, f);
        text[got] = '\0';
        fclose(f);
        printf(", BOM %d, heading: %.*s\n",
               (unsigned char)text[0] == 0xEF,
               (int)strcspn(text + 3, "\r\n"),
               text + 3);
    }
    else
    {
        printf("\n");
    }
    remove(scratch);
    EeUiTotals_Free(t);
}

int EeUiVoters_RosterSelfTest(const char *const *paths_utf8, uint32_t n)
{
    char err[1024] = "", text[256], path[640], msg[256], detail[512];
    const char *tmp = getenv("TMPDIR"), *summary;
    EeUiVoters *v = EeUiVoters_LoadRoster(paths_utf8, n, 1, NULL, NULL, NULL, err, sizeof(err));
    EeUiRule rule, *got = NULL;
    uint32_t c, nc, ngot = 0;
    int method, notable = 0, r;
    char *link;
    if (v == NULL)
    {
        printf("roster load failed: %s\n", err);
        return 1;
    }
    nc = EeUiVoters_Columns(v);
    printf("roster %d: rows %u, columns %u\n", EeUiVoters_IsRoster(v), EeUiVoters_Rows(v), nc);
    for (c = 0; c < nc; c++)
    {
        printf("column %u: \"%s\"%s\n",
               c,
               EeUiVoters_Title(v, c),
               EeUiVoters_IsNumeric(v, c) ? " (numeric)" : "");
    }
    EeUiVoters_CountText(v, text, sizeof(text));
    printf("status: %s\n", text);
    printf("address column: %d\n", EeUiVoters_AddressColumn(v));
    method = selftest_column(v, "Voting Method");
    if (method < 0 || EeUiVoters_Rows(v) == 0)
    {
        printf("no Voting Method column\n");
        EeUiVoters_Free(v);
        return 0;
    }

    /* a heading click, then the right-click Include on a Voting Method cell */
    EeUiVoters_SortTo(v, (uint32_t)method, 1);
    selftest_rows(v);
    printf("first row's method: \"%s\"\n", EeUiVoters_Cell(v, 0, (uint32_t)method));
    memset(&rule, 0, sizeof(rule));
    rule.column = (uint32_t)method;
    rule.relation = EE_UI_REL_IS;
    rule.enabled = 1;
    StringCchCopyA(rule.value, sizeof(rule.value), "Mail Ballot");
    EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, method, 1, &rule, 1, EE_UI_MARKS_NONE));
    EeUiVoters_CountText(v, text, sizeof(text));
    printf("filter Voting Method is \"Mail Ballot\": %s\n", text);
    EeUiVoters_GetRule(v, 0, &rule);
    printf("rule kept on column %u\n", rule.column);
    selftest_copy(v); /* (never the normalized fields first: a roster's own columns) */
    EeUiVoters_ExportName(v, "rosters/sample_roster.csv", 0, text, sizeof(text));
    printf("export names: \"%s\"", text);
    EeUiVoters_ExportName(v, "rosters/sample_roster.csv", 1, text, sizeof(text));
    printf(", \"%s\"", text);
    EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, -1, 1, NULL, 0, EE_UI_MARKS_NONE));
    EeUiVoters_ExportName(v, "rosters/sample_roster.csv", 0, text, sizeof(text));
    printf(", \"%s\"\n", text);

    /* a saved filter names Voting Method, and loads back onto the same shown column */
    StringCchPrintfA(path,
                     sizeof(path),
                     "%s/ee_selftest_roster_%ld.filter.csv",
                     tmp != NULL && tmp[0] ? tmp : "/tmp",
                     (long)getpid());
    rule.enabled = 1;
    r = EeUiVoters_SaveFilter(v, path, &rule, 1, ',', msg, sizeof(msg));
    r = r == 1
            ? EeUiVoters_LoadFilter(v, path, &got, &ngot, msg, sizeof(msg), detail, sizeof(detail))
            : -1;
    printf("roster filter file: loaded %d, %u rule on column %u\n",
           r,
           ngot,
           got != NULL ? got[0].column : 99);
    free(got);
    remove(path);

    selftest_marks(v, EE_UI_MARKS_DUP_IDS, EE_UI_COLUMN_VOTER_ID);
    EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, -1, 1, NULL, 0, EE_UI_MARKS_NONE));
    selftest_report(v, EE_UI_REPORT_PRECINCT);
    /* (a filter does not change the totals: they count every loaded record) */
    EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, -1, 1, &rule, 1, EE_UI_MARKS_NONE));
    selftest_totals(v, path);
    link = EeUiVoters_CellLink(v, 0, "see https://example.org/roster.");
    printf("roster link in the Voter ID column: %s\n", link != NULL ? link : "(none)");
    free(link);
    summary = EeUiVoters_LoadSummary(v, &notable);
    printf("load summary (notable %d):\n%s\n", notable, summary != NULL ? summary : "(none)");
    EeUiVoters_Free(v);
    return 0;
}
