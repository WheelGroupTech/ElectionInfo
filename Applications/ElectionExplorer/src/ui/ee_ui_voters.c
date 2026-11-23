/**
 * @file ee_ui_voters.c
 * @brief The voter list of ee_ui_voters.h over the engine's EeVoterTable and EeFilterSet.
 */

#include "ee_ui_voters.h"
#include "filter.h"
#include "voter_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

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
};

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

static void to_engine_rule(const EeUiRule *r, EeFilterRule *out)
{
    memset(out, 0, sizeof(*out));
    out->column = r->column;
    out->relation = (r->relation >= 0 && r->relation < EE_UI_REL_COUNT)
                        ? (EeFilterRelation)r->relation
                        : EeRel_Is;
    out->action = r->exclude ? EeFilt_Exclude : EeFilt_Include;
    out->enabled = r->enabled ? TRUE : FALSE;
    wide_value(r->value, out->value, ARRAYSIZE(out->value));
}

EeUiVoters *EeUiVoters_Load(const char *path_utf8,
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
    EeFilter_Init(&v->filters);
    st = EeVoterTable_LoadFromFile(path,
                                   &v->t,
                                   (volatile LONG *)cancel,
                                   on_progress,
                                   &pc,
                                   werr,
                                   ARRAYSIZE(werr));
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
                                         werr[0] ? werr : L"The file could not be loaded.",
                                         -1,
                                         err,
                                         (int)errcap,
                                         NULL,
                                         NULL) <= 0)
            {
                StringCchCopyA(err, errcap, "The file could not be loaded.");
            }
        }
        EeVoterTable_Clear(&v->t);
        free(v);
        return NULL;
    }
    for (c = 0; c < v->t.column_count; c++)
    {
        v->titles[c] = utf8_of(v->t.column_titles[c]);
    }
    v->shown = v->t.row_count;
    return v;
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
    return v != NULL ? v->t.column_count : 0;
}

const char *EeUiVoters_Title(const EeUiVoters *v, uint32_t c)
{
    return v != NULL && c < v->t.column_count && v->titles[c] != NULL ? v->titles[c] : "";
}

const char *EeUiVoters_Cell(const EeUiVoters *v, uint32_t row, uint32_t c)
{
    if (v == NULL || row >= v->shown || c >= v->t.column_count)
    {
        return "";
    }
    return EeVoterTable_GetViewCellUtf8(&v->t, v->map != NULL ? v->map[row] : row, c);
}

int EeUiVoters_IsNumeric(const EeUiVoters *v, uint32_t c)
{
    return v != NULL && c < v->t.column_count && EeVoterTable_ColumnIsNumericOrDate(&v->t, c);
}

int EeUiVoters_SortColumn(const EeUiVoters *v, int *ascending)
{
    if (ascending != NULL)
    {
        *ascending = v != NULL && v->t.sort_ascending;
    }
    return v != NULL ? v->t.sort_column : -1;
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
    out->column = r->column;
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
    if (v == NULL || rule == NULL || rule->column >= v->t.column_count)
    {
        if (why != NULL && whycap > 0)
        {
            StringCchCopyA(why, whycap, "Choose a column.");
        }
        return 0;
    }
    to_engine_rule(rule, &r);
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
                         "%s within filter: %u of %u voters",
                         EeUiVoters_MarksLabel(v->mark_kind),
                         v->shown,
                         v->t.row_count);
    }
    else if (v->mark_kind != EE_UI_MARKS_NONE)
    {
        StringCchPrintfA(out,
                         cap,
                         "Showing %u %s of %u voters",
                         v->shown,
                         EeUiVoters_MarksLabel(v->mark_kind),
                         v->t.row_count);
    }
    else if (v->filtered)
    {
        StringCchPrintfA(out, cap, "Filtered: %u of %u voters", v->shown, v->t.row_count);
    }
    else
    {
        StringCchPrintfA(out, cap, "%u voters", v->shown);
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
    if (sort_column >= 0 && (uint32_t)sort_column < v->t.column_count && n > 0)
    {
        if (!ascending)
        {
            /* (SortByColumn reverses a column that is already the sort column) */
            tmp->sort_column = sort_column;
            tmp->sort_ascending = TRUE;
        }
        EeVoterTable_SortByColumn(tmp, (uint32_t)sort_column);
    }
    view->sort_column = tmp->sort_column;
    view->sort_ascending = tmp->sort_ascending;

    /* the filter, over the new order */
    for (i = 0; i < nrules; i++)
    {
        EeFilterRule r;
        to_engine_rule(&rules[i], &r);
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
    if (!EeVoterTable_FormatCopyUtf8(&v->t, vr, n, normalized ? TRUE : FALSE, &text, len))
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
                                          normalized ? TRUE : FALSE,
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
        what = "-Selected_Voters";
    }
    else if (v != NULL && (v->filtered || v->mark_kind != EE_UI_MARKS_NONE))
    {
        what = "-Filtered_Voters";
    }
    else
    {
        what = "-All_Voters";
    }
    StringCchPrintfA(out, cap, "%s%s.csv", name, what);
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
    if (v == NULL || c >= v->t.column_count)
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
    if (v != NULL && c < v->t.column_count)
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

int EeUiVoters_SelfTest(const char *path_utf8)
{
    char err[1024] = "";
    EeUiVoters *v = EeUiVoters_Load(path_utf8, NULL, NULL, NULL, err, sizeof(err));
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
    }
    EeUiVoters_Free(v);
    return 0;
}
