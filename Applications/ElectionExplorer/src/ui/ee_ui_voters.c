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
    uint32_t *map; /* shown row -> view row; NULL when every row is shown or none is */
    uint32_t shown;
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

void EeUiView_Free(EeUiView *view)
{
    if (view != NULL)
    {
        free(view->view_index);
        EeFilter_Clear(&view->filters);
        free(view->map);
        free(view);
    }
}

EeUiView *EeUiVoters_ComputeView(const EeUiVoters *v,
                                 int sort_column,
                                 int ascending,
                                 const EeUiRule *rules,
                                 uint32_t nrules)
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
    free(view);
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
        EeUiView *view = EeUiVoters_ComputeView(v, (int)c, ascending, rules, n);
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
        view = EeUiVoters_ComputeView(v, 1, 0, rules, 1);
        EeUiVoters_SetView(v, view);
        selftest_rows(v);
        rules[1] = rules[0];
        rules[1].exclude = 1;
        printf("filter: and column 1 is \"%s\", exclude\n", rules[1].value);
        EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, 1, 0, rules, 2));
        selftest_rows(v);
        rules[1].enabled = 0;
        printf("filter: the exclude disabled; file order\n");
        EeUiVoters_SetView(v, EeUiVoters_ComputeView(v, -1, 1, rules, 2));
        selftest_rows(v);
        printf("rules kept: %u\n", EeUiVoters_RuleCount(v));
    }
    EeUiVoters_Free(v);
    return 0;
}
