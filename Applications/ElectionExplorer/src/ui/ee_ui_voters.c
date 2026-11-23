/**
 * @file ee_ui_voters.c
 * @brief The voter list of ee_ui_voters.h over the engine's EeVoterTable.
 */

#include "ee_ui_voters.h"
#include "voter_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>

struct EeUiVoters
{
    EeVoterTable t;
    char *titles[EE_MAX_COLUMNS]; /* UTF-8 */
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
    EeVoterTable_Clear(&v->t);
    free(v);
}

uint32_t EeUiVoters_Rows(const EeUiVoters *v)
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
    if (v == NULL || row >= v->t.row_count || c >= v->t.column_count)
    {
        return "";
    }
    return EeVoterTable_GetViewCellUtf8(&v->t, row, c);
}

int EeUiVoters_IsNumeric(const EeUiVoters *v, uint32_t c)
{
    return v != NULL && c < v->t.column_count && EeVoterTable_ColumnIsNumericOrDate(&v->t, c);
}

void EeUiVoters_Sort(EeUiVoters *v, uint32_t c)
{
    if (v != NULL && c < v->t.column_count)
    {
        EeVoterTable_SortByColumn(&v->t, c);
    }
}

void EeUiVoters_SortTo(EeUiVoters *v, uint32_t c, int ascending)
{
    int k;
    if (v == NULL || c >= v->t.column_count)
    {
        return;
    }
    /* (the engine's sort takes a new column ascending and reverses the same column) */
    for (k = 0;
         k < 2 && !(v->t.sort_column == (int)c && (v->t.sort_ascending != 0) == (ascending != 0));
         k++)
    {
        EeVoterTable_SortByColumn(&v->t, c);
    }
}

int EeUiVoters_SortColumn(const EeUiVoters *v, int *ascending)
{
    if (ascending != NULL)
    {
        *ascending = v != NULL && v->t.sort_ascending;
    }
    return v != NULL ? v->t.sort_column : -1;
}

static void selftest_rows(const EeUiVoters *v)
{
    uint32_t r;
    int asc = 0;
    int c = EeUiVoters_SortColumn(v, &asc);
    printf("sorted by column %d, ascending %d\n", c, asc);
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
    if (nc > 1)
    {
        EeUiVoters_Sort(v, 1);
        selftest_rows(v);
        EeUiVoters_SortTo(v, 1, 0);
        selftest_rows(v);
        EeUiVoters_SortTo(v, 1, 0); /* (already descending: no change) */
        EeUiVoters_SortTo(v, 1, 1);
        selftest_rows(v);
    }
    EeUiVoters_Free(v);
    return 0;
}
