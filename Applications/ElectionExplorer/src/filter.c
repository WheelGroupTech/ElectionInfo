/**
 * @file filter.c
 * @brief Filter rule storage and ProcMon-style evaluation.
 */

#include "filter.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>
#include <intsafe.h>

void EeFilter_Init(EeFilterSet *set)
{
    if (set == NULL)
    {
        return;
    }
    ZeroMemory(set, sizeof(*set));
}

void EeFilter_Clear(EeFilterSet *set)
{
    if (set == NULL)
    {
        return;
    }
    free(set->rules);
    ZeroMemory(set, sizeof(*set));
}

BOOL EeFilter_Add(EeFilterSet *set, const EeFilterRule *rule)
{
    if (set == NULL || rule == NULL)
    {
        return FALSE;
    }
    if (set->count == set->cap)
    {
        uint32_t new_cap = set->cap ? set->cap * 2u : 16u;
        EeFilterRule *grown;
        size_t bytes;
        if (new_cap < set->cap)
        {
            return FALSE;
        }
        if (FAILED(SizeTMult((size_t)new_cap, sizeof(EeFilterRule), &bytes)))
        {
            return FALSE;
        }
        grown = (EeFilterRule *)realloc(set->rules, bytes);
        if (grown == NULL)
        {
            return FALSE;
        }
        set->rules = grown;
        set->cap = new_cap;
    }
    set->rules[set->count++] = *rule;
    return TRUE;
}

BOOL EeFilter_Set(EeFilterSet *set, uint32_t index, const EeFilterRule *rule)
{
    if (set == NULL || rule == NULL || index >= set->count)
    {
        return FALSE;
    }
    set->rules[index] = *rule;
    return TRUE;
}

BOOL EeFilter_Remove(EeFilterSet *set, uint32_t index)
{
    uint32_t i;
    if (set == NULL || index >= set->count)
    {
        return FALSE;
    }
    for (i = index + 1; i < set->count; i++)
    {
        set->rules[i - 1] = set->rules[i];
    }
    set->count--;
    return TRUE;
}

BOOL EeFilter_Copy(EeFilterSet *dst, const EeFilterSet *src)
{
    uint32_t i;
    if (dst == NULL || src == NULL)
    {
        return FALSE;
    }
    EeFilter_Clear(dst);
    for (i = 0; i < src->count; i++)
    {
        if (!EeFilter_Add(dst, &src->rules[i]))
        {
            EeFilter_Clear(dst);
            return FALSE;
        }
    }
    return TRUE;
}

BOOL EeFilter_HasEnabled(const EeFilterSet *set)
{
    uint32_t i;
    if (set == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < set->count; i++)
    {
        if (set->rules[i].enabled)
        {
            return TRUE;
        }
    }
    return FALSE;
}

const wchar_t *EeFilter_RelationText(EeFilterRelation rel)
{
    switch (rel)
    {
        case EeRel_Is:
            return L"is";
        case EeRel_IsNot:
            return L"is not";
        case EeRel_LessThan:
            return L"less than";
        case EeRel_MoreThan:
            return L"more than";
        case EeRel_BeginsWith:
            return L"begins with";
        case EeRel_EndsWith:
            return L"ends with";
        case EeRel_Contains:
            return L"contains";
        case EeRel_Excludes:
            return L"excludes";
        default:
            return L"is";
    }
}

const wchar_t *EeFilter_ActionText(EeFilterAction action)
{
    return (action == EeFilt_Exclude) ? L"Exclude" : L"Include";
}

static BOOL parse_number(const wchar_t *s, double *out)
{
    wchar_t *end = NULL;
    double v;
    if (s == NULL || s[0] == L'\0')
    {
        return FALSE;
    }
    v = wcstod(s, &end);
    if (end == s)
    {
        return FALSE;
    }
    while (*end == L' ' || *end == L'\t')
    {
        end++;
    }
    if (*end != L'\0')
    {
        return FALSE;
    }
    if (out != NULL)
    {
        *out = v;
    }
    return TRUE;
}

static BOOL parse_date_ymd(const wchar_t *s, uint32_t *out_ymd)
{
    unsigned y = 0;
    unsigned m = 0;
    unsigned d = 0;
    const wchar_t *p = s;
    int n = 0;

    if (s == NULL || s[0] == L'\0')
    {
        return FALSE;
    }
    while (*p == L' ' || *p == L'\t')
    {
        p++;
    }
    /* YYYYMMDD */
    if (wcslen(p) >= 8)
    {
        int i;
        BOOL digits = TRUE;
        for (i = 0; i < 8; i++)
        {
            if (p[i] < L'0' || p[i] > L'9')
            {
                digits = FALSE;
                break;
            }
        }
        if (digits && (p[8] == L'\0' || p[8] == L' '))
        {
            y = (unsigned)((p[0] - L'0') * 1000 + (p[1] - L'0') * 100 + (p[2] - L'0') * 10 +
                           (p[3] - L'0'));
            m = (unsigned)((p[4] - L'0') * 10 + (p[5] - L'0'));
            d = (unsigned)((p[6] - L'0') * 10 + (p[7] - L'0'));
            if (m >= 1 && m <= 12 && d >= 1 && d <= 31)
            {
                if (out_ymd)
                {
                    *out_ymd = y * 10000u + m * 100u + d;
                }
                return TRUE;
            }
        }
    }
    if (swscanf_s(p, L"%u-%u-%u%n", &y, &m, &d, &n) == 3 && n > 0 && m >= 1 && m <= 12 && d >= 1 &&
        d <= 31)
    {
        if (out_ymd)
        {
            *out_ymd = y * 10000u + m * 100u + d;
        }
        return TRUE;
    }
    if (swscanf_s(p, L"%u/%u/%u%n", &m, &d, &y, &n) == 3 && n > 0 && m >= 1 && m <= 12 && d >= 1 &&
        d <= 31)
    {
        if (y < 100)
        {
            y += 2000;
        }
        if (out_ymd)
        {
            *out_ymd = y * 10000u + m * 100u + d;
        }
        return TRUE;
    }
    return FALSE;
}

static void cell_to_wide(const char *utf8, wchar_t *buf, size_t cch)
{
    buf[0] = L'\0';
    if (utf8 == NULL || cch == 0)
    {
        return;
    }
    if (utf8[0] == '\0')
    {
        return;
    }
    if (MultiByteToWideChar(CP_UTF8, 0, utf8, -1, buf, (int)cch) == 0)
    {
        MultiByteToWideChar(CP_ACP, 0, utf8, -1, buf, (int)cch);
    }
}

static BOOL rule_matches_cell(const EeFilterRule *rule, const wchar_t *cell)
{
    wchar_t a[EE_FILTER_VALUE_CCH];
    wchar_t b[EE_FILTER_VALUE_CCH];
    double na;
    double nb;
    uint32_t da;
    uint32_t db;
    size_t alen;
    size_t blen;

    if (rule == NULL || cell == NULL)
    {
        return FALSE;
    }
    StringCchCopyW(a, ARRAYSIZE(a), cell);
    StringCchCopyW(b, ARRAYSIZE(b), rule->value);
    CharLowerBuffW(a, (DWORD)wcslen(a));
    CharLowerBuffW(b, (DWORD)wcslen(b));
    alen = wcslen(a);
    blen = wcslen(b);

    switch (rule->relation)
    {
        case EeRel_Is:
            return CompareStringOrdinal(a, -1, b, -1, TRUE) == CSTR_EQUAL;
        case EeRel_IsNot:
            return CompareStringOrdinal(a, -1, b, -1, TRUE) != CSTR_EQUAL;
        case EeRel_BeginsWith:
            return blen <= alen && wcsncmp(a, b, blen) == 0;
        case EeRel_EndsWith:
            return blen <= alen && wcscmp(a + (alen - blen), b) == 0;
        case EeRel_Contains:
            return wcsstr(a, b) != NULL;
        case EeRel_Excludes:
            return wcsstr(a, b) == NULL;
        case EeRel_LessThan:
            if (parse_date_ymd(cell, &da) && parse_date_ymd(rule->value, &db))
            {
                return da < db;
            }
            if (parse_number(cell, &na) && parse_number(rule->value, &nb))
            {
                return na < nb;
            }
            return CompareStringOrdinal(a, -1, b, -1, TRUE) == CSTR_LESS_THAN;
        case EeRel_MoreThan:
            if (parse_date_ymd(cell, &da) && parse_date_ymd(rule->value, &db))
            {
                return da > db;
            }
            if (parse_number(cell, &na) && parse_number(rule->value, &nb))
            {
                return na > nb;
            }
            return CompareStringOrdinal(a, -1, b, -1, TRUE) == CSTR_GREATER_THAN;
        default:
            return FALSE;
    }
}

static BOOL rule_matches_row(const EeFilterRule *rule, const EeVoterTable *table, uint32_t view_row)
{
    wchar_t cell[EE_FILTER_VALUE_CCH];
    const char *utf8;

    if (rule == NULL || table == NULL || rule->column >= table->column_count)
    {
        return FALSE;
    }
    utf8 = EeVoterTable_GetViewCellUtf8(table, view_row, rule->column);
    cell_to_wide(utf8, cell, ARRAYSIZE(cell));
    return rule_matches_cell(rule, cell);
}

BOOL EeFilter_AcceptsViewRow(const EeFilterSet *set, const EeVoterTable *table, uint32_t view_row)
{
    uint32_t i;
    uint32_t j;
    BOOL any_include = FALSE;

    if (set == NULL || table == NULL)
    {
        return TRUE;
    }

    for (i = 0; i < set->count; i++)
    {
        if (set->rules[i].enabled && set->rules[i].action == EeFilt_Exclude &&
            rule_matches_row(&set->rules[i], table, view_row))
        {
            return FALSE;
        }
        if (set->rules[i].enabled && set->rules[i].action == EeFilt_Include)
        {
            any_include = TRUE;
        }
    }

    if (!any_include)
    {
        return TRUE;
    }

    for (i = 0; i < set->count; i++)
    {
        uint32_t col;
        BOOL matched;
        if (!set->rules[i].enabled || set->rules[i].action != EeFilt_Include)
        {
            continue;
        }
        col = set->rules[i].column;
        /* Skip if this include-column was already evaluated. */
        {
            BOOL seen = FALSE;
            for (j = 0; j < i; j++)
            {
                if (set->rules[j].enabled && set->rules[j].action == EeFilt_Include &&
                    set->rules[j].column == col)
                {
                    seen = TRUE;
                    break;
                }
            }
            if (seen)
            {
                continue;
            }
        }
        matched = FALSE;
        for (j = i; j < set->count; j++)
        {
            if (set->rules[j].enabled && set->rules[j].action == EeFilt_Include &&
                set->rules[j].column == col && rule_matches_row(&set->rules[j], table, view_row))
            {
                matched = TRUE;
                break;
            }
        }
        if (!matched)
        {
            return FALSE;
        }
    }
    return TRUE;
}

BOOL EeFilter_BuildMap(const EeFilterSet *set,
                       const EeVoterTable *table,
                       uint32_t **out_map,
                       uint32_t *out_count)
{
    uint32_t i;
    uint32_t n = 0;
    uint32_t *map = NULL;

    if (out_map == NULL || out_count == NULL || table == NULL)
    {
        return FALSE;
    }
    *out_map = NULL;
    *out_count = 0;

    if (!EeFilter_HasEnabled(set) || table->row_count == 0)
    {
        *out_count = table->row_count;
        return TRUE;
    }

    map = (uint32_t *)malloc((size_t)table->row_count * sizeof(uint32_t));
    if (map == NULL)
    {
        return FALSE;
    }
    for (i = 0; i < table->row_count; i++)
    {
        if (EeFilter_AcceptsViewRow(set, table, i))
        {
            map[n++] = i;
        }
    }
    if (n == 0)
    {
        free(map);
        *out_map = NULL;
        *out_count = 0;
        return TRUE;
    }
    if (n < table->row_count)
    {
        uint32_t *shrunk = (uint32_t *)realloc(map, (size_t)n * sizeof(uint32_t));
        if (shrunk != NULL)
        {
            map = shrunk;
        }
    }
    *out_map = map;
    *out_count = n;
    return TRUE;
}

static unsigned hash_ci_utf8(const char *s)
{
    unsigned h = 2166136261u;
    if (s == NULL)
    {
        s = "";
    }
    while (*s != '\0')
    {
        unsigned char c = (unsigned char)*s++;
        if (c >= 'A' && c <= 'Z')
        {
            c = (unsigned char)(c + ('a' - 'A'));
        }
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

static uint32_t distinct_bucket_count(uint32_t max_values)
{
    ULONG n = 0;
    if (FAILED(ULongMult((ULONG)max_values, 2u, &n)))
    {
        return 0;
    }
    if (n < 16u)
    {
        n = 16u;
    }
    /* Next power of two so probes can mask instead of divide. */
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n++;
    return (uint32_t)n;
}

static int distinct_wide_cmp(void *ctx, const void *a, const void *b)
{
    const wchar_t *sa = *(const wchar_t *const *)a;
    const wchar_t *sb = *(const wchar_t *const *)b;
    (void)ctx;
    if (sa == NULL)
    {
        sa = L"";
    }
    if (sb == NULL)
    {
        sb = L"";
    }
    return _wcsicmp(sa, sb);
}

static void distinct_free_vals(wchar_t **vals, uint32_t count)
{
    uint32_t i;
    if (vals == NULL)
    {
        return;
    }
    for (i = 0; i < count; i++)
    {
        free(vals[i]);
    }
    free(vals);
}

BOOL EeFilter_CollectDistinct(const EeVoterTable *table,
                              uint32_t column,
                              uint32_t max_values,
                              wchar_t ***out_values,
                              uint32_t *out_count)
{
    const char **slots = NULL;
    wchar_t **vals = NULL;
    uint32_t cap;
    uint32_t mask;
    uint32_t i;
    uint32_t unique = 0;

    if (out_values == NULL || out_count == NULL || table == NULL)
    {
        return FALSE;
    }
    *out_values = NULL;
    *out_count = 0;
    if (column >= table->column_count || table->row_count == 0 || max_values == 0)
    {
        return TRUE;
    }

    cap = distinct_bucket_count(max_values);
    if (cap == 0)
    {
        return FALSE;
    }
    mask = cap - 1u;
    slots = (const char **)calloc((size_t)cap, sizeof(char *));
    if (slots == NULL)
    {
        return FALSE;
    }

    for (i = 0; i < table->row_count && unique < max_values; i++)
    {
        const char *cur = EeVoterTable_GetViewCellUtf8(table, i, column);
        uint32_t b;
        if (cur == NULL)
        {
            cur = "";
        }
        b = hash_ci_utf8(cur) & mask;
        for (;;)
        {
            if (slots[b] == NULL)
            {
                slots[b] = cur;
                unique++;
                break;
            }
            if (_stricmp(slots[b], cur) == 0)
            {
                break;
            }
            b = (b + 1u) & mask;
        }
    }

    if (unique == 0)
    {
        free(slots);
        return TRUE;
    }

    vals = (wchar_t **)calloc((size_t)unique, sizeof(wchar_t *));
    if (vals == NULL)
    {
        free(slots);
        return FALSE;
    }
    unique = 0;
    for (i = 0; i < cap; i++)
    {
        wchar_t tmp[EE_FILTER_VALUE_CCH];
        size_t cch;
        if (slots[i] == NULL)
        {
            continue;
        }
        cell_to_wide(slots[i], tmp, ARRAYSIZE(tmp));
        cch = wcslen(tmp) + 1;
        vals[unique] = (wchar_t *)malloc(cch * sizeof(wchar_t));
        if (vals[unique] == NULL)
        {
            distinct_free_vals(vals, unique);
            free(slots);
            return FALSE;
        }
        StringCchCopyW(vals[unique], cch, tmp);
        unique++;
    }
    free(slots);
    qsort_s((void *)vals, unique, sizeof(wchar_t *), distinct_wide_cmp, NULL);
    *out_values = vals;
    *out_count = unique;
    return TRUE;
}
