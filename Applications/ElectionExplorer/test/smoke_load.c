/**
 * @file smoke_load.c
 * @brief Console smoke test for EeVoterTable_LoadFromFile.
 */

#include "filter.h"
#include "voter_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strsafe.h>
#include <windows.h>

/** Travis-style history exports currently have ~389 source columns. */
static const uint32_t k_WideSourceColumns = 400;

static int load_sample(const wchar_t *path, const wchar_t *label)
{
    EeVoterTable t;
    wchar_t err[256];
    EeLoadStatus s;
    uint32_t i;

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    wprintf(L"%s status=%d rows=%u cols=%u err=%s\n",
            label,
            (int)s,
            t.row_count,
            t.column_count,
            err);
    if (s == EeLoadStatus_Ok && t.row_count > 0)
    {
        wchar_t buf[128];
        for (i = 0; i < t.column_count && i < 6; i++)
        {
            wprintf(L"  col%u: %s\n", i, t.column_titles[i]);
        }
        EeVoterTable_GetViewCellW(&t, 0, 0, buf, ARRAYSIZE(buf));
        wprintf(L"  row0 VoterID=%s\n", buf);
        EeVoterTable_GetViewCellW(&t, 0, 1, buf, ARRAYSIZE(buf));
        wprintf(L"  row0 Name=%s\n", buf);
    }
    EeVoterTable_Clear(&t);
    return (s == EeLoadStatus_Ok) ? 0 : 1;
}

static int load_wide_history(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    uint32_t i;
    DWORD n;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path))
    {
        wprintf(L"wide: GetTempPathW failed\n");
        return 1;
    }
    if (FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_wide_voters.csv")))
    {
        wprintf(L"wide: path too long\n");
        return 1;
    }

    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"wide: could not create %s\n", path);
        return 1;
    }

    fputs("VUIDNO,LSTNAM,FSTNAM", fp);
    for (i = 3; i < k_WideSourceColumns; i++)
    {
        fprintf(fp, ",H%u", i);
    }
    fputs("\n100001,Smith,John", fp);
    for (i = 3; i < k_WideSourceColumns; i++)
    {
        fputs(",Y", fp);
    }
    fputs("\n", fp);
    fclose(fp);
    fp = NULL;

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    wprintf(L"wide status=%d rows=%u cols=%u err=%s\n", (int)s, t.row_count, t.column_count, err);
    if (s == EeLoadStatus_Ok && t.row_count == 1 &&
        t.column_count == k_WideSourceColumns + EE_FROZEN_COLUMN_COUNT)
    {
        wchar_t buf[128];
        EeVoterTable_GetViewCellW(&t, 0, 0, buf, ARRAYSIZE(buf));
        if (wcscmp(buf, L"100001") == 0)
        {
            EeVoterTable_GetViewCellW(&t, 0, 1, buf, ARRAYSIZE(buf));
            if (wcscmp(buf, L"Smith, John") == 0)
            {
                rc = 0;
            }
        }
    }
    EeVoterTable_Clear(&t);
    DeleteFileW(path);
    return rc;
}

static int test_copy_format(void)
{
    EeVoterTable t;
    wchar_t err[256];
    char *text = NULL;
    uint32_t rows[2];
    int rc = 1;

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    if (EeVoterTable_LoadFromFile(L"test\\sample_voters.csv",
                                  &t,
                                  NULL,
                                  NULL,
                                  NULL,
                                  err,
                                  ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"copy: csv load failed %s\n", err);
        goto done;
    }
    if (t.delimiter != ',')
    {
        wprintf(L"copy: expected comma delimiter\n");
        goto done;
    }
    rows[0] = 0;
    if (!EeVoterTable_FormatCopyUtf8(&t, rows, 1, FALSE, &text, NULL) || text == NULL)
    {
        wprintf(L"copy: raw format failed\n");
        goto done;
    }
    if (strncmp(text, "100001,101,", 11) != 0)
    {
        wprintf(L"copy: raw prefix mismatch\n");
        goto done;
    }
    free(text);
    text = NULL;
    if (!EeVoterTable_FormatCopyUtf8(&t, rows, 1, TRUE, &text, NULL) || text == NULL)
    {
        wprintf(L"copy: prepend format failed\n");
        goto done;
    }
    if (strncmp(text, "100001,\"Smith, John A\",\"123 Main ST, Austin, 78701\",100001,", 59) != 0)
    {
        wprintf(L"copy: prepend prefix mismatch\n");
        goto done;
    }
    free(text);
    text = NULL;
    EeVoterTable_Clear(&t);

    if (EeVoterTable_LoadFromFile(L"test\\sample_voters.txt",
                                  &t,
                                  NULL,
                                  NULL,
                                  NULL,
                                  err,
                                  ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"copy: txt load failed %s\n", err);
        goto done;
    }
    if (t.delimiter != '\t')
    {
        wprintf(L"copy: expected tab delimiter\n");
        goto done;
    }
    rows[0] = 0;
    rows[1] = 1;
    if (!EeVoterTable_FormatCopyUtf8(&t, rows, 2, FALSE, &text, NULL) || text == NULL)
    {
        wprintf(L"copy: tab format failed\n");
        goto done;
    }
    if (strncmp(text, "200001\tC-1\t", 11) != 0)
    {
        wprintf(L"copy: tab prefix mismatch\n");
        goto done;
    }
    if (strstr(text, "\r\n200002\t") == NULL)
    {
        wprintf(L"copy: missing second tab row\n");
        goto done;
    }
    free(text);
    text = NULL;
    EeVoterTable_Clear(&t);

    if (EeVoterTable_LoadFromFile(L"test\\sample_voters.csv",
                                  &t,
                                  NULL,
                                  NULL,
                                  NULL,
                                  err,
                                  ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"copy: csv reload failed\n");
        goto done;
    }
    {
        wchar_t buf[128];
        EeVoterTable_GetViewCellW(&t, 0, 1, buf, ARRAYSIZE(buf));
        if (wcscmp(buf, L"Smith, John A") != 0)
        {
            wprintf(L"copy: default surname-first mismatch (%s)\n", buf);
            goto done;
        }
        EeVoterTable_GetViewCellW(&t, 0, 2, buf, ARRAYSIZE(buf));
        if (wcscmp(buf, L"123 Main ST, Austin, 78701") != 0)
        {
            wprintf(L"copy: address mismatch (%s)\n", buf);
            goto done;
        }
        if (!EeVoterTable_SetNameSurnameFirst(&t, FALSE, NULL, NULL))
        {
            wprintf(L"copy: set given-first failed\n");
            goto done;
        }
        EeVoterTable_GetViewCellW(&t, 0, 1, buf, ARRAYSIZE(buf));
        if (wcscmp(buf, L"John A Smith") != 0)
        {
            wprintf(L"copy: given-first mismatch (%s)\n", buf);
            goto done;
        }
        if (!EeVoterTable_SetNameSurnameFirst(&t, TRUE, NULL, NULL))
        {
            wprintf(L"copy: restore surname-first failed\n");
            goto done;
        }
        EeVoterTable_GetViewCellW(&t, 0, 1, buf, ARRAYSIZE(buf));
        if (wcscmp(buf, L"Smith, John A") != 0)
        {
            wprintf(L"copy: restored surname-first mismatch (%s)\n", buf);
            goto done;
        }
    }

    rc = 0;
    wprintf(L"copy format ok\n");

done:
    free(text);
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"copy format test failed\n");
    }
    return rc;
}

static int test_zip4_omits_zeros(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    wchar_t buf[128];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_zip4_voters.csv")))
    {
        wprintf(L"zip4: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"zip4: could not create %s\n", path);
        return 1;
    }
    fputs("VUIDNO,LSTNAM,FSTNAM,BLKNUM,STRNAM,STRTYP,RSCITY,RZIPCD,RZIP+4\n", fp);
    fputs("1,Smith,John,123,Main,ST,Austin,78701,0000\n", fp);
    fputs("2,Jones,Jane,456,Oak,AVE,Austin,78702,1234\n", fp);
    fputs("3,Lee,Ann,789,Pine,RD,Austin,78703,\n", fp);
    fputs("4,Ng,Tom,10,Elm,CT,Austin,787010000,\n", fp);
    fputs("5,Ortiz,Ana,20,Ash,LN,Austin,78701-0000,\n", fp);
    fputs("6,Park,Kim,30,Bay,DR,Austin,787011111,\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 6)
    {
        wprintf(L"zip4: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }

    EeVoterTable_GetViewCellW(&t, 0, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"123 Main ST, Austin, 78701") != 0)
    {
        wprintf(L"zip4: zero +4 field mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"456 Oak AVE, Austin, 78702-1234") != 0)
    {
        wprintf(L"zip4: real +4 field mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"789 Pine RD, Austin, 78703") != 0)
    {
        wprintf(L"zip4: missing +4 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 3, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"10 Elm CT, Austin, 78701") != 0)
    {
        wprintf(L"zip4: combined 0000 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 4, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"20 Ash LN, Austin, 78701") != 0)
    {
        wprintf(L"zip4: hyphen 0000 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 5, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"30 Bay DR, Austin, 78701-1111") != 0)
    {
        wprintf(L"zip4: combined 1111 mismatch (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"zip4 ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"zip4 test failed\n");
    }
    return rc;
}

static int test_res_addr_fields(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    wchar_t buf[160];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_res_addr.csv")))
    {
        wprintf(L"resaddr: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"resaddr: could not create %s\n", path);
        return 1;
    }
    fputs("COUNTY_CODE,LAST_NAME,FIRST_NAME,MIDDLE_NAME,VUID,RES_ADDR,RESIDENT_CITY,"
          "RESIDENT_ZIP_CODE,MAIL_ADRS_1,MAIL_CITY,MAIL_POSTAL_CODE\n",
          fp);
    fputs("227,Smith,John,A,100001,123 Main St,Austin,78701,PO Box 9,Dallas,75201\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 1)
    {
        wprintf(L"resaddr: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 0, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"123 Main St, Austin, 78701") != 0)
    {
        wprintf(L"resaddr: mismatch (%s)\n", buf);
        EeVoterTable_Clear(&t);
        return 1;
    }
    rc = 0;
    wprintf(L"resaddr ok\n");
    EeVoterTable_Clear(&t);
    return rc;
}

static int test_res_addr_no_duplicate_city_state_zip(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    wchar_t buf[200];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_res_dup.csv")))
    {
        wprintf(L"resdup: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"resdup: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,NAME,Residential Address,City,State,Zip Code 5\n", fp);
    fputs("100001,Smith John,1109 N IH 35  NB AUSTIN TX 78702,AUSTIN,TX,78702\n", fp);
    fputs("100002,Jones Jane,123 Main St,Austin,TX,78701\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 2)
    {
        wprintf(L"resdup: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 0, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"1109 N IH 35  NB AUSTIN TX 78702") != 0)
    {
        wprintf(L"resdup: duplicate city/state/zip (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"123 Main St, Austin, TX 78701") != 0)
    {
        wprintf(L"resdup: street-only append mismatch (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"resdup ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"resdup test failed\n");
    }
    return rc;
}

static int find_column(const EeVoterTable *t, const wchar_t *title)
{
    uint32_t i;
    if (t == NULL || title == NULL)
    {
        return -1;
    }
    for (i = 0; i < t->column_count; i++)
    {
        if (t->column_titles[i] != NULL && _wcsicmp(t->column_titles[i], title) == 0)
        {
            return (int)i;
        }
    }
    return -1;
}

static EeFilterRule make_rule(uint32_t column,
                              EeFilterRelation rel,
                              EeFilterAction action,
                              const wchar_t *value,
                              BOOL enabled)
{
    EeFilterRule r;
    ZeroMemory(&r, sizeof(r));
    r.column = column;
    r.relation = rel;
    r.action = action;
    r.enabled = enabled;
    if (value != NULL)
    {
        StringCchCopyW(r.value, ARRAYSIZE(r.value), value);
    }
    return r;
}

static uint32_t count_accepted(const EeFilterSet *set, const EeVoterTable *t)
{
    uint32_t i;
    uint32_t n = 0;
    for (i = 0; i < t->row_count; i++)
    {
        if (EeFilter_AcceptsViewRow(set, t, i))
        {
            n++;
        }
    }
    return n;
}

static int test_filter_logic(void)
{
    EeVoterTable t;
    EeFilterSet set;
    EeFilterRule r;
    wchar_t err[256];
    uint32_t *map = NULL;
    uint32_t map_n = 0;
    int city;
    int pct;
    int gender;
    int edr;
    int rc = 1;

    EeVoterTable_Init(&t);
    EeFilter_Init(&set);
    err[0] = L'\0';
    if (EeVoterTable_LoadFromFile(L"test\\sample_voters.csv",
                                  &t,
                                  NULL,
                                  NULL,
                                  NULL,
                                  err,
                                  ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"filter: csv load failed %s\n", err);
        goto done;
    }
    city = find_column(&t, L"RSCITY");
    pct = find_column(&t, L"PCTCOD");
    gender = find_column(&t, L"GENDER");
    edr = find_column(&t, L"EDRDAT");
    if (city < 0 || pct < 0 || gender < 0 || edr < 0 || t.row_count != 5)
    {
        wprintf(L"filter: unexpected columns/rows city=%d pct=%d gender=%d rows=%u\n",
                city,
                pct,
                gender,
                t.row_count);
        goto done;
    }

    if (EeFilter_HasEnabled(&set) || count_accepted(&set, &t) != 5)
    {
        wprintf(L"filter: empty set should accept every row\n");
        goto done;
    }
    if (!EeFilter_BuildMap(&set, &t, &map, &map_n) || map != NULL || map_n != 5)
    {
        wprintf(L"filter: empty BuildMap should return NULL map\n");
        goto done;
    }

    r = make_rule((uint32_t)city, EeRel_Is, EeFilt_Exclude, L"Austin", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 1)
    {
        wprintf(L"filter: exclude Austin expected 1 row\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule((uint32_t)city, EeRel_Is, EeFilt_Include, L"Austin", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 4)
    {
        wprintf(L"filter: include Austin expected 4 rows\n");
        goto done;
    }

    r = make_rule((uint32_t)city, EeRel_Is, EeFilt_Include, L"Round Rock", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 5)
    {
        wprintf(L"filter: same-column includes should OR\n");
        goto done;
    }

    r = make_rule((uint32_t)pct, EeRel_Is, EeFilt_Include, L"101", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 3)
    {
        wprintf(L"filter: different-column includes should AND (expected 3)\n");
        goto done;
    }

    r = make_rule((uint32_t)gender, EeRel_Is, EeFilt_Exclude, L"M", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 0)
    {
        wprintf(L"filter: exclude matching include group should hide all\n");
        goto done;
    }

    set.rules[set.count - 1].enabled = FALSE;
    if (count_accepted(&set, &t) != 3)
    {
        wprintf(L"filter: disabled exclude should be ignored\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule((uint32_t)gender, EeRel_IsNot, EeFilt_Include, L"F", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 3)
    {
        wprintf(L"filter: is not F expected 3 rows\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule(2, EeRel_Contains, EeFilt_Include, L"Oak", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 1)
    {
        wprintf(L"filter: contains Oak expected 1 row\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule(1, EeRel_BeginsWith, EeFilt_Include, L"Smith", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 1)
    {
        wprintf(L"filter: begins with Smith expected 1 row\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule((uint32_t)pct, EeRel_LessThan, EeFilt_Include, L"102", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 3)
    {
        wprintf(L"filter: PCTCOD less than 102 expected 3 rows\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule((uint32_t)pct, EeRel_MoreThan, EeFilt_Include, L"102", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 1)
    {
        wprintf(L"filter: PCTCOD more than 102 expected 1 row\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule((uint32_t)edr, EeRel_LessThan, EeFilt_Include, L"abc", TRUE);
    if (EeFilter_RuleIsValid(&r, &t))
    {
        wprintf(L"filter: date less-than should reject non-date value\n");
        goto done;
    }
    r = make_rule((uint32_t)edr, EeRel_LessThan, EeFilt_Include, L"20200101", TRUE);
    if (!EeFilter_RuleIsValid(&r, &t) || !EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 2)
    {
        wprintf(L"filter: EDRDAT less than 20200101 expected 2 rows\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule((uint32_t)edr, EeRel_MoreThan, EeFilt_Include, L"1/1/2020", TRUE);
    if (!EeFilter_RuleIsValid(&r, &t) || !EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 3)
    {
        wprintf(L"filter: EDRDAT more than 1/1/2020 expected 3 rows\n");
        goto done;
    }

    {
        wchar_t **vals = NULL;
        uint32_t n = 0;
        if (!EeFilter_CollectDistinct(&t, (uint32_t)city, EE_FILTER_MAX_DISTINCT, &vals, &n) ||
            n != 2)
        {
            wprintf(L"filter: CollectDistinct RSCITY expected 2 values, got %u\n", n);
            goto done;
        }
        if (_wcsicmp(vals[0], L"Austin") != 0 || _wcsicmp(vals[1], L"Round Rock") != 0)
        {
            wprintf(L"filter: CollectDistinct order/values mismatch\n");
            goto done;
        }
        {
            uint32_t i;
            for (i = 0; i < n; i++)
            {
                free(vals[i]);
            }
        }
        free(vals);
    }

    EeFilter_Clear(&set);
    r = make_rule((uint32_t)city, EeRel_Is, EeFilt_Exclude, L"Austin", TRUE);
    if (!EeFilter_Add(&set, &r) || !EeFilter_BuildMap(&set, &t, &map, &map_n) || map == NULL ||
        map_n != 1)
    {
        wprintf(L"filter: BuildMap exclude Austin expected 1 row\n");
        goto done;
    }
    free(map);
    map = NULL;

    if (!EeVoterTable_ColumnIsNumericOrDate(&t, 0) || EeVoterTable_ColumnIsNumericOrDate(&t, 1) ||
        EeVoterTable_ColumnIsNumericOrDate(&t, 2) ||
        !EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)pct) ||
        EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)city) ||
        EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)gender))
    {
        wprintf(L"filter: column kind mismatch id=%d name=%d addr=%d pct=%d city=%d gender=%d\n",
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, 0),
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, 1),
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, 2),
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)pct),
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)city),
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)gender));
        goto done;
    }
    {
        int split = find_column(&t, L"PCTSPT");
        int dob = find_column(&t, L"EDRDAT");
        if (split < 0 || EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)split) || dob < 0 ||
            !EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)dob))
        {
            wprintf(L"filter: PCTSPT/EDRDAT kind mismatch\n");
            goto done;
        }
    }

    rc = 0;
    wprintf(L"filter ok\n");

done:
    free(map);
    EeFilter_Clear(&set);
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"filter test failed\n");
    }
    return rc;
}

static int test_empty_numeric_header(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int age;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_empty_age.csv")))
    {
        wprintf(L"agehdr: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"agehdr: could not create %s\n", path);
        return 1;
    }
    fputs("VUIDNO,AGE,LSTNAM\n1,,Smith\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 1)
    {
        wprintf(L"agehdr: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    age = find_column(&t, L"AGE");
    if (age < 0 || !EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)age) ||
        !EeVoterTable_ColumnIsNumericOrDate(&t, 0) || EeVoterTable_ColumnIsNumericOrDate(&t, 1))
    {
        wprintf(L"agehdr: expected empty AGE to be numeric by header\n");
        EeVoterTable_Clear(&t);
        return 1;
    }
    rc = 0;
    wprintf(L"agehdr ok\n");
    EeVoterTable_Clear(&t);
    return rc;
}

static int test_date_sort(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    wchar_t buf[64];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int reg;
    int edr;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_dates.csv")))
    {
        wprintf(L"datesort: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"datesort: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,Registration Date,EDR,Status Date,Candidate\n", fp);
    fputs("1,12/1/2019,20190115,1/15/2020,Smith\n", fp);
    fputs("2,1/2/2020,20200615,12/1/2019,Jones\n", fp);
    fputs("3,3/1/2019,20181201,2/1/2020,Garcia\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 3)
    {
        wprintf(L"datesort: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }

    reg = find_column(&t, L"Registration Date");
    edr = find_column(&t, L"EDR");
    if (reg < 0 || edr < 0 || find_column(&t, L"Candidate") < 0)
    {
        wprintf(L"datesort: missing columns\n");
        goto done;
    }
    if (!t.column_is_date[reg] || !t.column_is_date[edr] ||
        !t.column_is_date[find_column(&t, L"Status Date")] ||
        t.column_is_date[find_column(&t, L"Candidate")])
    {
        wprintf(L"datesort: date-column flags mismatch\n");
        goto done;
    }

    if (!EeVoterTable_SortByColumn(&t, (uint32_t)reg))
    {
        wprintf(L"datesort: sort Registration Date failed\n");
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, (uint32_t)reg, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"3/1/2019") != 0)
    {
        wprintf(L"datesort: expected 3/1/2019 first, got %s\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, (uint32_t)reg, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"12/1/2019") != 0)
    {
        wprintf(L"datesort: expected 12/1/2019 second, got %s\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, (uint32_t)reg, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"1/2/2020") != 0)
    {
        wprintf(L"datesort: expected 1/2/2020 third, got %s\n", buf);
        goto done;
    }

    if (!EeVoterTable_SortByColumn(&t, (uint32_t)edr))
    {
        wprintf(L"datesort: sort EDR failed\n");
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, (uint32_t)edr, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"20181201") != 0)
    {
        wprintf(L"datesort: expected 20181201 first EDR, got %s\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, (uint32_t)edr, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"20200615") != 0)
    {
        wprintf(L"datesort: expected 20200615 last EDR, got %s\n", buf);
        goto done;
    }

    rc = 0;
    wprintf(L"datesort ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"datesort test failed\n");
    }
    return rc;
}

int wmain(void)
{
    int failed = 0;

    failed |= load_sample(L"test\\sample_voters.csv", L"csv");
    failed |= load_sample(L"test\\sample_voters.txt", L"txt");
    failed |= load_wide_history();
    failed |= test_copy_format();
    failed |= test_zip4_omits_zeros();
    failed |= test_res_addr_fields();
    failed |= test_res_addr_no_duplicate_city_state_zip();
    failed |= test_filter_logic();
    failed |= test_empty_numeric_header();
    failed |= test_date_sort();
    return failed == 0 ? 0 : 1;
}
