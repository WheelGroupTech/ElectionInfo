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
        EeVoterTable_GetViewCellW(&t, 0, EE_COL_NAME, buf, ARRAYSIZE(buf));
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
            EeVoterTable_GetViewCellW(&t, 0, EE_COL_NAME, buf, ARRAYSIZE(buf));
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
    if (strncmp(text, "100001,101,\"Smith, John A\",\"123 Main ST, Austin, 78701\",100001,", 63) !=
        0)
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
        EeVoterTable_GetViewCellW(&t, 0, EE_COL_NAME, buf, ARRAYSIZE(buf));
        if (wcscmp(buf, L"Smith, John A") != 0)
        {
            wprintf(L"copy: default surname-first mismatch (%s)\n", buf);
            goto done;
        }
        EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
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
        EeVoterTable_GetViewCellW(&t, 0, EE_COL_NAME, buf, ARRAYSIZE(buf));
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
        EeVoterTable_GetViewCellW(&t, 0, EE_COL_NAME, buf, ARRAYSIZE(buf));
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

    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"123 Main ST, Austin, 78701") != 0)
    {
        wprintf(L"zip4: zero +4 field mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"456 Oak AVE, Austin, 78702-1234") != 0)
    {
        wprintf(L"zip4: real +4 field mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"789 Pine RD, Austin, 78703") != 0)
    {
        wprintf(L"zip4: missing +4 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 3, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"10 Elm CT, Austin, 78701") != 0)
    {
        wprintf(L"zip4: combined 0000 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 4, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"20 Ash LN, Austin, 78701") != 0)
    {
        wprintf(L"zip4: hyphen 0000 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 5, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
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
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
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
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"1109 N IH 35  NB AUSTIN TX 78702") != 0)
    {
        wprintf(L"resdup: duplicate city/state/zip (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
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

static int test_res_addr_zip_dash_and_unit(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    wchar_t buf[220];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_res_zipdash.csv")))
    {
        wprintf(L"zipdash: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"zipdash: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,NAME,Residential Address,Street Number 1,Street Name 1,Unit,Unit Type,"
          "City,State,Zip Code 5,Zip Code 4\n",
          fp);
    fputs("2163340117,\"MULRY, CAILIN LORAINE\",3001 MEDICAL ARTS ST AUSTIN TX 78705 -,3001,"
          "MEDICAL ARTS ST,116,APT,AUSTIN,TX,78705,\n",
          fp);
    fputs("2149934808,\"NDEDA, SHANE MARCUS AGANYO\",3400 HARMON AVE AUSTIN TX 78705 -2119,3400,"
          "HARMON AVE,367,APT,AUSTIN,TX,78705,2119\n",
          fp);
    fputs("3382566260,\"NEWHOUSE, MARIE ELIZABETH\",3502 RED RIVER ST AUSTIN TX 78705 -,3502,"
          "RED RIVER ST,,,AUSTIN,TX,78705,\n",
          fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 3)
    {
        wprintf(L"zipdash: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }

    EeVoterTable_GetViewCellW(&t, 0, 0, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"2163340117") != 0)
    {
        wprintf(L"zipdash: VUID mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_NAME, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"MULRY, CAILIN LORAINE") != 0)
    {
        wprintf(L"zipdash: NAME mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"3001 MEDICAL ARTS ST AUSTIN TX 78705") != 0)
    {
        wprintf(L"zipdash: empty +4 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"3400 HARMON AVE AUSTIN TX 78705-2119") != 0)
    {
        wprintf(L"zipdash: zip+4 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"3502 RED RIVER ST AUSTIN TX 78705") != 0)
    {
        wprintf(L"zipdash: no-unit empty +4 mismatch (%s)\n", buf);
        goto done;
    }
    {
        uint32_t row;
        wchar_t norm[220];
        wchar_t full[220];
        for (row = 0; row < t.row_count; row++)
        {
            EeVoterTable_GetViewCellW(&t, row, EE_COL_ADDRESS, norm, ARRAYSIZE(norm));
            EeVoterTable_GetViewCellW(&t, row, 6, full, ARRAYSIZE(full));
            if (!EeVoterTable_NormalizedMatchesFullAddress(norm, full))
            {
                wprintf(L"zipdash: row %u normalized '%s' != full '%s'\n", row, norm, full);
                goto done;
            }
        }
    }
    rc = 0;
    wprintf(L"zipdash ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"zipdash test failed\n");
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

static int test_house_number_dot_zero(void)
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
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_blk_dot.csv")))
    {
        wprintf(L"blkdot: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"blkdot: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM,BLKNUM,STRNAM,STRTYP,RSCITY,RZIPCD\n", fp);
    fputs("1,Smith,John,6007.0,SUN VISTA,DR,Austin,78749\n", fp);
    fputs("2,Jones,Jane,12,OAK,ST,Austin,78701\n", fp);
    fputs("3,Lee,Ann,100.50,PINE,RD,Austin,78702\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 3)
    {
        wprintf(L"blkdot: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"6007 SUN VISTA DR, Austin, 78749") != 0)
    {
        wprintf(L"blkdot: .0 house number mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"12 OAK ST, Austin, 78701") != 0)
    {
        wprintf(L"blkdot: plain house number mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"100.50 PINE RD, Austin, 78702") != 0)
    {
        wprintf(L"blkdot: non-zero fraction should remain (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"blkdot ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"blkdot test failed\n");
    }
    return rc;
}

static int test_lot_unit_ignored(void)
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
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_lot_unit.csv")))
    {
        wprintf(L"lotunit: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"lotunit: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM,BLKNUM,STRNAM,STRTYP,UNITYP,UNITNO,RSCITY,RZIPCD\n", fp);
    fputs("1,Smith,John,12,Oak,ST,LOT,4,Austin,78701\n", fp);
    fputs("2,Jones,Jane,90,Pine,RD,APT,2,Austin,78702\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 2)
    {
        wprintf(L"lotunit: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"12 Oak ST, Austin, 78701") != 0)
    {
        wprintf(L"lotunit: LOT should be omitted (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"90 Pine RD APT 2, Austin, 78702") != 0)
    {
        wprintf(L"lotunit: APT should remain (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"lotunit ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"lotunit test failed\n");
    }
    return rc;
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
    r = make_rule(EE_COL_ADDRESS, EeRel_Contains, EeFilt_Include, L"Oak", TRUE);
    if (!EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 1)
    {
        wprintf(L"filter: contains Oak expected 1 row\n");
        goto done;
    }

    EeFilter_Clear(&set);
    r = make_rule(EE_COL_NAME, EeRel_BeginsWith, EeFilt_Include, L"Smith", TRUE);
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

    if (!EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_VOTER_ID) ||
        !EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_PRECINCT) ||
        EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_NAME) ||
        EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_ADDRESS) ||
        !EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)pct) ||
        EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)city) ||
        EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)gender))
    {
        wprintf(L"filter: column kind mismatch id=%d pct=%d name=%d addr=%d srcpct=%d city=%d "
                L"gender=%d\n",
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_VOTER_ID),
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_PRECINCT),
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_NAME),
                (int)EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_ADDRESS),
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
        !EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_VOTER_ID) ||
        EeVoterTable_ColumnIsNumericOrDate(&t, EE_COL_NAME))
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

static int test_name_last_first_no_address(void)
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
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_name_parts.csv")))
    {
        wprintf(L"nameparts: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"nameparts: could not create %s\n", path);
        return 1;
    }
    fputs("ID_County_VR,Reg_Precinct,VID,VUID,Name_Last,Name_First,Name_Middle,Name_Suffix,"
          "Date_Last_Voted,Date_Last_Election,Birth_Month,Birth_Day,Birth_Year,"
          "Birth_Calculated_Age,Reg_Date,Date_Last_Contact,Date_Last_Modfied,Reg_Status,"
          "section,finding,DOD,SubmissionURL,created_at,created_by,id\n",
          fp);
    fputs("11,101,99,100001,Smith,John,A,Jr,1/1/2020,11/5/2019,3,15,1970,54,1/2/2018,"
          "2/2/2024,3/3/2024,Active,A,ok,,https://example.com/x,2024-01-01,admin,7\n",
          fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 1)
    {
        wprintf(L"nameparts: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 0, 0, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"100001") != 0)
    {
        wprintf(L"nameparts: VUID mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_NAME, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Smith, John A Jr") != 0)
    {
        wprintf(L"nameparts: Name mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_PRECINCT, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"101") != 0)
    {
        wprintf(L"nameparts: Precinct mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (buf[0] != L'\0')
    {
        wprintf(L"nameparts: expected empty Address, got (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"nameparts ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"nameparts test failed\n");
    }
    return rc;
}

static int test_precinct_normalize(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    wchar_t buf[64];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_precinct.csv")))
    {
        wprintf(L"pct: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"pct: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,PCTCOD,PCTSPT,LSTNAM,FSTNAM\n", fp);
    fputs("1,234,A,Smith,John\n", fp);
    fputs("2,P 204,B,Jones,Jane\n", fp);
    fputs("3,425.6,C,Lee,Ann\n", fp);
    fputs("4,1006.10,D,Ng,Tom\n", fp);
    fputs("5,2.3,E,Park,Kim\n", fp);
    fputs("6,234 S,F,Ortiz,Ana\n", fp);
    fputs("7,S2,G,Brown,Rob\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 7)
    {
        wprintf(L"pct: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }

    {
        static const wchar_t *expect[] = {L"234", L"204", L"425", L"1006", L"2", L"234", L"2"};
        uint32_t i;
        for (i = 0; i < t.row_count; i++)
        {
            EeVoterTable_GetViewCellW(&t, i, EE_COL_PRECINCT, buf, ARRAYSIZE(buf));
            if (wcscmp(buf, expect[i]) != 0)
            {
                wprintf(L"pct: row %u expected %s got %s\n", i, expect[i], buf);
                goto done;
            }
        }
    }

    if (!EeVoterTable_SortByColumn(&t, EE_COL_PRECINCT))
    {
        wprintf(L"pct: sort failed\n");
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_PRECINCT, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"2") != 0)
    {
        wprintf(L"pct: expected 2 first after sort, got %s\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 6, EE_COL_PRECINCT, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"1006") != 0)
    {
        wprintf(L"pct: expected 1006 last after sort, got %s\n", buf);
        goto done;
    }

    rc = 0;
    wprintf(L"pct ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"pct test failed\n");
    }
    return rc;
}

static int test_duplicate_voter_ids(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    wchar_t **ids = NULL;
    uint32_t n_ids = 0;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_dup_vuid.csv")))
    {
        wprintf(L"dupvuid: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"dupvuid: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM\n", fp);
    fputs("100,Smith,John\n", fp);
    fputs("200,Jones,Jane\n", fp);
    fputs("100,Smith,Jon\n", fp);
    fputs("300,Lee,Ann\n", fp);
    fputs("200,Jones,Janet\n", fp);
    fputs("200,Jones,Jan\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 6)
    {
        wprintf(L"dupvuid: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    if (!EeVoterTable_CollectDuplicateVoterIds(&t, &ids, &n_ids) || n_ids != 2 || ids == NULL)
    {
        wprintf(L"dupvuid: expected 2 duplicate IDs, got %u\n", n_ids);
        goto done;
    }
    if (!((wcscmp(ids[0], L"100") == 0 && wcscmp(ids[1], L"200") == 0) ||
          (wcscmp(ids[0], L"200") == 0 && wcscmp(ids[1], L"100") == 0)))
    {
        wprintf(L"dupvuid: unexpected IDs %s %s\n", ids[0], ids[1]);
        goto done;
    }
    {
        uint32_t i;
        for (i = 0; i < n_ids; i++)
        {
            free(ids[i]);
        }
    }
    free(ids);
    ids = NULL;

    EeVoterTable_Clear(&t);
    EeVoterTable_Init(&t);
    if (EeVoterTable_LoadFromFile(L"test\\sample_voters.csv",
                                  &t,
                                  NULL,
                                  NULL,
                                  NULL,
                                  err,
                                  ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"dupvuid: sample load failed %s\n", err);
        goto done;
    }
    if (!EeVoterTable_CollectDuplicateVoterIds(&t, &ids, &n_ids) || n_ids != 0)
    {
        wprintf(L"dupvuid: sample should have no duplicates, got %u\n", n_ids);
        goto done;
    }

    rc = 0;
    wprintf(L"dupvuid ok\n");

done:
    if (ids != NULL)
    {
        uint32_t i;
        for (i = 0; i < n_ids; i++)
        {
            free(ids[i]);
        }
        free(ids);
    }
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"dupvuid test failed\n");
    }
    return rc;
}

static void free_wide_ids(wchar_t **ids, uint32_t n)
{
    uint32_t i;
    if (ids == NULL)
    {
        return;
    }
    for (i = 0; i < n; i++)
    {
        free(ids[i]);
    }
    free(ids);
}

static BOOL wide_ids_contain(wchar_t **ids, uint32_t n, const wchar_t *want)
{
    uint32_t i;
    for (i = 0; i < n; i++)
    {
        if (ids[i] != NULL && wcscmp(ids[i], want) == 0)
        {
            return TRUE;
        }
    }
    return FALSE;
}

static int test_duplicate_voters(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    wchar_t **ids = NULL;
    uint32_t n_ids = 0;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_dup_voters.csv")))
    {
        wprintf(L"dupvoter: temp path failed\n");
        return 1;
    }

    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"dupvoter: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM\n", fp);
    fputs("100,Smith,John\n", fp);
    fputs("101,Smith,John\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 2)
    {
        wprintf(L"dupvoter: no-dob load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    if (EeVoterTable_FindBirthdateColumn(&t) != -1)
    {
        wprintf(L"dupvoter: expected no birth date column\n");
        goto done;
    }
    if (!EeVoterTable_CollectDuplicateVotersByNameDob(&t, &ids, &n_ids) || n_ids != 0)
    {
        wprintf(L"dupvoter: no-dob collect expected 0, got %u\n", n_ids);
        goto done;
    }
    EeVoterTable_Clear(&t);

    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"dupvoter: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM,Birth Date\n", fp);
    fputs("100,Smith,John,1/15/1990\n", fp);
    fputs("200,Jones,Jane,2/20/1985\n", fp);
    fputs("101,Smith,John,01/15/1990\n", fp);
    fputs("300,Smith,John,3/1/1991\n", fp);
    fputs("400,Lee,Ann,1/15/1990\n", fp);
    fputs("201,Jones,Jane,2/20/1985\n", fp);
    fputs(",Smith,John,1/15/1990\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 7)
    {
        wprintf(L"dupvoter: load failed %s\n", err);
        goto done;
    }
    if (EeVoterTable_FindBirthdateColumn(&t) < 0)
    {
        wprintf(L"dupvoter: Birth Date column not found\n");
        goto done;
    }
    if (!EeVoterTable_CollectDuplicateVotersByNameDob(&t, &ids, &n_ids) || n_ids != 4 ||
        ids == NULL)
    {
        wprintf(L"dupvoter: expected 4 duplicate voter IDs, got %u\n", n_ids);
        goto done;
    }
    if (!wide_ids_contain(ids, n_ids, L"100") || !wide_ids_contain(ids, n_ids, L"101") ||
        !wide_ids_contain(ids, n_ids, L"200") || !wide_ids_contain(ids, n_ids, L"201"))
    {
        wprintf(L"dupvoter: unexpected IDs\n");
        goto done;
    }
    if (wide_ids_contain(ids, n_ids, L"300") || wide_ids_contain(ids, n_ids, L"400"))
    {
        wprintf(L"dupvoter: unique name/DOB rows were treated as duplicates\n");
        goto done;
    }
    free_wide_ids(ids, n_ids);
    ids = NULL;
    n_ids = 0;
    EeVoterTable_Clear(&t);

    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"dupvoter: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM,DOB\n", fp);
    fputs("1,Able,Ann,1/1/2000\n", fp);
    fputs("2,Baker,Bob,1/1/2000\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok)
    {
        wprintf(L"dupvoter: dob-header load failed %s\n", err);
        goto done;
    }
    if (EeVoterTable_FindBirthdateColumn(&t) < 0)
    {
        wprintf(L"dupvoter: DOB column not found\n");
        goto done;
    }
    if (!EeVoterTable_CollectDuplicateVotersByNameDob(&t, &ids, &n_ids) || n_ids != 0)
    {
        wprintf(L"dupvoter: unique names expected 0, got %u\n", n_ids);
        goto done;
    }
    EeVoterTable_Clear(&t);

    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"dupvoter: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM,Birth_Day,EDRDAT\n", fp);
    fputs("1,Smith,John,15,20200115\n", fp);
    fputs("2,Smith,John,15,20200115\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok)
    {
        wprintf(L"dupvoter: birth-day load failed %s\n", err);
        goto done;
    }
    if (EeVoterTable_FindBirthdateColumn(&t) != -1)
    {
        wprintf(L"dupvoter: Birth_Day / EDRDAT should not count as DOB\n");
        goto done;
    }

    EeVoterTable_Clear(&t);
    EeVoterTable_Init(&t);
    if (EeVoterTable_LoadFromFile(L"test\\sample_voters.csv",
                                  &t,
                                  NULL,
                                  NULL,
                                  NULL,
                                  err,
                                  ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"dupvoter: sample load failed %s\n", err);
        goto done;
    }
    if (EeVoterTable_FindBirthdateColumn(&t) != -1)
    {
        wprintf(L"dupvoter: sample should have no birth date column\n");
        goto done;
    }

    rc = 0;
    wprintf(L"dupvoter ok\n");

done:
    free_wide_ids(ids, n_ids);
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"dupvoter test failed\n");
    }
    return rc;
}

static int test_partial_birthdate(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    wchar_t buf[64];
    FILE *fp = NULL;
    EeVoterTable t;
    EeFilterSet set;
    EeFilterRule r;
    EeLoadStatus s;
    DWORD n;
    int dob;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_dob_partial.csv")))
    {
        wprintf(L"dobpart: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"dobpart: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,Birthdate\n", fp);
    fputs("1,2004\n", fp);
    fputs("2,*/*/2005\n", fp);
    fputs("3,**/**/2005\n", fp);
    fputs("4,6/15/2005\n", fp);
    fputs("5,2006\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    EeFilter_Init(&set);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 5)
    {
        wprintf(L"dobpart: load failed %s\n", err);
        goto done;
    }
    dob = find_column(&t, L"Birthdate");
    if (dob < 0 || !t.column_is_date[dob] || !EeVoterTable_ColumnIsNumericOrDate(&t, (uint32_t)dob))
    {
        wprintf(L"dobpart: Birthdate should be a date column\n");
        goto done;
    }
    if (!EeVoterTable_ParseDateYmdW(L"2004", NULL) ||
        !EeVoterTable_ParseDateYmdW(L"*/*/2005", NULL) ||
        !EeVoterTable_ParseDateYmdW(L"**/**/2005", NULL) ||
        EeVoterTable_ParseDateYmdW(L"abc", NULL))
    {
        wprintf(L"dobpart: year/mask parse mismatch\n");
        goto done;
    }

    if (!EeVoterTable_SortByColumn(&t, (uint32_t)dob))
    {
        wprintf(L"dobpart: sort failed\n");
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, (uint32_t)dob, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"2004") != 0)
    {
        wprintf(L"dobpart: expected 2004 first, got %s\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 3, (uint32_t)dob, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"6/15/2005") != 0)
    {
        wprintf(L"dobpart: expected 6/15/2005 after year-only 2005, got %s\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 4, (uint32_t)dob, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"2006") != 0)
    {
        wprintf(L"dobpart: expected 2006 last, got %s\n", buf);
        goto done;
    }

    r = make_rule((uint32_t)dob, EeRel_LessThan, EeFilt_Include, L"2006", TRUE);
    if (!EeFilter_RuleIsValid(&r, &t) || !EeFilter_Add(&set, &r) || count_accepted(&set, &t) != 4)
    {
        wprintf(L"dobpart: less than 2006 expected 4 rows\n");
        goto done;
    }

    rc = 0;
    wprintf(L"dobpart ok\n");

done:
    EeFilter_Clear(&set);
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"dobpart test failed\n");
    }
    return rc;
}

static int test_mark_duplicates(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    uint8_t *marks = NULL;
    uint32_t count = 0;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_mark_dups.csv")))
    {
        wprintf(L"markdup: temp path failed\n");
        return 1;
    }

    /* Voter-ID duplicates: 100 x2, 200 x3, 300 x1 -> 5 marked physical rows. */
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"markdup: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM\n", fp);
    fputs("100,Smith,John\n", fp);
    fputs("200,Jones,Jane\n", fp);
    fputs("100,Smith,Jon\n", fp);
    fputs("300,Lee,Ann\n", fp);
    fputs("200,Jones,Janet\n", fp);
    fputs("200,Jones,Jan\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 6)
    {
        wprintf(L"markdup: vuid load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    marks = (uint8_t *)calloc(t.row_count, 1);
    if (marks == NULL)
    {
        wprintf(L"markdup: out of memory\n");
        goto done;
    }
    if (!EeVoterTable_MarkDuplicateVoterIds(&t, marks, &count, NULL, NULL, NULL) || count != 5)
    {
        wprintf(L"markdup: vuid expected 5 marked, got %u\n", count);
        goto done;
    }
    if (!marks[0] || !marks[1] || !marks[2] || marks[3] || !marks[4] || !marks[5])
    {
        wprintf(L"markdup: vuid marked the wrong rows\n");
        goto done;
    }
    free(marks);
    marks = NULL;
    count = 0;
    EeVoterTable_Clear(&t);

    /* Name + DOB duplicates, including an empty-VUID row: rows 0/2/6 share
     * Smith/John/1990-01-15 and rows 1/5 share Jones/Jane -> 5 marked. */
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"markdup: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,LSTNAM,FSTNAM,Birth Date\n", fp);
    fputs("100,Smith,John,1/15/1990\n", fp);
    fputs("200,Jones,Jane,2/20/1985\n", fp);
    fputs("101,Smith,John,01/15/1990\n", fp);
    fputs("300,Smith,John,3/1/1991\n", fp);
    fputs("400,Lee,Ann,1/15/1990\n", fp);
    fputs("201,Jones,Jane,2/20/1985\n", fp);
    fputs(",Smith,John,1/15/1990\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 7)
    {
        wprintf(L"markdup: namedob load failed %s\n", err);
        goto done;
    }
    marks = (uint8_t *)calloc(t.row_count, 1);
    if (marks == NULL)
    {
        wprintf(L"markdup: out of memory\n");
        goto done;
    }
    if (!EeVoterTable_MarkDuplicateVotersByNameDob(&t, marks, &count, NULL, NULL, NULL) ||
        count != 5)
    {
        wprintf(L"markdup: namedob expected 5 marked, got %u\n", count);
        goto done;
    }
    if (!marks[0] || !marks[1] || !marks[2] || marks[3] || marks[4] || !marks[5] || !marks[6])
    {
        wprintf(L"markdup: namedob marked the wrong rows\n");
        goto done;
    }

    rc = 0;
    wprintf(L"markdup ok\n");

done:
    free(marks);
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"markdup test failed\n");
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
    failed |= test_res_addr_zip_dash_and_unit();
    failed |= test_house_number_dot_zero();
    failed |= test_lot_unit_ignored();
    failed |= test_filter_logic();
    failed |= test_empty_numeric_header();
    failed |= test_date_sort();
    failed |= test_duplicate_voter_ids();
    failed |= test_duplicate_voters();
    failed |= test_mark_duplicates();
    failed |= test_precinct_normalize();
    failed |= test_partial_birthdate();
    failed |= test_name_last_first_no_address();
    return failed == 0 ? 0 : 1;
}
