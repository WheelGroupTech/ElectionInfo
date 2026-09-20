/**
 * @file smoke_load.c
 * @brief Console smoke test for EeVoterTable_LoadFromFile.
 */

#include "filter.h"
#include "settings.h"
#include "voter_table.h"
#include "ee_cvr.h"

#include "third_party/miniz/miniz.h"

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
    if (wcscmp(buf, L"1109 N IH 35  NB, AUSTIN, TX 78702") != 0)
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

/* A full Residential Address that already ends with its own ZIP must not get
 * unrelated jurisdiction/district columns appended. Travis exports name a
 * district-code column "CITY" (e.g. "C10") and "STATE BOARD OF EDUCATION"
 * (e.g. "5"), which the header heuristics classify as city/state (tag: distcode). */
static int test_district_codes_not_appended(void)
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
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_distcode.csv")))
    {
        wprintf(L"distcode: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"distcode: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,NAME,Residential Address,Precinct,STATE BOARD OF EDUCATION,CITY\n", fp);
    fputs("2128393968,ABAGARO MOSISA,1109 N IH 35  NB AUSTIN TX 78702 ,P 100,5,C10\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 1)
    {
        wprintf(L"distcode: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"1109 N IH 35  NB AUSTIN TX 78702") != 0)
    {
        wprintf(L"distcode: district codes appended (%s)\n", buf);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_Clear(&t);
    rc = 0;
    wprintf(L"distcode ok\n");
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
    /* Built from parts now: the unit (from Unit/Unit Type) is included and the
     * tail is emitted in the consistent "street, city, STATE zip" style. */
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"3001 MEDICAL ARTS ST APT 116, AUSTIN, TX 78705") != 0)
    {
        wprintf(L"zipdash: empty +4 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"3400 HARMON AVE APT 367, AUSTIN, TX 78705-2119") != 0)
    {
        wprintf(L"zipdash: zip+4 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"3502 RED RIVER ST, AUSTIN, TX 78705") != 0)
    {
        wprintf(L"zipdash: no-unit empty +4 mismatch (%s)\n", buf);
        goto done;
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

static int test_value_counts(void)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    EeValueCount *items = NULL;
    uint32_t count = 0;
    uint32_t blank = 0;
    int col;
    uint32_t i;
    uint32_t austin = 0, dallas = 0, houston = 0, other = 0;
    int rc = 1;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_valcounts.csv")))
    {
        wprintf(L"valcount: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"valcount: could not create %s\n", path);
        return 1;
    }
    fputs("VUID,City\n", fp);
    fputs("1,Austin\n", fp);
    fputs("2,austin\n", fp); /* case-insensitive grouping with row 1 */
    fputs("3,Dallas\n", fp);
    fputs("4,Austin\n", fp);
    fputs("5,Houston\n", fp);
    fputs("6,\n", fp); /* empty -> ignored */
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 6)
    {
        wprintf(L"valcount: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    col = find_column(&t, L"City");
    if (col < 0)
    {
        wprintf(L"valcount: City column not found\n");
        goto done;
    }
    if (!EeVoterTable_CollectValueCounts(&t, (uint32_t)col, &items, &count, &blank) || count != 3 ||
        blank != 1 || items == NULL)
    {
        wprintf(L"valcount: expected 3 distinct cities + 1 blank, got %u / %u\n", count, blank);
        goto done;
    }
    for (i = 0; i < count; i++)
    {
        if (_wcsicmp(items[i].value, L"Austin") == 0)
        {
            austin = items[i].count;
        }
        else if (_wcsicmp(items[i].value, L"Dallas") == 0)
        {
            dallas = items[i].count;
        }
        else if (_wcsicmp(items[i].value, L"Houston") == 0)
        {
            houston = items[i].count;
        }
        else
        {
            other++;
        }
    }
    if (austin != 3 || dallas != 1 || houston != 1 || other != 0)
    {
        wprintf(L"valcount: bad counts austin=%u dallas=%u houston=%u other=%u\n",
                austin,
                dallas,
                houston,
                other);
        goto done;
    }

    rc = 0;
    wprintf(L"valcount ok\n");

done:
    EeVoterTable_FreeValueCounts(items, count);
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"valcount test failed\n");
    }
    return rc;
}

/* Write @p contents to a temp file named @p leaf; load it into @p t.
 * Returns TRUE on success (caller clears @p t and deletes nothing else). */
static BOOL cmp_write_and_load(const wchar_t *leaf, const char *contents, EeVoterTable *t)
{
    wchar_t path[MAX_PATH];
    wchar_t err[256];
    FILE *fp = NULL;
    DWORD n;

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) || FAILED(StringCchCatW(path, ARRAYSIZE(path), leaf)))
    {
        return FALSE;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        return FALSE;
    }
    fputs(contents, fp);
    fclose(fp);

    EeVoterTable_Init(t);
    err[0] = L'\0';
    if (EeVoterTable_LoadFromFile(path, t, NULL, NULL, NULL, err, ARRAYSIZE(err)) !=
        EeLoadStatus_Ok)
    {
        DeleteFileW(path);
        wprintf(L"cmp: load failed %s\n", err);
        return FALSE;
    }
    DeleteFileW(path);
    return TRUE;
}

static int test_compare(void)
{
    EeVoterTable a;
    EeVoterTable b;
    uint8_t *class_a = NULL;
    uint8_t *class_b = NULL;
    EeCompareResult r;
    int rc = 1;
    BOOL a_ok = FALSE;
    BOOL b_ok = FALSE;

    /* Header maps Voter ID / Precinct / Name (Last+First) / Address. Each matched
     * row exercises one change kind; only the intended field differs per row. */
    a_ok = cmp_write_and_load(L"ee_cmp_a.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 Main St\n"    /* identical */
                              "2,101,Meyer,Anne,200 Oak Ave\n"    /* name minor (Meyer->Meyers) */
                              "3,101,Garcia,Carlos,300 Pine Rd\n" /* addr minor (300->301) */
                              "4,101,Brown,Robert,400 Elm St\n"   /* addr major (moved) */
                              "5,101,Lee,Ann,500 Cedar Ln\n"      /* precinct changed (101->205) */
                              "6,102,Davis,Major,600 Birch St\n"  /* name major */
                              "7,103,Only,Aaa,700 Only Rd\n"      /* only in A */
                              ",104,Blank,Voter,800 Blank St\n",  /* blank ID -> only in A */
                              &a);
    b_ok = cmp_write_and_load(
        L"ee_cmp_b.csv",
        "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
        "1,101,Smith,John,100 Main St\n"              /* identical */
        "2,101,Meyers,Anne,200 Oak Ave\n"             /* name minor */
        "3,101,Garcia,Carlos,301 Pine Rd\n"           /* addr minor */
        "4,210,Brown,Robert,9900 Zephyr Blvd Apt 7\n" /* addr major (+pct: suppressed) */
        "5,205,Lee,Ann,500 Cedar Ln\n"                /* precinct changed */
        "6,102,Wellington,Bartholomew,600 Birch St\n" /* name major */
        "8,103,New,Bbb,900 New Rd\n"                  /* only in B */
        ",105,Other,Blank,950 Other St\n",            /* blank ID -> only in B */
        &b);
    if (!a_ok || !b_ok)
    {
        goto done;
    }
    if (a.row_count != 8 || b.row_count != 8)
    {
        wprintf(L"cmp: expected 8 rows each, got %u / %u\n", a.row_count, b.row_count);
        goto done;
    }

    class_a = (uint8_t *)calloc(a.row_count, 1);
    class_b = (uint8_t *)calloc(b.row_count, 1);
    if (class_a == NULL || class_b == NULL)
    {
        wprintf(L"cmp: out of memory\n");
        goto done;
    }
    if (!EeVoterTable_CompareByVoterId(&a, &b, class_a, class_b, &r, NULL, NULL, NULL))
    {
        wprintf(L"cmp: compare failed\n");
        goto done;
    }

    if (r.only_a != 2 || r.identical_a != 1 || r.name_minor_a != 1 || r.name_major_a != 1 ||
        r.addr_minor_a != 1 || r.addr_major_a != 1 || r.pct_changed_a != 1)
    {
        wprintf(L"cmp: bad A counts only=%u id=%u nmin=%u nmaj=%u amin=%u amaj=%u pct=%u\n",
                r.only_a,
                r.identical_a,
                r.name_minor_a,
                r.name_major_a,
                r.addr_minor_a,
                r.addr_major_a,
                r.pct_changed_a);
        goto done;
    }
    if (r.only_b != 2 || r.identical_b != 1 || r.name_minor_b != 1 || r.name_major_b != 1 ||
        r.addr_minor_b != 1 || r.addr_major_b != 1 || r.pct_changed_b != 1)
    {
        wprintf(L"cmp: bad B counts only=%u id=%u nmin=%u nmaj=%u amin=%u amaj=%u pct=%u\n",
                r.only_b,
                r.identical_b,
                r.name_minor_b,
                r.name_major_b,
                r.addr_minor_b,
                r.addr_major_b,
                r.pct_changed_b);
        goto done;
    }
    /* Spot-check a few per-row bit sets on the A side. */
    if (!(class_a[0] & EE_CMP_MATCHED) || (class_a[0] & EE_CMP_CHANGE_BITS) != 0)
    {
        wprintf(L"cmp: row0 should be identical, got 0x%02X\n", class_a[0]);
        goto done;
    }
    if (class_a[1] != (uint8_t)(EE_CMP_MATCHED | EE_CMP_NAME_MINOR) ||
        class_a[3] != (uint8_t)(EE_CMP_MATCHED | EE_CMP_ADDR_MAJOR) ||
        class_a[4] != (uint8_t)(EE_CMP_MATCHED | EE_CMP_PCT_CHANGED) ||
        class_a[6] != EE_CMP_ONLY_HERE || class_a[7] != EE_CMP_ONLY_HERE)
    {
        wprintf(L"cmp: bad A bits n=0x%02X am=0x%02X pct=0x%02X only=0x%02X blank=0x%02X\n",
                class_a[1],
                class_a[3],
                class_a[4],
                class_a[6],
                class_a[7]);
        goto done;
    }

    rc = 0;
    wprintf(L"cmp ok\n");

done:
    free(class_a);
    free(class_b);
    if (a_ok)
    {
        EeVoterTable_Clear(&a);
    }
    if (b_ok)
    {
        EeVoterTable_Clear(&b);
    }
    if (rc != 0)
    {
        wprintf(L"cmp test failed\n");
    }
    return rc;
}

static int test_preamble_skip(void)
{
    EeVoterTable t;
    int rc = 1;
    BOOL loaded;

    /* Line 1 is a portal banner padded with empty cells; the real header is on
     * line 2 (no address columns). The loader must skip the banner. */
    loaded = cmp_write_and_load(
        L"ee_preamble.csv",
        "TX 20260501 Submissions 07-23-2026_09-21-12,,,,,,,,\n"
        "ID_County_VR,Reg_Precinct,VID,VUID,Name_Last,Name_First,Name_Middle,Name_Suffix,DOD\n"
        "Travis,358,TX1002114877,1002114877,HUDSON,BERTHA,DEAN,,2016-11-02\n",
        &t);
    if (!loaded)
    {
        wprintf(L"preamble: load failed\n");
        return 1;
    }
    if (t.row_count != 1)
    {
        wprintf(L"preamble: expected 1 data row, got %u\n", t.row_count);
        goto done;
    }
    if (strcmp(EeVoterTable_GetCellUtf8(&t, 0, EE_COL_VOTER_ID), "1002114877") != 0)
    {
        wprintf(L"preamble: Voter ID not normalized (got '%S')\n",
                EeVoterTable_GetCellUtf8(&t, 0, EE_COL_VOTER_ID));
        goto done;
    }
    if (strcmp(EeVoterTable_GetCellUtf8(&t, 0, EE_COL_PRECINCT), "358") != 0)
    {
        wprintf(L"preamble: Precinct wrong (got '%S')\n",
                EeVoterTable_GetCellUtf8(&t, 0, EE_COL_PRECINCT));
        goto done;
    }
    if (strstr(EeVoterTable_GetCellUtf8(&t, 0, EE_COL_NAME), "HUDSON") == NULL)
    {
        wprintf(L"preamble: Name missing (got '%S')\n",
                EeVoterTable_GetCellUtf8(&t, 0, EE_COL_NAME));
        goto done;
    }

    rc = 0;
    wprintf(L"preamble ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"preamble test failed\n");
    }
    return rc;
}

static int test_id_voter_header(void)
{
    EeVoterTable t;
    int rc = 1;

    /* "ID_VOTER" should map to the normalized Voter ID column. */
    if (!cmp_write_and_load(L"ee_idvoter.csv",
                            "ID_VOTER,PCTCOD,LSTNAM,FSTNAM\n"
                            "1002114877,358,HUDSON,BERTHA\n",
                            &t))
    {
        wprintf(L"idvoter: load failed\n");
        return 1;
    }
    if (t.row_count != 1)
    {
        wprintf(L"idvoter: expected 1 row, got %u\n", t.row_count);
        goto done;
    }
    if (strcmp(EeVoterTable_GetCellUtf8(&t, 0, EE_COL_VOTER_ID), "1002114877") != 0)
    {
        wprintf(L"idvoter: Voter ID not mapped (got '%S')\n",
                EeVoterTable_GetCellUtf8(&t, 0, EE_COL_VOTER_ID));
        goto done;
    }

    rc = 0;
    wprintf(L"idvoter ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"idvoter test failed\n");
    }
    return rc;
}

static int test_compare_formatting(void)
{
    EeVoterTable a;
    EeVoterTable b;
    uint8_t *class_a = NULL;
    uint8_t *class_b = NULL;
    EeCompareResult r;
    int rc = 1;
    BOOL a_ok = FALSE;
    BOOL b_ok = FALSE;

    /* Same voter/address; the addresses differ only by commas before city/state
     * (as happens when one file supplies a full address line and the other parts).
     * The compare must treat them as unchanged. */
    a_ok = cmp_write_and_load(L"ee_cmpfmt_a.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 MAIN ST AUSTIN TX 78701\n",
                              &a);
    b_ok = cmp_write_and_load(L"ee_cmpfmt_b.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,\"100 MAIN ST, AUSTIN, TX 78701\"\n",
                              &b);
    if (!a_ok || !b_ok)
    {
        goto done;
    }
    class_a = (uint8_t *)calloc(a.row_count ? a.row_count : 1, 1);
    class_b = (uint8_t *)calloc(b.row_count ? b.row_count : 1, 1);
    if (class_a == NULL || class_b == NULL)
    {
        wprintf(L"cmpfmt: out of memory\n");
        goto done;
    }
    if (!EeVoterTable_CompareByVoterId(&a, &b, class_a, class_b, &r, NULL, NULL, NULL))
    {
        wprintf(L"cmpfmt: compare failed\n");
        goto done;
    }
    if (r.identical_a != 1 || r.addr_minor_a != 0 || r.addr_major_a != 0 || r.name_minor_a != 0 ||
        r.name_major_a != 0 || r.pct_changed_a != 0 || r.only_a != 0)
    {
        wprintf(L"cmpfmt: comma-only address flagged as change "
                L"(id=%u amin=%u amaj=%u nmin=%u nmaj=%u pct=%u only=%u)\n",
                r.identical_a,
                r.addr_minor_a,
                r.addr_major_a,
                r.name_minor_a,
                r.name_major_a,
                r.pct_changed_a,
                r.only_a);
        goto done;
    }

    rc = 0;
    wprintf(L"cmpfmt ok\n");

done:
    free(class_a);
    free(class_b);
    if (a_ok)
    {
        EeVoterTable_Clear(&a);
    }
    if (b_ok)
    {
        EeVoterTable_Clear(&b);
    }
    if (rc != 0)
    {
        wprintf(L"cmpfmt test failed\n");
    }
    return rc;
}

static int test_compare_missing_state(void)
{
    EeVoterTable a;
    EeVoterTable b;
    uint8_t *class_a = NULL;
    uint8_t *class_b = NULL;
    EeCompareResult r;
    int rc = 1;
    BOOL a_ok = FALSE;
    BOOL b_ok = FALSE;

    /* File A has the state token; file B omits it. Same residence -> not a change
     * (ZIP already encodes the state). */
    a_ok = cmp_write_and_load(L"ee_cmpstate_a.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 MAIN ST AUSTIN TX 78701\n",
                              &a);
    b_ok = cmp_write_and_load(L"ee_cmpstate_b.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 MAIN ST AUSTIN 78701\n",
                              &b);
    if (!a_ok || !b_ok)
    {
        goto done;
    }
    class_a = (uint8_t *)calloc(a.row_count ? a.row_count : 1, 1);
    class_b = (uint8_t *)calloc(b.row_count ? b.row_count : 1, 1);
    if (class_a == NULL || class_b == NULL)
    {
        wprintf(L"cmpstate: out of memory\n");
        goto done;
    }
    if (!EeVoterTable_CompareByVoterId(&a, &b, class_a, class_b, &r, NULL, NULL, NULL))
    {
        wprintf(L"cmpstate: compare failed\n");
        goto done;
    }
    if (r.identical_a != 1 || r.addr_minor_a != 0 || r.addr_major_a != 0)
    {
        wprintf(L"cmpstate: missing-state address flagged (id=%u amin=%u amaj=%u)\n",
                r.identical_a,
                r.addr_minor_a,
                r.addr_major_a);
        goto done;
    }

    rc = 0;
    wprintf(L"cmpstate ok\n");

done:
    free(class_a);
    free(class_b);
    if (a_ok)
    {
        EeVoterTable_Clear(&a);
    }
    if (b_ok)
    {
        EeVoterTable_Clear(&b);
    }
    if (rc != 0)
    {
        wprintf(L"cmpstate test failed\n");
    }
    return rc;
}

static int test_compare_zip4(void)
{
    EeVoterTable a;
    EeVoterTable b;
    uint8_t *class_a = NULL;
    uint8_t *class_b = NULL;
    EeCompareResult r;
    int rc = 1;
    BOOL a_ok = FALSE;
    BOOL b_ok = FALSE;

    /* Same residence; one file has ZIP5, the other ZIP5-4. Not a change. */
    a_ok = cmp_write_and_load(L"ee_cmpzip_a.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 MAIN ST AUSTIN TX 78702\n",
                              &a);
    b_ok = cmp_write_and_load(L"ee_cmpzip_b.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 MAIN ST AUSTIN TX 78702-1234\n",
                              &b);
    if (!a_ok || !b_ok)
    {
        goto done;
    }
    class_a = (uint8_t *)calloc(a.row_count ? a.row_count : 1, 1);
    class_b = (uint8_t *)calloc(b.row_count ? b.row_count : 1, 1);
    if (class_a == NULL || class_b == NULL)
    {
        wprintf(L"cmpzip: out of memory\n");
        goto done;
    }
    if (!EeVoterTable_CompareByVoterId(&a, &b, class_a, class_b, &r, NULL, NULL, NULL))
    {
        wprintf(L"cmpzip: compare failed\n");
        goto done;
    }
    if (r.identical_a != 1 || r.addr_minor_a != 0 || r.addr_major_a != 0)
    {
        wprintf(L"cmpzip: ZIP+4-only difference flagged (id=%u amin=%u amaj=%u)\n",
                r.identical_a,
                r.addr_minor_a,
                r.addr_major_a);
        goto done;
    }

    rc = 0;
    wprintf(L"cmpzip ok\n");

done:
    free(class_a);
    free(class_b);
    if (a_ok)
    {
        EeVoterTable_Clear(&a);
    }
    if (b_ok)
    {
        EeVoterTable_Clear(&b);
    }
    if (rc != 0)
    {
        wprintf(L"cmpzip test failed\n");
    }
    return rc;
}

static int test_compare_diffs(void)
{
    EeVoterTable a;
    EeVoterTable b;
    EeCompareDiff *diffs = NULL;
    uint32_t n = 0;
    int rc = 1;
    BOOL a_ok = FALSE;
    BOOL b_ok = FALSE;

    a_ok = cmp_write_and_load(L"ee_cmpdiff_a.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 Main St\n"  /* identical */
                              "2,101,Jones,Jane,200 Oak Ave\n", /* name minor + addr major */
                              &a);
    b_ok = cmp_write_and_load(L"ee_cmpdiff_b.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 Main St\n"
                              "2,101,Jones,Janet,999 New Blvd\n",
                              &b);
    if (!a_ok || !b_ok)
    {
        goto done;
    }
    if (!EeVoterTable_CollectDifferences(&a, &b, &diffs, &n, NULL, NULL, NULL))
    {
        wprintf(L"cmpdiff: collect failed\n");
        goto done;
    }
    if (n != 1 || diffs == NULL)
    {
        wprintf(L"cmpdiff: expected 1 diff, got %u\n", n);
        goto done;
    }
    if (diffs[0].row_a != 1 || diffs[0].row_b != 1)
    {
        wprintf(L"cmpdiff: bad pairing a=%u b=%u\n", diffs[0].row_a, diffs[0].row_b);
        goto done;
    }
    if (!(diffs[0].bits & EE_CMP_NAME_MINOR) || !(diffs[0].bits & EE_CMP_ADDR_MAJOR) ||
        (diffs[0].bits & EE_CMP_PCT_CHANGED))
    {
        wprintf(L"cmpdiff: bad bits 0x%02X\n", diffs[0].bits);
        goto done;
    }

    rc = 0;
    wprintf(L"cmpdiff ok\n");

done:
    free(diffs);
    if (a_ok)
    {
        EeVoterTable_Clear(&a);
    }
    if (b_ok)
    {
        EeVoterTable_Clear(&b);
    }
    if (rc != 0)
    {
        wprintf(L"cmpdiff test failed\n");
    }
    return rc;
}

/* Round-trip the registry-backed settings through a throwaway test key so the
 * user's real options are never touched (tag: settings). */
static int test_settings_roundtrip(void)
{
    static const wchar_t k_TestKey[] = L"Software\\WheelGroupTech\\ElectionExplorer Test";
    EeSettings a;
    EeSettings b;
    int rc = 0;

    /* A missing key must yield defaults and report FALSE. */
    RegDeleteKeyW(HKEY_CURRENT_USER, k_TestKey);
    EeSettings_Defaults(&a);
    ZeroMemory(&b, sizeof(b));
    if (EeSettings_LoadFrom(k_TestKey, &b))
    {
        wprintf(L"settings: load of absent key should return FALSE\n");
        rc = 1;
    }
    if (b.zoom_percent != a.zoom_percent || b.map_engine != a.map_engine ||
        b.copy_prepend_normalized != a.copy_prepend_normalized ||
        b.name_surname_first != a.name_surname_first ||
        b.cvr_merge_writeins != a.cvr_merge_writeins)
    {
        wprintf(L"settings: absent key did not yield defaults\n");
        rc = 1;
    }
    if (!a.cvr_merge_writeins)
    {
        wprintf(L"settings: cvr_merge_writeins default should be TRUE\n");
        rc = 1;
    }

    /* Round-trip a non-default set of options. */
    a.zoom_percent = 175;
    a.map_engine = 3;
    a.copy_prepend_normalized = FALSE;
    a.name_surname_first = FALSE;
    a.cvr_merge_writeins = FALSE;
    if (!EeSettings_SaveTo(k_TestKey, &a))
    {
        wprintf(L"settings: SaveTo failed\n");
        rc = 1;
    }
    ZeroMemory(&b, sizeof(b));
    if (!EeSettings_LoadFrom(k_TestKey, &b))
    {
        wprintf(L"settings: LoadFrom of saved key failed\n");
        rc = 1;
    }
    if (b.zoom_percent != 175 || b.map_engine != 3 || b.copy_prepend_normalized ||
        b.name_surname_first || b.cvr_merge_writeins)
    {
        wprintf(L"settings: round-trip mismatch (zoom=%d map=%d pre=%d sur=%d mrg=%d)\n",
                b.zoom_percent,
                b.map_engine,
                (int)b.copy_prepend_normalized,
                (int)b.name_surname_first,
                (int)b.cvr_merge_writeins);
        rc = 1;
    }

    RegDeleteKeyW(HKEY_CURRENT_USER, k_TestKey);
    wprintf(L"settings roundtrip: %s\n", rc == 0 ? L"ok" : L"FAIL");
    return rc;
}

/* Author a minimal .xlsx in memory (miniz) that uses the SHARED-STRING table
 * (which the openpyxl fixture does not), a numeric cell, a boolean, and an error
 * cell, load it via EeVoterTable_LoadXlsxSheet, and verify the pipeline output
 * (tag: xlsx). */
static int test_xlsx_roundtrip(void)
{
    static const char *k_workbook =
        "<?xml version=\"1.0\"?><workbook "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
        "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
        "<sheets><sheet name=\"Voters\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>";
    static const char *k_rels =
        "<?xml version=\"1.0\"?><Relationships "
        "xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" "
        "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" "
        "Target=\"worksheets/sheet1.xml\"/></Relationships>";
    static const char *k_shared =
        "<?xml version=\"1.0\"?><sst "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" count=\"7\" "
        "uniqueCount=\"7\"><si><t>VUID</t></si><si><t>NAME</t></si>"
        "<si><t>Residential Address</t></si><si><t>Active</t></si><si><t>Note</t></si>"
        "<si><t>Smith, John</t></si><si><t>100 Main St Austin TX 78701</t></si></sst>";
    static const char *k_sheet =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>"
        "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\" t=\"s\"><v>1</v></c>"
        "<c r=\"C1\" t=\"s\"><v>2</v></c><c r=\"D1\" t=\"s\"><v>3</v></c>"
        "<c r=\"E1\" t=\"s\"><v>4</v></c></row>"
        "<row r=\"2\"><c r=\"A2\"><v>100</v></c><c r=\"B2\" t=\"s\"><v>5</v></c>"
        "<c r=\"C2\" t=\"s\"><v>6</v></c><c r=\"D2\" t=\"b\"><v>1</v></c>"
        "<c r=\"E2\" t=\"e\"><v>#DIV/0!</v></c></row></sheetData></worksheet>";

    wchar_t path[MAX_PATH];
    wchar_t err[256] = L"";
    wchar_t buf[128];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int rc = 1;
    mz_zip_archive zip;
    void *zbuf = NULL;
    size_t zsize = 0;

    mz_zip_zero_struct(&zip);
    if (!mz_zip_writer_init_heap(&zip, 0, 0) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/workbook.xml",
                               k_workbook,
                               strlen(k_workbook),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/_rels/workbook.xml.rels",
                               k_rels,
                               strlen(k_rels),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/sharedStrings.xml",
                               k_shared,
                               strlen(k_shared),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/worksheets/sheet1.xml",
                               k_sheet,
                               strlen(k_sheet),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_finalize_heap_archive(&zip, &zbuf, &zsize))
    {
        wprintf(L"xlsx: failed to author test workbook\n");
        mz_zip_writer_end(&zip);
        return 1;
    }

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_roundtrip.xlsx")))
    {
        wprintf(L"xlsx: temp path failed\n");
        mz_free(zbuf);
        mz_zip_writer_end(&zip);
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL || fwrite(zbuf, 1, zsize, fp) != zsize)
    {
        wprintf(L"xlsx: could not write %s\n", path);
        if (fp != NULL)
            fclose(fp);
        mz_free(zbuf);
        mz_zip_writer_end(&zip);
        return 1;
    }
    fclose(fp);
    mz_free(zbuf);
    mz_zip_writer_end(&zip);

    EeVoterTable_Init(&t);
    s = EeVoterTable_LoadXlsxSheet(path, 0, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 1 || t.column_count != 9)
    {
        wprintf(L"xlsx: load failed s=%d rows=%u cols=%u err=%s\n",
                (int)s,
                t.row_count,
                t.column_count,
                err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    /* Frozen: 0=Voter ID 1=Precinct 2=Name 3=Address; source: 4=VUID 5=NAME
     * 6=Residential Address 7=Active 8=Note. */
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_VOTER_ID, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"100") != 0)
    {
        wprintf(L"xlsx: Voter ID (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"100 Main St Austin TX 78701") != 0)
    {
        wprintf(L"xlsx: Address (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, 7, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"TRUE") != 0)
    {
        wprintf(L"xlsx: boolean (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, 8, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"#DIV/0!") != 0)
    {
        wprintf(L"xlsx: error cell (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"xlsx ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"xlsx test failed\n");
    }
    return rc;
}

/* Phase 3: a numeric cell styled as a date must load as a date string, and a
 * number styled with a zero-pad format (e.g. a ZIP "00000") must keep its leading
 * zero. Authors a workbook with styles.xml (tag: xlsxfmt). */
static int test_xlsx_styles(void)
{
    static const char *k_workbook =
        "<?xml version=\"1.0\"?><workbook "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
        "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
        "<sheets><sheet name=\"S\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>";
    static const char *k_rels =
        "<?xml version=\"1.0\"?><Relationships "
        "xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" "
        "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" "
        "Target=\"worksheets/sheet1.xml\"/></Relationships>";
    static const char *k_shared =
        "<?xml version=\"1.0\"?><sst "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" count=\"3\" "
        "uniqueCount=\"3\"><si><t>VUID</t></si><si><t>BirthDate</t></si>"
        "<si><t>Postal</t></si></sst>";
    static const char *k_styles =
        "<?xml version=\"1.0\"?><styleSheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
        "<numFmts count=\"1\"><numFmt numFmtId=\"164\" formatCode=\"00000\"/></numFmts>"
        "<cellXfs count=\"3\"><xf numFmtId=\"0\"/><xf numFmtId=\"14\"/>"
        "<xf numFmtId=\"164\"/></cellXfs></styleSheet>";
    static const char *k_sheet =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>"
        "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\" t=\"s\"><v>1</v></c>"
        "<c r=\"C1\" t=\"s\"><v>2</v></c></row>"
        "<row r=\"2\"><c r=\"A2\"><v>100</v></c><c r=\"B2\" s=\"1\"><v>43831</v></c>"
        "<c r=\"C2\" s=\"2\"><v>7001</v></c></row></sheetData></worksheet>";

    wchar_t path[MAX_PATH];
    wchar_t err[256] = L"";
    wchar_t buf[128];
    FILE *fp = NULL;
    EeVoterTable t;
    EeLoadStatus s;
    DWORD n;
    int rc = 1;
    mz_zip_archive zip;
    void *zbuf = NULL;
    size_t zsize = 0;

    mz_zip_zero_struct(&zip);
    if (!mz_zip_writer_init_heap(&zip, 0, 0) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/workbook.xml",
                               k_workbook,
                               strlen(k_workbook),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/_rels/workbook.xml.rels",
                               k_rels,
                               strlen(k_rels),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/sharedStrings.xml",
                               k_shared,
                               strlen(k_shared),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/styles.xml",
                               k_styles,
                               strlen(k_styles),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip,
                               "xl/worksheets/sheet1.xml",
                               k_sheet,
                               strlen(k_sheet),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_finalize_heap_archive(&zip, &zbuf, &zsize))
    {
        wprintf(L"xlsxfmt: failed to author test workbook\n");
        mz_zip_writer_end(&zip);
        return 1;
    }

    n = GetTempPathW(ARRAYSIZE(path), path);
    if (n == 0 || n >= ARRAYSIZE(path) ||
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_xlsxfmt.xlsx")))
    {
        wprintf(L"xlsxfmt: temp path failed\n");
        mz_free(zbuf);
        mz_zip_writer_end(&zip);
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL || fwrite(zbuf, 1, zsize, fp) != zsize)
    {
        wprintf(L"xlsxfmt: could not write %s\n", path);
        if (fp != NULL)
            fclose(fp);
        mz_free(zbuf);
        mz_zip_writer_end(&zip);
        return 1;
    }
    fclose(fp);
    mz_free(zbuf);
    mz_zip_writer_end(&zip);

    EeVoterTable_Init(&t);
    s = EeVoterTable_LoadXlsxSheet(path, 0, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 1)
    {
        wprintf(L"xlsxfmt: load failed s=%d rows=%u err=%s\n", (int)s, t.row_count, err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    /* Source cols: 4=VUID 5=BirthDate 6=Postal. */
    EeVoterTable_GetViewCellW(&t, 0, 5, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"2020-01-01") != 0)
    {
        wprintf(L"xlsxfmt: date serial not converted (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, 6, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"07001") != 0)
    {
        wprintf(L"xlsxfmt: zero-pad not applied (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"xlsxfmt ok\n");

done:
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"xlsxfmt test failed\n");
    }
    return rc;
}

/* Package a minimal .xlsx (given the full <worksheet> body) at @p path. */
static BOOL cvr_write_xlsx(const wchar_t *path, const char *sheet_xml)
{
    static const char *k_workbook =
        "<?xml version=\"1.0\"?><workbook "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
        "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
        "<sheets><sheet name=\"CVR\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>";
    static const char *k_rels =
        "<?xml version=\"1.0\"?><Relationships "
        "xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" "
        "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" "
        "Target=\"worksheets/sheet1.xml\"/></Relationships>";
    mz_zip_archive zip;
    void *zbuf = NULL;
    size_t zsize = 0;
    FILE *fp = NULL;
    BOOL ok = FALSE;

    mz_zip_zero_struct(&zip);
    if (mz_zip_writer_init_heap(&zip, 0, 0) &&
        mz_zip_writer_add_mem(&zip,
                              "xl/workbook.xml",
                              k_workbook,
                              strlen(k_workbook),
                              MZ_DEFAULT_COMPRESSION) &&
        mz_zip_writer_add_mem(&zip,
                              "xl/_rels/workbook.xml.rels",
                              k_rels,
                              strlen(k_rels),
                              MZ_DEFAULT_COMPRESSION) &&
        mz_zip_writer_add_mem(&zip,
                              "xl/worksheets/sheet1.xml",
                              sheet_xml,
                              strlen(sheet_xml),
                              MZ_DEFAULT_COMPRESSION) &&
        mz_zip_writer_finalize_heap_archive(&zip, &zbuf, &zsize))
    {
        if (_wfopen_s(&fp, path, L"wb") == 0 && fp != NULL && fwrite(zbuf, 1, zsize, fp) == zsize)
        {
            ok = TRUE;
        }
        if (fp != NULL)
        {
            fclose(fp);
        }
    }
    if (zbuf != NULL)
    {
        mz_free(zbuf);
    }
    mz_zip_writer_end(&zip);
    return ok;
}

static BOOL cvr_temp_path(wchar_t *buf, size_t cch, const wchar_t *name)
{
    DWORD n = GetTempPathW((DWORD)cch, buf);
    return (n != 0 && n < cch && SUCCEEDED(StringCchCatW(buf, cch, name)));
}

/* CVR loader: identical-schema concatenation, sparse cells, mismatch rejection,
 * sort (tag: cvr). */
static int test_cvr(void)
{
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Ballot Style</t></is></c>"
        "<c r=\"D1\" t=\"inlineStr\"><is><t>Governor</t></is></c>"
        "<c r=\"E1\" t=\"inlineStr\"><is><t>Senator</t></is></c></row>";
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    /* File A: two ballots; CVR stored as a float (ES&S P26 style) -> "1"/"2".
     * One ballot has a blank Governor cell (D3 omitted). */
    static const char *k_a = "<row r=\"2\"><c r=\"A2\"><v>1.0</v></c>"
                             "<c r=\"B2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
                             "<c r=\"C2\" t=\"inlineStr\"><is><t>A</t></is></c>"
                             "<c r=\"D2\" t=\"inlineStr\"><is><t>Alice</t></is></c>"
                             "<c r=\"E2\" t=\"inlineStr\"><is><t>undervote</t></is></c></row>"
                             "<row r=\"3\"><c r=\"A3\"><v>2.0</v></c>"
                             "<c r=\"B3\" t=\"inlineStr\"><is><t>P2</t></is></c>"
                             "<c r=\"C3\" t=\"inlineStr\"><is><t>B</t></is></c>"
                             "<c r=\"E3\" t=\"inlineStr\"><is><t>Bob</t></is></c></row>";
    /* File B: same schema, one ballot. */
    static const char *k_b = "<row r=\"2\"><c r=\"A2\"><v>3.0</v></c>"
                             "<c r=\"B2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
                             "<c r=\"C2\" t=\"inlineStr\"><is><t>A</t></is></c>"
                             "<c r=\"D2\" t=\"inlineStr\"><is><t>Carol</t></is></c>"
                             "<c r=\"E2\" t=\"inlineStr\"><is><t>Dave</t></is></c></row>";

    wchar_t pa[MAX_PATH], pb[MAX_PATH], pc[MAX_PATH];
    wchar_t err[512];
    wchar_t buf[128];
    char sheet[4096];
    const wchar_t *pair[2];
    EeCvrTable t;
    EeLoadStatus s;
    int rc = 1;

    if (!cvr_temp_path(pa, ARRAYSIZE(pa), L"ee_cvr_a.xlsx") ||
        !cvr_temp_path(pb, ARRAYSIZE(pb), L"ee_cvr_b.xlsx") ||
        !cvr_temp_path(pc, ARRAYSIZE(pc), L"ee_cvr_c.xlsx"))
    {
        wprintf(L"cvr: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr, k_a);
    if (!cvr_write_xlsx(pa, sheet))
    {
        wprintf(L"cvr: write A failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr, k_b);
    if (!cvr_write_xlsx(pb, sheet))
    {
        wprintf(L"cvr: write B failed\n");
        return 1;
    }
    /* File C: different last header -> schema mismatch. */
    StringCchPrintfA(sheet,
                     ARRAYSIZE(sheet),
                     "%s<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is>"
                     "</c><c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
                     "<c r=\"C1\" t=\"inlineStr\"><is><t>Ballot Style</t></is></c>"
                     "<c r=\"D1\" t=\"inlineStr\"><is><t>Governor</t></is></c>"
                     "<c r=\"E1\" t=\"inlineStr\"><is><t>Attorney General</t></is></c></row>"
                     "<row r=\"2\"><c r=\"A2\"><v>9</v></c></row></sheetData></worksheet>",
                     k_head);
    if (!cvr_write_xlsx(pc, sheet))
    {
        wprintf(L"cvr: write C failed\n");
        return 1;
    }

    /* Same-schema concatenation. */
    EeCvr_Init(&t);
    pair[0] = pa;
    pair[1] = pb;
    s = EeCvr_LoadFromFiles(pair, 2, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.ncols != 5 || t.frozen_count != 3 || t.nrows != 3)
    {
        wprintf(L"cvr: concat load s=%d cols=%u frozen=%u rows=%u err=%s\n",
                (int)s,
                t.ncols,
                t.frozen_count,
                t.nrows,
                err);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 0, 0, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"1") != 0)
    {
        wprintf(L"cvr: row0 col0 (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 0, 4, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"undervote") != 0)
    {
        wprintf(L"cvr: row0 senator (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 1, 3, buf, ARRAYSIZE(buf)); /* blank Governor */
    if (buf[0] != L'\0')
    {
        wprintf(L"cvr: row1 governor not blank (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 2, 3, buf, ARRAYSIZE(buf)); /* from file B */
    if (wcscmp(buf, L"Carol") != 0)
    {
        wprintf(L"cvr: row2 governor (%s)\n", buf);
        goto done;
    }
    EeCvr_SortByColumn(&t, 0, FALSE); /* descending by Cast Vote Record */
    EeCvr_GetViewCellW(&t, 0, 0, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"3") != 0)
    {
        wprintf(L"cvr: desc sort top (%s)\n", buf);
        goto done;
    }
    EeCvr_Clear(&t);

    /* Mismatched schema must be rejected with no data. */
    EeCvr_Init(&t);
    pair[0] = pa;
    pair[1] = pc;
    s = EeCvr_LoadFromFiles(pair, 2, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Error || t.nrows != 0)
    {
        wprintf(L"cvr: mismatch not rejected s=%d rows=%u\n", (int)s, t.nrows);
        goto done;
    }
    rc = 0;
    wprintf(L"cvr ok\n");

done:
    EeCvr_Clear(&t);
    DeleteFileW(pa);
    DeleteFileW(pb);
    DeleteFileW(pc);
    if (rc != 0)
    {
        wprintf(L"cvr test failed\n");
    }
    return rc;
}

/* "Vote for N" contests: the first column is titled and the following BLANK-header
 * columns continue it. The continuation columns must be attributed to the contest
 * (derived title "<contest> (2)"/"(3)", col_group -> the title column), and their
 * per-selection data must load. A repeated identical title, by contrast, is a
 * distinct race and must NOT be merged (verified against official election results
 * for Travis County L26 -- see docs/cvr-design.md) (tag: cvrmulti). */
static int test_cvr_multiselect(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    /* Header: 3 key columns, then a vote-for-3 "City Council (100)" contest whose
     * 2nd/3rd columns have blank headers (explicit empty cells set the width), then
     * a vote-for-2 "School Board (200)" contest encoded as a repeated title. */
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Ballot Style</t></is></c>"
        "<c r=\"D1\" t=\"inlineStr\"><is><t>City Council (100)</t></is></c>"
        "<c r=\"E1\"/><c r=\"F1\"/>"
        "<c r=\"G1\" t=\"inlineStr\"><is><t>School Board (200)</t></is></c>"
        "<c r=\"H1\" t=\"inlineStr\"><is><t>School Board (200)</t></is></c></row>";
    /* One ballot: two picks then an undervote for the third allowed selection; the
     * two same-named School Board columns are separate single-winner races. */
    static const char *k_row =
        "<row r=\"2\"><c r=\"A2\"><v>1</v></c>"
        "<c r=\"B2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C2\" t=\"inlineStr\"><is><t>A</t></is></c>"
        "<c r=\"D2\" t=\"inlineStr\"><is><t>Alice (E1)</t></is></c>"
        "<c r=\"E2\" t=\"inlineStr\"><is><t>Bob (E2)</t></is></c>"
        "<c r=\"F2\" t=\"inlineStr\"><is><t>undervote</t></is></c>"
        "<c r=\"G2\" t=\"inlineStr\"><is><t>Carol (E3)</t></is></c>"
        "<c r=\"H2\" t=\"inlineStr\"><is><t>undervote</t></is></c></row>";

    wchar_t path[MAX_PATH];
    wchar_t err[512] = L"";
    wchar_t buf[128];
    char sheet[2048];
    const wchar_t *one[1];
    EeCvrTable t;
    EeLoadStatus s;
    int rc = 1;

    if (!cvr_temp_path(path, ARRAYSIZE(path), L"ee_cvr_multi.xlsx"))
    {
        wprintf(L"cvrmulti: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr, k_row);
    if (!cvr_write_xlsx(path, sheet))
    {
        wprintf(L"cvrmulti: write failed\n");
        return 1;
    }

    EeCvr_Init(&t);
    one[0] = path;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.ncols != 8 || t.frozen_count != 3 || t.nrows != 1)
    {
        wprintf(L"cvrmulti: load s=%d cols=%u frozen=%u rows=%u err=%s\n",
                (int)s,
                t.ncols,
                t.frozen_count,
                t.nrows,
                err);
        goto done;
    }
    /* Blank-header continuations are suffixed; the two same-named School Board
     * columns stay separate (each keeps the plain title). */
    if (wcscmp(t.col_titles[3], L"City Council (100)") != 0 ||
        wcscmp(t.col_titles[4], L"City Council (100) (2)") != 0 ||
        wcscmp(t.col_titles[5], L"City Council (100) (3)") != 0 ||
        wcscmp(t.col_titles[6], L"School Board (200)") != 0 ||
        wcscmp(t.col_titles[7], L"School Board (200)") != 0)
    {
        wprintf(L"cvrmulti: titles [3]=%s [4]=%s [5]=%s [6]=%s [7]=%s\n",
                t.col_titles[3],
                t.col_titles[4],
                t.col_titles[5],
                t.col_titles[6],
                t.col_titles[7]);
        goto done;
    }
    /* Grouping: blank continuations point at the title column; the repeated-title
     * columns are independent groups (7 -> 7, not 6). */
    if (t.col_group == NULL || t.col_group[3] != 3 || t.col_group[4] != 3 ||
        t.col_group[5] != 3 || t.col_group[6] != 6 || t.col_group[7] != 7 ||
        t.col_group[0] != 0 || t.col_group[2] != 2)
    {
        wprintf(L"cvrmulti: col_group mismatch\n");
        goto done;
    }
    /* Each selection loads in its own column. */
    EeCvr_GetViewCellW(&t, 0, 3, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Alice (E1)") != 0)
    {
        wprintf(L"cvrmulti: col3 (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 0, 4, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Bob (E2)") != 0)
    {
        wprintf(L"cvrmulti: col4 (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 0, 5, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"undervote") != 0)
    {
        wprintf(L"cvrmulti: col5 (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 0, 6, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Carol (E3)") != 0)
    {
        wprintf(L"cvrmulti: col6 (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 0, 7, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"undervote") != 0)
    {
        wprintf(L"cvrmulti: col7 (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"cvrmulti ok\n");

done:
    EeCvr_Clear(&t);
    DeleteFileW(path);
    if (rc != 0)
    {
        wprintf(L"cvrmulti test failed\n");
    }
    return rc;
}

/* Phase 2 tabulation: EeCvr_Tabulate counts each selection per contest, summing a
 * multi-column ("vote for N") contest across its columns. Within a contest,
 * candidates come first (count desc), then write-in, overvote, undervote — even when
 * a special outcome has a higher count than a candidate (tag: cvrtab). */
static int test_cvr_tabulate(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    /* 3 key columns; a vote-for-2 "Council (10)" contest = D + blank continuation E. */
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Ballot Style</t></is></c>"
        "<c r=\"D1\" t=\"inlineStr\"><is><t>Council (10)</t></is></c>"
        "<c r=\"E1\"/></row>";
/* Author one ballot row: CVR number @n, Council selections @d (col D) and @e (col E). */
#define CVRTAB_ROW(n, d, e)                                                                        \
    "<row r=\"" n "\"><c r=\"A" n "\"><v>" n "</v></c>"                                             \
    "<c r=\"B" n "\" t=\"inlineStr\"><is><t>P1</t></is></c>"                                        \
    "<c r=\"C" n "\" t=\"inlineStr\"><is><t>X</t></is></c>"                                         \
    "<c r=\"D" n "\" t=\"inlineStr\"><is><t>" d "</t></is></c>"                                     \
    "<c r=\"E" n "\" t=\"inlineStr\"><is><t>" e "</t></is></c></row>"
    /* Combined D+E tallies: Alice 6, Bob 4, [write-in] 1, overvote 2, undervote 3.
     * Note undervote/overvote out-count [write-in] yet must still list after it. */
    static const char *k_rows = CVRTAB_ROW("2", "Alice", "Bob")           /* Alice, Bob         */
        CVRTAB_ROW("3", "Alice", "[write-in]")                            /* Alice, write-in    */
        CVRTAB_ROW("4", "Alice", "undervote")                            /* Alice, undervote   */
        CVRTAB_ROW("5", "Bob", "overvote")                               /* Bob, overvote      */
        CVRTAB_ROW("6", "undervote", "undervote")                        /* undervote x2       */
        CVRTAB_ROW("7", "Alice", "Bob")                                  /* Alice, Bob         */
        CVRTAB_ROW("8", "Bob", "Alice")                                  /* Bob, Alice         */
        CVRTAB_ROW("9", "overvote", "Alice");                            /* overvote, Alice    */
#undef CVRTAB_ROW

    wchar_t path[MAX_PATH];
    wchar_t err[512] = L"";
    char sheet[4096];
    const wchar_t *one[1];
    EeCvrTable t;
    EeCvrTally *items = NULL;
    uint32_t count = 0;
    EeLoadStatus s;
    int rc = 1;

    if (!cvr_temp_path(path, ARRAYSIZE(path), L"ee_cvr_tab.xlsx"))
    {
        wprintf(L"cvrtab: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr,
                     k_rows);
    if (!cvr_write_xlsx(path, sheet))
    {
        wprintf(L"cvrtab: write failed\n");
        return 1;
    }

    EeCvr_Init(&t);
    one[0] = path;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok)
    {
        wprintf(L"cvrtab: load s=%d err=%s\n", (int)s, err);
        goto done;
    }
    if (!EeCvr_Tabulate(&t, FALSE, &items, &count)) /* keep write-in variants separate */
    {
        wprintf(L"cvrtab: tabulate failed\n");
        goto done;
    }
    /* Candidates first by count desc (Alice 6, Bob 4), then the specials in the
     * fixed order write-in, overvote, undervote — regardless of their counts. */
    if (count != 5)
    {
        wprintf(L"cvrtab: count=%u (want 5)\n", count);
        goto done;
    }
    {
        struct
        {
            const wchar_t *contest;
            const wchar_t *sel;
            uint32_t n;
        } want[5] = {
            {L"Council (10)", L"Alice", 6},
            {L"Council (10)", L"Bob", 4},
            {L"Council (10)", L"[write-in]", 1},
            {L"Council (10)", L"overvote", 2},
            {L"Council (10)", L"undervote", 3},
        };
        uint32_t i;
        for (i = 0; i < 5; i++)
        {
            if (wcscmp(items[i].contest, want[i].contest) != 0 ||
                wcscmp(items[i].selection, want[i].sel) != 0 || items[i].count != want[i].n)
            {
                wprintf(L"cvrtab: row %u = (%s | %s | %u), want (%s | %s | %u)\n",
                        i,
                        items[i].contest,
                        items[i].selection,
                        items[i].count,
                        want[i].contest,
                        want[i].sel,
                        want[i].n);
                goto done;
            }
        }
    }
    rc = 0;
    wprintf(L"cvrtab ok\n");

done:
    EeCvr_FreeTally(items, count);
    EeCvr_Clear(&t);
    DeleteFileW(path);
    if (rc != 0)
    {
        wprintf(L"cvrtab test failed\n");
    }
    return rc;
}

/* Write-in merge option: a contest with both the [write-in] image marker and a
 * literal "Write-in" text value tabulates as a single "write-in" row when merging is
 * on, or as separate rows when off (tag: cvrmerge). */
static int test_cvr_merge_writeins(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Mayor (10)</t></is></c></row>";
/* Mayor selections: Alice x1, [write-in] x2, Write-in x1, No image found x1,
 * undervote x1 -- three distinct write-in variants. */
#define CVRMRG_ROW(n, sel)                                                                         \
    "<row r=\"" n "\"><c r=\"A" n "\"><v>" n "</v></c>"                                             \
    "<c r=\"B" n "\" t=\"inlineStr\"><is><t>P1</t></is></c>"                                        \
    "<c r=\"C" n "\" t=\"inlineStr\"><is><t>" sel "</t></is></c></row>"
    static const char *k_rows =
        CVRMRG_ROW("2", "Alice") CVRMRG_ROW("3", "[write-in]") CVRMRG_ROW("4", "[write-in]")
            CVRMRG_ROW("5", "Write-in") CVRMRG_ROW("6", "No image found")
                CVRMRG_ROW("7", "undervote");
#undef CVRMRG_ROW

    wchar_t path[MAX_PATH];
    wchar_t err[512] = L"";
    char sheet[4096];
    const wchar_t *one[1];
    EeCvrTable t;
    EeCvrTally *items = NULL;
    uint32_t count = 0;
    EeLoadStatus s;
    int rc = 1;

    if (!cvr_temp_path(path, ARRAYSIZE(path), L"ee_cvr_merge.xlsx"))
    {
        wprintf(L"cvrmerge: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr,
                     k_rows);
    if (!cvr_write_xlsx(path, sheet))
    {
        wprintf(L"cvrmerge: write failed\n");
        return 1;
    }

    EeCvr_Init(&t);
    one[0] = path;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok)
    {
        wprintf(L"cvrmerge: load s=%d err=%s\n", (int)s, err);
        goto done;
    }

    /* Merge ON: Alice 1, write-in 4 ([write-in] 2 + Write-in 1 + No image found 1),
     * undervote 1. */
    if (!EeCvr_Tabulate(&t, TRUE, &items, &count) || count != 3)
    {
        wprintf(L"cvrmerge: merged count=%u (want 3)\n", count);
        goto done;
    }
    if (wcscmp(items[0].selection, L"Alice") != 0 || items[0].count != 1 ||
        wcscmp(items[1].selection, L"write-in") != 0 || items[1].count != 4 ||
        wcscmp(items[2].selection, L"undervote") != 0 || items[2].count != 1)
    {
        wprintf(L"cvrmerge: merged rows (%s=%u, %s=%u, %s=%u)\n",
                items[0].selection,
                items[0].count,
                items[1].selection,
                items[1].count,
                items[2].selection,
                items[2].count);
        goto done;
    }
    EeCvr_FreeTally(items, count);
    items = NULL;
    count = 0;

    /* Merge OFF: Alice 1, then the write-in variants by count desc then name
     * ([write-in] 2, then "No image found" 1 and "Write-in" 1 tie -> "No image
     * found" sorts first), then undervote 1. */
    if (!EeCvr_Tabulate(&t, FALSE, &items, &count) || count != 5)
    {
        wprintf(L"cvrmerge: unmerged count=%u (want 5)\n", count);
        goto done;
    }
    if (wcscmp(items[0].selection, L"Alice") != 0 ||
        wcscmp(items[1].selection, L"[write-in]") != 0 || items[1].count != 2 ||
        wcscmp(items[2].selection, L"No image found") != 0 || items[2].count != 1 ||
        wcscmp(items[3].selection, L"Write-in") != 0 || items[3].count != 1 ||
        wcscmp(items[4].selection, L"undervote") != 0)
    {
        wprintf(L"cvrmerge: unmerged rows (%s, %s=%u, %s=%u, %s=%u, %s)\n",
                items[0].selection,
                items[1].selection,
                items[1].count,
                items[2].selection,
                items[2].count,
                items[3].selection,
                items[3].count,
                items[4].selection);
        goto done;
    }
    rc = 0;
    wprintf(L"cvrmerge ok\n");

done:
    EeCvr_FreeTally(items, count);
    EeCvr_Clear(&t);
    DeleteFileW(path);
    if (rc != 0)
    {
        wprintf(L"cvrmerge test failed\n");
    }
    return rc;
}

/* Multi-card detection. Three cases:
 *  A) a long ballot style split across two cards (a continuation row blank in the
 *     top contest, which is on every style's first page) -> flagged;
 *  B) a clean one-row-per-ballot CVR -> not flagged;
 *  C) a combined-party primary where the leading contest is blank on the other
 *     party's (full) ballots -> not flagged (that blank fraction isn't a card).
 * (tag: cvrmc) */
static int test_cvr_multicard(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Ballot Style</t></is></c>"
        "<c r=\"D1\" t=\"inlineStr\"><is><t>President</t></is></c>"
        "<c r=\"E1\" t=\"inlineStr\"><is><t>Judge</t></is></c></row>";
    /* Cells: President (col D, a reference contest) and Judge (col E, down-ballot),
     * each present or omitted (blank). */
#define MCP "<c r=\"D%d\" t=\"inlineStr\"><is><t>P</t></is></c>"
#define MCJ "<c r=\"E%d\" t=\"inlineStr\"><is><t>J</t></is></c>"

    wchar_t patha[MAX_PATH], pathb[MAX_PATH], pathc[MAX_PATH];
    wchar_t err[512] = L"";
    char sheet[8192];
    char row[512];
    const wchar_t *one[1];
    EeCvrTable t;
    EeLoadStatus s;
    int rc = 1;
    int i;
    size_t used;

    if (!cvr_temp_path(patha, ARRAYSIZE(patha), L"ee_cvr_mc_a.xlsx") ||
        !cvr_temp_path(pathb, ARRAYSIZE(pathb), L"ee_cvr_mc_b.xlsx") ||
        !cvr_temp_path(pathc, ARRAYSIZE(pathc), L"ee_cvr_mc_c.xlsx"))
    {
        wprintf(L"cvrmc: temp path failed\n");
        return 1;
    }

    /* File A (multi-card): 5 short-ballot rows (President+Judge on one card), then a
     * long ballot split into a page-1 row (President, Judge blank) and a page-2 row
     * (President blank, Judge). President filled 6/7, Judge 6/7 -> max 85.7%, blank
     * one row -> flagged. */
    StringCchCopyA(sheet, ARRAYSIZE(sheet), k_head);
    StringCchCatA(sheet, ARRAYSIZE(sheet), k_hdr);
    for (i = 2; i <= 6; i++) /* short ballots */
    {
        StringCchPrintfA(row, ARRAYSIZE(row),
                         "<row r=\"%d\"><c r=\"A%d\"><v>%d</v></c>"
                         "<c r=\"B%d\" t=\"inlineStr\"><is><t>P1</t></is></c>"
                         "<c r=\"C%d\" t=\"inlineStr\"><is><t>S1</t></is></c>" MCP MCJ "</row>",
                         i, i, i, i, i, i, i);
        StringCchCatA(sheet, ARRAYSIZE(sheet), row);
    }
    /* long-ballot page 1: President, Judge omitted */
    StringCchPrintfA(row, ARRAYSIZE(row),
                     "<row r=\"7\"><c r=\"A7\"><v>7</v></c>"
                     "<c r=\"B7\" t=\"inlineStr\"><is><t>P1</t></is></c>"
                     "<c r=\"C7\" t=\"inlineStr\"><is><t>S2</t></is></c>" MCP "</row>", 7);
    StringCchCatA(sheet, ARRAYSIZE(sheet), row);
    /* long-ballot page 2: President omitted, Judge present */
    StringCchPrintfA(row, ARRAYSIZE(row),
                     "<row r=\"8\"><c r=\"A8\"><v>8</v></c>"
                     "<c r=\"B8\" t=\"inlineStr\"><is><t>P1</t></is></c>"
                     "<c r=\"C8\" t=\"inlineStr\"><is><t>S2 [2]</t></is></c>" MCJ "</row>", 8);
    StringCchCatA(sheet, ARRAYSIZE(sheet), row);
    StringCchCatA(sheet, ARRAYSIZE(sheet), "</sheetData></worksheet>");
    if (!cvr_write_xlsx(patha, sheet))
    {
        wprintf(L"cvrmc: write A failed\n");
        return 1;
    }

    /* File B (clean): every row has President + Judge -> no contest blank -> not flagged. */
    StringCchCopyA(sheet, ARRAYSIZE(sheet), k_head);
    StringCchCatA(sheet, ARRAYSIZE(sheet), k_hdr);
    for (i = 2; i <= 6; i++)
    {
        StringCchPrintfA(row, ARRAYSIZE(row),
                         "<row r=\"%d\"><c r=\"A%d\"><v>%d</v></c>"
                         "<c r=\"B%d\" t=\"inlineStr\"><is><t>P1</t></is></c>"
                         "<c r=\"C%d\" t=\"inlineStr\"><is><t>S1</t></is></c>" MCP MCJ "</row>",
                         i, i, i, i, i, i, i);
        StringCchCatA(sheet, ARRAYSIZE(sheet), row);
    }
    StringCchCatA(sheet, ARRAYSIZE(sheet), "</sheetData></worksheet>");
    if (!cvr_write_xlsx(pathb, sheet))
    {
        wprintf(L"cvrmc: write B failed\n");
        return 1;
    }

    /* File C (combined primary): each party ballot carries its OWN top race, both
     * reference contests ("Dem President"/"Rep President"). 6 DEM rows fill col D, 4
     * REP rows fill col E. Every row has a reference contest -> extra == 0 -> not
     * flagged, even though each party's race is blank on the other party's ballots. */
    {
        static const char *k_hdr_c =
            "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
            "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
            "<c r=\"C1\" t=\"inlineStr\"><is><t>Ballot Style</t></is></c>"
            "<c r=\"D1\" t=\"inlineStr\"><is><t>Dem President</t></is></c>"
            "<c r=\"E1\" t=\"inlineStr\"><is><t>Rep President</t></is></c></row>";
        StringCchCopyA(sheet, ARRAYSIZE(sheet), k_head);
        StringCchCatA(sheet, ARRAYSIZE(sheet), k_hdr_c);
        for (i = 2; i <= 11; i++)
        {
            char cell[128];
            StringCchPrintfA(cell, ARRAYSIZE(cell), (i <= 7) ? MCP : MCJ, i); /* 6 DEM, 4 REP */
            StringCchPrintfA(row, ARRAYSIZE(row),
                             "<row r=\"%d\"><c r=\"A%d\"><v>%d</v></c>"
                             "<c r=\"B%d\" t=\"inlineStr\"><is><t>P1</t></is></c>"
                             "<c r=\"C%d\" t=\"inlineStr\"><is><t>S1</t></is></c>%s</row>",
                             i, i, i, i, i, cell);
            StringCchCatA(sheet, ARRAYSIZE(sheet), row);
        }
        StringCchCatA(sheet, ARRAYSIZE(sheet), "</sheetData></worksheet>");
    }
    used = strlen(sheet);
    (void)used;
    if (!cvr_write_xlsx(pathc, sheet))
    {
        wprintf(L"cvrmc: write C failed\n");
        return 1;
    }
#undef MCP
#undef MCJ

    /* A -> flagged; B -> not; C -> not. */
    EeCvr_Init(&t);
    one[0] = patha;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || !EeCvr_HasMultiCard(&t))
    {
        wprintf(L"cvrmc: multi-card file not detected (s=%d rows=%u)\n", (int)s, t.nrows);
        EeCvr_Clear(&t);
        goto done;
    }
    EeCvr_Clear(&t);

    EeCvr_Init(&t);
    one[0] = pathb;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || EeCvr_HasMultiCard(&t))
    {
        wprintf(L"cvrmc: clean file wrongly flagged (s=%d)\n", (int)s);
        EeCvr_Clear(&t);
        goto done;
    }
    EeCvr_Clear(&t);

    EeCvr_Init(&t);
    one[0] = pathc;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || EeCvr_HasMultiCard(&t))
    {
        wprintf(L"cvrmc: combined-primary file wrongly flagged (s=%d)\n", (int)s);
        EeCvr_Clear(&t);
        goto done;
    }
    EeCvr_Clear(&t);
    rc = 0;
    wprintf(L"cvrmc ok\n");

done:
    DeleteFileW(patha);
    DeleteFileW(pathb);
    DeleteFileW(pathc);
    if (rc != 0)
    {
        wprintf(L"cvrmc test failed\n");
    }
    return rc;
}

/* CVR filter primitives: EeCvr_CollectColumnValues returns a column's distinct
 * selections (sorted, blanks excluded) for the value dropdown, and EeCvr_GetCellW
 * reads a physical-row cell (tag: cvrfilt). */
static int test_cvr_filter_values(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Mayor (10)</t></is></c>"
        "<c r=\"D1\" t=\"inlineStr\"><is><t>Council (11)</t></is></c></row>";
    /* Mayor: Bob, Alice, undervote, Alice, (blank). Distinct = Alice, Bob, undervote.
     * A second contest (Council) is filled on every ballot so the last ballot, which
     * is blank in Mayor, is still a real ballot (a row with no contest at all is
     * dropped as export padding), letting us exercise a genuine blank contest cell. */
    static const char *k_rows =
        "<row r=\"2\"><c r=\"A2\"><v>1</v></c>"
        "<c r=\"B2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C2\" t=\"inlineStr\"><is><t>Bob</t></is></c>"
        "<c r=\"D2\" t=\"inlineStr\"><is><t>Yes</t></is></c></row>"
        "<row r=\"3\"><c r=\"A3\"><v>2</v></c>"
        "<c r=\"B3\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C3\" t=\"inlineStr\"><is><t>Alice</t></is></c>"
        "<c r=\"D3\" t=\"inlineStr\"><is><t>Yes</t></is></c></row>"
        "<row r=\"4\"><c r=\"A4\"><v>3</v></c>"
        "<c r=\"B4\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C4\" t=\"inlineStr\"><is><t>undervote</t></is></c>"
        "<c r=\"D4\" t=\"inlineStr\"><is><t>No</t></is></c></row>"
        "<row r=\"5\"><c r=\"A5\"><v>4</v></c>"
        "<c r=\"B5\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C5\" t=\"inlineStr\"><is><t>Alice</t></is></c>"
        "<c r=\"D5\" t=\"inlineStr\"><is><t>Yes</t></is></c></row>"
        "<row r=\"6\"><c r=\"A6\"><v>5</v></c>"
        "<c r=\"B6\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"D6\" t=\"inlineStr\"><is><t>No</t></is></c></row>";

    wchar_t path[MAX_PATH];
    wchar_t err[512] = L"";
    wchar_t buf[128];
    char sheet[4096];
    const wchar_t *one[1];
    EeCvrTable t;
    EeLoadStatus s;
    wchar_t **vals = NULL;
    uint32_t n = 0;
    uint32_t r;
    int rc = 1;

    if (!cvr_temp_path(path, ARRAYSIZE(path), L"ee_cvr_filt.xlsx"))
    {
        wprintf(L"cvrfilt: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr,
                     k_rows);
    if (!cvr_write_xlsx(path, sheet))
    {
        wprintf(L"cvrfilt: write failed\n");
        return 1;
    }
    EeCvr_Init(&t);
    one[0] = path;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 5)
    {
        wprintf(L"cvrfilt: load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    if (!EeCvr_CollectColumnValues(&t, 2, 100, &vals, &n) || n != 3)
    {
        wprintf(L"cvrfilt: distinct count=%u (want 3)\n", n);
        goto done;
    }
    if (wcscmp(vals[0], L"Alice") != 0 || wcscmp(vals[1], L"Bob") != 0 ||
        wcscmp(vals[2], L"undervote") != 0)
    {
        wprintf(L"cvrfilt: distinct order (%s, %s, %s)\n", vals[0], vals[1], vals[2]);
        goto done;
    }
    /* EeCvr_GetCellW reads by physical row; find the blank Mayor cell (5th ballot). */
    {
        BOOL saw_blank = FALSE;
        for (r = 0; r < t.nrows; r++)
        {
            EeCvr_GetCellW(&t, r, 2, buf, ARRAYSIZE(buf));
            if (buf[0] == L'\0')
            {
                saw_blank = TRUE;
            }
        }
        if (!saw_blank)
        {
            wprintf(L"cvrfilt: expected a blank Mayor cell\n");
            goto done;
        }
    }
    rc = 0;
    wprintf(L"cvrfilt ok\n");

done:
    if (vals != NULL)
    {
        for (r = 0; r < n; r++)
        {
            free(vals[r]);
        }
        free(vals);
    }
    EeCvr_Clear(&t);
    DeleteFileW(path);
    if (rc != 0)
    {
        wprintf(L"cvrfilt test failed\n");
    }
    return rc;
}

/* Write raw bytes to a file (returns TRUE on success). */
static BOOL cvr_write_bytes(const wchar_t *path, const void *data, size_t len)
{
    HANDLE h = CreateFileW(path, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL, NULL);
    DWORD wrote = 0;
    BOOL ok;
    if (h == INVALID_HANDLE_VALUE)
    {
        return FALSE;
    }
    ok = WriteFile(h, data, (DWORD)len, &wrote, NULL) && wrote == (DWORD)len;
    CloseHandle(h);
    return ok;
}

/* CSV / TSV loading: the CVR loader accepts delimited-text exports as well as
 * .xlsx. Covers RFC-4180 quoting (a contest name and a selection each carrying a
 * comma), a sparse blank contest cell, tab-delimited .tsv, a UTF-16LE (BOM)
 * export, and concatenating a .csv with a .tsv of identical schema (tag: cvrcsv). */
static int test_cvr_delimited(void)
{
    /* Header quotes a contest name that contains a comma; row 2 quotes a value that
     * contains a comma; row 3 is blank in Mayor. A second contest (Prop A) is filled
     * on every ballot so the Mayor-blank ballot is still a real ballot -- a row with
     * no contest selection at all is dropped as export padding. */
    static const char *k_csv =
        "Cast Vote Record,Precinct,\"Mayor, City of X (10)\",Prop A (11)\r\n"
        "1,P1,Alice,Yes\r\n"
        "2,P1,\"Bob, Jr.\",Yes\r\n"
        "3,P1,,No\r\n"
        /* Export artifacts that must be dropped (no contest selection): a lone
         * Cast-Vote-Record-id line (Excel occasionally breaks a record with a
         * spurious newline after the first field) and the trailing all-empty line
         * Excel appends when saving a sheet as CSV. Neither is a countable ballot. */
        "98\r\n"
        ",,,\r\n";
    /* Same schema, tab-delimited, two more ballots (no quoting needed). */
    static const char *k_tsv =
        "Cast Vote Record\tPrecinct\tMayor, City of X (10)\tProp A (11)\n"
        "4\tP2\tAlice\tYes\n"
        "5\tP2\tundervote\tNo\n";

    wchar_t pcsv[MAX_PATH];
    wchar_t ptsv[MAX_PATH];
    wchar_t pu16[MAX_PATH];
    wchar_t err[512] = L"";
    wchar_t buf[128];
    const wchar_t *pair[2];
    const wchar_t *one[1];
    EeCvrTable t;
    EeLoadStatus s;
    uint32_t r;
    int rc = 1;
    BOOL saw_bobjr = FALSE, saw_blank = FALSE;

    if (!cvr_temp_path(pcsv, ARRAYSIZE(pcsv), L"ee_cvr_d.csv") ||
        !cvr_temp_path(ptsv, ARRAYSIZE(ptsv), L"ee_cvr_d.tsv") ||
        !cvr_temp_path(pu16, ARRAYSIZE(pu16), L"ee_cvr_u16.csv"))
    {
        wprintf(L"cvrcsv: temp path failed\n");
        return 1;
    }
    if (!cvr_write_bytes(pcsv, k_csv, strlen(k_csv)) ||
        !cvr_write_bytes(ptsv, k_tsv, strlen(k_tsv)))
    {
        wprintf(L"cvrcsv: write failed\n");
        return 1;
    }

    /* --- CSV + TSV concatenation (identical schema) --- */
    EeCvr_Init(&t);
    pair[0] = pcsv;
    pair[1] = ptsv;
    s = EeCvr_LoadFromFiles(pair, 2, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.ncols != 4 || t.nrows != 5)
    {
        wprintf(L"cvrcsv: load s=%d cols=%u rows=%u err=%s\n", (int)s, t.ncols, t.nrows,
                err);
        goto done;
    }
    if (wcscmp(t.col_titles[2], L"Mayor, City of X (10)") != 0)
    {
        wprintf(L"cvrcsv: quoted header parsed as \"%s\"\n", t.col_titles[2]);
        goto done;
    }
    for (r = 0; r < t.nrows; r++)
    {
        EeCvr_GetCellW(&t, r, 2, buf, ARRAYSIZE(buf));
        if (wcscmp(buf, L"Bob, Jr.") == 0)
        {
            saw_bobjr = TRUE; /* quoted value with an embedded comma survived */
        }
        if (buf[0] == L'\0')
        {
            saw_blank = TRUE; /* blank Mayor cell stayed sparse */
        }
    }
    if (!saw_bobjr || !saw_blank)
    {
        wprintf(L"cvrcsv: quoted-comma=%d blank=%d\n", saw_bobjr, saw_blank);
        goto done;
    }
    EeCvr_Clear(&t);

    /* --- UTF-16LE (BOM) export decodes correctly --- */
    {
        /* Build a UTF-16LE byte image with a BOM from a wide literal. */
        static const wchar_t k_w[] =
            L"Cast Vote Record,Precinct,Mayor\r\n"
            L"1,P1,Ren\x00e9\r\n"; /* René exercises non-ASCII transcoding */
        size_t nwch = ARRAYSIZE(k_w) - 1; /* drop the terminating NUL */
        size_t nbytes = 2 + nwch * 2;
        unsigned char *img = (unsigned char *)malloc(nbytes);
        BOOL wrote_ok;
        if (img == NULL)
        {
            wprintf(L"cvrcsv: oom\n");
            goto done;
        }
        img[0] = 0xFF;
        img[1] = 0xFE;
        memcpy(img + 2, k_w, nwch * 2);
        wrote_ok = cvr_write_bytes(pu16, img, nbytes);
        free(img);
        if (!wrote_ok)
        {
            wprintf(L"cvrcsv: u16 write failed\n");
            goto done;
        }
    }
    EeCvr_Init(&t);
    one[0] = pu16;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.ncols != 3 || t.nrows != 1)
    {
        wprintf(L"cvrcsv: u16 load s=%d cols=%u rows=%u err=%s\n", (int)s, t.ncols,
                t.nrows, err);
        goto done;
    }
    EeCvr_GetCellW(&t, 0, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Ren\x00e9") != 0)
    {
        wprintf(L"cvrcsv: u16 value \"%s\"\n", buf);
        goto done;
    }

    rc = 0;
    wprintf(L"cvrcsv ok\n");

done:
    EeCvr_Clear(&t);
    DeleteFileW(pcsv);
    DeleteFileW(ptsv);
    DeleteFileW(pu16);
    if (rc != 0)
    {
        wprintf(L"cvrcsv test failed\n");
    }
    return rc;
}

/* Per-column value reports: EeCvr_FindColumnByTitle locates a key column by header,
 * EeCvr_ColumnHasReportableData is FALSE for an all-redacted column, and
 * EeCvr_CollectColumnCounts returns per-value ballot-record counts + a blank tally
 * (tag: cvrcnt). */
static int test_cvr_colcounts(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    /* A=Cast Vote Record, B=Batch, C=Precinct, D=Ballot Style (all key), E=Mayor. */
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Batch</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"D1\" t=\"inlineStr\"><is><t>Ballot Style</t></is></c>"
        "<c r=\"E1\" t=\"inlineStr\"><is><t>Mayor (10)</t></is></c></row>";
    /* Batch is entirely redacted (mixed spellings). Precinct: P1 x3, P2 x1, blank x1.
     * Every row has a Mayor selection so none is dropped as an empty ballot. */
    static const char *k_rows =
        "<row r=\"2\"><c r=\"A2\"><v>1</v></c>"
        "<c r=\"B2\" t=\"inlineStr\"><is><t>&lt;Redacted&gt;</t></is></c>"
        "<c r=\"C2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"D2\" t=\"inlineStr\"><is><t>S1</t></is></c>"
        "<c r=\"E2\" t=\"inlineStr\"><is><t>Alice</t></is></c></row>"
        "<row r=\"3\"><c r=\"A3\"><v>2</v></c>"
        "<c r=\"B3\" t=\"inlineStr\"><is><t>&lt;REDACTED&gt;</t></is></c>"
        "<c r=\"C3\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"D3\" t=\"inlineStr\"><is><t>S1</t></is></c>"
        "<c r=\"E3\" t=\"inlineStr\"><is><t>Bob</t></is></c></row>"
        "<row r=\"4\"><c r=\"A4\"><v>3</v></c>"
        "<c r=\"B4\" t=\"inlineStr\"><is><t>&lt;Redact&gt;</t></is></c>"
        "<c r=\"C4\" t=\"inlineStr\"><is><t>P2</t></is></c>"
        "<c r=\"D4\" t=\"inlineStr\"><is><t>S2</t></is></c>"
        "<c r=\"E4\" t=\"inlineStr\"><is><t>Alice</t></is></c></row>"
        "<row r=\"5\"><c r=\"A5\"><v>4</v></c>"
        "<c r=\"B5\" t=\"inlineStr\"><is><t>&lt;Redacted&gt;</t></is></c>"
        "<c r=\"D5\" t=\"inlineStr\"><is><t>S2</t></is></c>"
        "<c r=\"E5\" t=\"inlineStr\"><is><t>Bob</t></is></c></row>"
        "<row r=\"6\"><c r=\"A6\"><v>5</v></c>"
        "<c r=\"B6\" t=\"inlineStr\"><is><t>&lt;Redacted&gt;</t></is></c>"
        "<c r=\"C6\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"D6\" t=\"inlineStr\"><is><t>S1</t></is></c>"
        "<c r=\"E6\" t=\"inlineStr\"><is><t>Alice</t></is></c></row>";

    wchar_t path[MAX_PATH];
    wchar_t err[512] = L"";
    char sheet[6144];
    const wchar_t *one[1];
    EeCvrTable t;
    EeLoadStatus s;
    EeCvrValueCount *items = NULL;
    uint32_t count = 0;
    uint32_t blank = 0;
    uint32_t colBatch = 99, colPrec = 99, colStyle = 99, colBogus = 99;
    uint32_t p1 = 0, p2 = 0;
    uint32_t i;
    int rc = 1;

    if (!cvr_temp_path(path, ARRAYSIZE(path), L"ee_cvr_cnt.xlsx"))
    {
        wprintf(L"cvrcnt: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr,
                     k_rows);
    if (!cvr_write_xlsx(path, sheet))
    {
        wprintf(L"cvrcnt: write failed\n");
        return 1;
    }
    EeCvr_Init(&t);
    one[0] = path;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 5)
    {
        wprintf(L"cvrcnt: load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    if (!EeCvr_FindColumnByTitle(&t, L"Batch", &colBatch) ||
        !EeCvr_FindColumnByTitle(&t, L"Precinct", &colPrec) ||
        !EeCvr_FindColumnByTitle(&t, L"Ballot Style", &colStyle) ||
        colBatch != 1 || colPrec != 2 || colStyle != 3)
    {
        wprintf(L"cvrcnt: find columns batch=%u prec=%u style=%u\n", colBatch, colPrec, colStyle);
        goto done;
    }
    if (EeCvr_FindColumnByTitle(&t, L"Nonexistent", &colBogus))
    {
        wprintf(L"cvrcnt: found a nonexistent column\n");
        goto done;
    }
    /* Redacted Batch is not reportable; Precinct and Ballot Style are. */
    if (EeCvr_ColumnHasReportableData(&t, colBatch) ||
        !EeCvr_ColumnHasReportableData(&t, colPrec) ||
        !EeCvr_ColumnHasReportableData(&t, colStyle))
    {
        wprintf(L"cvrcnt: reportable batch=%d prec=%d style=%d\n",
                EeCvr_ColumnHasReportableData(&t, colBatch),
                EeCvr_ColumnHasReportableData(&t, colPrec),
                EeCvr_ColumnHasReportableData(&t, colStyle));
        goto done;
    }
    if (!EeCvr_CollectColumnCounts(&t, colPrec, &items, &count, &blank) || count != 2 || blank != 1)
    {
        wprintf(L"cvrcnt: precinct count=%u blank=%u (want 2,1)\n", count, blank);
        goto done;
    }
    for (i = 0; i < count; i++)
    {
        if (wcscmp(items[i].value, L"P1") == 0)
        {
            p1 = items[i].count;
        }
        else if (wcscmp(items[i].value, L"P2") == 0)
        {
            p2 = items[i].count;
        }
    }
    if (p1 != 3 || p2 != 1)
    {
        wprintf(L"cvrcnt: precinct P1=%u P2=%u (want 3,1)\n", p1, p2);
        goto done;
    }
    rc = 0;
    wprintf(L"cvrcnt ok\n");

done:
    EeCvr_FreeColumnCounts(items, count);
    EeCvr_Clear(&t);
    DeleteFileW(path);
    if (rc != 0)
    {
        wprintf(L"cvrcnt test failed\n");
    }
    return rc;
}

/* Whitespace normalization: CVR selection values with stray internal spacing or
 * leading/trailing spaces are normalized at load, so the grid shows them cleanly and
 * equivalent selections share one tally (tag: cvrws). */
static int test_cvr_whitespace(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Senate (10)</t></is></c></row>";
    /* Ballot 1: "John   Cornyn" (3 spaces); ballot 2: "John Cornyn" (1 space) -> must
     * merge. Ballot 3: "  Jane Doe  " (leading/trailing) -> trimmed. */
    static const char *k_rows =
        "<row r=\"2\"><c r=\"A2\"><v>1</v></c>"
        "<c r=\"B2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C2\" t=\"inlineStr\"><is><t>John   Cornyn</t></is></c></row>"
        "<row r=\"3\"><c r=\"A3\"><v>2</v></c>"
        "<c r=\"B3\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C3\" t=\"inlineStr\"><is><t>John Cornyn</t></is></c></row>"
        "<row r=\"4\"><c r=\"A4\"><v>3</v></c>"
        "<c r=\"B4\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C4\" t=\"inlineStr\"><is><t>  Jane Doe  </t></is></c></row>";

    wchar_t path[MAX_PATH];
    wchar_t err[512] = L"";
    wchar_t buf[128];
    char sheet[4096];
    const wchar_t *one[1];
    EeCvrTable t;
    EeCvrTally *items = NULL;
    uint32_t count = 0;
    EeLoadStatus s;
    int rc = 1;

    if (!cvr_temp_path(path, ARRAYSIZE(path), L"ee_cvr_ws.xlsx"))
    {
        wprintf(L"cvrws: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr,
                     k_rows);
    if (!cvr_write_xlsx(path, sheet))
    {
        wprintf(L"cvrws: write failed\n");
        return 1;
    }

    EeCvr_Init(&t);
    one[0] = path;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 3)
    {
        wprintf(L"cvrws: load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    /* Both spellings collapse to "John Cornyn"; leading/trailing trimmed. */
    EeCvr_GetViewCellW(&t, 0, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"John Cornyn") != 0)
    {
        wprintf(L"cvrws: row0 (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 1, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"John Cornyn") != 0)
    {
        wprintf(L"cvrws: row1 (%s)\n", buf);
        goto done;
    }
    EeCvr_GetViewCellW(&t, 2, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Jane Doe") != 0)
    {
        wprintf(L"cvrws: row2 (%s)\n", buf);
        goto done;
    }
    /* The two Cornyn spellings tabulate as a single selection with count 2. */
    if (!EeCvr_Tabulate(&t, FALSE, &items, &count) || count != 2)
    {
        wprintf(L"cvrws: tabulate count=%u (want 2)\n", count);
        goto done;
    }
    if (wcscmp(items[0].selection, L"John Cornyn") != 0 || items[0].count != 2 ||
        wcscmp(items[1].selection, L"Jane Doe") != 0 || items[1].count != 1)
    {
        wprintf(L"cvrws: tally (%s=%u, %s=%u)\n",
                items[0].selection,
                items[0].count,
                items[1].selection,
                items[1].count);
        goto done;
    }
    rc = 0;
    wprintf(L"cvrws ok\n");

done:
    EeCvr_FreeTally(items, count);
    EeCvr_Clear(&t);
    DeleteFileW(path);
    if (rc != 0)
    {
        wprintf(L"cvrws test failed\n");
    }
    return rc;
}

/* Write-in detection: a worksheet whose drawing anchors a picture onto an
 * otherwise-empty contest cell (ES&S write-in) must surface as "[write-in]",
 * while an ordinary selection is untouched (tag: writein). */
static int test_xlsx_writein(void)
{
    static const char *k_workbook =
        "<?xml version=\"1.0\"?><workbook "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
        "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
        "<sheets><sheet name=\"CVR\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>";
    static const char *k_rels =
        "<?xml version=\"1.0\"?><Relationships "
        "xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" "
        "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" "
        "Target=\"worksheets/sheet1.xml\"/></Relationships>";
    /* Header + two ballots. Row 2 (data row 0) picks "Smith" for President;
     * row 3 (data row 1) leaves President (col C, 0-based 2) blank -- a write-in
     * image is anchored there. */
    static const char *k_sheet =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
        "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><sheetData>"
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>President</t></is></c></row>"
        "<row r=\"2\"><c r=\"A2\"><v>1</v></c>"
        "<c r=\"B2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C2\" t=\"inlineStr\"><is><t>Smith</t></is></c></row>"
        "<row r=\"3\"><c r=\"A3\"><v>2</v></c>"
        "<c r=\"B3\" t=\"inlineStr\"><is><t>P2</t></is></c></row>"
        "</sheetData><drawing r:id=\"rId1\"/></worksheet>";
    static const char *k_sheet_rels =
        "<?xml version=\"1.0\"?><Relationships "
        "xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" "
        "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing\" "
        "Target=\"../drawings/drawing1.xml\"/></Relationships>";
    /* One anchor at (col 2, row 2) 0-based == cell C3 == data row 1, President. */
    static const char *k_drawing =
        "<?xml version=\"1.0\"?><xdr:wsDr "
        "xmlns:xdr=\"http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing\" "
        "xmlns:a=\"http://schemas.openxmlformats.org/drawingml/2006/main\">"
        "<xdr:twoCellAnchor editAs=\"oneCell\">"
        "<xdr:from><xdr:col>2</xdr:col><xdr:colOff>0</xdr:colOff>"
        "<xdr:row>2</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:from>"
        "<xdr:to><xdr:col>3</xdr:col><xdr:colOff>0</xdr:colOff>"
        "<xdr:row>3</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:to>"
        "<xdr:pic><xdr:nvPicPr/><xdr:blipFill><a:blip r:embed=\"rId1\"/></xdr:blipFill>"
        "</xdr:pic><xdr:clientData/></xdr:twoCellAnchor></xdr:wsDr>";

    wchar_t path[MAX_PATH];
    wchar_t err[512] = L"";
    wchar_t buf[128];
    FILE *fp = NULL;
    mz_zip_archive zip;
    void *zbuf = NULL;
    size_t zsize = 0;
    EeCvrTable t;
    EeLoadStatus s;
    const wchar_t *one[1];
    int rc = 1;

    mz_zip_zero_struct(&zip);
    if (!mz_zip_writer_init_heap(&zip, 0, 0) ||
        !mz_zip_writer_add_mem(&zip, "xl/workbook.xml", k_workbook, strlen(k_workbook),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip, "xl/_rels/workbook.xml.rels", k_rels, strlen(k_rels),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip, "xl/worksheets/sheet1.xml", k_sheet, strlen(k_sheet),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip, "xl/worksheets/_rels/sheet1.xml.rels", k_sheet_rels,
                               strlen(k_sheet_rels), MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_add_mem(&zip, "xl/drawings/drawing1.xml", k_drawing, strlen(k_drawing),
                               MZ_DEFAULT_COMPRESSION) ||
        !mz_zip_writer_finalize_heap_archive(&zip, &zbuf, &zsize))
    {
        wprintf(L"writein: failed to author test workbook\n");
        mz_zip_writer_end(&zip);
        return 1;
    }
    if (!cvr_temp_path(path, ARRAYSIZE(path), L"ee_writein.xlsx") ||
        _wfopen_s(&fp, path, L"wb") != 0 || fp == NULL || fwrite(zbuf, 1, zsize, fp) != zsize)
    {
        wprintf(L"writein: could not write temp file\n");
        if (fp != NULL)
            fclose(fp);
        mz_free(zbuf);
        mz_zip_writer_end(&zip);
        return 1;
    }
    fclose(fp);
    mz_free(zbuf);
    mz_zip_writer_end(&zip);

    EeCvr_Init(&t);
    one[0] = path;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.ncols != 3 || t.nrows != 2)
    {
        wprintf(L"writein: load s=%d cols=%u rows=%u err=%s\n", (int)s, t.ncols, t.nrows, err);
        goto done;
    }
    /* Data row 0 (CVR 1): ordinary selection preserved. */
    EeCvr_GetViewCellW(&t, 0, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Smith") != 0)
    {
        wprintf(L"writein: row0 President (%s)\n", buf);
        goto done;
    }
    /* Data row 1 (CVR 2): blank President cell carries a write-in image. */
    EeCvr_GetViewCellW(&t, 1, 2, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"[write-in]") != 0)
    {
        wprintf(L"writein: row1 President (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"writein ok\n");

done:
    EeCvr_Clear(&t);
    DeleteFileW(path);
    if (rc != 0)
    {
        wprintf(L"writein test failed\n");
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
    failed |= test_district_codes_not_appended();
    failed |= test_res_addr_zip_dash_and_unit();
    failed |= test_house_number_dot_zero();
    failed |= test_lot_unit_ignored();
    failed |= test_filter_logic();
    failed |= test_empty_numeric_header();
    failed |= test_date_sort();
    failed |= test_duplicate_voter_ids();
    failed |= test_duplicate_voters();
    failed |= test_mark_duplicates();
    failed |= test_value_counts();
    failed |= test_precinct_normalize();
    failed |= test_partial_birthdate();
    failed |= test_name_last_first_no_address();
    failed |= test_compare();
    failed |= test_compare_formatting();
    failed |= test_compare_missing_state();
    failed |= test_compare_zip4();
    failed |= test_compare_diffs();
    failed |= test_preamble_skip();
    failed |= test_id_voter_header();
    failed |= test_settings_roundtrip();
    failed |= test_xlsx_roundtrip();
    failed |= test_xlsx_styles();
    failed |= test_cvr();
    failed |= test_cvr_multiselect();
    failed |= test_cvr_tabulate();
    failed |= test_cvr_merge_writeins();
    failed |= test_cvr_multicard();
    failed |= test_cvr_filter_values();
    failed |= test_cvr_delimited();
    failed |= test_cvr_colcounts();
    failed |= test_cvr_whitespace();
    failed |= test_xlsx_writein();
    return failed == 0 ? 0 : 1;
}
