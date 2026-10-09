/**
 * @file smoke_load.c
 * @brief Console smoke test for EeVoterTable_LoadFromFile.
 */

#include "filter.h"
#include "settings.h"
#include "voter_table.h"
#include "ee_cvr.h"

#include "third_party/miniz/miniz.h"

#include <stdarg.h>
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
    {
        const char *pfx =
            "100001,101,\"Smith, John A\",\"123 Main ST, Austin, TX 78701\",100001,";
        if (strncmp(text, pfx, strlen(pfx)) != 0)
        {
            wprintf(L"copy: prepend prefix mismatch\n");
            goto done;
        }
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
        if (wcscmp(buf, L"123 Main ST, Austin, TX 78701") != 0)
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

/* Voter delimited export: EeVoterTable_FormatDelimitedUtf8 emits a header row of
 * column titles and honors an explicit delimiter (tag: vexport). */
static int test_voter_export(void)
{
    EeVoterTable t;
    wchar_t err[256];
    char *text = NULL;
    uint32_t rows[1];
    int rc = 1;

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    if (EeVoterTable_LoadFromFile(L"test\\sample_voters.csv", &t, NULL, NULL, NULL, err,
                                  ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"vexport: load failed %s\n", err);
        goto done;
    }
    rows[0] = 0;
    /* With header + normalized columns, comma delimiter. */
    if (!EeVoterTable_FormatDelimitedUtf8(&t, rows, 1, TRUE, ',', TRUE, &text, NULL) ||
        text == NULL)
    {
        wprintf(L"vexport: csv format failed\n");
        goto done;
    }
    /* First line is a header (starts with the normalized Voter ID column title, not a
     * data value), and the data row for voter 100001 follows on the next line. */
    if (strstr(text, "100001,101,") == NULL || strstr(text, "\r\n100001,101,") == NULL)
    {
        wprintf(L"vexport: expected header + data row:\n%hs\n", text);
        goto done;
    }
    free(text);
    text = NULL;
    /* Tab delimiter, no header, source columns only: first field is the raw Voter ID. */
    if (!EeVoterTable_FormatDelimitedUtf8(&t, rows, 1, FALSE, '\t', FALSE, &text, NULL) ||
        text == NULL)
    {
        wprintf(L"vexport: tsv format failed\n");
        goto done;
    }
    if (strncmp(text, "100001\t", 7) != 0)
    {
        wprintf(L"vexport: tsv prefix mismatch:\n%hs\n", text);
        goto done;
    }
    rc = 0;
    wprintf(L"vexport ok\n");

done:
    free(text);
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"vexport test failed\n");
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
    if (wcscmp(buf, L"123 Main ST, Austin, TX 78701") != 0)
    {
        wprintf(L"zip4: zero +4 field mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"456 Oak AVE, Austin, TX 78702-1234") != 0)
    {
        wprintf(L"zip4: real +4 field mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"789 Pine RD, Austin, TX 78703") != 0)
    {
        wprintf(L"zip4: missing +4 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 3, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"10 Elm CT, Austin, TX 78701") != 0)
    {
        wprintf(L"zip4: combined 0000 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 4, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"20 Ash LN, Austin, TX 78701") != 0)
    {
        wprintf(L"zip4: hyphen 0000 mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 5, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"30 Bay DR, Austin, TX 78701-1111") != 0)
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
    if (wcscmp(buf, L"123 Main St, Austin, TX 78701") != 0)
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
    /* A confidential row has no ZIP tail of its own, so only the value-based column check
     * (the "CITY" values are digit-bearing district codes) keeps "C10" off it. */
    fputs("2128393969,REDACTED VOTER,****,P 100,5,C10\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 2)
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
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"****") != 0)
    {
        wprintf(L"distcode: district code on redacted row (%s)\n", buf);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_Clear(&t);
    rc = 0;
    wprintf(L"distcode ok\n");
    return rc;
}

/* Some exports (e.g. the Texas SOS registered-voter list) carry residence city and ZIP
 * in their own columns but omit residence STATE entirely, and the RES_ADDR line ends in a
 * unit number that must not be mistaken for a ZIP. The composed address must keep the real
 * city/ZIP AND gain the dataset-inferred state (plurality of residence ZIPs), which is
 * also applied to rows whose own ZIP is blank/redacted (tag: resstate). */
static int test_infer_residence_state(void)
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
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_resstate.csv")))
    {
        wprintf(L"resstate: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"resstate: could not create %s\n", path);
        return 1;
    }
    fputs("COUNTY_CODE,LAST_NAME,FIRST_NAME,VUID,RES_ADDR,RESIDENT_CITY,RESIDENT_ZIP_CODE\n", fp);
    /* Row 0: RES_ADDR ends in a unit number (11210) that must not read as a ZIP. */
    fputs("227,Tovar,Leslie,1,8000 W US 290 HWY 11210,AUSTIN,78736\n", fp);
    fputs("227,Rohan,Johanna,2,6804 COVERED BRIDGE DR 12104,AUSTIN,78736\n", fp);
    /* Row 2: blank ZIP -- gets the dataset's inferred state (TX) even so. */
    fputs("227,Doe,Jane,3,100 MAIN ST,AUSTIN,\n", fp);
    /* Row 3: confidential voter -- stays masked, no inferred state appended. */
    fputs("227,Hall,Steven,4,*****,*****,*****\n", fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    DeleteFileW(path);
    if (s != EeLoadStatus_Ok || t.row_count != 4)
    {
        wprintf(L"resstate: load failed %s\n", err);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"8000 W US 290 HWY 11210, AUSTIN, TX 78736") != 0)
    {
        wprintf(L"resstate: unit-number row (%s)\n", buf);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 2, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"100 MAIN ST, AUSTIN, TX") != 0)
    {
        wprintf(L"resstate: blank-zip row did not get inferred state (%s)\n", buf);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_GetViewCellW(&t, 3, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"*****") != 0)
    {
        wprintf(L"resstate: redacted row should stay masked (%s)\n", buf);
        EeVoterTable_Clear(&t);
        return 1;
    }
    EeVoterTable_Clear(&t);
    rc = 0;
    wprintf(L"resstate ok\n");
    return rc;
}

/* El Paso County layout: a generic "VoterID" is the Voter ID when no VUID column exists;
 * "Apartment_Number", "Street_Number_Suffix" and "Street_Dir_Suffix" are address parts;
 * the pure "City_Name" beats an earlier combined "City_State"; and the district column
 * "STATE BOARD OF EDU 23" (value "1") is not the residence state -- the state is inferred
 * (TX). A file that also has an explicit VUID column uses it over "VoterID" (tag: elpaso). */
static int test_el_paso_layout(void)
{
    static const char *k_hdr =
        "VoterID,Voter_Name,City_State,Zip_Country,Street_Number,Street_Number_Suffix,"
        "Street_Dir,Street_Name,Street_Type,Street_Dir_Suffix,Unit_Type,Apartment_Number,"
        "Zip_Code,City_Name,Precinct,STATE BOARD OF EDU 23\n";
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
        FAILED(StringCchCatW(path, ARRAYSIZE(path), L"ee_elpaso.csv")))
    {
        wprintf(L"elpaso: temp path failed\n");
        return 1;
    }
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"elpaso: could not create %s\n", path);
        return 1;
    }
    fputs(k_hdr, fp);
    fputs("2208823355,\"A CENICEROS, ISABEL G\",EL PASO TX,79907,1009,1/2,N,MACADAMIA,CIR,E,"
          "SPC,27,79907,EL PASO,195.1,1\n",
          fp);
    fclose(fp);

    EeVoterTable_Init(&t);
    err[0] = L'\0';
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.row_count != 1)
    {
        wprintf(L"elpaso: load failed %s\n", err);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_VOTER_ID, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"2208823355") != 0)
    {
        wprintf(L"elpaso: VoterID not used (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"1009 1/2 N MACADAMIA CIR E SPC 27, EL PASO, TX 79907") != 0)
    {
        wprintf(L"elpaso: address (%s)\n", buf);
        goto done;
    }
    EeVoterTable_Clear(&t);

    /* With an explicit VUID column present, it wins over the generic VoterID. */
    if (_wfopen_s(&fp, path, L"wb") != 0 || fp == NULL)
    {
        wprintf(L"elpaso: could not recreate %s\n", path);
        return 1;
    }
    fputs("VoterID,VUID,Voter_Name\n111,2208823355,\"DOE, JANE\"\n", fp);
    fclose(fp);
    EeVoterTable_Init(&t);
    s = EeVoterTable_LoadFromFile(path, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.row_count != 1)
    {
        wprintf(L"elpaso: second load failed %s\n", err);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 0, EE_COL_VOTER_ID, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"2208823355") != 0)
    {
        wprintf(L"elpaso: VUID should beat VoterID (%s)\n", buf);
        goto done;
    }
    rc = 0;
    wprintf(L"elpaso ok\n");

done:
    DeleteFileW(path);
    EeVoterTable_Clear(&t);
    if (rc != 0)
    {
        wprintf(L"elpaso test failed\n");
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
    if (wcscmp(buf, L"6007 SUN VISTA DR, Austin, TX 78749") != 0)
    {
        wprintf(L"blkdot: .0 house number mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"12 OAK ST, Austin, TX 78701") != 0)
    {
        wprintf(L"blkdot: plain house number mismatch (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 2, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"100.50 PINE RD, Austin, TX 78702") != 0)
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
    if (wcscmp(buf, L"12 Oak ST, Austin, TX 78701") != 0)
    {
        wprintf(L"lotunit: LOT should be omitted (%s)\n", buf);
        goto done;
    }
    EeVoterTable_GetViewCellW(&t, 1, EE_COL_ADDRESS, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"90 Pine RD APT 2, Austin, TX 78702") != 0)
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
    EeVoterTable_Clear(&t);

    /* Dallas County in-person roster style: "State ID" is Texas's statewide voter ID
     * (VUID), not the residence State column. It must map to Voter ID, and must not
     * leak into the normalized Address. */
    if (!cmp_write_and_load(L"ee_stateid.csv",
                            "Name,State ID,Address,Polling Place,Date,Precinct,Split,Party\n"
                            "\"Hudson, Bertha\",1002114877,100 MAIN ST,Site 3,03/03/2026,358,A,REP\n",
                            &t))
    {
        wprintf(L"idvoter: State ID load failed\n");
        return 1;
    }
    if (strcmp(EeVoterTable_GetCellUtf8(&t, 0, EE_COL_VOTER_ID), "1002114877") != 0)
    {
        wprintf(L"idvoter: 'State ID' not mapped to Voter ID (got '%S')\n",
                EeVoterTable_GetCellUtf8(&t, 0, EE_COL_VOTER_ID));
        goto done;
    }
    if (strstr(EeVoterTable_GetCellUtf8(&t, 0, EE_COL_ADDRESS), "1002114877") != NULL)
    {
        wprintf(L"idvoter: 'State ID' leaked into Address ('%S')\n",
                EeVoterTable_GetCellUtf8(&t, 0, EE_COL_ADDRESS));
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
     * (ZIP already encodes the state). Voter 2 is address-confidential and the two
     * exports mask it differently (A: inferred state appended to a single mask, as the
     * SOS list does; B: every part masked) -> the same redacted value, not a change. */
    a_ok = cmp_write_and_load(L"ee_cmpstate_a.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 MAIN ST AUSTIN TX 78701\n"
                              "2,126,Hall,Steven,\"*****, TX\"\n"
                              "3,127,Doe,Jane,\"*****, TX\"\n",
                              &a);
    /* Voter 3's mask leaves the street type and ZIP+4 dash visible -- still redacted. */
    b_ok = cmp_write_and_load(L"ee_cmpstate_b.csv",
                              "VUID,PCTCOD,LSTNAM,FSTNAM,Residential Address\n"
                              "1,101,Smith,John,100 MAIN ST AUSTIN 78701\n"
                              "2,126,Hall,Steven,\"*** *** *** ***, ***, ***\"\n"
                              "3,127,Doe,Jane,\"*** *** RD *** -***, ***, ***\"\n",
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
    if (r.identical_a != 3 || r.addr_minor_a != 0 || r.addr_major_a != 0)
    {
        wprintf(L"cmpstate: missing-state or masked address flagged (id=%u amin=%u amaj=%u)\n",
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

/* CVR delimited export: EeCvr_FormatDelimitedUtf8 emits a header row + rows with the
 * requested delimiter and RFC-4180 quoting (a comma-bearing contest name/value is
 * quoted for CSV but not for TSV) (tag: cvrexp). */
static int test_cvr_export(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    /* Contest header carries a comma so CSV must quote it. */
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Mayor, City X (10)</t></is></c></row>";
    static const char *k_rows =
        "<row r=\"2\"><c r=\"A2\"><v>1</v></c>"
        "<c r=\"B2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C2\" t=\"inlineStr\"><is><t>Bob, Jr.</t></is></c></row>"
        "<row r=\"3\"><c r=\"A3\"><v>2</v></c>"
        "<c r=\"B3\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C3\" t=\"inlineStr\"><is><t>Alice</t></is></c></row>";

    wchar_t path[MAX_PATH];
    wchar_t err[512] = L"";
    char sheet[4096];
    const wchar_t *one[1];
    EeCvrTable t;
    EeLoadStatus s;
    uint32_t rows[8];
    uint32_t i;
    char *text = NULL;
    size_t len = 0;
    int rc = 1;

    if (!cvr_temp_path(path, ARRAYSIZE(path), L"ee_cvr_exp.xlsx"))
    {
        wprintf(L"cvrexp: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr,
                     k_rows);
    if (!cvr_write_xlsx(path, sheet))
    {
        wprintf(L"cvrexp: write failed\n");
        return 1;
    }
    EeCvr_Init(&t);
    one[0] = path;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 2)
    {
        wprintf(L"cvrexp: load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    for (i = 0; i < t.nrows; i++)
    {
        rows[i] = i;
    }

    /* CSV: comma-bearing fields are quoted, header present. */
    if (!EeCvr_FormatDelimitedUtf8(&t, rows, t.nrows, ',', TRUE, &text, &len))
    {
        wprintf(L"cvrexp: csv format failed\n");
        goto done;
    }
    if (strstr(text, "Cast Vote Record,Precinct,\"Mayor, City X (10)\"\r\n") == NULL ||
        strstr(text, "1,P1,\"Bob, Jr.\"\r\n") == NULL ||
        strstr(text, "2,P1,Alice\r\n") == NULL)
    {
        wprintf(L"cvrexp: csv content unexpected:\n%hs\n", text);
        goto done;
    }
    free(text);
    text = NULL;

    /* TSV: no comma quoting needed (fields hold no tab). */
    if (!EeCvr_FormatDelimitedUtf8(&t, rows, t.nrows, '\t', TRUE, &text, &len))
    {
        wprintf(L"cvrexp: tsv format failed\n");
        goto done;
    }
    if (strstr(text, "Cast Vote Record\tPrecinct\tMayor, City X (10)\r\n") == NULL ||
        strstr(text, "1\tP1\tBob, Jr.\r\n") == NULL)
    {
        wprintf(L"cvrexp: tsv content unexpected:\n%hs\n", text);
        goto done;
    }
    rc = 0;
    wprintf(L"cvrexp ok\n");

done:
    free(text);
    EeCvr_Clear(&t);
    DeleteFileW(path);
    if (rc != 0)
    {
        wprintf(L"cvrexp test failed\n");
    }
    return rc;
}

/* Export round-trip fidelity for a "vote for N" contest: a contest's blank
 * continuation columns must export with BLANK headers (not their derived "(2)"
 * display titles), so re-importing the exported CSV regroups the columns into one
 * contest and tabulation is unchanged (tag: cvrrt). */
static int test_cvr_roundtrip(void)
{
    static const char *k_head =
        "<?xml version=\"1.0\"?><worksheet "
        "xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>";
    /* Column C is a titled 2-seat contest; column D is its BLANK continuation. */
    static const char *k_hdr =
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Cast Vote Record</t></is></c>"
        "<c r=\"B1\" t=\"inlineStr\"><is><t>Precinct</t></is></c>"
        "<c r=\"C1\" t=\"inlineStr\"><is><t>Council 2 Seats (10)</t></is></c>"
        "<c r=\"D1\"/>"
        "<c r=\"E1\" t=\"inlineStr\"><is><t>Mayor (11)</t></is></c></row>";
    static const char *k_rows =
        "<row r=\"2\"><c r=\"A2\"><v>1</v></c>"
        "<c r=\"B2\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C2\" t=\"inlineStr\"><is><t>Alice</t></is></c>"
        "<c r=\"D2\" t=\"inlineStr\"><is><t>Bob</t></is></c>"
        "<c r=\"E2\" t=\"inlineStr\"><is><t>Xavier</t></is></c></row>"
        "<row r=\"3\"><c r=\"A3\"><v>2</v></c>"
        "<c r=\"B3\" t=\"inlineStr\"><is><t>P1</t></is></c>"
        "<c r=\"C3\" t=\"inlineStr\"><is><t>Bob</t></is></c>"
        "<c r=\"D3\" t=\"inlineStr\"><is><t>Alice</t></is></c>"
        "<c r=\"E3\" t=\"inlineStr\"><is><t>Yolanda</t></is></c></row>";

    wchar_t xpath[MAX_PATH];
    wchar_t cpath[MAX_PATH];
    wchar_t err[512] = L"";
    char sheet[4096];
    const wchar_t *one[1];
    EeCvrTable t;
    EeLoadStatus s;
    EeCvrTally *ta = NULL;
    EeCvrTally *tb = NULL;
    uint32_t na = 0, nb = 0, i;
    uint32_t *rows = NULL;
    char *csv = NULL;
    size_t csvlen = 0;
    int rc = 1;

    if (!cvr_temp_path(xpath, ARRAYSIZE(xpath), L"ee_cvr_rt.xlsx") ||
        !cvr_temp_path(cpath, ARRAYSIZE(cpath), L"ee_cvr_rt.csv"))
    {
        wprintf(L"cvrrt: temp path failed\n");
        return 1;
    }
    StringCchPrintfA(sheet, ARRAYSIZE(sheet), "%s%s%s</sheetData></worksheet>", k_head, k_hdr,
                     k_rows);
    if (!cvr_write_xlsx(xpath, sheet))
    {
        wprintf(L"cvrrt: write xlsx failed\n");
        return 1;
    }
    EeCvr_Init(&t);
    one[0] = xpath;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 2)
    {
        wprintf(L"cvrrt: xlsx load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    /* Baseline tally from the .xlsx (Council summed across C+D: Alice 2, Bob 2). */
    if (!EeCvr_Tabulate(&t, TRUE, &ta, &na))
    {
        wprintf(L"cvrrt: baseline tabulate failed\n");
        goto done;
    }
    /* Export all rows as CSV. */
    rows = (uint32_t *)malloc((size_t)t.nrows * sizeof(uint32_t));
    for (i = 0; i < t.nrows; i++)
    {
        rows[i] = i;
    }
    if (!EeCvr_FormatDelimitedUtf8(&t, rows, t.nrows, ',', TRUE, &csv, &csvlen))
    {
        wprintf(L"cvrrt: export failed\n");
        goto done;
    }
    /* The continuation column's header must be blank in the export. */
    if (strstr(csv, "Council 2 Seats (10),,Mayor (11)") == NULL)
    {
        wprintf(L"cvrrt: continuation header not blank in export:\n%hs\n", csv);
        goto done;
    }
    if (!cvr_write_bytes(cpath, csv, csvlen))
    {
        wprintf(L"cvrrt: write csv failed\n");
        goto done;
    }
    EeCvr_Clear(&t);
    EeCvr_Init(&t);
    one[0] = cpath;
    s = EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok)
    {
        wprintf(L"cvrrt: csv reload s=%d err=%s\n", (int)s, err);
        goto done;
    }
    if (!EeCvr_Tabulate(&t, TRUE, &tb, &nb))
    {
        wprintf(L"cvrrt: reloaded tabulate failed\n");
        goto done;
    }
    /* Tallies must be identical: same contest count and per-row values (no split
     * "Council 2 Seats (10) (2)" contest). */
    if (na != nb)
    {
        wprintf(L"cvrrt: tally count differs baseline=%u reloaded=%u\n", na, nb);
        goto done;
    }
    for (i = 0; i < na; i++)
    {
        if (wcscmp(ta[i].contest, tb[i].contest) != 0 ||
            wcscmp(ta[i].selection, tb[i].selection) != 0 || ta[i].count != tb[i].count)
        {
            wprintf(L"cvrrt: row %u differs (%s/%s/%u vs %s/%s/%u)\n", i, ta[i].contest,
                    ta[i].selection, ta[i].count, tb[i].contest, tb[i].selection, tb[i].count);
            goto done;
        }
    }
    rc = 0;
    wprintf(L"cvrrt ok\n");

done:
    EeCvr_FreeTally(ta, na);
    EeCvr_FreeTally(tb, nb);
    free(rows);
    free(csv);
    EeCvr_Clear(&t);
    DeleteFileW(xpath);
    DeleteFileW(cpath);
    if (rc != 0)
    {
        wprintf(L"cvrrt test failed\n");
    }
    return rc;
}

/* Author a zip of (name, xml) entries at @p path. */
static BOOL hart_write_zip(const wchar_t *path, const char *const *names,
                           const char *const *xmls, int n)
{
    mz_zip_archive zip;
    void *zbuf = NULL;
    size_t zsize = 0;
    FILE *fp = NULL;
    int i;
    BOOL ok = TRUE;
    memset(&zip, 0, sizeof(zip));
    if (!mz_zip_writer_init_heap(&zip, 0, 0))
    {
        return FALSE;
    }
    for (i = 0; i < n && ok; i++)
    {
        ok = mz_zip_writer_add_mem(&zip, names[i], xmls[i], strlen(xmls[i]),
                                   MZ_DEFAULT_COMPRESSION);
    }
    ok = ok && mz_zip_writer_finalize_heap_archive(&zip, &zbuf, &zsize);
    if (ok)
    {
        ok = (_wfopen_s(&fp, path, L"wb") == 0 && fp != NULL &&
              fwrite(zbuf, 1, zsize, fp) == zsize);
        if (fp != NULL)
        {
            fclose(fp);
        }
    }
    mz_free(zbuf);
    mz_zip_writer_end(&zip);
    return ok;
}

/* Find a tabulation entry's count by (contest, selection); -1 if absent. */
static long hart_find_count(EeCvrTally *items, uint32_t n, const wchar_t *contest,
                            const wchar_t *sel)
{
    uint32_t i;
    for (i = 0; i < n; i++)
    {
        if (wcscmp(items[i].contest, contest) == 0 && wcscmp(items[i].selection, sel) == 0)
        {
            return (long)items[i].count;
        }
    }
    return -1;
}

/* Export @p src to a CSV, reload it through the file loader, and verify the reloaded
 * table freezes @p want_frozen leading key columns and tabulates identically to @p src.
 * Guards the round-trip bug where a Hart export's key columns (Sheet Number, Batch
 * Sequence, Party, Is Blank, ...) were re-tabulated as contests because the CSV frozen-
 * column detector only knew the ES&S key names. Returns TRUE on success. */
static BOOL hart_csv_roundtrip_ok(const EeCvrTable *src, uint32_t want_frozen)
{
    wchar_t cpath[MAX_PATH] = L"";
    wchar_t err[512] = L"";
    const wchar_t *one[1];
    EeCvrTable t2;
    EeCvrTally *ba = NULL, *rb = NULL;
    uint32_t nba = 0, nrb = 0, i;
    uint32_t *rows = NULL;
    char *csv = NULL;
    size_t csvlen = 0;
    BOOL ok = FALSE;

    EeCvr_Init(&t2);
    if (!EeCvr_Tabulate(src, TRUE, &ba, &nba))
    {
        wprintf(L"hart-rt: baseline tabulate failed\n");
        goto out;
    }
    rows = (uint32_t *)malloc((size_t)src->nrows * sizeof(uint32_t));
    if (rows == NULL)
    {
        goto out;
    }
    for (i = 0; i < src->nrows; i++)
    {
        rows[i] = i;
    }
    if (!EeCvr_FormatDelimitedUtf8(src, rows, src->nrows, ',', TRUE, &csv, &csvlen))
    {
        wprintf(L"hart-rt: export failed\n");
        goto out;
    }
    if (!cvr_temp_path(cpath, ARRAYSIZE(cpath), L"ee_hart_rt.csv") ||
        !cvr_write_bytes(cpath, csv, csvlen))
    {
        wprintf(L"hart-rt: write csv failed\n");
        goto out;
    }
    one[0] = cpath;
    if (EeCvr_LoadFromFiles(one, 1, &t2, NULL, NULL, NULL, err, ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"hart-rt: csv reload failed: %s\n", err);
        goto out;
    }
    if (t2.frozen_count != want_frozen)
    {
        wprintf(L"hart-rt: reloaded frozen=%u (want %u)\n", t2.frozen_count, want_frozen);
        goto out;
    }
    if (!EeCvr_Tabulate(&t2, TRUE, &rb, &nrb))
    {
        wprintf(L"hart-rt: reloaded tabulate failed\n");
        goto out;
    }
    if (nba != nrb)
    {
        wprintf(L"hart-rt: tally count differs baseline=%u reloaded=%u\n", nba, nrb);
        goto out;
    }
    for (i = 0; i < nba; i++)
    {
        if (wcscmp(ba[i].contest, rb[i].contest) != 0 ||
            wcscmp(ba[i].selection, rb[i].selection) != 0 || ba[i].count != rb[i].count)
        {
            wprintf(L"hart-rt: row %u differs (%s/%s/%u vs %s/%s/%u)\n", i, ba[i].contest,
                    ba[i].selection, ba[i].count, rb[i].contest, rb[i].selection, rb[i].count);
            goto out;
        }
    }
    ok = TRUE;

out:
    EeCvr_FreeTally(ba, nba);
    EeCvr_FreeTally(rb, nrb);
    free(rows);
    free(csv);
    EeCvr_Clear(&t2);
    DeleteFileW(cpath);
    return ok;
}

/* A Hart GENERAL-election export has no Party column, so its key block is 6 columns
 * (CvrGuid, Sheet Number, Batch Sequence, Batch Number, Precinct, Is Blank). Verify a
 * CSV with that header reloads with frozen_count == 6 and that the key columns are not
 * tabulated as contests. Returns TRUE on success. */
static BOOL hart_ge_reload_ok(void)
{
    static const char *k_csv =
        "CvrGuid,Sheet Number,Batch Sequence,Batch Number,Precinct,Is Blank,"
        "President,United States Senator\r\n"
        "AAA,1,1,1,101,false,Alice,Bob\r\n"
        "BBB,1,2,1,101,false,Alice,Carol\r\n";
    wchar_t cpath[MAX_PATH] = L"";
    wchar_t err[512] = L"";
    const wchar_t *one[1];
    EeCvrTable t;
    EeCvrTally *items = NULL;
    uint32_t nt = 0;
    BOOL ok = FALSE;

    EeCvr_Init(&t);
    if (!cvr_temp_path(cpath, ARRAYSIZE(cpath), L"ee_hart_ge.csv") ||
        !cvr_write_bytes(cpath, k_csv, strlen(k_csv)))
    {
        wprintf(L"hart-ge: write csv failed\n");
        goto out;
    }
    one[0] = cpath;
    if (EeCvr_LoadFromFiles(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err)) != EeLoadStatus_Ok)
    {
        wprintf(L"hart-ge: load failed: %s\n", err);
        goto out;
    }
    if (t.frozen_count != 6)
    {
        wprintf(L"hart-ge: frozen=%u (want 6)\n", t.frozen_count);
        goto out;
    }
    if (!EeCvr_Tabulate(&t, TRUE, &items, &nt))
    {
        wprintf(L"hart-ge: tabulate failed\n");
        goto out;
    }
    /* Real contests are counted; key columns are not. */
    if (hart_find_count(items, nt, L"President", L"Alice") != 2 ||
        hart_find_count(items, nt, L"United States Senator", L"Bob") != 1 ||
        hart_find_count(items, nt, L"Sheet Number", L"1") != -1 ||
        hart_find_count(items, nt, L"Is Blank", L"false") != -1 ||
        hart_find_count(items, nt, L"Batch Sequence", L"1") != -1)
    {
        wprintf(L"hart-ge: key column tabulated as a contest\n");
        goto out;
    }
    ok = TRUE;

out:
    EeCvr_FreeTally(items, nt);
    EeCvr_Clear(&t);
    DeleteFileW(cpath);
    return ok;
}

/* Hart CVR loader: a zip of per-sheet XML files -> one row each; category-ordered
 * contests; vote-for-N expansion; write-in/overvote/undervote; multi-card via
 * SheetNumber (tag: hart). */
static int test_hart_cvr(void)
{
    /* Sheet 1: Governor listed BEFORE President (to prove federal-first reordering);
     * a write-in Senator, a vote-for-2 City Council, and an overvoted Attorney
     * General. Sheet 2: a proposition (SheetNumber 2 -> multi-card). */
    static const char *k_sheet1 =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<Cvr xmlns=\"http://tempuri.org/CVRDesign.xsd\"><Contests>"
        "<Contest><Name>Governor</Name><Id>g1</Id><Options /><Undervotes>1</Undervotes></Contest>"
        "<Contest><Name>President</Name><Id>p1</Id><Options><Option><Name>Alice</Name><Id>a1</Id>"
        "<Value>1</Value></Option></Options></Contest>"
        /* Two US Rep districts in NON-numeric XML order, to prove natural-order sorting. */
        "<Contest><Name>United States Representative, District 33</Name><Id>r33</Id><Options>"
        "<Option><Name>Cand33</Name><Id>c33</Id><Value>1</Value></Option></Options></Contest>"
        "<Contest><Name>United States Representative, District 6</Name><Id>r6</Id><Options>"
        "<Option><Name>Cand6</Name><Id>c6</Id><Value>1</Value></Option></Options></Contest>"
        "<Contest><Name>United States Senator</Name><Id>s1</Id><Options><Option><Name /><Id>w1</Id>"
        "<Value>1</Value><WriteInData><OriginalText>ZZ</OriginalText>"
        "<WriteInDataStatus>Unresolved</WriteInDataStatus></WriteInData></Option></Options></Contest>"
        "<Contest><Name>City Council</Name><Id>c1</Id><Options>"
        "<Option><Name>Bob</Name><Id>b1</Id><Value>1</Value></Option>"
        "<Option><Name>Carol</Name><Id>c2</Id><Value>1</Value></Option></Options></Contest>"
        "<Contest><Name>Attorney General</Name><Id>ag1</Id><Options>"
        "<Option><Name>Dan</Name><Id>d1</Id><Value>1</Value></Option>"
        "<Option><Name>Eve</Name><Id>e1</Id><Value>1</Value></Option></Options><Overvoted /></Contest>"
        "</Contests><SheetNumber>1</SheetNumber>"
        "<PrecinctSplit><Name>101</Name><Id>x</Id></PrecinctSplit>"
        "<Party><Name>Democratic Party Ballot</Name><Id>y</Id></Party>"
        "<BatchSequence>1</BatchSequence><BatchNumber>1</BatchNumber>"
        "<CvrGuid>AAA</CvrGuid><IsBlank>false</IsBlank></Cvr>";
    static const char *k_sheet2 =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<Cvr xmlns=\"http://tempuri.org/CVRDesign.xsd\"><Contests>"
        "<Contest><Name>Proposition 1</Name><Id>pr1</Id><Options>"
        "<Option><Name>Yes</Name><Id>yy</Id><Value>1</Value></Option></Options></Contest>"
        "</Contests><SheetNumber>2</SheetNumber>"
        "<PrecinctSplit><Name>101</Name><Id>x</Id></PrecinctSplit>"
        "<Party><Name>Democratic Party Ballot</Name><Id>y</Id></Party>"
        "<BatchSequence>1</BatchSequence><BatchNumber>1</BatchNumber>"
        "<CvrGuid>BBB</CvrGuid><IsBlank>false</IsBlank></Cvr>";
    /* A Republican ballot -> its President is a separate contest ("REP President"). */
    static const char *k_sheet3 =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<Cvr xmlns=\"http://tempuri.org/CVRDesign.xsd\"><Contests>"
        "<Contest><Name>President</Name><Id>rp1</Id><Options><Option><Name>Zach</Name><Id>z1</Id>"
        "<Value>1</Value></Option></Options></Contest>"
        "</Contests><SheetNumber>1</SheetNumber>"
        "<PrecinctSplit><Name>101</Name><Id>x</Id></PrecinctSplit>"
        "<Party><Name>Republican Party Ballot</Name><Id>r</Id></Party>"
        "<BatchSequence>1</BatchSequence><BatchNumber>1</BatchNumber>"
        "<CvrGuid>CCC</CvrGuid><IsBlank>false</IsBlank></Cvr>";
    const char *names[3] = {"1_AAA.xml", "BBB.xml", "1_CCC.xml"};
    const char *xmls[3];
    wchar_t zpath[MAX_PATH];
    wchar_t err[512] = L"";
    const wchar_t *one[1];
    EeCvrTable t;
    EeLoadStatus s;
    EeCvrTally *items = NULL;
    uint32_t nt = 0;
    uint32_t colPres = 0, colGov = 0;
    int rc = 1;

    xmls[0] = k_sheet1;
    xmls[1] = k_sheet2;
    xmls[2] = k_sheet3;

    if (!cvr_temp_path(zpath, ARRAYSIZE(zpath), L"ee_hart.zip"))
    {
        wprintf(L"hart: temp path failed\n");
        return 1;
    }
    if (!hart_write_zip(zpath, names, xmls, 3))
    {
        wprintf(L"hart: write zip failed\n");
        return 1;
    }
    EeCvr_Init(&t);
    one[0] = zpath;
    s = EeCvr_LoadFromHartZips(one, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 3)
    {
        wprintf(L"hart: load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    /* Frozen keys: CvrGuid, Sheet Number, Batch Sequence, Batch Number, Precinct,
     * Party, Is Blank -> 7 (Party present). */
    if (t.frozen_count != 7)
    {
        wprintf(L"hart: frozen=%u (want 7)\n", t.frozen_count);
        goto done;
    }
    if (!EeCvr_HasMultiCard(&t))
    {
        wprintf(L"hart: multi-card not detected\n");
        goto done;
    }
    /* Primary: contests are party-prefixed; federal (President) sorts before state
     * (Governor) despite the XML order. */
    if (!EeCvr_FindColumnByTitle(&t, L"DEM President", &colPres) ||
        !EeCvr_FindColumnByTitle(&t, L"DEM Governor", &colGov) || !(colPres < colGov))
    {
        wprintf(L"hart: ordering DEM President=%u DEM Governor=%u\n", colPres, colGov);
        goto done;
    }
    if (!EeCvr_Tabulate(&t, TRUE, &items, &nt))
    {
        wprintf(L"hart: tabulate failed\n");
        goto done;
    }
    if (hart_find_count(items, nt, L"DEM President", L"Alice") != 1 ||
        hart_find_count(items, nt, L"REP President", L"Zach") != 1 ||
        hart_find_count(items, nt, L"DEM United States Senator", L"write-in") != 1 ||
        hart_find_count(items, nt, L"DEM Governor", L"undervote") != 1 ||
        hart_find_count(items, nt, L"DEM Attorney General", L"overvote") != 1 ||
        hart_find_count(items, nt, L"DEM City Council", L"Bob") != 1 ||
        hart_find_count(items, nt, L"DEM City Council", L"Carol") != 1 ||
        hart_find_count(items, nt, L"DEM Proposition 1", L"Yes") != 1)
    {
        wprintf(L"hart: tally mismatch\n");
        goto done;
    }
    /* Natural contest order: District 6 must sort before District 33 (numeric, not
     * lexicographic and not XML order, which put 33 first). */
    {
        uint32_t cR6 = 0, cR33 = 0;
        if (!EeCvr_FindColumnByTitle(&t, L"DEM United States Representative, District 6", &cR6) ||
            !EeCvr_FindColumnByTitle(&t, L"DEM United States Representative, District 33", &cR33) ||
            cR6 >= cR33)
        {
            wprintf(L"hart: district natural order R6=%u R33=%u\n", cR6, cR33);
            goto done;
        }
    }
    /* Party-first reorder: REP-first puts a REP contest at the top; DEM-first a DEM. */
    EeCvr_ReorderTallyByParty(items, nt, EE_TAB_PARTY_REP);
    if (wcsncmp(items[0].contest, L"REP ", 4) != 0)
    {
        wprintf(L"hart: REP-first put %s at top\n", items[0].contest);
        goto done;
    }
    EeCvr_ReorderTallyByParty(items, nt, EE_TAB_PARTY_DEM);
    if (wcsncmp(items[0].contest, L"DEM ", 4) != 0)
    {
        wprintf(L"hart: DEM-first put %s at top\n", items[0].contest);
        goto done;
    }
    /* CSV round-trip: export this primary table (7 frozen keys incl. Party) and reload
     * it through the file loader; the key columns must stay frozen, not tabulated. */
    if (!hart_csv_roundtrip_ok(&t, 7))
    {
        goto done;
    }
    /* General-election reload: no Party column -> 6 frozen keys, none tabulated. */
    if (!hart_ge_reload_ok())
    {
        goto done;
    }
    rc = 0;
    wprintf(L"hart ok\n");

done:
    EeCvr_FreeTally(items, nt);
    EeCvr_Clear(&t);
    DeleteFileW(zpath);
    if (rc != 0)
    {
        wprintf(L"hart test failed\n");
    }
    return rc;
}

/* ---- Hart PDF CVR Report fixtures ------------------------------------------------- */

/* Growable byte buffer for authoring test PDFs. */
typedef struct TBuf
{
    char *p;
    size_t len;
    size_t cap;
} TBuf;

static BOOL tb_add(TBuf *b, const void *s, size_t n)
{
    if (b->len + n + 1 > b->cap)
    {
        size_t nc = b->cap ? b->cap * 2 : 4096;
        char *np;
        while (nc < b->len + n + 1)
        {
            nc *= 2;
        }
        np = (char *)realloc(b->p, nc);
        if (np == NULL)
        {
            return FALSE;
        }
        b->p = np;
        b->cap = nc;
    }
    memcpy(b->p + b->len, s, n);
    b->len += n;
    b->p[b->len] = '\0';
    return TRUE;
}

static BOOL tb_printf(TBuf *b, const char *fmt, ...)
{
    char tmp[2048];
    va_list ap;
    va_start(ap, fmt);
    if (FAILED(StringCchVPrintfA(tmp, ARRAYSIZE(tmp), fmt, ap)))
    {
        va_end(ap);
        return FALSE;
    }
    va_end(ap);
    return tb_add(b, tmp, strlen(tmp));
}

/* One Reporting-Services-style cell: a clip rectangle with one text line per entry of
 * @p lines (NULL-terminated). A line starting with '<' is emitted as a hex string in
 * the Type0 font F255; anything else is a literal string in the WinAnsi font F5. */
static BOOL tpdf_cell(TBuf *c, double x, double y, double w, double h, const char *const *lines)
{
    int i, n = 0;
    BOOL ok;
    while (lines[n] != NULL)
    {
        n++;
    }
    ok = tb_printf(c, "q %.1f %.3f %.1f %.2f re W n\n", x, y, w, h);
    for (i = 0; i < n && ok; i++)
    {
        double ty = y + h - 10.8 - 13.32 * i;
        if (lines[i][0] == '<')
        {
            ok = tb_printf(c, "BT /F255 9.999 Tf 0 0 0 rg 523.653 TL %.3f %.3f Td %s Tj T* ET\n",
                           x + 0.016, ty, lines[i]);
        }
        else
        {
            ok = tb_printf(c, "BT /F5 9.999 Tf 0 0 0 rg 418.896 TL %.3f %.3f Td (", x + 0.016, ty);
            {
                const char *s;
                for (s = lines[i]; *s && ok; s++)
                {
                    if (*s == '(' || *s == ')' || *s == '\\')
                    {
                        ok = tb_add(c, "\\", 1);
                    }
                    ok = ok && tb_add(c, s, 1);
                }
            }
            ok = ok && tb_printf(c, ") Tj T* ET\n");
        }
    }
    return ok && tb_printf(c, "Q\n");
}

static BOOL tpdf_cell1(TBuf *c, double x, double y, double w, double h, const char *text)
{
    const char *lines[2];
    lines[0] = text;
    lines[1] = NULL;
    return tpdf_cell(c, x, y, w, h, lines);
}

/* A Hart CVR Report page header block + table header. */
typedef struct TPdfHdr
{
    const char *precinct;
    const char *party;
    const char *pplace;
    const char *vtype;
    const char *dtype;
    const char *dserial;
    const char *ddata;
    const char *cvrid;
    const char *batch;
} TPdfHdr;

static BOOL tpdf_header(TBuf *c, const TPdfHdr *h, int page, int pages)
{
    char tmp[256];
    BOOL ok = tpdf_cell1(c, 20.0, 741.4, 147.2, 24.8, "CVR Report");
    StringCchPrintfA(tmp, ARRAYSIZE(tmp), "Page %d of %d", page, pages);
    ok = ok && tpdf_cell1(c, 172.2, 688.8, 269.6, 10.4, tmp);
/* A NULL value models a county redaction: the label's cell is removed entirely. */
#define TPDF_LABEL(x, y, label, val)                                                   \
    if ((val) != NULL)                                                                 \
    {                                                                                  \
        StringCchPrintfA(tmp, ARRAYSIZE(tmp), "%s%s", label, val);                     \
        ok = ok && tpdf_cell1(c, x, y, 270.8, 14.4, tmp);                              \
    }
    TPDF_LABEL(23.0, 632.1, "Precinct: ", h->precinct);
    TPDF_LABEL(23.0, 617.7, "Party: ", h->party);
    TPDF_LABEL(23.0, 603.3, "Polling Place: ", h->pplace);
    TPDF_LABEL(23.0, 588.9, "Voting Type: ", h->vtype);
    TPDF_LABEL(318.2, 632.1, "Device Type: ", h->dtype);
    TPDF_LABEL(318.2, 617.7, "Device Serial: ", h->dserial);
    TPDF_LABEL(318.2, 603.3, "Device Data Id: ", h->ddata);
    TPDF_LABEL(318.2, 588.9, "Cvr Id: ", h->cvrid);
    TPDF_LABEL(23.0, 574.5, "Central Batch Id: ", h->batch);
#undef TPDF_LABEL
    ok = ok && tpdf_cell1(c, 20.0, 555.6, 269.6, 17.0, "Contest Title");
    ok = ok && tpdf_cell1(c, 315.2, 555.6, 269.6, 17.0, "Option");
    return ok;
}

/* One table row at row index @p r (top row 0): title + option(s). A title may be given
 * as two lines (wrapped -> a taller cell, options vertically centred). */
static BOOL tpdf_row(TBuf *c, double *y_top, const char *t1, const char *t2,
                     const char *const *opts)
{
    const char *tl[3];
    int nopt = 0, k;
    double h, y;
    BOOL ok;
    while (opts[nopt] != NULL)
    {
        nopt++;
    }
    tl[0] = t1;
    tl[1] = t2;
    tl[2] = NULL;
    h = (t2 != NULL || nopt > 1) ? 26.64 : 13.32;
    y = *y_top - h;
    ok = tpdf_cell(c, 27.2, y, 269.6, h, tl);
    for (k = 0; k < nopt && ok; k++)
    {
        double oy = (nopt == 1) ? y + (h - 13.32) / 2 : y + h - 13.32 * (k + 1);
        ok = tpdf_cell1(c, 315.2, oy, 269.6, 13.32, opts[k]);
    }
    *y_top = y - 4.0;
    return ok;
}

/* Write a PDF whose pages have the given content streams. @p hart adds the Info Title
 * and the Type0/ToUnicode font; @p break_startxref writes a bogus negative startxref so
 * the reader must rebuild its object index by scanning. */
static BOOL tpdf_write(const wchar_t *path, TBuf *pages, int npages, BOOL break_startxref)
{
    static const char k_Cmap[] =
        "/CIDInit /ProcSet findresource begin\n12 dict begin\nbegincmap\n"
        "/CMapName /Adobe-Identity-UCS def\n/CMapType 2 def\n"
        "1 begincodespacerange\n<0000> <FFFF>\nendcodespacerange\n"
        "1 beginbfchar\n<0078> <00f1>\nendbfchar\n"
        "1 beginbfrange\n<0003> <0061> <0020>\nendbfrange\n"
        "endcmap\nCMapName currentdict /CMap defineresource pop\nend\nend\n";
    TBuf b = {0};
    uint64_t offs[64];
    int nobj, i, xref_num;
    BOOL ok = TRUE;
    FILE *fp = NULL;
    uint64_t xref_off;

    /* objects: 1 catalog, 2 pages, 3 F5, 4 F255, 5 cmap, 6 info,
     * then per page: page, content, length (3 each), then the xref stream. */
    nobj = 6 + npages * 3;
    xref_num = nobj + 1;
    if (xref_num >= (int)ARRAYSIZE(offs))
    {
        return FALSE;
    }
    ok = tb_printf(&b, "%%PDF-1.7\r\n");
#define OBJ(n) (offs[n] = b.len, ok = ok && tb_printf(&b, "%d 0 obj\r\n", n))
    OBJ(1);
    ok = ok && tb_printf(&b, "<< /Type /Catalog /Pages 2 0 R >>\r\nendobj\r\n");
    OBJ(2);
    ok = ok && tb_printf(&b, "<< /Type /Pages /Count %d /Kids [", npages);
    for (i = 0; i < npages; i++)
    {
        ok = ok && tb_printf(&b, " %d 0 R", 7 + i * 3);
    }
    ok = ok && tb_printf(&b, " ] >>\r\nendobj\r\n");
    OBJ(3);
    ok = ok && tb_printf(&b, "<< /Type /Font /Subtype /TrueType /BaseFont /ABCDEE+Segoe#20UI "
                             "/Encoding /WinAnsiEncoding >>\r\nendobj\r\n");
    OBJ(4);
    ok = ok && tb_printf(&b, "<< /Type /Font /Subtype /Type0 /BaseFont /ABCDEE+Segoe#20UI "
                             "/Encoding /Identity-H /ToUnicode 5 0 R >>\r\nendobj\r\n");
    OBJ(5);
    ok = ok && tb_printf(&b, "<< /Length %u >>\r\nstream\r\n", (unsigned)strlen(k_Cmap));
    ok = ok && tb_add(&b, k_Cmap, strlen(k_Cmap));
    ok = ok && tb_printf(&b, "\r\nendstream\r\nendobj\r\n");
    OBJ(6);
    ok = ok && tb_printf(&b, "<< /Title (Count_CvrReport) /Producer (Microsoft Reporting "
                             "Services PDF Rendering Extension 2019.11.0.0) >>\r\nendobj\r\n");
    for (i = 0; i < npages && ok; i++)
    {
        int pn = 7 + i * 3;
        mz_ulong clen = mz_compressBound((mz_ulong)pages[i].len);
        unsigned char *z = (unsigned char *)malloc(clen);
        if (z == NULL ||
            mz_compress(z, &clen, (const unsigned char *)pages[i].p, (mz_ulong)pages[i].len) != MZ_OK)
        {
            free(z);
            ok = FALSE;
            break;
        }
        OBJ(pn);
        ok = ok && tb_printf(&b, "<< /Type /Page /Parent 2 0 R /MediaBox [ 0 0 612 792 ] "
                                 "/Contents %d 0 R /Resources << /Font << /F5 3 0 R "
                                 "/F255 4 0 R >> >> >>\r\nendobj\r\n", pn + 1);
        OBJ(pn + 1);
        /* /Length is an indirect object written AFTER the stream (as SSRS does). */
        ok = ok && tb_printf(&b, "<</Length %d 0 R\r\n/Filter /FlateDecode >>\r\nstream\r\n", pn + 2);
        ok = ok && tb_add(&b, z, clen);
        ok = ok && tb_printf(&b, "\r\nendstream\r\nendobj\r\n");
        OBJ(pn + 2);
        ok = ok && tb_printf(&b, "%lu\r\nendobj\r\n", (unsigned long)clen);
        free(z);
    }
    /* xref stream: W [1 4 2], one entry per object 0..xref_num */
    xref_off = b.len;
    offs[xref_num] = xref_off;
    if (ok)
    {
        unsigned char raw[64 * 7];
        mz_ulong clen = mz_compressBound(sizeof(raw));
        unsigned char *z = (unsigned char *)malloc(clen);
        int nent = xref_num + 1;
        for (i = 0; i < nent; i++)
        {
            unsigned char *e = raw + i * 7;
            uint64_t o = (i == 0) ? 0 : offs[i];
            e[0] = (unsigned char)(i == 0 ? 0 : 1);
            e[1] = (unsigned char)(o >> 24);
            e[2] = (unsigned char)(o >> 16);
            e[3] = (unsigned char)(o >> 8);
            e[4] = (unsigned char)o;
            e[5] = (unsigned char)(i == 0 ? 0xFF : 0);
            e[6] = (unsigned char)(i == 0 ? 0xFF : 0);
        }
        if (z == NULL || mz_compress(z, &clen, raw, (mz_ulong)(nent * 7)) != MZ_OK)
        {
            ok = FALSE;
        }
        else
        {
            ok = tb_printf(&b, "%d 0 obj\r\n<< /Type /XRef /Index [ 0 %d ] /W [ 1 4 2 ] "
                               "/Filter /FlateDecode /Size %d /Length %lu /Root 1 0 R "
                               "/Info 6 0 R >>\r\nstream\r\n",
                           xref_num, nent, nent, (unsigned long)clen) &&
                 tb_add(&b, z, clen) && tb_printf(&b, "\r\nendstream\r\nendobj\r\n");
        }
        free(z);
    }
#undef OBJ
    ok = ok && tb_printf(&b, "startxref\r\n%s%lu\r\n%%%%EOF", break_startxref ? "-" : "",
                         break_startxref ? 1234ul : (unsigned long)xref_off);
    if (ok)
    {
        ok = (_wfopen_s(&fp, path, L"wb") == 0 && fp != NULL && fwrite(b.p, 1, b.len, fp) == b.len);
        if (fp != NULL)
        {
            fclose(fp);
        }
    }
    free(b.p);
    return ok;
}

/* Build the 3-page Hart CVR Report fixture: record A (DEM) spans pages 1-2, record B
 * (REP) is page 3. @p guid_a / @p guid_b set the Cvr Ids. */
static BOOL tpdf_hart_fixture(const wchar_t *path, const char *guid_a, const char *guid_b,
                              BOOL break_startxref)
{
    static const char *o_alice[] = {"Alice", NULL};
    static const char *o_under[] = {"Undervotes: 1", NULL};
    static const char *o_over[] = {"Overvote", NULL};
    static const char *o_wi[] = {"Write-in", NULL};
    static const char *o_council[] = {"Bob", "Carol", NULL};
    /* "Perla Mu\xf1oz" through the Type0 font: ASCII c -> code c - 29, n-tilde -> 0x0078 */
    static const char *o_munoz[] = {"<003300480055004f004400030030005800780052005d>", NULL};
    static const char *o_yes[] = {"Yes", NULL};
    static const char *o_zach[] = {"Zach", NULL};
    TBuf pg[3];
    TPdfHdr ha, hb;
    double y;
    BOOL ok;
    int i;
    ZeroMemory(pg, sizeof(pg));
    ZeroMemory(&ha, sizeof(ha));
    ha.precinct = "101 - 001"; /* PDF spacing; loads as "101-001" like the XML */
    ha.party = "Democratic Party Ballot";
    ha.pplace = "Central Library";
    ha.vtype = "Election Day Voting";
    ha.dtype = "Scan";
    ha.dserial = "S1902990909";
    ha.ddata = "XY(1)Z";
    ha.cvrid = guid_a;
    ha.batch = "";
    hb = ha;
    hb.party = "Republican Party Ballot";
    hb.pplace = "EV - Town Hall";
    hb.vtype = "Early Voting";
    hb.cvrid = guid_b;

    ok = tpdf_header(&pg[0], &ha, 1, 3);
    y = 551.9;
    ok = ok && tpdf_row(&pg[0], &y, "Governor", NULL, o_under); /* listed before President */
    ok = ok && tpdf_row(&pg[0], &y, "President", NULL, o_alice);
    ok = ok && tpdf_row(&pg[0], &y, "Attorney General", NULL, o_over);
    ok = ok && tpdf_row(&pg[0], &y, "United States Senator", NULL, o_wi);
    ok = ok && tpdf_row(&pg[0], &y, "City Council", NULL, o_council);
    ok = ok && tpdf_header(&pg[1], &ha, 2, 3); /* same Cvr Id: continuation page */
    y = 551.9;
    ok = ok && tpdf_row(&pg[1], &y, "Lieutenant Governor", NULL, o_munoz);
    ok = ok && tpdf_row(&pg[1], &y, "Proposition ", "1", o_yes); /* wrapped title */
    ok = ok && tpdf_header(&pg[2], &hb, 3, 3);
    y = 551.9;
    ok = ok && tpdf_row(&pg[2], &y, "President", NULL, o_zach);
    ok = ok && tpdf_write(path, pg, 3, break_startxref);
    for (i = 0; i < 3; i++)
    {
        free(pg[i].p);
    }
    return ok;
}

/* A one-page PDF that is not a Hart CVR Report. */
static BOOL tpdf_other_fixture(const wchar_t *path)
{
    TBuf pg = {0};
    BOOL ok = tpdf_cell1(&pg, 72.0, 700.0, 300.0, 14.0, "Cast Vote Record Export") &&
              tpdf_cell1(&pg, 72.0, 680.0, 300.0, 14.0, "Ballot ID: 1 Precinct: 101") &&
              tpdf_write(path, &pg, 1, FALSE);
    free(pg.p);
    return ok;
}

/* Ballot-record count of @p value in the column titled @p title (what a CVR value
 * report shows); -1 if the column is missing, not reportable, or lacks the value. */
static long hart_value_count(const EeCvrTable *t, const wchar_t *title, const wchar_t *value)
{
    uint32_t col = 0, n = 0, blank = 0, i;
    EeCvrValueCount *items = NULL;
    long found = -1;
    if (!EeCvr_FindColumnByTitle(t, title, &col) || !EeCvr_ColumnHasReportableData(t, col) ||
        !EeCvr_CollectColumnCounts(t, col, &items, &n, &blank))
    {
        return -1;
    }
    for (i = 0; i < n; i++)
    {
        if (wcscmp(items[i].value, value) == 0)
        {
            found = (long)items[i].count;
        }
    }
    EeCvr_FreeColumnCounts(items, n);
    return found;
}

/* A redacted Hart report (Burnet County style): only Precinct and Cvr Id remain in the
 * header, and a vote-for-3 contest is printed as one row per seat with its title
 * repeated. Two records. */
static BOOL tpdf_redacted_fixture(const wchar_t *path)
{
    static const char *o_under[] = {"Undervotes: 1", NULL};
    static const char *o_ann[] = {"Ann", NULL};
    static const char *o_ben[] = {"Ben", NULL};
    static const char *o_cy[] = {"Cy", NULL};
    static const char *o_dee[] = {"Dee", NULL};
    static const char *o_yes[] = {"YES", NULL};
    TBuf pg[2];
    TPdfHdr h;
    double y;
    BOOL ok;
    ZeroMemory(pg, sizeof(pg));
    ZeroMemory(&h, sizeof(h)); /* every field NULL = redacted */
    h.precinct = "BURNT";
    h.cvrid = "DDDDDDDD-1111-2222-3333-444444444444";
    ok = tpdf_header(&pg[0], &h, 1, 2);
    y = 551.9;
    ok = ok && tpdf_row(&pg[0], &y, "CITY COUNCIL MEMBERS", NULL, o_under);
    ok = ok && tpdf_row(&pg[0], &y, "CITY COUNCIL MEMBERS", NULL, o_ann);
    ok = ok && tpdf_row(&pg[0], &y, "CITY COUNCIL MEMBERS", NULL, o_ben);
    ok = ok && tpdf_row(&pg[0], &y, "PROPOSITION A", NULL, o_yes);
    h.precinct = "18 - 03";
    h.cvrid = "EEEEEEEE-1111-2222-3333-444444444444";
    ok = ok && tpdf_header(&pg[1], &h, 2, 2);
    y = 551.9;
    ok = ok && tpdf_row(&pg[1], &y, "CITY COUNCIL MEMBERS", NULL, o_ann);
    ok = ok && tpdf_row(&pg[1], &y, "CITY COUNCIL MEMBERS", NULL, o_cy);
    ok = ok && tpdf_row(&pg[1], &y, "CITY COUNCIL MEMBERS", NULL, o_dee);
    ok = ok && tpdf_write(path, pg, 2, FALSE);
    free(pg[0].p);
    free(pg[1].p);
    return ok;
}

/* Hart PDF CVR Report loading (tag: hartpdf): PDF-only rows/keys/tallies (incl. a
 * record continued across pages, a wrapped title, vote-for-2, overvote, undervote,
 * write-in, and a ToUnicode-mapped accented name); object-index rebuild after a bogus
 * startxref; rejection of a non-Hart PDF; ZIP+PDF decoration by Cvr Id; an error
 * when a ZIP and PDF share no Cvr Id; and a county-redacted report (header fields
 * removed -> no columns for them; a vote-for-3 contest printed as repeated rows). */
static int test_hart_pdf(void)
{
    static const char *k_GuidA = "AAAAAAAA-1111-2222-3333-444444444444";
    static const char *k_GuidB = "BBBBBBBB-1111-2222-3333-444444444444";
    static const char *k_xa =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><Cvr><Contests>"
        "<Contest><Name>President</Name><Id>p</Id><Options><Option><Name>Alice</Name><Id>a</Id>"
        "<Value>1</Value></Option></Options></Contest></Contests>"
        "<BatchSequence>7</BatchSequence><BatchNumber>12</BatchNumber><SheetNumber>1</SheetNumber>"
        "<PrecinctSplit><Name>101-001</Name><Id>x</Id></PrecinctSplit>"
        "<Party><Name>Democratic Party Ballot</Name><Id>y</Id></Party>"
        "<CvrGuid>aaaaaaaa-1111-2222-3333-444444444444</CvrGuid><IsBlank>false</IsBlank></Cvr>";
    static const char *k_xc =
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><Cvr><Contests>"
        "<Contest><Name>President</Name><Id>p</Id><Options><Option><Name>Zed</Name><Id>z</Id>"
        "<Value>1</Value></Option></Options></Contest></Contests>"
        "<BatchSequence>8</BatchSequence><BatchNumber>12</BatchNumber><SheetNumber>1</SheetNumber>"
        "<PrecinctSplit><Name>101-001</Name><Id>x</Id></PrecinctSplit>"
        "<Party><Name>Democratic Party Ballot</Name><Id>y</Id></Party>"
        "<CvrGuid>cccccccc-1111-2222-3333-444444444444</CvrGuid><IsBlank>false</IsBlank></Cvr>";
    wchar_t pdf[MAX_PATH], pdf_broken[MAX_PATH], pdf_other[MAX_PATH], zip_ok[MAX_PATH],
        zip_none[MAX_PATH], pdf_redacted[MAX_PATH];
    wchar_t err[512] = L"";
    wchar_t buf[256];
    const wchar_t *paths[2];
    const char *names[2];
    const char *xmls[2];
    EeCvrTable t;
    EeLoadStatus s;
    EeCvrTally *items = NULL;
    uint32_t nt = 0, col = 0, col2 = 0;
    int rc = 1;

    EeCvr_Init(&t);
    if (!cvr_temp_path(pdf, ARRAYSIZE(pdf), L"ee_hart.pdf") ||
        !cvr_temp_path(pdf_broken, ARRAYSIZE(pdf_broken), L"ee_hart_broken.pdf") ||
        !cvr_temp_path(pdf_other, ARRAYSIZE(pdf_other), L"ee_other.pdf") ||
        !cvr_temp_path(zip_ok, ARRAYSIZE(zip_ok), L"ee_hart_pdf.zip") ||
        !cvr_temp_path(zip_none, ARRAYSIZE(zip_none), L"ee_hart_pdf_none.zip") ||
        !cvr_temp_path(pdf_redacted, ARRAYSIZE(pdf_redacted), L"ee_hart_redacted.pdf"))
    {
        wprintf(L"hartpdf: temp path failed\n");
        return 1;
    }
    if (!tpdf_hart_fixture(pdf, k_GuidA, k_GuidB, FALSE) ||
        !tpdf_hart_fixture(pdf_broken, k_GuidA, k_GuidB, TRUE) || !tpdf_other_fixture(pdf_other) ||
        !tpdf_redacted_fixture(pdf_redacted))
    {
        wprintf(L"hartpdf: write pdf failed\n");
        goto done;
    }

    /* ---- detection ---- */
    if (!EeCvr_IsHartCvrPdf(pdf, err, ARRAYSIZE(err)))
    {
        wprintf(L"hartpdf: fixture not detected as Hart: %s\n", err);
        goto done;
    }
    if (EeCvr_IsHartCvrPdf(pdf_other, err, ARRAYSIZE(err)) || wcsstr(err, L"not a Hart") == NULL)
    {
        wprintf(L"hartpdf: non-Hart PDF accepted (err=%s)\n", err);
        goto done;
    }
    paths[0] = pdf_other;
    s = EeCvr_LoadFromHartFiles(paths, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Error || wcsstr(err, L"ee_other.pdf") == NULL)
    {
        wprintf(L"hartpdf: non-Hart load s=%d err=%s\n", (int)s, err);
        goto done;
    }

    /* ---- PDF only ---- */
    paths[0] = pdf;
    s = EeCvr_LoadFromHartFiles(paths, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 2)
    {
        wprintf(L"hartpdf: load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    /* CvrGuid, Precinct, Party, Voting Type, Polling Place, Device Type, Device Serial,
     * Device Data Id -- no Batch Number: the fixture's Central Batch Id is blank on every
     * record, so (like a redacted field) it gets no column. */
    if (t.frozen_count != 8 || !EeCvr_FindColumnByTitle(&t, L"Polling Place", &col) ||
        EeCvr_FindColumnByTitle(&t, L"Batch Number", &col2))
    {
        wprintf(L"hartpdf: frozen=%u (want 8) / Polling Place / Batch Number\n",
                t.frozen_count);
        goto done;
    }
    EeCvr_GetCellW(&t, 0, 0, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"aaaaaaaa-1111-2222-3333-444444444444") != 0)
    {
        wprintf(L"hartpdf: guid (%s)\n", buf);
        goto done;
    }
    EeCvr_GetCellW(&t, 0, col, buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Central Library") != 0)
    {
        wprintf(L"hartpdf: polling place (%s)\n", buf);
        goto done;
    }
    if (!EeCvr_FindColumnByTitle(&t, L"Precinct", &col) ||
        (EeCvr_GetCellW(&t, 0, col, buf, ARRAYSIZE(buf)), wcscmp(buf, L"101-001") != 0))
    {
        wprintf(L"hartpdf: precinct (%s)\n", buf);
        goto done;
    }
    if (!EeCvr_FindColumnByTitle(&t, L"Device Data Id", &col) ||
        (EeCvr_GetCellW(&t, 0, col, buf, ARRAYSIZE(buf)), wcscmp(buf, L"XY(1)Z") != 0))
    {
        wprintf(L"hartpdf: device data id (%s)\n", buf);
        goto done;
    }
    {
        uint32_t cPres = 0, cGov = 0;
        if (!EeCvr_FindColumnByTitle(&t, L"DEM President", &cPres) ||
            !EeCvr_FindColumnByTitle(&t, L"DEM Governor", &cGov) || cPres >= cGov)
        {
            wprintf(L"hartpdf: contest order President=%u Governor=%u\n", cPres, cGov);
            goto done;
        }
    }
    if (!EeCvr_Tabulate(&t, TRUE, &items, &nt))
    {
        wprintf(L"hartpdf: tabulate failed\n");
        goto done;
    }
    if (hart_find_count(items, nt, L"DEM President", L"Alice") != 1 ||
        hart_find_count(items, nt, L"REP President", L"Zach") != 1 ||
        hart_find_count(items, nt, L"DEM Governor", L"undervote") != 1 ||
        hart_find_count(items, nt, L"DEM Attorney General", L"overvote") != 1 ||
        hart_find_count(items, nt, L"DEM United States Senator", L"write-in") != 1 ||
        hart_find_count(items, nt, L"DEM City Council", L"Bob") != 1 ||
        hart_find_count(items, nt, L"DEM City Council", L"Carol") != 1 ||
        hart_find_count(items, nt, L"DEM Lieutenant Governor", L"Perla Mu\x00F1oz") != 1 ||
        hart_find_count(items, nt, L"DEM Proposition 1", L"Yes") != 1)
    {
        uint32_t k;
        wprintf(L"hartpdf: tally mismatch\n");
        for (k = 0; k < nt; k++)
        {
            wprintf(L"   %s | %s | %u\n", items[k].contest, items[k].selection, items[k].count);
        }
        goto done;
    }
    EeCvr_FreeTally(items, nt);
    items = NULL;
    nt = 0;
    /* Value reports on the PDF key columns (Reports -> Polling Place / Device Serial /
     * Voting Type). The fixture's Central Batch Id is blank, so there is no Batch Number
     * column (the Batch report is greyed). */
    if (hart_value_count(&t, L"Polling Place", L"Central Library") != 1 ||
        hart_value_count(&t, L"Polling Place", L"EV - Town Hall") != 1 ||
        hart_value_count(&t, L"Device Serial", L"S1902990909") != 2 ||
        hart_value_count(&t, L"Voting Type", L"Election Day Voting") != 1 ||
        hart_value_count(&t, L"Voting Type", L"Early Voting") != 1)
    {
        wprintf(L"hartpdf: polling place / device serial / voting type counts\n");
        goto done;
    }
    if (EeCvr_FindColumnByTitle(&t, L"Batch Number", &col) &&
        EeCvr_ColumnHasReportableData(&t, col))
    {
        wprintf(L"hartpdf: blank Batch Number reported as reportable\n");
        goto done;
    }
    /* The PDF key columns must stay frozen (not tabulated) after a CSV round trip. */
    if (!hart_csv_roundtrip_ok(&t, 8))
    {
        goto done;
    }

    /* ---- bogus startxref: the object index is rebuilt by scanning ---- */
    paths[0] = pdf_broken;
    s = EeCvr_LoadFromHartFiles(paths, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 2)
    {
        wprintf(L"hartpdf: broken-xref load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }

    /* ---- ZIP + PDF: votes from the zip, device/polling fields from the PDF ---- */
    names[0] = "1_aaaaaaaa-1111-2222-3333-444444444444.xml";
    names[1] = "1_cccccccc-1111-2222-3333-444444444444.xml";
    xmls[0] = k_xa;
    xmls[1] = k_xc;
    if (!hart_write_zip(zip_ok, names, xmls, 2) || !hart_write_zip(zip_none, names + 1, xmls + 1, 1))
    {
        wprintf(L"hartpdf: write zip failed\n");
        goto done;
    }
    paths[0] = zip_ok;
    paths[1] = pdf;
    s = EeCvr_LoadFromHartFiles(paths, 2, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 2)
    {
        wprintf(L"hartpdf: zip+pdf load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    /* CvrGuid, Sheet Number, Batch Sequence, Batch Number, Precinct, Party, Voting Type,
     * Polling Place, Device Type, Device Serial, Device Data Id, Is Blank */
    if (t.frozen_count != 12 || !EeCvr_FindColumnByTitle(&t, L"Device Serial", &col))
    {
        wprintf(L"hartpdf: zip+pdf frozen=%u (want 12)\n", t.frozen_count);
        goto done;
    }
    {
        uint32_t r, matched = 0, blank = 0;
        for (r = 0; r < t.nrows; r++)
        {
            wchar_t g[64];
            EeCvr_GetCellW(&t, r, 0, g, ARRAYSIZE(g));
            EeCvr_GetCellW(&t, r, col, buf, ARRAYSIZE(buf));
            if (wcscmp(g, L"aaaaaaaa-1111-2222-3333-444444444444") == 0 &&
                wcscmp(buf, L"S1902990909") == 0)
            {
                matched++;
            }
            if (wcscmp(g, L"cccccccc-1111-2222-3333-444444444444") == 0 && buf[0] == L'\0')
            {
                blank++; /* no PDF record for this sheet: left blank */
            }
        }
        if (matched != 1 || blank != 1)
        {
            wprintf(L"hartpdf: zip+pdf decoration matched=%u blank=%u\n", matched, blank);
            goto done;
        }
    }
    if (!EeCvr_Tabulate(&t, TRUE, &items, &nt) ||
        hart_find_count(items, nt, L"DEM President", L"Alice") != 1 ||
        hart_find_count(items, nt, L"DEM President", L"Zed") != 1 ||
        hart_find_count(items, nt, L"REP President", L"Zach") != -1)
    {
        wprintf(L"hartpdf: zip+pdf tallies must come from the zip\n");
        goto done;
    }
    EeCvr_FreeTally(items, nt);
    items = NULL;
    nt = 0;
    /* Hart's batch column is "Batch Number" (the Batch report accepts it). */
    if (hart_value_count(&t, L"Batch Number", L"12") != 2 ||
        hart_value_count(&t, L"Polling Place", L"Central Library") != 1)
    {
        wprintf(L"hartpdf: zip+pdf Batch Number / Polling Place counts\n");
        goto done;
    }

    /* ---- ZIP + PDF with no Cvr Id in common -> error ---- */
    paths[0] = zip_none;
    paths[1] = pdf;
    s = EeCvr_LoadFromHartFiles(paths, 2, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Error || wcsstr(err, L"do not match") == NULL)
    {
        wprintf(L"hartpdf: mismatched zip+pdf s=%d err=%s\n", (int)s, err);
        goto done;
    }
    /* ---- county-redacted report ---- */
    if (!EeCvr_IsHartCvrPdf(pdf_redacted, err, ARRAYSIZE(err)))
    {
        wprintf(L"hartpdf: redacted report not detected: %s\n", err);
        goto done;
    }
    paths[0] = pdf_redacted;
    s = EeCvr_LoadFromHartFiles(paths, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 2)
    {
        wprintf(L"hartpdf: redacted load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    /* Only CvrGuid + Precinct survive redaction; the removed fields get no column. */
    if (t.frozen_count != 2 || EeCvr_FindColumnByTitle(&t, L"Polling Place", &col))
    {
        wprintf(L"hartpdf: redacted frozen=%u (want 2)\n", t.frozen_count);
        goto done;
    }
    /* Repeated "CITY COUNCIL MEMBERS" rows are one vote-for-3 contest: a titled column
     * plus two continuation columns, tallied together. */
    if (!EeCvr_FindColumnByTitle(&t, L"CITY COUNCIL MEMBERS", &col) ||
        t.col_group[col + 1] != col || t.col_group[col + 2] != col ||
        (col + 3 < t.ncols && t.col_group[col + 3] == col))
    {
        wprintf(L"hartpdf: vote-for-3 columns not grouped\n");
        goto done;
    }
    if (!EeCvr_Tabulate(&t, TRUE, &items, &nt) ||
        hart_find_count(items, nt, L"CITY COUNCIL MEMBERS", L"Ann") != 2 ||
        hart_find_count(items, nt, L"CITY COUNCIL MEMBERS", L"Ben") != 1 ||
        hart_find_count(items, nt, L"CITY COUNCIL MEMBERS", L"Cy") != 1 ||
        hart_find_count(items, nt, L"CITY COUNCIL MEMBERS", L"Dee") != 1 ||
        hart_find_count(items, nt, L"CITY COUNCIL MEMBERS", L"undervote") != 1 ||
        hart_find_count(items, nt, L"PROPOSITION A", L"YES") != 1 ||
        hart_value_count(&t, L"Precinct", L"18-03") != 1)
    {
        wprintf(L"hartpdf: redacted tallies\n");
        goto done;
    }
    EeCvr_FreeTally(items, nt);
    items = NULL;
    nt = 0;
    rc = 0;
    wprintf(L"hartpdf ok\n");

done:
    EeCvr_FreeTally(items, nt);
    EeCvr_Clear(&t);
    DeleteFileW(pdf);
    DeleteFileW(pdf_broken);
    DeleteFileW(pdf_other);
    DeleteFileW(zip_ok);
    DeleteFileW(zip_none);
    DeleteFileW(pdf_redacted);
    if (rc != 0)
    {
        wprintf(L"hartpdf test failed\n");
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

/* Copy @p s replacing ' with " so JSON fixtures stay readable in C source. */
static char *dom_json(const char *s)
{
    size_t n = strlen(s);
    char *d = (char *)malloc(n + 1);
    size_t i;
    if (d == NULL)
    {
        return NULL;
    }
    for (i = 0; i <= n; i++)
    {
        d[i] = (s[i] == '\'') ? '"' : s[i];
    }
    return d;
}

/* Write a zip of JSON entries given with ' for ". */
static BOOL dom_write_zip(const wchar_t *path,
                          const char *const *names,
                          const char *const *json,
                          int n)
{
    char *conv[16];
    int i;
    BOOL ok = TRUE;
    if (n > 16)
    {
        return FALSE;
    }
    for (i = 0; i < n; i++)
    {
        conv[i] = dom_json(json[i]);
        ok = ok && conv[i] != NULL;
    }
    ok = ok && hart_write_zip(path, names, (const char *const *)conv, n);
    for (i = 0; i < n; i++)
    {
        free(conv[i]);
    }
    return ok;
}

/* Cell text of physical row @p r, column titled @p title ("" if absent). */
static void dom_cell(const EeCvrTable *t,
                     uint32_t r,
                     const wchar_t *title,
                     wchar_t *buf,
                     size_t cch)
{
    uint32_t col = 0;
    buf[0] = L'\0';
    if (EeCvr_FindColumnByTitle(t, title, &col))
    {
        EeCvr_GetCellW(t, r, col, buf, cch);
    }
}

/* Dominion CVR export loading (tag: dominion): manifests + CvrExport_<n>.json ordering,
 * key columns, adjudicated (Modified) sessions, vote-for-2 with overvote/undervote,
 * an ambiguous mark ignored, ranked-choice columns (duplicate ranking, overvoted
 * rank, two write-in lines at one rank = overvote), a county-redacted contest and
 * ballot type, multi-card via the Card column, a 5.2-format export (no Cards; lower
 * rankings and overvoted marks flagged IsVote=false), detection, a multi-zip layout
 * mismatch, and a CSV round trip that keeps the key columns frozen and the RCV
 * contest recognizable. */
static int test_dominion_cvr(void)
{
    static const char *k_contests =
        "{'Version':'5.10.50.85','List':["
        "{'Description':'GOVERNOR','Id':1,'ExternalId':'','DistrictId':1,'VoteFor':1,"
        "'NumOfRanks':0,'Disabled':0},"
        "{'Description':'BOARD','Id':2,'VoteFor':2,'NumOfRanks':0,'Disabled':0},"
        "{'Description':'OLD CONTEST','Id':4,'VoteFor':1,'NumOfRanks':0,'Disabled':1},"
        "{'Description':' MAYOR ','Id':3,'VoteFor':1,'NumOfRanks':3,'Disabled':0}]}";
    static const char *k_cands =
        "{'Version':'5.10.50.85','List':["
        "{'Description':'ANN','Id':10,'ContestId':1,'Type':'Regular'},"
        "{'Description':'BOB','Id':11,'ContestId':1,'Type':'Regular'},"
        "{'Description':'CY','Id':20,'ContestId':2,'Type':'Regular'},"
        "{'Description':'DI','Id':21,'ContestId':2,'Type':'Regular'},"
        "{'Description':'ED','Id':22,'ContestId':2,'Type':'Regular'},"
        "{'Description':'FAY','Id':30,'ContestId':3,'Type':'Regular'},"
        "{'Description':'GUS','Id':31,'ContestId':3,'Type':'Regular'},"
        "{'Description':'HAL \\u00c9','Id':32,'ContestId':3,'Type':'Regular'},"
        "{'Description':'Write-in','Id':33,'ContestId':3,'Type':'WriteIn'}]}";
    static const char *k_portions =
        "{'Version':'x','List':[{'Description':'PCT 1101','Id':1,'ExternalId':'1101-1'}]}";
    static const char *k_btypes =
        "{'Version':'x','List':[{'Description':'Ballot Type 1','Id':1,'ExternalId':'VBM'}]}";
    static const char *k_groups = "{'Version':'x','List':[{'Description':'Election Day','Id':1},"
                                  "{'Description':'Vote by Mail','Id':2}]}";
    static const char *k_tabs =
        "{'Version':'x','List':[{'Description':'ICC01 Vote by Mail','Id':5,"
        "'VotingLocationNumber':1,'VotingLocationName':'City Hall','Type':'ImagecastCentral'}]}";
    /* CvrExport_2.json: S1 (plain), S2 (adjudicated: Modified is current). */
    static const char *k_cvr2 =
        "{'Version':'5.10.50.85','ElectionId':'Test','Sessions':["
        "{'TabulatorId':5,'BatchId':1,'RecordId':1,'CountingGroupId':2,"
        "'ImageMask':'D:\\\\NAS\\\\Batch001\\\\00005_00001_000001*.*','SessionType':'ScannedVote',"
        "'VotingSessionIdentifier':'','UniqueVotingIdentifier':'',"
        "'Original':{'PrecinctPortionId':1,'BallotTypeId':1,'IsCurrent':true,'Cards':[{'Id':1,"
        "'KeyInId':1,'PaperIndex':0,'Contests':["
        "{'Id':1,'Undervotes':0,'Overvotes':0,'OutstackConditionIds':[],'Marks':[{'CandidateId':10,"
        "'Rank':1,'MarkDensity':90,'IsAmbiguous':false,'IsVote':true,'OutstackConditionIds':[]}]},"
        "{'Id':2,'Undervotes':0,'Overvotes':0,'Marks':[{'CandidateId':20,'Rank':1,"
        "'IsAmbiguous':false,'IsVote':true},{'CandidateId':21,'Rank':1,'IsAmbiguous':false,"
        "'IsVote':true}]},"
        "{'Id':3,'Undervotes':0,'Overvotes':0,'Marks':[{'CandidateId':30,'Rank':1,"
        "'IsAmbiguous':false,'IsVote':true},{'CandidateId':31,'Rank':2,'IsAmbiguous':false,"
        "'IsVote':true},{'CandidateId':31,'Rank':3,'IsAmbiguous':false,'IsVote':true}]}],"
        "'OutstackConditionIds':[]}]}},"
        "{'TabulatorId':5,'BatchId':1,'RecordId':2,'CountingGroupId':2,"
        "'ImageMask':'D:\\\\NAS\\\\Batch001\\\\00005_00001_000002*.*','SessionType':'ScannedVote',"
        "'Original':{'PrecinctPortionId':1,'BallotTypeId':1,'IsCurrent':false,'Cards':[{'PaperIndex':0,"
        "'Contests':[{'Id':1,'Undervotes':0,'Overvotes':0,'Marks':[{'CandidateId':11,'Rank':1,"
        "'IsAmbiguous':false,'IsVote':true}]}]}]},"
        "'Modified':{'PrecinctPortionId':1,'BallotTypeId':1,'IsCurrent':true,'Cards':[{'PaperIndex':0,"
        "'Contests':["
        "{'Id':1,'Undervotes':0,'Overvotes':0,'Marks':[{'CandidateId':10,'Rank':1,"
        "'IsAmbiguous':false,'IsVote':true}]},"
        "{'Id':2,'Undervotes':0,'Overvotes':1,'Marks':[{'CandidateId':20,'Rank':1,"
        "'IsAmbiguous':false,'IsVote':false},{'CandidateId':21,'Rank':1,'IsAmbiguous':false,"
        "'IsVote':false},{'CandidateId':22,'Rank':1,'IsAmbiguous':false,'IsVote':false}]},"
        "{'Id':3,'Undervotes':0,'Overvotes':0,'Marks':[{'CandidateId':30,'Rank':1,"
        "'IsAmbiguous':false,'IsVote':true},{'CandidateId':31,'Rank':1,'IsAmbiguous':false,"
        "'IsVote':true},{'CandidateId':32,'Rank':2,'IsAmbiguous':false,'IsVote':true}]}]}]}}]}";
    /* CvrExport_10.json: S3 (card 2; ambiguous mark; write-in), S4 (redacted contest and
     * ballot type), S5 (two write-in lines at rank 1). */
    static const char *k_cvr10 =
        "{'Version':'5.10.50.85','ElectionId':'Test','Sessions':["
        "{'TabulatorId':5,'BatchId':2,'RecordId':'X','CountingGroupId':1,"
        "'ImageMask':'D:\\\\NAS\\\\Batch002\\\\00005_00002_000007*.*','SessionType':'QRVote',"
        "'Original':{'PrecinctPortionId':1,'BallotTypeId':1,'IsCurrent':true,'Cards':[{'PaperIndex':1,"
        "'Contests':["
        "{'Id':1,'Undervotes':1,'Overvotes':0,'Marks':[{'CandidateId':11,'Rank':1,"
        "'IsAmbiguous':true,'IsVote':false}]},"
        "{'Id':2,'Undervotes':1,'Overvotes':0,'Marks':[{'CandidateId':22,'Rank':1,"
        "'IsAmbiguous':false,'IsVote':true}]},"
        "{'Id':3,'Undervotes':0,'Overvotes':0,'Marks':[{'CandidateId':33,'Rank':1,'WriteinIndex':0,"
        "'IsAmbiguous':false,'IsVote':true},{'CandidateId':32,'Rank':2,'IsAmbiguous':false,"
        "'IsVote':true}]}]}]}},"
        "{'TabulatorId':5,'BatchId':2,'RecordId':'X','CountingGroupId':1,"
        "'ImageMask':'D:\\\\NAS\\\\Batch002\\\\00005_00002_000008*.*','SessionType':'ScannedVote',"
        "'Original':{'PrecinctPortionId':1,'BallotTypeId':'*** REDACTED ***','IsCurrent':true,"
        "'Cards':[{'PaperIndex':0,'Contests':["
        "{'Id':1,'Undervotes':0,'Overvotes':0,'Marks':[{'CandidateId':11,'Rank':1,"
        "'IsAmbiguous':false,'IsVote':true}]},"
        "{'Id':3,'Undervotes':'*** REDACTED ***','Overvotes':'*** REDACTED ***',"
        "'OutstackConditionIds':'*** REDACTED ***','Marks':'*** REDACTED ***'}]}]}},"
        "{'TabulatorId':5,'BatchId':2,'RecordId':'X','CountingGroupId':1,"
        "'ImageMask':'D:\\\\NAS\\\\Batch002\\\\00005_00002_000009*.*','SessionType':'ScannedVote',"
        "'Original':{'PrecinctPortionId':1,'BallotTypeId':1,'IsCurrent':true,'Cards':[{'PaperIndex':0,"
        "'Contests':[{'Id':3,'Undervotes':0,'Overvotes':1,'Marks':[{'CandidateId':33,'Rank':1,"
        "'WriteinIndex':0,'IsAmbiguous':false,'IsVote':true},{'CandidateId':33,'Rank':1,"
        "'WriteinIndex':1,'IsAmbiguous':false,'IsVote':true}]}]}]}}]}";
    /* A 5.2-format export: no Cards/SessionType, no contest counts; lower rankings and
     * overvoted marks carry IsVote=false. */
    static const char *k_old_contests =
        "{'Version':'5.2.18.2','List':[{'Description':'MAYOR','Id':1,'VoteFor':1,'NumOfRanks':2},"
        "{'Description':'PROP A','Id':2,'VoteFor':1,'NumOfRanks':0}]}";
    static const char *k_old_cands =
        "{'Version':'5.2.18.2','List':[{'Description':'FAY','Id':30,'ContestId':1,'Type':'Regular'},"
        "{'Description':'GUS','Id':31,'ContestId':1,'Type':'Regular'},"
        "{'Description':'YES','Id':40,'ContestId':2,'Type':'Regular'},"
        "{'Description':'NO','Id':41,'ContestId':2,'Type':'Regular'}]}";
    static const char *k_old_tabs =
        "{'Version':'5.2.18.2','List':[{'Description':'BSM 1101','Id':1101}]}";
    static const char *k_old_cvr =
        "{'Version':'5.2.18.2','ElectionId':'Old','Sessions':["
        "{'TabulatorId':1101,'BatchId':1,'RecordId':1,'CountingGroupId':1,"
        "'ImageMask':'D:\\\\NAS\\\\01101_00001_000001*.*','Original':{'PrecinctPortionId':1,"
        "'BallotTypeId':1,'IsCurrent':true,'Contests':["
        "{'Id':1,'Marks':[{'CandidateId':30,'PartyId':0,'Rank':1,'MarkDensity':85,"
        "'IsAmbiguous':false,'IsVote':true},{'CandidateId':31,'Rank':2,'IsAmbiguous':false,"
        "'IsVote':false}]},"
        "{'Id':2,'Marks':[{'CandidateId':40,'Rank':1,'IsAmbiguous':false,'IsVote':false},"
        "{'CandidateId':41,'Rank':1,'IsAmbiguous':false,'IsVote':false}]}]}},"
        "{'TabulatorId':1101,'BatchId':1,'RecordId':2,'CountingGroupId':1,"
        "'ImageMask':'D:\\\\NAS\\\\01101_00001_000002*.*','Original':{'PrecinctPortionId':1,"
        "'BallotTypeId':1,'IsCurrent':true,'Contests':["
        "{'Id':1,'Marks':[{'CandidateId':30,'Rank':1,'IsAmbiguous':false,'IsVote':false},"
        "{'CandidateId':31,'Rank':1,'IsAmbiguous':false,'IsVote':false}]},"
        "{'Id':2,'Marks':[{'CandidateId':40,'Rank':1,'IsAmbiguous':false,'IsVote':true}]}]}}]}";
    /* Entry order deliberately puts _10 before _2: rows must follow the export number. */
    const char *names[8] = {"ContestManifest.json",
                            "CandidateManifest.json",
                            "PrecinctPortionManifest.json",
                            "BallotTypeManifest.json",
                            "CountingGroupManifest.json",
                            "TabulatorManifest.json",
                            "CvrExport_10.json",
                            "CvrExport_2.json"};
    const char *json[8];
    const char *old_names[6] = {"ContestManifest.json",
                                "CandidateManifest.json",
                                "PrecinctPortionManifest.json",
                                "BallotTypeManifest.json",
                                "TabulatorManifest.json",
                                "CvrExport.json"};
    const char *old_json[6];
    wchar_t zpath[MAX_PATH];
    wchar_t opath[MAX_PATH];
    wchar_t hpath[MAX_PATH];
    wchar_t cpath[MAX_PATH];
    wchar_t err[512] = L"";
    wchar_t buf[128];
    const wchar_t *paths[2];
    EeCvrTable t;
    EeCvrTable t2;
    EeLoadStatus s;
    EeCvrTally *items = NULL;
    uint32_t nt = 0;
    uint32_t rcv[4];
    int rc = 1;

    json[0] = k_contests;
    json[1] = k_cands;
    json[2] = k_portions;
    json[3] = k_btypes;
    json[4] = k_groups;
    json[5] = k_tabs;
    json[6] = k_cvr10;
    json[7] = k_cvr2;
    old_json[0] = k_old_contests;
    old_json[1] = k_old_cands;
    old_json[2] = k_portions;
    old_json[3] = k_btypes;
    old_json[4] = k_old_tabs;
    old_json[5] = k_old_cvr;

    EeCvr_Init(&t);
    EeCvr_Init(&t2);
    if (!cvr_temp_path(zpath, ARRAYSIZE(zpath), L"ee_dominion.zip") ||
        !cvr_temp_path(opath, ARRAYSIZE(opath), L"ee_dominion_52.zip") ||
        !cvr_temp_path(hpath, ARRAYSIZE(hpath), L"ee_dominion_not.zip") ||
        !cvr_temp_path(cpath, ARRAYSIZE(cpath), L"ee_dominion_rt.csv") ||
        !dom_write_zip(zpath, names, json, 8) || !dom_write_zip(opath, old_names, old_json, 6))
    {
        wprintf(L"dominion: fixture write failed\n");
        return 1;
    }
    {
        const char *hn[1] = {"1_AAA.xml"};
        const char *hx[1] = {"<Cvr><Contests /></Cvr>"};
        if (!hart_write_zip(hpath, hn, hx, 1))
        {
            wprintf(L"dominion: hart fixture write failed\n");
            return 1;
        }
    }
    if (!EeCvr_IsDominionZip(zpath) || !EeCvr_IsDominionZip(opath) || EeCvr_IsDominionZip(hpath))
    {
        wprintf(L"dominion: detection wrong\n");
        return 1;
    }

    paths[0] = zpath;
    s = EeCvr_LoadFromDominionZips(paths, 1, &t, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t.nrows != 5)
    {
        wprintf(L"dominion: load s=%d rows=%u err=%s\n", (int)s, t.nrows, err);
        goto done;
    }
    /* Keys: Record Id, Tabulator, Batch, Counting Group, Polling Place, Precinct Portion,
     * Ballot Type, Session Type, Card, Adjudicated; then GOVERNOR, BOARD x2, MAYOR x3
     * (the disabled contest has no column). */
    if (t.frozen_count != 10 || t.ncols != 16 || wcscmp(t.col_titles[0], L"Record Id") != 0 ||
        wcscmp(t.col_titles[11], L"BOARD") != 0 || wcscmp(t.col_titles[12], L"BOARD (2)") != 0 ||
        wcscmp(t.col_titles[13], L"MAYOR (Rank 1)") != 0 ||
        wcscmp(t.col_titles[15], L"MAYOR (Rank 3)") != 0)
    {
        wprintf(L"dominion: layout frozen=%u ncols=%u\n", t.frozen_count, t.ncols);
        goto done;
    }
    dom_cell(&t, 0, L"Record Id", buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"00005_00001_000001") != 0)
    {
        wprintf(L"dominion: row order / record id = %s\n", buf);
        goto done;
    }
    dom_cell(&t, 0, L"Batch", buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"00005-00001") != 0)
    {
        wprintf(L"dominion: batch = %s\n", buf);
        goto done;
    }
    dom_cell(&t, 0, L"Polling Place", buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"City Hall") != 0)
    {
        wprintf(L"dominion: polling place = %s\n", buf);
        goto done;
    }
    dom_cell(&t, 1, L"Adjudicated", buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"Yes") != 0)
    {
        wprintf(L"dominion: adjudicated = %s\n", buf);
        goto done;
    }
    dom_cell(&t, 2, L"Card", buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"2") != 0)
    {
        wprintf(L"dominion: card = %s\n", buf);
        goto done;
    }
    dom_cell(&t, 3, L"Ballot Type", buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"<Redacted>") != 0)
    {
        wprintf(L"dominion: redacted ballot type = %s\n", buf);
        goto done;
    }
    dom_cell(&t, 0, L"MAYOR (Rank 3)", buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"GUS") != 0)
    {
        wprintf(L"dominion: duplicate ranking = %s\n", buf);
        goto done;
    }
    dom_cell(&t, 1, L"MAYOR (Rank 2)", buf, ARRAYSIZE(buf));
    if (wcscmp(buf, L"HAL \x00C9") != 0) /* \u escape decoded to UTF-8 */
    {
        wprintf(L"dominion: unicode name = %s\n", buf);
        goto done;
    }
    if (!EeCvr_HasMultiCard(&t))
    {
        wprintf(L"dominion: multi-card (Card 2) not detected\n");
        goto done;
    }
    if (EeCvr_FindRcvContests(&t, rcv, 4) != 1 || rcv[0] != 13)
    {
        wprintf(L"dominion: rcv contests\n");
        goto done;
    }
    if (!EeCvr_Tabulate(&t, FALSE, &items, &nt))
    {
        wprintf(L"dominion: tabulate failed\n");
        goto done;
    }
    /* GOVERNOR: adjudicated ANN, ANN, BOB; S3's ambiguous mark is not a vote. */
    if (hart_find_count(items, nt, L"GOVERNOR", L"ANN") != 2 ||
        hart_find_count(items, nt, L"GOVERNOR", L"BOB") != 1 ||
        hart_find_count(items, nt, L"GOVERNOR", L"undervote") != 1 ||
        /* BOARD (vote for 2): CY+DI, overvoted (2 cells), ED + one unused vote. */
        hart_find_count(items, nt, L"BOARD", L"CY") != 1 ||
        hart_find_count(items, nt, L"BOARD", L"ED") != 1 ||
        hart_find_count(items, nt, L"BOARD", L"overvote") != 2 ||
        hart_find_count(items, nt, L"BOARD", L"undervote") != 1 ||
        /* MAYOR rank 1: FAY, overvote (FAY+GUS), Write-in, <Redacted>, overvote (two
         * write-in lines). */
        hart_find_count(items, nt, L"MAYOR (Rank 1)", L"FAY") != 1 ||
        hart_find_count(items, nt, L"MAYOR (Rank 1)", L"overvote") != 2 ||
        hart_find_count(items, nt, L"MAYOR (Rank 1)", L"Write-in") != 1 ||
        hart_find_count(items, nt, L"MAYOR (Rank 1)", L"<Redacted>") != 1)
    {
        wprintf(L"dominion: tallies wrong\n");
        goto done;
    }

    /* Two exports with different contests do not load together. */
    paths[1] = opath;
    s = EeCvr_LoadFromDominionZips(paths, 2, &t2, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Error || wcsstr(err, L"ee_dominion_52.zip") == NULL)
    {
        wprintf(L"dominion: mismatch not rejected s=%d err=%s\n", (int)s, err);
        goto done;
    }

    /* 5.2 format. */
    paths[0] = opath;
    s = EeCvr_LoadFromDominionZips(paths, 1, &t2, NULL, NULL, NULL, err, ARRAYSIZE(err));
    if (s != EeLoadStatus_Ok || t2.nrows != 2 || t2.frozen_count != 7)
    {
        wprintf(L"dominion: 5.2 load s=%d rows=%u frozen=%u err=%s\n",
                (int)s,
                t2.nrows,
                t2.frozen_count,
                err);
        goto done;
    }
    {
        wchar_t r2[64], ov[64], pa[64], pb[64];
        dom_cell(&t2, 0, L"MAYOR (Rank 2)", r2, ARRAYSIZE(r2));
        dom_cell(&t2, 1, L"MAYOR (Rank 1)", ov, ARRAYSIZE(ov));
        dom_cell(&t2, 0, L"PROP A", pa, ARRAYSIZE(pa));
        dom_cell(&t2, 1, L"PROP A", pb, ARRAYSIZE(pb));
        if (wcscmp(r2, L"GUS") != 0 || wcscmp(ov, L"overvote") != 0 ||
            wcscmp(pa, L"overvote") != 0 || wcscmp(pb, L"YES") != 0)
        {
            wprintf(L"dominion: 5.2 cells r2=%s ov=%s pa=%s pb=%s\n", r2, ov, pa, pb);
            goto done;
        }
    }
    EeCvr_Clear(&t2);

    /* CSV round trip: the Dominion key columns stay frozen; the RCV contest survives. */
    {
        uint32_t *rows = (uint32_t *)malloc(t.nrows * sizeof(uint32_t));
        char *text = NULL;
        size_t len = 0;
        FILE *fp = NULL;
        uint32_t i;
        BOOL ok = rows != NULL;
        for (i = 0; ok && i < t.nrows; i++)
        {
            rows[i] = i;
        }
        ok = ok && EeCvr_FormatDelimitedUtf8(&t, rows, t.nrows, ',', TRUE, &text, &len);
        ok = ok && _wfopen_s(&fp, cpath, L"wb") == 0 && fp != NULL &&
             fwrite(text, 1, len, fp) == len;
        if (fp != NULL)
        {
            fclose(fp);
        }
        free(rows);
        free(text);
        paths[0] = cpath;
        if (!ok ||
            EeCvr_LoadFromFiles(paths, 1, &t2, NULL, NULL, NULL, err, ARRAYSIZE(err)) !=
                EeLoadStatus_Ok ||
            t2.frozen_count != 10 || EeCvr_FindRcvContests(&t2, NULL, 0) != 1)
        {
            wprintf(L"dominion: csv round trip frozen=%u err=%s\n", t2.frozen_count, err);
            goto done;
        }
    }
    wprintf(L"dominion ok\n");
    rc = 0;

done:
    EeCvr_FreeTally(items, nt);
    EeCvr_Clear(&t);
    EeCvr_Clear(&t2);
    DeleteFileW(zpath);
    DeleteFileW(opath);
    DeleteFileW(hpath);
    DeleteFileW(cpath);
    return rc;
}

/* Ranked-choice instant runoff (tag: rcv): rounds, transfers, a tie for last broken by
 * name then by the earlier round, a skipped first ranking, an overvoted ranking (stops
 * the ballot when reached), an unresolved write-in (excluded -> blank), blanks, rows
 * without the contest, exhausted ballots, majority round, finishing order, and a
 * filtered subset. */
static int test_rcv(void)
{
    static const char *k_hdr[6] =
        {"Id", "X (Rank 1)", "X (Rank 2)", "X (Rank 3)", "Solo (Rank 1)", "Other"};
    /* ballots: rank1, rank2, rank3 ("" = contest absent) */
    static const char *k_rows[][3] = {{"A", "undervote", "undervote"},
                                      {"A", "undervote", "undervote"},
                                      {"A", "undervote", "undervote"},
                                      {"A", "undervote", "undervote"},
                                      {"B", "C", "undervote"},
                                      {"B", "C", "undervote"},
                                      {"B", "C", "undervote"},
                                      {"C", "B", "undervote"},
                                      {"C", "B", "undervote"},
                                      {"D", "overvote", "A"},
                                      {"undervote", "D", "C"},
                                      {"Write-in", "undervote", "undervote"},
                                      {"undervote", "undervote", "undervote"},
                                      {"overvote", "A", "B"},
                                      {"", "", ""}};
    const uint32_t nrows = (uint32_t)ARRAYSIZE(k_rows);
    EeCvrTable t;
    EeRcvResult r;
    uint32_t i;
    uint32_t first[4];
    uint32_t sub[7] = {0, 1, 2, 3, 4, 5, 6};
    int rc = 1;

    EeCvr_Init(&t);
    ZeroMemory(&r, sizeof(r));
    if (!EeCvr_BuildBegin(&t, k_hdr, 6, 1))
    {
        wprintf(L"rcv: build failed\n");
        return 1;
    }
    for (i = 0; i < nrows; i++)
    {
        char id[8];
        const char *cells[6];
        StringCchPrintfA(id, ARRAYSIZE(id), "%u", i + 1);
        cells[0] = id;
        cells[1] = k_rows[i][0];
        cells[2] = k_rows[i][1];
        cells[3] = k_rows[i][2];
        cells[4] = "";
        cells[5] = (i == nrows - 1) ? "yes" : "";
        if (!EeCvr_BuildAppendRow(&t, cells, 6))
        {
            wprintf(L"rcv: append failed\n");
            goto done;
        }
    }
    /* "Solo (Rank 1)" without a Rank 2 is not a ranked-choice contest. */
    if (EeCvr_FindRcvContests(&t, first, 4) != 1 || first[0] != 1)
    {
        wprintf(L"rcv: find contests\n");
        goto done;
    }
    if (!EeCvr_TabulateRcv(&t, 1, NULL, 0, &r))
    {
        wprintf(L"rcv: tabulate failed\n");
        goto done;
    }
    /* Round 1: A4 B3 C2 D2 (D via the skipped rank 1), blanks 2 (all-undervote and the
     * write-in-only ballot), overvotes 1. C/D tie -> D (later name) out, flagged.
     * Round 2: the D,overvote ballot stops (overvotes 2); the skip,D,C ballot moves to C:
     * A4 B3 C3 -> B/C tie broken by round 1 (C had 2) -> C out. Round 3: B5 A4, the
     * skip,D,C ballot exhausts. Finishing order B, A, C, D. */
    if (wcscmp(r.contest, L"X") != 0 || r.nranks != 3 || r.ncand != 4 || r.nrounds != 3 ||
        r.winner != 0 || wcscmp(r.cand[0], L"B") != 0 || wcscmp(r.cand[1], L"A") != 0 ||
        wcscmp(r.cand[2], L"C") != 0 || wcscmp(r.cand[3], L"D") != 0)
    {
        wprintf(L"rcv: shape rounds=%u ncand=%u\n", r.nrounds, r.ncand);
        goto done;
    }
    if (r.votes[0 * 4 + 0] != 3 || r.votes[0 * 4 + 1] != 4 || r.votes[0 * 4 + 2] != 2 ||
        r.votes[0 * 4 + 3] != 2 || r.votes[1 * 4 + 2] != 3 || r.votes[1 * 4 + 3] != 0 ||
        r.votes[2 * 4 + 0] != 5 || r.votes[2 * 4 + 1] != 4)
    {
        wprintf(L"rcv: votes wrong\n");
        goto done;
    }
    if (r.continuing[0] != 11 || r.blanks[0] != 2 || r.overvotes[0] != 1 || r.exhausted[0] != 0 ||
        r.continuing[1] != 10 || r.overvotes[1] != 2 || r.continuing[2] != 9 ||
        r.exhausted[2] != 1 || r.blanks[2] != 2)
    {
        wprintf(L"rcv: totals wrong\n");
        goto done;
    }
    if (r.eliminated[0] != 3 || !r.elim_tie[0] || r.eliminated[1] != 2 || !r.elim_tie[1] ||
        r.eliminated[2] != -1 || r.majority_round != 3)
    {
        wprintf(L"rcv: eliminations wrong\n");
        goto done;
    }
    EeCvr_FreeRcvResult(&r);
    /* Filtered subset (the A and B,C ballots): C (ranked only second) has no first-round
     * votes and goes out after round 1; A wins with a majority from round 1. */
    if (!EeCvr_TabulateRcv(&t, 1, sub, 7, &r) || r.nrounds != 2 || r.winner != 0 ||
        wcscmp(r.cand[0], L"A") != 0 || r.majority_round != 1 || r.continuing[0] != 7)
    {
        wprintf(L"rcv: filtered subset wrong\n");
        goto done;
    }
    wprintf(L"rcv ok\n");
    rc = 0;

done:
    EeCvr_FreeRcvResult(&r);
    EeCvr_Clear(&t);
    return rc;
}

int wmain(void)
{
    int failed = 0;

    failed |= load_sample(L"test\\sample_voters.csv", L"csv");
    failed |= load_sample(L"test\\sample_voters.txt", L"txt");
    failed |= load_wide_history();
    failed |= test_copy_format();
    failed |= test_voter_export();
    failed |= test_zip4_omits_zeros();
    failed |= test_res_addr_fields();
    failed |= test_res_addr_no_duplicate_city_state_zip();
    failed |= test_district_codes_not_appended();
    failed |= test_infer_residence_state();
    failed |= test_el_paso_layout();
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
    failed |= test_cvr_export();
    failed |= test_cvr_roundtrip();
    failed |= test_hart_cvr();
    failed |= test_hart_pdf();
    failed |= test_cvr_whitespace();
    failed |= test_xlsx_writein();
    failed |= test_dominion_cvr();
    failed |= test_rcv();
    return failed == 0 ? 0 : 1;
}
