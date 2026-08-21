/**
 * @file smoke_load.c
 * @brief Console smoke test for EeVoterTable_LoadFromFile.
 */

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
    if (strncmp(text, "100001,\"Smith, John A\",100001,", 30) != 0)
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

int wmain(void)
{
    int failed = 0;

    failed |= load_sample(L"test\\sample_voters.csv", L"csv");
    failed |= load_sample(L"test\\sample_voters.txt", L"txt");
    failed |= load_wide_history();
    failed |= test_copy_format();
    return failed == 0 ? 0 : 1;
}
