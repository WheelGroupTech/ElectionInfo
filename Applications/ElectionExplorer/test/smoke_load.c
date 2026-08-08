#include <stdio.h>
#include <windows.h>
#include "voter_table.h"
int wmain(void) {
    EeVoterTable t;
    wchar_t err[256];
    EeLoadStatus s;
    uint32_t i;
    EeVoterTable_Init(&t);
    err[0]=0;
    s = EeVoterTable_LoadFromFile(L"test\\sample_voters.csv", &t, NULL, NULL, NULL, err, 256);
    wprintf(L"csv status=%d rows=%u cols=%u err=%s\n", (int)s, t.row_count, t.column_count, err);
    if (s==EeLoadStatus_Ok && t.row_count>0) {
        wchar_t buf[128];
        for (i=0;i<t.column_count && i<6;i++) wprintf(L"  col%u: %s\n", i, t.column_titles[i]);
        EeVoterTable_GetViewCellW(&t, 0, 0, buf, 128); wprintf(L"  row0 VoterID=%s\n", buf);
        EeVoterTable_GetViewCellW(&t, 0, 1, buf, 128); wprintf(L"  row0 Name=%s\n", buf);
        EeVoterTable_SortByColumn(&t, 1);
        EeVoterTable_GetViewCellW(&t, 0, 1, buf, 128); wprintf(L"  after sort by name first=%s\n", buf);
    }
    EeVoterTable_Clear(&t);
    EeVoterTable_Init(&t);
    s = EeVoterTable_LoadFromFile(L"test\\sample_voters.txt", &t, NULL, NULL, NULL, err, 256);
    wprintf(L"txt status=%d rows=%u cols=%u\n", (int)s, t.row_count, t.column_count);
    if (s==EeLoadStatus_Ok) {
        wchar_t buf[128];
        EeVoterTable_GetViewCellW(&t, 0, 0, buf, 128); wprintf(L"  row0 VoterID=%s\n", buf);
        EeVoterTable_GetViewCellW(&t, 0, 1, buf, 128); wprintf(L"  row0 Name=%s\n", buf);
    }
    EeVoterTable_Clear(&t);
    return (s==EeLoadStatus_Ok)?0:1;
}
