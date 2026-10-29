/**
 * @file resource.h
 * @brief Resource and command IDs for ElectionExplorer.
 */
#pragma once

#define IDI_APPICON  1
#define IDR_MAINMENU 101

#define IDM_FILE_OPEN_VOTER_LIST  40001
#define IDM_FILE_EXIT             40002
#define IDM_FILE_CLOSE_VOTER_LIST 40003
#define IDM_FILE_OPEN_CVR         40004
#define IDM_FILE_CLOSE_CVR        40005
#define IDM_FILE_EXPORT_VOTERS    40006
#define IDM_FILE_EXPORT_CVR       40007
#define IDM_EDIT_COPY             40010
#define IDM_EDIT_OPTIONS          40011
#define IDM_CVR_TABULATE          40012
#define IDM_CVR_OPTIONS           40013
#define IDM_CVR_FILTER            40014
#define IDM_CVR_FILTER_RESET      40015
#define IDM_CVR_REPORT_BATCH      40016
#define IDM_CVR_REPORT_PRECINCT   40017
#define IDM_CVR_REPORT_BALLOTSTYLE 40018
#define IDM_EXPORT_SELECTED       40019
#define IDM_CVR_TABULATE_FILTERED 40073
/* Hart PDF key-column value reports (CVR Reports menu). */
#define IDM_CVR_REPORT_POLLINGPLACE 40074
#define IDM_CVR_REPORT_DEVICESERIAL 40075
#define IDM_CVR_REPORT_VOTINGTYPE   40076
/* Ranked-choice (instant-runoff) round-by-round tabulation (CVR Reports menu). */
#define IDM_CVR_TABULATE_RCV          40077
#define IDM_CVR_TABULATE_RCV_FILTERED 40078
/* Export "All" for report context menus. */
#define IDM_EXPORT_ALL            40072

/* CVR options dialog child controls. */
#define IDC_CVR_MERGE_WRITEINS 41400
#define IDC_CVR_PARTY_REP      41401
#define IDC_CVR_PARTY_DEM      41402

/* Export options dialog child control. */
#define IDC_EXPORT_NORMALIZED 41500
#define IDM_FILTER_EDIT           40020
#define IDM_FILTER_RESET          40021
#define IDM_FILTER_DUP_VOTER_IDS  40026
#define IDM_FILTER_DUP_VOTERS     40027
#define IDM_FILTER_RESET_VIEW     40028
#define IDM_REPORT_PRECINCT       40029
#define IDM_REPORT_ADDRESS        40030
#define IDM_COMPARE_SHOW_A        40032
#define IDM_COMPARE_SHOW_B        40033
/* Dynamic "Compare with <file>" items: one per other open viewer. */
#define IDM_COMPARE_WITH_FIRST 40040
#define IDM_COMPARE_WITH_LAST  40071
#define IDM_FILTER_INCLUDE     40022
#define IDM_FILTER_EXCLUDE     40023
#define IDM_SHOW_IN_MAPS       40024
#define IDM_OPEN_LINK          40025
#define IDM_HELP_OPTIONS       40090
#define IDM_HELP_FILTERS       40091
#define IDM_HELP_REPORTS       40092
#define IDM_HELP_COMPARE       40093
#define IDM_HELP_ABOUT         40094
#define IDM_HELP_EXPORT        40095
#define IDM_HELP_LOADING       40096
/* Voter Roster window (1.4). */
#define IDM_FILE_OPEN_ROSTER    40100
#define IDM_REPORT_LOAD_SUMMARY 40101
#define IDM_REPORT_VOTING_TOTALS 40102
#define IDM_FILTER_DUP_VOTERS_VOTING 40103
/* Election Results window (1.5). */
#define IDM_FILE_OPEN_RESULTS   40110
#define IDM_FILE_CLOSE_RESULTS  40111
#define IDM_RES_VIEW_TOTALS     40112
#define IDM_RES_VIEW_PRECINCT   40113
#define IDM_RES_OPTIONS         40114
#define IDM_RES_LOAD_SUMMARY    40115
#define IDM_RES_TURNOUT         40116
#define IDM_RESCMP_EXPORT       40117
#define IDM_RESCMP_CLOSE        40118
#define IDM_RESCMP_DIFFONLY     40119
#define IDM_FILE_EXPORT_RESULTS 40120
#define IDM_RES_VIEW_RESET      40121
/* Audit Log window (1.6). */
#define IDM_FILE_OPEN_AUDIT     40130
#define IDM_FILE_CLOSE_AUDIT    40131
#define IDM_AUD_OPTIONS         40132
#define IDM_AUD_VIEW_RESET      40133
#define IDM_AUD_LOG_ALL         40134
#define IDM_AUD_LOG_ELECTION    40135
#define IDM_AUD_LOG_ADMIN       40136
#define IDM_AUD_COPY_MERGED     40137
#define IDM_AUD_VIEW_EVENTS     40138
#define IDM_AUD_VIEW_BATCHES    40139
#define IDM_AUD_REP_COPIES      40140
#define IDM_AUD_REP_ACCT        40141
#define IDM_AUD_SELECT_SESSION  40142
#define IDM_AUD_GOTO_EVENT      40143
#define IDM_AUD_USE_SESSION     40144
#define IDM_AUD_REP_TIMELINE    40145
#define IDM_AUD_REP_NOTABLE     40146
#define IDM_AUD_REP_USERS       40147
#define IDM_AUD_SHOW_EVENTS     40148
/* View -> Copy: one item per copy, IDM_AUD_COPY_FIRST + log * EE_AUDIT_MAX_COPIES + copy
 * (two logs of up to 64 copies: 40200..40327). */
#define IDM_AUD_COPY_FIRST      40200

/* Election Results options dialog. */
#define IDC_RES_ZOOM      41600
#define IDC_RES_ZOOM_SPIN 41601
#define IDC_RES_SHOW_VT   41602
#define IDC_RES_PARTY_REP 41603
#define IDC_RES_PARTY_DEM 41604
#define IDC_RES_EXPORT_VIEW   41605
#define IDC_RES_EXPORT_DETAIL 41606

/* Audit Log options dialog. */
#define IDC_AUD_ZOOM      41700
#define IDC_AUD_ZOOM_SPIN 41701

/* Help / About dialog child controls. */
#define IDC_HELP_TEXT     41200
#define IDC_ABOUT_ICON    41201
#define IDC_ABOUT_TEXT    41202
#define IDC_ABOUT_LINK    41203
#define IDC_ABOUT_PRIVACY 41204

/* Sheet-picker dialog (XLSX). */
#define IDC_SHEET_LIST  41300
#define IDC_SHEET_LABEL 41301

/* Compare / Differences window child controls. */
#define IDC_CMP_DIFF 41210
#define IDC_CMP_EXPORT_SUMMARY 41211
#define IDC_CMP_EXPORT_ALL 41212

#define IDC_FLT_PROMPT   41100
#define IDC_FLT_COLUMN   41101
#define IDC_FLT_RELATION 41102
#define IDC_FLT_VALUE    41103
#define IDC_FLT_ACTION   41104
#define IDC_FLT_ADD      41105
#define IDC_FLT_REMOVE   41106
#define IDC_FLT_LIST     41107
#define IDC_FLT_RESET    41108
#define IDC_FLT_APPLY    41109
#define IDC_FLT_EDIT     41110
#define IDC_FLT_ENABLE   41111
#define IDC_FLT_DISABLE  41112
#define IDC_FLT_EXPORT   41113
#define IDC_FLT_LOAD     41114

#define IDC_OPT_PREPEND       41010
#define IDC_OPT_SURNAME_FIRST 41011
#define IDC_OPT_ZOOM_EDIT     41012
#define IDC_OPT_MAP_ENGINE    41013

#define IDC_PROGRESS_BAR    41001
#define IDC_PROGRESS_CANCEL 41002
#define IDC_PROGRESS_STATUS 41003

#define IDD_LOAD_PROGRESS 42001

/* User messages (main window). */
#define EEM_LOAD_PROGRESS      (WM_APP + 1)
#define EEM_LOAD_FINISHED      (WM_APP + 2)
#define EEM_SYNC_PANE_SCROLLUI (WM_APP + 3)
#define EEM_SYNC_SELECTION     (WM_APP + 4)
#define EEM_SCAN_PROGRESS      (WM_APP + 5)
#define EEM_SCAN_FINISHED      (WM_APP + 6)
#define EEM_CMP_FINISHED       (WM_APP + 7)
