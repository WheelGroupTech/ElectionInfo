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
#define IDM_EDIT_COPY             40010
#define IDM_EDIT_OPTIONS          40011
#define IDM_FILTER_EDIT           40020
#define IDM_FILTER_RESET          40021
#define IDM_FILTER_INCLUDE        40022
#define IDM_FILTER_EXCLUDE        40023
#define IDM_SHOW_IN_MAPS          40024

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
